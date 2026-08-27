package snowflake

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/snowflakedb/gosnowflake"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// ErrCreateReadBack marks a failure of the post-INSERT read-back used to populate
// DB-generated fields. Errors wrapping it mean the write itself succeeded and only
// the subsequent CHANGES(...) query failed, so callers can tell "the row was never
// written" apart from "the row exists but its generated fields are unpopulated".
// The underlying driver error is wrapped alongside it and stays reachable via
// errors.As.
var ErrCreateReadBack = errors.New("snowflake: insert succeeded but reading back DB-generated fields failed")

func Create(db *gorm.DB) {
	if db.Statement.Schema != nil && !db.Statement.Unscoped {
		for _, c := range db.Statement.Schema.CreateClauses {
			db.Statement.AddClause(c)
		}
	}

	if db.Statement.SQL.String() == "" {
		var (
			values                  = callbacks.ConvertToCreateValues(db.Statement)
			c                       = db.Statement.Clauses["ON CONFLICT"]
			onConflict, hasConflict = c.Expression.(clause.OnConflict)
		)

		if hasConflict {
			if len(db.Statement.Schema.PrimaryFields) > 0 {
				// Pre-allocate map with exact capacity
				columnsMap := make(map[string]bool, len(values.Columns))
				for _, column := range values.Columns {
					columnsMap[column.Name] = true
				}

				// Early exit on first missing field
				for _, field := range db.Statement.Schema.PrimaryFields {
					if !columnsMap[field.DBName] {
						hasConflict = false
						break
					}
				}
			} else {
				hasConflict = false
			}
		}

		if hasConflict {
			MergeCreate(db, onConflict, values)
		} else {
			db.Statement.AddClauseIfNotExists(clause.Insert{})
			db.Statement.Build("INSERT")
			db.Statement.WriteByte(' ')
			db.Statement.AddClause(values)

			if values, ok := db.Statement.Clauses["VALUES"].Expression.(clause.Values); ok {
				columnCount := len(values.Columns)
				if columnCount > 0 {
					// Determine insertion method based on configuration
					useUnionSelect := shouldUseUnionSelect(db)

					if useUnionSelect {
						buildUnionSelectInsert(db, values)
					} else {
						buildValuesInsert(db, values)
					}
				} else {
					// only one autoincrement column
					db.Statement.WriteString("VALUES (DEFAULT);")
				}
			}
		}
	}

	if !db.DryRun && db.Error == nil {
		db.RowsAffected = 0

		// Work out up front whether anything will need reading back. Only the fields some
		// row actually left unset are worth fetching: if the caller supplied every
		// DB-generated value there is nothing to learn, and issuing the query anyway turns
		// a healthy insert into a failure on tables without change tracking.
		var readBackFields []*schema.Field
		if sch := db.Statement.Schema; sch != nil && len(sch.FieldsWithDefaultDBValue) > 0 && shouldReadBackDefaults(db) {
			readBackFields = fieldsNeedingReadBack(db, sch)
		}

		execCtx := db.Statement.Context
		readQueryID := func() string { return "" }

		if len(readBackFields) > 0 {
			// Capture the query id Snowflake assigns to this insert. The version specifier
			// in BEFORE(statement=>...) must be a constant expression, so the id has to be
			// inlined as a literal; LAST_QUERY_ID() is a function call in that position and
			// is rejected. Carrying the id in the client rather than in session state also
			// keeps the read-back correct over a pooled *sql.DB, where the insert and the
			// read-back are not guaranteed to share a connection.
			//
			// Attached only when it will be used. gosnowflake sends on this channel and
			// closes it, but clears its own reference in a local variable that does not
			// propagate back to the caller, so a context handed to it twice would send on
			// a closed channel and panic.
			execCtx, readQueryID = queryIDCapture(db.Statement.Context)
		}

		// exec the merge/insert first
		if result, err := db.Statement.ConnPool.ExecContext(execCtx, db.Statement.SQL.String(), db.Statement.Vars...); err == nil {
			db.RowsAffected, _ = result.RowsAffected()
		} else {
			_ = db.AddError(err)
		}

		db.Logger.Info(db.Statement.Context, fmt.Sprintf("This is the result of insert %s, values %v, rows affected %d", db.Statement.SQL.String(), db.Statement.Vars, db.RowsAffected))

		// do another select on last inserted values to populate default values (e.g. ID)
		// this relies on the result of SELECT * FROM CHANGES to align with the order of the VALUES in MERGE statement
		if sch := db.Statement.Schema; sch != nil && db.Error == nil && len(readBackFields) > 0 {
			fields := readBackFields

			// Without a usable id there is no constant expression to time travel to. This
			// is an error rather than a warning: the insert is committed, so staying quiet
			// would hand the caller a struct whose generated fields are a plausible-looking
			// zero, and anything keyed on that value would target the wrong row.
			insertQueryID := readQueryID()
			if !validQueryID(insertQueryID) {
				db.AddError(fmt.Errorf(
					"%w: the insert into %s reported no usable query id, so %s cannot be populated. "+
						"Set Config.DisableCreateReadBack, or tag the model so they are not treated as "+
						"DB-generated, if these values are not needed",
					ErrCreateReadBack, sch.Table, strings.Join(fieldDBNames(fields), ", ")))
				return
			}

			db.Statement.SQL.Reset()
			writeReadBackQuery(db.Statement, sch.Table, fields, insertQueryID)

			values := make([]interface{}, len(fields))

			// The read-back carries no placeholders, so it takes no bind variables. Passing
			// the insert's vars would bind them to a statement with nowhere to put them.
			rows, err := db.Statement.ConnPool.QueryContext(db.Statement.Context, db.Statement.SQL.String())
			if err != nil {
				hint := "Tag the model so these are not treated as DB-generated, or set Config.DisableCreateReadBack."
				if isChangeTrackingError(err) {
					hint = fmt.Sprintf("Change tracking is unusable on %s. Run ALTER TABLE %s SET CHANGE_TRACKING = TRUE, "+
						"or tag the model so these are not treated as DB-generated.", sch.Table, sch.Table)
				}
				db.Logger.Warn(db.Statement.Context, fmt.Sprintf(
					"insert into %s succeeded but reading back %s failed: %v. %s",
					sch.Table, strings.Join(fieldDBNames(fields), ", "), err, hint))
				db.AddError(fmt.Errorf("%w: %w", ErrCreateReadBack, err))
				return
			}
			defer rows.Close()

			reflectValue := db.Statement.ReflectValue
			reflectKind := reflectValue.Kind()

			switch reflectKind {
			case reflect.Slice, reflect.Array:
				reflectIndex := 0
				maxLen := reflectValue.Len()

				// the strategy here is to match the returned rows with INSERT only values
				for rows.Next() && reflectIndex < maxLen {
					// Find next valid struct for insertion
					for reflectIndex < maxLen {
						currentValue := reflectValue.Index(reflectIndex)
						if reflect.Indirect(currentValue).Kind() != reflect.Struct {
							break
						}

						// Check if this row has zero defaults (indicates INSERT operation)
						hasNonZeroDefaults := false
						for _, field := range fields {
							fieldValue := field.ReflectValueOf(db.Statement.Context, currentValue)
							if !fieldValue.IsZero() {
								hasNonZeroDefaults = true
								break
							}
						}

						if hasNonZeroDefaults {
							// Skip this row, move to next record
							reflectIndex++
							if reflectIndex >= maxLen {
								return
							}
							continue
						}

						// Found a valid INSERT row - populate interface slice for scanning
						for idx, field := range fields {
							fieldValue := field.ReflectValueOf(db.Statement.Context, currentValue)
							values[idx] = fieldValue.Addr().Interface()
						}

						if err := rows.Scan(values...); err != nil {
							db.AddError(fmt.Errorf("%w: %w", ErrCreateReadBack, err))
						}
						reflectIndex++
						break
					}
				}
			case reflect.Struct:
				for idx, field := range fields {
					values[idx] = field.ReflectValueOf(db.Statement.Context, reflectValue).Addr().Interface()
				}

				if rows.Next() {
					if err := rows.Scan(values...); err != nil {
						db.AddError(fmt.Errorf("%w: %w", ErrCreateReadBack, err))
					}
				}
			}
		}
	}
}

func MergeCreate(db *gorm.DB, onConflict clause.OnConflict, values clause.Values) {
	// Transform any column references in DoUpdates to EXCLUDED.column format upfront
	// This prevents GORM from incorrectly quoting "excluded" as a table reference
	onConflict = prepareOnConflictForMerge(db, onConflict)

	valueCount := len(values.Values)
	columnCount := len(values.Columns)
	primaryFieldCount := len(db.Statement.Schema.PrimaryFields)
	useUnionSelect := shouldUseUnionSelect(db)

	// Pre-allocate statement capacity for better performance
	estimatedSize := 100 + len(db.Statement.Table)*2 +
		(valueCount * columnCount * 3) + // VALUES content
		(columnCount * 25) + // column names
		(primaryFieldCount * 50) // WHERE conditions
	db.Statement.SQL.Grow(estimatedSize)

	db.Statement.WriteString("MERGE INTO ")
	db.Statement.WriteQuoted(db.Statement.Table)
	db.Statement.WriteString(" USING (")

	if useUnionSelect {
		// Use SELECT ... UNION ALL SELECT syntax to support SQL functions
		for idx, value := range values.Values {
			if idx > 0 {
				db.Statement.WriteString(" UNION ALL SELECT ")
			} else {
				db.Statement.WriteString("SELECT ")
			}

			valueLen := len(value)
			for i := 0; i < valueLen; i++ {
				if i > 0 {
					db.Statement.WriteByte(',')
				}
				db.Statement.AddVar(db.Statement, value[i])
			}
		}
	} else {
		// Use VALUES syntax (faster but doesn't support SQL functions)
		db.Statement.WriteString("VALUES")
		for idx, value := range values.Values {
			if idx > 0 {
				db.Statement.WriteByte(',')
			}

			db.Statement.WriteByte('(')
			db.Statement.AddVar(db.Statement, value...)
			db.Statement.WriteByte(')')
		}
	}

	db.Statement.WriteString(") AS EXCLUDED (")
	for idx, column := range values.Columns {
		if idx > 0 {
			db.Statement.WriteByte(',')
		}
		db.Statement.WriteQuoted(column.Name)
	}
	db.Statement.WriteString(") ON ")

	// Build ON clause with proper quoting based on QuoteFields setting
	for i, field := range db.Statement.Schema.PrimaryFields {
		if i > 0 {
			db.Statement.WriteString(" AND ")
		}
		db.Statement.WriteQuoted(db.Statement.Table)
		db.Statement.WriteByte('.')
		db.Statement.WriteQuoted(field.DBName)
		db.Statement.WriteString(" = EXCLUDED.")
		db.Statement.WriteQuoted(field.DBName)
	}

	if len(onConflict.DoUpdates) > 0 {
		db.Statement.WriteString(" WHEN MATCHED")

		// OnConflict.Where maps to Snowflake's optional MERGE update predicate:
		// WHEN MATCHED AND <condition> THEN UPDATE. It is wrapped in parentheses
		// so that a condition containing OR cannot bind loosely against the
		// implicit AND and silently widen the set of updated rows.
		if len(onConflict.Where.Exprs) > 0 {
			db.Statement.WriteString(" AND (")
			onConflict.Where.Build(db.Statement)
			db.Statement.WriteByte(')')
		}

		db.Statement.WriteString(" THEN UPDATE SET ")
		onConflict.DoUpdates.Build(db.Statement)
	}

	db.Statement.WriteString(" WHEN NOT MATCHED THEN INSERT (")

	// Cache auto-increment field check
	autoIncrementField := db.Statement.Schema.PrioritizedPrimaryField
	written := false
	for _, column := range values.Columns {
		if autoIncrementField == nil || !autoIncrementField.AutoIncrement || autoIncrementField.DBName != column.Name {
			if written {
				db.Statement.WriteByte(',')
			}
			written = true
			db.Statement.WriteQuoted(column.Name)
		}
	}

	db.Statement.WriteString(") VALUES (")

	written = false
	for _, column := range values.Columns {
		if autoIncrementField == nil || !autoIncrementField.AutoIncrement || autoIncrementField.DBName != column.Name {
			if written {
				db.Statement.WriteByte(',')
			}
			written = true
			// Write EXCLUDED.<column> - use QuoteTo to handle quoting consistently
			db.Statement.WriteString("EXCLUDED.")
			db.Statement.WriteQuoted(column.Name)
		}
	}

	db.Statement.WriteString(")")
	db.Statement.WriteString(";")
}

// prepareOnConflictForMerge prepares the OnConflict clause for use in MERGE statements
// It converts column references to raw SQL expressions to prevent incorrect quoting
// GORM doesn't support unquoted table-qualified columns, so we use clause.Expr
func prepareOnConflictForMerge(db *gorm.DB, onConflict clause.OnConflict) clause.OnConflict {
	if len(onConflict.DoUpdates) == 0 {
		return onConflict
	}

	// Check if we should quote fields
	shouldQuote := false
	if dialector, ok := db.Dialector.(*Dialector); ok && dialector.Config != nil {
		shouldQuote = dialector.Config.QuoteFields
	}

	// Create a new Set with converted assignments
	transformed := make(clause.Set, len(onConflict.DoUpdates))

	for i, assignment := range onConflict.DoUpdates {
		transformed[i] = assignment

		// Convert clause.Column references to EXCLUDED.column format
		// We use clause.Expr because GORM's QuoteTo is called separately for
		// table and column parts, making it impossible to keep both unquoted
		if col, ok := assignment.Value.(clause.Column); ok {
			colName := col.Name

			// Check if user already provided "excluded.column" (case-insensitive)
			colNameLower := strings.ToLower(colName)
			if strings.HasPrefix(colNameLower, "excluded.") {
				// User provided excluded.column - transform to proper case
				// Extract the column name after "excluded."
				columnPart := colName[len("excluded."):]

				if shouldQuote {
					transformed[i].Value = clause.Expr{
						SQL: fmt.Sprintf(`EXCLUDED."%s"`, columnPart),
					}
				} else {
					transformed[i].Value = clause.Expr{
						SQL: fmt.Sprintf(`EXCLUDED.%s`, columnPart),
					}
				}
				continue
			}

			// Normal case: simple column name, wrap with EXCLUDED prefix
			if shouldQuote {
				transformed[i].Value = clause.Expr{
					SQL: fmt.Sprintf(`EXCLUDED."%s"`, colName),
				}
			} else {
				transformed[i].Value = clause.Expr{
					SQL: fmt.Sprintf(`EXCLUDED.%s`, colName),
				}
			}
		}
	}

	// Return a new OnConflict with the converted DoUpdates
	onConflict.DoUpdates = transformed
	return onConflict
}

// fieldsNeedingReadBack returns the DB-generated fields that at least one row in
// this create left unset, in schema order. A field every row supplied a value for
// needs no read-back, because the caller already knows it.
//
// This is the difference between a read-back that is genuinely required and one
// that only looks required. GORM promotes a field named ID to primary key when no
// primaryKey tag appears anywhere on a model, and assumes a lone integer primary
// key is auto-increment, so schemas routinely advertise DB-generated fields that
// the application populates itself.
func fieldsNeedingReadBack(db *gorm.DB, sch *schema.Schema) []*schema.Field {
	needed := make([]*schema.Field, 0, len(sch.FieldsWithDefaultDBValue))
	for _, field := range sch.FieldsWithDefaultDBValue {
		// An unreadable field cannot be scanned into, so fetching it is pointless.
		if !field.Readable {
			continue
		}
		if anyRowUnset(db, field) {
			needed = append(needed, field)
		}
	}
	return needed
}

// anyRowUnset reports whether any row being created left field at its zero value.
// Shapes it cannot inspect are treated as unset so behaviour stays conservative.
func anyRowUnset(db *gorm.DB, field *schema.Field) bool {
	rv := db.Statement.ReflectValue
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			elem := reflect.Indirect(rv.Index(i))
			if elem.Kind() != reflect.Struct {
				return true
			}
			if _, isZero := field.ValueOf(db.Statement.Context, elem); isZero {
				return true
			}
		}
		return false
	case reflect.Struct:
		_, isZero := field.ValueOf(db.Statement.Context, rv)
		return isZero
	default:
		return true
	}
}

// queryIDCapture prepares a context that will receive the query id of the next
// statement executed on it, and returns a reader for that id. Indirected through a
// variable so tests can supply an id without a live Snowflake connection.
var queryIDCapture = snowflakeQueryIDCapture

// snowflakeQueryIDCapture wires gosnowflake's query id channel into ctx.
//
// The channel must be buffered: gosnowflake sends on it inline while serving the
// statement and closes it afterwards, so there is never a receiver waiting. The
// read is non-blocking because nothing is sent when a request fails before reaching
// Snowflake, and the channel is left open in that case.
func snowflakeQueryIDCapture(ctx context.Context) (context.Context, func() string) {
	ch := make(chan string, 1)
	return gosnowflake.WithQueryIDChan(ctx, ch), func() string {
		select {
		case id := <-ch:
			return id
		default:
			return ""
		}
	}
}

// validQueryID reports whether id is safe to inline as a SQL literal. Snowflake
// query ids are UUIDs, but the check is by character class rather than exact shape
// so that format drift degrades into skipping the read-back instead of splicing
// server-supplied text into a statement.
func validQueryID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}

// writeReadBackQuery builds the CHANGES query that fetches DB-generated values for
// the rows the given statement inserted. queryID is inlined as a literal because
// Snowflake requires a constant expression in the BEFORE(statement=>...) position;
// callers must have checked it with validQueryID first.
func writeReadBackQuery(stmt *gorm.Statement, table string, fields []*schema.Field, queryID string) {
	stmt.SQL.Grow(7 + (len(fields) * 25) + len(table) + len(queryID) + 60)

	stmt.WriteString("SELECT ")
	for idx, field := range fields {
		if idx > 0 {
			stmt.WriteByte(',')
		}
		stmt.WriteQuoted(field.DBName)
	}
	stmt.WriteString(" FROM ")
	stmt.WriteQuoted(table)
	stmt.WriteString(" CHANGES(INFORMATION => APPEND_ONLY) BEFORE(statement=>'")
	stmt.WriteString(queryID)
	stmt.WriteString("');")
}

// fieldDBNames lists column names for diagnostics.
func fieldDBNames(fields []*schema.Field) []string {
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.DBName
	}
	return names
}

// shouldReadBackDefaults reports whether the post-INSERT read-back of DB-generated
// fields should run. Enabled unless the caller opts out via Config.
func shouldReadBackDefaults(db *gorm.DB) bool {
	if d, ok := db.Dialector.(*Dialector); ok && d.Config != nil {
		return !d.Config.DisableCreateReadBack
	}
	return true
}

// shouldUseUnionSelect determines whether to use UNION ALL SELECT or VALUES syntax
func shouldUseUnionSelect(db *gorm.DB) bool {
	// Try to get the config from the dialector
	if d, ok := db.Dialector.(*Dialector); ok && d.Config != nil {
		// Prefer the clearer-named InsertWithUnionSelect when set, falling back to
		// the deprecated UseUnionSelect field for backward compatibility.
		return d.Config.useUnionSelectForInserts()
	}
	// Default to UNION ALL SELECT for backward compatibility
	return true
}

// buildUnionSelectInsert builds INSERT statement using UNION ALL SELECT syntax
// This supports SQL functions in values but is slower than VALUES syntax
func buildUnionSelectInsert(db *gorm.DB, values clause.Values) {
	columnCount := len(values.Columns)
	valueCount := len(values.Values)

	// Pre-allocate variables slice with exact capacity for better performance
	totalVars := valueCount * columnCount
	if cap(db.Statement.Vars) < len(db.Statement.Vars)+totalVars {
		// Grow the vars slice if needed
		newVars := make([]interface{}, len(db.Statement.Vars), len(db.Statement.Vars)+totalVars)
		copy(newVars, db.Statement.Vars)
		db.Statement.Vars = newVars
	}

	// Pre-allocate statement builder capacity for better performance
	estimatedSize := (columnCount * 20) + // column names with quotes
		(valueCount * 15) + // " UNION ALL SELECT " strings
		(valueCount * columnCount * 2) + // placeholders
		50 // base structure
	db.Statement.SQL.Grow(estimatedSize)

	db.Statement.WriteByte('(')
	for idx, column := range values.Columns {
		if idx > 0 {
			db.Statement.WriteByte(',')
		}
		db.Statement.WriteQuoted(column)
	}

	db.Statement.WriteString(") SELECT ")

	// Cache the union string to avoid repeated allocations
	const unionSelect = " UNION ALL SELECT "
	for idx, value := range values.Values {
		if idx > 0 {
			db.Statement.WriteString(unionSelect)
		}

		valueLen := len(value)
		for i := 0; i < valueLen; i++ {
			if i > 0 {
				db.Statement.WriteByte(',')
			}
			db.Statement.AddVar(db.Statement, value[i])
		}
	}

	db.Statement.WriteString(";")
}

// buildValuesInsert builds INSERT statement using traditional VALUES syntax
// This is faster than UNION ALL SELECT but doesn't support SQL functions in values
func buildValuesInsert(db *gorm.DB, values clause.Values) {
	columnCount := len(values.Columns)
	valueCount := len(values.Values)

	// Pre-allocate variables slice with exact capacity for better performance
	totalVars := valueCount * columnCount
	if cap(db.Statement.Vars) < len(db.Statement.Vars)+totalVars {
		// Grow the vars slice if needed
		newVars := make([]interface{}, len(db.Statement.Vars), len(db.Statement.Vars)+totalVars)
		copy(newVars, db.Statement.Vars)
		db.Statement.Vars = newVars
	}

	// Pre-allocate statement builder capacity for better performance
	estimatedSize := (columnCount * 15) + // column names with quotes
		(valueCount * 10) + // "(),(),()," patterns
		(valueCount * columnCount * 2) + // placeholders
		50 // base structure
	db.Statement.SQL.Grow(estimatedSize)

	db.Statement.WriteByte('(')
	for idx, column := range values.Columns {
		if idx > 0 {
			db.Statement.WriteByte(',')
		}
		db.Statement.WriteQuoted(column)
	}
	db.Statement.WriteByte(')')

	db.Statement.WriteString(" VALUES ")

	for idx, value := range values.Values {
		if idx > 0 {
			db.Statement.WriteByte(',')
		}

		db.Statement.WriteByte('(')
		db.Statement.AddVar(db.Statement, value...)
		db.Statement.WriteByte(')')
	}

	db.Statement.WriteString(";")
}
