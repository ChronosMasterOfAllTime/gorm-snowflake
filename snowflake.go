package snowflake

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/snowflakedb/gosnowflake"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

const (
	SnowflakeDriverName = "snowflake"
)

var (
	// Pre-compiled regex patterns for better performance
	functionRegex = regexp.MustCompile(`([a-zA-Z0-9|_]+)\((.+?)\)`)
)

type Dialector struct {
	*Config
}

type Config struct {
	QuoteFields bool
	DriverName  string
	DSN         string
	Conn        gorm.ConnPool
	// Connection pooling configuration for better performance
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime int // in seconds
	// DisableCreateReadBack turns off the post-INSERT read-back that populates
	// fields Snowflake generates (auto-increment keys, `default:` tags). That
	// read-back issues a CHANGES(...) query against the table, which requires
	// CHANGE_TRACKING to be enabled on it. Tables created outside this driver's
	// migrator do not have it, and the read-back fails there. The driver already
	// skips the read-back when no field actually needs one, so this is an escape
	// hatch rather than the usual remedy.
	//
	// Set it when the generated values are genuinely not needed, including when
	// running against a connection pool that is not the Snowflake driver and so
	// cannot report a query id. Without it, a create that cannot populate a
	// DB-generated field fails with ErrCreateReadBack rather than leaving a zero
	// behind: the insert is committed either way, and a silent zero is far harder
	// to notice than an error.
	// Default: false (maintains backward compatibility)
	DisableCreateReadBack bool
	// UseUnionSelect enables UNION ALL SELECT syntax for INSERT statements
	// Required for using SQL functions in values, but slower than VALUES syntax
	// Default: true (maintains backward compatibility)
	//
	// Deprecated: use InsertWithUnionSelect instead, whose name better conveys its
	// purpose. UseUnionSelect is kept for backward compatibility and continues to
	// work; if both fields are set, InsertWithUnionSelect takes precedence.
	UseUnionSelect bool
	// InsertWithUnionSelect enables UNION ALL SELECT syntax (instead of VALUES) for
	// INSERT statements. Required when using SQL functions in insert values, but
	// slower than the VALUES syntax. This is a clearer-named alias for
	// UseUnionSelect. It is a pointer so "unset" can be distinguished from
	// "explicitly false"; when nil, the value of UseUnionSelect is used instead.
	// Default: true (maintains backward compatibility)
	InsertWithUnionSelect *bool
}

// useUnionSelectForInserts resolves the effective UNION ALL SELECT setting,
// preferring InsertWithUnionSelect when explicitly set and falling back to the
// deprecated UseUnionSelect field otherwise.
func (c *Config) useUnionSelectForInserts() bool {
	if c.InsertWithUnionSelect != nil {
		return *c.InsertWithUnionSelect
	}
	return c.UseUnionSelect
}

func (dialector Dialector) Name() string {
	return SnowflakeDriverName
}

func Open(dsn string) *Dialector {
	return &Dialector{
		Config: &Config{
			DSN:            dsn,
			DriverName:     SnowflakeDriverName,
			UseUnionSelect: true, // Default to UNION ALL SELECT for backward compatibility
		},
	}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})
	_ = db.Callback().Create().Replace("gorm:create", Create)

	if dialector.DriverName == "" {
		dialector.DriverName = SnowflakeDriverName
	}

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.DSN)
		if err != nil {
			return err
		}
	}

	for k, v := range dialector.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func (dialector Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"LIMIT": func(c clause.Clause, builder clause.Builder) {
			if limit, ok := c.Expression.(clause.Limit); ok {
				if stmt, ok := builder.(*gorm.Statement); ok {
					if _, ok := stmt.Clauses["ORDER BY"]; !ok {
						if stmt.Schema != nil && stmt.Schema.PrioritizedPrimaryField != nil {
							builder.WriteString("ORDER BY ")
							builder.WriteQuoted(stmt.Schema.PrioritizedPrimaryField.DBName)
							builder.WriteByte(' ')
						} else {
							builder.WriteString("ORDER BY (SELECT NULL) ")
						}
					}
				}

				if limit.Offset > 0 {
					builder.WriteString("OFFSET ")
					builder.WriteString(strconv.Itoa(limit.Offset))
					builder.WriteString(" ROWS")
				}

				if limit.Limit != nil && *limit.Limit >= 0 {
					if limit.Offset == 0 {
						builder.WriteString("OFFSET 0 ROW")
					}
					builder.WriteString(" FETCH NEXT ")
					builder.WriteString(strconv.Itoa(*limit.Limit))
					builder.WriteString(" ROWS ONLY")
				}
			}
		},
	}
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "NULL"}
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:        db,
		Dialector: dialector,
	}}}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	if dialector.QuoteFields {
		quoteString := str
		isFunction := functionRegex.MatchString(str)

		if isFunction {
			matches := functionRegex.FindStringSubmatch(str)
			writer.WriteString(matches[1])
			writer.WriteByte('(')
			quoteString = matches[2]
		}

		writer.WriteByte('"')
		if strings.Contains(quoteString, ".") {
			parts := strings.Split(quoteString, ".")
			for idx, splitStr := range parts {
				if idx > 0 {
					writer.WriteString(`."`)
				}
				writer.WriteString(splitStr)
				writer.WriteByte('"')
			}
		} else {
			writer.WriteString(quoteString)
			writer.WriteByte('"')
		}

		if isFunction {
			writer.WriteByte(')')
		}
	} else {
		writer.WriteString(strings.ToLower(str))
	}
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "BOOLEAN"
	case schema.Int, schema.Uint:
		var sqlType string
		switch {
		case field.Size < 16:
			sqlType = "SMALLINT"
		case field.Size < 31:
			sqlType = "INT"
		default:
			sqlType = "BIGINT"
		}

		if field.AutoIncrement {
			return sqlType + " IDENTITY(1,1)"
		}
		return sqlType
	case schema.Float:
		return "FLOAT"
	case schema.String:
		size := field.Size

		hasIndex := field.TagSettings["INDEX"] != "" || field.TagSettings["UNIQUE"] != ""
		if (field.PrimaryKey || hasIndex) && size == 0 {
			size = 256
		}
		if size > 0 && size <= 4000 {
			return fmt.Sprintf("VARCHAR(%d)", size)
		}
		return "VARCHAR"
	case schema.Time:
		return "TIMESTAMP_NTZ"
	case schema.Bytes:
		return "VARBINARY"
	}

	return string(field.DataType)
}

// no support for savepoint
func (dialectopr Dialector) SavePoint(tx *gorm.DB, name string) error {
	return nil
}

func (dialectopr Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TRANSACTION " + name)
	return nil
}

// NamingStrategy for snowflake (always uppercase)
type NamingStrategy struct {
	defaultNS *schema.NamingStrategy
}

// NewNamingStrategy create new instance of snowflake naming strat
func NewNamingStrategy() *NamingStrategy {
	return &NamingStrategy{
		defaultNS: &schema.NamingStrategy{},
	}
}

// ColumnName snowflake edition
func (sns NamingStrategy) ColumnName(table, column string) string {
	return sns.defaultNS.ColumnName(table, column)
}

// TableName snowflake edition
func (sns NamingStrategy) TableName(table string) string {
	return sns.defaultNS.TableName(table)
}

// JoinTableName snowflake edition
func (sns NamingStrategy) JoinTableName(joinTable string) string {
	return sns.defaultNS.JoinTableName(joinTable)
}

// RelationshipFKName snowflake edition
func (sns NamingStrategy) RelationshipFKName(rel schema.Relationship) string {
	return sns.defaultNS.RelationshipFKName(rel)
}

// CheckerName snowflake edition
func (sns NamingStrategy) CheckerName(table, column string) string {
	return sns.defaultNS.CheckerName(table, column)
}

// IndexName snowflake edition
func (sns NamingStrategy) IndexName(table, column string) string {
	return sns.defaultNS.IndexName(table, column)
}

// isChangeTrackingError reports whether err is Snowflake refusing a CHANGES query
// because change tracking is unavailable on the table. Matched on message text: the
// server's numeric codes are not enumerated by gosnowflake, so there is no constant
// to compare against. Used only to choose a diagnostic hint.
func isChangeTrackingError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "change tracking") || strings.Contains(msg, "change_tracking")
}

// Translate implements the ErrorTranslator interface to convert Snowflake-specific
// errors into standard GORM errors. This allows GORM's error handling to work
// consistently across different database dialects.
func (dialector Dialector) Translate(err error) error {
	if err == nil {
		return nil
	}

	// Try to extract a SnowflakeError from the error chain
	var sfErr *gosnowflake.SnowflakeError
	if errors.As(err, &sfErr) {
		// Note: Snowflake does not enforce most constraints (only NOT NULL)
		// as documented in https://docs.snowflake.com/en/user-guide/table-considerations.html
		// However, we still translate common error patterns when they occur

		// Check for duplicate key violations
		// Snowflake error code for duplicate key is typically indicated in the message
		// since Snowflake doesn't strictly enforce UNIQUE constraints
		if strings.Contains(strings.ToLower(sfErr.Message), "duplicate") ||
			strings.Contains(strings.ToLower(sfErr.Message), "unique") {
			return gorm.ErrDuplicatedKey
		}

		// Check for foreign key violations
		// While Snowflake doesn't enforce FK constraints by default,
		// if they are defined and validated, errors may mention foreign key
		if strings.Contains(strings.ToLower(sfErr.Message), "foreign key") {
			return gorm.ErrForeignKeyViolated
		}

		// Check for check constraint violations
		if strings.Contains(strings.ToLower(sfErr.Message), "check constraint") {
			return gorm.ErrCheckConstraintViolated
		}

		// Check for invalid data/value errors
		if strings.Contains(strings.ToLower(sfErr.Message), "invalid") &&
			(strings.Contains(strings.ToLower(sfErr.Message), "value") ||
				strings.Contains(strings.ToLower(sfErr.Message), "data")) {
			return gorm.ErrInvalidData
		}
	}

	// Return the original error if no translation is needed
	return err
}
