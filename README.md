# gorm-snowflake

Snowflake driver for [gorm](https://gorm.io/)

## Snowflake Features

Notable Snowflake (SF) features that affect decisions in this driver

- Use of quotes in SF enforces case-sensitivity which requires string conditions to match. Right now, we are removing all quotes in the internals to make the driver case-insensitive and only uppercase when working with internal tables (INFORMATION_SCHEMA)
- SF does not support INDEX, it does micro-partitioning automatically in all tables for optimizations. Therefore all Index related functions are nil-returned.
- Transactions in SF do not support SAVEPOINT (https://docs.snowflake.com/en/sql-reference/transactions.html)
- GORM rely on being able to query back inserted rows in every transaction in order to get default values back. There is no easy way to do this ala SQL Server (`OUTPUT INSERTED`) or Postgres (`RETURNING`). Instead, we automatically turn on SF `CHANGE_TRACKING` feature on for all tables. This allows us to run `CHANGES` query on the table after running any DML. However due to non-deterministic nature of return from `MERGE`, it doesn't support updates.
- The read-back only runs for fields a row actually left unset. If the application supplies a value for every DB-generated column, there is nothing to fetch and no `CHANGES` query is issued. This matters because GORM _infers_ DB-generated fields: with no `primaryKey` tag anywhere on a model it promotes a field named `ID` to primary key and assumes a lone integer primary key is auto-increment. Without that check, a model whose `ID` is supplied by the application would still trigger a read-back it never needed.
- The read-back identifies the inserted rows by inlining the insert's query id as a literal: `BEFORE(statement=>'<query id>')`. Snowflake requires a **constant expression** in that position, so the `LAST_QUERY_ID()` this driver used previously was rejected as a function call. The id is captured client-side from the driver rather than through a session variable, which keeps it correct over a pooled `*sql.DB` where the insert and the read-back need not share a connection.
- When a field needs reading back but no usable query id is reported — typically a connection pool that is not the Snowflake driver — the create fails with `ErrCreateReadBack`. The insert is committed either way, so the alternative would be handing back a struct whose generated fields hold a plausible-looking zero, which is much harder to notice. Callers that do not need those values say so explicitly, via `Config.DisableCreateReadBack` or by tagging the model so the fields are not treated as DB-generated.
- `CHANGE_TRACKING` is only set by this driver's own `CreateTable`. Tables created by external migrations do not have it, and a read-back against them fails; the error is recognised and reported with the `ALTER TABLE ... SET CHANGE_TRACKING = TRUE` remedy.

  Failures are wrapped in `ErrCreateReadBack`, which signals that the insert itself succeeded and only the read-back did not.

- The `SELECT...CHANGES` feature of SF does not return unchanged rows from `MERGE` statement, therefore we can only rely on the `APPEND_ONLY` option and only support returning fields from inserted rows in the same order.
- SF does not enforce any constraint other than NOT NULL. This driver expect all tests and features related to enforcing constraint to be disabled. (https://docs.snowflake.com/en/user-guide/table-considerations.html#referential-integrity-constraints)
- This supports the latest SF driver for go.
