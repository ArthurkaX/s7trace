# Schema Migrations

## Contract

The live database stores its schema version in `schema_meta`:

```sql
select value from schema_meta where key = 'schema_version';
```

Version `1` is the initial schema in `docs/schema.sql`.

## Layout

Future implementation should keep migrations as ordered files:

```text
migrations/
  0001_initial.sql
  0002_add_column.sql
```

`docs/schema.sql` remains the current full schema for fresh databases.

## Startup Rule

On startup, the service should:

1. open SQLite with `foreign_keys = ON`
2. read `schema_meta.schema_version`
3. refuse to run if the database is newer than the binary supports
4. apply pending migrations if automatic migration is enabled
5. otherwise print the required migration command and exit

## Migration Safety

Migrations should be additive where possible:

- add tables
- add nullable columns
- backfill in bounded batches
- only then make behavior depend on the new data

Avoid large blocking rewrites while polling is active. For production systems,
prefer an explicit maintenance command:

```text
s7trace migrate --to latest
```

## Validation

At minimum, CI should run:

```text
sqlite3 :memory: ".read docs/schema.sql"
```

The Rust implementation should also verify that Rust enums match SQL CHECK
constraints.
