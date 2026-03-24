
### Coding standards

The core developers of this project like to establish some guidelines and standards 
to ensure the code is consistent and easier to maintain.

#### Patterns

##### Options Pattern over Builder Patters

Both patterns solve the problem of having flexibility and variations in constructing an object.

Functional options is a pattern in which you declare an opaque Option type that records information in some internal struct.
You accept a variadic number of these options and act upon the full information recorded by the options on the internal struct.

Use this pattern for optional arguments in constructors and other public APIs that you foresee needing to expand,
especially if you already have three or more arguments on those functions.

Example code: https://github.com/uber-go/guide/blob/master/style.md#functional-options



### SQL Migrations

Migration filenames follow the `golang-migrate` convention:

```text
{version}_{title}.up.sql
{version}_{title}.down.sql
```

The `title` part is only for readability. In this repository, use a zero-padded four-digit numeric `version` in the range `0000` to `9999`.

`sqlc` parses migration files in lexicographic order and ignores `down` migrations when generating code, so zero-padding is required to keep lexicographic and numeric ordering aligned.

SQL schema for `sqlc` is loaded from [`internal/sql/migrations`](./internal/sql/migrations), as configured in [`sqlc.yaml`](./sqlc.yaml). 

Examples:

```text
internal/sql/migrations/0000_init.up.sql
internal/sql/migrations/0001_schema.up.sql
internal/sql/migrations/0002_add_job_priority.up.sql
```

If a rollback is needed, use the matching `.down.sql` filename, for example `0002_add_job_priority.down.sql`.
