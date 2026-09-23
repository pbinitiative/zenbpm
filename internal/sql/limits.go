package sql

// MaxQueryParameters is the most bind parameters one SQL statement may carry:
// SQLITE_MAX_VARIABLE_NUMBER of the SQLite bundled with rqlite, which keeps
// SQLite's compile-time default. A statement with more fails to prepare, so a
// query which expands a slice into one parameter per element must bound the
// slice by it.
const MaxQueryParameters = 32766
