// Package storage contains types and interfaces, so that different persistence layers can be implemented.
//
// Interfaces in this package must:
//   - return ErrNotFound if the method is looking for one exact item in the database and it is not found
//   - return empty array for methods that can return multiple results and no result is found
package storage
