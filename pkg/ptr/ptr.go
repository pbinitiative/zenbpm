// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package ptr

// To returns a pointer to the given value.
func To[T any](v T) *T {
	return &v
}

// Deref dereferences ptr and returns the value it points to if no nil, or else
// returns def.
func Deref[T any](ptr *T, def T) T {
	if ptr != nil {
		return *ptr
	}
	return def
}

// ConvertSliceToPointerSlice converts a slice to a slice of pointers.
func ConvertSliceToPointerSlice[T any](input []T) []*T {
	result := make([]*T, len(input))
	for i := range input {
		result[i] = &input[i]
	}
	return result
}
