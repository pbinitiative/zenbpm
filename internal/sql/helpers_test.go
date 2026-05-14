package sql

import (
	"database/sql"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/stretchr/testify/assert"
)

func TestToNullString(t *testing.T) {
	t.Run("nil pointer yields invalid", func(t *testing.T) {
		got := ToNullString[string](nil)
		assert.False(t, got.Valid)
		assert.Equal(t, "", got.String)
	})
	t.Run("non-nil pointer yields valid", func(t *testing.T) {
		got := ToNullString(ptr.To("hello"))
		assert.True(t, got.Valid)
		assert.Equal(t, "hello", got.String)
	})
	t.Run("empty string is still valid", func(t *testing.T) {
		got := ToNullString(ptr.To(""))
		assert.True(t, got.Valid)
		assert.Equal(t, "", got.String)
	})
}

func TestToNullInt64(t *testing.T) {
	t.Run("nil pointer yields invalid", func(t *testing.T) {
		got := ToNullInt64[int64](nil)
		assert.False(t, got.Valid)
		assert.Equal(t, int64(0), got.Int64)
	})
	t.Run("non-nil pointer yields valid", func(t *testing.T) {
		got := ToNullInt64(ptr.To(int64(42)))
		assert.True(t, got.Valid)
		assert.Equal(t, int64(42), got.Int64)
	})
	t.Run("zero value is still valid when pointer is non-nil", func(t *testing.T) {
		got := ToNullInt64(ptr.To(int64(0)))
		assert.True(t, got.Valid)
		assert.Equal(t, int64(0), got.Int64)
	})
}

func TestFromNullInt64(t *testing.T) {
	t.Run("invalid yields nil", func(t *testing.T) {
		got := FromNullInt64(sql.NullInt64{Valid: false, Int64: 99})
		assert.Nil(t, got)
	})
	t.Run("valid yields pointer to value", func(t *testing.T) {
		got := FromNullInt64(sql.NullInt64{Valid: true, Int64: 7})
		if assert.NotNil(t, got) {
			assert.Equal(t, int64(7), *got)
		}
	})
}

func TestFromNullString(t *testing.T) {
	t.Run("invalid yields nil", func(t *testing.T) {
		got := FromNullString(sql.NullString{Valid: false, String: "ignored"})
		assert.Nil(t, got)
	})
	t.Run("valid yields pointer to value", func(t *testing.T) {
		got := FromNullString(sql.NullString{Valid: true, String: "hi"})
		if assert.NotNil(t, got) {
			assert.Equal(t, "hi", *got)
		}
	})
	t.Run("valid empty string yields pointer to empty string", func(t *testing.T) {
		got := FromNullString(sql.NullString{Valid: true, String: ""})
		if assert.NotNil(t, got) {
			assert.Equal(t, "", *got)
		}
	})
}

func TestRoundTripNullInt64(t *testing.T) {
	in := int64(123)
	out := FromNullInt64(ToNullInt64(&in))
	if assert.NotNil(t, out) {
		assert.Equal(t, in, *out)
	}
	assert.Nil(t, FromNullInt64(ToNullInt64[int64](nil)))
}

func TestRoundTripNullString(t *testing.T) {
	in := "abc"
	out := FromNullString(ToNullString(&in))
	if assert.NotNil(t, out) {
		assert.Equal(t, in, *out)
	}
	assert.Nil(t, FromNullString(ToNullString[string](nil)))
}
