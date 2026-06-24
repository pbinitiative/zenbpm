package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowScanSurfacesError(t *testing.T) {
	t.Run("custom error is surfaced, not masked as ErrNoRows", func(t *testing.T) {
		wantErr := errors.New("operation is not supported")
		row := ConstructRow(context.Background(), []string{}, []string{}, nil, wantErr)

		var dest int
		err := row.Scan(&dest)

		assert.ErrorIs(t, err, wantErr)
		assert.NotErrorIs(t, err, ErrNoRows)
	})

	t.Run("ErrNoRows on the not-found path is preserved", func(t *testing.T) {
		row := ConstructRow(context.Background(), []string{}, []string{}, nil, ErrNoRows)

		var dest int
		err := row.Scan(&dest)

		assert.ErrorIs(t, err, ErrNoRows)
	})

	t.Run("nil err with nil values still yields ErrNoRows", func(t *testing.T) {
		row := ConstructRow(context.Background(), []string{}, []string{}, nil, nil)

		var dest int
		err := row.Scan(&dest)

		assert.ErrorIs(t, err, ErrNoRows)
	})
}
