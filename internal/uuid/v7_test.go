package uuid_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/uuid"
)

func TestV7At(t *testing.T) {
	t.Run("return a sorted list of uuid v7", func(t *testing.T) {
		// arrange
		var (
			now   = time.Now()
			count = 10000
		)

		// act
		ids := uuid.V7At(now, count)

		// assert
		assert.Equal(t, count, len(ids))
		assert.Equal(t, true, slices.IsSorted(ids))
	})

	t.Run("return an uuid for specific time", func(t *testing.T) {
		// arrange
		var (
			then = time.Date(1977, time.October, 15, 16, 0, 0, 0, time.UTC)
		)

		// ct
		got := uuid.V7AtTime(then)

		// assert
		assert.Truef(t, strings.HasPrefix(got, "00393994-f800-7"), "got: %s", got)
	})
}
