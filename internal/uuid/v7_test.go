package uuid_test

import (
	"slices"
	"testing"
	"time"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/uuid"
)

func TestV7At(t *testing.T) {
	t.Run("should return a sorted list of uuid v7", func(t *testing.T) {
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
}
