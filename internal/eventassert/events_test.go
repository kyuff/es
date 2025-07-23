package eventassert_test

import (
	"testing"

	"github.com/kyuff/es"
	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/eventassert"
)

func TestEqualEvent(t *testing.T) {
	t.Run("EqualEvent succeed", func(t *testing.T) {
		// arrange
		var (
			x        = &testing.T{}
			expected = es.Event{
				EventNumber: 42,
			}
			actual = es.Event{
				EventNumber: 42,
			}
		)
		// act
		got := eventassert.EqualEvent(x, expected, actual)

		// assert
		assert.Truef(t, got, "got")
		assert.Truef(t, !x.Failed(), "status")
	})

	t.Run("EqualEvent failed", func(t *testing.T) {
		// arrange
		var (
			x        = &testing.T{}
			expected = es.Event{
				StreamID:    "a stream id",
				EventNumber: 42,
			}
			actual = es.Event{
				StreamID:    "different id",
				EventNumber: 42,
			}
		)
		// act
		got := eventassert.EqualEvent(x, expected, actual)

		// assert
		assert.Truef(t, !got, "got")
		assert.Truef(t, x.Failed(), "status")
	})
}
