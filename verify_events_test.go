package es_test

import (
	"testing"

	"github.com/kyuff/es"
	"github.com/kyuff/es/internal/assert"
)

func TestVerifyEvents(t *testing.T) {
	t.Run("fail when len differs", func(t *testing.T) {
		// arrange
		var (
			tt     = &testing.T{}
			events = []es.Content{
				EventMock{ID: 2},
				EventMock{ID: 4},
			}
		)
		// act
		valid := es.VerifyEvents(tt, events,
			EventMock{ID: 2},
			EventMock{ID: 4},
			EventMock{ID: 3},
		)

		// assert
		assert.Truef(t, !valid, "response")
		assert.Truef(t, tt.Failed(), "len mismatch")
	})

	t.Run("fail when item has different content", func(t *testing.T) {
		// arrange
		var (
			tt     = &testing.T{}
			events = []es.Content{
				EventMock{ID: 2},
				EventMock{ID: 4},
			}
		)
		// act
		valid := es.VerifyEvents(tt, events,
			EventMock{ID: 2},
			EventMock{ID: 3},
		)

		// assert
		assert.Truef(t, !valid, "response")
		assert.Truef(t, tt.Failed(), "content differs")
	})

	t.Run("fail when matcher declines", func(t *testing.T) {
		// arrange
		var (
			tt     = &testing.T{}
			events = []es.Content{
				EventMock{ID: 2},
				EventMock{ID: 4},
			}
		)
		// act
		valid := es.VerifyEvents(tt, events,
			EventMock{ID: 2},
			func(got any) bool {
				return false
			},
		)

		// assert
		assert.Truef(t, !valid, "response")
		assert.Truef(t, tt.Failed(), "content differs")
	})

	t.Run("verify all events", func(t *testing.T) {
		// arrange
		var (
			tt     = &testing.T{}
			events = []es.Content{
				EventMock{ID: 2},
				EventMock{ID: 4},
				UpgradedEventMock{Number: 3},
			}
		)
		// act
		valid := es.VerifyEvents(tt, events,
			EventMock{ID: 2},
			func(got any) bool {
				return true
			},
			UpgradedEventMock{Number: 3},
		)

		// assert
		assert.Truef(t, valid, "response")
		assert.Truef(t, !tt.Failed(), "should verify")
	})
}
