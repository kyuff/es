package es_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/kyuff/es"
	"github.com/kyuff/es/internal/assert"
)

func TestApplyState(t *testing.T) {
	t.Run("do not call Handle with no content", func(t *testing.T) {
		var (
			state = &HandlerMock{}
		)

		// act
		got := es.HydrateState(t, state)

		// assert
		assert.Equal(t, state, got)
		assert.Equal(t, 0, len(state.HandleCalls()))
	})

	t.Run("fail with state.Handle", func(t *testing.T) {
		var (
			tt    = &testing.T{}
			state = &HandlerMock{}
		)

		state.HandleFunc = func(ctx context.Context, event es.Event) error {
			return errors.New("error")
		}

		// act
		got := es.HydrateState(tt, state, EventMock{})

		// assert
		assert.Equal(t, state, got)
		assert.Equal(t, 1, len(state.HandleCalls()))
		assert.Truef(t, tt.Failed(), "test should fail")
	})

	t.Run("apply all events applied", func(t *testing.T) {
		var (
			state    = &HandlerMock{}
			expected = []es.Content{
				EventMock{ID: rand.Int()},
				EventMock{ID: rand.Int()},
				EventMock{ID: rand.Int()},
			}
			events []es.Content
		)

		state.HandleFunc = func(ctx context.Context, event es.Event) error {
			events = append(events, event.Content)
			return nil
		}

		// act
		got := es.HydrateState(t, state, expected...)

		// assert
		assert.Equal(t, state, got)
		assert.Equal(t, len(expected), len(state.HandleCalls()))
		assert.EqualSlice(t, expected, events)
	})
}
