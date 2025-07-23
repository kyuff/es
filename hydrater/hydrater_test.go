package hydrater_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/kyuff/es"
	"github.com/kyuff/es/hydrater"
	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/uuid"
)

type mockState struct {
	value string
}

func (mockState) Handle(ctx context.Context, event es.Event) error {
	return nil
}

func TestGetter(t *testing.T) {
	var (
		testType   = "testStream"
		newID      = uuid.V7
		maxChecker = func(maxChecks int) func(state *mockState) bool {
			checks := 0
			return func(state *mockState) bool {
				checks++
				if maxChecks > 0 && checks >= maxChecks {
					return true
				}
				return false
			}
		}
	)

	t.Run("should return value first attempt", func(t *testing.T) {
		// arrange
		var (
			ctx       = context.Background()
			projector = &ProjectorMock{}
			sut       = hydrater.New[*mockState](projector, testType)
			id        = newID()
			expected  = newID()
		)

		projector.ProjectFunc = func(ctx context.Context, streamType, streamID string, handler es.Handler) error {
			assert.Equalf(t, testType, streamType, "stream type should match")
			assert.Equalf(t, id, streamID, "stream ID should match")
			state, isType := handler.(*mockState)
			if assert.Truef(t, isType, "handler should match") {
				state.value = expected
			}

			return nil
		}

		// act
		state, err := sut.Hydrate(ctx, id)

		// assert
		assert.NoError(t, err)
		if assert.NotNil(t, state) {
			assert.Equal(t, expected, state.value)
		}
		assert.Equal(t, 1, len(projector.ProjectCalls()))
	})

	t.Run("should fail with projector", func(t *testing.T) {
		// arrange
		var (
			maxChecks = 100

			ctx       = context.Background()
			projector = &ProjectorMock{}

			sut = hydrater.New[*mockState](projector, testType,
				hydrater.WithFixedBackoff[*mockState](time.Millisecond*10),
				hydrater.WithChecker(maxChecker(maxChecks)),
			)

			id = newID()
		)

		projector.ProjectFunc = func(ctx context.Context, streamType, streamID string, handler es.Handler) error {
			return errors.New("FAIL")
		}

		// act
		_, err := sut.Hydrate(ctx, id)

		// assert
		assert.Error(t, err)
		assert.Equal(t, 1, len(projector.ProjectCalls()))
	})

	t.Run("should fail with maker", func(t *testing.T) {
		// arrange
		var (
			maxChecks = 100

			ctx       = context.Background()
			projector = &ProjectorMock{}

			sut = hydrater.New[*mockState](projector, testType,
				hydrater.WithFixedBackoff[*mockState](time.Millisecond*10),
				hydrater.WithChecker(maxChecker(maxChecks)),
				hydrater.WithMaker[*mockState](func(ctx context.Context, id string) (*mockState, error) {
					return nil, errors.New("FAIL")
				}),
			)

			id = newID()
		)

		projector.ProjectFunc = func(ctx context.Context, streamType, streamID string, handler es.Handler) error {
			return nil
		}

		// act
		_, err := sut.Hydrate(ctx, id)

		// assert
		assert.Error(t, err)
		assert.Equal(t, 0, len(projector.ProjectCalls()))
	})

	t.Run("should check state with fixed backoff", func(t *testing.T) {
		// arrange
		var (
			maxChecks = rand.IntN(3) + 2

			ctx       = context.Background()
			projector = &ProjectorMock{}

			sut = hydrater.New[*mockState](projector, testType,
				hydrater.WithFixedBackoff[*mockState](time.Millisecond*10),
				hydrater.WithChecker(maxChecker(maxChecks)),
			)

			id = newID()
		)

		projector.ProjectFunc = func(ctx context.Context, streamType, streamID string, handler es.Handler) error {
			return nil
		}

		// act
		_, err := sut.Hydrate(ctx, id)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, maxChecks, len(projector.ProjectCalls()))
	})

	t.Run("should check state with linear backoff", func(t *testing.T) {
		// arrange
		var (
			maxChecks = rand.IntN(3) + 2

			ctx       = context.Background()
			projector = &ProjectorMock{}

			sut = hydrater.New[*mockState](projector, testType,
				hydrater.WithLinearBackoff[*mockState](time.Millisecond*10),
				hydrater.WithChecker(maxChecker(maxChecks)),
			)

			id = newID()
		)

		projector.ProjectFunc = func(ctx context.Context, streamType, streamID string, handler es.Handler) error {
			return nil
		}

		// act
		_, err := sut.Hydrate(ctx, id)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, maxChecks, len(projector.ProjectCalls()))
	})

	t.Run("should check state with linear backoff", func(t *testing.T) {
		// arrange
		var (
			maxChecks = rand.IntN(3) + 2

			ctx       = context.Background()
			projector = &ProjectorMock{}

			sut = hydrater.New[*mockState](projector, testType,
				hydrater.WithExponentialBackoff[*mockState](time.Millisecond*10),
				hydrater.WithChecker(maxChecker(maxChecks)),
			)

			id = newID()
		)

		projector.ProjectFunc = func(ctx context.Context, streamType, streamID string, handler es.Handler) error {
			return nil
		}

		// act
		_, err := sut.Hydrate(ctx, id)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, maxChecks, len(projector.ProjectCalls()))
	})

	t.Run("should retry until cancelled", func(t *testing.T) {
		// arrange
		var (
			maxRetry    = 5
			ctx, cancel = context.WithCancel(context.Background())
			projector   = &ProjectorMock{}

			sut = hydrater.New[*mockState](projector, testType,
				hydrater.WithBackoff(func(t *mockState, retries int) time.Duration {
					if retries == maxRetry {
						cancel()
					}
					return time.Millisecond * 1
				}),
				hydrater.WithChecker(func(state *mockState) bool { return false }),
			)

			id = newID()
		)

		projector.ProjectFunc = func(ctx context.Context, streamType, streamID string, handler es.Handler) error {
			return nil
		}

		// act
		_, err := sut.Hydrate(ctx, id)

		// assert
		if assert.Error(t, err) {
			assert.Equal(t, err, context.Canceled)
		}
		assert.Equal(t, maxRetry, len(projector.ProjectCalls()))

		t.Cleanup(cancel)
	})
}
