package es

import (
	"testing"
	"time"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/uuid"
)

// HydrateState is a test helper meant to make it easy to hydrate a state using event data.
func HydrateState[T Handler](t *testing.T, state T, contents ...Content) T {
	t.Helper()
	if len(contents) == 0 {
		return state
	}

	var (
		streamType    = uuid.V7()
		streamID      = uuid.V7()
		eventTime     = time.Now()
		storeStreamID = uuid.V7AtTime(eventTime)
		storeEventIDs = uuid.V7At(eventTime, len(contents))
	)

	for i, content := range contents {
		err := state.Handle(t.Context(), Event{
			StreamID:      streamID,
			StreamType:    streamType,
			EventNumber:   int64(i + 1),
			EventTime:     time.Now(),
			Content:       content,
			StoreEventID:  storeEventIDs[i],
			StoreStreamID: storeStreamID,
		})
		assert.NoError(t, err)
	}

	return state
}
