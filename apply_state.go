package es

import (
	"testing"
	"time"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/uuid"
)

// ApplyState is a test helper meant to make it easy to hydrate a state using event data.
func ApplyState[T Handler](t *testing.T, state T, contents ...Content) T {
	t.Helper()
	if len(contents) == 0 {
		return state
	}

	var (
		entityType    = uuid.V7()
		entityID      = uuid.V7()
		eventTime     = time.Now()
		storeEntityID = uuid.V7AtTime(eventTime)
		storeEventIDs = uuid.V7At(eventTime, len(contents))
	)

	for i, content := range contents {
		err := state.Handle(t.Context(), Event{
			EntityID:      entityID,
			EntityType:    entityType,
			EventNumber:   int64(i + 1),
			EventTime:     time.Now(),
			Content:       content,
			StoreEventID:  storeEventIDs[i],
			StoreEntityID: storeEntityID,
		})
		assert.NoError(t, err)
	}

	return state
}
