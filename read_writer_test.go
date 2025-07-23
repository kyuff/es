package es

import (
	"context"
	"iter"
	"testing"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/seqs"
)

func TestReadWriter(t *testing.T) {
	t.Run("readerFunc implements Reader", func(t *testing.T) {
		var (
			sut Reader = readerFunc(func(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[Event, error] {
				return seqs.Seq2(
					Event{
						StreamType:  entityType,
						StreamID:    entityID,
						EventNumber: eventNumber,
					},
				)
			})
		)

		// act
		got := sut.Read(t.Context(), "entityType", "entityID", 42)

		// assert
		for event, err := range got {
			assert.NoError(t, err)
			assert.Equal(t, "entityType", event.StreamType)
			assert.Equal(t, "entityID", event.StreamID)
			assert.Equal(t, 42, event.EventNumber)
		}
	})

	t.Run("writerFunc implements Writer", func(t *testing.T) {
		var (
			got []Event
			sut Writer = writerFunc(func(ctx context.Context, entityType string, events iter.Seq2[Event, error]) error {
				for event, _ := range events {
					got = append(got, event)
					return nil
				}
				return nil
			})
			event = Event{
				StreamType:  "entityType",
				StreamID:    "entityID",
				EventNumber: 42,
			}
		)

		// act
		err := sut.Write(t.Context(), "entityType", seqs.Seq2(event))

		// assert
		assert.NoError(t, err)
		assert.EqualSlice(t, []Event{event}, got)
	})
}
