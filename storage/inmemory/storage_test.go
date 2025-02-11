package inmemory_test

import (
	"context"
	"fmt"
	"iter"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/kyuff/es"
	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/eventassert"
	"github.com/kyuff/es/internal/seqs"
	"github.com/kyuff/es/storage/inmemory"
)

type MockEvent struct {
	ID int64
}

func (e MockEvent) Name() string {
	return "MockEvent"
}

func TestStorage(t *testing.T) {
	var (
		ctx      = context.Background()
		newEvent = func(eventNumber int64, mods ...func(e *es.Event)) es.Event {
			e := es.Event{
				EntityID:     fmt.Sprintf("EntityID-%d", eventNumber),
				EntityType:   fmt.Sprintf("EntityType-%d", eventNumber),
				EventNumber:  eventNumber,
				StoreEventID: fmt.Sprintf("StoreEventID-%d", eventNumber),
				EventTime:    time.Now().Add(time.Second * time.Duration(eventNumber)).Truncate(time.Second),
				Content:      MockEvent{ID: eventNumber},
			}
			for _, mod := range mods {
				mod(&e)
			}

			return e
		}
		newEntityType = func() string {
			return fmt.Sprintf("EntityType-%d", rand.Int63())
		}
		newEvents = func(entityType string, count int) []es.Event {
			var entityID = fmt.Sprintf("EntityID-%d-%d", count, rand.Int63())
			var events []es.Event
			for i := 1; i <= count; i++ {
				events = append(events, newEvent(int64(i), func(e *es.Event) {
					e.EntityType = entityType
					e.EntityID = entityID
				}))
			}

			return events
		}
		collectEvents = func(t *testing.T, i iter.Seq2[es.Event, error]) []es.Event {
			var events []es.Event
			for event, err := range i {
				assert.NoError(t, err)
				events = append(events, event)
			}

			return events
		}
	)
	t.Run("should write events", func(t *testing.T) {
		// arrange
		var (
			entityType = newEntityType()
			events     = seqs.Seq2(newEvents(entityType, 5)...)
			writer     = &WriterMock{}
			sut        = inmemory.New()
		)

		assert.NoError(t, sut.StartPublish(writer))

		writer.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		// act
		err := sut.Write(ctx, entityType, events)

		// assert
		assert.NoError(t, err)
	})

	t.Run("should read events", func(t *testing.T) {
		// arrange
		var (
			entityType = newEntityType()
			events     = newEvents(entityType, 5)
			sut        = inmemory.New()
		)

		assert.NoError(t, sut.Register(events[0].EntityType, MockEvent{}))
		assert.NoError(t, sut.Write(ctx, entityType, seqs.Seq2(events...)))

		// act
		i := sut.Read(ctx, events[0].EntityType, events[0].EntityID, 0)

		// assert
		got := collectEvents(t, i)
		assert.EqualSlice(t, events, got)
	})

	t.Run("should publish events", func(t *testing.T) {
		// arrange
		var (
			entityType = newEntityType()
			events     = newEvents(entityType, 5)
			expected   = seqs.Seq2(events...)
			writer     = &WriterMock{}
			sut        = inmemory.New()
			got        = seqs.EmptySeq2[es.Event, error]()
			wg         sync.WaitGroup
		)

		wg.Add(len(events))
		assert.NoError(t, sut.Register(events[0].EntityType, MockEvent{}))
		t.Cleanup(func() {
			_ = sut.Close()
		})
		writer.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
			got = seqs.Concat2(got, events)
			wg.Done()
			return nil
		}

		assert.NoError(t, sut.Write(ctx, entityType, expected))

		// act
		err := sut.StartPublish(writer)

		// assert
		wg.Wait()
		assert.NoError(t, err)
		assert.EqualSeq2(t, expected, got, func(expected, got assert.KeyValue[es.Event, error]) bool {
			return eventassert.EqualEvent(t, expected.Key, got.Key)
		})
	})

	t.Run("should not write same event number", func(t *testing.T) {
		// arrange
		var (
			entityType = "entityType"
			entityID   = "entityID"
			eventA     = newEvent(1, func(e *es.Event) {
				e.EntityType = entityType
				e.EntityID = entityID
			})
			eventB = newEvent(1, func(e *es.Event) {
				e.EntityType = entityType
				e.EntityID = entityID
			})
			writer = &WriterMock{}
			sut    = inmemory.New()
		)

		writer.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, entityType, seqs.Seq2(eventA)))

		assert.NoError(t, sut.StartPublish(writer))

		// act
		err := sut.Write(ctx, entityType, seqs.Seq2(eventB))

		// assert
		assert.Error(t, err)
	})

	t.Run("should not write same events out of order", func(t *testing.T) {
		// arrange
		var (
			entityType = "entityType"
			entityID   = "entityID"
			eventA     = newEvent(1, func(e *es.Event) {
				e.EntityType = entityType
				e.EntityID = entityID
			})
			eventB = newEvent(3, func(e *es.Event) {
				e.EntityType = entityType
				e.EntityID = entityID
			})
			writer = &WriterMock{}
			sut    = inmemory.New()
		)

		writer.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, entityType, seqs.Seq2(eventA)))

		// act
		err := sut.Write(ctx, entityType, seqs.Seq2(eventB))

		// assert
		assert.Error(t, err)
	})
}
