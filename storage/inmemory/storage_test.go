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
	"github.com/kyuff/es/internal/uuid"
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
			var storeEntityIDs = uuid.V7At(time.Now(), count)
			for i := 1; i <= count; i++ {
				events = append(events, newEvent(int64(i), func(e *es.Event) {
					e.EntityType = entityType
					e.EntityID = entityID
					e.StoreEntityID = storeEntityIDs[i-1]
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
	t.Run("write events with no error", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			entityType  = newEntityType()
			events      = seqs.Seq2(newEvents(entityType, 5)...)
			writer      = &WriterMock{}
			sut         = inmemory.New()
		)

		go func() {
			assert.NoError(t, sut.StartPublish(ctx, writer))
		}()

		writer.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		// act
		err := sut.Write(ctx, entityType, events)

		// assert
		assert.NoError(t, err)
		cancel()
	})

	t.Run("read events written", func(t *testing.T) {
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
		assert.EqualSliceFunc(t, events, got, func(want, item es.Event) bool {
			return assert.Equal(t, want.Content, item.Content) &&
				assert.Equal(t, want.EventNumber, item.EventNumber) &&
				assert.Equal(t, want.EntityType, item.EntityType) &&
				assert.Equal(t, want.EntityID, item.EntityID) &&
				assert.Equal(t, want.StoreEventID, item.StoreEventID) &&
				assert.EqualTime(t, want.EventTime, item.EventTime)
		})
	})

	t.Run("publish events after write", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			entityType  = newEntityType()
			events      = newEvents(entityType, 5)
			expected    = seqs.Seq2(events...)
			writer      = &WriterMock{}
			sut         = inmemory.New()
			got         = seqs.EmptySeq2[es.Event, error]()
			wg          sync.WaitGroup
		)

		wg.Add(len(events))
		assert.NoError(t, sut.Register(events[0].EntityType, MockEvent{}))

		writer.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
			got = seqs.Concat2(got, events)
			wg.Done()
			return nil
		}

		assert.NoError(t, sut.Write(ctx, entityType, expected))

		go func() {
			// act
			err := sut.StartPublish(ctx, writer)

			// assert
			assert.NoError(t, err)
		}()

		// assert
		wg.Wait()
		assert.EqualSeq2(t, expected, got, func(expected, got assert.KeyValue[es.Event, error]) bool {
			return eventassert.EqualEvent(t, expected.Key, got.Key)
		})
		cancel()
	})

	t.Run("write no events with same event number", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			entityType  = "entityType"
			entityID    = "entityID"
			eventA      = newEvent(1, func(e *es.Event) {
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

		go func() {
			err := sut.StartPublish(ctx, writer)

			// assert
			assert.NoError(t, err)
		}()

		// act
		err := sut.Write(ctx, entityType, seqs.Seq2(eventB))

		// assert
		assert.Error(t, err)
		cancel()
	})

	t.Run("write no events out of order", func(t *testing.T) {
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

	t.Run("return entity ids written", func(t *testing.T) {
		// arrange
		var (
			entityType = newEntityType()
			events     = newEvents(entityType, 5)
			writer     = &WriterMock{}
			sut        = inmemory.New()
		)

		writer.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, entityType, seqs.Seq2(events...)))

		// act
		got, pageToken, err := sut.GetEntityIDs(t.Context(), entityType, "", 10)

		// assert
		assert.NoError(t, err)
		var ids []string
		for _, event := range events {
			ids = append(ids, event.EntityID)
		}
		if assert.EqualSlice(t, ids, got) {
			assert.Truef(t, events[len(events)-1].StoreEntityID == pageToken, "got page token %v", got)
		}
	})

	t.Run("return entity ids written with limit", func(t *testing.T) {
		// arrange
		var (
			entityType = newEntityType()
			events     = newEvents(entityType, 10)
			writer     = &WriterMock{}
			sut        = inmemory.New()
		)

		writer.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, entityType, seqs.Seq2(events...)))

		// act
		got, _, err := sut.GetEntityIDs(t.Context(), entityType, "", 5)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 5, len(got))
	})

	t.Run("return entity ids from same entity type", func(t *testing.T) {
		// arrange
		var (
			entityType = newEntityType()
			events     = newEvents(entityType, 10)
			writer     = &WriterMock{}
			sut        = inmemory.New()
		)

		writer.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, entityType, seqs.Seq2(newEvents(newEntityType(), 10)...)))
		assert.NoError(t, sut.Write(ctx, entityType, seqs.Seq2(events...)))

		// act
		got, _, err := sut.GetEntityIDs(t.Context(), entityType, "", 5)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 5, len(got))
	})
}
