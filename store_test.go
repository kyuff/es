package es_test

import (
	"context"
	"iter"
	"math/rand/v2"
	"testing"

	"github.com/kyuff/es"
	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/eventassert"
	"github.com/kyuff/es/internal/seqs"
	"github.com/kyuff/es/internal/uuid"
)

type EventMock struct{ ID int }

func (EventMock) Name() string {
	return "EventMock"
}

func TestStore(t *testing.T) {
	var (
		ctx           = context.Background()
		newEntityType = uuid.V7
		newEntityID   = uuid.V7
		newEvent      = func(id, typ string, mods ...func(e *es.Event)) es.Event {
			e := es.Event{
				EntityID:      id,
				EntityType:    typ,
				EventNumber:   1,
				StoreEventID:  uuid.V7(),
				StoreEntityID: id + "-" + typ,
				Content:       EventMock{ID: rand.IntN(124)},
			}
			for _, mod := range mods {
				mod(&e)
			}

			return e
		}
		newEvents = func(c int, id, typ string) []es.Event {
			var events []es.Event
			for i := range c {
				events = append(events, newEvent(id, typ, func(e *es.Event) {
					e.EventNumber = int64(i) + 1
				}))
			}
			return events
		}
	)

	t.Run("Store", func(t *testing.T) {
		t.Run("project all events in order", func(t *testing.T) {
			// arrange
			var (
				entityType = newEntityType()
				entityID   = newEntityID()
				storage    = &StorageMock{}
				events     = newEvents(3, entityID, entityType)
				store      = es.NewStore(storage)

				got []es.Event
			)

			storage.ReadFunc = func(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[es.Event, error] {
				return seqs.Seq2(events...)
			}

			// act
			err := store.Project(ctx, entityType, entityID, es.HandlerFunc(func(ctx context.Context, event es.Event) error {
				got = append(got, event)
				return nil
			}))

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, events, got)
		})

		t.Run("open storage at start", func(t *testing.T) {
			// arrange
			var (
				entityType = newEntityType()
				entityID   = newEntityID()
				storage    = &StorageMock{}
				store      = es.NewStore(storage)

				got int64 = -1
			)

			storage.ReadFunc = func(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[es.Event, error] {
				got = eventNumber
				return func(yield func(es.Event, error) bool) {}
			}

			// act
			_ = store.Open(ctx, entityType, entityID).Project(es.HandlerFunc(func(ctx context.Context, event es.Event) error {
				return nil
			}))

			// assert
			assert.Equal(t, 0, got)
		})

		t.Run("open from storage at event number", func(t *testing.T) {
			// arrange
			var (
				entityType = newEntityType()
				entityID   = newEntityID()
				storage    = &StorageMock{}
				store      = es.NewStore(storage)

				got int64 = -1
			)

			storage.ReadFunc = func(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[es.Event, error] {
				got = eventNumber
				return func(yield func(es.Event, error) bool) {}
			}

			// act
			_ = store.OpenFrom(ctx, entityType, entityID, 7).Project(es.HandlerFunc(func(ctx context.Context, event es.Event) error {
				return nil
			}))

			// assert
			assert.Equal(t, 7, got)
		})

		t.Run("read once from the storage", func(t *testing.T) {
			// arrange
			var (
				entityType = newEntityType()
				entityID   = newEntityID()
				storage    = &StorageMock{}
				events     = newEvents(3, entityID, entityType)
				store      = es.NewStore(storage)
				stream     = store.Open(ctx, entityType, entityID)

				got []es.Event
			)

			storage.ReadFunc = func(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[es.Event, error] {
				return seqs.Seq2(events...)
			}

			_ = stream.All()

			// act
			err := stream.Project(es.HandlerFunc(func(ctx context.Context, event es.Event) error {
				got = append(got, event)
				return nil
			}))

			// assert
			assert.NoError(t, err)
			assert.Equal(t, 1, len(storage.ReadCalls()))
		})
	})

	t.Run("Stream", func(t *testing.T) {
		t.Run("project all events in order", func(t *testing.T) {
			// arrange
			var (
				entityType = newEntityType()
				entityID   = newEntityID()
				storage    = &StorageMock{}
				events     = newEvents(3, entityID, entityType)
				store      = es.NewStore(storage)
				stream     = store.Open(ctx, entityType, entityID)
				got        []es.Event
			)

			storage.ReadFunc = func(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[es.Event, error] {
				return seqs.Seq2(events...)
			}

			// act
			err := stream.Project(es.HandlerFunc(func(ctx context.Context, event es.Event) error {
				got = append(got, event)
				return nil
			}))

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, events, got)
		})

		t.Run("write at next event number", func(t *testing.T) {
			// arrange
			var (
				entityType = newEntityType()
				entityID   = newEntityID()
				storage    = &StorageMock{}
				events     = seqs.Seq2(newEvents(3, entityID, entityType)...)
				store      = es.NewStore(storage)
				stream     = store.Open(ctx, entityType, entityID)

				got      iter.Seq2[es.Event, error]
				expected = seqs.Seq2(
					newEvent(entityID, entityType, func(e *es.Event) {
						e.Content = EventMock{ID: 123}
						e.EventNumber = 4
					}),
					newEvent(entityID, entityType, func(e *es.Event) {
						e.Content = EventMock{ID: 512}
						e.EventNumber = 5
					}),
				)
			)

			storage.ReadFunc = func(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[es.Event, error] {
				return events
			}

			storage.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
				got = events
				return nil
			}
			assert.NoError(t, stream.Project(es.HandlerFunc(func(ctx context.Context, event es.Event) error {
				return nil
			})))

			// act
			err := stream.Write(
				EventMock{ID: 123},
				EventMock{ID: 512},
			)

			// assert
			assert.NoError(t, err)
			assert.EqualSeq2(t, expected, got, func(expected, got assert.KeyValue[es.Event, error]) bool {
				return eventassert.EqualEvent(t, expected.Key, got.Key)
			})
		})
	})
}
