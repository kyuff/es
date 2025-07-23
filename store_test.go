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

func TestStore(t *testing.T) {
	var (
		ctx             = context.Background()
		newEntityType   = uuid.V7
		newEntityID     = uuid.V7
		newSubscriberID = uuid.V7
		newEvent        = func(id, typ string, mods ...func(e *es.Event)) es.Event {
			e := es.Event{
				StreamID:      id,
				StreamType:    typ,
				EventNumber:   1,
				StoreEventID:  uuid.V7(),
				StoreStreamID: id + "-" + typ,
				Content:       EventMock{ID: rand.IntN(124)},
			}
			for _, mod := range mods {
				mod(&e)
			}

			return e
		}
		newEvents = func(c int, id, typ string, mods ...func(e *es.Event)) []es.Event {
			var events []es.Event
			for i := range c {
				m := append(mods, func(e *es.Event) {
					e.EventNumber = int64(i) + 1
				})
				events = append(events, newEvent(id, typ, m...))
			}
			return events
		}
		newEventUpgrade = func(factor int) es.EventUpgradeFunc {
			return func(ctx context.Context, i iter.Seq2[es.Event, error]) iter.Seq2[es.Event, error] {
				return func(yield func(es.Event, error) bool) {
					for event, err := range i {
						switch e := event.Content.(type) {
						case EventMock:
							event.Content = UpgradedEventMock{Number: e.ID * factor}
						case UpgradedEventMock:
							event.Content = UpgradedEventMock{Number: e.Number * factor}
						}
						yield(event, err)
					}
				}
			}
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
				store      = es.NewStore(storage, es.WithEvents(entityType, []es.Content{EventMock{}}))

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

		t.Run("support logging", func(t *testing.T) {
			// arrange
			var (
				storage = &StorageMock{}
				store   = es.NewStore(storage, es.WithDefaultSlog())
			)

			// act
			err := store.Close()

			// assert
			assert.NoError(t, err)
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

	t.Run("EventUpgrade", func(t *testing.T) {
		t.Run("upgrade events when projecting", func(t *testing.T) {
			// arrange
			var (
				entityType = newEntityType()
				entityID   = newEntityID()
				storage    = &StorageMock{}
				events     = newEvents(3, entityID, entityType, func(e *es.Event) {
					e.Content = EventMock{ID: rand.IntN(300)}
				})
				factorA, factorB = 2, 3
				store            = es.NewStore(storage,
					es.WithEventUpgrades(entityType,
						newEventUpgrade(factorA),
						newEventUpgrade(factorB),
					),
				)
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
			assert.EqualSliceFunc(t, events, got, func(want, item es.Event) bool {
				got, ok := item.Content.(UpgradedEventMock)
				assert.Truef(t, ok, "expected event to be of type UpgradedEventMock, got %T", item.Content)

				expected, ok := want.Content.(EventMock)
				assert.Truef(t, ok, "expected event to be of type EventMock, got %T", want.Content)

				return assert.Equal(t, expected.ID*factorA*factorB, got.Number)
			})
		})

		t.Run("upgrade events when event bus publishes", func(t *testing.T) {
			// arrange
			var (
				entityType   = newEntityType()
				entityID     = newEntityID()
				subscriberID = newSubscriberID()
				storage      = &StorageMock{}
				eventBus     es.Writer
				events       = newEvents(3, entityID, entityType, func(e *es.Event) {
					e.Content = EventMock{ID: rand.IntN(300)}
				})
				factorA, factorB = 2, 3
				store            = es.NewStore(storage,
					es.WithEventUpgrades(entityType,
						newEventUpgrade(factorA),
						newEventUpgrade(factorB),
					),
				)
				got []es.Event
			)

			storage.StartPublishFunc = func(ctx context.Context, w es.Writer) error {
				eventBus = w
				return nil
			}
			storage.WriteFunc = func(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
				return eventBus.Write(ctx, entityType, events)
			}
			assert.NoError(t, store.Start(t.Context()))

			assert.NoError(t, store.Subscribe(ctx, entityType, subscriberID, es.HandlerFunc(func(ctx context.Context, event es.Event) error {
				got = append(got, event)
				return nil
			})))
			stream := store.Open(ctx, entityType, entityID)

			// act
			for _, event := range events {
				assert.NoError(t, stream.Write(event.Content))
			}

			// assert
			assert.EqualSliceFunc(t, events, got, func(want, item es.Event) bool {
				got, ok := item.Content.(UpgradedEventMock)
				assert.Truef(t, ok, "expected event to be of type UpgradedEventMock, got %T", item.Content)

				expected, ok := want.Content.(EventMock)
				assert.Truef(t, ok, "expected event to be of type EventMock, got %T", want.Content)

				return assert.Equal(t, expected.ID*factorA*factorB, got.Number)
			})
		})
	})
}
