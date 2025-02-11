package es_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/kyuff/es"
	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/seqs"
	"github.com/kyuff/es/internal/uuid"
)

func TestInMemoryEventBus(t *testing.T) {
	var (
		ctx             = context.Background()
		newEntityType   = uuid.V7
		newSubscriberID = uuid.V7
		newHandler      = func() es.HandlerFunc {
			return func(ctx context.Context, event es.Event) error {
				return nil
			}
		}
		newEvent = func(entityType string, eventNumber int) es.Event {
			return es.Event{EventNumber: int64(eventNumber), EntityType: entityType}
		}
		newEventList = func(entityType string, count int) []es.Event {
			var events []es.Event
			for i := 1; i <= count; i++ {
				events = append(events, newEvent(entityType, i))
			}
			return events
		}
		newChanHandler = func(ch chan es.Event) es.HandlerFunc {
			return func(ctx context.Context, event es.Event) error {
				ch <- event
				return nil
			}
		}
	)
	t.Run("Subscribe", func(t *testing.T) {
		t.Run("should register a subscriber", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				entityType   = newEntityType()
				subscriberID = newSubscriberID()
				handler      = newHandler()
			)

			// act
			err := sut.Subscribe(ctx, entityType, subscriberID, handler)

			// assert
			assert.NoError(t, err)
		})

		t.Run("should return error registering a subscriber two times", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				entityType   = newEntityType()
				subscriberID = newSubscriberID()
				handler      = newHandler()
			)

			assert.NoError(t, sut.Subscribe(ctx, entityType, subscriberID, handler))

			// act
			err := sut.Subscribe(ctx, entityType, subscriberID, handler)

			// assert
			assert.Error(t, err)
		})

		t.Run("should return a list of subscriber ids", func(t *testing.T) {
			// arrange
			var (
				sut           = es.NewInMemoryEventBus()
				entityType    = newEntityType()
				subscriberIDs = []string{
					newSubscriberID(),
					newSubscriberID(),
					newSubscriberID(),
					newSubscriberID(),
				}
			)
			slices.Sort(subscriberIDs)

			for _, subscriberID := range subscriberIDs {
				assert.NoError(t, sut.Subscribe(ctx, entityType, subscriberID, newHandler()))
			}

			// act
			got, err := sut.GetSubscriberIDs(ctx, entityType)

			// assert
			assert.NoError(t, err)
			slices.Sort(got)
			assert.EqualSlice(t, subscriberIDs, got)
		})
	})

	t.Run("Write", func(t *testing.T) {
		t.Run("should publish events written", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				entityType   = newEntityType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					return nil
				})
				events = newEventList(entityType, 3)
			)

			assert.NoError(t, sut.Subscribe(ctx, entityType, subscriberID, handler))

			// act
			err := sut.Write(ctx, entityType, seqs.Seq2(events...))

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, events, got)
		})

		t.Run("should stop publish after error", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				entityType   = newEntityType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					if event.EventNumber == 3 {
						return errors.New("FAIL")
					}
					return nil
				})
				events = newEventList(entityType, 5)
			)

			assert.NoError(t, sut.Subscribe(ctx, entityType, subscriberID, handler))

			// act
			err := sut.Write(ctx, entityType, seqs.Seq2(events...))

			// assert
			assert.Error(t, err)
			assert.EqualSlice(t, events[0:3], got)
		})

		t.Run("should publish to all subscribers", func(t *testing.T) {
			// arrange
			var (
				sut        = es.NewInMemoryEventBus()
				entityType = newEntityType()
				got        []es.Event
				ch         = make(chan es.Event, 9)
				handlers   = map[string]es.Handler{
					newSubscriberID(): newChanHandler(ch),
					newSubscriberID(): newChanHandler(ch),
					newSubscriberID(): newChanHandler(ch),
				}
				events = newEventList(entityType, 3)
			)

			for subscriberID, handler := range handlers {
				assert.NoError(t, sut.Subscribe(ctx, entityType, subscriberID, handler))
			}

			// act
			err := sut.Write(ctx, entityType, seqs.Seq2(events...))

			// assert
			close(ch)
			for e := range ch {
				got = append(got, e)
			}
			assert.NoError(t, err)
			for i := 0; i < len(handlers); i++ {
				assert.Equal(t, events[i].EventNumber, got[3*i+0].EventNumber)
				assert.Equal(t, events[i].EventNumber, got[3*i+1].EventNumber)
				assert.Equal(t, events[i].EventNumber, got[3*i+2].EventNumber)
			}
		})
	})
}
