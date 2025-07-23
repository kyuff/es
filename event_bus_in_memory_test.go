package es_test

import (
	"context"
	"errors"
	"maps"
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
		newStreamType   = uuid.V7
		newSubscriberID = uuid.V7
		newHandler      = func() es.HandlerFunc {
			return func(ctx context.Context, event es.Event) error {
				return nil
			}
		}
		newEvent = func(streamType string, eventNumber int) es.Event {
			return es.Event{EventNumber: int64(eventNumber), StreamType: streamType}
		}
		newEventList = func(streamType string, count int) []es.Event {
			var events []es.Event
			for i := 1; i <= count; i++ {
				events = append(events, newEvent(streamType, i))
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
		t.Run("close with no error", func(t *testing.T) {
			// arrange
			var (
				sut = es.NewInMemoryEventBus()
			)

			// act
			err := sut.Close()

			// assert
			assert.NoError(t, err)
		})

		t.Run("should register a subscriber", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				handler      = newHandler()
			)

			// act
			err := sut.Subscribe(ctx, streamType, subscriberID, handler)

			// assert
			assert.NoError(t, err)
		})

		t.Run("should return error registering a subscriber two times", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				handler      = newHandler()
			)

			assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))

			// act
			err := sut.Subscribe(ctx, streamType, subscriberID, handler)

			// assert
			assert.Error(t, err)
		})

		t.Run("should return a list of subscriber ids", func(t *testing.T) {
			// arrange
			var (
				sut           = es.NewInMemoryEventBus()
				streamType    = newStreamType()
				subscriberIDs = []string{
					newSubscriberID(),
					newSubscriberID(),
					newSubscriberID(),
					newSubscriberID(),
				}
			)
			slices.Sort(subscriberIDs)

			for _, subscriberID := range subscriberIDs {
				assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, newHandler()))
			}

			// act
			got, err := sut.GetSubscriberIDs(ctx, streamType)

			// assert
			assert.NoError(t, err)
			slices.Sort(got)
			assert.EqualSlice(t, subscriberIDs, got)
		})
	})

	t.Run("Write", func(t *testing.T) {
		t.Run("do nothing if no registered subscriber", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					return nil
				})
				events = newEventList(streamType, 3)
			)

			assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))

			// act
			err := sut.Write(ctx, "other", seqs.Seq2(events...))

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, []es.Event{}, got)
		})

		t.Run("should publish events written", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					return nil
				})
				events = newEventList(streamType, 3)
			)

			assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))

			// act
			err := sut.Write(ctx, streamType, seqs.Seq2(events...))

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, events, got)
		})

		t.Run("should stop publish after error", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					if event.EventNumber == 3 {
						return errors.New("FAIL")
					}
					return nil
				})
				events = newEventList(streamType, 5)
			)

			assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))

			// act
			err := sut.Write(ctx, streamType, seqs.Seq2(events...))

			// assert
			assert.Error(t, err)
			assert.EqualSlice(t, events[0:3], got)
		})

		t.Run("should publish to all subscribers", func(t *testing.T) {
			// arrange
			var (
				sut        = es.NewInMemoryEventBus()
				streamType = newStreamType()
				got        []es.Event
				ch         = make(chan es.Event, 9)
				handlers   = map[string]es.Handler{
					newSubscriberID(): newChanHandler(ch),
					newSubscriberID(): newChanHandler(ch),
					newSubscriberID(): newChanHandler(ch),
				}
				events = newEventList(streamType, 3)
			)

			for subscriberID, handler := range handlers {
				assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))
			}

			// act
			err := sut.Write(ctx, streamType, seqs.Seq2(events...))

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

		t.Run("should fail with sequence", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					return errors.New("FAIL")
				})
				events = maps.All(map[es.Event]error{
					es.Event{}: errors.New("FAIL"),
				})
			)

			assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))

			// act
			err := sut.Write(ctx, streamType, events)

			// assert
			assert.Error(t, err)
			assert.EqualSlice(t, []es.Event{}, got)
		})
	})

	t.Run("WriteTo", func(t *testing.T) {
		t.Run("do nothing if no registered subscriber", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					return nil
				})
				events = newEventList(streamType, 3)
			)

			assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))

			// act
			err := sut.WriteTo(ctx, "other", seqs.Seq2(events...), subscriberID)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, []es.Event{}, got)
		})

		t.Run("should publish events written", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					return nil
				})
				events = newEventList(streamType, 3)
			)

			assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))

			// act
			err := sut.WriteTo(ctx, streamType, seqs.Seq2(events...), subscriberID)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, events, got)
		})

		t.Run("should fail with publish", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					return errors.New("FAIL")
				})
				events = newEventList(streamType, 3)
			)

			assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))

			// act
			err := sut.WriteTo(ctx, streamType, seqs.Seq2(events...), subscriberID)

			// assert
			assert.Error(t, err)
			assert.EqualSlice(t, events[0:1], got)
		})

		t.Run("should fail with sequence", func(t *testing.T) {
			// arrange
			var (
				sut          = es.NewInMemoryEventBus()
				streamType   = newStreamType()
				subscriberID = newSubscriberID()
				got          []es.Event
				handler      = es.HandlerFunc(func(ctx context.Context, event es.Event) error {
					got = append(got, event)
					return errors.New("FAIL")
				})
				events = maps.All(map[es.Event]error{
					es.Event{}: errors.New("FAIL"),
				})
			)

			assert.NoError(t, sut.Subscribe(ctx, streamType, subscriberID, handler))

			// act
			err := sut.WriteTo(ctx, streamType, events, subscriberID)

			// assert
			assert.Error(t, err)
			assert.EqualSlice(t, []es.Event{}, got)
		})
	})
}
