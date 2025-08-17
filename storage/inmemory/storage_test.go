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

func (e MockEvent) EventName() string {
	return "MockEvent"
}

func TestStorage(t *testing.T) {
	var (
		ctx      = context.Background()
		newEvent = func(eventNumber int64, mods ...func(e *es.Event)) es.Event {
			e := es.Event{
				StreamID:     fmt.Sprintf("StreamID-%d", eventNumber),
				StreamType:   fmt.Sprintf("StreamType-%d", eventNumber),
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
		newStreamType = func() string {
			return fmt.Sprintf("StreamType-%d", rand.Int63())
		}
		newEvents = func(streamType string, count int) []es.Event {
			var streamID = fmt.Sprintf("StreamID-%d-%d", count, rand.Int63())
			var events []es.Event
			var storeStreamIDs = uuid.V7At(time.Now(), count)
			for i := 1; i <= count; i++ {
				events = append(events, newEvent(int64(i), func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
					e.StoreStreamID = storeStreamIDs[i-1]
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
			streamType  = newStreamType()
			events      = seqs.Seq2(newEvents(streamType, 5)...)
			writer      = &WriterMock{}
			sut         = inmemory.New()
		)

		go func() {
			assert.NoError(t, sut.StartPublish(ctx, writer))
		}()

		writer.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		// act
		err := sut.Write(ctx, streamType, events)

		// assert
		assert.NoError(t, err)
		cancel()
	})

	t.Run("read events written", func(t *testing.T) {
		// arrange
		var (
			streamType = newStreamType()
			events     = newEvents(streamType, 5)
			sut        = inmemory.New()
		)

		assert.NoError(t, sut.Register(events[0].StreamType, MockEvent{}))
		assert.NoError(t, sut.Write(ctx, streamType, seqs.Seq2(events...)))

		// act
		i := sut.Read(ctx, events[0].StreamType, events[0].StreamID, 0)

		// assert
		got := collectEvents(t, i)
		assert.EqualSliceFunc(t, events, got, func(want, item es.Event) bool {
			return assert.Equal(t, want.Content, item.Content) &&
				assert.Equal(t, want.EventNumber, item.EventNumber) &&
				assert.Equal(t, want.StreamType, item.StreamType) &&
				assert.Equal(t, want.StreamID, item.StreamID) &&
				assert.Equal(t, want.StoreEventID, item.StoreEventID) &&
				assert.EqualTime(t, want.EventTime, item.EventTime)
		})
	})

	t.Run("publish events after write", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			streamType  = newStreamType()
			events      = newEvents(streamType, 5)
			expected    = seqs.Seq2(events...)
			writer      = &WriterMock{}
			sut         = inmemory.New()
			got         = seqs.EmptySeq2[es.Event, error]()
			wg          sync.WaitGroup
		)

		wg.Add(len(events))
		assert.NoError(t, sut.Register(events[0].StreamType, MockEvent{}))

		writer.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			got = seqs.Concat2(got, events)
			wg.Done()
			return nil
		}

		assert.NoError(t, sut.Write(ctx, streamType, expected))

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
			streamType  = "streamType"
			streamID    = "streamID"
			eventA      = newEvent(1, func(e *es.Event) {
				e.StreamType = streamType
				e.StreamID = streamID
			})
			eventB = newEvent(1, func(e *es.Event) {
				e.StreamType = streamType
				e.StreamID = streamID
			})
			writer = &WriterMock{}
			sut    = inmemory.New()
		)

		writer.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, streamType, seqs.Seq2(eventA)))

		go func() {
			err := sut.StartPublish(ctx, writer)

			// assert
			assert.NoError(t, err)
		}()

		// act
		err := sut.Write(ctx, streamType, seqs.Seq2(eventB))

		// assert
		assert.Error(t, err)
		cancel()
	})

	t.Run("write no events out of order", func(t *testing.T) {
		// arrange
		var (
			streamType = "streamType"
			streamID   = "streamID"
			eventA     = newEvent(1, func(e *es.Event) {
				e.StreamType = streamType
				e.StreamID = streamID
			})
			eventB = newEvent(3, func(e *es.Event) {
				e.StreamType = streamType
				e.StreamID = streamID
			})
			writer = &WriterMock{}
			sut    = inmemory.New()
		)

		writer.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, streamType, seqs.Seq2(eventA)))

		// act
		err := sut.Write(ctx, streamType, seqs.Seq2(eventB))

		// assert
		assert.Error(t, err)
	})

	t.Run("return stream ids written", func(t *testing.T) {
		// arrange
		var (
			streamType = newStreamType()
			events     = newEvents(streamType, 5)
			writer     = &WriterMock{}
			sut        = inmemory.New()
			expected   []es.StreamReference
		)

		for _, event := range events {
			expected = append(expected, es.StreamReference{
				StreamType:    event.StreamType,
				StreamID:      event.StreamID,
				StoreStreamID: event.StoreStreamID,
			})
		}

		writer.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, streamType, seqs.Seq2(events...)))

		// act
		got := sut.GetStreamReferences(t.Context(), streamType, "", 10)

		// assert
		assert.EqualSeq2(t, seqs.Seq2(expected...), got, func(expected, got assert.KeyValue[es.StreamReference, error]) bool {
			return assert.Equal(t, expected.Key, got.Key)
		})
	})

	t.Run("return stream ids written with limit", func(t *testing.T) {
		// arrange
		var (
			streamType = newStreamType()
			events     = newEvents(streamType, 10)
			writer     = &WriterMock{}
			sut        = inmemory.New()
			expected   []es.StreamReference
		)

		for _, event := range events[0:5] {
			expected = append(expected, es.StreamReference{
				StreamType:    event.StreamType,
				StreamID:      event.StreamID,
				StoreStreamID: event.StoreStreamID,
			})
		}

		writer.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, streamType, seqs.Seq2(events...)))

		// act
		got := sut.GetStreamReferences(t.Context(), streamType, "", 5)

		// assert
		assert.EqualSeq2(t, seqs.Seq2(expected...), got, func(expected, got assert.KeyValue[es.StreamReference, error]) bool {
			return assert.Equal(t, expected.Key, got.Key)
		})
	})

	t.Run("return stream ids from same stream type", func(t *testing.T) {
		// arrange
		var (
			streamType = newStreamType()
			events     = newEvents(streamType, 10)
			writer     = &WriterMock{}
			sut        = inmemory.New()

			expected []es.StreamReference
		)

		for _, event := range events[0:5] {
			expected = append(expected, es.StreamReference{
				StreamType:    event.StreamType,
				StreamID:      event.StreamID,
				StoreStreamID: event.StoreStreamID,
			})
		}
		writer.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			return nil
		}

		assert.NoError(t, sut.Write(ctx, streamType, seqs.Seq2(newEvents(newStreamType(), 10)...)))
		assert.NoError(t, sut.Write(ctx, streamType, seqs.Seq2(events...)))

		// act
		got := sut.GetStreamReferences(t.Context(), streamType, "", 5)

		// assert
		assert.EqualSeq2(t, seqs.Seq2(expected...), got, func(expected, got assert.KeyValue[es.StreamReference, error]) bool {
			return assert.Equal(t, expected.Key, got.Key)
		})
	})
}
