package inmemory

import (
	"context"
	"fmt"
	"iter"
	"sync"

	"github.com/kyuff/es"
	"github.com/kyuff/es/codecs"
)

type Writer interface {
	Write(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error
}

func New() *Storage {
	return &Storage{
		uniqueIndex: make(map[indexKey]int),
		codec:       codecs.NewJSON(),
		outbox:      make(chan es.Event, 1000),
	}
}

var _ es.Storage = (*Storage)(nil)

type Storage struct {
	uniqueIndex map[indexKey]int
	tablesMux   sync.RWMutex
	table       table
	codec       *codecs.JSON
	outbox      chan es.Event

	handlersMux sync.RWMutex
	handlers    []es.Handler
}

func (s *Storage) GetStreamReferences(ctx context.Context, streamType string, storeStreamID string, limit int64) iter.Seq2[es.StreamReference, error] {
	return func(yield func(es.StreamReference, error) bool) {
		s.tablesMux.RLock()
		defer s.tablesMux.RUnlock()

		var sent int64 = 0
		for _, row := range s.table {
			if streamType != row.StreamType {
				continue
			}

			if storeStreamID > row.StoreStreamID {
				return
			}

			if sent >= limit {
				return
			}

			if !yield(es.StreamReference{
				StreamType:    row.StreamType,
				StreamID:      row.StreamID,
				StoreStreamID: row.StoreStreamID,
			}, nil) {
				return
			}
			sent++
		}
	}
}

func (s *Storage) Write(ctx context.Context, _ string, events iter.Seq2[es.Event, error]) error {
	for event, err := range events {
		if err != nil {
			return err
		}

		err := s.writeEvent(ctx, event)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) writeEvent(ctx context.Context, event es.Event) error {
	key := indexKey{
		StreamType:  event.StreamType,
		StreamID:    event.StreamID,
		EventNumber: event.EventNumber,
	}
	s.tablesMux.Lock()
	defer s.tablesMux.Unlock()

	_, ok := s.uniqueIndex[key]
	if ok {
		return fmt.Errorf("event already exists: %d", key.EventNumber)
	}

	row, err := newData(event, s.codec)
	if err != nil {
		return err
	}

	_, isNext := s.uniqueIndex[indexKey{
		StreamType:  event.StreamType,
		StreamID:    event.StreamID,
		EventNumber: event.EventNumber - 1,
	}]
	if !isNext && event.EventNumber > 1 {
		return fmt.Errorf("event number mismatch: expected %d, got %d", event.EventNumber+1, event.EventNumber)
	}

	s.table = append(s.table, row)
	s.uniqueIndex[key] = len(s.table) - 1

	// TODO If this had a real outbox, it should only be deleted after the event has been published
	s.outbox <- event

	return nil
}

func (s *Storage) Read(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[es.Event, error] {
	return func(yield func(es.Event, error) bool) {
		s.tablesMux.RLock()
		defer s.tablesMux.RUnlock()

		key := indexKey{
			StreamType:  streamType,
			StreamID:    streamID,
			EventNumber: eventNumber + 1,
		}

		for {
			rowIndex, ok := s.uniqueIndex[key]
			if !ok {
				return
			}

			row := s.table[rowIndex]

			event, err := row.Event(s.codec)
			if err != nil {
				yield(es.Event{}, err)
				return
			}

			if !yield(event, nil) {
				return
			}

			key.EventNumber = key.EventNumber + 1
		}
	}
}

func (s *Storage) Register(streamType string, contentTypes ...es.Content) error {
	return s.codec.Register(streamType, contentTypes...)
}

func (s *Storage) StartPublish(ctx context.Context, writer es.Writer) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-s.outbox:
			err := writer.Write(ctx, event.StreamType, func(yield func(es.Event, error) bool) {
				_ = yield(event, nil)
			})
			if err != nil {
				// TODO log error
			}
		}
	}
}
