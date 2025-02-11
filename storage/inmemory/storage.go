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
	Write(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error
}

func New() *Storage {
	return &Storage{
		uniqueIndex: make(map[indexKey]int),
		codec:       codecs.NewJSON(),
		outbox:      make(chan es.Event, 1000),
		closed:      make(chan struct{}),
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
	writer      Writer
	closed      chan struct{}
}

func (s *Storage) GetEntityIDs(ctx context.Context, entityType string, storeStreamID string, limit int64) ([]string, string, error) {
	return nil, "", fmt.Errorf("not implemented")
}

func (s *Storage) Write(ctx context.Context, entityType string, events iter.Seq2[es.Event, error]) error {
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
		EntityType:  event.EntityType,
		EntityID:    event.EntityID,
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
		EntityType:  event.EntityType,
		EntityID:    event.EntityID,
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

func (s *Storage) Read(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[es.Event, error] {
	return func(yield func(es.Event, error) bool) {
		s.tablesMux.RLock()
		defer s.tablesMux.RUnlock()

		key := indexKey{
			EntityType:  entityType,
			EntityID:    entityID,
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

func (s *Storage) Register(entityType string, contentTypes ...es.Content) error {
	return s.codec.Register(entityType, contentTypes...)
}

func (s *Storage) Close() error {
	s.closed <- struct{}{}
	close(s.closed)
	return nil
}

func (s *Storage) StartPublish(bus es.Writer) error {
	s.writer = bus
	go s.startOutbox()
	return nil
}

func (s *Storage) startOutbox() {
	for {
		select {
		case <-s.closed:
			return
		case event := <-s.outbox:
			ctx := context.Background() // TODO
			err := s.writer.Write(ctx, event.EntityType, func(yield func(es.Event, error) bool) {
				_ = yield(event, nil)
			})
			if err != nil {
				// TODO log error
			}
		}
	}
}
