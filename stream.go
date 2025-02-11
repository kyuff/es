package es

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/kyuff/es/internal/uuid"
)

// Stream is a sequence of Events that in combination represent the state of an entity.
// The Stream can be written and read from, which enables applications to alter and get the state.
type Stream interface {
	// Project iterates over all events in the stream and calls the handler for each event.
	// The Stream will stop projecting if the handler returns an error.
	Project(handler Handler) error
	// All returns a iter.Seq2 of all events in the stream.
	// The returned iter.Seq2 will stop and return an error if there was an error
	// reading the events.
	// Calling this method twice will return the same iter.Seq2
	All() iter.Seq2[Event, error]
	// Write writes the given events to the stream.
	// The Events will be written in the order they are given and starting
	// at the most recent event number + 1.
	Write(events ...Content) error
	// Position returns the current position of the stream.
	Position() int64
	Close() error
}

func newStream(ctx context.Context, entityType, entityID string, eventNumber int64, rd Reader, w Writer, cfg *Config) Stream {
	return &stream{
		ctx:        ctx,
		cfg:        cfg,
		entityType: entityType,
		entityID:   entityID,
		rd:         rd,
		w:          w,
		position:   eventNumber,
	}
}

type stream struct {
	ctx        context.Context
	cfg        *Config
	entityType string
	entityID   string

	rd            Reader
	w             Writer
	once          sync.Once
	iter          iter.Seq2[Event, error]
	position      int64
	storeEntityID string
}

func (s *stream) Project(handler Handler) error {
	for event, err := range s.All() {
		if err != nil {
			return err
		}

		if err := handler.Handle(s.ctx, event); err != nil {
			return err
		}
	}

	return nil
}

func (s *stream) Write(eventContents ...Content) error {
	var (
		eventTime   = time.Now()
		eventNumber = s.position
		eventIDs    = uuid.V7At(eventTime, len(eventContents))
	)

	if s.position == 0 {
		s.storeEntityID = uuid.V7AtTime(eventTime)
	} else if s.storeEntityID == "" {
		return fmt.Errorf("must project stream before writing events after event number %d", s.position)
	}

	err := s.w.Write(s.ctx, s.entityType, func(yield func(Event, error) bool) {
		for i, content := range eventContents {
			eventNumber = eventNumber + 1
			event := Event{
				EntityID:      s.entityID,
				EntityType:    s.entityType,
				EventNumber:   eventNumber,
				EventTime:     eventTime,
				Content:       content,
				StoreEventID:  eventIDs[i],
				StoreEntityID: s.storeEntityID,
			}
			if !yield(event, nil) {
				return
			}
		}
	})
	if err != nil {
		return err
	}

	s.position = eventNumber

	return nil
}

func (s *stream) All() iter.Seq2[Event, error] {
	s.once.Do(func() {
		i := s.rd.Read(s.ctx, s.entityType, s.entityID, s.position)
		s.iter = func(yield func(Event, error) bool) {
			for event, err := range i {
				if err != nil {
					yield(Event{}, err)
					return
				}

				s.position = event.EventNumber
				s.storeEntityID = event.StoreEntityID
				if !yield(event, nil) {
					return
				}
			}
		}
	})

	return s.iter
}

func (s *stream) Position() int64 {
	return s.position
}

func (s *stream) Close() error {
	// TODO
	return nil
}
