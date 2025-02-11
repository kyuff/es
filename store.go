package es

import (
	"context"
	"errors"
)

type Store struct {
}

// Open a stream by the type and id of the entity. The Stream will be opened at the start and must be closed.
func (s *Store) Open(ctx context.Context, entityType string, entityID string) Stream {
	return s.OpenFrom(ctx, entityType, entityID, 0)
}

// OpenFrom opens a Stream so the first event read will be eventNumber + 1.
// The Stream must be closed.
func (s *Store) OpenFrom(ctx context.Context, entityType string, entityID string, eventNumber int64) Stream {
	return nil
}

// Project onto a Handler all Events by the type and id of the entity.
func (s *Store) Project(ctx context.Context, entityType, entityID string, handler Handler) (err error) {
	stream := s.Open(ctx, entityType, entityID)
	defer func() {
		err = errors.Join(err, stream.Close())
	}()

	err = stream.Project(handler)
	if err != nil {
		return err
	}

	return err
}

// Subscribe to all events on an entityType to be passed to the Handler
func (s *Store) Subscribe(ctx context.Context, entityType string, subscriberID string, handler Handler) error {
	return nil
}

// GetSubscriberIDs returns a list of all subscriber IDs registered for a given entityType.
func (s *Store) GetSubscriberIDs(ctx context.Context, entityType string) ([]string, error) {
	return nil, nil
}
