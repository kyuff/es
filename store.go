package es

import (
	"context"
	"errors"
	"sync"
)

func NewStore(storage Storage, opts ...Option) *Store {
	return &Store{
		storage: storage,
		cfg:     applyOptions(defaultOptions(), opts...),

		upgradeReaders: make(map[string]Reader),
	}
}

type Store struct {
	storage Storage
	cfg     *Config

	mux            sync.RWMutex
	upgradeReaders map[string]Reader
}

// Open a stream by the type and id of the entity. The Stream will be opened at the start and must be closed.
func (s *Store) Open(ctx context.Context, entityType string, entityID string) Stream {
	return s.OpenFrom(ctx, entityType, entityID, 0)
}

// OpenFrom opens a Stream so the first event read will be eventNumber + 1.
// The Stream must be closed.
func (s *Store) OpenFrom(ctx context.Context, entityType string, entityID string, eventNumber int64) Stream {
	return newStream(
		ctx,
		entityType,
		entityID,
		eventNumber,
		s.getStreamReader(entityType),
		s.storage,
		s.cfg,
	)
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

// Subscribe to all events on an streamType to be passed to the Handler
func (s *Store) Subscribe(ctx context.Context, entityType string, subscriberID string, handler Handler) error {
	return s.cfg.eventBus.Subscribe(ctx, entityType, subscriberID, handler)
}

// GetSubscriberIDs returns a list of all subscriber IDs registered for a given streamType.
func (s *Store) GetSubscriberIDs(ctx context.Context, entityType string) ([]string, error) {
	return s.cfg.eventBus.GetSubscriberIDs(ctx, entityType)
}

// Start starts the store by starting the storage and the event bus
// The method is blocking and will return when the store is closed.
// The method will return an error if the storage fails to publish events.
func (s *Store) Start(ctx context.Context) error {
	for entityType, contentTypes := range s.cfg.contentTypes {
		err := s.storage.Register(entityType, contentTypes...)
		if err != nil {
			return err
		}
	}

	return s.storage.StartPublish(ctx, newUpgradeWriter(s.cfg.eventBus, s.cfg.eventUpgrades))
}

func (s *Store) Close() error {
	return s.cfg.eventBus.Close()
}

func (s *Store) getStreamReader(entityType string) Reader {
	s.mux.RLock()
	rd, ok := s.upgradeReaders[entityType]
	s.mux.RUnlock()
	if !ok {
		s.mux.Lock()
		rd = newUpgradeReader(s.storage, s.cfg.eventUpgrades[entityType])
		s.upgradeReaders[entityType] = rd
		s.mux.Unlock()
	}

	return rd
}
