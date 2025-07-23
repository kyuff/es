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

// Open a stream by its type and id. The Stream will be opened at the start and must be closed.
func (s *Store) Open(ctx context.Context, streamType string, streamID string) Stream {
	return s.OpenFrom(ctx, streamType, streamID, 0)
}

// OpenFrom opens a Stream so the first event read will be eventNumber + 1.
// The Stream must be closed.
func (s *Store) OpenFrom(ctx context.Context, streamType string, streamID string, eventNumber int64) Stream {
	return newStream(
		ctx,
		streamType,
		streamID,
		eventNumber,
		s.getStreamReader(streamType),
		s.storage,
		s.cfg,
	)
}

// Project onto a Handler all Events by the type and id of the stream.
func (s *Store) Project(ctx context.Context, streamType, streamID string, handler Handler) (err error) {
	stream := s.Open(ctx, streamType, streamID)
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
func (s *Store) Subscribe(ctx context.Context, streamType string, subscriberID string, handler Handler) error {
	return s.cfg.eventBus.Subscribe(ctx, streamType, subscriberID, handler)
}

// GetSubscriberIDs returns a list of all subscriber IDs registered for a given streamType.
func (s *Store) GetSubscriberIDs(ctx context.Context, streamType string) ([]string, error) {
	return s.cfg.eventBus.GetSubscriberIDs(ctx, streamType)
}

// Start starts the store by starting the storage and the event bus
// The method is blocking and will return when the store is closed.
// The method will return an error if the storage fails to publish events.
func (s *Store) Start(ctx context.Context) error {
	for streamType, contentTypes := range s.cfg.contentTypes {
		err := s.storage.Register(streamType, contentTypes...)
		if err != nil {
			return err
		}
	}

	return s.storage.StartPublish(ctx, newUpgradeWriter(s.cfg.eventBus, s.cfg.eventUpgrades))
}

func (s *Store) Close() error {
	return s.cfg.eventBus.Close()
}

func (s *Store) getStreamReader(streamType string) Reader {
	s.mux.RLock()
	rd, ok := s.upgradeReaders[streamType]
	s.mux.RUnlock()
	if !ok {
		s.mux.Lock()
		rd = newUpgradeReader(s.storage, s.cfg.eventUpgrades[streamType])
		s.upgradeReaders[streamType] = rd
		s.mux.Unlock()
	}

	return rd
}
