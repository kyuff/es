package es

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"

	"golang.org/x/sync/errgroup"
)

func NewInMemoryEventBus() *InMemoryEventBus {
	return &InMemoryEventBus{
		subList: make(map[string][]subscriber),
		subMap:  make(map[string]map[string]subscriber),
	}
}

var _ EventBus = (*InMemoryEventBus)(nil)

type InMemoryEventBus struct {
	mux     sync.RWMutex
	subList map[string][]subscriber
	subMap  map[string]map[string]subscriber
}

func (bus *InMemoryEventBus) GetSubscriberIDs(ctx context.Context, streamType string) ([]string, error) {
	bus.mux.RLock()
	defer bus.mux.RUnlock()

	return slices.Collect(maps.Keys(bus.subMap[streamType])), nil
}

func (bus *InMemoryEventBus) Write(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error {
	subs := bus.subList[streamType]
	if len(subs) == 0 {
		return nil
	}

	for event, err := range events {
		if err != nil {
			return err
		}

		err := bus.publish(ctx, streamType, event, subs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bus *InMemoryEventBus) WriteTo(ctx context.Context, streamType string, events iter.Seq2[Event, error], subscribers ...string) error {
	bus.mux.RLock()
	defer bus.mux.RUnlock()

	var subs []subscriber
	if len(bus.subMap[streamType]) == 0 {
		return nil
	}

	for _, id := range subscribers {
		if sub, ok := bus.subMap[streamType][id]; ok {
			subs = append(subs, sub)
		}
	}

	for event, err := range events {
		if err != nil {
			return err
		}

		err := bus.publish(ctx, streamType, event, subs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bus *InMemoryEventBus) publish(ctx context.Context, streamType string, event Event, subs []subscriber) error {
	bus.mux.RLock()
	defer bus.mux.RUnlock()

	var g errgroup.Group
	g.SetLimit(10) // TODO make option
	for _, sub := range subs {
		g.Go(func() error {
			return sub.Handle(ctx, event)
		})
	}

	return g.Wait()
}

func (bus *InMemoryEventBus) Subscribe(ctx context.Context, streamType, subscriberID string, handler Handler) error {
	bus.mux.Lock()
	defer bus.mux.Unlock()

	sub := &eventBusHandler{
		id:      subscriberID,
		handler: handler,
	}
	if _, ok := bus.subMap[streamType]; !ok {
		bus.subMap[streamType] = make(map[string]subscriber)
	}

	if _, ok := bus.subMap[streamType][subscriberID]; ok {
		return fmt.Errorf("subscriber already exists: %s.%s", streamType, sub.ID())
	}

	bus.subMap[streamType][subscriberID] = sub
	bus.subList[streamType] = append(bus.subList[streamType], sub)

	return nil
}

func (bus *InMemoryEventBus) Close() error {
	return nil
}

type subscriber interface {
	Handler
	ID() string
}

type eventBusHandler struct {
	id      string
	handler Handler
}

func (h *eventBusHandler) Handle(ctx context.Context, event Event) error {
	return h.handler.Handle(ctx, event)
}

func (h *eventBusHandler) ID() string {
	return h.id
}
