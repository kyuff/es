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

func (bus *InMemoryEventBus) GetSubscriberIDs(ctx context.Context, entityType string) ([]string, error) {
	bus.mux.RLock()
	defer bus.mux.RUnlock()

	return slices.Collect(maps.Keys(bus.subMap[entityType])), nil
}

func (bus *InMemoryEventBus) Write(ctx context.Context, entityType string, events iter.Seq2[Event, error]) error {
	subs := bus.subList[entityType]
	if len(subs) == 0 {
		return nil
	}

	for event, err := range events {
		if err != nil {
			return err
		}

		err := bus.publish(ctx, entityType, event, subs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bus *InMemoryEventBus) WriteTo(ctx context.Context, entityType string, events iter.Seq2[Event, error], subscribers ...string) error {
	bus.mux.RLock()
	defer bus.mux.RUnlock()

	var subs []subscriber
	if len(bus.subMap[entityType]) == 0 {
		return nil
	}

	for _, id := range subscribers {
		if sub, ok := bus.subMap[entityType][id]; ok {
			subs = append(subs, sub)
		}
	}

	for event, err := range events {
		if err != nil {
			return err
		}

		err := bus.publish(ctx, entityType, event, subs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bus *InMemoryEventBus) publish(ctx context.Context, entityType string, event Event, subs []subscriber) error {
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

func (bus *InMemoryEventBus) Subscribe(ctx context.Context, entityType, subscriberID string, handler Handler) error {
	bus.mux.Lock()
	defer bus.mux.Unlock()

	sub := &eventBusHandler{
		id:      subscriberID,
		handler: handler,
	}
	if _, ok := bus.subMap[entityType]; !ok {
		bus.subMap[entityType] = make(map[string]subscriber)
	}

	if _, ok := bus.subMap[entityType][subscriberID]; ok {
		return fmt.Errorf("subscriber already exists: %s.%s", entityType, sub.ID())
	}

	bus.subMap[entityType][subscriberID] = sub
	bus.subList[entityType] = append(bus.subList[entityType], sub)

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
