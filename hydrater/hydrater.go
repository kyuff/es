package hydrater

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/kyuff/es"
)

type Projector interface {
	Project(ctx context.Context, entityType, entityID string, handler es.Handler) (err error)
}

type Hydrater[T es.Handler] struct {
	projector  Projector
	entityType string
	cfg        *Config[T]
}

func New[T es.Handler](projector Projector, entityType string, opts ...Option[T]) *Hydrater[T] {
	return &Hydrater[T]{
		projector:  projector,
		entityType: entityType,
		cfg: applyOptions(
			defaultOptions[T](),
			opts...,
		),
	}
}

func (r *Hydrater[T]) Hydrate(ctx context.Context, id string) (T, error) {

	var (
		result  = make(chan T)
		errChan = make(chan error)
		done    = atomic.Bool{}
	)

	defer func() {
		done.Store(true)
	}()

	go func() {
		retries := 0
		for !done.Load() {
			state, err := r.cfg.maker(ctx, id)
			if err != nil {
				errChan <- fmt.Errorf("making state for %s: %w", id, err)
				return
			}

			err = r.projector.Project(ctx, r.entityType, id, state)
			if err != nil {
				errChan <- err
				return
			}

			if r.cfg.checker(state) {
				result <- state
				return
			}

			retries++
			time.Sleep(r.cfg.backoff(state, retries))
		}
	}()

	select {
	case <-ctx.Done():
		var empty T
		return empty, ctx.Err()
	case err := <-errChan:
		var empty T
		return empty, err
	case value := <-result:
		return value, nil
	}

}
