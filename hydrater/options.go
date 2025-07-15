package hydrater

import (
	"context"
	"reflect"
	"time"

	"github.com/kyuff/es"
)

type Config[T es.Handler] struct {
	backoff func(state T, retries int) time.Duration
	checker func(state T) bool
	maker   func(ctx context.Context, id string) (T, error)
}

func defaultOptions[T es.Handler]() *Config[T] {
	return applyOptions(&Config[T]{},
		WithLinearBackoff[T](time.Millisecond*100),
		WithChecker(defaultChecker[T]()),
		WithMaker(defaultMaker[T]()),
	)
}

func applyOptions[T es.Handler](opts *Config[T], options ...Option[T]) *Config[T] {
	for _, opt := range options {
		opt(opts)
	}
	return opts
}

type Option[T es.Handler] func(*Config[T])

// WithBackoff allows you to provide a custom backoff function that determines
// the wait time between retries based on the current state and the number of retries.
func WithBackoff[T es.Handler](backoff func(T, int) time.Duration) Option[T] {
	return func(o *Config[T]) {
		o.backoff = backoff
	}
}

// WithFixedBackoff waits a fixed amount of time between retries.
func WithFixedBackoff[T es.Handler](d time.Duration) Option[T] {
	return WithBackoff[T](func(_ T, _ int) time.Duration {
		return d
	})
}

// WithLinearBackoff increases the wait time linearly with each retry.
func WithLinearBackoff[T es.Handler](increment time.Duration) Option[T] {
	return WithBackoff[T](func(_ T, retries int) time.Duration {
		return increment * time.Duration(retries)
	})
}

// WithExponentialBackoff doubles the wait time with each retry.
func WithExponentialBackoff[T es.Handler](base time.Duration) Option[T] {
	return WithBackoff[T](func(_ T, retries int) time.Duration {
		return base * time.Duration(1<<retries)
	})
}

// WithChecker allows you to provide a custom checker function that determines
// whether the operation should continue retrying.
func WithChecker[T es.Handler](checker func(state T) bool) Option[T] {
	return func(config *Config[T]) {
		config.checker = checker
	}
}

// WithMaker allows you to provide a custom maker function that creates a new instance of the handler.
func WithMaker[T es.Handler](maker func(ctx context.Context, id string) (T, error)) Option[T] {
	return func(config *Config[T]) {
		config.maker = maker
	}
}

func defaultMaker[T any]() func(ctx context.Context, id string) (T, error) {
	var (
		state     T
		stateType = reflect.TypeOf(state)
	)

	return func(_ context.Context, _ string) (T, error) {
		return reflect.New(stateType.Elem()).Interface().(T), nil
	}
}

func defaultChecker[T es.Handler]() func(state T) bool {
	return func(state T) bool {
		if any(state) == nil {
			return false
		}

		return true
	}
}
