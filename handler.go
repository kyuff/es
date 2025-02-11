package es

import "context"

// Handler is code that can Handle when an Event happens.
type Handler interface {
	Handle(ctx context.Context, event Event) error
}

// HandlerFunc is a convenience type to allow an inline func to act as a Handler
type HandlerFunc func(ctx context.Context, event Event) error

func (fn HandlerFunc) Handle(ctx context.Context, event Event) error {
	return fn(ctx, event)
}
