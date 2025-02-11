package es

import (
	"context"
	"iter"
)

// Reader allows getting a sequence of Events for an EntityType and EntityID
type Reader interface {
	Read(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[Event, error]
}

type readerFunc func(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[Event, error]

func (fn readerFunc) Read(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[Event, error] {
	return fn(ctx, entityType, entityID, eventNumber)
}

// Writer allows writing a sequence of Events to an entityType
type Writer interface {
	Write(ctx context.Context, entityType string, events iter.Seq2[Event, error]) error
}

type writerFunc func(ctx context.Context, entityType string, events iter.Seq2[Event, error]) error

func (fn writerFunc) Write(ctx context.Context, entityType string, events iter.Seq2[Event, error]) error {
	return fn(ctx, entityType, events)
}

type ReadWriter interface {
	Reader
	Writer
}
