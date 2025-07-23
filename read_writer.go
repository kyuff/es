package es

import (
	"context"
	"iter"
)

// Reader allows getting a sequence of Events for an StreamType and StreamID
type Reader interface {
	Read(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[Event, error]
}

type readerFunc func(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[Event, error]

func (fn readerFunc) Read(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[Event, error] {
	return fn(ctx, streamType, streamID, eventNumber)
}

// Writer allows writing a sequence of Events to an streamType
type Writer interface {
	Write(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error
}

type writerFunc func(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error

func (fn writerFunc) Write(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error {
	return fn(ctx, streamType, events)
}

type ReadWriter interface {
	Reader
	Writer
}
