package es

import (
	"context"
	"iter"
)

// Storage is the abstracts the persistence of a Store.
type Storage interface {
	// Read the events of of an streamType with the streamID from eventNumber
	Read(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[Event, error]
	// Write writes the events to the store.
	// All of the events must be written by sequence.
	// They should all be written or fully fail.
	Write(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error
	// StartPublish should begin the process where newly written events are published to the Writer.
	// The publishing must be cancelled with the context
	StartPublish(ctx context.Context, w Writer) error
	// Register allows the Storage to Unmarshal multiple shapes of Content for an streamType.
	// It is considered an error if a Storage contains a shape of Content that have not been registered.
	Register(streamType string, types ...Content) error
	// GetStreamReferences returns a list of references to streams for the given streamType.
	// The returned list is ordered by the StoreStreamID and limited in size by the limit.
	// Use the last StreamReference.StoreStreamID as the pagination token.
	GetStreamReferences(ctx context.Context, streamType string, storeStreamID string, limit int64) iter.Seq2[StreamReference, error]
}
