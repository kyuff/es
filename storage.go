package es

import (
	"context"
	"iter"
)

// Storage is the abstracts the persistence of a Store.
type Storage interface {
	// Read the events of of an entityType with the entityID from eventNumber
	Read(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[Event, error]
	// Write writes the events to the store.
	// All of the events must be written by sequence.
	// They should all be written or fully fail.
	Write(ctx context.Context, entityType string, events iter.Seq2[Event, error]) error
	// StartPublish should begin the process where newly written events are published to the Writer.
	// The publishing must be cancelled with the context
	StartPublish(ctx context.Context, w Writer) error
	// Register allows the Storage to Unmarshal multiple shapes of Content for an entityType.
	// It is considered an error if a Storage contains a shape of Content that have not been registered.
	Register(entityType string, types ...Content) error
	// GetEntityIDs returns a list of EntityIDs for the given entityType.
	// The returned list is ordered by the storeEntityID and limited in size by the limit.
	// The second return value is the next storeEntityID and works as a pagination token
	GetEntityIDs(ctx context.Context, entityType string, storeEntityID string, limit int64) ([]string, string, error)
}
