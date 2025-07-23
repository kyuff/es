package es

import (
	"context"
	"iter"
)

// EventBus is responsible for distributing an Event to all subscribing Handler's after
// they are written to the Storage.
type EventBus interface {
	// Write should write the events to all subscriptions
	Write(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error
	// Subscribe a Handler by it's subscriptionID
	Subscribe(ctx context.Context, streamType string, subscriberID string, handler Handler) error
	// GetSubscriberIDs returns a list of all subscription IDs for the streamType
	GetSubscriberIDs(ctx context.Context, streamType string) ([]string, error)
	// WriteTo call all Handler with subscriberIDs with the events
	WriteTo(ctx context.Context, streamType string, events iter.Seq2[Event, error], subscriberIDs ...string) error
	Close() error
}
