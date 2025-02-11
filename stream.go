package es

import "iter"

// Stream is a sequence of Events that in combination represent the state of an entity.
// The Stream can be written and read from, which enables applications to alter and get the state.
type Stream interface {
	// Project iterates over all events in the stream and calls the handler for each event.
	// The Stream will stop projecting if the handler returns an error.
	Project(handler Handler) error
	// All returns a iter.Seq2 of all events in the stream.
	// The returned iter.Seq2 will stop and return an error if there was an error
	// reading the events.
	// Calling this method twice will return the same iter.Seq2
	All() iter.Seq2[Event, error]
	// Write writes the given events to the stream.
	// The Events will be written in the order they are given and starting
	// at the most recent event number + 1.
	Write(events ...Content) error
	// Position returns the current position of the stream.
	Position() int64
	Close() error
}
