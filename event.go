package es

import "time"

// Content is the application specific data model used in an Event.
type Content interface {
	EventName() string
}

// Event is a combination of the metadata and content of a business event in the system.
// It is part of a Stream that makes up the current state of a business entity.
type Event struct {
	// StreamID is the ID of the stream the event belongs to.
	StreamID string
	// StreamType is the type of the stream the event belongs to.
	StreamType string
	// EventNumber is the number of the event in the stream.
	EventNumber int64
	// EventTime is the time the event was recorded in the Store
	EventTime time.Time
	// Content is the actual content of the event. Expected to be a struct defined
	// by the application.
	Content Content
	// StoreEventID is the ID of the event assigned by the Store
	// The StoreEventID is a UUIDv7 with the underlying time matching the EventTime
	StoreEventID string
	// StoreStreamID is the ID of the stream assigned by the Store
	// The StoreStreamID is a UUIDv7 with the underlying time matching the EventTime
	// of the first event in the stream.
	StoreStreamID string
}
