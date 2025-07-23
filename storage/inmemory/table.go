package inmemory

import (
	"time"

	"github.com/kyuff/es"
	"github.com/kyuff/es/codecs"
)

type indexKey struct {
	StreamType  string
	StreamID    string
	EventNumber int64
}

type tableRow struct {
	StreamID      string
	StreamType    string
	EventNumber   int64
	StoreEventID  string
	StoreStreamID string
	EventTime     string
	Content       []byte
	ContentName   string
}

type table []tableRow

func newData(event es.Event, c *codecs.JSON) (tableRow, error) {
	eventData, err := c.Encode(event)
	if err != nil {
		return tableRow{}, err
	}

	return tableRow{
		StreamID:      event.StreamID,
		StreamType:    event.StreamType,
		EventNumber:   event.EventNumber,
		StoreEventID:  event.StoreEventID,
		StoreStreamID: event.StoreStreamID,
		EventTime:     event.EventTime.Format(time.RFC3339),
		Content:       eventData,
		ContentName:   event.Content.EventName(),
	}, nil
}

func (row tableRow) Event(c *codecs.JSON) (es.Event, error) {
	eventTime, err := time.Parse(time.RFC3339, row.EventTime)
	if err != nil {
		return es.Event{}, err
	}

	eventData, err := c.Decode(row.StreamType, row.ContentName, row.Content)
	if err != nil {
		return es.Event{}, err
	}

	return es.Event{
		StreamID:      row.StreamID,
		StreamType:    row.StreamType,
		EventNumber:   row.EventNumber,
		EventTime:     eventTime,
		Content:       eventData,
		StoreEventID:  row.StoreEventID,
		StoreStreamID: row.StoreStreamID,
	}, nil
}
