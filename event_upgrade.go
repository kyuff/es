package es

import (
	"context"
	"iter"
	"sync"
)

// EventUpgrade events into a new version.
// The events will all be from the same Stream and come in order by EventNumber.
//
// The EventUpgrade is used when reading a Stream and when an Event is published
// to subscribing Handlers. As a result, it is only the events that are in-flight
// that will go through the EventUpgrade.
// Example: If you Open a Stream from EventNumber 3, Event 1 and 2 will not be
// in the events sequence.
type EventUpgrade interface {
	Upgrade(ctx context.Context, events iter.Seq2[Event, error]) iter.Seq2[Event, error]
}

type EventUpgradeFunc func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error]

func (fn EventUpgradeFunc) Upgrade(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
	return fn(ctx, i)
}

func newUpgradeReader(rd Reader, upgrades []EventUpgrade) Reader {
	return &upgradeReader{
		rd:       rd,
		upgrades: upgrades,
	}
}

type upgradeReader struct {
	upgrades []EventUpgrade
	rd       Reader
}

func (s *upgradeReader) Read(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[Event, error] {
	i := s.rd.Read(ctx, streamType, streamID, eventNumber)
	for _, u := range s.upgrades {
		i = u.Upgrade(ctx, i)
	}

	return func(yield func(Event, error) bool) {
		for event, err := range i {
			if err != nil {
				return
			}

			if !yield(event, err) {
				return
			}
		}
	}
}

type upgradeWriter struct {
	mux      sync.RWMutex
	w        Writer
	upgrades map[string]Writer
}

func newUpgradeWriter(w Writer, upgradeMap map[string][]EventUpgrade) *upgradeWriter {
	var upgrades = make(map[string]Writer)
	for streamType, upgradeList := range upgradeMap {
		upgrades[streamType] = combineEventUpgradesToWriter(w, upgradeList)
	}

	return &upgradeWriter{
		w:        w,
		upgrades: upgrades,
	}
}

func (u *upgradeWriter) Write(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error {
	u.mux.RLock()
	defer u.mux.RUnlock()

	upgrades, ok := u.upgrades[streamType]
	if !ok {
		return u.w.Write(ctx, streamType, events)
	}

	return upgrades.Write(ctx, streamType, events)
}

func combineEventUpgradesToWriter(w Writer, upgrades []EventUpgrade) writerFunc {
	return func(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error {
		for _, u := range upgrades {
			events = u.Upgrade(ctx, events)
		}

		return w.Write(ctx, streamType, events)
	}
}
