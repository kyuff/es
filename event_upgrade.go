package es

import (
	"context"
	"iter"
	"sync"
)

type EventUpgrade interface {
	Upgrade(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error]
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

func (s *upgradeReader) Read(ctx context.Context, entityType string, entityID string, eventNumber int64) iter.Seq2[Event, error] {
	i := s.rd.Read(ctx, entityType, entityID, eventNumber)
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
	for entityType, upgradeList := range upgradeMap {
		upgrades[entityType] = combineEventUpgradesToWriter(w, upgradeList)
	}

	return &upgradeWriter{
		w:        w,
		upgrades: upgrades,
	}
}

func (u *upgradeWriter) Write(ctx context.Context, entityType string, events iter.Seq2[Event, error]) error {
	u.mux.RLock()
	defer u.mux.RUnlock()

	upgrades, ok := u.upgrades[entityType]
	if !ok {
		return u.w.Write(ctx, entityType, events)
	}

	return upgrades.Write(ctx, entityType, events)
}

func combineEventUpgradesToWriter(w Writer, upgrades []EventUpgrade) writerFunc {
	return func(ctx context.Context, entityType string, events iter.Seq2[Event, error]) error {
		for _, u := range upgrades {
			events = u.Upgrade(ctx, events)
		}

		return w.Write(ctx, entityType, events)
	}
}
