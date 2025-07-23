package es

import (
	"log/slog"

	"github.com/kyuff/es/internal/logger"
)

type Option func(*Config)

func WithEvents(streamType string, contentTypes []Content) Option {
	return func(o *Config) {
		for _, contentType := range contentTypes {
			o.contentTypes[streamType] = append(o.contentTypes[streamType], contentType)
		}
	}
}

func WithLogger(logger Logger) Option {
	return func(opt *Config) {
		opt.logger = logger
	}
}
func WithNoopLogger() Option {
	return WithLogger(logger.Noop{})
}

func WithDefaultSlog() Option {
	return WithSlog(slog.Default())
}

func WithSlog(log *slog.Logger) Option {
	return WithLogger(
		logger.NewSlog(log),
	)
}

func WithEventBus(bus EventBus) Option {
	return func(opt *Config) {
		opt.eventBus = bus
	}
}

func WithEventUpgrades(streamType string, upgrades ...EventUpgrade) Option {
	return func(o *Config) {
		o.eventUpgrades[streamType] = append(o.eventUpgrades[streamType], upgrades...)
	}
}
