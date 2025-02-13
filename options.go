package es

import (
	"log/slog"

	"github.com/kyuff/es/internal/logger"
)

type Option func(*Config)

func WithEvents(entityType string, contentTypes []Content) Option {
	return func(o *Config) {
		for _, contentType := range contentTypes {
			o.contentTypes[entityType] = append(o.contentTypes[entityType], contentType)
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

func WithEventUpgrades(entityType string, upgrades ...EventUpgrade) Option {
	return func(o *Config) {
		o.eventUpgrades[entityType] = append(o.eventUpgrades[entityType], upgrades...)
	}
}

func WithMetadata(key string, value Metadata) Option {
	return func(o *Config) {

	}
}
