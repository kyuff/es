package es

import "context"

type Config struct {
	logger       Logger
	contentTypes map[string][]Content
	eventBus     EventBus
}

type Logger interface {
	InfofCtx(ctx context.Context, template string, args ...any)
	ErrorfCtx(ctx context.Context, template string, args ...any)
}

func defaultOptions() *Config {
	return applyOptions(&Config{
		contentTypes: make(map[string][]Content),
	},
		// add default options here
		WithNoopLogger(),
		WithEventBus(NewInMemoryEventBus()),
	)

}

func applyOptions(options *Config, opts ...Option) *Config {
	for _, opt := range opts {
		opt(options)
	}

	return options
}
