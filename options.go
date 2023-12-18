package cacheinv

import (
	"github.com/QuangTung97/eventx"
)

type jobConfig struct {
	runnerOptions    []eventx.Option
	retryOptions     []eventx.RetryConsumerOption
	retentionOptions []eventx.RetentionOption
}

func newJobConfig(options []Option) jobConfig {
	conf := jobConfig{
		runnerOptions:    nil,
		retentionOptions: nil,
	}

	for _, fn := range options {
		fn(&conf)
	}

	return conf
}

// Option ...
type Option func(conf *jobConfig)

// WithRunnerOptions ...
func WithRunnerOptions(options ...eventx.Option) Option {
	return func(conf *jobConfig) {
		conf.runnerOptions = options
	}
}

// WithRetryConsumerOptions ...
func WithRetryConsumerOptions(options ...eventx.RetryConsumerOption) Option {
	return func(conf *jobConfig) {
		conf.retryOptions = options
	}
}

// WithRetentionOptions ...
func WithRetentionOptions(options ...eventx.RetentionOption) Option {
	return func(conf *jobConfig) {
		conf.retentionOptions = options
	}
}
