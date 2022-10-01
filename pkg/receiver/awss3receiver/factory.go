package awss3receiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
)

const (
	typeStr                   = "awss3"
	defaultCollectionInterval = 15 * time.Second
	// stability of the component
	stability = component.StabilityLevelAlpha
)

func NewFactory() component.ReceiverFactory {
	return component.NewReceiverFactory(
		typeStr,
		createDefaultConfig,
		component.WithMetricsReceiver(createMetricsReceiver, stability),
	)
}

func createDefaultConfig() config.Receiver {
	return &Config{
		ReceiverSettings:   config.NewReceiverSettings(config.NewComponentID(typeStr)),
		CollectionInterval: defaultCollectionInterval,
	}
}

func createMetricsReceiver(
	_ context.Context,
	params component.ReceiverCreateSettings,
	cfg config.Receiver,
	consumer consumer.Metrics,
) (component.MetricsReceiver, error) {
	c := cfg.(*Config)
	err := c.validate()
	if err != nil {
		return nil, err
	}
	return newAwsS3Receiver(params, *c, consumer)
}
