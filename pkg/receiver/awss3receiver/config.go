package awss3receiver

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/config"
	"go.uber.org/multierr"
)

type Config struct {
	config.ReceiverSettings `mapstructure:",squash"`
	BucketName              string            `mapstructure:"bucket_name"`
	BucketRegion            string            `mapstructure:"bucket_region"`
	BucketIOTimeout         time.Duration     `mapstructure:"bucket_io_timeout"`
	BucketEndpoint          string            `mapstructure:"bucket_endpoint"`
	GzipCompression         bool              `mapstructure:"gzip_compression"`
	CheckpointKey           string            `mapstructure:"checkpoint_key"`
	CollectionInterval      time.Duration     `mapstructure:"collection_interval"`
	ScanOffset              time.Duration     `mapstructure:"scan_offset"`
	ProcessingDelay         time.Duration     `mapstructure:"processing_delay"`
	FilterStaleDataPoints   bool              `mapstructure:"filter_stale_data_points"`
	StalenessThreshold      time.Duration     `mapstructure:"staleness_threshold"`
	Prefix                  string            `mapstructure:"prefix"`
	LogLevel                string            `mapstructure:"log_level"`
	ExtraLabels             map[string]string `mapstructure:"extra_labels"`
}

func (c *Config) validate() error {
	var errs error
	if c.CollectionInterval <= 0 {
		// Default to 15 seconds
		c.CollectionInterval, _ = time.ParseDuration("15s")
	}
	if c.BucketIOTimeout <= 0 {
		// Default to 30 seconds
		c.BucketIOTimeout, _ = time.ParseDuration("30s")
	}
	if c.ScanOffset <= 0 {
		// Default to 5 minute scan window when configuration not provided
		c.ScanOffset, _ = time.ParseDuration("5m")
	}
	if c.ProcessingDelay <= 0 {
		// Default to 2 minute delay when configuration not provided
		c.ProcessingDelay, _ = time.ParseDuration("2m")
	}
	if c.StalenessThreshold <= 0 {
		// Default to 15 minute delay when configuration not provided
		c.StalenessThreshold, _ = time.ParseDuration("15m")
	}
	if c.CheckpointKey == "" {
		// Default to "s3://BucketName/awss3receiver_checkpoint.txt"
		c.CheckpointKey = "awss3receiver_checkpoint.txt"
	}
	if c.ScanOffset < (c.CollectionInterval + c.ProcessingDelay) {
		errs = multierr.Append(errs, fmt.Errorf("scan_offset should be greater than or equal to the sum of collection_interval and processing_delay"))
	}

	return errs
}
