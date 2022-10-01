package awss3receiver

import (
	"strings"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	receiverKey     = "receiver"
	receiverTagKey  = tag.MustNewKey(receiverKey)
	receiverTagKeys = []tag.Key{receiverTagKey}

	// typeTagKey = tag.MustNewKey("type")

	mCwMetricsProcessed          = "cloudwatch_metrics_processed_count"
	mCwIntGaugeProcessed         = "cloudwatch_int_gauge_processed_count"
	mCwDoubleGaugeProcessed      = "cloudwatch_double_gauge_processed_count"
	mCwIntSumProcessed           = "cloudwatch_int_sum_processed_count"
	mCwDoubleSumProcessed        = "cloudwatch_double_sum_processed_count"
	mCwIntHistogramProcessed     = "cloudwatch_int_histogram_processed_count"
	mCwDoubleHistogramProcessed  = "cloudwatch_double_histogram_processed_count"
	mCwDoubleSummaryProcessed    = "cloudwatch_double_summary_processed_count"
	mCwTimestampDefaulted        = "cloudwatch_timestamp_default_count"
	mCwS3ConnectErrors           = "cloudwatch_s3_connect_error_count"
	mCwScanBucketErrors          = "cloudwatch_scan_bucket_error_count"
	mCwScanBucketEmptyErrors     = "cloudwatch_scan_bucket_empty_error_count"
	mCwReadBucketContentsErrors  = "cloudwatch_read_bucket_contents_error_count"
	mCwEmitMetricsErrors         = "cloudwatch_emit_metrics_error_count"
	mCwCheckpointNotFoundErrors  = "cloudwatch_checkpoint_not_found_error_count"
	mCwCheckpointPersistErrors   = "cloudwatch_checkpoint_persist_error_count"
	mCwDecodingErrors            = "cloudwatch_decoding_error_count"
	mCwStaleDataPoint            = "cloudwatch_stale_data_point_count"
	mCwMetricsProcessingDuration = "cloudwatch_metrics_processing_duration_ms"
	mCwMetricsTimeLag            = "cloudwatch_metrics_time_lag"

	// counters
	statCwMetricsProcessed         = stats.Int64(mCwMetricsProcessed, "Number of Cloudwatch metrics processed by awss3receiver", stats.UnitDimensionless)
	statCwIntGaugeProcessed        = stats.Int64(mCwIntGaugeProcessed, "Number of Cloudwatch IntGauge metrics processed by awss3receiver", stats.UnitDimensionless)
	statCwDoubleGaugeProcessed     = stats.Int64(mCwDoubleGaugeProcessed, "Number of Cloudwatch DoubleGauge metrics processed by awss3receiver", stats.UnitDimensionless)
	statCwIntSumProcessed          = stats.Int64(mCwIntSumProcessed, "Number of Cloudwatch IntSum metrics processed by awss3receiver", stats.UnitDimensionless)
	statCwDoubleSumProcessed       = stats.Int64(mCwDoubleSumProcessed, "Number of Cloudwatch DoubleSum metrics processed by awss3receiver", stats.UnitDimensionless)
	statCwIntHistogramProcessed    = stats.Int64(mCwIntHistogramProcessed, "Number of Cloudwatch IntHistogram metrics processed by awss3receiver", stats.UnitDimensionless)
	statCwDoubleHistogramProcessed = stats.Int64(mCwDoubleHistogramProcessed, "Number of Cloudwatch DoubleHistogram metrics processed by awss3receiver", stats.UnitDimensionless)
	statCwDoubleSummaryProcessed   = stats.Int64(mCwDoubleSummaryProcessed, "Number of Cloudwatch DoubleSummary metrics processed by awss3receiver", stats.UnitDimensionless)
	statCwTimestampDefaulted       = stats.Int64(mCwTimestampDefaulted, "Number of Cloudwatch metrics where timestamp was defaulted to current time", stats.UnitDimensionless)
	statCwS3ConnectErrors          = stats.Int64(mCwS3ConnectErrors, "Number of failed attempts to connect to AWS for S3", stats.UnitDimensionless)
	statCwScanBucketErrors         = stats.Int64(mCwScanBucketErrors, "Number of failed attempts to scan the S3 bucket objects", stats.UnitDimensionless)
	statCwScanBucketEmptyErrors    = stats.Int64(mCwScanBucketEmptyErrors, "Number of attempts to scan the S3 bucket objects and getting 0 items", stats.UnitDimensionless)
	statCwReadBucketContentsErrors = stats.Int64(mCwReadBucketContentsErrors, "Number of failed attempts to read an S3 bucket object", stats.UnitDimensionless)
	statCwEmitMetricsErrors        = stats.Int64(mCwEmitMetricsErrors, "Number of failed attempts to emit metrics to the pipeline", stats.UnitDimensionless)
	statCwCheckpointNotFoundErrors = stats.Int64(mCwCheckpointNotFoundErrors, "Number of failed attempts to find checkpoint key in list from s3 bucket scan", stats.UnitDimensionless)
	statCwCheckpointPersistErrors  = stats.Int64(mCwCheckpointPersistErrors, "Number of failed attempts to persist checkpoint key to s3 bucket", stats.UnitDimensionless)
	statCwDecodingErrors           = stats.Int64(mCwDecodingErrors, "Number of failed attempts to decode an s3 bucket item", stats.UnitDimensionless)
	statCwStaleDataPoint           = stats.Int64(mCwStaleDataPoint, "Number of Cloudwatch metric data points with stale timestamp filtered from the processing stream", stats.UnitDimensionless)

	// distributions
	statCwMetricsProcessingDurationMs = stats.Int64(mCwMetricsProcessingDuration, "Cloudwatch metrics processing duration in milliseconds", stats.UnitMilliseconds)
	statCwMetricsTimeLag              = stats.Int64(mCwMetricsTimeLag, "Cloudwatch metrics data point timestamp lag", stats.UnitSeconds)
)

func buildCustomMetricName(metricName string) string {
	parts := []string{receiverKey, typeStr, metricName}
	return strings.Join(parts, "/")
}

// metricViews returns the metrics views related to batching
func metricViews() []*view.View {

	countCwMetricsProcessed := &view.View{
		Name:        buildCustomMetricName(statCwMetricsProcessed.Name()),
		Measure:     statCwMetricsProcessed,
		Description: statCwMetricsProcessed.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwIntGaugeProcessed := &view.View{
		Name:        buildCustomMetricName(statCwIntGaugeProcessed.Name()),
		Measure:     statCwIntGaugeProcessed,
		Description: statCwIntGaugeProcessed.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwDoubleGaugeProcessed := &view.View{
		Name:        buildCustomMetricName(statCwDoubleGaugeProcessed.Name()),
		Measure:     statCwDoubleGaugeProcessed,
		Description: statCwDoubleGaugeProcessed.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwIntSumProcessed := &view.View{
		Name:        buildCustomMetricName(statCwIntSumProcessed.Name()),
		Measure:     statCwIntSumProcessed,
		Description: statCwIntSumProcessed.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwDoubleSumProcessed := &view.View{
		Name:        buildCustomMetricName(statCwDoubleSumProcessed.Name()),
		Measure:     statCwDoubleSumProcessed,
		Description: statCwDoubleSumProcessed.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwIntHistogramProcessed := &view.View{
		Name:        buildCustomMetricName(statCwIntHistogramProcessed.Name()),
		Measure:     statCwIntHistogramProcessed,
		Description: statCwIntHistogramProcessed.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwDoubleHistogramProcessed := &view.View{
		Name:        buildCustomMetricName(statCwDoubleHistogramProcessed.Name()),
		Measure:     statCwDoubleHistogramProcessed,
		Description: statCwDoubleHistogramProcessed.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwDoubleSummaryProcessed := &view.View{
		Name:        buildCustomMetricName(statCwDoubleSummaryProcessed.Name()),
		Measure:     statCwDoubleSummaryProcessed,
		Description: statCwDoubleSummaryProcessed.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwTimestampDefaulted := &view.View{
		Name:        buildCustomMetricName(statCwTimestampDefaulted.Name()),
		Measure:     statCwTimestampDefaulted,
		Description: statCwTimestampDefaulted.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwS3ConnectErrors := &view.View{
		Name:        buildCustomMetricName(statCwS3ConnectErrors.Name()),
		Measure:     statCwS3ConnectErrors,
		Description: statCwS3ConnectErrors.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwScanBucketErrors := &view.View{
		Name:        buildCustomMetricName(statCwScanBucketErrors.Name()),
		Measure:     statCwScanBucketErrors,
		Description: statCwScanBucketErrors.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwScanBucketEmptyErrors := &view.View{
		Name:        buildCustomMetricName(statCwScanBucketEmptyErrors.Name()),
		Measure:     statCwScanBucketEmptyErrors,
		Description: statCwScanBucketEmptyErrors.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countmCwReadBucketContentsErrors := &view.View{
		Name:        buildCustomMetricName(statCwReadBucketContentsErrors.Name()),
		Measure:     statCwReadBucketContentsErrors,
		Description: statCwReadBucketContentsErrors.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwEmitMetricsErrors := &view.View{
		Name:        buildCustomMetricName(statCwEmitMetricsErrors.Name()),
		Measure:     statCwEmitMetricsErrors,
		Description: statCwEmitMetricsErrors.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwCheckpointNotFoundErrors := &view.View{
		Name:        buildCustomMetricName(statCwCheckpointNotFoundErrors.Name()),
		Measure:     statCwCheckpointNotFoundErrors,
		Description: statCwCheckpointNotFoundErrors.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwCheckpointPersistErrors := &view.View{
		Name:        buildCustomMetricName(statCwCheckpointPersistErrors.Name()),
		Measure:     statCwCheckpointPersistErrors,
		Description: statCwCheckpointPersistErrors.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwDecodingErrors := &view.View{
		Name:        buildCustomMetricName(statCwDecodingErrors.Name()),
		Measure:     statCwDecodingErrors,
		Description: statCwDecodingErrors.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	countCwStaleDataPoint := &view.View{
		Name:        buildCustomMetricName(statCwStaleDataPoint.Name()),
		Measure:     statCwStaleDataPoint,
		Description: statCwStaleDataPoint.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Count(),
	}

	distributionCwMetricsProcessingDurationMsView := &view.View{
		Name:        buildCustomMetricName(statCwMetricsProcessingDurationMs.Name()),
		Measure:     statCwMetricsProcessingDurationMs,
		Description: statCwMetricsProcessingDurationMs.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Distribution(250, 500, 750, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000),
	}

	distributionCwMetricsTimeLag := &view.View{
		Name:        buildCustomMetricName(statCwMetricsTimeLag.Name()),
		Measure:     statCwMetricsTimeLag,
		Description: statCwMetricsTimeLag.Description(),
		TagKeys:     receiverTagKeys,
		Aggregation: view.Distribution(180, 240, 300, 360, 420, 480, 540, 600, 660, 720, 780, 840, 900),
	}

	return []*view.View{
		countCwMetricsProcessed,
		countCwIntGaugeProcessed,
		countCwDoubleGaugeProcessed,
		countCwIntSumProcessed,
		countCwDoubleSumProcessed,
		countCwIntHistogramProcessed,
		countCwDoubleHistogramProcessed,
		countCwDoubleSummaryProcessed,
		countCwTimestampDefaulted,
		countCwS3ConnectErrors,
		countCwScanBucketErrors,
		countCwScanBucketEmptyErrors,
		countmCwReadBucketContentsErrors,
		countCwEmitMetricsErrors,
		countCwCheckpointNotFoundErrors,
		countCwCheckpointPersistErrors,
		countCwDecodingErrors,
		countCwStaleDataPoint,
		distributionCwMetricsProcessingDurationMsView,
		distributionCwMetricsTimeLag,
	}
}
