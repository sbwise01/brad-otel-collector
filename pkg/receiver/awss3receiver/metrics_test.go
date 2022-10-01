package awss3receiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAwsS3ReceiverMetrics(t *testing.T) {
	viewNames := []string{
		mCwMetricsProcessed,
		mCwIntGaugeProcessed,
		mCwDoubleGaugeProcessed,
		mCwIntSumProcessed,
		mCwDoubleSumProcessed,
		mCwIntHistogramProcessed,
		mCwDoubleHistogramProcessed,
		mCwDoubleSummaryProcessed,
		mCwTimestampDefaulted,
		mCwS3ConnectErrors,
		mCwScanBucketErrors,
		mCwScanBucketEmptyErrors,
		mCwReadBucketContentsErrors,
		mCwEmitMetricsErrors,
		mCwCheckpointNotFoundErrors,
		mCwCheckpointPersistErrors,
		mCwDecodingErrors,
		mCwStaleDataPoint,
		mCwMetricsProcessingDuration,
		mCwMetricsTimeLag,
	}
	views := metricViews()
	for i, viewName := range viewNames {
		assert.Equal(t, "receiver/awss3/"+viewName, views[i].Name)
	}
}
