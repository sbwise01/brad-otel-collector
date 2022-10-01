package awss3receiver

import (
	"testing"
	"time"

	commonv1 "github.com/open-telemetry/opentelemetry-proto/gen/go/common/v1"
	metricsv1 "github.com/open-telemetry/opentelemetry-proto/gen/go/metrics/v1"
	resourcev1 "github.com/open-telemetry/opentelemetry-proto/gen/go/resource/v1"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// Global test data
var testDoubleValue = float64(1586)
var testIntValue = int64(1587)
var testQuantile = float64(1588)
var testCount = uint64(1)
var testBucketCounts = []uint64{1, 2}
var testExplicitBounds = []float64{1, 2}
var testSpanId = []byte{65, 66, 67, 68, 69, 70, 71, 72}
var testTraceId = []byte{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45}
var testTimestamp = uint64(time.Now().UnixNano())
var testStartTime = uint64(time.Now().UnixNano())
var testResourceAttibuteKey = "cloud.account.id"
var testResourceAttibuteValue = "123456789"
var testLabels = []*commonv1.StringKeyValue{
	{Key: "testlabel1", Value: "testvalue1"},
	{Key: "testlabel2", Value: "testvalue2"},
}
var s3Receiver = &awsS3Receiver{
	config: &Config{ExtraLabels: map[string]string{
		"extralabel1": "extravalue1",
		"extralabel2": "extravalue2"},
		StalenessThreshold: parseDuration("5m"),
	},
	logger: zap.NewExample(),
}

func setExpectedLabels(am pcommon.Map) {
	am.InsertString("testlabel1", "testvalue1")
	am.InsertString("testlabel2", "testvalue2")
	am.InsertString("extralabel1", "extravalue1")
	am.InsertString("extralabel2", "extravalue2")
}

func TestProcessDataPointDoubleGauge(t *testing.T) {
	var srcDataPoint = &metricsv1.DoubleDataPoint{
		TimeUnixNano:      testTimestamp,
		StartTimeUnixNano: testStartTime,
		Labels:            testLabels,
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeGauge)
	destDataPoint := destMetrics.Gauge().DataPoints().AppendEmpty()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeGauge)
	expected := expectedMetrics.Gauge().DataPoints().AppendEmpty()
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processDataPoint(srcDataPoint, destDataPoint)
	expected.Attributes().Sort()
	destDataPoint.Attributes().Sort()
	assert.Equal(t, expected, destDataPoint)
}

func TestProcessDataPointIntGauge(t *testing.T) {
	var srcDataPoint = &metricsv1.IntDataPoint{
		TimeUnixNano:      testTimestamp,
		StartTimeUnixNano: testStartTime,
		Labels:            testLabels,
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeGauge)
	destDataPoint := destMetrics.Gauge().DataPoints().AppendEmpty()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeGauge)
	expected := expectedMetrics.Gauge().DataPoints().AppendEmpty()
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processDataPoint(srcDataPoint, destDataPoint)
	expected.Attributes().Sort()
	destDataPoint.Attributes().Sort()
	assert.Equal(t, expected, destDataPoint)
}

func TestProcessDataPointDoubleSum(t *testing.T) {
	var srcDataPoint = &metricsv1.DoubleDataPoint{
		TimeUnixNano:      testTimestamp,
		StartTimeUnixNano: testStartTime,
		Labels:            testLabels,
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeSum)
	destDataPoint := destMetrics.Sum().DataPoints().AppendEmpty()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeSum)
	expected := expectedMetrics.Sum().DataPoints().AppendEmpty()
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processDataPoint(srcDataPoint, destDataPoint)
	expected.Attributes().Sort()
	destDataPoint.Attributes().Sort()
	assert.Equal(t, expected, destDataPoint)
}

func TestProcessDataPointIntSum(t *testing.T) {
	var srcDataPoint = &metricsv1.IntDataPoint{
		TimeUnixNano:      testTimestamp,
		StartTimeUnixNano: testStartTime,
		Labels:            testLabels,
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeSum)
	destDataPoint := destMetrics.Sum().DataPoints().AppendEmpty()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeSum)
	expected := expectedMetrics.Sum().DataPoints().AppendEmpty()
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processDataPoint(srcDataPoint, destDataPoint)
	expected.Attributes().Sort()
	destDataPoint.Attributes().Sort()
	assert.Equal(t, expected, destDataPoint)
}

func TestProcessDataPointDoubleHistogram(t *testing.T) {
	var srcDataPoint = &metricsv1.DoubleHistogramDataPoint{
		TimeUnixNano:      testTimestamp,
		StartTimeUnixNano: testStartTime,
		Labels:            testLabels,
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
	destMetrics.Histogram().DataPoints().AppendEmpty()
	destDataPoint := destMetrics.Histogram().DataPoints().AppendEmpty()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
	expected := expectedMetrics.Histogram().DataPoints().AppendEmpty()
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processDataPoint(srcDataPoint, destDataPoint)
	expected.Attributes().Sort()
	destDataPoint.Attributes().Sort()
	assert.Equal(t, expected, destDataPoint)
}

func TestProcessDataPointIntHistogram(t *testing.T) {
	var srcDataPoint = &metricsv1.IntHistogramDataPoint{
		TimeUnixNano:      testTimestamp,
		StartTimeUnixNano: testStartTime,
		Labels:            testLabels,
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
	destMetrics.Histogram().DataPoints().AppendEmpty()
	destDataPoint := destMetrics.Histogram().DataPoints().AppendEmpty()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
	expected := expectedMetrics.Histogram().DataPoints().AppendEmpty()
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processDataPoint(srcDataPoint, destDataPoint)
	expected.Attributes().Sort()
	destDataPoint.Attributes().Sort()
	assert.Equal(t, expected, destDataPoint)
}

func TestProcessDataPointDoubleSummary(t *testing.T) {
	var srcDataPoint = &metricsv1.DoubleSummaryDataPoint{
		TimeUnixNano:      testTimestamp,
		StartTimeUnixNano: testStartTime,
		Labels:            testLabels,
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeSummary)
	destDataPoint := destMetrics.Summary().DataPoints().AppendEmpty()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeSummary)
	expected := expectedMetrics.Summary().DataPoints().AppendEmpty()
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processDataPoint(srcDataPoint, destDataPoint)
	expected.Attributes().Sort()
	destDataPoint.Attributes().Sort()
	assert.Equal(t, expected, destDataPoint)
}

func TestProcessNumberDoubleGauge(t *testing.T) {
	var srcMetric = &metricsv1.DoubleGauge{
		DataPoints: []*metricsv1.DoubleDataPoint{
			{
				Value:             testDoubleValue,
				TimeUnixNano:      testTimestamp,
				StartTimeUnixNano: testStartTime,
				Labels:            testLabels,
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeGauge)
	destDataPoints := destMetrics.Gauge().DataPoints()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeGauge)
	expectedDataPoints := expectedMetrics.Gauge().DataPoints()
	expected := expectedDataPoints.AppendEmpty()
	expected.SetDoubleVal(testDoubleValue)
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processNumber(srcMetric, destDataPoints)
	expected.Attributes().Sort()
	for i := 0; i < destDataPoints.Len(); i++ {
		destDataPoints.At(i).Attributes().Sort()
	}
	assert.Equal(t, expectedDataPoints, destDataPoints)
}

func TestProcessNumberIntGauge(t *testing.T) {
	var srcMetric = &metricsv1.IntGauge{
		DataPoints: []*metricsv1.IntDataPoint{
			{
				Value:             testIntValue,
				TimeUnixNano:      testTimestamp,
				StartTimeUnixNano: testStartTime,
				Labels:            testLabels,
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeGauge)
	destDataPoints := destMetrics.Gauge().DataPoints()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeGauge)
	expectedDataPoints := expectedMetrics.Gauge().DataPoints()
	expected := expectedDataPoints.AppendEmpty()
	expected.SetIntVal(testIntValue)
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processNumber(srcMetric, destDataPoints)
	expected.Attributes().Sort()
	for i := 0; i < destDataPoints.Len(); i++ {
		destDataPoints.At(i).Attributes().Sort()
	}
	assert.Equal(t, expectedDataPoints, destDataPoints)
}

func TestProcessNumberDoubleSum(t *testing.T) {
	var srcMetric = &metricsv1.DoubleSum{
		DataPoints: []*metricsv1.DoubleDataPoint{
			{
				Value:             testDoubleValue,
				TimeUnixNano:      testTimestamp,
				StartTimeUnixNano: testStartTime,
				Labels:            testLabels,
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeSum)
	destDataPoints := destMetrics.Sum().DataPoints()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeSum)
	expectedDataPoints := expectedMetrics.Sum().DataPoints()
	expected := expectedDataPoints.AppendEmpty()
	expected.SetDoubleVal(testDoubleValue)
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processNumber(srcMetric, destDataPoints)
	expected.Attributes().Sort()
	for i := 0; i < destDataPoints.Len(); i++ {
		destDataPoints.At(i).Attributes().Sort()
	}
	assert.Equal(t, expectedDataPoints, destDataPoints)
}

func TestProcessNumberIntSum(t *testing.T) {
	var srcMetric = &metricsv1.IntSum{
		DataPoints: []*metricsv1.IntDataPoint{
			{
				Value:             testIntValue,
				TimeUnixNano:      testTimestamp,
				StartTimeUnixNano: testStartTime,
				Labels:            testLabels,
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeSum)
	destDataPoints := destMetrics.Sum().DataPoints()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeSum)
	expectedDataPoints := expectedMetrics.Sum().DataPoints()
	expected := expectedDataPoints.AppendEmpty()
	expected.SetIntVal(testIntValue)
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())

	s3Receiver.processNumber(srcMetric, destDataPoints)
	expected.Attributes().Sort()
	for i := 0; i < destDataPoints.Len(); i++ {
		destDataPoints.At(i).Attributes().Sort()
	}
	assert.Equal(t, expectedDataPoints, destDataPoints)
}

func TestProcessHistogramDoubleHistogram(t *testing.T) {
	var srcMetric = &metricsv1.DoubleHistogram{
		DataPoints: []*metricsv1.DoubleHistogramDataPoint{
			{
				Count:          testCount,
				Sum:            testDoubleValue,
				BucketCounts:   testBucketCounts,
				ExplicitBounds: testExplicitBounds,
				Exemplars: []*metricsv1.DoubleExemplar{
					{
						TimeUnixNano: testTimestamp,
						Value:        testDoubleValue,
						SpanId:       testSpanId,
						TraceId:      testTraceId,
					},
				},
				TimeUnixNano:      testTimestamp,
				StartTimeUnixNano: testStartTime,
				Labels:            testLabels,
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
	destDataPoints := destMetrics.Histogram().DataPoints()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
	expectedDataPoints := expectedMetrics.Histogram().DataPoints()
	expected := expectedDataPoints.AppendEmpty()
	expected.SetCount(testCount)
	expected.SetSum(testDoubleValue)
	expected.SetBucketCounts(pcommon.NewImmutableUInt64Slice(testBucketCounts))
	expected.SetExplicitBounds(pcommon.NewImmutableFloat64Slice(testExplicitBounds))
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())
	expectedExemplar := expected.Exemplars().AppendEmpty()
	expectedExemplar.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedExemplar.SetDoubleVal(testDoubleValue)
	var spanId [8]byte
	copy(spanId[:], testSpanId)
	expectedExemplar.SetSpanID(pcommon.NewSpanID(spanId))
	var traceId [16]byte
	copy(traceId[:], testTraceId)
	expectedExemplar.SetTraceID(pcommon.NewTraceID(traceId))

	s3Receiver.processHistogram(srcMetric, destDataPoints)
	expected.Attributes().Sort()
	for i := 0; i < destDataPoints.Len(); i++ {
		destDataPoints.At(i).Attributes().Sort()
	}
	assert.Equal(t, expectedDataPoints, destDataPoints)
}

func TestProcessHistogramIntHistogram(t *testing.T) {
	var srcMetric = &metricsv1.IntHistogram{
		DataPoints: []*metricsv1.IntHistogramDataPoint{
			{
				Count:          testCount,
				Sum:            testIntValue,
				BucketCounts:   testBucketCounts,
				ExplicitBounds: testExplicitBounds,
				Exemplars: []*metricsv1.IntExemplar{
					{
						TimeUnixNano: testTimestamp,
						Value:        testIntValue,
						SpanId:       testSpanId,
						TraceId:      testTraceId,
					},
				},
				TimeUnixNano:      testTimestamp,
				StartTimeUnixNano: testStartTime,
				Labels:            testLabels,
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
	destDataPoints := destMetrics.Histogram().DataPoints()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
	expectedDataPoints := expectedMetrics.Histogram().DataPoints()
	expected := expectedDataPoints.AppendEmpty()
	expected.SetCount(testCount)
	expected.SetSum(float64(testIntValue))
	expected.SetBucketCounts(pcommon.NewImmutableUInt64Slice(testBucketCounts))
	expected.SetExplicitBounds(pcommon.NewImmutableFloat64Slice(testExplicitBounds))
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())
	expectedExemplar := expected.Exemplars().AppendEmpty()
	expectedExemplar.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedExemplar.SetIntVal(testIntValue)
	var spanId [8]byte
	copy(spanId[:], testSpanId)
	expectedExemplar.SetSpanID(pcommon.NewSpanID(spanId))
	var traceId [16]byte
	copy(traceId[:], testTraceId)
	expectedExemplar.SetTraceID(pcommon.NewTraceID(traceId))

	s3Receiver.processHistogram(srcMetric, destDataPoints)
	expected.Attributes().Sort()
	for i := 0; i < destDataPoints.Len(); i++ {
		destDataPoints.At(i).Attributes().Sort()
	}
	assert.Equal(t, expectedDataPoints, destDataPoints)
}

func TestProcessSummaryDoubleSummary(t *testing.T) {
	var srcMetric = &metricsv1.DoubleSummary{
		DataPoints: []*metricsv1.DoubleSummaryDataPoint{
			{
				Count: testCount,
				Sum:   testDoubleValue,
				QuantileValues: []*metricsv1.DoubleSummaryDataPoint_ValueAtQuantile{
					{
						Quantile: testQuantile,
						Value:    testDoubleValue,
					},
				},
				TimeUnixNano:      testTimestamp,
				StartTimeUnixNano: testStartTime,
				Labels:            testLabels,
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	destMetrics.SetDataType(pmetric.MetricDataTypeSummary)
	destDataPoints := destMetrics.Summary().DataPoints()

	expectedMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetrics.SetDataType(pmetric.MetricDataTypeSummary)
	expectedDataPoints := expectedMetrics.Summary().DataPoints()
	expected := expectedDataPoints.AppendEmpty()
	expected.SetCount(testCount)
	expected.SetSum(testDoubleValue)
	expected.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expected.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expected.Attributes())
	expectedQuantileValues := expected.QuantileValues().AppendEmpty()
	expectedQuantileValues.SetQuantile(testQuantile)
	expectedQuantileValues.SetValue(testDoubleValue)

	s3Receiver.processSummary(srcMetric, destDataPoints)
	expected.Attributes().Sort()
	for i := 0; i < destDataPoints.Len(); i++ {
		destDataPoints.At(i).Attributes().Sort()
	}
	assert.Equal(t, expectedDataPoints, destDataPoints)
}

func TestProcessMetricDoubleGauge(t *testing.T) {
	var srcMetrics = &metricsv1.Metric{
		Data: &metricsv1.Metric_DoubleGauge{
			DoubleGauge: &metricsv1.DoubleGauge{
				DataPoints: []*metricsv1.DoubleDataPoint{
					{
						Value:             testDoubleValue,
						TimeUnixNano:      testTimestamp,
						StartTimeUnixNano: testStartTime,
						Labels:            testLabels,
					},
				},
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()

	expected := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expected.SetDataType(pmetric.MetricDataTypeGauge)
	expectedDataPoints := expected.Gauge().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetDoubleVal(testDoubleValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())

	s3Receiver.processMetric(srcMetrics, destMetrics)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetrics.Gauge().DataPoints().Len(); i++ {
		destMetrics.Gauge().DataPoints().At(i).Attributes().Sort()
	}
	assert.Equal(t, expected, destMetrics)
}

func TestProcessMetricIntGauge(t *testing.T) {
	var srcMetrics = &metricsv1.Metric{
		Data: &metricsv1.Metric_IntGauge{
			IntGauge: &metricsv1.IntGauge{
				DataPoints: []*metricsv1.IntDataPoint{
					{
						Value:             testIntValue,
						TimeUnixNano:      testTimestamp,
						StartTimeUnixNano: testStartTime,
						Labels:            testLabels,
					},
				},
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()

	expected := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expected.SetDataType(pmetric.MetricDataTypeGauge)
	expectedDataPoints := expected.Gauge().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetIntVal(testIntValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())

	s3Receiver.processMetric(srcMetrics, destMetrics)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetrics.Gauge().DataPoints().Len(); i++ {
		destMetrics.Gauge().DataPoints().At(i).Attributes().Sort()
	}
	assert.Equal(t, expected, destMetrics)
}

func TestProcessMetricDoubleSum(t *testing.T) {
	var srcMetrics = &metricsv1.Metric{
		Data: &metricsv1.Metric_DoubleSum{
			DoubleSum: &metricsv1.DoubleSum{
				DataPoints: []*metricsv1.DoubleDataPoint{
					{
						Value:             testDoubleValue,
						TimeUnixNano:      testTimestamp,
						StartTimeUnixNano: testStartTime,
						Labels:            testLabels,
					},
				},
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()

	expected := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expected.SetDataType(pmetric.MetricDataTypeSum)
	expectedDataPoints := expected.Sum().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetDoubleVal(testDoubleValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())

	s3Receiver.processMetric(srcMetrics, destMetrics)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetrics.Sum().DataPoints().Len(); i++ {
		destMetrics.Sum().DataPoints().At(i).Attributes().Sort()
	}
	assert.Equal(t, expected, destMetrics)
}

func TestProcessMetricIntSum(t *testing.T) {
	var srcMetrics = &metricsv1.Metric{
		Data: &metricsv1.Metric_IntSum{
			IntSum: &metricsv1.IntSum{
				DataPoints: []*metricsv1.IntDataPoint{
					{
						Value:             testIntValue,
						TimeUnixNano:      testTimestamp,
						StartTimeUnixNano: testStartTime,
						Labels:            testLabels,
					},
				},
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()

	expected := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expected.SetDataType(pmetric.MetricDataTypeSum)
	expectedDataPoints := expected.Sum().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetIntVal(testIntValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())

	s3Receiver.processMetric(srcMetrics, destMetrics)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetrics.Sum().DataPoints().Len(); i++ {
		destMetrics.Sum().DataPoints().At(i).Attributes().Sort()
	}
	assert.Equal(t, expected, destMetrics)
}

func TestProcessMetricDoubleHistogram(t *testing.T) {
	var srcMetrics = &metricsv1.Metric{
		Data: &metricsv1.Metric_DoubleHistogram{
			DoubleHistogram: &metricsv1.DoubleHistogram{
				DataPoints: []*metricsv1.DoubleHistogramDataPoint{
					{
						Count:          testCount,
						Sum:            testDoubleValue,
						BucketCounts:   testBucketCounts,
						ExplicitBounds: testExplicitBounds,
						Exemplars: []*metricsv1.DoubleExemplar{
							{
								TimeUnixNano: testTimestamp,
								Value:        testDoubleValue,
								SpanId:       testSpanId,
								TraceId:      testTraceId,
							},
						},
						TimeUnixNano:      testTimestamp,
						StartTimeUnixNano: testStartTime,
						Labels:            testLabels,
					},
				},
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()

	expected := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expected.SetDataType(pmetric.MetricDataTypeHistogram)
	expectedDataPoints := expected.Histogram().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetCount(testCount)
	expectedDataPoint.SetSum(testDoubleValue)
	expectedDataPoint.SetBucketCounts(pcommon.NewImmutableUInt64Slice(testBucketCounts))
	expectedDataPoint.SetExplicitBounds(pcommon.NewImmutableFloat64Slice(testExplicitBounds))
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())
	expectedExemplar := expectedDataPoint.Exemplars().AppendEmpty()
	expectedExemplar.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedExemplar.SetDoubleVal(testDoubleValue)
	var spanId [8]byte
	copy(spanId[:], testSpanId)
	expectedExemplar.SetSpanID(pcommon.NewSpanID(spanId))
	var traceId [16]byte
	copy(traceId[:], testTraceId)
	expectedExemplar.SetTraceID(pcommon.NewTraceID(traceId))

	s3Receiver.processMetric(srcMetrics, destMetrics)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetrics.Histogram().DataPoints().Len(); i++ {
		destMetrics.Histogram().DataPoints().At(i).Attributes().Sort()
	}
	assert.Equal(t, expected, destMetrics)
}

func TestProcessMetricIntHistogram(t *testing.T) {
	var srcMetrics = &metricsv1.Metric{
		Data: &metricsv1.Metric_IntHistogram{
			IntHistogram: &metricsv1.IntHistogram{
				DataPoints: []*metricsv1.IntHistogramDataPoint{
					{
						Count:          testCount,
						Sum:            testIntValue,
						BucketCounts:   testBucketCounts,
						ExplicitBounds: testExplicitBounds,
						Exemplars: []*metricsv1.IntExemplar{
							{
								TimeUnixNano: testTimestamp,
								Value:        testIntValue,
								SpanId:       testSpanId,
								TraceId:      testTraceId,
							},
						},
						TimeUnixNano:      testTimestamp,
						StartTimeUnixNano: testStartTime,
						Labels:            testLabels,
					},
				},
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()

	expected := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expected.SetDataType(pmetric.MetricDataTypeHistogram)
	expectedDataPoints := expected.Histogram().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetCount(testCount)
	expectedDataPoint.SetSum(float64(testIntValue))
	expectedDataPoint.SetBucketCounts(pcommon.NewImmutableUInt64Slice(testBucketCounts))
	expectedDataPoint.SetExplicitBounds(pcommon.NewImmutableFloat64Slice(testExplicitBounds))
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())
	expectedExemplar := expectedDataPoint.Exemplars().AppendEmpty()
	expectedExemplar.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedExemplar.SetIntVal(testIntValue)
	var spanId [8]byte
	copy(spanId[:], testSpanId)
	expectedExemplar.SetSpanID(pcommon.NewSpanID(spanId))
	var traceId [16]byte
	copy(traceId[:], testTraceId)
	expectedExemplar.SetTraceID(pcommon.NewTraceID(traceId))

	s3Receiver.processMetric(srcMetrics, destMetrics)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetrics.Histogram().DataPoints().Len(); i++ {
		destMetrics.Histogram().DataPoints().At(i).Attributes().Sort()
	}
	assert.Equal(t, expected, destMetrics)
}

func TestProcessMetricDoubleSummary(t *testing.T) {
	var srcMetrics = &metricsv1.Metric{
		Data: &metricsv1.Metric_DoubleSummary{
			DoubleSummary: &metricsv1.DoubleSummary{
				DataPoints: []*metricsv1.DoubleSummaryDataPoint{
					{
						Count: testCount,
						Sum:   testDoubleValue,
						QuantileValues: []*metricsv1.DoubleSummaryDataPoint_ValueAtQuantile{
							{
								Quantile: testQuantile,
								Value:    testDoubleValue,
							},
						},
						TimeUnixNano:      testTimestamp,
						StartTimeUnixNano: testStartTime,
						Labels:            testLabels,
					},
				},
			},
		},
	}

	destMetrics := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()

	expected := pmetric.NewMetrics().
		ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expected.SetDataType(pmetric.MetricDataTypeSummary)
	expectedDataPoints := expected.Summary().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetCount(testCount)
	expectedDataPoint.SetSum(testDoubleValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())
	expectedQuantileValues := expectedDataPoint.QuantileValues().AppendEmpty()
	expectedQuantileValues.SetQuantile(testQuantile)
	expectedQuantileValues.SetValue(testDoubleValue)

	s3Receiver.processMetric(srcMetrics, destMetrics)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetrics.Summary().DataPoints().Len(); i++ {
		destMetrics.Summary().DataPoints().At(i).Attributes().Sort()
	}
	assert.Equal(t, expected, destMetrics)
}

func TestProcessResourceMetricDoubleGauge(t *testing.T) {
	var srcResourceMetric = &metricsv1.ResourceMetrics{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{
					Key: testResourceAttibuteKey,
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{
							StringValue: testResourceAttibuteValue,
						},
					},
				},
			},
		},
		InstrumentationLibraryMetrics: []*metricsv1.InstrumentationLibraryMetrics{
			{
				Metrics: []*metricsv1.Metric{
					{
						Data: &metricsv1.Metric_DoubleGauge{
							DoubleGauge: &metricsv1.DoubleGauge{
								DataPoints: []*metricsv1.DoubleDataPoint{
									{
										Value:             testDoubleValue,
										TimeUnixNano:      testTimestamp,
										StartTimeUnixNano: testStartTime,
										Labels:            testLabels,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	destMetricsCollection := pmetric.NewMetrics()

	expected := pmetric.NewMetrics()
	expectedResourceMetric := expected.ResourceMetrics().AppendEmpty()
	expectedResourceMetric.Resource().Attributes().InsertString(testResourceAttibuteKey, testResourceAttibuteValue)
	expectedMetric := expectedResourceMetric.
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetric.SetDataType(pmetric.MetricDataTypeGauge)
	expectedDataPoints := expectedMetric.Gauge().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetDoubleVal(testDoubleValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())

	s3Receiver.processResourceMetric(destMetricsCollection, srcResourceMetric)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetricsCollection.ResourceMetrics().Len(); i++ {
		rm := destMetricsCollection.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
					m.Gauge().DataPoints().At(l).Attributes().Sort()
				}
			}
		}
	}
	assert.Equal(t, expected, destMetricsCollection)
}

func TestProcessResourceMetricIntGauge(t *testing.T) {
	var srcResourceMetric = &metricsv1.ResourceMetrics{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{
					Key: testResourceAttibuteKey,
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{
							StringValue: testResourceAttibuteValue,
						},
					},
				},
			},
		},
		InstrumentationLibraryMetrics: []*metricsv1.InstrumentationLibraryMetrics{
			{
				Metrics: []*metricsv1.Metric{
					{
						Data: &metricsv1.Metric_IntGauge{
							IntGauge: &metricsv1.IntGauge{
								DataPoints: []*metricsv1.IntDataPoint{
									{
										Value:             testIntValue,
										TimeUnixNano:      testTimestamp,
										StartTimeUnixNano: testStartTime,
										Labels:            testLabels,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	destMetricsCollection := pmetric.NewMetrics()

	expected := pmetric.NewMetrics()
	expectedResourceMetric := expected.ResourceMetrics().AppendEmpty()
	expectedResourceMetric.Resource().Attributes().InsertString(testResourceAttibuteKey, testResourceAttibuteValue)
	expectedMetric := expectedResourceMetric.
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetric.SetDataType(pmetric.MetricDataTypeGauge)
	expectedDataPoints := expectedMetric.Gauge().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetIntVal(testIntValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())

	s3Receiver.processResourceMetric(destMetricsCollection, srcResourceMetric)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetricsCollection.ResourceMetrics().Len(); i++ {
		rm := destMetricsCollection.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
					m.Gauge().DataPoints().At(l).Attributes().Sort()
				}
			}
		}
	}
	assert.Equal(t, expected, destMetricsCollection)
}

func TestProcessResourceMetricDoubleSum(t *testing.T) {
	var srcResourceMetric = &metricsv1.ResourceMetrics{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{
					Key: testResourceAttibuteKey,
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{
							StringValue: testResourceAttibuteValue,
						},
					},
				},
			},
		},
		InstrumentationLibraryMetrics: []*metricsv1.InstrumentationLibraryMetrics{
			{
				Metrics: []*metricsv1.Metric{
					{
						Data: &metricsv1.Metric_DoubleSum{
							DoubleSum: &metricsv1.DoubleSum{
								DataPoints: []*metricsv1.DoubleDataPoint{
									{
										Value:             testDoubleValue,
										TimeUnixNano:      testTimestamp,
										StartTimeUnixNano: testStartTime,
										Labels:            testLabels,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	destMetricsCollection := pmetric.NewMetrics()

	expected := pmetric.NewMetrics()
	expectedResourceMetric := expected.ResourceMetrics().AppendEmpty()
	expectedResourceMetric.Resource().Attributes().InsertString(testResourceAttibuteKey, testResourceAttibuteValue)
	expectedMetric := expectedResourceMetric.
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetric.SetDataType(pmetric.MetricDataTypeSum)
	expectedDataPoints := expectedMetric.Sum().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetDoubleVal(testDoubleValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())

	s3Receiver.processResourceMetric(destMetricsCollection, srcResourceMetric)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetricsCollection.ResourceMetrics().Len(); i++ {
		rm := destMetricsCollection.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				for l := 0; l < m.Sum().DataPoints().Len(); l++ {
					m.Sum().DataPoints().At(l).Attributes().Sort()
				}
			}
		}
	}
	assert.Equal(t, expected, destMetricsCollection)
}

func TestProcessResourceMetricIntSum(t *testing.T) {
	var srcResourceMetric = &metricsv1.ResourceMetrics{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{
					Key: testResourceAttibuteKey,
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{
							StringValue: testResourceAttibuteValue,
						},
					},
				},
			},
		},
		InstrumentationLibraryMetrics: []*metricsv1.InstrumentationLibraryMetrics{
			{
				Metrics: []*metricsv1.Metric{
					{
						Data: &metricsv1.Metric_IntSum{
							IntSum: &metricsv1.IntSum{
								DataPoints: []*metricsv1.IntDataPoint{
									{
										Value:             testIntValue,
										TimeUnixNano:      testTimestamp,
										StartTimeUnixNano: testStartTime,
										Labels:            testLabels,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	destMetricsCollection := pmetric.NewMetrics()

	expected := pmetric.NewMetrics()
	expectedResourceMetric := expected.ResourceMetrics().AppendEmpty()
	expectedResourceMetric.Resource().Attributes().InsertString(testResourceAttibuteKey, testResourceAttibuteValue)
	expectedMetric := expectedResourceMetric.
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetric.SetDataType(pmetric.MetricDataTypeSum)
	expectedDataPoints := expectedMetric.Sum().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetIntVal(testIntValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())

	s3Receiver.processResourceMetric(destMetricsCollection, srcResourceMetric)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetricsCollection.ResourceMetrics().Len(); i++ {
		rm := destMetricsCollection.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				for l := 0; l < m.Sum().DataPoints().Len(); l++ {
					m.Sum().DataPoints().At(l).Attributes().Sort()
				}
			}
		}
	}
	assert.Equal(t, expected, destMetricsCollection)
}

func TestProcessResourceMetricDoubleHistogram(t *testing.T) {
	var srcResourceMetric = &metricsv1.ResourceMetrics{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{
					Key: testResourceAttibuteKey,
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{
							StringValue: testResourceAttibuteValue,
						},
					},
				},
			},
		},
		InstrumentationLibraryMetrics: []*metricsv1.InstrumentationLibraryMetrics{
			{
				Metrics: []*metricsv1.Metric{
					{
						Data: &metricsv1.Metric_DoubleHistogram{
							DoubleHistogram: &metricsv1.DoubleHistogram{
								DataPoints: []*metricsv1.DoubleHistogramDataPoint{
									{
										Count:          testCount,
										Sum:            testDoubleValue,
										BucketCounts:   testBucketCounts,
										ExplicitBounds: testExplicitBounds,
										Exemplars: []*metricsv1.DoubleExemplar{
											{
												TimeUnixNano: testTimestamp,
												Value:        testDoubleValue,
												SpanId:       testSpanId,
												TraceId:      testTraceId,
											},
										},
										TimeUnixNano:      testTimestamp,
										StartTimeUnixNano: testStartTime,
										Labels:            testLabels,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	destMetricsCollection := pmetric.NewMetrics()

	expected := pmetric.NewMetrics()
	expectedResourceMetric := expected.ResourceMetrics().AppendEmpty()
	expectedResourceMetric.Resource().Attributes().InsertString(testResourceAttibuteKey, testResourceAttibuteValue)
	expectedMetric := expectedResourceMetric.
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetric.SetDataType(pmetric.MetricDataTypeHistogram)
	expectedDataPoints := expectedMetric.Histogram().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetCount(testCount)
	expectedDataPoint.SetSum(testDoubleValue)
	expectedDataPoint.SetBucketCounts(pcommon.NewImmutableUInt64Slice(testBucketCounts))
	expectedDataPoint.SetExplicitBounds(pcommon.NewImmutableFloat64Slice(testExplicitBounds))
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())
	expectedExemplar := expectedDataPoint.Exemplars().AppendEmpty()
	expectedExemplar.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedExemplar.SetDoubleVal(testDoubleValue)
	var spanId [8]byte
	copy(spanId[:], testSpanId)
	expectedExemplar.SetSpanID(pcommon.NewSpanID(spanId))
	var traceId [16]byte
	copy(traceId[:], testTraceId)
	expectedExemplar.SetTraceID(pcommon.NewTraceID(traceId))

	s3Receiver.processResourceMetric(destMetricsCollection, srcResourceMetric)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetricsCollection.ResourceMetrics().Len(); i++ {
		rm := destMetricsCollection.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
					m.Histogram().DataPoints().At(l).Attributes().Sort()
				}
			}
		}
	}
	assert.Equal(t, expected, destMetricsCollection)
}

func TestProcessResourceMetricIntHistogram(t *testing.T) {
	var srcResourceMetric = &metricsv1.ResourceMetrics{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{
					Key: testResourceAttibuteKey,
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{
							StringValue: testResourceAttibuteValue,
						},
					},
				},
			},
		},
		InstrumentationLibraryMetrics: []*metricsv1.InstrumentationLibraryMetrics{
			{
				Metrics: []*metricsv1.Metric{
					{
						Data: &metricsv1.Metric_IntHistogram{
							IntHistogram: &metricsv1.IntHistogram{
								DataPoints: []*metricsv1.IntHistogramDataPoint{
									{
										Count:          testCount,
										Sum:            testIntValue,
										BucketCounts:   testBucketCounts,
										ExplicitBounds: testExplicitBounds,
										Exemplars: []*metricsv1.IntExemplar{
											{
												TimeUnixNano: testTimestamp,
												Value:        testIntValue,
												SpanId:       testSpanId,
												TraceId:      testTraceId,
											},
										},
										TimeUnixNano:      testTimestamp,
										StartTimeUnixNano: testStartTime,
										Labels:            testLabels,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	destMetricsCollection := pmetric.NewMetrics()

	expected := pmetric.NewMetrics()
	expectedResourceMetric := expected.ResourceMetrics().AppendEmpty()
	expectedResourceMetric.Resource().Attributes().InsertString(testResourceAttibuteKey, testResourceAttibuteValue)
	expectedMetric := expectedResourceMetric.
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetric.SetDataType(pmetric.MetricDataTypeHistogram)
	expectedDataPoints := expectedMetric.Histogram().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetCount(testCount)
	expectedDataPoint.SetSum(float64(testIntValue))
	expectedDataPoint.SetBucketCounts(pcommon.NewImmutableUInt64Slice(testBucketCounts))
	expectedDataPoint.SetExplicitBounds(pcommon.NewImmutableFloat64Slice(testExplicitBounds))
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())
	expectedExemplar := expectedDataPoint.Exemplars().AppendEmpty()
	expectedExemplar.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedExemplar.SetIntVal(testIntValue)
	var spanId [8]byte
	copy(spanId[:], testSpanId)
	expectedExemplar.SetSpanID(pcommon.NewSpanID(spanId))
	var traceId [16]byte
	copy(traceId[:], testTraceId)
	expectedExemplar.SetTraceID(pcommon.NewTraceID(traceId))

	s3Receiver.processResourceMetric(destMetricsCollection, srcResourceMetric)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetricsCollection.ResourceMetrics().Len(); i++ {
		rm := destMetricsCollection.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
					m.Histogram().DataPoints().At(l).Attributes().Sort()
				}
			}
		}
	}
	assert.Equal(t, expected, destMetricsCollection)
}

func TestProcessResourceMetricDoubleSummary(t *testing.T) {
	var srcResourceMetric = &metricsv1.ResourceMetrics{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{
					Key: testResourceAttibuteKey,
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{
							StringValue: testResourceAttibuteValue,
						},
					},
				},
			},
		},
		InstrumentationLibraryMetrics: []*metricsv1.InstrumentationLibraryMetrics{
			{
				Metrics: []*metricsv1.Metric{
					{
						Data: &metricsv1.Metric_DoubleSummary{
							DoubleSummary: &metricsv1.DoubleSummary{
								DataPoints: []*metricsv1.DoubleSummaryDataPoint{
									{
										Count: testCount,
										Sum:   testDoubleValue,
										QuantileValues: []*metricsv1.DoubleSummaryDataPoint_ValueAtQuantile{
											{
												Quantile: testQuantile,
												Value:    testDoubleValue,
											},
										},
										TimeUnixNano:      testTimestamp,
										StartTimeUnixNano: testStartTime,
										Labels:            testLabels,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	destMetricsCollection := pmetric.NewMetrics()

	expected := pmetric.NewMetrics()
	expectedResourceMetric := expected.ResourceMetrics().AppendEmpty()
	expectedResourceMetric.Resource().Attributes().InsertString(testResourceAttibuteKey, testResourceAttibuteValue)
	expectedMetric := expectedResourceMetric.
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	expectedMetric.SetDataType(pmetric.MetricDataTypeSummary)
	expectedDataPoints := expectedMetric.Summary().DataPoints()
	expectedDataPoint := expectedDataPoints.AppendEmpty()
	expectedDataPoint.SetCount(testCount)
	expectedDataPoint.SetSum(testDoubleValue)
	expectedDataPoint.SetTimestamp(pcommon.Timestamp(testTimestamp))
	expectedDataPoint.SetStartTimestamp(pcommon.Timestamp(testStartTime))
	setExpectedLabels(expectedDataPoint.Attributes())
	expectedQuantileValues := expectedDataPoint.QuantileValues().AppendEmpty()
	expectedQuantileValues.SetQuantile(testQuantile)
	expectedQuantileValues.SetValue(testDoubleValue)

	s3Receiver.processResourceMetric(destMetricsCollection, srcResourceMetric)
	expectedDataPoint.Attributes().Sort()
	for i := 0; i < destMetricsCollection.ResourceMetrics().Len(); i++ {
		rm := destMetricsCollection.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				for l := 0; l < m.Summary().DataPoints().Len(); l++ {
					m.Summary().DataPoints().At(l).Attributes().Sort()
				}
			}
		}
	}
	assert.Equal(t, expected, destMetricsCollection)
}
