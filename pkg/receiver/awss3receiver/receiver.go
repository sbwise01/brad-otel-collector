package awss3receiver

import (
	"context"
	"strings"
	"time"

	//lint:ignore SA1019 This is the required way of decoding OTLP wire format from https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-metric-streams-formats-opentelemetry-parse.html
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	protomessage "github.com/open-telemetry/opentelemetry-proto/gen/go/collector/metrics/v1"
	commonv1 "github.com/open-telemetry/opentelemetry-proto/gen/go/common/v1"
	metricsv1 "github.com/open-telemetry/opentelemetry-proto/gen/go/metrics/v1"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type awsS3Receiver struct {
	settings       component.ReceiverCreateSettings
	config         *Config
	logger         *zap.Logger
	metricsCtx     context.Context
	nextConsumer   consumer.Metrics
	cancel         context.CancelFunc
	checkpointItem string
}

func newAwsS3Receiver(
	set component.ReceiverCreateSettings,
	config Config,
	nextConsumer consumer.Metrics,
) (component.MetricsReceiver, error) {
	metricsCtx, err := tag.New(context.Background(), tag.Insert(receiverTagKey, config.ID().String()+uuid.NewString()[0:6]))
	if err != nil {
		return nil, err
	}
	if err := view.Register(metricViews()...); err != nil {
		if strings.Contains(err.Error(), "a different view with the same name is already registered") {
			set.Logger.Warn("Another instance of receiver already registered the metricViews")
		} else {
			return nil, err
		}
	}

	if nextConsumer == nil {
		return nil, component.ErrNilNextConsumer
	}
	r := &awsS3Receiver{
		settings:       set,
		logger:         set.Logger,
		metricsCtx:     metricsCtx,
		config:         &config,
		nextConsumer:   nextConsumer,
		checkpointItem: "",
	}
	if !r.config.GzipCompression {
		r.logger.Warn("awsS3Receiver running with gzip compression turned off")
	}
	return r, nil
}

// srcDataPoint is just wrapping existing exported functions for different types
type srcDataPoint interface {
	GetLabels() []*commonv1.StringKeyValue
	GetStartTimeUnixNano() uint64
	GetTimeUnixNano() uint64
}

// srcHistogramDataPoint is just wrapping existing exported functions for different types
type srcHistogramDataPoint interface {
	srcDataPoint
	GetCount() uint64
	GetBucketCounts() []uint64
	GetExplicitBounds() []float64
}

func getSum(dp srcHistogramDataPoint) (r float64) {
	switch dpType := dp.(type) {
	case *metricsv1.IntHistogramDataPoint:
		r = float64(dpType.GetSum())
	case *metricsv1.DoubleHistogramDataPoint:
		r = dpType.GetSum()
	}
	return
}

// destDataPoint is just wrapping existing exported functions for different types
type destDataPoint interface {
	Attributes() pcommon.Map
	SetTimestamp(pcommon.Timestamp)
	SetStartTimestamp(pcommon.Timestamp)
}

func (s *awsS3Receiver) processDataPoint(srcDataPoint srcDataPoint, destDataPoint destDataPoint) {
	destTimestamp := getTimeOrNow(s.metricsCtx, srcDataPoint.GetTimeUnixNano(), s.logger)
	timeLag := time.Since(destTimestamp.AsTime())
	stats.Record(s.metricsCtx, statCwMetricsTimeLag.M(int64(timeLag.Seconds())))
	destDataPoint.SetTimestamp(destTimestamp)
	if srcDataPoint.GetStartTimeUnixNano() != 0 {
		destDataPoint.SetStartTimestamp(pcommon.Timestamp(srcDataPoint.GetStartTimeUnixNano()))
	}
	destAm := destDataPoint.Attributes()
	for _, srcLabel := range srcDataPoint.GetLabels() {
		destAm.InsertString(srcLabel.GetKey(), srcLabel.GetValue())
	}

	for k, v := range s.config.ExtraLabels {
		destAm.InsertString(k, v)
	}
}

func (s *awsS3Receiver) processResourceMetric(destMetricsCollection pmetric.Metrics, srcResourceMetric *metricsv1.ResourceMetrics) {
	destResourceMetric := destMetricsCollection.ResourceMetrics().AppendEmpty()
	destResource := destResourceMetric.Resource()
	srcResource := srcResourceMetric.GetResource()

	// loop through all resource attributes and keep
	for _, srcResourceAttribute := range srcResource.GetAttributes() {
		destResource.Attributes().InsertString(srcResourceAttribute.GetKey(), srcResourceAttribute.GetValue().GetStringValue())
	}

	// loop through instrumentation_library_metrics/metrics and transfer to destMetricsCollection
	for _, srcIlm := range srcResourceMetric.GetInstrumentationLibraryMetrics() {
		destIlm := destResourceMetric.ScopeMetrics().AppendEmpty()
		for _, srcMetric := range srcIlm.GetMetrics() {
			destMetrics := destIlm.Metrics().AppendEmpty()
			destMetrics.SetName(srcMetric.GetName())
			s.processMetric(srcMetric, destMetrics)
			stats.Record(s.metricsCtx, statCwMetricsProcessed.M(1))
		}
	}
}

// intMetric is just wrapping existing exported functions for use in switch
type intMetric interface {
	GetDataPoints() []*metricsv1.IntDataPoint
}

// doubleMetric is just wrapping existing exported functions for use in switch
type doubleMetric interface {
	GetDataPoints() []*metricsv1.DoubleDataPoint
}

func dpRange(srcMetricData interface{}, f func(srcDataPoint)) {
	switch srcMetricDataType := srcMetricData.(type) {
	case intMetric:
		for _, srcDataPoint := range srcMetricDataType.GetDataPoints() {
			f(srcDataPoint)
		}
	case doubleMetric:
		for _, srcDataPoint := range srcMetricDataType.GetDataPoints() {
			f(srcDataPoint)
		}
	}
}

func histogramRange(srcMetricData interface{}, f func(srcHistogramDataPoint)) {
	switch srcMetricDataType := srcMetricData.(type) {
	case *metricsv1.IntHistogram:
		for _, srcDataPoint := range srcMetricDataType.GetDataPoints() {
			f(srcDataPoint)
		}
	case *metricsv1.DoubleHistogram:
		for _, srcDataPoint := range srcMetricDataType.GetDataPoints() {
			f(srcDataPoint)
		}
	}
}

// examplar is just wrapping existing exported functions for different types
type exemplar interface {
	GetTimeUnixNano() uint64
	GetSpanId() []byte
	GetTraceId() []byte
	GetFilteredLabels() []*commonv1.StringKeyValue
}

func exemplarRange(h interface{}, f func(exemplar)) {
	switch hType := h.(type) {
	case *metricsv1.IntHistogramDataPoint:
		if es := hType.GetExemplars(); es != nil {
			for _, e := range es {
				f(e)
			}
		}
	case *metricsv1.DoubleHistogramDataPoint:
		if es := hType.GetExemplars(); es != nil {
			for _, e := range es {
				f(e)
			}
		}
	}
}

func (s *awsS3Receiver) keepDataPoint(srcDataPoint srcDataPoint) bool {
	if s.config.FilterStaleDataPoints {
		dataPointTimestamp := getTimeOrNow(s.metricsCtx, srcDataPoint.GetTimeUnixNano(), s.logger)
		if beforeThreshold(time.Now(), dataPointTimestamp.AsTime(), s.config.StalenessThreshold) {
			stats.Record(s.metricsCtx, statCwStaleDataPoint.M(1))
			return false
		}
	}

	return true
}

func (s *awsS3Receiver) processNumber(srcMetricData interface{}, destDps pmetric.NumberDataPointSlice) {
	dpRange(srcMetricData, func(srcDataPoint srcDataPoint) {
		if s.keepDataPoint(srcDataPoint) {
			destDataPoint := destDps.AppendEmpty()
			switch srcDataPointType := srcDataPoint.(type) {
			case *metricsv1.IntDataPoint:
				destDataPoint.SetIntVal(srcDataPointType.GetValue())
			case *metricsv1.DoubleDataPoint:
				destDataPoint.SetDoubleVal(srcDataPointType.GetValue())
			}
			s.processDataPoint(srcDataPoint, destDataPoint)
		}
	})
	destDps.Sort(func(a, b pmetric.NumberDataPoint) bool { return a.Timestamp() < b.Timestamp() })
}

func (s *awsS3Receiver) processHistogram(srcMetricData interface{}, destDps pmetric.HistogramDataPointSlice) {
	histogramRange(srcMetricData, func(srcDataPoint srcHistogramDataPoint) {
		if s.keepDataPoint(srcDataPoint) {
			destDataPoint := destDps.AppendEmpty()
			destDataPoint.SetCount(srcDataPoint.GetCount())
			destDataPoint.SetSum(getSum(srcDataPoint))
			if srcDataPoint.GetBucketCounts() != nil {
				destDataPoint.SetBucketCounts(pcommon.NewImmutableUInt64Slice(srcDataPoint.GetBucketCounts()))
			}
			if srcDataPoint.GetExplicitBounds() != nil {
				destDataPoint.SetExplicitBounds(pcommon.NewImmutableFloat64Slice(srcDataPoint.GetExplicitBounds()))
			}
			exemplarRange(srcDataPoint, func(srcExemplar exemplar) {
				destExemplar := destDataPoint.Exemplars().AppendEmpty()
				destExemplar.SetTimestamp(pcommon.Timestamp(srcExemplar.GetTimeUnixNano()))
				switch srcExemplarType := srcExemplar.(type) {
				case *metricsv1.IntExemplar:
					destExemplar.SetIntVal(srcExemplarType.GetValue())
				case *metricsv1.DoubleExemplar:
					destExemplar.SetDoubleVal(srcExemplarType.GetValue())
				}
				if srcExemplar.GetSpanId() != nil && len(srcExemplar.GetSpanId()) <= 8 {
					var srcSpanId [8]byte
					copy(srcSpanId[:], srcExemplar.GetSpanId())
					destExemplar.SetSpanID(pcommon.NewSpanID(srcSpanId))
				}
				if srcExemplar.GetTraceId() != nil && len(srcExemplar.GetTraceId()) <= 16 {
					var srcTraceId [16]byte
					copy(srcTraceId[:], srcExemplar.GetTraceId())
					destExemplar.SetTraceID(pcommon.NewTraceID(srcTraceId))
				}
				if srcExemplar.GetFilteredLabels() != nil {
					for _, srcFl := range srcExemplar.GetFilteredLabels() {
						destExemplar.FilteredAttributes().InsertString(srcFl.GetKey(), srcFl.GetValue())
					}
				}
			})
			s.processDataPoint(srcDataPoint, destDataPoint)
		}
	})
	destDps.Sort(func(a, b pmetric.HistogramDataPoint) bool { return a.Timestamp() < b.Timestamp() })
}

func (s *awsS3Receiver) processSummary(srcMetricData *metricsv1.DoubleSummary, destDps pmetric.SummaryDataPointSlice) {
	for _, srcDataPoint := range srcMetricData.GetDataPoints() {
		if s.keepDataPoint(srcDataPoint) {
			destDataPoint := destDps.AppendEmpty()
			destDataPoint.SetCount(srcDataPoint.GetCount())
			destDataPoint.SetSum(srcDataPoint.GetSum())
			if srcDataPoint.GetQuantileValues() != nil {
				for _, srcQuantile := range srcDataPoint.GetQuantileValues() {
					destQuantile := destDataPoint.QuantileValues().AppendEmpty()
					destQuantile.SetQuantile(srcQuantile.GetQuantile())
					destQuantile.SetValue(srcQuantile.GetValue())
				}
			}
			s.processDataPoint(srcDataPoint, destDataPoint)
		}
	}
	destDps.Sort(func(a, b pmetric.SummaryDataPoint) bool { return a.Timestamp() < b.Timestamp() })
}

func (s *awsS3Receiver) processMetric(srcMetric *metricsv1.Metric, destMetrics pmetric.Metric) {
	srcMetricData := srcMetric.GetData()

	switch srcMetricType := srcMetricData.(type) {
	case *metricsv1.Metric_DoubleGauge:
		destMetrics.SetDataType(pmetric.MetricDataTypeGauge)
		s.processNumber(srcMetricType.DoubleGauge, destMetrics.Gauge().DataPoints())
		stats.Record(s.metricsCtx, statCwDoubleGaugeProcessed.M(1))
	case *metricsv1.Metric_IntGauge:
		destMetrics.SetDataType(pmetric.MetricDataTypeGauge)
		s.processNumber(srcMetricType.IntGauge, destMetrics.Gauge().DataPoints())
		stats.Record(s.metricsCtx, statCwIntGaugeProcessed.M(1))
	case *metricsv1.Metric_DoubleSum:
		destMetrics.SetDataType(pmetric.MetricDataTypeSum)
		s.processNumber(srcMetricType.DoubleSum, destMetrics.Sum().DataPoints())
		stats.Record(s.metricsCtx, statCwDoubleSumProcessed.M(1))
	case *metricsv1.Metric_IntSum:
		destMetrics.SetDataType(pmetric.MetricDataTypeSum)
		s.processNumber(srcMetricType.IntSum, destMetrics.Sum().DataPoints())
		stats.Record(s.metricsCtx, statCwIntSumProcessed.M(1))
	case *metricsv1.Metric_DoubleHistogram:
		destMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
		s.processHistogram(srcMetricType.DoubleHistogram, destMetrics.Histogram().DataPoints())
		stats.Record(s.metricsCtx, statCwDoubleHistogramProcessed.M(1))
	case *metricsv1.Metric_IntHistogram:
		destMetrics.SetDataType(pmetric.MetricDataTypeHistogram)
		s.processHistogram(srcMetricType.IntHistogram, destMetrics.Histogram().DataPoints())
		stats.Record(s.metricsCtx, statCwIntHistogramProcessed.M(1))
	case *metricsv1.Metric_DoubleSummary:
		destMetrics.SetDataType(pmetric.MetricDataTypeSummary)
		s.processSummary(srcMetricType.DoubleSummary, destMetrics.Summary().DataPoints())
		stats.Record(s.metricsCtx, statCwDoubleSummaryProcessed.M(1))
	}
}

func (s *awsS3Receiver) compactDest(destMetricsCollection pmetric.Metrics) {
	destMetricsCollection.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				var numDataPoints int
				switch m.DataType() {
				case pmetric.MetricDataTypeGauge:
					numDataPoints = m.Gauge().DataPoints().Len()
				case pmetric.MetricDataTypeSum:
					numDataPoints = m.Sum().DataPoints().Len()
				case pmetric.MetricDataTypeHistogram:
					numDataPoints = m.Histogram().DataPoints().Len()
				case pmetric.MetricDataTypeSummary:
					numDataPoints = m.Summary().DataPoints().Len()
				}
				return numDataPoints == 0
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
}

func (s *awsS3Receiver) collectS3OtlpMetrics(ctx context.Context) error {
	s.logger.Info("Starting processing loop")
	startTime := time.Now()

	bucket, err := newBucket(s.config, s.logger)
	if err != nil {
		s.logger.Error("Failed to create new bucket", zap.Error(err))
		stats.Record(s.metricsCtx, statCwS3ConnectErrors.M(1))
		return err
	}
	bucket.setCheckpointItem(s.checkpointItem)

	bucketItems, err := bucket.scanBucket(s.metricsCtx)
	if err != nil {
		s.logger.Error("Scan of items from S3 bucket failed", zap.Error(err))
		stats.Record(s.metricsCtx, statCwScanBucketErrors.M(1))
		return err
	}

	if len(bucketItems) == 0 {
		s.logger.Warn("Bucket scan returned no items, consider expanding scanOffset")
		stats.Record(s.metricsCtx, statCwScanBucketEmptyErrors.M(1))
	}

	// loop through all S3 bucket objects
	for _, bucketItem := range bucketItems {
		contents, err := bucket.getItemContents(bucketItem, s.config.GzipCompression)
		if err != nil {
			s.logger.Error("Failed to get bucket object's file contents", zap.Error(err))
			stats.Record(s.metricsCtx, statCwReadBucketContentsErrors.M(1))
			return err
		}

		var metricsServiceResponse protomessage.ExportMetricsServiceRequest
		destMetricsCollection := pmetric.NewMetrics()
		decodingSuccessful := false
		finishedDecoding := false
		buffer := proto.NewBuffer(contents)
		// for each S3 bucket object, loop through 1 or more decoded wire messages
		for !finishedDecoding {
			err := buffer.DecodeMessage(&metricsServiceResponse)
			if err != nil {
				finishedDecoding = true
			} else {
				decodingSuccessful = true
				// loop through all resource metrics in decoded message
				for _, srcResourceMetric := range metricsServiceResponse.GetResourceMetrics() {
					s.processResourceMetric(destMetricsCollection, srcResourceMetric)
				}
				metricsServiceResponse.Reset()
			}
		}

		if decodingSuccessful {
			s.compactDest(destMetricsCollection)
			err = s.nextConsumer.ConsumeMetrics(ctx, destMetricsCollection)
			if err != nil {
				s.logger.Error("Adding S3 metrics to consumer failed", zap.Error(err))
				stats.Record(s.metricsCtx, statCwEmitMetricsErrors.M(1))
				return err
			}

			bucket.setCheckpointItem(bucketItem)
			s.checkpointItem = bucketItem
		} else {
			s.logger.Warn("Was not able to decode the S3 item", zap.String("object_key", bucketItem))
			stats.Record(s.metricsCtx, statCwDecodingErrors.M(1))
		}
	}

	stats.Record(s.metricsCtx, statCwMetricsProcessingDurationMs.M(millisecondsSince(startTime)))

	bucket.persistCheckpointItem(s.metricsCtx)

	s.logger.Info("Finished processing loop")
	return nil
}

func (s *awsS3Receiver) Start(ctx context.Context, host component.Host) error {
	ctx, s.cancel = context.WithCancel(ctx)
	ticker := time.NewTicker(s.config.CollectionInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := s.collectS3OtlpMetrics(ctx)
				if err != nil {
					s.logger.Error("Failed to collect S3 OTLP metrics", zap.Error(err))
				}
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	return nil
}

func (s *awsS3Receiver) Shutdown(context.Context) error {
	s.cancel()
	return nil
}
