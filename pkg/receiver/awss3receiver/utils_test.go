package awss3receiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opencensus.io/tag"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

var logger = zap.NewExample()

type beforeThresholdTest struct {
	currentTime time.Time
	testTime    time.Time
	threshold   time.Duration
	expected    bool
}

type extractTimeFromKeyTest struct {
	key      string
	expected time.Time
}

func parseRFC3339Time(timeStr string) time.Time {
	parsedTime, _ := time.Parse(time.RFC3339, timeStr)
	return parsedTime
}

func parseDuration(durationStr string) time.Duration {
	parsedDuration, _ := time.ParseDuration(durationStr)
	return parsedDuration
}

func TestBeforeTheshold(t *testing.T) {
	var testData = []beforeThresholdTest{
		{parseRFC3339Time("2022-03-16T18:01:18Z"), parseRFC3339Time("2022-03-16T17:56:17Z"), parseDuration("5m"), true},
		{parseRFC3339Time("2022-03-16T18:01:18Z"), parseRFC3339Time("2022-03-16T17:56:18Z"), parseDuration("5m"), false},
		{parseRFC3339Time("2022-03-16T18:01:18Z"), parseRFC3339Time("2022-03-16T17:56:19Z"), parseDuration("5m"), false},
		{parseRFC3339Time("2022-03-16T18:01:18Z"), time.Unix(0, 0), parseDuration("5m"), true},
	}
	for _, td := range testData {
		isBefore := beforeThreshold(td.currentTime, td.testTime, td.threshold)
		assert.Equal(t, td.expected, isBefore)
	}
}

func TestExtractTimeFromKey(t *testing.T) {
	var testData = []extractTimeFromKeyTest{
		{"2022/03/16/18/MetricStreams-QuickFull-k2WzmU-hf35wBIw-1-2022-03-16-18-01-18-b4a5d362-39b0-4b3e-9920-af39d30b3720", parseRFC3339Time("2022-03-16T18:01:18Z")},
		{"2022/03/16/18/test-metricname-2022-03-16-18-01-18-b4a5d362-39b0-4b3e-9920-af39d30b3720", parseRFC3339Time("2022-03-16T18:01:18Z")},
	}
	for _, td := range testData {
		extractedTime := extractTimeFromObjectKey(td.key)
		assert.Equal(t, td.expected, extractedTime)
	}
}

func TestFilterByTimeWindow(t *testing.T) {
	var testData = []string{
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-17-56-262e568b-e7e9-42a2-9a75-6e2b63f23733.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-17-57-2ee598bc-e1b3-4811-913d-0228b9b898cf.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-17-57-4fff4fc9-2558-4422-878b-81807e1cd412.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-17-58-cda1fe39-877e-49e3-912d-aa7ac337ef31.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-17-58-8a3547e7-3061-414f-ac2a-f24e885f324c.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-17-59-7ca607be-8890-46fd-af5a-856a792445d2.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-18-00-b2ec8500-e0c0-4ce0-b509-93a96847a33d.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-18-00-4d1c472d-19cb-4ba5-983e-03f1c378fcf1.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-18-02-4af40344-66ed-4365-9811-cf1fe8dd59d8.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-18-04-b4217643-0102-462c-ba18-e6b7b0f8449c.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-18-05-c7c6f1e9-0f18-4a43-a86b-3b626f1dcd93.gz",
	}
	var expected = []string{
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-17-58-8a3547e7-3061-414f-ac2a-f24e885f324c.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-17-58-cda1fe39-877e-49e3-912d-aa7ac337ef31.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-17-59-7ca607be-8890-46fd-af5a-856a792445d2.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-18-00-4d1c472d-19cb-4ba5-983e-03f1c378fcf1.gz",
		"2022/04/29/15/datadog-metrics-stream-5-2022-04-29-15-18-00-b2ec8500-e0c0-4ce0-b509-93a96847a33d.gz",
	}
	filtered := filterByTimeWindow(testData, parseRFC3339Time("2022-04-29T15:20:02Z"), parseDuration("125s"), parseDuration("120s"))
	assert.Equal(t, expected, filtered)
}

type generateBucketPrefixesTest struct {
	testTime   time.Time
	prefix     string
	scanOffset time.Duration
	expected   []string
}

func TestGenerateBucketPrefixes(t *testing.T) {
	var testData = []generateBucketPrefixesTest{
		{parseRFC3339Time("2022-03-29T13:17:00Z"), "", parseDuration("5m"), []string{"2022/03/29/13"}},
		{parseRFC3339Time("2022-03-29T13:04:00Z"), "", parseDuration("5m"), []string{"2022/03/29/12", "2022/03/29/13"}},
		{parseRFC3339Time("2022-03-29T13:04:00Z"), "", parseDuration("1h5m"), []string{"2022/03/29/11", "2022/03/29/12", "2022/03/29/13"}},
		{parseRFC3339Time("2022-03-29T13:17:00Z"), "test/prefix", parseDuration("5m"), []string{"test/prefix/2022/03/29/13"}},
		{parseRFC3339Time("2022-03-29T13:04:00Z"), "test/prefix", parseDuration("5m"), []string{"test/prefix/2022/03/29/12", "test/prefix/2022/03/29/13"}},
		{parseRFC3339Time("2022-03-29T13:04:00Z"), "test/prefix", parseDuration("1h5m"), []string{"test/prefix/2022/03/29/11", "test/prefix/2022/03/29/12", "test/prefix/2022/03/29/13"}},
	}
	for _, td := range testData {
		bucketPrefixes := generateBucketPrefixes(td.testTime, td.prefix, td.scanOffset)
		assert.Equal(t, td.expected, bucketPrefixes)
	}
}

type generateDateTimePrefixTest struct {
	testTime time.Time
	prefix   string
	expected string
}

func TestGenerateDateTimePrefixNoLeadingPrefix(t *testing.T) {
	var testData = []generateDateTimePrefixTest{
		{parseRFC3339Time("2022-03-29T13:17:00Z"), "", "2022/03/29/13"},
		{parseRFC3339Time("2022-03-29T13:17:00Z"), "test/prefix", "test/prefix/2022/03/29/13"},
	}
	for _, td := range testData {
		dateTimePrefix := generateDateTimePrefix(td.testTime, td.prefix)
		assert.Equal(t, td.expected, dateTimePrefix)
	}
}

func TestGetTimeOrNowNoTimeProvided(t *testing.T) {
	currentTime := pcommon.NewTimestampFromTime(time.Now())
	metricsCtx, _ := tag.New(context.Background(), tag.Insert(receiverTagKey, "unittest"))
	returnTime := getTimeOrNow(metricsCtx, 0, logger)
	if currentTime > returnTime {
		t.Errorf("Current time %d should be less than or equal to return time %d", currentTime, returnTime)
		t.Fail()
	}
}

func TestGetTimeOrNowTimeProvided(t *testing.T) {
	currentTime := time.Now()
	pCurrentTime := pcommon.NewTimestampFromTime(currentTime)
	metricsCtx, _ := tag.New(context.Background(), tag.Insert(receiverTagKey, "unittest"))
	returnTime := getTimeOrNow(metricsCtx, uint64(currentTime.UnixNano()), logger)
	assert.Equal(t, pCurrentTime, returnTime)
}
