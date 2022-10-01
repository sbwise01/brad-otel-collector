package awss3receiver

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.opencensus.io/stats"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

// Global variables
var bucketKeyTimeRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}`)

func beforeThreshold(currentTime time.Time, compareTime time.Time, threshold time.Duration) bool {
	thresholdCutoffTime := currentTime.Add(-threshold)
	return compareTime.Before(thresholdCutoffTime)
}

func extractTimeFromObjectKey(key string) time.Time {
	keyTimeStr := bucketKeyTimeRe.FindString(key)
	// Note: the time parsing format doesn't include timezone,
	//       so the parser will return parsed time as UTC
	t, _ := time.Parse("2006-01-02-15-04-05", keyTimeStr)
	// Note:  we are ignoring error here because the key format is well known
	return t
}

func filterByTimeWindow(bucketItems []string, currentTime time.Time, scanOffset time.Duration, processingDelay time.Duration) []string {
	filteredBucketItems := []string{}

	scanOffsetCutoffTime := currentTime.Add(-scanOffset)
	processingDelayCutoffTime := currentTime.Add(-processingDelay)
	for _, bucketKey := range bucketItems {
		bucketObjectTime := extractTimeFromObjectKey(bucketKey)
		if bucketObjectTime.Before(processingDelayCutoffTime) && bucketObjectTime.After(scanOffsetCutoffTime) {
			filteredBucketItems = append(filteredBucketItems, bucketKey)
		}
	}
	sort.Strings(filteredBucketItems)

	return filteredBucketItems
}

func generateBucketPrefixes(currentTime time.Time, prefix string, scanOffset time.Duration) []string {
	var bucketPrefixes []string

	bucketPrefixes = append(bucketPrefixes, generateDateTimePrefix(currentTime, prefix))

	// Use time window truncated by hours to get accurate list of hour based prefixes to scan
	scanOffsetTime := currentTime.Add(-scanOffset)
	currentTime = currentTime.Truncate(time.Hour)
	scanOffsetTime = scanOffsetTime.Truncate(time.Hour)
	scanOffsetHours := int(currentTime.Sub(scanOffsetTime).Hours())

	for i := 1; i <= scanOffsetHours; i++ {
		offset, _ := time.ParseDuration(fmt.Sprintf("%dh", i))
		offsetTime := currentTime.Add(-offset)
		bucketPrefixes = append(bucketPrefixes, generateDateTimePrefix(offsetTime, prefix))
	}

	sort.Strings(bucketPrefixes)

	return bucketPrefixes
}

func generateDateTimePrefix(dateTime time.Time, prefix string) string {
	dateTimeStr := fmt.Sprintf("%04d/%02d/%02d/%02d", dateTime.Year(), dateTime.Month(), dateTime.Day(), dateTime.Hour())
	if len(prefix) > 0 {
		dateTimeStr = fmt.Sprintf("%v/%v", strings.TrimRight(prefix, "/"), dateTimeStr)
	}

	return dateTimeStr
}

func getTimeOrNow(metricsCtx context.Context, unixNanoTime uint64, logger *zap.Logger) pcommon.Timestamp {
	var timeVal pcommon.Timestamp

	if unixNanoTime != 0 {
		timeVal = pcommon.Timestamp(unixNanoTime)
	} else {
		timeVal = pcommon.NewTimestampFromTime(time.Now())
		logger.Debug("Cloudwatch metrics stream should not produce metrics without a Timestamp ... defaulting to now()")
		stats.Record(metricsCtx, statCwTimestampDefaulted.M(1))
	}

	return timeVal
}

func millisecondsSince(startTime time.Time) int64 {
	return time.Since(startTime).Nanoseconds() / time.Millisecond.Nanoseconds()
}
