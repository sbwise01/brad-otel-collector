package awss3receiver

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/sbwise01/brad-otel-collector/pkg/util"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.opencensus.io/stats"
	"go.uber.org/zap"
)

type bucket struct {
	config         *Config
	logger         *zap.Logger
	sess           *session.Session
	svc            *s3.S3
	mgr            util.TransferManager
	checkpointItem string
}

func newBucket(config *Config, logger *zap.Logger) (*bucket, error) {
	cfg := aws.Config{Region: aws.String(config.BucketRegion)}
	if config.BucketEndpoint != "" {
		cfg.Endpoint = aws.String(config.BucketEndpoint)
		cfg.DisableSSL = aws.Bool(true)
		cfg.S3ForcePathStyle = aws.Bool(true)
		logger.Info("Creating S3 bucket with endpoint", zap.String("endpoint", config.BucketEndpoint))
	}
	mgr, err := util.NewS3TransferManager(config.BucketIOTimeout, &cfg)
	if err != nil {
		return nil, err
	}

	sess, err := session.NewSession(&cfg)
	if err != nil {
		return nil, err
	}

	// For now we just use the default configuration, which should
	// work correctly in k8s
	svc := s3.New(sess)

	checkpointItem := ""

	return &bucket{
		config:         config,
		logger:         logger,
		sess:           sess,
		svc:            svc,
		mgr:            mgr,
		checkpointItem: checkpointItem,
	}, nil
}

func (b *bucket) getCheckpointItem() string {
	if b.checkpointItem == "" {
		checkpointItemBytes, err := b.getItemContents(b.config.CheckpointKey, false)
		if err != nil {
			b.logger.Warn("Could not read checkpoint item from S3 bucket ... leaving empty", zap.Error(err))
		} else {
			b.checkpointItem = string(checkpointItemBytes[:])
		}
	}

	return b.checkpointItem
}

func (b *bucket) getItemContents(bucketItem string, gzipCompression bool) ([]byte, error) {
	buf, err := b.mgr.Download(b.config.BucketName, bucketItem)
	if err != nil {
		return nil, err
	}

	if gzipCompression {
		var buf2 bytes.Buffer
		err = util.GunzipWrite(&buf2, buf)
		if err != nil {
			b.logger.Error("Couldn't decompress bucket contents", zap.Error(err))
			return nil, err
		}
		return buf2.Bytes(), nil
	}

	return buf, nil
}

func (b *bucket) getListOfBucketItems(bucketPrefix string) ([]string, error) {
	bucketItems := []string{}

	err := b.svc.ListObjectsV2Pages(
		&s3.ListObjectsV2Input{
			Bucket:  aws.String(b.config.BucketName),
			Prefix:  aws.String(bucketPrefix),
			MaxKeys: aws.Int64(1000),
		},
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, item := range page.Contents {
				bucketItems = append(bucketItems, *item.Key)
			}
			return true
		})

	if err != nil {
		b.logger.Error("Couldn't retrieve bucket items", zap.Error(err))
		return nil, err
	}

	return bucketItems, nil
}

// The persistCheckpointItem function writes an object to the S3 bucket
// at the configured Checkpoint key
// i.e. s3://metricstreams-quickfull-k2wzmu-vcp0hesq/awss3receiver_checkpoint.txt
// which contains the value of the last S3 object processed by the receiver
// Doing this checkpoint prevents the receiver from reprocessing and duplicating
// metrics on restarts and serves as the mechanism to keep track of processed
// metrics.  Using a checkpoint tracking mechanism prevents the need to track
// metrics processing at an object level.
func (b *bucket) persistCheckpointItem(metricsCtx context.Context) {
	err := b.putItemContents(b.config.CheckpointKey, b.checkpointItem)
	if err != nil {
		b.logger.Error("Error persisting checking item")
		stats.Record(metricsCtx, statCwCheckpointPersistErrors.M(1))
	}
}

func (b *bucket) putItemContents(bucketKey, contents string) error {
	return b.mgr.Upload(b.config.BucketName, bucketKey, []byte(contents))
}

func (b *bucket) scanBucket(metricsCtx context.Context) ([]string, error) {
	allBucketItems := []string{}

	bucketPrefixes := generateBucketPrefixes(time.Now(), b.config.Prefix, b.config.ScanOffset)
	for _, bucketPrefix := range bucketPrefixes {
		bucketItems, err := b.getListOfBucketItems(bucketPrefix)
		if err != nil {
			b.logger.Error("Couldn't retrieve bucket items", zap.Error(err))
			return nil, err
		}
		allBucketItems = append(allBucketItems, bucketItems...)
	}
	sort.Strings(allBucketItems)
	truncatedBucketItems := b.truncateByCheckpoint(metricsCtx, allBucketItems)
	filteredBucketItems := filterByTimeWindow(truncatedBucketItems, time.Now(), b.config.ScanOffset, b.config.ProcessingDelay)

	return filteredBucketItems, nil
}

func (b *bucket) setCheckpointItem(bucketItem string) {
	b.checkpointItem = bucketItem
}

func (b *bucket) truncateByCheckpoint(metricsCtx context.Context, bucketItems []string) []string {
	truncatedBucketItems := bucketItems
	checkpointItem := b.getCheckpointItem()
	if checkpointItem == "" {
		b.logger.Warn("Checkpoint item empty ... processing all entries")
	} else {
		i := sort.SearchStrings(bucketItems, checkpointItem)
		if i < len(bucketItems) && bucketItems[i] == checkpointItem {
			b.logger.Info("Truncating scanned item list from", zap.String("checkpoint_item", checkpointItem))
			truncatedBucketItems = bucketItems[i+1:]
		} else {
			b.logger.Warn("Could not find checkpoint item in scanned bucket list ... processing all entries", zap.String("checkpoint_item", checkpointItem))
			stats.Record(metricsCtx, statCwCheckpointNotFoundErrors.M(1))
		}
	}

	return truncatedBucketItems
}
