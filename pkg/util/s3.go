package util

import (
	"bytes"
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Using an interface makes things easier to test
type TransferManager interface {
	Download(string, string) ([]byte, error)
	Upload(string, string, []byte) error
}

type S3TransferManager struct {
	Downloader *s3manager.Downloader
	Uploader   *s3manager.Uploader
	ioTimeout  time.Duration
}

func NewS3TransferManager(ioTimeout time.Duration, cfg *aws.Config) (*S3TransferManager, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	svc := s3.New(sess, cfg)
	return &S3TransferManager{
		Downloader: s3manager.NewDownloaderWithClient(svc),
		Uploader:   s3manager.NewUploaderWithClient(svc),
		ioTimeout:  ioTimeout,
	}, nil
}

func (m *S3TransferManager) Download(bucket string, key string) ([]byte, error) {
	buf := aws.NewWriteAtBuffer([]byte{})
	ctxTimeout, cancel := context.WithTimeout(context.Background(), m.ioTimeout)
	defer cancel()

	_, err := m.Downloader.DownloadWithContext(ctxTimeout, buf, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (m *S3TransferManager) Upload(bucket string, key string, data []byte) error {
	// Create a downloader with the session and default options
	reader := bytes.NewReader(data)
	ctxTimeout, cancel := context.WithTimeout(context.Background(), m.ioTimeout)
	defer cancel()

	_, err := m.Uploader.UploadWithContext(ctxTimeout, &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   reader,
	})

	return err
}
