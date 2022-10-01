package util

import (
	"context"
	"fmt"
)

type MockTransferManager struct {
	objects map[string]map[string][]byte
	ctx     context.Context
}

func NewMockTransferManager(ctx context.Context) *MockTransferManager {
	return &MockTransferManager{
		objects: make(map[string]map[string][]byte),
		ctx:     ctx,
	}
}

func (m *MockTransferManager) Upload(bucket string, key string, data []byte) error {
	b, exists := m.objects[bucket]
	if !exists {
		b = make(map[string][]byte)
		m.objects[bucket] = b
	}

	b[key] = data
	return nil
}

func (m *MockTransferManager) Download(bucket string, key string) ([]byte, error) {
	b, exists := m.objects[bucket]
	if !exists {
		return nil, fmt.Errorf("bucket %s does not exist", bucket)
	}

	data, exists := b[key]
	if !exists {
		return nil, fmt.Errorf("key %s does not exist in bucket %s", key, bucket)
	}

	return data, nil
}
