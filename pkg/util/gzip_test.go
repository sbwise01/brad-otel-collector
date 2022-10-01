package util

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGzipDecompress(t *testing.T) {
	testDataGzipCompressed := []byte{31, 139, 8, 0, 0, 0, 0, 0, 4, 255, 0, 9, 0, 246, 255, 116, 101, 115, 116, 32, 100, 97, 116, 97, 1, 0, 0, 255, 255, 178, 174, 8, 211, 9, 0, 0, 0}
	expected := []byte("test data")
	var buf bytes.Buffer
	GunzipWrite(&buf, testDataGzipCompressed)
	assert.Equal(t, expected, buf.Bytes())
}
