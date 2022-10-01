package util

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMockTransferManager(t *testing.T) {
	ctx := context.Background()
	mgr := NewMockTransferManager(ctx)
	s := "Foo!"
	if err := mgr.Upload("mybucket", "mykey", []byte(s)); err != nil {
		t.Errorf("Got error %s trying to upload", err.Error())
		t.Fail()
	}

	buf, err := mgr.Download("mybucket", "mykey")
	if assert.NoError(t, err) {
		assert.Equal(t, s, string(buf))
	}

	_, err = mgr.Download("mybucket", "otherkey")
	assert.Error(t, err)

	_, err = mgr.Download("otherbucket", "mykey")
	assert.Error(t, err)
}
