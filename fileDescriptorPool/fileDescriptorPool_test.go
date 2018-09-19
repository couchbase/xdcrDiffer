package fileDescriptorPool

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFD(t *testing.T) {
	assert := assert.New(t)
	fdp := NewFileDescriptorPool(1)
	assert.NotNil(fdp)

	testFile := "/tmp/poolTest"
	testFile2 := "/tmp/poolTest2"

	defer os.Remove(testFile)
	defer os.Remove(testFile2)

	cb, err := fdp.RegisterFileHandle(testFile)
	assert.Nil(err)
	assert.NotNil(cb)

	cb2, err := fdp.RegisterFileHandle(testFile2)
	assert.Nil(err)
	assert.NotNil(cb2)

	testBytes := []byte("TestString")
	lenCheck := len(testBytes)
	written, err := cb(testBytes)
	assert.Nil(err)
	assert.Equal(lenCheck, written)

	_, err = os.Stat(testFile2)
	assert.True(os.IsNotExist(err))

	written, err = cb2(testBytes)
	assert.Nil(err)
	assert.Equal(lenCheck, written)

	assert.Equal(1, len(fdp.fdsInUseChan))
	assert.Equal(2, len(fdp.fdMap))

	fdp.DeRegisterFileHandle(testFile)
	fdp.DeRegisterFileHandle(testFile2)
}
