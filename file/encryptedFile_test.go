// Copyright (c) 2025 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package file

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/couchbase/xdcrDiffer/encryption"
	"github.com/stretchr/testify/assert"
)

func newTestFactory(t *testing.T) *Factory {
	t.Helper()
	passphrase := []byte(hex.EncodeToString(randomBytes(t, 32)))
	factory := NewFactory(true, nil)
	if err := factory.InitEncryption(false, passphrase); err != nil {
		t.Fatalf("Factory.Init failed: %v", err)
	}
	return factory
}

func TestEncryptedFile_Suffix(t *testing.T) {
	factory := newTestFactory(t)

	tmpFile, err := os.CreateTemp("", "suffix_test_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	f, err := factory.OpenFile(tmpFile.Name(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)
	defer f.Close()

	assert.Equal(t, encryption.EncSuffix, f.Suffix())
}

func TestEncryptedFile_Name(t *testing.T) {
	factory := newTestFactory(t)

	tmpFile, err := os.CreateTemp("", "name_test_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)
	defer f.Close()

	assert.Equal(t, tmpFileName, f.Name())
}

func TestEncryptedFile_WriteRead_RoundTrip(t *testing.T) {
	factory := newTestFactory(t)

	tmpFile, err := os.CreateTemp("", "roundtrip_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// Write
	f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)

	plaintext := []byte("secret data payload for round trip")
	n, err := f.Write(plaintext)
	assert.NoError(t, err)
	assert.Equal(t, len(plaintext), n)
	f.Close()

	// Read back
	f2, err := factory.OpenFile(tmpFileName, os.O_RDONLY, 0, ReadMode)
	assert.NoError(t, err)

	buf := make([]byte, len(plaintext))
	readN, err := f2.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(plaintext), readN)
	assert.True(t, bytes.Equal(plaintext, buf))
	f2.Close()
}

func TestEncryptedFile_SmallData(t *testing.T) {
	factory := newTestFactory(t)

	tmpFile, err := os.CreateTemp("", "smalldata_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// Small plaintext
	plaintext := []byte("{\"10\":[\"d2\"]}")

	// Write
	f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)
	n, err := f.Write(plaintext)
	assert.NoError(t, err)
	assert.Equal(t, len(plaintext), n)
	f.Close()

	// Read back
	f2, err := factory.OpenFile(tmpFileName, os.O_RDONLY, 0, ReadMode)
	assert.NoError(t, err)

	buf := make([]byte, len(plaintext))
	readN, err := f2.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(plaintext), readN)
	assert.True(t, bytes.Equal(plaintext, buf))
	f2.Close()
}

func TestEncryptedFile_Read_ChunkedReads(t *testing.T) {
	type tc struct {
		name           string
		totalSize      int
		writeChunks    []int // pattern for writing chunks
		readBufPattern []int // pattern for read buffer sizes
	}
	tests := []tc{
		{
			name:           "mixed_chunks_large_reads",
			totalSize:      25 * 1024,
			writeChunks:    []int{1024, 2048, 4096, 8192},
			readBufPattern: []int{8192, 16384, 512},
		},
		{
			name:           "single_large_write_many_tiny_reads",
			totalSize:      10 * 1024,
			writeChunks:    []int{1 << 20}, // effectively one big write truncated to totalSize
			readBufPattern: []int{1, 2, 3, 4, 5, 7, 11, 13, 32, 64, 128},
		},
		{
			name:           "unaligned_edge_sizes",
			totalSize:      7777,
			writeChunks:    []int{500, 333, 2048, 4096},
			readBufPattern: []int{4095, 1024, 7777, 64},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := newTestFactory(t)

			// Random plaintext
			plaintext := randomBytes(t, tt.totalSize)

			// Create temp file
			pattern := "readchunk_*" + factory.GetSuffix()
			tmpFile, err := os.CreateTemp("", pattern)
			assert.NoError(t, err)
			fileName := tmpFile.Name()
			defer os.Remove(fileName)
			tmpFile.Close()

			// Write encrypted chunks
			f, err := factory.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
			assert.NoError(t, err)

			offset := 0
			wcIdx := 0
			for offset < len(plaintext) {
				chunkSize := tt.writeChunks[wcIdx%len(tt.writeChunks)]
				wcIdx++
				if chunkSize > len(plaintext)-offset {
					chunkSize = len(plaintext) - offset
				}
				chunk := plaintext[offset : offset+chunkSize]
				n, err := f.Write(chunk)
				assert.NoError(t, err)
				assert.Equal(t, len(chunk), n)
				offset += chunkSize
			}
			f.Close()

			// Open for reading
			reader, err := factory.OpenFile(fileName, os.O_RDONLY, 0, ReadMode)
			assert.NoError(t, err)
			defer reader.Close()

			// Repeated reads with varying buffer sizes
			var assembled bytes.Buffer
			readIdx := 0
			for {
				bufSize := tt.readBufPattern[readIdx%len(tt.readBufPattern)]
				readIdx++
				buf := make([]byte, bufSize)
				n, err := reader.Read(buf)
				if n > 0 {
					assembled.Write(buf[:n])
				}
				if err == io.EOF {
					break
				}
				assert.NoError(t, err)
			}

			// Validate
			got := assembled.Bytes()
			assert.Equal(t, len(plaintext), len(got), "length mismatch")
			assert.True(t, bytes.Equal(plaintext, got), "content mismatch")
		})
	}
}

func TestEncryptedFile_Read_ZeroLengthFinal(t *testing.T) {
	factory := newTestFactory(t)

	data := randomBytes(t, 2048)

	tmpFile, err := os.CreateTemp("", "zero_final_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// Write
	f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)
	f.Write(data)
	f.Close()

	// Read
	reader, err := factory.OpenFile(tmpFileName, os.O_RDONLY, 0, ReadMode)
	assert.NoError(t, err)
	defer reader.Close()

	buf := make([]byte, 1500)
	n1, err1 := reader.Read(buf)
	assert.NoError(t, err1)
	assert.Equal(t, 1500, n1)

	// Second read gets remainder
	buf2 := make([]byte, 1000)
	n2, err2 := reader.Read(buf2)
	assert.NoError(t, err2)
	assert.Equal(t, len(data)-n1, n2)

	combined := append(buf[:n1], buf2[:n2]...)
	assert.True(t, bytes.Equal(data, combined))
}

func TestEncryptedFile_ReadAll(t *testing.T) {
	type fileSizeCase struct {
		name string
		size int
	}
	cases := []fileSizeCase{
		{name: "empty_file", size: 0},
		{name: "tiny_file", size: 1},
		{name: "small_file", size: 32},
		{name: "medium_unaligned", size: 4096 + 123},
		{name: "large_file", size: 512 * 1024},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			factory := newTestFactory(t)

			tmpFile, err := os.CreateTemp("", "readall_"+c.name+"_*.enc")
			assert.NoError(t, err)
			defer os.Remove(tmpFile.Name())
			tmpFileName := tmpFile.Name()
			tmpFile.Close()

			// Write with varying chunk sizes
			f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
			assert.NoError(t, err)

			original := randomBytes(t, c.size)
			if c.size > 0 {
				chunkPattern := []int{1, 7, 64, 1024, 8192}
				offset := 0
				i := 0
				for offset < len(original) {
					cs := chunkPattern[i%len(chunkPattern)]
					i++
					if cs > len(original)-offset {
						cs = len(original) - offset
					}
					n, err := f.Write(original[offset : offset+cs])
					assert.NoError(t, err)
					assert.Equal(t, cs, n)
					offset += cs
				}
			}
			f.Close()

			// ReadAll
			reader, err := factory.OpenFile(tmpFileName, os.O_RDONLY, 0, ReadMode)
			assert.NoError(t, err)

			out, err := reader.ReadAll()
			assert.NoError(t, err)
			assert.True(t, bytes.Equal(original, out), "decrypted mismatch size=%d", c.size)
			reader.Close()
		})
	}
}

func TestEncryptedFile_Write_NotInWriteMode(t *testing.T) {
	factory := newTestFactory(t)

	tmpFile, err := os.CreateTemp("", "writemode_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// Create file first
	f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)
	f.Write([]byte("test"))
	f.Close()

	// Open for reading
	f2, err := factory.OpenFile(tmpFileName, os.O_RDONLY, 0, ReadMode)
	assert.NoError(t, err)
	defer f2.Close()

	// Write should fail
	_, err = f2.Write([]byte("should fail"))
	assert.Error(t, err)
}

func TestEncryptedFile_Read_NotInReadMode(t *testing.T) {
	factory := newTestFactory(t)

	tmpFile, err := os.CreateTemp("", "readmode_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// Open for writing
	f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)
	defer f.Close()

	// Read should fail
	buf := make([]byte, 10)
	_, err = f.Read(buf)
	assert.Error(t, err)
}

func TestEncryptedFile_MultipleWrites(t *testing.T) {
	factory := newTestFactory(t)

	tmpFile, err := os.CreateTemp("", "multiwrite_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// Write multiple chunks
	f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)

	chunks := [][]byte{
		[]byte("first chunk of data"),
		[]byte("second chunk with more content"),
		[]byte("third and final chunk!"),
	}

	var expected bytes.Buffer
	for _, chunk := range chunks {
		n, err := f.Write(chunk)
		assert.NoError(t, err)
		assert.Equal(t, len(chunk), n)
		expected.Write(chunk)
	}
	f.Close()

	// Read all back
	f2, err := factory.OpenFile(tmpFileName, os.O_RDONLY, 0, ReadMode)
	assert.NoError(t, err)

	readData, err := f2.ReadAll()
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(expected.Bytes(), readData))
	f2.Close()
}
