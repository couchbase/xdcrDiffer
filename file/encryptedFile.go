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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/couchbase/xdcrDiffer/encryption"
)

// available returns the number of unread bytes.
func (d *decryptBuffer) available() int {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	return len(d.buf) - d.offset
}

// read copies up to len(dst) bytes from buffer into dst.
// Returns number of bytes read.
func (d *decryptBuffer) read(dst []byte) int {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	avail := len(d.buf) - d.offset
	if avail <= 0 {
		return 0
	}

	n := copy(dst, d.buf[d.offset:])
	d.offset += n
	return n
}

// append adds data to the buffer.
func (d *decryptBuffer) append(data []byte) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	// Fast path: no offset
	if d.offset == 0 {
		d.buf = append(d.buf, data...)
		return
	}

	actualBufferLen := len(d.buf) - d.offset

	// Compact ONLY when append would overflow
	// AND compaction would avoid allocation
	if len(d.buf)+len(data) > cap(d.buf) && actualBufferLen+len(data) <= cap(d.buf) {
		n := copy(d.buf, d.buf[d.offset:])
		d.buf = d.buf[:n]
		d.offset = 0
	}

	d.buf = append(d.buf, data...)
}

var _ File = (*EncryptedFile)(nil)

// Write encrypts data and writes it to the file.
// Format: [4-byte length][nonce+ciphertext]
func (e *EncryptedFile) Write(data []byte) (int, error) {
	if e.mode != WriteMode {
		return 0, fmt.Errorf("file not opened for writing")
	}
	if e.encryptor == nil {
		return 0, fmt.Errorf("encryptor is nil")
	}
	if e.file == nil {
		return 0, fmt.Errorf("file descriptor is nil")
	}

	cipherTextWithNonce, nonce, err := e.encryptor.Encrypt(data)
	if err != nil {
		return 0, err
	}
	if nonce == nil {
		return 0, fmt.Errorf("encryption returned nil nonce")
	}

	// Write length as uint32 big endian
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(cipherTextWithNonce)))

	if n, err := e.file.Write(lenBuf[:]); err != nil {
		return 0, fmt.Errorf("failed to write length: %w", err)
	} else if n != 4 {
		return 0, fmt.Errorf("incomplete write of length: %d/4", n)
	}

	if n, err := e.file.Write(cipherTextWithNonce); err != nil {
		return 0, fmt.Errorf("failed to write ciphertext: %w", err)
	} else if n != len(cipherTextWithNonce) {
		return 0, fmt.Errorf("incomplete write of ciphertext: %d/%d", n, len(cipherTextWithNonce))
	}

	return len(data), nil
}

// Read decrypts data from the file and writes the requested data bytes into the buffer.
// Returns the number of bytes read and any error encountered.
func (e *EncryptedFile) Read(buffer []byte) (int, error) {
	if e.mode == WriteMode {
		return 0, fmt.Errorf("file not opened for reading")
	}
	return e.readInner(buffer)
}

// readInner reads up to len(buffer) bytes into the buffer.
// Returns the number of bytes read and any error encountered.
func (e *EncryptedFile) readInner(buffer []byte) (int, error) {
	requested := len(buffer)
	if requested == 0 {
		return 0, nil
	}

	e.mtx.Lock()
	defer e.mtx.Unlock()

	// Fill decryptBuf until we have enough data or hit EOF
	for e.decryptBuf.available() < requested && !e.eof {
		plaintext, err := e.readNextChunk()
		if err != nil {
			if err == io.EOF {
				e.eof = true
				break
			}
			return 0, err
		}
		e.decryptBuf.append(plaintext)
	}

	n := e.decryptBuf.read(buffer)

	if n == 0 && e.eof {
		return 0, io.EOF
	}

	return n, nil
}

// readNextChunk reads and decrypts the next encrypted chunk from file.
// Returns the decrypted data and any error encountered.
func (e *EncryptedFile) readNextChunk() ([]byte, error) {
	// Read length prefix
	var lenBuf [4]byte
	n, err := io.ReadFull(e.file, lenBuf[:])
	if err != nil {
		if err == io.EOF && n == 0 {
			return nil, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("incomplete length read: %d/4", n)
		}
		return nil, err
	}

	// Read nonce+ciphertext
	dataLen := binary.BigEndian.Uint32(lenBuf[:])
	data := make([]byte, dataLen)
	if n, err := io.ReadFull(e.file, data); err != nil {
		return nil, fmt.Errorf("read ciphertext error: %w", err)
	} else if n != int(dataLen) {
		return nil, fmt.Errorf("incomplete ciphertext read: %d/%d", n, dataLen)
	}

	// Split nonce (12 bytes for GCM) and ciphertext
	if len(data) < 12 {
		return nil, fmt.Errorf("data too short for nonce: %d < 12", len(data))
	}
	nonce := data[:12]
	ciphertext := data[12:]

	// Ciphertext must include auth tag (16 bytes)
	if len(ciphertext) < 16 {
		return nil, fmt.Errorf("ciphertext too short: %d < 16", len(ciphertext))
	}

	// Decrypt
	plaintext, err := e.encryptor.Decrypt(ciphertext, nonce, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt error: %w", err)
	}

	return plaintext, nil
}

// ReadAll reads and decrypts the entire file, returning all contents at once.
// Returns the decrypted data and any error encountered.
func (e *EncryptedFile) ReadAll() ([]byte, error) {
	if e.mode == WriteMode {
		return nil, fmt.Errorf("file not opened for reading")
	}

	e.mtx.Lock()
	defer e.mtx.Unlock()

	// Estimate output size from file size (plaintext is usually ~85-95% of ciphertext for typical data)
	// Each chunk has: 4-byte length + 12-byte nonce + ciphertext + 16-byte auth tag
	// So overhead per chunk is ~32 bytes minimum
	var estimatedSize int64
	if stat, err := e.file.Stat(); err == nil {
		fileSize := stat.Size()
		// Conservative estimate: assume 90% efficiency
		estimatedSize = fileSize * 9 / 10
		if estimatedSize < 0 {
			estimatedSize = 0
		}
	}

	// Pre-allocate output buffer
	var result []byte
	if estimatedSize > 0 {
		result = make([]byte, 0, estimatedSize)
	}

	// Read and decrypt all remaining chunks directly into result
	for !e.eof {
		plaintext, err := e.readNextChunk()
		if err != nil {
			if err == io.EOF {
				e.eof = true
				break
			}
			return nil, err
		}
		result = append(result, plaintext...)
	}

	return result, nil
}

// Close closes the file.
// Returns any error encountered.
func (e *EncryptedFile) Close() error {
	if e.file != nil {
		return e.file.Close()
	}
	return nil
}

// Name returns the name of the file.
func (e *EncryptedFile) Name() string {
	return e.name
}

// Suffix returns the file extension/suffix (".enc" for encrypted files)
func (e *EncryptedFile) Suffix() string {
	return encryption.EncSuffix
}
