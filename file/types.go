// Copyright (c) 2025 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// Package file provides abstractions for file I/O operations with optional encryption.
// Application code should use the File interface for all file operations.
package file

import (
	"io"
	"os"
	"sync"

	"github.com/couchbase/xdcrDiffer/encryption"
)

type FileMode int

const (
	ReadMode FileMode = iota
	WriteMode
	ReadWriteMode
)

// File is the primary interface for file operations in xdcrDiffer.
// It is an extension of io.ReadWriteCloser with metadata methods.
type File interface {
	io.ReadWriteCloser
	// ReadAll reads and returns the entire file contents.
	ReadAll() ([]byte, error)

	// Name returns the name of the file
	Name() string

	// Suffix returns the file extension/suffix (e.g., ".enc" for encrypted files, "" for plain)
	Suffix() string
}

// decryptBuffer holds decrypted data for chunked reads.
type decryptBuffer struct {
	mtx    sync.RWMutex
	buf    []byte // data buffer
	offset int    // read offset into buf
}

// EncryptedFile implements the File interface for encrypted files.
// It handles transparent encryption on Write and decryption on Read.
type EncryptedFile struct {
	// mtx is used to synchronize access to the file
	mtx sync.Mutex
	// file is the underlying file descriptor
	file *os.File
	// name is the name of the file
	name string
	// encryptor is the encryptor/decryptor used to encrypt/decrypt data
	encryptor encryption.EncryptorDecryptor
	// decryptBuf is the buffer used to store decrypted data
	decryptBuf decryptBuffer
	// mode is the mode of the file
	mode FileMode
	// eof denotes if the end of file has been reached
	eof bool
}

func newEncryptedFile(fd *os.File, name string, enc encryption.EncryptorDecryptor, mode FileMode) *EncryptedFile {
	return &EncryptedFile{
		file:      fd,
		name:      name,
		encryptor: enc,
		mode:      mode,
	}
}

// PlainFile implements the File interface for unencrypted files.
type PlainFile struct {
	// file is the underlying file descriptor
	file *os.File
	// name is the name of the file
	name string
}
