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
	"fmt"
	"io"
)

var _ File = (*PlainFile)(nil)

// Read reads up to len(b) bytes from the file and stores them in b.
// Returns the number of bytes read and any error encountered.
func (p *PlainFile) Read(b []byte) (int, error) {
	return p.file.Read(b)
}

// Write writes len(b) bytes to the file.
// Returns the number of bytes written and any error encountered.
func (p *PlainFile) Write(b []byte) (int, error) {
	return p.file.Write(b)
}

// Close closes the file.
func (p *PlainFile) Close() error {
	if p.file != nil {
		return p.file.Close()
	}
	return nil
}

// Name returns the file name
func (p *PlainFile) Name() string {
	return p.name
}

// Suffix returns empty string for plain files
func (p *PlainFile) Suffix() string {
	return ""
}

// ReadAll reads the entire file contents optimally using page-aligned buffered IO.
// Pre-allocates based on file size when known to minimize reallocations.
func (p *PlainFile) ReadAll() ([]byte, error) {
	if p.file == nil {
		return nil, fmt.Errorf("file descriptor is nil")
	}

	if _, err := p.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek error: %w", err)
	}

	stat, err := p.file.Stat()
	if err != nil {
		return io.ReadAll(p.file)
	}

	size := stat.Size()
	if size <= 0 {
		return io.ReadAll(p.file)
	}

	buf := make([]byte, size)
	n, err := io.ReadFull(p.file, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	return buf[:n], nil
}
