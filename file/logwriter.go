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
	"os"

	xdcrLog "github.com/couchbase/goxdcr/v8/log"
)

// stdoutTeeWriter wraps a File and writes to both the file and stdout.
type stdoutTeeWriter struct {
	file File
}

func (w *stdoutTeeWriter) Write(p []byte) (int, error) {
	// Write to stdout first (ignore errors - logging shouldn't fail due to stdout)
	_, _ = os.Stdout.Write(p)
	return w.file.Write(p)
}

// NewEncryptedLogWriter creates an io.Writer that writes encrypted data to the specified file.
// The writer also echoes all output to stdout.
// Returns the writer and a cleanup function that must be called when done.
func NewEncryptedLogWriter(factory *Factory, fileName string) (io.Writer, func() error, error) {
	if factory == nil {
		return nil, nil, fmt.Errorf("factory is nil")
	}

	// Check if file already exists - log files should be new
	if _, err := os.Stat(fileName); err == nil {
		return nil, nil, fmt.Errorf("file %s already exists", fileName)
	}

	file, err := factory.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600, WriteMode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create log file %s: %w", fileName, err)
	}

	writer := &stdoutTeeWriter{file: file}
	cleanup := func() error {
		return file.Close()
	}

	return writer, cleanup, nil
}

// NewEncryptedLoggerContext creates a goxdcr LoggerContext that writes encrypted logs.
// It returns the context, a cleanup function, and any error.
// The cleanup function must be called when logging is complete.
func NewEncryptedLoggerContext(factory *Factory, fileName string) (*xdcrLog.LoggerContext, func(), error) {
	writer, cleanup, err := NewEncryptedLogWriter(factory, fileName)
	if err != nil {
		return nil, nil, err
	}

	// Create log writers map for all log levels
	logWriters := make(map[xdcrLog.LogLevel]io.Writer)
	logWriters[xdcrLog.LogLevelFatal] = writer
	logWriters[xdcrLog.LogLevelError] = writer
	logWriters[xdcrLog.LogLevelWarn] = writer
	logWriters[xdcrLog.LogLevelInfo] = writer
	logWriters[xdcrLog.LogLevelDebug] = writer
	logWriters[xdcrLog.LogLevelTrace] = writer

	ctx := &xdcrLog.LoggerContext{
		Log_writers: logWriters,
		Log_level:   xdcrLog.LogLevelInfo,
	}

	// Wrap cleanup to match expected signature (no error return)
	doneCb := func() {
		_ = cleanup()
	}

	return ctx, doneCb, nil
}
