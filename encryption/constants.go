// Copyright (c) 2025 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package encryption

import (
	"errors"
	"time"
)

const (
	EncSuffix = ".enc"
	SaltLen   = 16 // 128 bits of salt
	IterLen   = 8  // uint64 iteration count (big endian)
)

const (
	// KeyLen is the length of the encryption key (32 bytes for AES-256)
	KeyLen = 32
	// CalibrationBudget is the max time spent benchmarking PBKDF2
	CalibrationBudget = 3 * time.Second
	// TargetDerivationTime is the target time for a single key derivation
	TargetDerivationTime = 100 * time.Millisecond
)

// 8-byte magic header: 'ENC1' + version + reserved.
var FileMagic = []byte{'E', 'N', 'C', '1', 0x01, 0x00, 0x00, 0x00}
var ErrorAlreadyEnabled = errors.New("encryption at rest is already enabled")
var ErrorEncryptionFormatUnrecog = errors.New("encryption file format unrecognized")
