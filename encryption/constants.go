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
	// EncSuffix is the file suffix for encrypted files
	EncSuffix = ".enc"
	// SaltLen is the length of the salt used in key derivation (16 bytes)
	SaltLen = 16
)

const SaltPrefix = "Couchbase Encrypted File/"

const (
	// KeyLen is the length of the encryption key (32 bytes for AES-256)
	KeyLen = 32
	// CalibrationBudget is the max time spent benchmarking PBKDF2
	CalibrationBudget = 3 * time.Second
	// TargetDerivationTime is the target time for a single key derivation
	TargetDerivationTime = 100 * time.Millisecond
)

// Compression denotes the compression algorithm used
type Compression int

const (
	// Nocompression denotes no compression
	Nocompression Compression = iota
	// Snappy denotes Snappy compression
	Snappy
	// Zlib denotes Zlib compression
	Zlib
	// Gzip denotes Gzip compression
	Gzip
	// Zstd denotes Zstandard compression
	Zstd
	// Bzip2 denotes Bzip2 compression
	Bzip2
)

// Version denotes the encryption file format version
type Version int

const (
	// VersionNoKeyDerivation denotes version 0 (no password based key derivation support)
	VersionNoKeyDerivation Version = iota
	// VersionKeyDerivation denotes version 1 (with password based key derivation support)
	VersionKeyDerivation
)

// KDF denotes the key derivation function used
type KDF int

const (
	// PBKDF2 denotes the PBKDF2 key derivation function
	PBKDF2 KDF = iota
)

// MagicLength is the length of the magic header for encrypted files
const MagicLength = 21

// HeaderLength is the length of the encryption header
const HeaderLength = 80

// FileMagic denotes the 21 bytes expected magic for encrypted files
var FileMagic = []byte("\x00Couchbase Encrypted\x00")

// The encrypted file header (as specified in
// https://github.com/couchbase/platform/blob/master/cbcrypto/EncryptedFileFormat.md)
// stores a 4-bit exponent in the key-derivation parameter.
//
// The iteration count is computed as:
//
//	iterations = 1024 * (2^exponent)
//
// Since the exponent is 4 bits wide, its maximum value is 15.
// Therefore, the maximum iteration count is:
//
//	1024 * 2^15 = 33,554,432
const MaxIterations = 1024 * 32768 // 33,554,432

// KeyID is the identifier for the encryption key.
// For password-based key derivation, this is simply "password".
const KeyID = "password"

// Errors related to encryption
var ErrorAlreadyEnabled = errors.New("encryption at rest is already enabled")
var ErrorEncryptionFormatUnrecog = errors.New("encryption file format unrecognized")
var ErrorEncryptionFormatUnsupported = errors.New("encryption file format unsupported")
var ErrorCompressionUnsupported = errors.New("compression algorithm unsupported")
var ErrorKDFUnsupported = errors.New("key derivation function unsupported")
