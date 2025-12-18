// Copyright (c) 2025 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package encryption

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"time"

	"golang.org/x/crypto/pbkdf2"
)

// DeriveKeyPBKDF2 derives an encryption key from a passphrase using PBKDF2.
func DeriveKeyPBKDF2(passphrase, salt []byte, iterations, keyLen int) []byte {
	return pbkdf2.Key(passphrase, salt, iterations, keyLen, sha256.New)
}

// GenerateSalt generates a salt of the specified length.
func GenerateSalt(length int) ([]byte, error) {
	randInt, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		return nil, fmt.Errorf("failed to generate random int: %w", err)
	}
	salt := []byte(time.Now().String() + strconv.Itoa(int(randInt.Int64())))
	// Trim the salt to salt length if it's longer than the desired length
	if len(salt) > length {
		salt = salt[:length]
	} else if len(salt) < length {
		// If the salt is shorter than the desired length, pad it with random data
		padding := make([]byte, length-len(salt))
		if _, err := io.ReadFull(rand.Reader, padding); err != nil {
			return nil, err
		}
		salt = append(salt, padding...)
	}
	return salt, nil
}

// CalibrateIterations benchmarks PBKDF2 within a time budget to find an iteration
// count whose single derivation time is close to targetDerivationDur.
func CalibrateIterations(passphrase, salt []byte, keyLen int, budgetDur, targetDerivationDur time.Duration) int {
	// Minimum sane starting point.
	iter := 1000
	start := time.Now()

	// Measure helper.
	measure := func(it int) (time.Duration, bool) {
		if time.Since(start) >= budgetDur {
			return 0, false
		}
		t0 := time.Now()
		_ = DeriveKeyPBKDF2(passphrase, salt, it, keyLen)
		return time.Since(t0), true
	}

	// Handle extremely fast systems (avoid zero duration).
	dur, ok := measure(iter)
	if !ok {
		return iter
	}

	maxIter := 1_000_000
	for dur < time.Millisecond && iter < maxIter {
		iter *= 2
		dur, ok = measure(iter)
		if !ok {
			return iter
		}
	}

	// Exponential search to overshoot targetDerivationDur.
	lowIter := iter
	highIter, highDur := iter, dur
	for highDur < targetDerivationDur && time.Since(start) < budgetDur && highIter < maxIter {
		highIter *= 2
		dur, ok = measure(highIter)
		if !ok {
			break
		}
		highDur = dur
		lowIter = highIter / 2 // update lower bound
	}

	// If even doubled upper bound is still below target, return highest tested.
	if highDur < targetDerivationDur || time.Since(start) >= budgetDur || highIter >= maxIter {
		return highIter
	}

	// Binary search between lowIter and highIter.
	best := lowIter
	for lowIter <= highIter && time.Since(start) < budgetDur {
		mid := (lowIter + highIter) / 2
		dur, ok = measure(mid)
		if !ok {
			break
		}
		if dur <= targetDerivationDur {
			best = mid
			lowIter = mid + 1
		} else {
			highIter = mid - 1
		}
	}

	return best
}
