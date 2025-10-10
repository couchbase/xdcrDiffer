package serviceImpl

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	xdcrLog "github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/xdcrDiffer/encryption"
	"golang.org/x/crypto/pbkdf2"
)

// deriveKey uses PBKDF2 with the given iteration count.
func deriveKey(passphrase, salt []byte, iterations, keyLen int) []byte {
	return pbkdf2.Key(passphrase, salt, iterations, keyLen, sha256.New)
}

// CalibrateIterations benchmarks PBKDF2 within a time budget to find an iteration
// count whose single derivation time is close to targetDerivationDur (undershooting if needed).
// budgetDur limits total calibration time (e.g. 3 * time.Second).
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
		_ = deriveKey(passphrase, salt, it, keyLen)
		return time.Since(t0), true
	}

	// Handle extremely fast systems (avoid zero duration).
	dur, ok := measure(iter)
	if !ok {
		return iter
	}
	for dur < time.Millisecond && iter < 1_000_000 {
		iter *= 2
		dur, ok = measure(iter)
		if !ok {
			return iter
		}
	}

	// Exponential search to overshoot targetDerivationDur.
	lowIter, _ := iter, dur
	highIter, highDur := iter, dur
	for highDur < targetDerivationDur && time.Since(start) < budgetDur {
		highIter *= 2
		dur, ok = measure(highIter)
		if !ok {
			break
		}
		highDur = dur
		lowIter = highIter / 2 // update lower bound
	}

	// If even doubled upper bound is still below target, return highest tested.
	if highDur < targetDerivationDur || time.Since(start) >= budgetDur {
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

type aes256Config struct {
	iteration int
	key       []byte
}

type EncryptionServiceImpl struct {
	enabled uint32

	logger *xdcrLog.CommonLogger
	config *aes256Config
}

func NewEncryptionService() *EncryptionServiceImpl {
	logger := xdcrLog.NewLogger("differEncryption", xdcrLog.DefaultLoggerContext)
	return &EncryptionServiceImpl{
		logger: logger,
	}
}

func (e *EncryptionServiceImpl) InitAESGCM256(passPhrase string) error {
	if !atomic.CompareAndSwapUint32(&e.enabled, 0, 1) {
		// already enabled
		return encryption.ErrorAlreadyEnabled
	}

	e.logger.Infof("Initializing encryption at rest with AES-GCM-256 and calculating key...")
	// Generate a salt given the time and some random data
	randInt := rand.Int()
	salt := []byte(time.Now().String() + strconv.Itoa(randInt))

	budgetDur := 3 * time.Second
	targetDerivationDur := 100 * time.Millisecond
	// AES-256 requires a 32-byte key
	keyLen := 32
	iterToUse := CalibrateIterations([]byte(passPhrase), salt, keyLen, budgetDur, targetDerivationDur)
	e.logger.Infof(fmt.Sprintf("PBKDF2 iteration count calibrated to %d (target %s, budget %s)", iterToUse, targetDerivationDur, budgetDur))

	derivedKey := deriveKey([]byte(passPhrase), salt, iterToUse, keyLen)
	e.config = &aes256Config{
		iteration: iterToUse,
		key:       derivedKey,
	}
	return nil
}
