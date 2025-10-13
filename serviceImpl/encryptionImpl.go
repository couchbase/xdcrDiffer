package serviceImpl

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"strconv"
	"sync"
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

func splitNonceAndCipherText(input []byte) ([]byte, []byte, error) {
	if len(input) < 12 {
		return nil, nil, fmt.Errorf("nonce+ciphertext too short to contain nonce: %d < 12", len(input))
	}
	nonce := input[:12]
	ciphertext := input[12:]
	return nonce, ciphertext, nil
}

type aes256Config struct {
	iteration uint64
	key       []byte
	salt      []byte // 128-bits (16 bytes)
}

type decryptorReaderCtx struct {
	mtx sync.Mutex

	decryptor func([]byte, []byte) ([]byte, error)

	enabled bool
	logger  *xdcrLog.CommonLogger
	config  *aes256Config
	file    *os.File

	decryptedBuf   []byte
	bytesDecrypted int
}

type EncryptionServiceImpl struct {
	enabled      uint32
	nonceCounter uint64

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
	if e == nil {
		return fmt.Errorf("encryption service is nil")
	}
	if !atomic.CompareAndSwapUint32(&e.enabled, 0, 1) {
		// already enabled
		return encryption.ErrorAlreadyEnabled
	}

	e.logger.Infof("Initializing encryption at rest with AES-GCM-256 and calculating key...")
	// Generate a salt given the time and some random data
	randInt := mrand.Int()
	salt := []byte(time.Now().String() + strconv.Itoa(randInt))

	budgetDur := 3 * time.Second
	targetDerivationDur := 100 * time.Millisecond
	// AES-256 requires a 32-byte key
	keyLen := 32
	iterToUse := CalibrateIterations([]byte(passPhrase), salt, keyLen, budgetDur, targetDerivationDur)
	e.logger.Infof(fmt.Sprintf("PBKDF2 iteration count calibrated to %d (target %s, budget %s)", iterToUse, targetDerivationDur, budgetDur))

	derivedKey := deriveKey([]byte(passPhrase), salt, iterToUse, keyLen)
	e.config = &aes256Config{
		iteration: uint64(iterToUse),
		salt:      salt,
		key:       derivedKey,
	}
	return nil
}

func (e *EncryptionServiceImpl) GetEncryptionFilenameSuffix() string {
	if e == nil || !e.IsEnabled() {
		return ""
	}
	return encryption.EncSuffix
}

// Encrypt will take a plain text, and return
// 1a. ciphertext if encryption is enabled
// 1b. plaintext as-is if encryption is disabled
// In both cases, the second return value is the nonce (nil if encryption is disabled).
// The return "output" value of Encrypt will contain the nonce prepended
// Error is returned if encryption is enabled but fails.
func (e *EncryptionServiceImpl) Encrypt(plaintext []byte) ([]byte, []byte, error) {
	if e == nil || !e.IsEnabled() {
		// No encryption; return plaintext as-is
		return plaintext, nil, nil
	}

	if e.config == nil || e.config.key == nil {
		return nil, nil, fmt.Errorf("encryption config not initialized properly")
	}

	block, err := aes.NewCipher(e.config.key)
	if err != nil {
		return nil, nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}

	// The nonce must be unique for each encryption operation with the same key.
	// GCM recommends a 12-byte nonce.
	nonce := make([]byte, gcm.NonceSize())

	// Use an ever-incrementing counter for the first 8 bytes to guarantee uniqueness.
	counter := atomic.AddUint64(&e.nonceCounter, 1)
	binary.BigEndian.PutUint64(nonce[:8], counter)
	// Fill the remaining bytes (if any) with cryptographic randomness.
	if len(nonce) > 8 {
		if _, err = io.ReadFull(rand.Reader, nonce[8:]); err != nil {
			return nil, nil, err
		}
	}

	out := make([]byte, 0, len(nonce)+len(plaintext)+gcm.Overhead())
	out = append(out, nonce...)
	out = gcm.Seal(out, nonce, plaintext, nil)
	return out, nonce, nil
}

// Beginning of each encryption file is composed of
// 1. Magic header
// 2. 128-bits of unencrypted salt
// 3. uint64 of PBKDF2 iteration count
// 4. Encrypted header
func (e *EncryptionServiceImpl) WriteEncHeader(fileDescriptor *os.File) error {
	if e == nil || !e.IsEnabled() {
		// Do nothing
		return nil
	}

	// Sanity check
	if e.config == nil {
		return fmt.Errorf("encryption config not initialized")
	}
	// nil check for fd
	if fileDescriptor == nil {
		return fmt.Errorf("file descriptor is nil")
	}

	// 1. Magic header
	if n, err := fileDescriptor.Write(encryption.FileMagic); err != nil {
		return err
	} else if n != len(encryption.FileMagic) {
		return fmt.Errorf("failed to write magic header: wrote %d of %d bytes", n, len(encryption.FileMagic))
	}

	// 2. 128-bits (16 bytes) salt (truncate or pad with zeros to 16 bytes)
	salt := make([]byte, encryption.SaltLen)
	copy(salt, e.config.salt)
	if n, err := fileDescriptor.Write(salt); err != nil {
		return err
	} else if n != len(salt) {
		return fmt.Errorf("failed to write salt: wrote %d of %d bytes", n, len(salt))
	}

	// 3. uint64 PBKDF2 iteration count (big endian)
	var iterBuf [encryption.IterLen]byte
	binary.BigEndian.PutUint64(iterBuf[:], e.config.iteration)
	if n, err := fileDescriptor.Write(iterBuf[:]); err != nil {
		return err
	} else if n != len(iterBuf) {
		return fmt.Errorf("failed to write iteration count: wrote %d of %d bytes", n, len(iterBuf))
	}

	// 4. An encrypted magic
	// Encrypted writes is in the format of
	// [nonce][ciphertextLen][ciphertext]
	// where nonce is of variable length depending on the cipher (12 bytes for GCM)
	// and ciphertextLen is uint32 big endian

	encryptedHeader, nonce, err := e.Encrypt(encryption.FileMagic)
	if err != nil {
		return fmt.Errorf("failed to encrypt header: %w", err)
	}

	// nonce
	if n, err := fileDescriptor.Write(nonce); err != nil {
		return fmt.Errorf("failed to write nonce: %w", err)
	} else if n != len(nonce) {
		return fmt.Errorf("failed to write nonce: wrote %d of %d bytes", n, len(nonce))
	}

	// ciphertext length
	ciphertextLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(ciphertextLenBuf, uint32(len(encryptedHeader)))
	if n, err := fileDescriptor.Write(ciphertextLenBuf); err != nil {
		return fmt.Errorf("failed to write ciphertext length: %w", err)
	} else if n != len(ciphertextLenBuf) {
		return fmt.Errorf("failed to write ciphertext length: wrote %d of %d bytes", n, len(ciphertextLenBuf))
	}

	// ciphertext
	if n, err := fileDescriptor.Write(encryptedHeader); err != nil {
		return err
	} else if n != len(encryptedHeader) {
		return fmt.Errorf("failed to write encrypted header: wrote %d of %d bytes", n, len(encryptedHeader))
	}
	return nil
}

// ValidateHeader reads and validates the encryption header of the given file.
// If encryption is disabled, it returns true without reading the file.
// If encryption is enabled, it checks the magic header, salt, and iteration count.
// If valid and e.config is not yet set, it populates e.config with the salt and iteration count (key remains nil).
// Returns (true, nil) if valid, (false, nil) if not valid, or (false, err) if an error occurs.
// Also returns the fd that is ready for other read operations
func (e *EncryptionServiceImpl) ValidateHeader(fileDescriptor *os.File) (*os.File, bool, error) {
	if fileDescriptor == nil {
		return nil, false, fmt.Errorf("file descriptor is nil")
	}

	if e == nil || !e.IsEnabled() {
		// If encryption is disabled, we consider all files valid
		return nil, true, nil
	}

	headerLen := len(encryption.FileMagic) + encryption.SaltLen + encryption.IterLen

	// Ensure file has enough bytes
	stat, err := fileDescriptor.Stat()
	if err != nil {
		return nil, false, fmt.Errorf("stat error: %w", err)
	}
	if stat.Size() < int64(headerLen) {
		return nil, false, fmt.Errorf("file too small for header: %d < %d", stat.Size(), headerLen)
	}

	// Seek to start
	if _, err = fileDescriptor.Seek(0, 0); err != nil {
		return nil, false, fmt.Errorf("seek error: %w", err)
	}

	// Read full header into buffer
	buf := make([]byte, headerLen)
	if n, err := io.ReadFull(fileDescriptor, buf); err != nil {
		return nil, false, fmt.Errorf("read header error: %w", err)
	} else if n < headerLen {
		return nil, false, fmt.Errorf("incomplete read of header: %d < %d", n, headerLen)
	}

	offset := 0

	// 1. Magic header
	magicLen := len(encryption.FileMagic)
	if !bytes.Equal(buf[offset:offset+magicLen], encryption.FileMagic) {
		return nil, false, nil
	}
	offset += magicLen

	// 2. Salt
	salt := make([]byte, encryption.SaltLen)
	copy(salt, buf[offset:offset+encryption.SaltLen])
	offset += encryption.SaltLen

	// 3. Iteration count
	iter := binary.BigEndian.Uint64(buf[offset : offset+encryption.IterLen])
	if iter == 0 {
		return nil, false, fmt.Errorf("invalid iteration count 0")
	}

	// 4. encrypted magic
	// Encrypted magic section: [nonce(12)][ciphertextLen(4)][ciphertext]
	nonce := make([]byte, 12)
	if n, err := io.ReadFull(fileDescriptor, nonce); err != nil {
		return nil, false, fmt.Errorf("read nonce error: %w", err)
	} else if n < len(nonce) {
		return nil, false, fmt.Errorf("incomplete read of nonce: %d < %d", n, len(nonce))
	}

	ciphertextLenBuf := make([]byte, 4)
	if n, err := io.ReadFull(fileDescriptor, ciphertextLenBuf); err != nil {
		return nil, false, fmt.Errorf("read ciphertext length error: %w", err)
	} else if n < len(ciphertextLenBuf) {
		return nil, false, fmt.Errorf("incomplete read of ciphertext length: %d < %d", n, len(ciphertextLenBuf))
	}
	ciphertextLen := binary.BigEndian.Uint32(ciphertextLenBuf)

	if ciphertextLen == 0 || ciphertextLen > 10*1024 {
		return nil, false, fmt.Errorf("invalid ciphertext length %d", ciphertextLen)
	}

	ciphertext := make([]byte, ciphertextLen)
	if n, err := io.ReadFull(fileDescriptor, ciphertext); err != nil {
		return nil, false, fmt.Errorf("read ciphertext error: %w", err)
	} else if n < int(ciphertextLen) {
		return nil, false, fmt.Errorf("incomplete read of ciphertext: %d < %d", n, ciphertextLen)
	}

	// Populate config if enabled and not yet set
	if atomic.LoadUint32(&e.enabled) == 1 && e.config == nil {
		e.config = &aes256Config{
			iteration: iter,
			salt:      salt,
			// key cannot be reconstructed without passphrase; leave nil
		}
	}
	return fileDescriptor, true, nil
}

// WriteToFile is a method that will write to a file descriptor the data either in
// encrypted or unencrypted form.
// It abstracts the actual writing and encryption logic away from the caller.
// If encryption is disabled, it writes the data as-is.
// If encryption is enabled, it encrypts the data and writes the encrypted form.
// It returns the number of bytes *of the data* it had written and any error encountered
// If encryption is enabled, there may be more bytes written to the file but the returned number of bytes
// will still be the length of data itself.
func (e *EncryptionServiceImpl) WriteToFile(fileDescriptor *os.File, data []byte) (int, error) {
	if !e.IsEnabled() {
		// Encryption disabled; write as-is
		return fileDescriptor.Write(data)
	}

	cipherTextWithNonce, nonce, err := e.Encrypt(data)
	if err != nil {
		return 0, err
	}
	if nonce == nil {
		return 0, fmt.Errorf("encryption enabled but nonce is nil")
	}

	// Write the length of nonce and ciphertext as uint32 big endian
	var nonceAndCiphertextLen [4]byte
	binary.BigEndian.PutUint32(nonceAndCiphertextLen[:], uint32(len(cipherTextWithNonce)))

	// Write [nonceAndCipherTextLen][nonce][ciphertext]
	if n, err := fileDescriptor.Write(nonceAndCiphertextLen[:]); err != nil {
		return 0, fmt.Errorf("failed to write nonce and ciphertext length: %w", err)
	} else if n != len(nonceAndCiphertextLen) {
		return 0, fmt.Errorf("failed to write nonce and ciphertext length: wrote %d of %d bytes", n, len(nonceAndCiphertextLen))
	}

	if n, err := fileDescriptor.Write(cipherTextWithNonce); err != nil {
		return 0, fmt.Errorf("failed to write ciphertext: %w", err)
	} else if n != len(cipherTextWithNonce) {
		return 0, fmt.Errorf("failed to write ciphertext: wrote %d of %d bytes", n, len(cipherTextWithNonce))
	}

	return len(data), nil
}

func (e *EncryptionServiceImpl) IsEnabled() bool {
	return e != nil && atomic.LoadUint32(&e.enabled) == 1
}

// OpenFile returns a FileReaderOps that can be used to read and decrypt data from the given file
// It opens the file and then validates the header to ensure the encryption format is recognized
// If encryption is disabled, it returns a no-op decryptor that reads data as-is
func (e *EncryptionServiceImpl) OpenFile(fileName string) (encryption.FileReaderOps, error) {
	if e == nil {
		return nil, fmt.Errorf("encryption service is nil")
	}

	if e.config.key == nil && e.IsEnabled() {
		return nil, fmt.Errorf("encryption is enabled but key is not set")
	}

	decryptorCtx := &decryptorReaderCtx{
		logger: e.logger,
		config: e.config,
	}

	var err error
	decryptorCtx.file, err = os.Open(fileName)
	if err != nil {
		decryptorCtx.file.Close()
		return nil, fmt.Errorf("failed to open file %s: %w", fileName, err)
	}

	if !e.IsEnabled() {
		// Encryption disabled; return a no-op decryptor
		return decryptorCtx, nil
	}

	decryptorCtx.enabled = true
	decryptorCtx.decryptor = e.Decrypt

	decryptorCtx.decryptedBuf = make([]byte, 64) // arbitrary size
	var validHeader bool
	decryptorCtx.file, validHeader, err = e.ValidateHeader(decryptorCtx.file)
	if err != nil {
		decryptorCtx.file.Close()
		return nil, fmt.Errorf("failed to validate header for file %s: %w", fileName, err)
	}
	if !validHeader {
		decryptorCtx.file.Close()
		return nil, encryption.ErrorEncryptionFormatUnrecog
	}

	return decryptorCtx, nil
}

func (e *EncryptionServiceImpl) Decrypt(ciphertext, nonce []byte) ([]byte, error) {
	if e == nil || !e.IsEnabled() {
		// No encryption; return ciphertext as-is
		return ciphertext, nil
	}

	if e.config == nil || e.config.key == nil {
		return nil, fmt.Errorf("encryption config not initialized properly")
	}
	block, err := aes.NewCipher(e.config.key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}

// ReadAndFillBytes be passed in a buffer with a pre-allocated length
// The expectation is that this method will decrypt data and fill the buffer up with the number of bytes of
// decrypted data
// It would then return the number of bytes filled and any error encountered
// The context internally needs to dynamically decrypt data from the Encrypted (WriteToFile) format
// If the request asks for less bytes than the buffered data, the context will return the necessary decrypted data
// If the request asks for more data, the decryptorContext will need to ensure more data is decrypted and fill up
// the buffer to able to return decrypted data back to the caller
// Returns EOF if the end of file is reached cleanly
func (d *decryptorReaderCtx) ReadAndFillBytes(buffer []byte) (int, error) {
	if !d.enabled {
		// No encryption; read as-is
		return d.file.Read(buffer)
	}

	lenToRead := len(buffer)
	if lenToRead <= 0 {
		return 0, nil
	}

	if err := d.decryptAndFillInternalBuffer(lenToRead); err != nil {
		return 0, err
	}

	d.mtx.Lock()
	defer d.mtx.Unlock()

	if d.bytesDecrypted == 0 {
		// Reached EOF cleanly
		return 0, io.EOF
	}

	if lenToRead > d.bytesDecrypted {
		lenToRead = d.bytesDecrypted
	}

	copy(buffer, d.decryptedBuf[:lenToRead])
	// Shift remaining decrypted data to the front of the buffer
	copy(d.decryptedBuf, d.decryptedBuf[lenToRead:d.bytesDecrypted])
	d.bytesDecrypted -= lenToRead

	if d.bytesDecrypted < 0 {
		return 0, fmt.Errorf("internal error: bytesDecrypted < 0")
	} else if d.bytesDecrypted == 0 {
		return lenToRead, io.EOF
	} else {
		return lenToRead, nil
	}
}

func (d *decryptorReaderCtx) decryptAndFillInternalBuffer(lenToRead int) error {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	if lenToRead <= 0 {
		return nil
	}

	for d.bytesDecrypted < lenToRead {
		// Need to decrypt more data
		// First read the length of nonce and ciphertext stored as uint32 big endian
		var nonceAndCipherLen [4]byte
		if n, err := io.ReadFull(d.file, nonceAndCipherLen[:]); err != nil {
			if err == io.EOF && n == 0 {
				// Reached EOF cleanly
				return nil
			}
		} else if n != len(nonceAndCipherLen) {
			return fmt.Errorf("incomplete read of nonce+ciphertext length: %d < %d", n, len(nonceAndCipherLen))
		}

		// Read nonce and ciphertext into one and then split
		nonceAndCipherLenVal := binary.BigEndian.Uint32(nonceAndCipherLen[:])
		nonceAndCipherBuf := make([]byte, nonceAndCipherLenVal)
		if n, err := io.ReadFull(d.file, nonceAndCipherBuf[:nonceAndCipherLenVal]); err != nil {
			return fmt.Errorf("read nonce+ciphertext error: %w", err)
		} else if n != int(nonceAndCipherLenVal) {
			return fmt.Errorf("incomplete read of nonce+ciphertext: %d < %d", n, nonceAndCipherLenVal)
		}

		nonce, ciphertext, err := splitNonceAndCipherText(nonceAndCipherBuf)
		if err != nil {
			return fmt.Errorf("split nonce and ciphertext error: %w", err)
		}

		// AES-GCM auth tag is 16 bytes; plaintext len = len(ciphertext)-16
		if len(ciphertext) < 16 {
			return fmt.Errorf("ciphertext too short: %d < 16", len(ciphertext))
		}
		expectedPlainLen := len(ciphertext) - 16
		needed := d.bytesDecrypted + expectedPlainLen

		// Ensure decryptedBuf has enough length to hold new plaintext
		if len(d.decryptedBuf) < needed {
			newSize := len(d.decryptedBuf)
			if newSize == 0 {
				newSize = needed
			}
			for newSize < needed {
				// For small size, double the buffer. For large size, increase by needed-newSize
				if newSize < 1024 {
					newSize *= 2
				} else {
					newSize += needed - newSize
				}
			}
			newBuf := make([]byte, newSize)
			copy(newBuf, d.decryptedBuf[:d.bytesDecrypted])
			d.decryptedBuf = newBuf
		}

		plaintext, err := d.decryptor(ciphertext, nonce)
		if err != nil {
			return fmt.Errorf("decrypt error: %w", err)
		}
		if len(plaintext) != expectedPlainLen {
			return fmt.Errorf("unexpected plaintext length: got %d want %d", len(plaintext), expectedPlainLen)
		}

		copy(d.decryptedBuf[d.bytesDecrypted:], plaintext)
		d.bytesDecrypted += len(plaintext)
		continue
	}

	return nil
}
