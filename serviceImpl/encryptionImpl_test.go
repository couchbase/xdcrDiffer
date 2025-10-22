package serviceImpl

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	xdcrLog "github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/xdcrDiffer/encryption"
	"github.com/stretchr/testify/assert"
)

func newTestSvc() *EncryptionServiceImpl {
	return NewEncryptionService()
}

func TestInitAESGCM256_Idempotent(t *testing.T) {
	passphrase := []byte("once only")
	svc := newTestSvc()

	if err := svc.InitAESGCM256(passphrase); err != nil {
		t.Fatalf("first InitAESGCM256 failed: %v", err)
	}
	err := svc.InitAESGCM256(passphrase)
	if err == nil {
		t.Fatalf("expected error on second InitAESGCM256")
	}
	if !errors.Is(err, encryption.ErrorAlreadyEnabled) {
		t.Fatalf("expected ErrorAlreadyEnabled, got %v", err)
	}
}

func TestInitAESGCM256_DerivesKey(t *testing.T) {
	passphrase := []byte("correct horse battery staple")

	svc := newTestSvc()
	if err := svc.InitAESGCM256(passphrase); err != nil {
		t.Fatalf("InitAESGCM256 failed: %v", err)
	}

	// Access internal config (tests are in same package).
	cfg := svc.config
	if cfg == nil {
		t.Fatalf("config not initialized")
	}

	// Re-derive expected key using the same salt & iteration.
	expected := deriveKey([]byte(passphrase), cfg.salt, int(cfg.iteration), len(cfg.key))

	if !bytes.Equal(expected, cfg.key) {
		t.Fatalf("derived key mismatch")
	}
	if len(cfg.key) != 32 {
		t.Fatalf("expected 32-byte key, got %d", len(cfg.key))
	}
}

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	passphrase := []byte("round trip pass")

	svc := newTestSvc()
	if err := svc.InitAESGCM256(passphrase); err != nil {
		t.Fatalf("InitAESGCM256 failed: %v", err)
	}

	plaintext := []byte("secret data payload")
	ciphertext, _, err := svc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	nonce, actualCipherText, err := splitNonceAndCipherText(ciphertext)
	assert.NoError(t, err, "SplitNonce failed")

	out, err := svc.Decrypt(actualCipherText, nonce)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}
	if !bytes.Equal(out, plaintext) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestEncryptionServiceImpl_GetEncryptionFilenameSuffix(t *testing.T) {
	type fields struct {
		enabled uint32
		logger  *xdcrLog.CommonLogger
		config  *aes256Config
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "enabled returns suffix",
			fields: fields{enabled: 1},
			want:   encryption.EncSuffix,
		},
		{
			name:   "disabled returns empty",
			fields: fields{enabled: 0},
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EncryptionServiceImpl{
				enabled: tt.fields.enabled,
				logger:  tt.fields.logger,
				config:  tt.fields.config,
			}
			if got := e.GetEncryptionFilenameSuffix(); got != tt.want {
				t.Errorf("GetEncryptionFilenameSuffix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEncryptionServiceImpl_WriteEncHeader_And_ValidateHeader(t *testing.T) {
	type fields struct {
		enabled uint32
		logger  *xdcrLog.CommonLogger
		config  *aes256Config
	}
	type args struct {
		fileDescriptor *os.File
	}

	passPhrase := []byte("header awesome passphrase")
	salt := []byte("abcdefghijklmnop") // 16 bytes
	encryptionKey := deriveKey(passPhrase, salt, 12345, 32)

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:    "nil fd disabled encryptor",
			fields:  fields{enabled: 0},
			args:    args{fileDescriptor: nil},
			wantErr: false,
		},
		{
			name: "nil fd enabled encryptor",
			fields: fields{
				enabled: 1,
				config:  &aes256Config{iteration: 12345, key: encryptionKey, salt: salt},
			},
			args:    args{fileDescriptor: nil},
			wantErr: true,
		},
		{
			name:    "non-nil fd disabled encryptor",
			fields:  fields{enabled: 0},
			wantErr: false,
		},
		{
			name: "non-nil fd enabled encryptor",
			fields: fields{
				enabled: 1,
				config:  &aes256Config{iteration: 12345, key: encryptionKey, salt: salt},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EncryptionServiceImpl{
				enabled: tt.fields.enabled,
				logger:  tt.fields.logger,
				config:  tt.fields.config,
			}

			var fd *os.File
			var origContent []byte
			var err error

			switch tt.name {
			case "non-nil fd disabled encryptor":
				fd, err = os.CreateTemp("", "enc_test_disabled")
				if err != nil {
					t.Fatalf("temp file: %v", err)
				}
				defer os.Remove(fd.Name())
				defer fd.Close()
				if _, err = fd.Write([]byte("original data")); err != nil {
					t.Fatalf("write: %v", err)
				}
				if err = fd.Sync(); err != nil {
					t.Fatalf("sync: %v", err)
				}
				if _, err = fd.Seek(0, 0); err != nil {
					t.Fatalf("seek: %v", err)
				}
				origContent, _ = os.ReadFile(fd.Name())

			case "non-nil fd enabled encryptor":
				fd, err = os.CreateTemp("", "enc_test_enabled")
				if err != nil {
					t.Fatalf("temp file: %v", err)
				}
				defer os.Remove(fd.Name())
				defer fd.Close()
				info, _ := fd.Stat()
				if info.Size() != 0 {
					t.Fatalf("expected empty file before write")
				}
			}

			callFD := tt.args.fileDescriptor
			if fd != nil {
				callFD = fd
			}

			if err := e.WriteEncHeader(callFD); (err != nil) != tt.wantErr {
				t.Errorf("WriteEncHeader() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			switch tt.name {
			case "non-nil fd disabled encryptor":
				after, _ := os.ReadFile(fd.Name())
				if string(after) != string(origContent) {
					t.Errorf("file modified when encryption disabled. got=%q want=%q", string(after), string(origContent))
				}
			case "non-nil fd enabled encryptor":
				data, _ := os.ReadFile(fd.Name())
				// Magic
				if string(data[:len(encryption.FileMagic)]) != string(encryption.FileMagic) {
					t.Errorf("magic mismatch")
				}
				// Salt
				salt := data[len(encryption.FileMagic) : len(encryption.FileMagic)+16]
				if string(salt) != "abcdefghijklmnop" {
					t.Errorf("salt mismatch got %q", string(salt))
				}
				// Iteration
				iterBytes := data[len(encryption.FileMagic)+16:]
				iter := binary.BigEndian.Uint64(iterBytes)
				if iter != e.config.iteration {
					t.Errorf("iteration mismatch got %d want %d", iter, e.config.iteration)
				}

				assert := assert.New(t)
				_, err = fd.Seek(0, 0)
				assert.Nil(err)
				_, valid, err := e.ValidateHeader(fd)
				assert.True(valid)
				assert.Nil(err)
			}
		})
	}
}

// New test focusing on the described OpenFile flow.
func TestEncryptionServiceImpl_OpenFile_Success(t *testing.T) {
	svc := newTestSvc()

	// Random passphrase
	rawPass := make([]byte, 32)
	_, err := rand.Read(rawPass)
	if err != nil {
		t.Fatalf("rand.Read passphrase: %v", err)
	}
	passphrase := []byte(hex.EncodeToString(rawPass))

	// Init service
	if err := svc.InitAESGCM256(passphrase); err != nil {
		t.Fatalf("InitAESGCM256 failed: %v", err)
	}

	// Random plaintext payload
	plaintext := make([]byte, 256)
	if _, err := rand.Read(plaintext); err != nil {
		t.Fatalf("rand.Read data: %v", err)
	}

	// Create temp file with encryption suffix
	assert.NotEqual(t, "", svc.GetEncryptionFilenameSuffix())
	pattern := "openfile_*" + svc.GetEncryptionFilenameSuffix()
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	fileName := f.Name()
	defer os.Remove(fileName)

	// Write encryption header
	if err := svc.WriteEncHeader(f); err != nil {
		t.Fatalf("WriteEncHeader: %v", err)
	}

	// Explicit Encrypt call (per requirement); discard result
	cipherPreview, noncePreview, err := svc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt preview failed: %v", err)
	}
	if len(cipherPreview) == 0 || len(noncePreview) == 0 {
		t.Fatalf("preview encryption produced empty output")
	}

	// Write data using WriteToFile (will encrypt again)
	n, err := svc.WriteToFile(f, plaintext)
	if err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}
	if n != len(plaintext) {
		t.Fatalf("WriteToFile reported %d bytes, want %d", n, len(plaintext))
	}

	// Close before reopening
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Re-open via OpenFile
	handle, err := svc.OpenFile(fileName)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if handle == nil {
		t.Fatalf("OpenFile returned nil handle")
	}

	buf := make([]byte, len(plaintext))
	readN, err := handle.ReadAndFillBytes(buf)
	assert.Nil(t, err)
	assert.Equal(t, len(plaintext), readN)
	assert.True(t, bytes.Equal(plaintext, buf))

	_ = bytes.MinRead // keep bytes import until read test is enabled
	assert.NotNil(t, handle)
}

func TestEncryptionServiceImpl_Encrypt(t *testing.T) {
	type fields struct {
		enabled      uint32
		nonceCounter uint64
		logger       *xdcrLog.CommonLogger
		config       *aes256Config
	}
	type args struct {
		plaintext []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		want1   []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "encryption disabled returns plaintext",
			fields: fields{
				enabled: 0,
			},
			args:    args{plaintext: []byte("hello world")},
			want:    []byte("hello world"),
			want1:   nil,
			wantErr: assert.NoError,
		},
		{
			name: "encryption enabled success",
			fields: fields{
				enabled: 1,
				config: func() *aes256Config {
					pass := []byte("test passphrase")
					salt := []byte("1234567890abcdef") // 16 bytes
					iter := 5000
					key := deriveKey(pass, salt, iter, 32)
					return &aes256Config{
						iteration: uint64(iter),
						key:       key,
						salt:      salt,
					}
				}(),
			},
			args:    args{plaintext: []byte("secret payload")},
			want:    nil, // validated dynamically
			want1:   nil,
			wantErr: assert.NoError,
		},
		{
			name: "encryption enabled missing config error",
			fields: fields{
				enabled: 1,
				config:  nil,
			},
			args:    args{plaintext: []byte("data")},
			want:    nil,
			want1:   nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EncryptionServiceImpl{
				enabled:      tt.fields.enabled,
				nonceCounter: tt.fields.nonceCounter,
				logger:       tt.fields.logger,
				config:       tt.fields.config,
			}
			got, nonce, err := e.Encrypt(tt.args.plaintext)
			if !tt.wantErr(t, err, fmt.Sprintf("Encrypt(%v)", tt.args.plaintext)) {
				return
			}
			switch tt.name {
			case "encryption disabled returns plaintext":
				assert.Equal(t, tt.args.plaintext, got)
				assert.Nil(t, nonce)
			case "encryption enabled success":
				assert.NotNil(t, nonce)
				assert.NotEmpty(t, nonce)
				assert.NotNil(t, got)
				assert.NotEqual(t, tt.args.plaintext, got) // ciphertext should differ
			case "encryption enabled missing config error":
				// error already asserted
			default:
				assert.Equalf(t, tt.want, got, "Encrypt(%v)", tt.args.plaintext)
				assert.Equalf(t, tt.want1, nonce, "Encrypt(%v)", tt.args.plaintext)
			}
		})
	}
}

func TestEncryptionServiceImpl_Decrypt(t *testing.T) {
	type fields struct {
		enabled      uint32
		nonceCounter uint64
		logger       *xdcrLog.CommonLogger
		config       *aes256Config
	}
	type args struct {
		ciphertext []byte
		nonce      []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "disabled returns input unchanged",
			fields: fields{
				enabled: 0,
			},
			args: args{
				ciphertext: []byte("plain data"),
				nonce:      []byte("ignorednonce"),
			},
			want:    []byte("plain data"),
			wantErr: assert.NoError,
		},
		{
			name: "enabled success roundtrip",
			fields: fields{
				enabled: 1,
				config: func() *aes256Config {
					pass := []byte("rt pass")
					salt := []byte("1234567890abcdef")
					iter := 2000
					key := deriveKey(pass, salt, iter, 32)
					return &aes256Config{iteration: uint64(iter), key: key, salt: salt}
				}(),
			},
			args: func() args {
				pass := []byte("rt pass")
				salt := []byte("1234567890abcdef")
				iter := 2000
				key := deriveKey(pass, salt, iter, 32)
				tmp := &EncryptionServiceImpl{enabled: 1, config: &aes256Config{iteration: uint64(iter), key: key, salt: salt}}
				plaintext := []byte("roundtrip plaintext")
				full, nonce, _ := tmp.Encrypt(plaintext)
				ct := full[len(nonce):]
				return args{ciphertext: ct, nonce: nonce}
			}(),
			want:    []byte("roundtrip plaintext"),
			wantErr: assert.NoError,
		},
		{
			name: "enabled tampered ciphertext error",
			fields: fields{
				enabled: 1,
				config: func() *aes256Config {
					pass := []byte("tamper pass")
					salt := []byte("abcdefghijklmnop")
					iter := 3000
					key := deriveKey(pass, salt, iter, 32)
					return &aes256Config{iteration: uint64(iter), key: key, salt: salt}
				}(),
			},
			args: func() args {
				pass := []byte("tamper pass")
				salt := []byte("abcdefghijklmnop")
				iter := 3000
				key := deriveKey(pass, salt, iter, 32)
				tmp := &EncryptionServiceImpl{enabled: 1, config: &aes256Config{iteration: uint64(iter), key: key, salt: salt}}
				full, nonce, _ := tmp.Encrypt([]byte("secret payload"))
				ct := append([]byte{}, full[len(nonce):]...)
				ct[0] ^= 0xFF
				return args{ciphertext: ct, nonce: nonce}
			}(),
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "enabled wrong nonce error",
			fields: fields{
				enabled: 1,
				config: func() *aes256Config {
					pass := []byte("wrong nonce pass")
					salt := []byte("1234567890abcdef")
					iter := 3500
					key := deriveKey(pass, salt, iter, 32)
					return &aes256Config{iteration: uint64(iter), key: key, salt: salt}
				}(),
			},
			args: func() args {
				pass := []byte("wrong nonce pass")
				salt := []byte("1234567890abcdef")
				iter := 3500
				key := deriveKey(pass, salt, iter, 32)
				tmp := &EncryptionServiceImpl{enabled: 1, config: &aes256Config{iteration: uint64(iter), key: key, salt: salt}}
				full, nonce, _ := tmp.Encrypt([]byte("another secret"))
				ct := full[len(nonce):]
				badNonce := append([]byte{}, nonce...)
				badNonce[0] ^= 0xFF
				return args{ciphertext: ct, nonce: badNonce}
			}(),
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "enabled missing config error",
			fields: fields{
				enabled: 1,
				config:  nil,
			},
			args: args{
				ciphertext: []byte("irrelevant"),
				nonce:      []byte("badnoncehere"),
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EncryptionServiceImpl{
				enabled:      tt.fields.enabled,
				nonceCounter: tt.fields.nonceCounter,
				logger:       tt.fields.logger,
				config:       tt.fields.config,
			}
			got, err := e.Decrypt(tt.args.ciphertext, tt.args.nonce)
			if !tt.wantErr(t, err, fmt.Sprintf("Decrypt(%v, %v)", tt.args.ciphertext, tt.args.nonce)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Decrypt(%v, %v)", tt.args.ciphertext, tt.args.nonce)
		})
	}
}

func TestEncryptionServiceImpl_WriteToFile(t *testing.T) {
	type fields struct {
		enabled      uint32
		nonceCounter uint64
		logger       *xdcrLog.CommonLogger
		config       *aes256Config
	}
	type args struct {
		fileDescriptor *os.File
		data           []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "write plaintext when encryption disabled",
			fields: fields{
				enabled: 0,
			},
			args: args{
				fileDescriptor: nil, // will create inside test
				data:           []byte("unencrypted plain content"),
			},
			want:    len([]byte("unencrypted plain content")),
			wantErr: assert.NoError,
		},
		{
			name: "write encrypted returns plaintext length",
			fields: fields{
				enabled: 1,
				config: func() *aes256Config {
					pass := []byte("write passphrase")
					salt := []byte("saltforwritetest1") // 16 bytes
					iter := 7777
					key := deriveKey(pass, salt, iter, 32)
					return &aes256Config{iteration: uint64(iter), key: key, salt: salt}
				}(),
			},
			args: args{
				fileDescriptor: nil,
				data:           []byte("secret content that will be encrypted"),
			},
			want:    len([]byte("secret content that will be encrypted")),
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EncryptionServiceImpl{
				enabled:      tt.fields.enabled,
				nonceCounter: tt.fields.nonceCounter,
				logger:       tt.fields.logger,
				config:       tt.fields.config,
			}

			// Prepare temp file
			var f *os.File
			var err error
			if tt.fields.enabled == 1 {
				// must carry encryption suffix
				f, err = os.CreateTemp("", "write_enc_*"+encryption.EncSuffix)
				if err != nil {
					t.Fatalf("CreateTemp: %v", err)
				}
				// header first
				if err = e.WriteEncHeader(f); err != nil {
					t.Fatalf("WriteEncHeader: %v", err)
				}
			} else {
				f, err = os.CreateTemp("", "write_plain_*")
				if err != nil {
					t.Fatalf("CreateTemp: %v", err)
				}
			}
			defer os.Remove(f.Name())
			defer f.Close()

			got, err := e.WriteToFile(f, tt.args.data)
			if !tt.wantErr(t, err, fmt.Sprintf("WriteToFile(%v, %v)", f, len(tt.args.data))) {
				return
			}
			assert.Equalf(t, tt.want, got, "returned byte count should equal plaintext length")

			// Flush & inspect
			_ = f.Sync()
			info, _ := f.Stat()

			if tt.fields.enabled == 0 {
				// File contents must equal original data
				content, _ := os.ReadFile(f.Name())
				assert.Equal(t, string(tt.args.data), string(content))
				assert.Equal(t, int64(len(tt.args.data)), info.Size())
			} else {
				// On-disk size should be larger than plaintext length (header + nonce + tag)
				if int64(got) >= info.Size() {
					t.Fatalf("expected encrypted file size > returned bytes. fileSize=%d returned=%d", info.Size(), got)
				}
				// Re-open and decrypt via service
				f.Close()
				handle, err := e.OpenFile(f.Name())
				assert.NoError(t, err)
				assert.NotNil(t, handle)
				buf := make([]byte, len(tt.args.data))
				readN, err := handle.ReadAndFillBytes(buf)
				assert.Nil(t, err)
				assert.Equal(t, len(tt.args.data), readN)
				assert.Equal(t, tt.args.data, buf)
			}
			assert.Equalf(t, tt.want, got, "WriteToFile(%v, %v)", tt.args.fileDescriptor, tt.args.data)
		})
	}
}

func TestActualSmallDataForEncrypt(t *testing.T) {
	// Boilerplate: initialize encryption service with random passphrase.
	passphrase := []byte(hex.EncodeToString(randomBytes(t, 32)))

	svc := newTestSvc()
	if err := svc.InitAESGCM256(passphrase); err != nil {
		t.Fatalf("InitAESGCM256 failed: %v", err)
	}

	// Confirm service is enabled.
	if svc.GetEncryptionFilenameSuffix() == "" {
		t.Fatalf("encryption suffix empty; service not enabled")
	}

	// Small plaintext to encrypt.
	plaintext := []byte("{\"10\":[\"d2\"]}")

	// Create temp file with encryption suffix and write encrypted plaintext.
	f, err := os.CreateTemp("", "smalldata_*"+svc.GetEncryptionFilenameSuffix())
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())
	// Write header first.
	if err := svc.WriteEncHeader(f); err != nil {
		t.Fatalf("WriteEncHeader failed: %v", err)
	}
	// Write encrypted data (returns plaintext length).
	n, err := svc.WriteToFile(f, plaintext)
	if err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}
	if n != len(plaintext) {
		t.Fatalf("WriteToFile returned %d want %d", n, len(plaintext))
	}
	f.Close()

	// Initialize a new decryptor service with the same passphrase and reopen file
	decryptorSvc := newTestSvc()
	passPhraseGetter := func() ([]byte, error) {
		return passphrase, nil
	}
	// Reopen and verify decrypted round trip using new service
	r, err := decryptorSvc.OpenFileForDecrypting(f.Name(), passPhraseGetter)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	buf := make([]byte, len(plaintext))
	readN, _ := r.ReadAndFillBytes(buf)
	if readN != len(plaintext) {
		t.Fatalf("read %d want %d", readN, len(plaintext))
	}
	if !bytes.Equal(buf, plaintext) {
		t.Fatalf("content mismatch")
	}
}

// Helper to create random bytes.
func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

func TestDecryptorReaderCtx_ReadAndFillBytes_RoundTrip(t *testing.T) {
	type tc struct {
		name           string
		totalSize      int
		writeChunks    []int // pattern for WriteToFile chunking
		readBufPattern []int // pattern for ReadAndFillBytes buffer sizes
	}
	tests := []tc{
		//{
		//	name:           "small_chunks_small_reads",
		//	totalSize:      4096,
		//	writeChunks:    []int{64, 32, 128, 256},
		//	readBufPattern: []int{17, 33, 64, 95},
		//},
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
			// 1. Random passphrase and plaintext
			passphrase := []byte(hex.EncodeToString(randomBytes(t, 32)))
			plaintext := randomBytes(t, tt.totalSize)

			// 2. Init service
			svc := newTestSvc()
			if err := svc.InitAESGCM256(passphrase); err != nil {
				t.Fatalf("InitAESGCM256: %v", err)
			}

			// 3. Temp file with suffix
			pattern := "readfill_*" + svc.GetEncryptionFilenameSuffix()
			f, err := os.CreateTemp("", pattern)
			if err != nil {
				t.Fatalf("CreateTemp: %v", err)
			}
			fileName := f.Name()
			defer os.Remove(fileName)

			// 4. Write header
			if err := svc.WriteEncHeader(f); err != nil {
				t.Fatalf("WriteEncHeader: %v", err)
			}

			// 5. Write encrypted chunks
			offset := 0
			wcIdx := 0
			for offset < len(plaintext) {
				chunkSize := tt.writeChunks[wcIdx%len(tt.writeChunks)]
				wcIdx++
				if chunkSize > len(plaintext)-offset {
					chunkSize = len(plaintext) - offset
				}
				chunk := plaintext[offset : offset+chunkSize]
				n, err := svc.WriteToFile(f, chunk)
				if err != nil {
					t.Fatalf("WriteToFile: %v", err)
				}
				if n != len(chunk) {
					t.Fatalf("WriteToFile returned %d expected %d", n, len(chunk))
				}
				offset += chunkSize
			}

			// Close writer before reading
			if err := f.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			// 6. Open file (produces decryptorReaderCtx)
			reader, err := svc.OpenFile(fileName)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			defer func() {
				// Close underlying file if possible
				if dc, ok := reader.(*decryptorReaderCtx); ok && dc.file != nil {
					_ = dc.file.Close()
				}
			}()

			// 7. Repeated reads
			var assembled bytes.Buffer
			readIdx := 0
			for {
				bufSize := tt.readBufPattern[readIdx%len(tt.readBufPattern)]
				readIdx++
				buf := make([]byte, bufSize)
				n, err := reader.ReadAndFillBytes(buf)
				if n > 0 {
					assembled.Write(buf[:n])
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("ReadAndFillBytes error: %v", err)
				}
			}

			// 8. Validate
			got := assembled.Bytes()
			assert.Equal(t, len(plaintext), len(got), "length mismatch")
			assert.True(t, bytes.Equal(plaintext, got), "content mismatch")
		})
	}
}

// Additional test focusing on zero-length final read edge.
func TestDecryptorReaderCtx_ReadAndFillBytes_ZeroLengthFinal(t *testing.T) {
	passphrase := []byte(hex.EncodeToString(randomBytes(t, 16)))
	data := randomBytes(t, 2048)

	svc := newTestSvc()
	if err := svc.InitAESGCM256(passphrase); err != nil {
		t.Fatalf("InitAESGCM256: %v", err)
	}

	f, err := os.CreateTemp("", "zero_final_*"+svc.GetEncryptionFilenameSuffix())
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := f.Name()
	defer os.Remove(name)

	if err := svc.WriteEncHeader(f); err != nil {
		t.Fatalf("WriteEncHeader: %v", err)
	}
	if _, err := svc.WriteToFile(f, data); err != nil {
		t.Fatalf("WriteToFile: %v", err)
	}
	f.Close()

	r, err := svc.OpenFile(name)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	buf := make([]byte, 1500)
	n1, err1 := r.ReadAndFillBytes(buf)
	assert.NoError(t, err1)
	assert.Equal(t, 1500, n1)

	// Second read gets remainder
	buf2 := make([]byte, 1000)
	n2, err2 := r.ReadAndFillBytes(buf2)
	assert.Nil(t, err2)
	assert.Equal(t, len(data)-n1, n2)
	combined := append(buf[:n1], buf2[:n2]...)
	assert.True(t, bytes.Equal(data, combined))
}

func Test_decryptorReaderCtx_ReadFile(t *testing.T) {
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
			passphrase := []byte(hex.EncodeToString(randomBytes(t, 32)))
			svc := newTestSvc()
			if err := svc.InitAESGCM256(passphrase); err != nil {
				t.Fatalf("InitAESGCM256: %v", err)
			}

			f, err := os.CreateTemp("", fmt.Sprintf("readfile_%s_*%s", c.name, svc.GetEncryptionFilenameSuffix()))
			if err != nil {
				t.Fatalf("CreateTemp: %v", err)
			}
			defer os.Remove(f.Name())

			if err := svc.WriteEncHeader(f); err != nil {
				t.Fatalf("WriteEncHeader: %v", err)
			}

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
					if n, err := svc.WriteToFile(f, original[offset:offset+cs]); err != nil {
						t.Fatalf("WriteToFile: %v", err)
					} else if n != cs {
						t.Fatalf("WriteToFile returned %d want %d", n, cs)
					}
					offset += cs
				}
			}

			if err := f.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			handle, err := svc.OpenFile(f.Name())
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			dc, ok := handle.(*decryptorReaderCtx)
			if !ok {
				t.Fatalf("expected *decryptorReaderCtx")
			}

			out, err := dc.ReadFile()
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if !bytes.Equal(original, out) {
				t.Fatalf("decrypted mismatch size=%d", c.size)
			}
		})
	}
}
