package serviceImpl

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	passphrase := "once only"
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
	passphrase := "correct horse battery staple"

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
	passphrase := "round trip pass"

	svc := newTestSvc()
	if err := svc.InitAESGCM256(passphrase); err != nil {
		t.Fatalf("InitAESGCM256 failed: %v", err)
	}

	plaintext := []byte("secret data payload")
	_, _, err := svc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// TODO NEIL - add decrypt
	//out, err := svc.Decrypt(ciphertext)
	//if err != nil {
	//	t.Fatalf("Decrypt failed: %v", err)
	//}
	//if !bytes.Equal(out, plaintext) {
	//	t.Fatalf("round-trip mismatch")
	//}
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
				//expLen := len(encryption.FileMagic) + len(e.config.salt) + binary.Size(e.config.iteration) +
				//if len(data) != expLen {
				//	t.Fatalf("unexpected header length %d want %d", len(data), expLen)
				//}
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

				// Encrypted magic section: [nonce(12)][ciphertextLen(4)][ciphertext]
				const nonceLen = 12
				baseLen := len(encryption.FileMagic) + 16 + 8 // magic + salt (16) + iteration (8)
				if len(data) < baseLen+nonceLen+4 {
					t.Fatalf("encrypted header too short: got %d need at least %d", len(data), baseLen+nonceLen+4)
				}
				encSection := data[baseLen:]
				nonceBytes := encSection[:nonceLen]
				if len(nonceBytes) != nonceLen {
					t.Fatalf("nonce length mismatch got %d want %d", len(nonceBytes), nonceLen)
				}
				cipherLenBytes := encSection[nonceLen : nonceLen+4]
				cipherLen := binary.BigEndian.Uint32(cipherLenBytes)
				ciphertext := encSection[nonceLen+4:]
				if uint32(len(ciphertext)) != cipherLen {
					t.Fatalf("ciphertext length mismatch got %d want %d", len(ciphertext), cipherLen)
				}
				expectedTotal := baseLen + nonceLen + 4 + int(cipherLen)
				if len(data) != expectedTotal {
					t.Fatalf("overall header length mismatch got %d want %d", len(data), expectedTotal)
				}

				assert := assert.New(t)
				_, err = fd.Seek(0, 0)
				assert.Nil(err)
				valid, err := e.ValidateHeader(fd)
				assert.True(valid)
				assert.Nil(err)
			}
		})
	}
}
