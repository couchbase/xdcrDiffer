package encryption

import (
	"errors"

	xdcrLog "github.com/couchbase/goxdcr/v8/log"
)

const (
	EncSuffix = ".enc"
	SaltLen   = 16 // 128 bits of salt
	IterLen   = 8  // uint64 iteration count (big endian)
)

// 8-byte magic header: 'ENC1' + version + reserved.
var FileMagic = []byte{'E', 'N', 'C', '1', 0x01, 0x00, 0x00, 0x00}

// Validate header before attempting decryption.
func IsValidHeader(hdr []byte) bool {
	if len(hdr) < len(FileMagic) {
		return false
	}
	for i := range FileMagic {
		if hdr[i] != FileMagic[i] {
			return false
		}
	}
	return true
}

var ErrorAlreadyEnabled = errors.New("encryption at rest is already enabled")
var ErrorEncryptionFormatUnrecog = errors.New("encryption file format unrecognized")

type EncryptionSvc interface {
	FileOps
	IsEnabled() bool
	InitAESGCM256(passPhrase string) error
	Encrypt(plaintext []byte) ([]byte, []byte, error)
	DecryptFile(fileName string, passphraseGetter func() (string, error)) ([]byte, error)
	GetLoggerContext(fileName string) (*xdcrLog.LoggerContext, func(), error)
}
