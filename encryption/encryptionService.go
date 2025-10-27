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
var ErrorAlreadyEnabled = errors.New("encryption at rest is already enabled")
var ErrorEncryptionFormatUnrecog = errors.New("encryption file format unrecognized")

type PassphraseGetter func() ([]byte, error)

type EncryptionSvc interface {
	FileOps
	IsEnabled() bool
	Init(passPhrase []byte) error
	Encrypt(plaintext []byte) ([]byte, []byte, error)
	DecryptFile(fileName string, passphraseGetter PassphraseGetter) ([]byte, error)
	GetLoggerContext(fileName string) (*xdcrLog.LoggerContext, func(), error)
}
