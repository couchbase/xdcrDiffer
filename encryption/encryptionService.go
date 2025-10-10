package encryption

import "errors"

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
	InitAESGCM256(passPhrase string) error
	Encrypt(plaintext []byte) ([]byte, []byte, error)
}
