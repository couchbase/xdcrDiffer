package encryption

import "errors"

const (
	EncSuffix = ".enc"
)

// 8-byte magic header: 'ENC1' + version + reserved.
var FileMagic = [8]byte{'E', 'N', 'C', '1', 0x01, 0x00, 0x00, 0x00}

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

type EncryptionSvc interface {
	FileOps
	InitAESGCM256(passPhrase string) error
}
