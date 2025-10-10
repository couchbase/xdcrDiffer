package encryption

import "errors"

var ErrorAlreadyEnabled = errors.New("encryption at rest is already enabled")

type EncryptionSvc interface {
	InitAESGCM256(passPhrase string) error
}
