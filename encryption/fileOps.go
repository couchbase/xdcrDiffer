package encryption

import "os"

type FileOps interface {
	GetEncryptionFilenameSuffix() string
	WriteEncHeader(fileDescriptor *os.File) error
	ValidateHeader(fileDescriptor *os.File) (bool, error)
}
