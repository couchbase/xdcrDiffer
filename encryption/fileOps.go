package encryption

import "os"

type FileOps interface {
	GetEncryptionFilenameSuffix() string
	WriteEncHeader(fileDescriptor *os.File) error
	ValidateHeader(fileDescriptor *os.File) (*os.File, bool, error)
	WriteToFile(fileDescriptor *os.File, data []byte) (int, error)

	OpenFile(fileName string) (FileReaderOps, error)
}

type FileReaderOps interface {
	ReadAndFillBytes(buffer []byte) (int, error)
}
