package filehandler

import (
	"fmt"
	"os"
	"sync"

	xdcrLog "github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/xdcrDiffer/base"
	fdp "github.com/couchbase/xdcrDiffer/fileDescriptorPool"
	"github.com/couchbase/xdcrDiffer/utils"
)

type Bucket struct {
	data []byte
	lock sync.RWMutex
	// current index in data for next write
	index    int
	file     *os.File
	fileName string

	fdPoolCb fdp.FileOp
	closeOp  func() error

	logger    *xdcrLog.CommonLogger
	bufferCap int
}
type FileHandler struct {
	fileDir             string
	fdPool              fdp.FdPoolIface
	numberOfVbuckets    uint16
	numberOfBins        int
	RequiresVBRemapping bool
	bufferCapacity      int
	BucketMap           map[uint16]map[int]*Bucket
	BucketLock          sync.RWMutex
	logger              *xdcrLog.CommonLogger
}

func NewBucket(fileDir string, vbno uint16, bucketIndex int, fdPool fdp.FdPoolIface, logger *xdcrLog.CommonLogger, bufferCap int) (*Bucket, error) {
	fileName := utils.GetFileName(fileDir, vbno, bucketIndex)
	var cb fdp.FileOp
	var closeOp func() error
	var err error
	var file *os.File

	if fdPool == nil {
		file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, base.FileModeReadWrite)
		if err != nil {
			return nil, err
		}
	} else {
		_, cb, err = fdPool.RegisterFileHandle(fileName)
		if err != nil {
			return nil, err
		}
		closeOp = func() error {
			return fdPool.DeRegisterFileHandle(fileName)
		}
	}
	return &Bucket{
		data:      make([]byte, bufferCap),
		index:     0,
		file:      file,
		fileName:  fileName,
		fdPoolCb:  cb,
		closeOp:   closeOp,
		logger:    logger,
		bufferCap: bufferCap,
	}, nil
}

func (b *Bucket) Write(item []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.index+len(item) > b.bufferCap {
		err := b.FlushToFile()
		if err != nil {
			return err
		}
	}

	copy(b.data[b.index:], item)
	b.index += len(item)
	return nil
}

// caller should lock the bucket
func (b *Bucket) FlushToFile() error {
	var numOfBytes int
	var err error
	if b.fdPoolCb != nil {
		numOfBytes, err = b.fdPoolCb(b.data[:b.index])
	} else {
		numOfBytes, err = b.file.Write(b.data[:b.index])
	}
	if err != nil {
		return err
	}
	if numOfBytes != b.index {
		return fmt.Errorf("incomplete write. expected=%v, actual=%v", b.index, numOfBytes)
	}
	b.index = 0
	return nil
}

func (b *Bucket) Close() {
	b.lock.Lock()
	defer b.lock.Unlock()
	err := b.FlushToFile()
	if err != nil {
		b.logger.Errorf("Error flushing to file %v at bucket close err=%v\n", b.fileName, err)
	}
	if b.fdPoolCb != nil {
		err = b.closeOp()
		if err != nil {
			b.logger.Errorf("Error closing file %v.  err=%v\n", b.fileName, err)
		}
	} else {
		err = b.file.Close()
		if err != nil {
			b.logger.Errorf("Error closing file %v.  err=%v\n", b.fileName, err)
		}
	}
}

func NewFileHandler(fileDir string, fdPool fdp.FdPoolIface, numberOfVbuckets uint16, numberOfBins int, bufferCapacity int, requiresVBRemapping bool, logger *xdcrLog.CommonLogger) *FileHandler {
	return &FileHandler{
		fileDir:             fileDir,
		fdPool:              fdPool,
		numberOfVbuckets:    numberOfVbuckets,
		numberOfBins:        numberOfBins,
		bufferCapacity:      bufferCapacity,
		RequiresVBRemapping: requiresVBRemapping,
		logger:              logger,
	}
}

func (fh *FileHandler) Initialize() error {
	if fh.RequiresVBRemapping {
		fh.numberOfVbuckets = base.TraditionalNumberOfVbuckets
	}
	fh.BucketMap = make(map[uint16]map[int]*Bucket)
	fh.BucketLock.Lock()
	defer fh.BucketLock.Unlock()
	var vbno uint16
	for vbno = 0; vbno < fh.numberOfVbuckets; vbno++ {
		innerMap := make(map[int]*Bucket)
		fh.BucketMap[vbno] = innerMap
		for bin := 0; bin < fh.numberOfBins; bin++ {
			bucket, err := NewBucket(fh.fileDir, vbno, bin, fh.fdPool, fh.logger, fh.bufferCapacity)
			if err != nil {
				return err
			}
			innerMap[bin] = bucket
		}
	}
	return nil
}

func (fh *FileHandler) GetBucket(docKey []byte, actualVbNo uint16) (*Bucket, error) {
	var bucket *Bucket
	var vbno uint16 = actualVbNo
	if fh.RequiresVBRemapping {
		vbno = utils.CbcVbMap(docKey, uint32(base.TraditionalNumberOfVbuckets))
	}
	index := utils.GetBucketIndexFromKey(docKey, fh.numberOfBins)
	fh.BucketLock.RLock()
	defer fh.BucketLock.RUnlock()
	innerMap := fh.BucketMap[vbno]
	if innerMap == nil {
		return nil, fmt.Errorf("cannot find bucketMap for Vbno %v", vbno)
	}
	bucket = innerMap[index]
	if bucket == nil {
		return nil, fmt.Errorf("cannot find bucket for index %v", index)
	}
	return bucket, nil
}

func (fh *FileHandler) Close() {
	var vbno uint16
	fh.BucketLock.RLock()
	defer fh.BucketLock.RUnlock()
	for vbno = 0; vbno < fh.numberOfVbuckets; vbno++ {
		innerMap := fh.BucketMap[vbno]
		if innerMap == nil {
			fh.logger.Warnf("Cannot find innerMap for Vbno %v at cleanup", vbno)
			continue
		}
		for bin := 0; bin < fh.numberOfBins; bin++ {
			bucket := innerMap[bin]
			if bucket == nil {
				fh.logger.Warnf("Cannot find bucket for Vbno %v and index %v at cleanup\n", vbno, bin)
				continue
			}
			bucket.Close()
		}
	}
}
