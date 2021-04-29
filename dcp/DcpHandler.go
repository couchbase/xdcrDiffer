// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package dcp

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	gocbcore "github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	xdcrParts "github.com/couchbase/goxdcr/base/filter"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"os"
	"sync"
	"xdcrDiffer/base"
	fdp "xdcrDiffer/fileDescriptorPool"
	"xdcrDiffer/utils"
)

// implements StreamObserver
type DcpHandler struct {
	dcpClient    *DcpClient
	fileDir      string
	index        int
	vbList       []uint16
	numberOfBins int
	dataChan     chan *Mutation
	waitGrp      sync.WaitGroup
	finChan      chan bool
	bucketMap    map[uint16]map[int]*Bucket
	fdPool       fdp.FdPoolIface
	logger       *xdcrLog.CommonLogger
	filter       xdcrParts.Filter
}

func NewDcpHandler(dcpClient *DcpClient, fileDir string, index int, vbList []uint16, numberOfBins, dataChanSize int, fdPool fdp.FdPoolIface) (*DcpHandler, error) {
	if len(vbList) == 0 {
		return nil, fmt.Errorf("vbList is empty for handler %v", index)
	}
	return &DcpHandler{
		dcpClient:    dcpClient,
		fileDir:      fileDir,
		index:        index,
		vbList:       vbList,
		numberOfBins: numberOfBins,
		dataChan:     make(chan *Mutation, dataChanSize),
		finChan:      make(chan bool),
		bucketMap:    make(map[uint16]map[int]*Bucket),
		fdPool:       fdPool,
		logger:       dcpClient.logger,
		filter:       dcpClient.dcpDriver.filter,
	}, nil
}

func (dh *DcpHandler) Start() error {
	err := dh.initialize()
	if err != nil {
		return err
	}

	dh.waitGrp.Add(1)
	go dh.processData()

	return nil
}

func (dh *DcpHandler) Stop() {
	close(dh.finChan)
	// this sometimes does not return after a long time
	//dh.waitGrp.Wait()

	dh.cleanup()
}

func (dh *DcpHandler) initialize() error {
	for _, vbno := range dh.vbList {
		innerMap := make(map[int]*Bucket)
		dh.bucketMap[vbno] = innerMap
		for i := 0; i < dh.numberOfBins; i++ {
			bucket, err := NewBucket(dh.fileDir, vbno, i, dh.fdPool, dh.logger)
			if err != nil {
				return err
			}
			innerMap[i] = bucket
		}
	}

	return nil
}

func (dh *DcpHandler) cleanup() {
	for _, vbno := range dh.vbList {
		innerMap := dh.bucketMap[vbno]
		if innerMap == nil {
			dh.logger.Warnf("Cannot find innerMap for vbno %v at cleanup\n", vbno)
			continue
		}
		for i := 0; i < dh.numberOfBins; i++ {
			bucket := innerMap[i]
			if bucket == nil {
				dh.logger.Warnf("Cannot find bucket for vbno %v and index %v at cleanup\n", vbno, i)
				continue
			}
			//fmt.Printf("%v DcpHandler closing bucket %v\n", dh.dcpClient.Name, i)
			bucket.close()
		}
	}
}

func (dh *DcpHandler) processData() {
	dh.logger.Debugf("%v DcpHandler %v processData starts..........\n", dh.dcpClient.Name, dh.index)
	defer dh.logger.Debugf("%v DcpHandler %v processData exits..........\n", dh.dcpClient.Name, dh.index)
	defer dh.waitGrp.Done()

	for {
		select {
		case <-dh.finChan:
			goto done
		case mut := <-dh.dataChan:
			dh.processMutation(mut)
		}
	}
done:
}

func (dh *DcpHandler) processMutation(mut *Mutation) {
	var matched bool
	var err error
	var errStr string
	var filterResult base.FilterResultType
	if dh.filter != nil && mut.IsMutation() {
		matched, err, errStr, _ = dh.filter.FilterUprEvent(mut.ToUprEvent())
		if !matched {
			filterResult = base.Filtered
		}
		if err != nil {
			filterResult = base.UnableToFilter
			dh.logger.Warnf("Err %v - (%v) when filtering mutation %v", err, errStr, mut)
		}
	}

	valid := dh.dcpClient.dcpDriver.checkpointManager.HandleMutationEvent(mut, filterResult)
	if !valid {
		// if mutation is out of range, ignore it
		return
	}

	// Ignore system events - we only care about actual data
	if mut.IsSystemEvent() {
		vbno := mut.vbno
		if vbno == 0 {
			fmt.Printf("NEIL DEBUG got system event for vb0\n")
		}
		return
	}

	vbno := mut.vbno
	index := utils.GetBucketIndexFromKey(mut.key, dh.numberOfBins)
	innerMap := dh.bucketMap[vbno]
	if innerMap == nil {
		panic(fmt.Sprintf("cannot find bucketMap for vbno %v", vbno))
	}
	bucket := innerMap[index]
	if bucket == nil {
		panic(fmt.Sprintf("cannot find bucket for index %v", index))
	}
	bucket.write(mut.Serialize())
}

func (dh *DcpHandler) writeToDataChan(mut *Mutation) {
	select {
	case dh.dataChan <- mut:
	// provides an alternative exit path when dh stops
	case <-dh.finChan:
	}
}

func (dh *DcpHandler) SnapshotMarker(startSeqno, endSeqno uint64, vbno uint16, streamID uint16, snapshotType gocbcore.SnapshotState) {
	dh.dcpClient.dcpDriver.checkpointManager.updateSnapshot(vbno, startSeqno, endSeqno)
}

func (dh *DcpHandler) Mutation(seqno, revId uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbno uint16, collectionID uint32, streamID uint16, key, value []byte) {
	dh.writeToDataChan(CreateMutation(vbno, key, seqno, revId, cas, flags, expiry, gomemcached.UPR_MUTATION, value, datatype, collectionID))
}

func (dh *DcpHandler) Deletion(seqno, revId uint64, deleteTime uint32, cas uint64, datatype uint8, vbno uint16, collectionID uint32, streamID uint16, key, value []byte) {
	dh.writeToDataChan(CreateMutation(vbno, key, seqno, revId, cas, 0, 0, gomemcached.UPR_DELETION, value, datatype, collectionID))
}

func (dh *DcpHandler) Expiration(seqno, revId uint64, deleteTime uint32, cas uint64, vbno uint16, collectionID uint32, streamID uint16, key []byte) {
	dh.writeToDataChan(CreateMutation(vbno, key, seqno, revId, cas, 0, 0, gomemcached.UPR_EXPIRATION, nil, 0, collectionID))
}

func (dh *DcpHandler) End(vbno uint16, streamID uint16, err error) {
	dh.dcpClient.dcpDriver.handleVbucketCompletion(vbno, err, "dcp stream ended")
}

func (dh *DcpHandler) CreateCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, collectionID uint32, ttl uint32, streamID uint16, key []byte) {
	dh.writeToDataChan(CreateMutation(vbID, key, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, collectionID))
}

func (dh *DcpHandler) DeleteCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, collectionID uint32, streamID uint16) {
	dh.writeToDataChan(CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, collectionID))
}

func (dh *DcpHandler) FlushCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, collectionID uint32) {
	// Don't care - not implemented anyway
}

func (dh *DcpHandler) CreateScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, streamID uint16, key []byte) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, scopeID))
}

func (dh *DcpHandler) DeleteScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, streamID uint16) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, scopeID))
}

func (dh *DcpHandler) ModifyCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, collectionID uint32, ttl uint32, streamID uint16) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, collectionID))
}

func (dh *DcpHandler) OSOSnapshot(vbID uint16, snapshotType uint32, streamID uint16) {
	// Don't care
}

func (dh *DcpHandler) SeqNoAdvanced(vbID uint16, bySeqno uint64, streamID uint16) {
	// Don't care
}

type Bucket struct {
	data []byte
	// current index in data for next write
	index    int
	file     *os.File
	fileName string

	fdPoolCb fdp.FileOp
	closeOp  func() error

	logger *xdcrLog.CommonLogger
}

func NewBucket(fileDir string, vbno uint16, bucketIndex int, fdPool fdp.FdPoolIface, logger *xdcrLog.CommonLogger) (*Bucket, error) {
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
		data:     make([]byte, base.BucketBufferCapacity),
		index:    0,
		file:     file,
		fileName: fileName,
		fdPoolCb: cb,
		closeOp:  closeOp,
		logger:   logger,
	}, nil
}

func (b *Bucket) write(item []byte) error {
	if b.index+len(item) > base.BucketBufferCapacity {
		err := b.flushToFile()
		if err != nil {
			return err
		}
	}

	copy(b.data[b.index:], item)
	b.index += len(item)
	return nil
}

func (b *Bucket) flushToFile() error {
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
		return fmt.Errorf("Incomplete write. expected=%v, actual=%v", b.index, numOfBytes)
	}
	b.index = 0
	return nil
}

func (b *Bucket) close() {
	err := b.flushToFile()
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

type Mutation struct {
	vbno     uint16
	key      []byte
	seqno    uint64
	revId    uint64
	cas      uint64
	flags    uint32
	expiry   uint32
	opCode   gomemcached.CommandCode
	value    []byte
	datatype uint8
	colId    uint32
}

func CreateMutation(vbno uint16, key []byte, seqno, revId, cas uint64, flags, expiry uint32, opCode gomemcached.CommandCode, value []byte, datatype uint8, collectionId uint32) *Mutation {
	return &Mutation{
		vbno:   vbno,
		key:    key,
		seqno:  seqno,
		revId:  revId,
		cas:    cas,
		flags:  flags,
		expiry: expiry,
		opCode: opCode,
		value:  value,
		colId:  collectionId,
	}
}

func (m *Mutation) IsExpiration() bool {
	return m.opCode == gomemcached.UPR_EXPIRATION
}

func (m *Mutation) IsDeletion() bool {
	return m.opCode == gomemcached.UPR_DELETION
}

func (m *Mutation) IsMutation() bool {
	return m.opCode == gomemcached.UPR_MUTATION
}

func (m *Mutation) IsSystemEvent() bool {
	return m.opCode == gomemcached.DCP_SYSTEM_EVENT
}

func (m *Mutation) ToUprEvent() *mcc.UprEvent {
	return &mcc.UprEvent{
		Opcode:       m.opCode,
		VBucket:      m.vbno,
		DataType:     m.datatype,
		Flags:        m.flags,
		Expiry:       m.expiry,
		Key:          m.key,
		Value:        m.value,
		Cas:          m.cas,
		Seqno:        m.seqno,
		CollectionId: m.colId,
	}
}

// serialize mutation into []byte
// format:
//  keyLen   - 2 bytes
//  key  - length specified by keyLen
//  seqno    - 8 bytes
//  revId    - 8 bytes
//  cas      - 8 bytes
//  flags    - 4 bytes
//  expiry   - 4 bytes
//  opType   - 2 byte
//  datatype - 2 byte
//  hash     - 64 bytes
//  collectionId - 4 bytes
func (mut *Mutation) Serialize() []byte {
	keyLen := len(mut.key)
	ret := make([]byte, keyLen+base.BodyLength+2)
	bodyHash := sha512.Sum512(mut.value)

	pos := 0
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(keyLen))
	pos += 2
	copy(ret[pos:pos+keyLen], mut.key)
	pos += keyLen
	binary.BigEndian.PutUint64(ret[pos:pos+8], mut.seqno)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], mut.revId)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], mut.cas)
	pos += 8
	binary.BigEndian.PutUint32(ret[pos:pos+4], mut.flags)
	pos += 4
	binary.BigEndian.PutUint32(ret[pos:pos+4], mut.expiry)
	pos += 4
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(mut.opCode))
	pos += 2
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(mut.datatype))
	pos += 2
	copy(ret[pos:], bodyHash[:])
	pos += 64
	binary.BigEndian.PutUint32(ret[pos:pos+4], mut.colId)

	return ret
}
