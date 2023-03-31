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
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrParts "github.com/couchbase/goxdcr/base/filter"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	xdcrUtils "github.com/couchbase/goxdcr/utils"
	"os"
	"strings"
	"sync"
	"xdcrDiffer/base"
	fdp "xdcrDiffer/fileDescriptorPool"
	"xdcrDiffer/utils"
)

// implements StreamObserver
type DcpHandler struct {
	dcpClient               *DcpClient
	fileDir                 string
	index                   int
	vbList                  []uint16
	numberOfBins            int
	dataChan                chan *Mutation
	waitGrp                 sync.WaitGroup
	finChan                 chan bool
	bucketMap               map[uint16]map[int]*Bucket
	fdPool                  fdp.FdPoolIface
	logger                  *xdcrLog.CommonLogger
	filter                  xdcrParts.Filter
	incrementCounter        func()
	incrementSysCounter     func()
	colMigrationFilters     []string
	colMigrationFiltersOn   bool // shortcut to avoid len() check
	colMigrationFiltersImpl []xdcrParts.Filter
	isSource                bool
	utils                   xdcrUtils.UtilsIface
	bufferCap               int
	migrationMapping        metadata.CollectionNamespaceMapping
}

func NewDcpHandler(dcpClient *DcpClient, fileDir string, index int, vbList []uint16, numberOfBins, dataChanSize int, fdPool fdp.FdPoolIface, incReceivedCounter, incSysEvtReceived func(), colMigrationFilters []string, utils xdcrUtils.UtilsIface, bufferCap int, migrationMapping metadata.CollectionNamespaceMapping) (*DcpHandler, error) {
	if len(vbList) == 0 {
		return nil, fmt.Errorf("vbList is empty for handler %v", index)
	}
	return &DcpHandler{
		dcpClient:             dcpClient,
		fileDir:               fileDir,
		index:                 index,
		vbList:                vbList,
		numberOfBins:          numberOfBins,
		dataChan:              make(chan *Mutation, dataChanSize),
		finChan:               make(chan bool),
		bucketMap:             make(map[uint16]map[int]*Bucket),
		fdPool:                fdPool,
		logger:                dcpClient.logger,
		filter:                dcpClient.dcpDriver.filter,
		incrementCounter:      incReceivedCounter,
		incrementSysCounter:   incSysEvtReceived,
		colMigrationFilters:   colMigrationFilters,
		colMigrationFiltersOn: len(colMigrationFilters) > 0,
		utils:                 utils,
		isSource:              strings.Contains(dcpClient.Name, base.SourceClusterName),
		bufferCap:             bufferCap,
		migrationMapping:      migrationMapping,
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

func (d *DcpHandler) compileMigrCollectionFiltersIfNeeded() error {
	if len(d.colMigrationFilters) == 0 {
		return nil
	}

	for i, filterStr := range d.colMigrationFilters {
		filter, err := xdcrParts.NewFilter(fmt.Sprintf("%d", i), filterStr, d.utils, true)
		if err != nil {
			return fmt.Errorf("compiling %v resulted in: %v", filterStr, err)
		}
		d.colMigrationFiltersImpl = append(d.colMigrationFiltersImpl, filter)
	}
	return nil
}

func (dh *DcpHandler) initialize() error {
	for _, vbno := range dh.vbList {
		innerMap := make(map[int]*Bucket)
		dh.bucketMap[vbno] = innerMap
		for i := 0; i < dh.numberOfBins; i++ {
			bucket, err := NewBucket(dh.fileDir, vbno, i, dh.fdPool, dh.logger, dh.bufferCap)
			if err != nil {
				return err
			}
			innerMap[i] = bucket
		}
	}

	if err := dh.compileMigrCollectionFiltersIfNeeded(); err != nil {
		return err
	}
	return nil
}

func (dh *DcpHandler) cleanup() {
	for _, vbno := range dh.vbList {
		innerMap := dh.bucketMap[vbno]
		if innerMap == nil {
			dh.logger.Warnf("Cannot find innerMap for Vbno %v at cleanup\n", vbno)
			continue
		}
		for i := 0; i < dh.numberOfBins; i++ {
			bucket := innerMap[i]
			if bucket == nil {
				dh.logger.Warnf("Cannot find bucket for Vbno %v and index %v at cleanup\n", vbno, i)
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
	var replicationFilterResult base.FilterResultType

	replicationFilterResult = dh.replicationFilter(mut, matched, replicationFilterResult)
	valid := dh.dcpClient.dcpDriver.checkpointManager.HandleMutationEvent(mut, replicationFilterResult)
	if !valid {
		// if mutation is out of range, ignore it
		return
	}

	dh.incrementCounter()

	// Ignore system events - we only care about actual data
	if mut.IsSystemEvent() {
		dh.incrementSysCounter()
		return
	}

	var filterIdsMatched []uint8
	if dh.colMigrationFiltersOn && dh.isSource {
		dh.checkColMigrationDataCloned(mut)

		filterIdsMatched = dh.checkColMigrationFilters(mut)
		if len(filterIdsMatched) == 0 {
			return
		}
	}

	vbno := mut.Vbno
	index := utils.GetBucketIndexFromKey(mut.Key, dh.numberOfBins)
	innerMap := dh.bucketMap[vbno]
	if innerMap == nil {
		panic(fmt.Sprintf("cannot find bucketMap for Vbno %v", vbno))
	}
	bucket := innerMap[index]
	if bucket == nil {
		panic(fmt.Sprintf("cannot find bucket for index %v", index))
	}

	if dh.colMigrationFiltersOn && len(filterIdsMatched) > 0 {
		mut.ColFiltersMatched = filterIdsMatched
	}
	bucket.write(mut.Serialize())
}

func (dh *DcpHandler) replicationFilter(mut *Mutation, matched bool, filterResult base.FilterResultType) base.FilterResultType {
	var err error
	var errStr string
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
	return filterResult
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

func (dh *DcpHandler) checkColMigrationFilters(mut *Mutation) []uint8 {
	var filterIdsMatched []uint8
	for i, filter := range dh.colMigrationFiltersImpl {
		// If at least one passed, let it through
		matched, _, _, _ := filter.FilterUprEvent(mut.ToUprEvent())
		if matched {
			filterIdsMatched = append(filterIdsMatched, uint8(i))
		}
	}
	return filterIdsMatched
}

func (dh *DcpHandler) checkColMigrationDataCloned(mut *Mutation) {
	if dh.logger.GetLogLevel() != xdcrLog.LogLevelDebug {
		return
	}

	uprEvent := mut.ToUprEvent()
	dummyReq := &xdcrBase.WrappedMCRequest{}
	dummyReq.Req = &gomemcached.MCRequest{}
	matchedNamespaces, errMap, errMCReqMap := dh.migrationMapping.GetTargetUsingMigrationFilter(uprEvent, dummyReq, dh.logger)
	if len(matchedNamespaces) > 1 {
		dh.logger.Debugf("Document %v matched more than once: %v, errMap %v, errMCReqMap %v",
			matchedNamespaces.String(), errMap, errMCReqMap)
	}
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

	bufferCap int
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

func (b *Bucket) write(item []byte) error {
	if b.index+len(item) > b.bufferCap {
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
	Vbno              uint16
	Key               []byte
	Seqno             uint64
	RevId             uint64
	Cas               uint64
	Flags             uint32
	Expiry            uint32
	OpCode            gomemcached.CommandCode
	Value             []byte
	Datatype          uint8
	ColId             uint32
	ColFiltersMatched []uint8 // Given a ordered list of filters, this list contains indexes of the ordered list of filter that matched
}

func CreateMutation(vbno uint16, key []byte, seqno, revId, cas uint64, flags, expiry uint32, opCode gomemcached.CommandCode, value []byte, datatype uint8, collectionId uint32) *Mutation {
	return &Mutation{
		Vbno:     vbno,
		Key:      key,
		Seqno:    seqno,
		RevId:    revId,
		Cas:      cas,
		Flags:    flags,
		Expiry:   expiry,
		OpCode:   opCode,
		Value:    value,
		Datatype: datatype,
		ColId:    collectionId,
	}
}

func (m *Mutation) IsExpiration() bool {
	return m.OpCode == gomemcached.UPR_EXPIRATION
}

func (m *Mutation) IsDeletion() bool {
	return m.OpCode == gomemcached.UPR_DELETION
}

func (m *Mutation) IsMutation() bool {
	return m.OpCode == gomemcached.UPR_MUTATION
}

func (m *Mutation) IsSystemEvent() bool {
	return m.OpCode == gomemcached.DCP_SYSTEM_EVENT
}

func (m *Mutation) ToUprEvent() *xdcrBase.WrappedUprEvent {
	uprEvent := &mcc.UprEvent{
		Opcode:       m.OpCode,
		VBucket:      m.Vbno,
		DataType:     m.Datatype,
		Flags:        m.Flags,
		Expiry:       m.Expiry,
		Key:          m.Key,
		Value:        m.Value,
		Cas:          m.Cas,
		Seqno:        m.Seqno,
		CollectionId: m.ColId,
	}

	return &xdcrBase.WrappedUprEvent{
		UprEvent:     uprEvent,
		ColNamespace: nil,
		Flags:        0,
		ByteSliceGetter: func(size uint64) ([]byte, error) {
			return make([]byte, int(size)), nil
		},
	}
}

// serialize mutation into []byte
// format:
//
//	keyLen   - 2 bytes
//	Key  - length specified by keyLen
//	Seqno    - 8 bytes
//	RevId    - 8 bytes
//	Cas      - 8 bytes
//	Flags    - 4 bytes
//	Expiry   - 4 bytes
//	opType   - 2 byte
//	Datatype - 2 byte
//	hash     - 64 bytes
//	collectionId - 4 bytes
//	colFiltersLen - 2 byte (number of collection migration filters)
//	(per col filter) - 2 byte
func (mut *Mutation) Serialize() []byte {
	keyLen := len(mut.Key)
	ret := make([]byte, base.GetFixedSizeMutationLen(keyLen, mut.ColFiltersMatched))
	bodyHash := sha512.Sum512(mut.Value)

	pos := 0
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(keyLen))
	pos += 2
	copy(ret[pos:pos+keyLen], mut.Key)
	pos += keyLen
	binary.BigEndian.PutUint64(ret[pos:pos+8], mut.Seqno)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], mut.RevId)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], mut.Cas)
	pos += 8
	binary.BigEndian.PutUint32(ret[pos:pos+4], mut.Flags)
	pos += 4
	binary.BigEndian.PutUint32(ret[pos:pos+4], mut.Expiry)
	pos += 4
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(mut.OpCode))
	pos += 2
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(mut.Datatype))
	pos += 2
	copy(ret[pos:], bodyHash[:])
	pos += 64
	binary.BigEndian.PutUint32(ret[pos:pos+4], mut.ColId)
	pos += 4
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(len(mut.ColFiltersMatched)))
	pos += 2
	for _, colFilterId := range mut.ColFiltersMatched {
		binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(colFilterId))
		pos += 2
	}
	return ret
}
