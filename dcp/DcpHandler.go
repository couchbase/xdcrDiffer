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
	"os"
	"sort"
	"strings"
	"sync"
	"xdcrDiffer/base"
	fdp "xdcrDiffer/fileDescriptorPool"
	"xdcrDiffer/utils"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrParts "github.com/couchbase/goxdcr/base/filter"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	xdcrUtils "github.com/couchbase/goxdcr/utils"
)

// implements StreamObserver
type DcpHandler struct {
	dcpClient                     *DcpClient
	fileDir                       string
	index                         int
	vbList                        []uint16
	numberOfBins                  int
	dataChan                      chan *Mutation
	waitGrp                       sync.WaitGroup
	finChan                       chan bool
	bucketMap                     map[uint16]map[int]*Bucket
	fdPool                        fdp.FdPoolIface
	logger                        *xdcrLog.CommonLogger
	filter                        xdcrParts.Filter
	incrementCounter              func()
	incrementSysOrUnsubbedCounter func()
	colMigrationFilters           []string
	colMigrationFiltersOn         bool // shortcut to avoid len() check
	colMigrationFiltersImpl       []xdcrParts.Filter
	isSource                      bool
	utils                         xdcrUtils.UtilsIface
	bufferCap                     int
	migrationMapping              metadata.CollectionNamespaceMapping
	mobileCompatible              int
	expDelMode                    xdcrBase.FilterExpDelType
	xattraIterator                *xdcrBase.XattrIterator
}

func NewDcpHandler(dcpClient *DcpClient, fileDir string, index int, vbList []uint16, numberOfBins, dataChanSize int, fdPool fdp.FdPoolIface, incReceivedCounter, incSysOrUnsubbedEvtReceived func(), colMigrationFilters []string, utils xdcrUtils.UtilsIface, bufferCap int, migrationMapping metadata.CollectionNamespaceMapping) (*DcpHandler, error) {
	if len(vbList) == 0 {
		return nil, fmt.Errorf("vbList is empty for handler %v", index)
	}
	return &DcpHandler{
		dcpClient:                     dcpClient,
		fileDir:                       fileDir,
		index:                         index,
		vbList:                        vbList,
		numberOfBins:                  numberOfBins,
		dataChan:                      make(chan *Mutation, dataChanSize),
		finChan:                       make(chan bool),
		bucketMap:                     make(map[uint16]map[int]*Bucket),
		fdPool:                        fdPool,
		logger:                        dcpClient.logger,
		filter:                        dcpClient.dcpDriver.filter,
		incrementCounter:              incReceivedCounter,
		incrementSysOrUnsubbedCounter: incSysOrUnsubbedEvtReceived,
		colMigrationFilters:           colMigrationFilters,
		colMigrationFiltersOn:         len(colMigrationFilters) > 0,
		utils:                         utils,
		isSource:                      strings.Contains(dcpClient.Name, base.SourceClusterName),
		bufferCap:                     bufferCap,
		migrationMapping:              migrationMapping,
		mobileCompatible:              dcpClient.dcpDriver.mobileCompatible,
		expDelMode:                    dcpClient.dcpDriver.expDelMode,
		xattraIterator:                &xdcrBase.XattrIterator{},
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
		filter, err := xdcrParts.NewFilter(fmt.Sprintf("%d", i), filterStr, d.utils, d.expDelMode, d.mobileCompatible)
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

	// Ignore system events
	// Ignore unsubscribed events - mutations/events from collections not subscribed during OpenStream
	// we only care about actual data
	if mut.IsSystemOrUnsubbedEvent() {
		dh.incrementSysOrUnsubbedCounter()
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
	ret, err := mut.Serialize()
	if err != nil {
		dh.logger.Errorf("Error in Serializing the mutation pertaining to the document with the key:%v ,err:%v\n", mut.Key, err)
	} else {
		bucket.write(ret)
	}
}

func (dh *DcpHandler) replicationFilter(mut *Mutation, matched bool, filterResult base.FilterResultType) base.FilterResultType {
	var err error
	var errStr string
	if dh.filter != nil && mut.IsMutation() {
		matched, err, errStr, _, _ = dh.filter.FilterUprEvent(mut.ToUprEvent())
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

func (dh *DcpHandler) SnapshotMarker(snapshot gocbcore.DcpSnapshotMarker) {
	dh.dcpClient.dcpDriver.checkpointManager.updateSnapshot(snapshot.VbID, snapshot.StartSeqNo, snapshot.EndSeqNo)
}

func (dh *DcpHandler) Mutation(mutation gocbcore.DcpMutation) {
	dh.writeToDataChan(CreateMutation(mutation.VbID, mutation.Key, mutation.SeqNo, mutation.RevNo, mutation.Cas, mutation.Flags, mutation.Expiry, gomemcached.UPR_MUTATION, mutation.Value, mutation.Datatype, mutation.CollectionID, dh.xattraIterator, dh.dcpClient.dcpDriver.xattrKeysForNoCompare))
}

func (dh *DcpHandler) Deletion(deletion gocbcore.DcpDeletion) {
	dh.writeToDataChan(CreateMutation(deletion.VbID, deletion.Key, deletion.SeqNo, deletion.RevNo, deletion.Cas, 0, 0, gomemcached.UPR_DELETION, deletion.Value, deletion.Datatype, deletion.CollectionID, dh.xattraIterator, dh.dcpClient.dcpDriver.xattrKeysForNoCompare))
}

func (dh *DcpHandler) Expiration(expiration gocbcore.DcpExpiration) {
	dh.writeToDataChan(CreateMutation(expiration.VbID, expiration.Key, expiration.SeqNo, expiration.RevNo, expiration.Cas, 0, 0, gomemcached.UPR_EXPIRATION, nil, 0, expiration.CollectionID, dh.xattraIterator, dh.dcpClient.dcpDriver.xattrKeysForNoCompare))
}

func (dh *DcpHandler) End(streamEnd gocbcore.DcpStreamEnd, err error) {
	dh.dcpClient.dcpDriver.handleVbucketCompletion(streamEnd.VbID, err, "dcp stream ended")
}

// want CreateCollection("github.com/couchbase/gocbcore/v10".DcpCollectionCreation)
func (dh *DcpHandler) CreateCollection(creation gocbcore.DcpCollectionCreation) {
	dh.writeToDataChan(CreateMutation(creation.VbID, creation.Key, creation.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, creation.CollectionID, nil, nil))
}

func (dh *DcpHandler) DeleteCollection(deletion gocbcore.DcpCollectionDeletion) {
	dh.writeToDataChan(CreateMutation(deletion.VbID, nil, deletion.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, deletion.CollectionID, nil, nil))
}

func (dh *DcpHandler) FlushCollection(flush gocbcore.DcpCollectionFlush) {
	// Don't care - not implemented anyway
}

func (dh *DcpHandler) CreateScope(creation gocbcore.DcpScopeCreation) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(CreateMutation(creation.VbID, nil, creation.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, creation.ScopeID, nil, nil))
}

func (dh *DcpHandler) DeleteScope(deletion gocbcore.DcpScopeDeletion) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(CreateMutation(deletion.VbID, nil, deletion.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, deletion.ScopeID, nil, nil))
}

func (dh *DcpHandler) ModifyCollection(modify gocbcore.DcpCollectionModification) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(CreateMutation(modify.VbID, nil, modify.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, modify.CollectionID, nil, nil))
}

func (dh *DcpHandler) OSOSnapshot(oso gocbcore.DcpOSOSnapshot) {
	// Don't care
}

func (dh *DcpHandler) SeqNoAdvanced(seqnoAdv gocbcore.DcpSeqNoAdvanced) {
	// This is needed because the seqnos of mutations/events of collections to which the consumer is not subscribed during OpenStream() has to be recorded
	// Eventhough such mutations/events are not streamed by the producer
	// bySeqno stores the value of the current high seqno of the vbucket
	// collectionId parameter of CreateMutation() is insignificant
	dh.writeToDataChan(CreateMutation(seqnoAdv.VbID, nil, seqnoAdv.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SEQNO_ADV, nil, 0, base.Uint32MaxVal, nil, nil))
}

func (dh *DcpHandler) checkColMigrationFilters(mut *Mutation) []uint8 {
	// We don't diff deletion/expiration for migration. Otherwise it is confusing.
	if mut.OpCode == gomemcached.UPR_EXPIRATION || mut.OpCode == gomemcached.UPR_DELETION {
		return []uint8{}
	}
	var filterIdsMatched []uint8
	for i, filter := range dh.colMigrationFiltersImpl {
		// If at least one passed, let it through
		matched, _, _, _, _ := filter.FilterUprEvent(mut.ToUprEvent())
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
		dh.logger.Debugf("Document %s (%x) with length %v opCode %v matched more than once: %v, errMap %v, errMCReqMap %v",
			uprEvent.UprEvent.Key, uprEvent.UprEvent.Key, len(uprEvent.UprEvent.Key), uprEvent.UprEvent.Opcode, matchedNamespaces.String(), errMap, errMCReqMap)
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
	Vbno                  uint16
	Key                   []byte
	Seqno                 uint64
	RevId                 uint64
	Cas                   uint64
	Flags                 uint32
	Expiry                uint32
	OpCode                gomemcached.CommandCode
	Value                 []byte
	Datatype              uint8
	ColId                 uint32
	ColFiltersMatched     []uint8 // Given a ordered list of filters, this list contains indexes of the ordered list of filter that matched
	XattrIterator         *xdcrBase.XattrIterator
	xattrKeysForNoCompare map[string]bool
}

func CreateMutation(vbno uint16, key []byte, seqno, revId, cas uint64, flags, expiry uint32, opCode gomemcached.CommandCode, value []byte, datatype uint8, collectionId uint32, xattrIterator *xdcrBase.XattrIterator, xattrKeysForNoCompare map[string]bool) *Mutation {
	return &Mutation{
		Vbno:                  vbno,
		Key:                   key,
		Seqno:                 seqno,
		RevId:                 revId,
		Cas:                   cas,
		Flags:                 flags,
		Expiry:                expiry,
		OpCode:                opCode,
		Value:                 value,
		Datatype:              datatype,
		ColId:                 collectionId,
		XattrIterator:         xattrIterator,
		xattrKeysForNoCompare: xattrKeysForNoCompare,
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

func (m *Mutation) IsSystemOrUnsubbedEvent() bool {
	return m.OpCode == gomemcached.DCP_SYSTEM_EVENT || m.OpCode == gomemcached.DCP_SEQNO_ADV
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
func (mut *Mutation) Serialize() ([]byte, error) {
	var bodyHash [64]byte
	var xattrSize uint32
	var xattr []byte
	var bodyWithoutXattr, trimmedXattrPlusBody, hlv, importCas []byte
	var err error
	if mut.Datatype&xdcrBase.XattrDataType > 0 {
		var KVsToBeExcluded map[string][]byte
		bodyWithoutXattr, err = xdcrBase.StripXattrAndGetBody(mut.Value)
		if err != nil {
			return nil, err
		}
		xattrSize, _ = xdcrBase.GetXattrSize(mut.Value)
		xattr = mut.Value[4 : xattrSize+4]
		trimmedXattrPlusBody, KVsToBeExcluded, err = removeKVSubsetFromXattr(xattr, len(mut.Value), xattrSize, mut.XattrIterator, mut.xattrKeysForNoCompare, bodyWithoutXattr)
		hlv = KVsToBeExcluded[xdcrBase.XATTR_HLV]
		importCas = KVsToBeExcluded[xdcrBase.XATTR_IMPORTCAS]
		if err != nil {
			return nil, err
		}
		bodyHash = sha512.Sum512(trimmedXattrPlusBody)
	} else {
		bodyHash = sha512.Sum512(mut.Value)
	}

	hlvLen := uint32(len(hlv))
	importCasLen := uint32(len(importCas))
	hlvPlusImportSize := hlvLen + importCasLen
	keyLen := len(mut.Key)
	ret := make([]byte, base.GetFixedSizeMutationLen(keyLen, hlvPlusImportSize, mut.ColFiltersMatched))

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
	binary.BigEndian.PutUint32(ret[pos:pos+4], hlvLen)
	pos += 4
	copy(ret[pos:pos+int(hlvLen)], hlv)
	pos += int(hlvLen)
	binary.BigEndian.PutUint32(ret[pos:pos+4], importCasLen)
	pos += 4
	copy(ret[pos:pos+int(importCasLen)], importCas)
	pos += int(importCasLen)
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
	return ret, nil
}

// This is function is used to remove specified KVs from the xattr and create a new one excluding them
// @param xattr - denotes the original xattr
// @param size - denotes the max size of the new xattr+doc_body
// @param xattrSize - represents the xatttr Size to initialize an iterator over the original one
// @param xattrIterator - is a pointer to the xattrIterator object
// @param keysToExclude - is map containing the keys to excluded from the xattr
// @param bodyWithoutXatt - denotes the document body alone
// Returns - trimmedXattr, KV pairs of the excluded Xattrs, error
func removeKVSubsetFromXattr(xattr []byte, size int, xattrSize uint32, xattrIterator *xdcrBase.XattrIterator, keysToExclude map[string]bool, bodyWithoutXattr []byte) ([]byte, map[string][]byte, error) {
	var err error
	var trimmedXattrPlusBody []byte = make([]byte, size)
	err = xattrIterator.ResetXattrIterator(xattr, xattrSize)
	if err != nil {
		return nil, nil, err
	}
	var key, value []byte
	var KVsToBeIncluded map[string][]byte = make(map[string][]byte)
	var KVsToBeExcluded map[string][]byte = make(map[string][]byte)
	var keys []string
	for xattrIterator.HasNext() {
		key, value, err = xattrIterator.Next()
		if err != nil {
			return nil, nil, err
		}
		keyStr := string(key)
		_, exists := keysToExclude[keyStr]
		if exists {
			KVsToBeExcluded[keyStr] = value
		} else {
			KVsToBeIncluded[keyStr] = value
		}
	}
	for keyToBeIncluded, _ := range KVsToBeIncluded {
		keys = append(keys, keyToBeIncluded)
	}
	sort.Strings(keys)
	xattrComposer := xdcrBase.NewXattrComposer(trimmedXattrPlusBody)
	for _, key := range keys {
		err = xattrComposer.WriteKV([]byte(key), KVsToBeIncluded[key])
		if err != nil {
			return nil, nil, err
		}
	}
	trimmedXattrPlusBody, _ = xattrComposer.FinishAndAppendDocValue(bodyWithoutXattr)
	return trimmedXattrPlusBody, KVsToBeExcluded, nil
}
