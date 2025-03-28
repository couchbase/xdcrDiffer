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
	"sort"
	"strings"
	"sync"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	xdcrBase "github.com/couchbase/goxdcr/v8/base"
	xdcrParts "github.com/couchbase/goxdcr/v8/base/filter"
	"github.com/couchbase/goxdcr/v8/crMeta"
	xdcrLog "github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	xdcrUtils "github.com/couchbase/goxdcr/v8/utils"
	"github.com/couchbase/xdcrDiffer/base"
	fh "github.com/couchbase/xdcrDiffer/fileHandler"
)

// implements StreamObserver
type DcpHandler struct {
	dcpClient                     *DcpClient
	index                         int
	vbList                        []uint16
	numberOfBins                  int
	dataChan                      chan *Mutation
	waitGrp                       sync.WaitGroup
	finChan                       chan bool
	logger                        *xdcrLog.CommonLogger
	filter                        xdcrParts.Filter
	incrementCounter              func()
	incrementSysOrUnsubbedCounter func()
	colMigrationFilters           []string
	colMigrationFiltersOn         bool // shortcut to avoid len() check
	colMigrationFiltersImpl       []xdcrParts.Filter
	isSource                      bool
	utils                         xdcrUtils.UtilsIface
	migrationMapping              metadata.CollectionNamespaceMapping
	mobileCompatible              int
	expDelMode                    xdcrBase.FilterExpDelType
	xattrIterator                 *xdcrBase.XattrIterator
	fileHandler                   *fh.FileHandler
}

func NewDcpHandler(dcpClient *DcpClient, index int, vbList []uint16, numberOfBins, dataChanSize int, incReceivedCounter, incSysOrUnsubbedEvtReceived func(), colMigrationFilters []string, utils xdcrUtils.UtilsIface, migrationMapping metadata.CollectionNamespaceMapping, fileHandler *fh.FileHandler) (*DcpHandler, error) {
	if len(vbList) == 0 {
		return nil, fmt.Errorf("vbList is empty for handler %v", index)
	}
	return &DcpHandler{
		dcpClient:                     dcpClient,
		index:                         index,
		vbList:                        vbList,
		numberOfBins:                  numberOfBins,
		dataChan:                      make(chan *Mutation, dataChanSize),
		finChan:                       make(chan bool),
		logger:                        dcpClient.logger,
		filter:                        dcpClient.dcpDriver.filter,
		incrementCounter:              incReceivedCounter,
		incrementSysOrUnsubbedCounter: incSysOrUnsubbedEvtReceived,
		colMigrationFilters:           colMigrationFilters,
		colMigrationFiltersOn:         len(colMigrationFilters) > 0,
		utils:                         utils,
		isSource:                      strings.Contains(dcpClient.Name, base.SourceClusterName),
		migrationMapping:              migrationMapping,
		mobileCompatible:              dcpClient.dcpDriver.mobileCompatible,
		expDelMode:                    dcpClient.dcpDriver.expDelMode,
		xattrIterator:                 &xdcrBase.XattrIterator{},
		fileHandler:                   fileHandler,
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
	dh.waitGrp.Wait()
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
	if err := dh.compileMigrCollectionFiltersIfNeeded(); err != nil {
		return err
	}
	return nil
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

	if dh.colMigrationFiltersOn && len(filterIdsMatched) > 0 {
		mut.ColFiltersMatched = filterIdsMatched
	}

	bucket, err := dh.fileHandler.GetBucket(mut.Key, mut.Vbno)
	if err != nil {
		dh.logger.Errorf("failed to write mutation for document %s with cas %v revID %v. err=%v", mut.Key, mut.Cas, mut.RevId, err)
		return
	}

	ret, err := mut.Serialize()
	if err != nil {
		dh.logger.Errorf("error in Serializing the mutation pertaining to the document with the key:%v ,err:%v\n", mut.Key, err)
	} else {
		bucket.Write(ret)
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
	dh.writeToDataChan(CreateMutation(mutation.VbID, mutation.Key, mutation.SeqNo, mutation.RevNo, mutation.Cas, mutation.Flags, mutation.Expiry, gomemcached.UPR_MUTATION, mutation.Value, mutation.Datatype, mutation.CollectionID, dh.xattrIterator, dh.dcpClient.dcpDriver.xattrKeysForNoCompare))
}

func (dh *DcpHandler) Deletion(deletion gocbcore.DcpDeletion) {
	dh.writeToDataChan(CreateMutation(deletion.VbID, deletion.Key, deletion.SeqNo, deletion.RevNo, deletion.Cas, 0, 0, gomemcached.UPR_DELETION, deletion.Value, deletion.Datatype, deletion.CollectionID, dh.xattrIterator, dh.dcpClient.dcpDriver.xattrKeysForNoCompare))
}

func (dh *DcpHandler) Expiration(expiration gocbcore.DcpExpiration) {
	dh.writeToDataChan(CreateMutation(expiration.VbID, expiration.Key, expiration.SeqNo, expiration.RevNo, expiration.Cas, 0, 0, gomemcached.UPR_EXPIRATION, nil, 0, expiration.CollectionID, dh.xattrIterator, dh.dcpClient.dcpDriver.xattrKeysForNoCompare))
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
	XattrKeysForNoCompare map[string]bool
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
		XattrKeysForNoCompare: xattrKeysForNoCompare,
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

// Darshan:TODO accomodate SGW xattr change from "import" to "_mou" when MB-60897 is checked-in
func (mut *Mutation) Serialize() ([]byte, error) {
	var bodyHash [64]byte
	var xattrSize uint32
	var xattr []byte
	var bodyWithoutXattr, trimmedXattrPlusBody, hlv []byte
	var importCas, pRev uint64
	var err error
	if mut.Datatype&xdcrBase.XattrDataType > 0 {
		var KVsToBeExcluded map[string][]byte
		bodyWithoutXattr, err = xdcrBase.StripXattrAndGetBody(mut.Value)
		if err != nil {
			return nil, err
		}
		xattrSize, _ = xdcrBase.GetXattrSize(mut.Value)
		xattr = mut.Value[4 : xattrSize+4]
		trimmedXattrPlusBody, KVsToBeExcluded, err = removeKVSubsetFromXattr(xattr, len(mut.Value), xattrSize, mut.XattrIterator, mut.XattrKeysForNoCompare, bodyWithoutXattr)
		if err != nil {
			return nil, err
		}
		hlv = KVsToBeExcluded[xdcrBase.XATTR_HLV]
		mou, ok := KVsToBeExcluded[xdcrBase.XATTR_MOU]
		if ok {
			_, _, importCas, pRev, err = crMeta.GetImportCasAndPrevFromMou(mou)
			if err != nil {
				return nil, err
			}
		}
		bodyHash = sha512.Sum512(trimmedXattrPlusBody)
	} else {
		bodyHash = sha512.Sum512(mut.Value)
	}

	hlvLen := uint64(len(hlv))
	keyLen := len(mut.Key)
	ret := make([]byte, base.GetFixedSizeMutationLen(keyLen, hlvLen, mut.ColFiltersMatched))

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
	binary.BigEndian.PutUint64(ret[pos:pos+8], importCas)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], pRev)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], hlvLen)
	pos += 8
	copy(ret[pos:pos+int(hlvLen)], hlv)
	pos += int(hlvLen)
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
// @param size - denotes the max size of the new xattr+docBody
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
	for keyToBeIncluded := range KVsToBeIncluded {
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
	trimmedXattrPlusBody, _ = xattrComposer.FinishAndAppendDocValue(bodyWithoutXattr, nil, nil)
	return trimmedXattrPlusBody, KVsToBeExcluded, nil
}
