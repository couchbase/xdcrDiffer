// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package differ

import (
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/couchbase/gomemcached"
	xdcrBase "github.com/couchbase/goxdcr/v8/base"
	crMeta "github.com/couchbase/goxdcr/v8/crMeta"
	hlv "github.com/couchbase/goxdcr/v8/hlv"
	xdcrLog "github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/xdcrDiffer/encryption"
	fdp "github.com/couchbase/xdcrDiffer/fileDescriptorPool"
	"github.com/couchbase/xdcrDiffer/utils"
)

// Given two DCP Dump files, perform necessary diffing task
type FilesDiffer struct {
	file1 FileAttributes
	file2 FileAttributes

	// errors corresponding to the attributes ops
	err1 error
	err2 error

	// for parallelism
	dataLoadWg sync.WaitGroup

	// Diff results
	MissingFromFile1     []*oneEntry
	MissingFromFile2     []*oneEntry
	BothExistButMismatch []*entryPair

	fdPool *fdp.FdPool

	collectionIdMapping map[uint32][]uint32
	colFilterStrings    []string
	colFilterTgtIds     []uint32 // target collection IDs

	file1ItemCount int
	file2ItemCount int

	// For 1->N,  it is possible for doc is mapped to multiple filter IDs
	duplicatedHintMap DuplicatedHintMap
	logger            *xdcrLog.CommonLogger
}

type DuplicatedHintMap map[string][]uint8

func (d DuplicatedHintMap) Merge(other DuplicatedHintMap) {
	for k, v := range other {
		if _, exists := d[k]; !exists {
			d[k] = v
		} else {
			replacement := utils.SortUint8List(d[k])
			for _, j := range v {
				_, found := utils.SearchUint8List(replacement, j)
				if !found {
					replacement = append(replacement, j)
					replacement = utils.SortUint8List(replacement)
				}
			}
			d[k] = replacement
		}
	}
}

func (d DuplicatedHintMap) ToIntMap() map[string][]int {
	outputMap := make(map[string][]int)
	for k, v := range d {
		var intSlice []int
		for _, j := range v {
			intSlice = append(intSlice, int(j))
		}
		outputMap[k] = intSlice
	}
	return outputMap
}

type FileAttributes struct {
	name          string
	actorId       hlv.DocumentSourceId
	entries       map[uint32]map[string]*oneEntry
	sortedEntries map[uint32][]*oneEntry
	readOp        fdp.FileOp
	closeOp       func() error
}

func NewFileAttribute(fileName string, encReader encryption.FileOps) (*FileAttributes, error) {
	fileReaderOps, err := encReader.OpenFile(fileName)
	if err != nil {
		return nil, err
	}

	attr := &FileAttributes{
		name:          fileName,
		entries:       make(map[uint32]map[string]*oneEntry),
		sortedEntries: make(map[uint32][]*oneEntry),
		readOp:        fileReaderOps.ReadAndFillBytes,
	}
	return attr, nil
}

type oneEntry struct {
	Key               string
	CrMeta            *crMeta.CRMetadata
	ActorID           hlv.DocumentSourceId
	Seqno             uint64
	Xattr             []byte
	XattrSize         uint32
	BodyHash          [sha512.Size]byte
	ColId             uint32
	ColMigrFilterLen  uint8
	ColFiltersMatched []uint8
}

func (oneEntry *oneEntry) String() string {
	if oneEntry.CrMeta == nil {
		panic(fmt.Sprintf("Coding Error: crMeta can never be nil. entry for document %s has crMeta to be nil", oneEntry.Key))
	}
	docMeta := oneEntry.CrMeta.GetDocumentMetadata()
	if docMeta == nil {
		panic(fmt.Sprintf("Coding Error: docMeta can never be nil. entry for document %s has docMeta to be nil", oneEntry.Key))
	}
	return fmt.Sprintf("<Key>: %v <Seqno>: %v <RevId>: %v <Cas>: %v <Flags>: %v <Expiry>: %v <OpCode>: %v <DataType>: %v <Hash>: %s <colId>: %v",
		oneEntry.Key, oneEntry.Seqno, docMeta.RevSeq, docMeta.Cas, docMeta.Flags, docMeta.Expiry, docMeta.Opcode, docMeta.DataType, hex.EncodeToString(oneEntry.BodyHash[:]), oneEntry.ColId)
}

type entryPair [2]*oneEntry

type ByKeyName []*oneEntry

func shaCompare(a, b [sha512.Size]byte) bool {
	for i := 0; i < sha512.Size; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Note Expiry is not used for conflict resolution
// Returns a boolean to showcase if the values all match
// For int return val:
//
//	0 - Names are the same
//	1 - If entry name > other name
//
// -1 - If entry name < other name
func (entry oneEntry) Diff(other oneEntry) (int, bool) {
	var err error
	var match bool
	if entry.Key != other.Key {
		if entry.Key > other.Key {
			return 1, false
		} else {
			return -1, false
		}
	}

	err = SetHlv(entry.CrMeta, other.CrMeta, entry.ActorID, other.ActorID)
	if err != nil {
		// An err is populated only if implict construction of HLVs are not possible --> this implies that there is a diff
		return 0, false
	}
	match, err = entry.CrMeta.Diff(other.CrMeta, xdcrBase.GetHLVPruneFunction(entry.CrMeta.GetDocumentMetadata().Cas, sourcePruningWindow.get()), xdcrBase.GetHLVPruneFunction(other.CrMeta.GetDocumentMetadata().Cas, targetPruningWindow.get()))
	if err != nil { // error is returned by the Diff method only if either of the HLVs are nil
		if entry.CrMeta.GetHLV() == nil && other.CrMeta.GetHLV() == nil { // if both the HLVs are nil return true
			return 0, true
		} else { // if only one them if nil => its a programming error(should not happen)
			panic(fmt.Sprintf("Programming error - found one of HLVs to be nil. SourceHlv: %v, TargetHlv: %v", entry.CrMeta.GetHLV(), other.CrMeta.GetHLV()))
		}
	}
	return 0, match
}

func SetHlv(crMeta1, crMeta2 *crMeta.CRMetadata, bucketUUID1, bucketUUID2 hlv.DocumentSourceId) error {
	hlv1 := crMeta1.GetHLV()
	hlv2 := crMeta2.GetHLV()
	cas1 := crMeta1.GetDocumentMetadata().Cas
	cas2 := crMeta2.GetDocumentMetadata().Cas
	// if both the HLVS are nil or if both are present then take no action
	if hlv1 != nil && hlv2 == nil { //this is the case where the document is replicated from target to source
		if hlv1.GetCvSrc() != bucketUUID2 || hlv1.GetCvVer() != cas2 { // this implies that the source did not recieve the document from the target(cannot implicitly construct HLV for target)
			return fmt.Errorf("Implicit construction of target HLV not possible")
		} else {
			newHlv, _ := hlv.NewHLV(bucketUUID2, cas2, 0, "", 0, nil, nil)
			crMeta2.SetHLV(newHlv)
		}
	}
	if hlv1 == nil && hlv2 != nil { //this is the case where the document is replicated from source to target
		if hlv2.GetCvSrc() != bucketUUID1 || hlv2.GetCvVer() != cas1 {
			return fmt.Errorf("Implicit construction of source HLV not possible")
		} else {
			newHlv, _ := hlv.NewHLV(bucketUUID1, cas1, 0, "", 0, nil, nil)
			crMeta1.SetHLV(newHlv)
		}
	}
	return nil
}

func (entry *oneEntry) IsMutation() bool {
	return entry.CrMeta.GetDocumentMetadata().Opcode == gomemcached.UPR_MUTATION
}

func (srcEntry *oneEntry) MapsToTargetCol(tgtColId uint32, colFilterTgtIds []uint32, currentTgtFileColId uint32) bool {
	for _, oneMatchedFilterIdx := range srcEntry.ColFiltersMatched {
		// Each matched entry represents a target collection ID that is supposed to be replicated
		if int(oneMatchedFilterIdx) >= len(colFilterTgtIds) {
			panic("FilterIdx matched is greater than available compiled filters")
		}
		filterTargetColId := colFilterTgtIds[oneMatchedFilterIdx]
		if filterTargetColId == tgtColId && tgtColId == currentTgtFileColId {
			// This source entry is meant to be replicated to this target entry's collection ID
			// and the current file differ is looking at contains the same collection ID
			return true
		}
	}

	// This source entry was never meant to replicated to this target entry's collection
	return false
}

func NewFilesDiffer(file1, file2 string, collectionMapping map[uint32][]uint32, colFilterStrings []string, colFilterTgtIds []uint32, logger *xdcrLog.CommonLogger, encReader encryption.FileOps) (*FilesDiffer, error) {
	file1Attr, err := NewFileAttribute(file1, encReader)
	if err != nil {
		return nil, err
	}
	file2Attr, err := NewFileAttribute(file2, encReader)
	if err != nil {
		return nil, err
	}

	differ := &FilesDiffer{
		file1:               *file1Attr,
		file2:               *file2Attr,
		collectionIdMapping: collectionMapping,
		colFilterStrings:    colFilterStrings,
		colFilterTgtIds:     colFilterTgtIds,
		duplicatedHintMap:   map[string][]uint8{},
		logger:              logger,
	}
	if len(collectionMapping) == 0 {
		// This means this is legacy mode - no collection support
		differ.collectionIdMapping = make(map[uint32][]uint32)
		differ.collectionIdMapping[0] = []uint32{0}
	}
	return differ, nil
}

func NewFilesDifferWithFDPool(file1, file2 string, fdPool *fdp.FdPool, collectionMapping map[uint32][]uint32, colFilterStrings []string, colFilterTgtIds []uint32, logger *xdcrLog.CommonLogger, encryptionSvc encryption.FileOps) (*FilesDiffer, error) {
	var err error
	differ, err := NewFilesDiffer(file1, file2, collectionMapping, colFilterStrings, colFilterTgtIds, logger, encryptionSvc)
	if err != nil {
		return nil, err
	}
	if fdPool != nil {
		differ.fdPool = fdPool
		differ.file1.readOp, err = fdPool.RegisterReadOnlyFileHandle(file1)
		if err != nil {
			return nil, err
		}
		differ.file1.closeOp = func() error {
			return fdPool.DeRegisterFileHandle(file1)
		}
		differ.file2.readOp, err = fdPool.RegisterReadOnlyFileHandle(file2)
		if err != nil {
			return nil, err
		}
		differ.file2.closeOp = func() error {
			return fdPool.DeRegisterFileHandle(file2)
		}
	}
	return differ, nil
}

func UpdateCrMeta(crMetadata *crMeta.CRMetadata, actorID hlv.DocumentSourceId, hlvbytes []byte, pRev uint64) error {
	cvCas, cvSrc, cvVer, pvMap, mvMap, err := crMeta.ParseHlvFields(crMetadata.GetDocumentMetadata().Cas, hlvbytes)
	if err != nil {
		return err
	}
	err = crMetadata.UpdateHLVIfNeeded(actorID, crMetadata.GetDocumentMetadata().Cas, cvCas, cvSrc, cvVer, pvMap, mvMap, crMetadata.GetImportCas(), pRev)
	return err
}

func getOneEntry(readOp fdp.FileOp, actorId hlv.DocumentSourceId) (*oneEntry, error) {
	entry := &oneEntry{}
	docMeta := &xdcrBase.DocumentMetadata{}
	entry.CrMeta = &crMeta.CRMetadata{}
	entry.ActorID = actorId
	keyLenBytes := make([]byte, 2)
	bytesRead, err := readOp(keyLenBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read keyLen, bytes read: %v, err: %v", bytesRead, err)
	}
	entryKeyLen := binary.BigEndian.Uint16(keyLenBytes)

	keyBytes := make([]byte, entryKeyLen)
	bytesRead, err = readOp(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read key, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Key = string(keyBytes)

	seqnoBytes := make([]byte, 8)
	bytesRead, err = readOp(seqnoBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read seqno, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Seqno = binary.BigEndian.Uint64(seqnoBytes)

	revIdBytes := make([]byte, 8)
	bytesRead, err = readOp(revIdBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read revIdBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	docMeta.RevSeq = binary.BigEndian.Uint64(revIdBytes)

	casBytes := make([]byte, 8)
	bytesRead, err = readOp(casBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read casBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	docMeta.Cas = binary.BigEndian.Uint64(casBytes)

	flagBytes := make([]byte, 4)
	bytesRead, err = readOp(flagBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read flagsBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	docMeta.Flags = binary.BigEndian.Uint32(flagBytes)

	expiryBytes := make([]byte, 4)
	bytesRead, err = readOp(expiryBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read expiryBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	docMeta.Expiry = binary.BigEndian.Uint32(expiryBytes)

	opCodeBytes := make([]byte, 2)
	bytesRead, err = readOp(opCodeBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read opCodeBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	docMeta.Opcode = gomemcached.CommandCode(binary.BigEndian.Uint16(opCodeBytes))

	dataTypeBytes := make([]byte, 2)
	bytesRead, err = readOp(dataTypeBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read dataTypeBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	docMeta.DataType = uint8(binary.BigEndian.Uint16(dataTypeBytes))

	entry.CrMeta.SetDocumentMetadata(docMeta)

	importCasBytes := make([]byte, 8)
	bytesRead, err = readOp(importCasBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read importCasBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.CrMeta.SetImportCas(binary.BigEndian.Uint64(importCasBytes))

	pRevIdBytes := make([]byte, 8)
	bytesRead, err = readOp(pRevIdBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read pRevIdBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	pRev := binary.BigEndian.Uint64(pRevIdBytes)

	hlvSizebytes := make([]byte, 8)
	bytesRead, err = readOp(hlvSizebytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read HlvSizeBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	hlvSize := binary.BigEndian.Uint64(hlvSizebytes)

	HlvBytes := make([]byte, hlvSize)
	bytesRead, err = readOp(HlvBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read hlv, bytes read: %v, err: %v", bytesRead, err)
	}
	if len(HlvBytes) != 0 {
		// UpdateCrMeta sets the appropriate doc version incase the mutation is an import Mutation
		err = UpdateCrMeta(entry.CrMeta, entry.ActorID, HlvBytes, pRev) // creates the HLV and sets it to crMeta ; updates the version if importCas is present
		if err != nil {
			return nil, fmt.Errorf("Error in constructing HLV, err: %v", err)
		}
	} else {
		// if HLV is not present then it implies that importCas is not present; True docCas and RevID represent the version of the doc
		entry.CrMeta.SetHLV(nil)
	}
	hashBytes := make([]byte, sha512.Size)
	bytesRead, err = readOp(hashBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read hashBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	copy(entry.BodyHash[:], hashBytes)

	collectionIdBytes := make([]byte, 4)
	bytesRead, err = readOp(collectionIdBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read collectionIdBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.ColId = binary.BigEndian.Uint32(collectionIdBytes)

	colFiltersLenByte := make([]byte, 2)
	bytesRead, err = readOp(colFiltersLenByte)
	if err != nil {
		return nil, fmt.Errorf("Unable to read filterLenBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.ColMigrFilterLen = uint8(binary.BigEndian.Uint16(colFiltersLenByte))

	var colFilterIds []uint8
	for i := uint8(0); i < entry.ColMigrFilterLen; i++ {
		idByte := make([]byte, 2)
		bytesRead, err = readOp(idByte)
		if err != nil {
			return nil, fmt.Errorf("Unable to read a single colFilterID for index %v, err: %v", i, err)
		}
		colFilterIds = append(colFilterIds, uint8(binary.BigEndian.Uint16(idByte)))
	}
	entry.ColFiltersMatched = colFilterIds
	return entry, nil
}

func (a ByKeyName) Len() int           { return len(a) }
func (a ByKeyName) Swap(i, j int)      { *a[i], *a[j] = *a[j], *a[i] }
func (a ByKeyName) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (attr *FileAttributes) fillAndDedupEntries() error {
	var err error
	var entry *oneEntry
	for {
		entry, err = getOneEntry(attr.readOp, attr.actorId)
		if err != nil {
			break
		}
		_, exists := attr.entries[entry.ColId]
		if !exists {
			attr.entries[entry.ColId] = make(map[string]*oneEntry)
		}
		if curEntry, ok := attr.entries[entry.ColId][entry.Key]; !ok {
			attr.entries[entry.ColId][entry.Key] = entry
		} else {
			// Replace the entry in the map if the seqno is newer
			if entry.Seqno > curEntry.Seqno {
				attr.entries[entry.ColId][entry.Key] = entry
			}
		}
	}

	if strings.Contains(err.Error(), io.EOF.Error()) {
		err = nil
	}
	return err
}

func (attr *FileAttributes) sortEntries() {
	for colId, entriesOfThisCollection := range attr.entries {
		for _, v := range entriesOfThisCollection {
			attr.sortedEntries[colId] = append(attr.sortedEntries[colId], v)
		}
		sort.Sort(ByKeyName(attr.sortedEntries[colId]))
	}
}

func (attr *FileAttributes) LoadFileIntoBuffer() error {
	if len(attr.name) == 0 {
		return fmt.Errorf("No file specified")
	}
	if attr.readOp != nil && attr.closeOp != nil {
		defer attr.closeOp()
	}
	err := attr.fillAndDedupEntries()
	if err != nil {
		return err
	}
	attr.sortEntries()
	return nil
}

func (differ *FilesDiffer) asyncLoad(attr *FileAttributes, err *error) {
	defer differ.dataLoadWg.Done()
	*err = attr.LoadFileIntoBuffer()
}

// This will take each collection ID to ID mapping and diff the keys within them to find
// any discrepancies
// Returns maps that requires further Get() to analyze:
// 1. map of [sourceColId] -> [key]
// 2. map of [targetColId] -> [key]
// 3. map of [sourceDocId] -> Maps to which target collection IDs (migration mode only)
func (differ *FilesDiffer) diffSorted() (map[uint32][]string, map[uint32][]string, map[string][]uint32) {
	srcDiffMap := make(map[uint32][]string)
	tgtDiffMap := make(map[uint32][]string)

	// For collection migration mode, diffing by colId -> file isn't enough
	// We need to check to make sure that only something that the source is meant to replicate to the target
	// should be there
	migrationHintMap := make(map[string][]uint32)
	colMigrationMode := len(differ.colFilterStrings) > 0

	for srcColId, tgtColIds := range differ.collectionIdMapping {
		srcDedupMap := make(map[string]bool)
		for _, tgtColId := range tgtColIds {
			diffKeys := make([]string, 0)
			file1Len := len(differ.file1.sortedEntries[srcColId])
			file2Len := len(differ.file2.sortedEntries[tgtColId])

			if file1Len == 0 && file2Len == 0 && !colMigrationMode {
				//return srcDiffKeys
				continue
			}

			var i int
			var j int

			for i < file1Len && j < file2Len {
				item1 := differ.file1.sortedEntries[srcColId][i]
				item2 := differ.file2.sortedEntries[tgtColId][j]
				differ.addMigrationHintIfNeeded(colMigrationMode, item1, migrationHintMap)

				keyCompare, match := item1.Diff(*item2)
				validComparison := !colMigrationMode || item1.MapsToTargetCol(item2.ColId, differ.colFilterTgtIds, tgtColId) && item1.IsMutation() && item2.IsMutation()
				if match {
					// Both items are the same
					i++
					j++
				} else {
					if keyCompare == 0 {
						// Both document are the same, but others mismatched
						if validComparison {
							var onePair entryPair
							onePair[0] = item1
							onePair[1] = item2
							differ.BothExistButMismatch = append(differ.BothExistButMismatch, &onePair)
							diffKeys = append(diffKeys, item1.Key)
							addToSrcDiffMapIfNotAdded(srcDedupMap, item1.Key, srcDiffMap, srcColId)
							tgtDiffMap[tgtColId] = append(tgtDiffMap[tgtColId], item1.Key)
						}
						i++
						j++
					} else if keyCompare < 0 {
						// Like "a" < "b", where a is 1 and b is 2
						if validComparison {
							differ.MissingFromFile2 = append(differ.MissingFromFile2, item1)
							diffKeys = append(diffKeys, item1.Key)
							addToSrcDiffMapIfNotAdded(srcDedupMap, item1.Key, srcDiffMap, srcColId)
							tgtDiffMap[tgtColId] = append(tgtDiffMap[tgtColId], item1.Key)
						}
						i++
					} else {
						// "b" > "a", leading to keyCompare > 0
						if validComparison {
							differ.MissingFromFile1 = append(differ.MissingFromFile1, item2)
							diffKeys = append(diffKeys, item2.Key)
							addToSrcDiffMapIfNotAdded(srcDedupMap, item2.Key, srcDiffMap, srcColId)
							tgtDiffMap[tgtColId] = append(tgtDiffMap[tgtColId], item2.Key)
						}
						j++
					}
				}
			}

			for ; i < file1Len; i++ {
				// This means that all the rest of the entries in file1 are missing from file2
				item1 := differ.file1.sortedEntries[srcColId][i]
				differ.addMigrationHintIfNeeded(colMigrationMode, item1, migrationHintMap)
				validComparison := !colMigrationMode || item1.MapsToTargetCol(tgtColId, differ.colFilterTgtIds, tgtColId) && item1.IsMutation()
				if validComparison {
					differ.MissingFromFile2 = append(differ.MissingFromFile2, item1)
					addToSrcDiffMapIfNotAdded(srcDedupMap, item1.Key, srcDiffMap, srcColId)
				}
			}

			// iterative migration means that it is possible target has more docs than the source as customers
			// do migration with a set of rules, and then do another set of migration with another set of rules, etc
			// Do not check the rest if it is migration mode
			if !colMigrationMode {
				for ; j < file2Len; j++ {
					// This means that all the rest of the entries in file2 are missing from file1
					differ.MissingFromFile1 = append(differ.MissingFromFile1, differ.file2.sortedEntries[tgtColId][j])
					tgtDiffMap[tgtColId] = append(tgtDiffMap[tgtColId], differ.file2.sortedEntries[tgtColId][j].Key)
				}
			}
		}
	}
	return srcDiffMap, tgtDiffMap, migrationHintMap
}

func addToSrcDiffMapIfNotAdded(srcDedupMap map[string]bool, key string, srcDiffMap map[uint32][]string, srcColId uint32) {
	if _, exists := srcDedupMap[key]; !exists {
		srcDiffMap[srcColId] = append(srcDiffMap[srcColId], key)
	}
}

// Diff Returns:
//  1. srcDiffMap - a map of <colId> -> []<key> : For a collection ID <colId>, the keys that shows some inconsistency
//  2. tgtDiffMap - a map of <colId> -> []<key> : For a target side colId, the keys that show inconsistency with source counterpart
//  3. migrationHintMap - map of <string] -> []<colId> :
//     Under collections migration mode, this map will allow a quick index of which source document
//     should belong in which target collection ID. This is needed because fileDiffer ingested this
//     information from actual DCP binary dump and needs to pass this to mutationDiffer for display
func (differ *FilesDiffer) Diff() (srcDiffMap, tgtDiffMap map[uint32][]string, migrationHintMap map[string][]uint32, diffBytes []byte, err error) {
	differ.dataLoadWg.Add(1)
	go differ.asyncLoad(&differ.file1, &differ.err1)
	differ.dataLoadWg.Add(1)
	go differ.asyncLoad(&differ.file2, &differ.err2)
	differ.dataLoadWg.Wait()

	if differ.err1 != nil {
		differ.logger.Errorf("Error when loading file %v contents: %v\n", differ.file1.name, differ.err1)
	}
	if differ.err2 != nil {
		differ.logger.Errorf("Error when loading file %v contents: %v\n", differ.file2.name, differ.err2)
	}

	srcDiffMap, tgtDiffMap, migrationHintMap = differ.diffSorted()
	diffBytes, err = differ.diffToJson()

	// Count source items
	for _, entryMap := range differ.file1.entries {
		differ.file1ItemCount += len(entryMap)
	}
	// Count target Items
	for _, entryMap := range differ.file2.entries {
		differ.file2ItemCount += len(entryMap)
	}
	return srcDiffMap, tgtDiffMap, migrationHintMap, diffBytes, err
}

func (differ *FilesDiffer) PrettyPrintResult() {
	mismatchCnt := len(differ.BothExistButMismatch)
	missing1Cnt := len(differ.MissingFromFile1)
	missing2Cnt := len(differ.MissingFromFile2)

	if len(differ.file1.entries) == 0 && len(differ.file2.entries) == 0 {
		fmt.Printf("Diff tool has not been run yet\n")
	} else if mismatchCnt == 0 && missing1Cnt == 0 && missing2Cnt == 0 {
		fmt.Printf("Both sides match\n")
	} else {
		if mismatchCnt > 0 {
			fmt.Printf("%v Docs exist in both %v and %v but mismatch:\n", mismatchCnt, differ.file1.name, differ.file2.name)
			fmt.Printf("=========================================\n")
			for i := 0; i < mismatchCnt; i++ {
				fmt.Printf("--------------------------------------\n")
				fmt.Printf("File1: %v\n", differ.BothExistButMismatch[i][0].String())
				fmt.Printf("File2: %v\n", differ.BothExistButMismatch[i][1].String())
			}
			fmt.Printf("=========================================\n")
		}
		if missing2Cnt > 0 {
			fmt.Printf("%v Docs exist in %v that are missing from %v:\n", missing2Cnt, differ.file1.name, differ.file2.name)
			fmt.Printf("-------------------------------------------------\n")
			for i := 0; i < missing2Cnt; i++ {
				fmt.Printf("%v\n", differ.MissingFromFile2[i].String())
			}
			fmt.Printf("-------------------------------------------------\n")
		}
		if missing1Cnt > 0 {
			fmt.Printf("%v Docs exist in %v that are missing from %v:\n", missing1Cnt, differ.file2.name, differ.file1.name)
			fmt.Printf("-------------------------------------------------\n")
			for i := 0; i < missing1Cnt; i++ {
				fmt.Printf("%v\n", differ.MissingFromFile1[i].String())
			}
			fmt.Printf("-------------------------------------------------\n")
		}
	}
}

func (differ *FilesDiffer) diffToJson() ([]byte, error) {
	outputMap := map[string]interface{}{
		"Mismatch":          differ.BothExistButMismatch,
		"MissingFromSource": differ.MissingFromFile1,
		"MissingFromTarget": differ.MissingFromFile2,
	}

	ret, err := json.Marshal(outputMap)

	return ret, err
}

func (differ *FilesDiffer) addMigrationHintIfNeeded(migrationMode bool, item1 *oneEntry, hintMap map[string][]uint32) {
	if !migrationMode {
		return
	}
	if _, exists := hintMap[item1.Key]; !exists {
		// The whole source needs to be added to hintMap
		var tgtColIds []uint32
		for _, filterIdxMatched := range item1.ColFiltersMatched {
			// Shouldn't panic
			tgtColIds = append(tgtColIds, differ.colFilterTgtIds[filterIdxMatched])
		}
		hintMap[item1.Key] = tgtColIds
		if item1.ColMigrFilterLen > 1 {
			differ.duplicatedHintMap[item1.Key] = item1.ColFiltersMatched
		}
	}
}
