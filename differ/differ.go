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
	"os"
	"sort"
	"strings"
	"sync"
	fdp "xdcrDiffer/fileDescriptorPool"
	"xdcrDiffer/utils"

	"github.com/couchbase/gomemcached"
	xdcrBase "github.com/couchbase/goxdcr/base"
	crMeta "github.com/couchbase/goxdcr/crMeta"
	hlv "github.com/couchbase/goxdcr/hlv"
)

const BodyNilError string = "body cannot be nil"

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
	bucketUUID    string
	entries       map[uint32]map[string]*oneEntry
	sortedEntries map[uint32][]*oneEntry
	readOp        fdp.FileOp
	closeOp       func() error
}

func NewFileAttribute(fileName string) *FileAttributes {
	attr := &FileAttributes{
		name:          fileName,
		entries:       make(map[uint32]map[string]*oneEntry),
		sortedEntries: make(map[uint32][]*oneEntry),
	}
	return attr
}

type oneEntry struct {
	Key               string
	bucketUUID        hlv.DocumentSourceId
	Seqno             uint64
	RevId             uint64
	Cas               uint64
	ImportCas         uint64
	Flags             uint32
	Expiry            uint32
	OpCode            gomemcached.CommandCode
	Datatype          uint8
	Xattr             []byte
	XattrSize         uint32
	Hlv               *hlv.HLV
	BodyHash          [sha512.Size]byte
	ColId             uint32
	ColMigrFilterLen  uint8
	ColFiltersMatched []uint8
}

func (oneEntry *oneEntry) String() string {
	return fmt.Sprintf("<Key>: %v <Seqno>: %v <RevId>: %v <Cas>: %v <Flags>: %v <Expiry>: %v <OpCode>: %v <DataType>: %v <Hash>: %s <colId>: %v",
		oneEntry.Key, oneEntry.Seqno, oneEntry.RevId, oneEntry.Cas, oneEntry.Flags, oneEntry.Expiry, oneEntry.OpCode, oneEntry.Datatype, hex.EncodeToString(oneEntry.BodyHash[:]), oneEntry.ColId)
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
	if entry.Key != other.Key {
		if entry.Key > other.Key {
			return 1, false
		} else {
			return -1, false
		}
	} else if entry.OpCode != other.OpCode {
		return 0, false
	} else if entry.OpCode == gomemcached.UPR_MUTATION {
		entryRevId := entry.GetRevId()
		otherRevId := other.GetRevId()
		if entryRevId != otherRevId && entryRevId != 0 && otherRevId != 0 { //Darshan : RevId comparison should be revisited
			return 0, false
		} else if entry.GetVersion() != other.GetVersion() {
			return 0, false
		} else if entry.Flags != other.Flags {
			return 0, false
		} else if !shaCompare(entry.BodyHash, other.BodyHash) {
			return 0, false
		} else if entry.Datatype != other.Datatype {
			return 0, false
		} else if !compareHlv(entry.Hlv, other.Hlv, entry.Cas, other.Cas, entry.ImportCas, other.ImportCas, entry.bucketUUID, other.bucketUUID) {
			return 0, false
		}
	}
	return 0, true
}

func compareHlv(hlv1, hlv2 *hlv.HLV, cas1, cas2, importCas1, importCas2 uint64, bucketUUID1, bucketUUID2 hlv.DocumentSourceId) bool {
	//if both(source and target) have no HLV we return true
	if hlv1 == nil && hlv2 == nil {
		return true
	} else if hlv1 != nil && hlv2 == nil { //this is the case where the document is replicated from target to source
		if hlv1.GetCvSrc() != bucketUUID2 || hlv1.GetCvVer() != cas2 {
			return false
		} else {
			hlv2, _ = hlv.NewHLV(bucketUUID2, cas2, 0, "", 0, nil, nil)
		}
	} else if hlv1 == nil && hlv2 != nil { //this is the case where the document is replicated from source to target
		if hlv2.GetCvSrc() != bucketUUID1 || hlv2.GetCvVer() != cas1 {
			return false
		} else {
			hlv1, _ = hlv.NewHLV(bucketUUID1, cas1, 0, "", 0, nil, nil)
		}
	}
	return actualCompare(hlv1, hlv2, cas1, cas2)
}

func actualCompare(item1 *hlv.HLV, item2 *hlv.HLV, item1Cas uint64, item2Cas uint64) bool {
	// the HLVs that this function recieves is up to date
	if item1.GetCvCas() != item2.GetCvCas() {
		return false
	} else if item1.GetCvSrc() != item2.GetCvSrc() || item1.GetCvVer() != item2.GetCvVer() {
		return false
	} else if !comparePv(item1.GetPV(), item2.GetPV(), item1Cas, item2Cas) {
		return false
	}
	return true
}

func comparePv(Pv1 hlv.VersionsMap, Pv2 hlv.VersionsMap, cas1 uint64, cas2 uint64) bool {
	if len(Pv1) == len(Pv2) {
		for key, value1 := range Pv1 {
			value2, ok := Pv2[key]
			if !ok {
				return false
			} else {
				if value1 != value2 {
					return false
				}
			}
		}
	} else { // Unequal length implies that the PVs are pruned
		pruningWindow := sourcePruningWindow.get()
		iteratePv := Pv1
		otherPv := Pv2
		cas := cas1
		if len(Pv2) > len(Pv1) {
			iteratePv = Pv2
			otherPv = Pv1
			cas = cas2
		}
		for key, value1 := range iteratePv {
			value2, ok := otherPv[key]
			if !ok {
				if xdcrBase.CasDuration(value1, cas) >= pruningWindow {
					continue
				} else {
					return false
				}
			} else {
				if value1 != value2 {
					return false
				}
			}

		}
	}
	return true
}

func (entry oneEntry) GetRevId() uint64 {
	if entry.ImportCas == entry.Cas {
		return 0
	} else {
		return entry.RevId
	}
}

func (entry oneEntry) GetVersion() uint64 {
	if entry.ImportCas == 0 {
		return entry.Cas
	} else {
		if entry.Cas == entry.ImportCas {
			return entry.Hlv.GetCvCas()
		} else if entry.Cas < entry.ImportCas {
			// can never happen : for now we panic
			//TODO Darshan : see if you can handle the error properly instead of creating a panic
			panic("Import Cas never be greater than the doc cas")
		} else {
			return entry.Cas
		}
	}
}

func (entry *oneEntry) IsMutation() bool {
	return entry.OpCode == gomemcached.UPR_MUTATION
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

func NewFilesDiffer(file1, file2 string, collectionMapping map[uint32][]uint32, colFilterStrings []string, colFilterTgtIds []uint32) *FilesDiffer {
	differ := &FilesDiffer{
		file1:               *NewFileAttribute(file1),
		file2:               *NewFileAttribute(file2),
		collectionIdMapping: collectionMapping,
		colFilterStrings:    colFilterStrings,
		colFilterTgtIds:     colFilterTgtIds,
		duplicatedHintMap:   map[string][]uint8{},
	}
	if len(collectionMapping) == 0 {
		// This means this is legacy mode - no collection support
		differ.collectionIdMapping = make(map[uint32][]uint32)
		differ.collectionIdMapping[0] = []uint32{0}
	}
	return differ
}

func NewFilesDifferWithFDPool(file1, file2 string, fdPool *fdp.FdPool, collectionMapping map[uint32][]uint32, colFilterStrings []string, colFilterTgtIds []uint32) (*FilesDiffer, error) {
	var err error
	differ := NewFilesDiffer(file1, file2, collectionMapping, colFilterStrings, colFilterTgtIds)
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

func constructHlv(docCas uint64, importCAS uint64, bucketUUID string, hlvbytes []byte) (*hlv.HLV, error) {
	cvCas, cvSrc, cvVer, pvMap, mvMap, err := crMeta.ParseHlvFields(docCas, hlvbytes)
	if err != nil {
		return nil, err
	}
	//need to convert the uuid(hex) to base64 i.e. hlv required format
	bucketId, err1 := hlv.UUIDtoDocumentSource(bucketUUID)
	if err1 != nil {
		return nil, err1
	}
	var Hlv *hlv.HLV
	var err2 error
	if docCas == importCAS {
		Hlv, err2 = hlv.NewHLV(bucketId, cvCas, cvCas, cvSrc, cvVer, pvMap, mvMap)
	} else {
		Hlv, err2 = hlv.NewHLV(bucketId, docCas, cvCas, cvSrc, cvVer, pvMap, mvMap)
	}
	if err2 != nil {
		return nil, err2
	}
	return Hlv, nil
}

func extractHlvAndImportCas(XattrIterator *xdcrBase.XattrIterator, bucketUUID string, docCas uint64) (hlv *hlv.HLV, importCas uint64, err error) {
	var key, value []byte
	var xattrHlv, xattrImportCas []byte
	for XattrIterator.HasNext() {
		key, value, err = XattrIterator.Next()
		if err != nil {
			return
		}
		if xdcrBase.Equals(key, xdcrBase.XATTR_HLV) {
			xattrHlv = value
		}
		if xdcrBase.Equals(key, xdcrBase.XATTR_IMPORTCAS) {
			xattrImportCas = value
		}
	}
	// any errors in parsing importCas we dont parse/construct the hlv beacuse it is not possible to determine if the hlv is outdated/updated
	if xattrImportCas != nil {
		// Remove the start/end quotes before converting it to uint64
		xattrLen := len(xattrImportCas)
		importCas, err = xdcrBase.HexLittleEndianToUint64(xattrImportCas[1 : xattrLen-1])
		if err != nil {
			return
		}
	}
	if xattrHlv == nil {
		return
	} else {
		hlv, err = constructHlv(docCas, importCas, bucketUUID, xattrHlv)
		return
	}

}

func getOneEntry(readOp fdp.FileOp) (*oneEntry, error) {
	entry := &oneEntry{}

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
	entry.RevId = binary.BigEndian.Uint64(revIdBytes)

	casBytes := make([]byte, 8)
	bytesRead, err = readOp(casBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read casBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Cas = binary.BigEndian.Uint64(casBytes)

	flagBytes := make([]byte, 4)
	bytesRead, err = readOp(flagBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read flagsBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Flags = binary.BigEndian.Uint32(flagBytes)

	expiryBytes := make([]byte, 4)
	bytesRead, err = readOp(expiryBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read expiryBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Expiry = binary.BigEndian.Uint32(expiryBytes)

	opCodeBytes := make([]byte, 2)
	bytesRead, err = readOp(opCodeBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read opCodeBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.OpCode = gomemcached.CommandCode(binary.BigEndian.Uint16(opCodeBytes))

	dataTypeBytes := make([]byte, 2)
	bytesRead, err = readOp(dataTypeBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read dataTypeBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Datatype = uint8(binary.BigEndian.Uint16(dataTypeBytes))

	xattrSizebytes := make([]byte, 4)
	bytesRead, err = readOp(xattrSizebytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read XattrSizeBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	xattrSize := uint32(binary.BigEndian.Uint32(xattrSizebytes))
	entry.XattrSize = xattrSize

	xattrBytes := make([]byte, xattrSize)
	bytesRead, err = readOp(xattrBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read xattrs, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Xattr = xattrBytes

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
	var XattrIterator *xdcrBase.XattrIterator = &xdcrBase.XattrIterator{}
	for {
		entry, err = getOneEntry(attr.readOp)
		if err != nil {
			break
		}
		entry.bucketUUID, err = hlv.UUIDtoDocumentSource(attr.bucketUUID)
		if err != nil {
			break
		}
		err := XattrIterator.ResetXattrIterator(entry.Xattr, entry.XattrSize)
		if err != nil {
			if !strings.Contains(err.Error(), BodyNilError) {
				break
			}
		} else {
			entry.Hlv, entry.ImportCas, err = extractHlvAndImportCas(XattrIterator, attr.bucketUUID, entry.Cas)
			if err != nil {
				fmt.Printf("An error occurred while constructing Hlv or ImportCas for document with key %v when read from file %v , err: %v", entry.Key, attr.name, err)
				break
			}
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
	} else {
		file, err := os.Open(attr.name)
		defer file.Close()
		if err != nil {
			return err
		}
		attr.readOp = file.Read
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
		fmt.Printf("Error when loading file1 contents: %v\n", differ.err1)
	}
	if differ.err2 != nil {
		fmt.Printf("Error when loading file2 contents: %v\n", differ.err2)
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
