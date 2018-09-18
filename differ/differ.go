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
}

type FileAttributes struct {
	name          string
	entries       map[string]*oneEntry
	sortedEntries []*oneEntry
}

func NewFileAttribute(fileName string) *FileAttributes {
	attr := &FileAttributes{
		name:    fileName,
		entries: make(map[string]*oneEntry),
	}
	return attr
}

type oneEntry struct {
	Key      string
	Seqno    uint64
	RevId    uint64
	Cas      uint64
	Flags    uint32
	Expiry   uint32
	BodyHash [sha512.Size]byte
}

func (oneEntry *oneEntry) String() string {
	return fmt.Sprintf("Key: %v Seqno: %v RevId: %v Cas: %v Flags %v Expiry: %v Hash: %s",
		oneEntry.Key, oneEntry.Seqno, oneEntry.RevId, oneEntry.Cas, oneEntry.Flags, oneEntry.Expiry, hex.EncodeToString(oneEntry.BodyHash[:]))
}

type entryPair struct {
	A *oneEntry
	B *oneEntry
}

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
//  0 - Names are the same
//  1 - If entry name > other name
// -1 - If entry name < other name
func (entry oneEntry) Diff(other oneEntry) (int, bool) {
	if entry.Key != other.Key {
		if entry.Key > other.Key {
			return 1, false
		} else {
			return -1, false
		}
	} else if entry.RevId != other.RevId {
		return 0, false
	} else if entry.Cas != other.Cas {
		return 0, false
	} else if entry.Flags != other.Flags {
		return 0, false
	} else if !shaCompare(entry.BodyHash, other.BodyHash) {
		return 0, false
	}
	return 0, true
}

func NewFilesDiffer(file1, file2 string) *FilesDiffer {
	differ := &FilesDiffer{
		file1: *NewFileAttribute(file1),
		file2: *NewFileAttribute(file2),
	}
	return differ
}

func getOneEntry(fileHandle *os.File) (*oneEntry, error) {
	entry := &oneEntry{}

	keyLenBytes := make([]byte, 2)
	bytesRead, err := fileHandle.Read(keyLenBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read keyLen, bytes read: %v, err: %v", bytesRead, err)
	}
	entryKeyLen := binary.BigEndian.Uint16(keyLenBytes)

	keyBytes := make([]byte, entryKeyLen)
	bytesRead, err = fileHandle.Read(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read key, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Key = string(keyBytes)

	seqnoBytes := make([]byte, 8)
	bytesRead, err = fileHandle.Read(seqnoBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read seqno, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Seqno = binary.BigEndian.Uint64(seqnoBytes)

	revIdBytes := make([]byte, 8)
	bytesRead, err = fileHandle.Read(revIdBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read revIdBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.RevId = binary.BigEndian.Uint64(revIdBytes)

	casBytes := make([]byte, 8)
	bytesRead, err = fileHandle.Read(casBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read casBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Cas = binary.BigEndian.Uint64(casBytes)

	flagBytes := make([]byte, 4)
	bytesRead, err = fileHandle.Read(flagBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read flagsBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Flags = binary.BigEndian.Uint32(flagBytes)

	expiryBytes := make([]byte, 4)
	bytesRead, err = fileHandle.Read(expiryBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read expiryBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	entry.Expiry = binary.BigEndian.Uint32(expiryBytes)

	hashBytes := make([]byte, sha512.Size)
	bytesRead, err = fileHandle.Read(hashBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read hashBytes, bytes read: %v, err: %v", bytesRead, err)
	}
	copy(entry.BodyHash[:], hashBytes)

	return entry, nil
}

func (a ByKeyName) Len() int           { return len(a) }
func (a ByKeyName) Swap(i, j int)      { *a[i], *a[j] = *a[j], *a[i] }
func (a ByKeyName) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (attr *FileAttributes) fillAndDedupEntries(fileHandle *os.File) error {
	var err error
	var entry *oneEntry

	for err == nil {
		entry, err = getOneEntry(fileHandle)
		if err != nil {
			break
		}

		if curEntry, ok := attr.entries[entry.Key]; !ok {
			attr.entries[entry.Key] = entry
		} else {
			// Replace the entry in the map if the seqno is newer
			if entry.Seqno > curEntry.Seqno {
				attr.entries[entry.Key] = entry
			}
		}
	}

	if err != nil && strings.Contains(err.Error(), io.EOF.Error()) {
		err = nil
	}

	return err
}

func (attr *FileAttributes) sortEntries() {
	for _, v := range attr.entries {
		attr.sortedEntries = append(attr.sortedEntries, v)
	}

	sort.Sort(ByKeyName(attr.sortedEntries))
}

func (attr *FileAttributes) LoadFileIntoBuffer() error {
	if len(attr.name) == 0 {
		return fmt.Errorf("No file specified")
	}
	file, err := os.Open(attr.name)
	defer file.Close()
	if err != nil {
		return err
	}
	err = attr.fillAndDedupEntries(file)
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

func (differ *FilesDiffer) diffSorted() bool {
	file1Len := len(differ.file1.sortedEntries)
	file2Len := len(differ.file2.sortedEntries)

	if file1Len == 0 && file2Len == 0 {
		return true
	}

	var i int
	var j int

	for i < file1Len && j < file2Len {
		item1 := differ.file1.sortedEntries[i]
		item2 := differ.file2.sortedEntries[j]

		keyCompare, match := item1.Diff(*item2)
		if match {
			// Both items are the same
			i++
			j++
		} else {
			if keyCompare == 0 {
				// Both document are the same, but others mismatched
				pair := &entryPair{
					A: item1,
					B: item2,
				}
				differ.BothExistButMismatch = append(differ.BothExistButMismatch, pair)
				i++
				j++
			} else if keyCompare < 0 {
				// Like "a" < "b", where a is 1 and b is 2
				differ.MissingFromFile2 = append(differ.MissingFromFile2, item1)
				i++
			} else {
				// "b" > "a", leading to keyCompare > 0
				differ.MissingFromFile1 = append(differ.MissingFromFile1, item2)
				j++
			}
		}
	}

	for ; i < file1Len; i++ {
		// This means that all the rest of the entries in file1 are missing from file2
		differ.MissingFromFile2 = append(differ.MissingFromFile2, differ.file1.sortedEntries[i])
	}

	for ; j < file2Len; j++ {
		// This means that all the rest of the entries in file2 are missing from file1
		differ.MissingFromFile1 = append(differ.MissingFromFile1, differ.file1.sortedEntries[j])
	}

	return len(differ.BothExistButMismatch) == 0 && len(differ.MissingFromFile1) == 0 && len(differ.MissingFromFile2) == 0
}

// Returns true if they are the same
func (differ *FilesDiffer) Diff() bool {
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

	return differ.diffSorted()
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
			fmt.Printf("%v Docs exist in both files but mismatch:\n", mismatchCnt)
			fmt.Printf("--------------------------------------\n")
			for i := 0; i < mismatchCnt; i++ {
				fmt.Printf("File1: %v\n", differ.BothExistButMismatch[i].A.String())
				fmt.Printf("File2: %v\n", differ.BothExistButMismatch[i].B.String())
			}
			fmt.Printf("--------------------------------------\n")
		}
		if missing2Cnt > 0 {
			fmt.Printf("%v Docs exist in file 1 that are missing from file2:\n", missing2Cnt)
			fmt.Printf("-------------------------------------------------\n")
			for i := 0; i < missing2Cnt; i++ {
				fmt.Printf("%v\n", differ.MissingFromFile2[i].String())
			}
			fmt.Printf("-------------------------------------------------\n")
		}
		if missing1Cnt > 0 {
			fmt.Printf("%v Docs exist in file 2 that are missing from file1:\n", missing1Cnt)
			fmt.Printf("-------------------------------------------------\n")
			for i := 0; i < missing1Cnt; i++ {
				fmt.Printf("%v\n", differ.MissingFromFile1[i].String())
			}
			fmt.Printf("-------------------------------------------------\n")
		}
	}
}

func (differ *FilesDiffer) ToJson() ([]byte, error) {
	outputMap := map[string]interface{}{
		"Mismatch":         differ.BothExistButMismatch,
		"MissingFromFile1": differ.MissingFromFile1,
		"MissingFromFile2": differ.MissingFromFile2,
	}

	ret, err := json.Marshal(outputMap)

	return ret, err
}
