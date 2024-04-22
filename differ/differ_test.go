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
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
	"xdcrDiffer/dcp"
	fdp "xdcrDiffer/fileDescriptorPool"

	"github.com/couchbase/gomemcached"
	xdcrBase "github.com/couchbase/goxdcr/base"
	hlv "github.com/couchbase/goxdcr/hlv"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/stretchr/testify/assert"
)

const MaxUint64 = ^uint64(0)
const MinUint = 0

const (
	eventingKey = "_eventing"
	ftsKey      = "_fts"
	queryKey    = "_query"
	indexingKey = "_indexing"
)

var randomOnce sync.Once

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

// serialize mutation into []byte
// format:
//
//	keyLen  - 2 bytes
//	key  - length specified by keyLen
//	seqno   - 8 bytes
//	revId   - 8 bytes
//	cas     - 8 bytes
//	flags   - 4 bytes
//	expiry  - 4 bytes
//	opCode - 1 bytes
//	hash    - 64 bytes
func genTestData(withXattrs, regularMutation, colFilters bool) (key string, seqno, revId, cas uint64, flags, expiry uint32, opCode gomemcached.CommandCode, hash [64]byte, ret []byte, colId uint32, filterIds []uint8, err error) {
	var mutationToSerialize dcp.Mutation
	randomOnce.Do(func() {
		rand.Seed(time.Now().UTC().UnixNano())
	})

	key = randomString(randInt(12, 64))
	seqno = rand.Uint64()
	revId = rand.Uint64()
	cas = rand.Uint64()
	flags = rand.Uint32()
	expiry = rand.Uint32()
	if regularMutation {
		opCode = gomemcached.UPR_MUTATION
	} else {
		opCodeArray := [3]gomemcached.CommandCode{gomemcached.UPR_MUTATION, gomemcached.UPR_DELETION, gomemcached.UPR_EXPIRATION}
		opCode = opCodeArray[rand.Uint32()%3]
	}
	// Note we don't have the actual body hash so just randomly generate a hash using key
	hash = sha512.Sum512([]byte(key))

	if colFilters {
		randomLen := uint8(rand.Int() % 8)
		for i := uint8(0); i < randomLen; i++ {
			filterIds = append(filterIds, i)
		}
	}

	//dataSlice := createDataByteSlice(key, seqno, revId, cas, flags, expiry, opCode, hash, colId, filterIds)
	if !withXattrs {
		mutationToSerialize = dcp.Mutation{
			Vbno:              0,
			Key:               []byte(key),
			Seqno:             seqno,
			RevId:             revId,
			Cas:               cas,
			Flags:             flags,
			Expiry:            expiry,
			OpCode:            opCode,
			Value:             []byte(key),
			Datatype:          0,
			ColId:             0,
			ColFiltersMatched: filterIds,
		}
	} else {
		xattrIterator := &xdcrBase.XattrIterator{}
		xattrKeysToExclude := genXattrKeysToExclude()
		ret, err = genXattrsWithBody(key)
		if err != nil {
			return
		}
		mutationToSerialize = dcp.Mutation{
			Vbno:                  0,
			Key:                   []byte(key),
			Seqno:                 seqno,
			RevId:                 revId,
			Cas:                   cas,
			Flags:                 flags,
			Expiry:                expiry,
			OpCode:                opCode,
			Value:                 ret,
			Datatype:              0,
			ColId:                 0,
			ColFiltersMatched:     filterIds,
			XattrIterator:         xattrIterator,
			XattrKeysForNoCompare: xattrKeysToExclude,
		}
	}

	ret, err = mutationToSerialize.Serialize()
	return key, seqno, revId, cas, flags, expiry, opCode, hash, ret, colId, filterIds, err
}

func genXattrsWithBody(body string) ([]byte, error) {
	var keySize int = randInt(12, 64)
	var valueSize int = randInt(15, 70)
	var numOfKeyValuePairs int = randInt(5, 10)
	var netSize int = (numOfKeyValuePairs * (keySize + valueSize + 6)) + len(body) + 4
	var xattrsPlusBody []byte = make([]byte, netSize)
	xattrComposer := xdcrBase.NewXattrComposer(xattrsPlusBody)
	for i := 0; i < numOfKeyValuePairs; i++ {
		key := randomString(keySize)
		value := randomString(valueSize)
		err := xattrComposer.WriteKV([]byte(key), []byte(value))
		if err != nil {
			return nil, err
		}
	}
	xattrsPlusBody, _ = xattrComposer.FinishAndAppendDocValue([]byte(body))
	return xattrsPlusBody, nil
}

func genXattrKeysToExclude() map[string]bool {
	keys := []string{eventingKey, ftsKey, indexingKey, queryKey}
	randomLen := rand.Int() % 4
	xattrKeysToExclude := make(map[string]bool)
	for i := 0; i < randomLen; i++ {
		xattrKeysToExclude[keys[i]] = true
	}
	return xattrKeysToExclude
}
func genMultipleRecords(numOfRecords int) []byte {
	var retSlice []byte

	for i := 0; i < numOfRecords; i++ {
		if i%2 == 0 {
			_, _, _, _, _, _, _, _, record, _, _, err := genTestData(false, true, false)
			if err != nil {
				i--
			} else {
				retSlice = append(retSlice, record...)
			}
		} else {
			_, _, _, _, _, _, _, _, record, _, _, err := genTestData(true, true, false)
			if err != nil {
				i--
			} else {
				retSlice = append(retSlice, record...)
			}
		}
	}

	return retSlice
}

func genSameFiles(numOfRecords int, fileName1, fileName2 string) error {
	data := genMultipleRecords(numOfRecords)

	err := ioutil.WriteFile(fileName1, data, 0644)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(fileName2, data, 0644)
	if err != nil {
		return err
	}

	return nil
}

func genMismatchedFiles(numOfRecords, mismatchCnt int, fileName1, fileName2 string) ([]string, error) {
	var mismatchedKeyNames []string
	data := genMultipleRecords(numOfRecords - mismatchCnt)

	err := ioutil.WriteFile(fileName1, data, 0644)
	if err != nil {
		return mismatchedKeyNames, err
	}

	err = ioutil.WriteFile(fileName2, data, 0644)
	if err != nil {
		return mismatchedKeyNames, err
	}

	// Now create mismatched entries
	f1, err := os.OpenFile(fileName1, os.O_APPEND|os.O_WRONLY, 644)
	if err != nil {
		return mismatchedKeyNames, err
	}
	defer f1.Close()

	f2, err := os.OpenFile(fileName2, os.O_APPEND|os.O_WRONLY, 644)
	if err != nil {
		return mismatchedKeyNames, err
	}
	defer f2.Close()

	for i := 0; i < mismatchCnt; i++ {
		withXattrs := i%2 == 0
		key, seqno, revId, cas, flags, expiry, opCode, _, oneData, colId, _, er := genTestData(withXattrs, true, false)
		if er != nil {
			i--
			continue
		}
		mismatchedDataMut := &dcp.Mutation{
			Vbno:              0,
			Key:               []byte(key),
			Seqno:             seqno,
			RevId:             revId,
			Cas:               cas,
			Flags:             flags,
			Expiry:            expiry,
			OpCode:            opCode,
			Value:             []byte(key),
			Datatype:          0,
			ColId:             colId,
			ColFiltersMatched: nil,
		}
		mismatchedData, err := mismatchedDataMut.Serialize()
		if err != nil {
			i--
			continue
		}
		_, err = f1.Write(oneData)
		if err != nil {
			return mismatchedKeyNames, err
		}

		_, err = f2.Write(mismatchedData)
		if err != nil {
			return mismatchedKeyNames, err
		}

		mismatchedKeyNames = append(mismatchedKeyNames, key)
	}

	return mismatchedKeyNames, nil
}

func verifyMisMatch(mismatchKeys []string, differ *FilesDiffer) bool {
	for _, key := range mismatchKeys {
		found := false
		for _, onePair := range differ.BothExistButMismatch {
			if key == onePair[0].Key {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestLoader(t *testing.T) {
	testLogger := xdcrLog.NewLogger("testLogger", xdcrLog.DefaultLoggerContext)
	assert := assert.New(t)
	var outputFileTemp string = "/tmp/xdcrDiffer.tmp"
	defer os.Remove(outputFileTemp)
	withXattrs := rand.Int()%2 == 0
	key, seqno, _, _, _, _, _, _, data, _, _, er := genTestData(withXattrs, true, false)
	if er != nil {
		fmt.Printf("An error occured while running TestLoader. err: %v", er)
		return
	}

	err := ioutil.WriteFile(outputFileTemp, data, 0644)
	assert.Nil(err)

	differ := NewFilesDiffer(outputFileTemp, "", nil, nil, nil, testLogger)
	err = differ.file1.LoadFileIntoBuffer()
	assert.Nil(err)

	assert.Equal(1, len(differ.file1.entries[0]))
	assert.Equal(seqno, differ.file1.entries[0][key].Seqno)

	assert.Equal(1, len(differ.file1.sortedEntries[0]))
	assert.Equal(seqno, differ.file1.sortedEntries[0][0].Seqno)
}

func TestLoaderWithColFilters(t *testing.T) {
	testLogger := xdcrLog.NewLogger("testLogger", xdcrLog.DefaultLoggerContext)
	assert := assert.New(t)
	var outputFileTemp string = "/tmp/xdcrDiffer.tmp"
	defer os.Remove(outputFileTemp)
	withXattrs := rand.Int()%2 == 0
	key, _, _, _, _, _, _, _, data, _, filterIds, er := genTestData(withXattrs, true, true)
	if er != nil {
		fmt.Printf("An error occured while running TestLoader. err: %v", er)
		return
	}
	err := ioutil.WriteFile(outputFileTemp, data, 0644)
	assert.Nil(err)

	differ := NewFilesDiffer(outputFileTemp, "", nil, nil, nil, testLogger)
	err = differ.file1.LoadFileIntoBuffer()
	assert.Nil(err)

	assert.Equal(1, len(differ.file1.entries[0]))
	assert.Equal(uint8(len(filterIds)), differ.file1.entries[0][key].ColMigrFilterLen)
	for i := 0; i < len(filterIds); i++ {
		assert.Equal(filterIds[i], differ.file1.entries[0][key].ColFiltersMatched[i])
	}
}

func TestLoadSameFile(t *testing.T) {
	testLogger := xdcrLog.NewLogger("testLogger", xdcrLog.DefaultLoggerContext)
	fmt.Println("============== Test case start: TestLoadSameFile =================")
	assert := assert.New(t)

	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 10000

	err := genSameFiles(entries, file1, file2)
	assert.Equal(nil, err)

	differ := NewFilesDiffer(file1, file2, nil, nil, nil, testLogger)
	assert.NotNil(differ)

	srcDiffMap, tgtDiffMap, _, _, _ := differ.Diff()

	assert.True(len(srcDiffMap) == 0)
	assert.True(len(tgtDiffMap) == 0)
	differ.PrettyPrintResult()
	fmt.Println("============== Test case end: TestLoadSameFile =================")
}

// This test used to work because it used a customized test generator
// But now that is incorrect and the test is no longer valid
func Disabled_TestLoadMismatchedFilesOnly(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadMismatchedFilesOnly =================")
	testLogger := xdcrLog.NewLogger("testLogger", xdcrLog.DefaultLoggerContext)
	assert := assert.New(t)

	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 10000
	numMismatch := 5

	keys, err := genMismatchedFiles(entries, numMismatch, file1, file2)
	assert.Nil(err)

	differ := NewFilesDiffer(file1, file2, nil, nil, nil, testLogger)
	assert.NotNil(differ)

	srcDiffMap, tgtDiffMap, _, _, _ := differ.Diff()

	assert.False(len(srcDiffMap) == 0)
	assert.False(len(tgtDiffMap) == 0)

	assert.Equal(numMismatch, len(differ.BothExistButMismatch))
	assert.True(verifyMisMatch(keys, differ))

	assert.Equal(0, len(differ.MissingFromFile1))
	assert.Equal(0, len(differ.MissingFromFile2))

	differ.PrettyPrintResult()
	fmt.Println("============== Test case end: TestLoadMismatchedFilesOnly =================")
}

// This test used to work because it used a customized test generator
// But now that is incorrect and the test is no longer valid
func Disabled_TestLoadMismatchedFilesAndUneven(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadMismatchedFilesAndUneven =================")
	assert := assert.New(t)
	testLogger := xdcrLog.NewLogger("testLogger", xdcrLog.DefaultLoggerContext)
	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 1000
	numMismatch := 5
	extraEntries := 2

	keys, err := genMismatchedFiles(entries, numMismatch, file1, file2)
	assert.Nil(err)

	// Add more records to one file
	extraSliceOfPizza := genMultipleRecords(extraEntries)
	f, err := os.OpenFile(file1, os.O_APPEND|os.O_WRONLY, 644)
	assert.Nil(err)
	_, err = f.Write(extraSliceOfPizza)
	assert.Nil(err)
	f.Close()

	differ := NewFilesDiffer(file1, file2, nil, nil, nil, testLogger)
	assert.NotNil(differ)

	srcDiffMap, tgtDiffMap, _, _, _ := differ.Diff()

	assert.False(len(srcDiffMap) == 0)
	assert.False(len(tgtDiffMap) == 0)

	assert.Equal(numMismatch, len(differ.BothExistButMismatch))
	assert.True(verifyMisMatch(keys, differ))

	assert.Equal(0, len(differ.MissingFromFile1))
	assert.Equal(extraEntries, len(differ.MissingFromFile2))
	differ.PrettyPrintResult()
	fmt.Println("============== Test case start: TestLoadMismatchedFilesAndUneven =================")
}

func TestLoadSameFileWPool(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadSameFileWPool =================")
	assert := assert.New(t)
	testLogger := xdcrLog.NewLogger("testLogger", xdcrLog.DefaultLoggerContext)
	fileDescPool := fdp.NewFileDescriptorPool(50)

	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 10000

	err := genSameFiles(entries, file1, file2)
	assert.Equal(nil, err)

	differ, err := NewFilesDifferWithFDPool(file1, file2, fileDescPool, nil, nil, nil, testLogger)
	assert.NotNil(differ)
	assert.Nil(err)

	srcDiffMap, tgtDiffMap, _, _, _ := differ.Diff()

	assert.True(len(srcDiffMap) == 0)
	assert.True(len(tgtDiffMap) == 0)
	fmt.Println("============== Test case end: TestLoadSameFileWPool =================")
}

func TestNoFilePool(t *testing.T) {
	fmt.Println("============== Test case start: TestNoFilePool =================")
	testLogger := xdcrLog.NewLogger("testLogger", xdcrLog.DefaultLoggerContext)
	assert := assert.New(t)

	differDriver := NewDifferDriver("", "", "", "", 2, 2, 0, nil, nil, nil, "", "", nil, nil, testLogger)
	assert.NotNil(differDriver)
	assert.Nil(differDriver.fileDescPool)
	fmt.Println("============== Test case end: TestNoFilePool =================")
}
func Test_compareHlv(t *testing.T) {
	sourcePruningWindow.duration = time.Duration(5) * time.Nanosecond
	targetPruningWindow.duration = time.Duration(5) * time.Nanosecond
	sourceBucketUUID := hlv.DocumentSourceId(randomString(10))
	targetBucketUUID := hlv.DocumentSourceId(randomString(10))
	type args struct {
		hlv1        *hlv.HLV
		hlv2        *hlv.HLV
		cas1        uint64
		cas2        uint64
		bucketUUID1 hlv.DocumentSourceId
		bucketUUID2 hlv.DocumentSourceId
	}
	tests := []struct {
		name string
		args args
		same bool
	}{
		//Test1 : both source and target HLVs are nil
		{
			name: "HLVs Absent",
			args: args{
				hlv1:        nil,
				hlv2:        nil,
				cas1:        rand.Uint64(),
				cas2:        rand.Uint64(),
				bucketUUID1: sourceBucketUUID,
				bucketUUID2: targetBucketUUID,
			},
			same: true,
		},
		//Test2 : HLV is present at the source
		{
			name: "HLV present at Source",
			args: args{
				hlv1:        generateHLV(sourceBucketUUID, 10, 10, targetBucketUUID, 10, nil, nil),
				hlv2:        nil,
				cas1:        10,
				cas2:        10,
				bucketUUID1: sourceBucketUUID,
				bucketUUID2: targetBucketUUID,
			},
			same: true,
		},
		//Test3 : HLV is present at the target
		{
			name: "HLV present at Target",
			args: args{
				hlv1:        nil,
				hlv2:        generateHLV(targetBucketUUID, 20, 20, sourceBucketUUID, 20, nil, nil),
				cas1:        20,
				cas2:        20,
				bucketUUID1: sourceBucketUUID,
				bucketUUID2: targetBucketUUID,
			},
			same: true,
		},
		//Test4 : both the HLVs are present - with one of them outdated(This case involves implicit construction of HLV)
		{
			name: "HLV present at both source and target but outdated at source",
			args: args{
				hlv1:        generateHLV(sourceBucketUUID, 30, 20, targetBucketUUID, 20, hlv.VersionsMap{}, nil),
				hlv2:        generateHLV(targetBucketUUID, 30, 30, sourceBucketUUID, 30, hlv.VersionsMap{targetBucketUUID: 20}, nil),
				cas1:        30,
				cas2:        30,
				bucketUUID1: sourceBucketUUID,
				bucketUUID2: targetBucketUUID,
			},
			same: true,
		},
		//Test5 : both the HLVs are present - with PV pruned in one of them
		{
			name: "HLV present at both source and target with Missing PVs(Pruned version at Target) ",
			args: args{
				hlv1:        generateHLV(sourceBucketUUID, 30, 20, sourceBucketUUID, 20, hlv.VersionsMap{hlv.DocumentSourceId(randomString(10)): 1}, nil),
				hlv2:        generateHLV(targetBucketUUID, 30, 30, sourceBucketUUID, 30, hlv.VersionsMap{}, nil),
				cas1:        30,
				cas2:        30,
				bucketUUID1: sourceBucketUUID,
				bucketUUID2: targetBucketUUID,
			},
			same: true,
		},
		//Test6: HLV cvCASs missmatch
		{
			name: "HLV cvCAS mismatch",
			args: args{
				hlv1:        generateHLV(sourceBucketUUID, 40, 40, sourceBucketUUID, 40, nil, nil),
				hlv2:        generateHLV(targetBucketUUID, 20, 20, sourceBucketUUID, 20, nil, nil),
				cas1:        40,
				cas2:        20,
				bucketUUID1: sourceBucketUUID,
				bucketUUID2: targetBucketUUID,
			},
			same: false,
		},
		//Test7: PVs present on both source and target with pruning and data mismatch
		{
			name: "PV pruning+mismatch",
			args: args{
				hlv1:        generateHLV(sourceBucketUUID, 30, 30, sourceBucketUUID, 30, hlv.VersionsMap{targetBucketUUID: 10, hlv.DocumentSourceId(randomString(10)): 1}, nil),
				hlv2:        generateHLV(targetBucketUUID, 30, 30, sourceBucketUUID, 30, hlv.VersionsMap{targetBucketUUID: 20}, nil),
				cas1:        30,
				cas2:        30,
				bucketUUID1: sourceBucketUUID,
				bucketUUID2: targetBucketUUID,
			},
			same: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareHlv(tt.args.hlv1, tt.args.hlv2, tt.args.cas1, tt.args.cas2, tt.args.bucketUUID1, tt.args.bucketUUID2); got != tt.same {
				t.Errorf("compareHlv() = %v, want %v", got, tt.same)
			}
		})
	}
}

func generateHLV(source hlv.DocumentSourceId, cas uint64, cvCas uint64, src hlv.DocumentSourceId, ver uint64, pv hlv.VersionsMap, mv hlv.VersionsMap) *hlv.HLV {
	HLV, _ := hlv.NewHLV(source, cas, cvCas, src, ver, pv, mv)
	return HLV
}
