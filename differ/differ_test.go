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
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

const MaxUint64 = ^uint64(0)
const MinUint = 0

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
//  keyLen  - 2 bytes
//  key  - length specified by keyLen
//  seqno   - 8 bytes
//  revId   - 8 bytes
//  cas     - 8 bytes
//  expiry  - 8 bytes?
//  hash    - 64 bytes
func genTestData() (key string, seqno, revId, cas, expiry uint64, hash [sha512.Size]byte, ret []byte) {
	randomOnce.Do(func() {
		rand.Seed(time.Now().UTC().UnixNano())
	})

	key = randomString(randInt(12, 64))
	seqno = rand.Uint64()
	revId = rand.Uint64()
	cas = rand.Uint64()
	expiry = rand.Uint64()
	// Note we don't have the actual body hash so just randomly generate a hash using key
	hash = sha512.Sum512([]byte(key))

	dataSlice := createDataByteSlice(key, seqno, revId, cas, expiry, hash)

	return key, seqno, revId, cas, expiry, hash, dataSlice
}

func createDataByteSlice(key string, seqno, revId, cas, expiry uint64, hash [sha512.Size]byte) []byte {
	var keyLen uint16 = uint16(len(key))
	retLength := 34 + sha512.Size + keyLen
	ret := make([]byte, retLength)

	// fmt.Printf("%v bytes generated: keyLen: %v key: %v seqno: %v revId: %v cas: %v expiry: %v hash: %v\n",
	// 	retLength, keyLen, key, seqno, revId, cas, expiry, hash)

	pos := 0
	binary.BigEndian.PutUint16(ret[pos:pos+2], keyLen)
	pos += 2
	copy(ret[pos:pos+int(keyLen)], key)
	pos += int(keyLen)
	binary.BigEndian.PutUint64(ret[pos:pos+8], seqno)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], revId)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], cas)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], expiry)
	pos += 8
	copy(ret[pos:], hash[:])

	return ret
}

func genMultipleRecords(numOfRecords int) []byte {
	var retSlice []byte

	for i := 0; i < numOfRecords; i++ {
		_, _, _, _, _, _, record := genTestData()
		retSlice = append(retSlice, record...)
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
		key, seqno, revId, cas, expiry, hash, oneData := genTestData()
		mismatchedData := createDataByteSlice(key, seqno, revId, cas+1, expiry, hash)

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
			if key == onePair.A.Key {
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
	assert := assert.New(t)
	var outputFileTemp string = "/tmp/xdcrDiffer.tmp"
	defer os.Remove(outputFileTemp)

	key, seqno, _, _, _, _, data := genTestData()

	err := ioutil.WriteFile(outputFileTemp, data, 0644)
	assert.Nil(err)

	differ := NewFilesDiffer(outputFileTemp, "")
	err = differ.file1.LoadFileIntoBuffer()
	assert.Nil(err)

	assert.Equal(1, len(differ.file1.entries))
	assert.Equal(seqno, differ.file1.entries[key].Seqno)

	assert.Equal(1, len(differ.file1.sortedEntries))
	assert.Equal(seqno, differ.file1.sortedEntries[0].Seqno)
}

func TestLoadSameFile(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadSameFile =================")
	assert := assert.New(t)

	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 10000

	err := genSameFiles(entries, file1, file2)
	assert.Equal(nil, err)

	differ := NewFilesDiffer(file1, file2)
	assert.NotNil(differ)

	result := differ.Diff()

	assert.True(result)
	differ.PrettyPrintResult()
	fmt.Println("============== Test case end: TestLoadSameFile =================")
}

func TestLoadMismatchedFiles(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadMismatchedFiles =================")
	assert := assert.New(t)

	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 10000
	numMismatch := 5

	keys, err := genMismatchedFiles(entries, numMismatch, file1, file2)
	assert.Nil(err)

	differ := NewFilesDiffer(file1, file2)
	assert.NotNil(differ)

	result := differ.Diff()

	assert.False(result)

	assert.Equal(numMismatch, len(differ.BothExistButMismatch))
	assert.True(verifyMisMatch(keys, differ))

	assert.Equal(0, len(differ.MissingFromFile1))
	assert.Equal(0, len(differ.MissingFromFile2))

	differ.PrettyPrintResult()
	fmt.Println("============== Test case end: TestLoadMismatchedFiles =================")
}

func TestLoadMismatchedFilesAndUneven(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadMismatchedFilesAndUneven =================")
	assert := assert.New(t)

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

	differ := NewFilesDiffer(file1, file2)
	assert.NotNil(differ)

	result := differ.Diff()

	assert.False(result)

	assert.Equal(numMismatch, len(differ.BothExistButMismatch))
	assert.True(verifyMisMatch(keys, differ))

	assert.Equal(0, len(differ.MissingFromFile1))
	assert.Equal(extraEntries, len(differ.MissingFromFile2))
	differ.PrettyPrintResult()
	fmt.Println("============== Test case start: TestLoadMismatchedFilesAndUneven =================")
}
