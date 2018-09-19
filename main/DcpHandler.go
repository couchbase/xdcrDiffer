// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package main

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
	"os"
	"sync"
)

// implements StreamObserver
type DcpHandler struct {
	dcpClient         *DcpClient
	checkpointManager *CheckpointManager
	fileDir           string
	index             int
	vbList            []uint16
	numberOfBuckets   int
	dataChan          chan *Mutation
	waitGrp           sync.WaitGroup
	finChan           chan bool
	bucketMap         map[uint16]map[int]*Bucket
}

func NewDcpHandler(dcpClient *DcpClient, checkpointManager *CheckpointManager, fileDir string, index int, vbList []uint16, numberOfBuckets int) (*DcpHandler, error) {
	if len(vbList) == 0 {
		return nil, fmt.Errorf("vbList is empty for handler %v", index)
	}
	return &DcpHandler{
		dcpClient:         dcpClient,
		checkpointManager: checkpointManager,
		fileDir:           fileDir,
		index:             index,
		vbList:            vbList,
		numberOfBuckets:   numberOfBuckets,
		dataChan:          make(chan *Mutation, DcpHandlerChanSize),
		finChan:           make(chan bool),
		bucketMap:         make(map[uint16]map[int]*Bucket),
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

	dh.cleanup()
}

func (dh *DcpHandler) initialize() error {
	for _, vbno := range dh.vbList {
		innerMap := make(map[int]*Bucket)
		dh.bucketMap[vbno] = innerMap
		for i := 0; i < dh.numberOfBuckets; i++ {
			bucket, err := NewBucket(dh.fileDir, vbno, i)
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
			fmt.Printf("Cannot find innerMap for vbno %v at cleanup\n", vbno)
			continue
		}
		for i := 0; i < dh.numberOfBuckets; i++ {
			bucket := innerMap[i]
			if bucket == nil {
				fmt.Printf("Cannot find bucket for vbno %v and index %v at cleanup\n", vbno, i)
				continue
			}
			bucket.close()
		}
	}
}

func (dh *DcpHandler) processData() {
	fmt.Printf("%v DcpHandler %v processData starts..........\n", dh.dcpClient.Name, dh.index)
	defer fmt.Printf("%v DcpHandler %v processData exits..........\n", dh.dcpClient.Name, dh.index)
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
	vbno := mut.vbno
	index := GetBucketIndexFromKey(mut.key, dh.numberOfBuckets)
	innerMap := dh.bucketMap[vbno]
	if innerMap == nil {
		panic(fmt.Sprintf("cannot find bucketMap for vbno %v", vbno))
	}
	bucket := innerMap[index]
	if bucket == nil {
		panic(fmt.Sprintf("cannot find bucket for index %v", index))
	}
	bucket.write(serializeMutation(mut))
	dh.checkpointManager.HandleMutationProcessedEvent(mut)
}

func (dh *DcpHandler) writeToDataChan(mut *Mutation) {
	select {
	case dh.dataChan <- mut:
	// provides an alternative exit path when dh stops
	case <-dh.finChan:
	}
}

func (dh *DcpHandler) SnapshotMarker(startSeqno, endSeqno uint64, vbno uint16, snapshotType gocbcore.SnapshotState) {
	dh.checkpointManager.updateSnapshot(vbno, startSeqno, endSeqno)
}

func (dh *DcpHandler) Mutation(seqno, revId uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbno uint16, key, value []byte) {
	dh.writeToDataChan(CreateMutation(vbno, key, seqno, revId, cas, flags, expiry, value))
}

func (dh *DcpHandler) Deletion(seqno, revId, cas uint64, datatype uint8, vbno uint16, key, value []byte) {
	dh.writeToDataChan(CreateMutation(vbno, key, seqno, revId, cas, 0, 0, value))
}

func (dh *DcpHandler) Expiration(seqno, revId, cas uint64, vbno uint16, key []byte) {
	dh.writeToDataChan(CreateMutation(vbno, key, seqno, revId, cas, 0, 0, nil))
}

func (dh *DcpHandler) End(vbno uint16, err error) {
	dh.dcpClient.handleVbucketCompletion(vbno, err)
}

type Bucket struct {
	data []byte
	// current index in data for next write
	index    int
	file     *os.File
	fileName string
}

func NewBucket(fileDir string, vbno uint16, bucketIndex int) (*Bucket, error) {
	fileName := GetFileName(fileDir, vbno, bucketIndex)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, FileModeReadWrite)
	if err != nil {
		return nil, err
	}
	return &Bucket{
		data:     make([]byte, BucketBufferCapacity),
		index:    0,
		file:     file,
		fileName: fileName,
	}, nil
}

func (b *Bucket) write(item []byte) error {
	if b.index+len(item) > BucketBufferCapacity {
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
	numOfBytes, err := b.file.Write(b.data[:b.index])
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
		fmt.Printf("Error flushing to file %v at bucket close err=%v\n", b.file.Name(), err)
	}
	err = b.file.Close()
	if err != nil {
		fmt.Printf("Error closing file %v.  err=%v\n", b.file.Name(), err)
	}
}

type Mutation struct {
	vbno   uint16
	key    []byte
	seqno  uint64
	revId  uint64
	cas    uint64
	flags  uint32
	expiry uint32
	value  []byte
}

func CreateMutation(vbno uint16, key []byte, seqno, revId, cas uint64, flags, expiry uint32, value []byte) *Mutation {
	return &Mutation{
		vbno:   vbno,
		key:    key,
		seqno:  seqno,
		revId:  revId,
		cas:    cas,
		flags:  flags,
		expiry: expiry,
		value:  value,
	}
}

// serialize mutation into []byte
// format:
//  keyLen  - 2 bytes
//  key  - length specified by keyLen
//  seqno   - 8 bytes
//  revId   - 8 bytes
//  cas     - 8 bytes
//  flags   - 4 bytes
//  expiry  - 4 bytes
//  hash    - 64 bytes
func serializeMutation(mut *Mutation) []byte {
	keyLen := len(mut.key)
	ret := make([]byte, keyLen+BodyLength+2)
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
	copy(ret[pos:], bodyHash[:])

	return ret
}
