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
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"xdcrDiffer/base"
	fdp "xdcrDiffer/fileDescriptorPool"
	"xdcrDiffer/utils"
)

type DiffKeysMap map[uint32][]string

func (d *DiffKeysMap) GetTotalCount() int {
	if d == nil {
		return 0
	}
	var count int
	for _, v := range *d {
		count += len(v)
	}
	return count
}

func (d *DiffKeysMap) ToFetchEntries(mappings map[uint32][]uint32) (MutationDiffFetchList, MutationDiffFetchListIdx) {
	var fetchList MutationDiffFetchList
	index := make(MutationDiffFetchListIdx)

	if d == nil {
		return fetchList, index
	}

	for srcColId, keys := range *d {
		for _, oneKey := range keys {
			tgtList, ok := mappings[srcColId]
			if !ok {
				// Shouldn't happen
				continue
			}
			entry := &MutationDifferFetchEntry{
				SrcColId:  srcColId,
				TgtColIds: tgtList,
				Key:       oneKey,
			}
			fetchList = append(fetchList, entry)
			index.AddEntry(entry)
		}
	}

	return fetchList, index
}

type MutationDifferFetchEntry struct {
	SrcColId  uint32
	TgtColIds []uint32
	Key       string
}

func (m *MutationDifferFetchEntry) Clone() *MutationDifferFetchEntry {
	copyTgt := make([]uint32, len(m.TgtColIds))
	for i, id := range m.TgtColIds {
		copyTgt[i] = id
	}
	return &MutationDifferFetchEntry{
		SrcColId:  m.SrcColId,
		TgtColIds: copyTgt,
		Key:       m.Key,
	}
}

// Typically used from the target's point of view
// If a target entry doesn't exist in the source
// then flip this around to the source's point of view
func (m *MutationDifferFetchEntry) Reverse() MutationDiffFetchList {
	var reversedList MutationDiffFetchList

	for _, oneSrcColId := range m.TgtColIds {
		srcPovEntry := &MutationDifferFetchEntry{
			SrcColId:  oneSrcColId,
			TgtColIds: []uint32{m.SrcColId},
			Key:       m.Key,
		}
		reversedList = append(reversedList, srcPovEntry)
	}
	return reversedList
}

type MutationDiffFetchList []*MutationDifferFetchEntry

func (m MutationDiffFetchList) Clone() MutationDiffFetchList {
	var cloneList MutationDiffFetchList
	for _, entry := range m {
		cloneList = append(cloneList, entry)
	}
	return cloneList
}

// Indexed by key name with multiple src/tgt colIds entries
type MutationDiffFetchListIdx map[string][]*MutationDifferFetchEntry

func (m MutationDiffFetchListIdx) AddEntry(entry *MutationDifferFetchEntry) {
	if _, keyExists := m[entry.Key]; !keyExists {
		m[entry.Key] = MutationDiffFetchList{entry}
	} else {
		m[entry.Key] = append(m[entry.Key], entry)
	}
}

type DifferDriver struct {
	sourceFileDir     string
	targetFileDir     string
	diffFileDir       string
	diffKeysFileName  string
	numberOfWorkers   int
	numberOfBins      int
	waitGroup         *sync.WaitGroup
	srcDiffKeys       DiffKeysMap
	tgtDiffKeys       DiffKeysMap
	stateLock         *sync.RWMutex
	fileDescPool      *fdp.FdPool
	vbCompleted       uint32
	finChan           chan bool
	stopOnce          sync.Once
	collectionMapping map[uint32][]uint32
}

func NewDifferDriver(sourceFileDir, targetFileDir, diffFileDir, diffKeysFileName string, numberOfWorkers, numberOfBins, numberOfFds int, collectionMapping map[uint32][]uint32) *DifferDriver {
	var fdPool *fdp.FdPool
	if numberOfFds > 0 {
		fdPool = fdp.NewFileDescriptorPool(numberOfFds)
	}

	return &DifferDriver{
		sourceFileDir:     sourceFileDir,
		targetFileDir:     targetFileDir,
		diffFileDir:       diffFileDir,
		diffKeysFileName:  diffKeysFileName,
		numberOfWorkers:   numberOfWorkers,
		numberOfBins:      numberOfBins,
		waitGroup:         &sync.WaitGroup{},
		stateLock:         &sync.RWMutex{},
		fileDescPool:      fdPool,
		finChan:           make(chan bool),
		collectionMapping: collectionMapping,
		srcDiffKeys:       make(DiffKeysMap),
		tgtDiffKeys:       make(DiffKeysMap),
	}
}

func (dr *DifferDriver) Run() error {
	loadDistribution := utils.BalanceLoad(dr.numberOfWorkers, base.NumberOfVbuckets)

	go dr.reportStatus()

	for i := 0; i < dr.numberOfWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = uint16(j)
		}

		dr.waitGroup.Add(1)
		differHandler := NewDifferHandler(dr, i, dr.sourceFileDir, dr.targetFileDir, vbList, dr.numberOfBins, dr.waitGroup, dr.fileDescPool, dr.collectionMapping)
		go differHandler.run()
	}

	dr.waitGroup.Wait()

	dr.Stop()

	return nil
}

func (dr *DifferDriver) Stop() {
	dr.stopOnce.Do(func() { dr.cleanup() })
}

func (dr *DifferDriver) cleanup() {
	close(dr.finChan)
	err := dr.writeDiffKeys()
	if err != nil {
		fmt.Printf("Error writing srcDiff fetchList. err=%v\n", err)
	}
}

func (dr *DifferDriver) reportStatus() {
	ticker := time.NewTicker(time.Duration(base.StatsReportInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			vbCompleted := atomic.LoadUint32(&dr.vbCompleted)
			fmt.Printf("%v File differ processed %v vbuckets\n", time.Now(), vbCompleted)
			if vbCompleted == base.NumberOfVbuckets {
				return
			}
		case <-dr.finChan:
			return
		}
	}
}

func (dr *DifferDriver) addSrcDiffKeys(diffKeys map[uint32][]string) {
	dr.stateLock.Lock()
	defer dr.stateLock.Unlock()
	for srcColId, keys := range diffKeys {
		dr.srcDiffKeys[srcColId] = append(dr.srcDiffKeys[srcColId], keys...)
	}
}

func (dr *DifferDriver) addTgtDiffKeys(diffKeys map[uint32][]string) {
	dr.stateLock.Lock()
	defer dr.stateLock.Unlock()
	for tgtColId, keys := range diffKeys {
		dr.tgtDiffKeys[tgtColId] = append(dr.tgtDiffKeys[tgtColId], keys...)
	}
}

func (dr *DifferDriver) writeDiffKeys() error {
	dr.stateLock.RLock()
	defer dr.stateLock.RUnlock()

	var writeWaitGrp sync.WaitGroup
	writeWaitGrp.Add(2)

	var srcErr error
	var tgtErr error
	go func() {
		srcErr = dr.writeSrcDiffKeys(true, &writeWaitGrp)
	}()

	go func() {
		tgtErr = dr.writeSrcDiffKeys(false, &writeWaitGrp)
	}()

	writeWaitGrp.Wait()

	if srcErr == nil && tgtErr == nil {
		return nil
	} else {
		return fmt.Errorf("writeDiffKeysSrc: %v writeDiffKeysTgt: %v", srcErr, tgtErr)
	}
}

func (dr *DifferDriver) writeSrcDiffKeys(isSrc bool, waitGrp *sync.WaitGroup) error {
	defer waitGrp.Done()
	diffKeys := dr.srcDiffKeys
	if !isSrc {
		diffKeys = dr.tgtDiffKeys
	}

	diffKeysBytes, err := json.Marshal(diffKeys)
	if err != nil {
		return err
	}

	diffKeysFileName := utils.DiffKeysFileName(isSrc, dr.diffFileDir, dr.diffKeysFileName)
	diffKeysFile, err := os.OpenFile(diffKeysFileName, os.O_RDWR|os.O_CREATE, base.FileModeReadWrite)
	if err != nil {
		return err
	}
	_, err = diffKeysFile.Write(diffKeysBytes)
	diffKeysFile.Close()
	return nil
}

type DifferHandler struct {
	driver            *DifferDriver
	index             int
	sourceFileDir     string
	targetFileDir     string
	vbList            []uint16
	diffDetailsFile   *os.File
	numberOfBins      int
	waitGroup         *sync.WaitGroup
	fileDescPool      *fdp.FdPool
	collectionMapping map[uint32][]uint32
}

func NewDifferHandler(driver *DifferDriver, index int, sourceFileDir, targetFileDir string, vbList []uint16, numberOfBins int, waitGroup *sync.WaitGroup, fdPool *fdp.FdPool, collectionMapping map[uint32][]uint32) *DifferHandler {
	return &DifferHandler{
		driver:            driver,
		index:             index,
		sourceFileDir:     sourceFileDir,
		targetFileDir:     targetFileDir,
		vbList:            vbList,
		numberOfBins:      numberOfBins,
		waitGroup:         waitGroup,
		fileDescPool:      fdPool,
		collectionMapping: collectionMapping,
	}
}

func (dh *DifferHandler) run() error {
	//fmt.Printf("DiffHandler %v starting\n", dh.index)
	//defer fmt.Printf("DiffHandler %v stopping\n", dh.index)
	defer dh.waitGroup.Done()

	err := dh.initialize()
	if err != nil {
		fmt.Printf("%v srcDiff handler failed to initialize. err=%v\n", dh.index, err)
		return err
	}

	var vbno uint16
	for _, vbno = range dh.vbList {
		for bucketIndex := 0; bucketIndex < dh.numberOfBins; bucketIndex++ {
			sourceFileName := utils.GetFileName(dh.sourceFileDir, vbno, bucketIndex)
			targetFileName := utils.GetFileName(dh.targetFileDir, vbno, bucketIndex)

			filesDiffer, err := NewFilesDifferWithFDPool(sourceFileName, targetFileName, dh.fileDescPool, dh.collectionMapping)
			if err != nil {
				// Most likely FD overrun, program should exit. Print a msg just in case
				fmt.Printf("Creating file differ for files %v and %v resulted in error: %v\n",
					sourceFileName, targetFileName, err)
				return err
			}

			srcDiffMap, tgtDiffMap, diffBytes, err := filesDiffer.Diff()
			if err != nil {
				fmt.Printf("error getting srcDiff from file differ. err=%v\n", err)
				continue
			}
			if len(srcDiffMap) > 0 || len(tgtDiffMap) > 0 {
				if len(srcDiffMap) > 0 {
					dh.driver.addSrcDiffKeys(srcDiffMap)
				}
				if len(tgtDiffMap) > 0 {
					dh.driver.addTgtDiffKeys(tgtDiffMap)
				}
				dh.writeDiffBytes(diffBytes)
			}
		}
		atomic.AddUint32(&dh.driver.vbCompleted, 1)
	}

	dh.cleanup()

	return nil
}

func (dh *DifferHandler) initialize() error {
	diffDetailsFileName := dh.driver.diffFileDir + base.FileDirDelimiter + base.DiffDetailsFileName + base.FileNameDelimiter + fmt.Sprintf("%v", dh.index)
	diffDetailsFile, err := os.OpenFile(diffDetailsFileName, os.O_RDWR|os.O_CREATE, base.FileModeReadWrite)
	if err != nil {
		return err
	}
	dh.diffDetailsFile = diffDetailsFile
	return nil
}

func (dh *DifferHandler) writeDiffBytes(diffBytes []byte) error {
	_, err := dh.diffDetailsFile.Write(diffBytes)
	if err != nil {
		fmt.Printf("Diff handler %v error writing srcDiff details. err=%v\n", dh.index, err)
	}
	return err
}

func (dh *DifferHandler) cleanup() {
	dh.diffDetailsFile.Close()
}
