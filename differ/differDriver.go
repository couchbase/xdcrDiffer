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
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"xdcrDiffer/base"
	fdp "xdcrDiffer/fileDescriptorPool"
	"xdcrDiffer/utils"

	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
)

// For each ColID, the keys that have diffs
type DiffKeysMap map[uint32][]string
type MigrationHintMap map[string][]uint32

type pruningWindow struct {
	duration time.Duration
	lock     sync.RWMutex
	isSource bool
}

var sourcePruningWindow *pruningWindow = &pruningWindow{isSource: true}

var targetPruningWindow *pruningWindow = &pruningWindow{}

func (p *pruningWindow) set(svc service_def.BucketTopologySvc, spec *metadata.ReplicationSpecification) error {
	subscriberId := "DiffTool"
	var pruningWindow int
	if p.isSource {
		notificationCh, err := svc.SubscribeToLocalBucketFeed(spec, subscriberId)
		if err != nil {
			fmt.Printf("Failed to fetch LocalBucketFeed. err=%v\n", err)
			return err
		}
		defer svc.UnSubscribeLocalBucketFeed(spec, subscriberId)
		latestNotification := <-notificationCh
		defer latestNotification.Recycle()
		pruningWindow = latestNotification.GetVersionPruningWindowHrs()
	} else {
		notificationCh, err := svc.SubscribeToRemoteBucketFeed(spec, subscriberId)
		if err != nil {
			fmt.Printf("Failed to fetch RemoteBucketFeed. err=%v\n", err)
			return err
		}
		defer svc.UnSubscribeRemoteBucketFeed(spec, subscriberId)
		latestNotification := <-notificationCh
		defer latestNotification.Recycle()
		pruningWindow = latestNotification.GetVersionPruningWindowHrs()
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	p.duration = time.Duration(uint32(pruningWindow)) * time.Hour
	return nil
}

func (p *pruningWindow) get() time.Duration {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.duration
}

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

// Translate into a list of mutations that needs fetching
// Returns alongside an index keyed by the document ID
// For each docID of the index, there can be multiple collection IDs that owns this key
// For migration source's default collection namespace, each entry will depend on the migrationHintMap to get the necessary info
func (d *DiffKeysMap) ToFetchEntries(mappings map[uint32][]uint32, migrationHintMap MigrationHintMap) (MutationDiffFetchList, MutationDiffFetchListIdx) {
	var fetchList MutationDiffFetchList
	index := make(MutationDiffFetchListIdx)

	if d == nil {
		return fetchList, index
	}

	migrationMode := len(migrationHintMap) > 0

	for srcColId, keys := range *d {
		for _, oneKey := range keys {
			var tgtList []uint32
			var ok bool

			if migrationMode {
				tgtList = migrationHintMap[oneKey]
			} else {
				tgtList, ok = mappings[srcColId]
				if !ok {
					if len(migrationHintMap) == 0 {
						// Shouldn't happen
						continue
					}
				}
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

func (d *DiffKeysMap) Merge(other DiffKeysMap) {
	if d == nil || other == nil {
		return
	}
	for colId, keys := range other {
		if _, exists := (*d)[colId]; !exists {
			(*d)[colId] = []string{}
		}
		sort.Strings((*d)[colId])
		for _, key := range keys {
			i := sort.Search(len((*d)[colId]), func(j int) bool {
				return key < ((*d)[colId])[j]
			})
			if i < len((*d)[colId]) && (*d)[colId][i] == key {
				// Found already. No need to add
			} else {
				(*d)[colId] = append((*d)[colId], key)
			}
		}
	}
}

// An doc that needs to be fetched multiple times, once for each collectionID
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
	sourceBucketUUID  string
	targetBucketUUID  string
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
	colFilterStrings  []string
	colFilterTgtIds   []uint32
	SourceItemCount   int64
	TargetItemCount   int64
	SrcVbItemCntMap   map[uint16]int
	TgtVbItemCntMap   map[uint16]int
	MapLock           *sync.RWMutex
	srcMigrationHint  MigrationHintMap
	DuplicatedHint    DuplicatedHintMap
	bucketTopologySvc service_def.BucketTopologySvc
	specifiedSpec     *metadata.ReplicationSpecification
	logger            *xdcrLog.CommonLogger
}

func NewDifferDriver(sourceFileDir, targetFileDir, diffFileDir, diffKeysFileName, sourceBucketUUID, targetBucketUUID string, numberOfWorkers, numberOfBins, numberOfFds int, collectionMapping map[uint32][]uint32, colFilterStrings []string, colFilterTgtIds []uint32, bucketTopologySvc service_def.BucketTopologySvc, specifiedSpec *metadata.ReplicationSpecification, logger *xdcrLog.CommonLogger) *DifferDriver {
	var fdPool *fdp.FdPool
	if numberOfFds > 0 {
		fdPool = fdp.NewFileDescriptorPool(numberOfFds)
	}

	return &DifferDriver{
		sourceFileDir:     sourceFileDir,
		targetFileDir:     targetFileDir,
		diffFileDir:       diffFileDir,
		diffKeysFileName:  diffKeysFileName,
		sourceBucketUUID:  sourceBucketUUID,
		targetBucketUUID:  targetBucketUUID,
		numberOfWorkers:   numberOfWorkers,
		numberOfBins:      numberOfBins,
		waitGroup:         &sync.WaitGroup{},
		stateLock:         &sync.RWMutex{},
		fileDescPool:      fdPool,
		finChan:           make(chan bool),
		collectionMapping: collectionMapping,
		srcDiffKeys:       make(DiffKeysMap),
		tgtDiffKeys:       make(DiffKeysMap),
		colFilterStrings:  colFilterStrings,
		colFilterTgtIds:   colFilterTgtIds,
		srcMigrationHint:  MigrationHintMap{},
		SrcVbItemCntMap:   make(map[uint16]int),
		TgtVbItemCntMap:   make(map[uint16]int),
		MapLock:           &sync.RWMutex{},
		DuplicatedHint:    DuplicatedHintMap{},
		bucketTopologySvc: bucketTopologySvc,
		specifiedSpec:     specifiedSpec,
		logger:            logger,
	}
}

func (dr *DifferDriver) Run() error {
	loadDistribution := utils.BalanceLoad(dr.numberOfWorkers, base.NumberOfVbuckets)
	err := sourcePruningWindow.set(dr.bucketTopologySvc, dr.specifiedSpec)
	if err != nil {
		return err
	}
	err1 := targetPruningWindow.set(dr.bucketTopologySvc, dr.specifiedSpec)
	if err1 != nil {
		return err1
	}
	go dr.reportStatus()

	var differHandlers []*DifferHandler

	for i := 0; i < dr.numberOfWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = uint16(j)
		}

		dr.waitGroup.Add(1)
		differHandler := NewDifferHandler(dr, i, dr.sourceFileDir, dr.targetFileDir, vbList, dr.numberOfBins, dr.waitGroup, dr.fileDescPool, dr.collectionMapping, dr.colFilterStrings, dr.colFilterTgtIds)
		differHandlers = append(differHandlers, differHandler)
		go differHandler.run()
	}
	dr.waitGroup.Wait()

	// Each handler contains a different set of VBs, and DuplicatedHint is one entity that
	// contains all documents (from all VBs)
	// Thus, merge is needed to ensure a complete view of all documents across all VBs
	for _, handler := range differHandlers {
		dr.DuplicatedHint.Merge(handler.duplicatedHintMap)
	}

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

func (dr *DifferDriver) addSrcDiffKeys(diffKeys map[uint32][]string, migrationHints map[string][]uint32) {
	dr.stateLock.Lock()
	defer dr.stateLock.Unlock()
	for srcColId, keys := range diffKeys {
		dr.srcDiffKeys[srcColId] = append(dr.srcDiffKeys[srcColId], keys...)
		if srcColId == 0 && len(migrationHints) > 0 {
			for _, key := range keys {
				dr.srcMigrationHint[key] = migrationHints[key]
			}
		}
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

	if isSrc && len(dr.colFilterStrings) > 0 {
		migrationHintFile := fmt.Sprintf("%v_%v", diffKeysFileName, base.DiffKeysSrcMigrationHintSuffix)
		data, err := json.Marshal(dr.srcMigrationHint)
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(migrationHintFile, data, 0644)
		if err != nil {
			return err
		}
	}
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
	colFilterStrings  []string
	colFilterTgtIds   []uint32

	duplicatedHintMap DuplicatedHintMap
}

func NewDifferHandler(driver *DifferDriver, index int, sourceFileDir, targetFileDir string, vbList []uint16, numberOfBins int, waitGroup *sync.WaitGroup, fdPool *fdp.FdPool, collectionMapping map[uint32][]uint32, colFilterStrings []string, colFilterTgtIds []uint32) *DifferHandler {
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
		colFilterStrings:  colFilterStrings,
		colFilterTgtIds:   colFilterTgtIds,
		duplicatedHintMap: DuplicatedHintMap{},
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
		srcVbItemCnt := 0
		tgtVbItemCnt := 0
		for bucketIndex := 0; bucketIndex < dh.numberOfBins; bucketIndex++ {
			sourceFileName := utils.GetFileName(dh.sourceFileDir, vbno, bucketIndex)
			targetFileName := utils.GetFileName(dh.targetFileDir, vbno, bucketIndex)

			filesDiffer, err := NewFilesDifferWithFDPool(sourceFileName, targetFileName, dh.fileDescPool, dh.collectionMapping, dh.colFilterStrings, dh.colFilterTgtIds, dh.driver.logger)
			filesDiffer.file1.bucketUUID = dh.driver.sourceBucketUUID
			filesDiffer.file2.bucketUUID = dh.driver.targetBucketUUID
			if err != nil {
				// Most likely FD overrun, program should exit. Print a msg just in case
				dh.driver.logger.Errorf("Creating file differ for files %v and %v resulted in error: %v\n",
					sourceFileName, targetFileName, err)
				return err
			}
			srcDiffMap, tgtDiffMap, migrationHints, diffBytes, err := filesDiffer.Diff()
			if err != nil {
				fmt.Printf("error getting srcDiff from file differ. err=%v\n", err)
				continue
			}
			if len(srcDiffMap) > 0 || len(tgtDiffMap) > 0 {
				if len(srcDiffMap) > 0 {
					dh.driver.addSrcDiffKeys(srcDiffMap, migrationHints)
				}
				if len(tgtDiffMap) > 0 {
					dh.driver.addTgtDiffKeys(tgtDiffMap)
				}
				dh.writeDiffBytes(diffBytes)
			}
			srcVbItemCnt += filesDiffer.file1ItemCount
			tgtVbItemCnt += filesDiffer.file2ItemCount

			dh.duplicatedHintMap.Merge(filesDiffer.duplicatedHintMap)
		}
		atomic.AddInt64(&dh.driver.SourceItemCount, int64(srcVbItemCnt))
		atomic.AddInt64(&dh.driver.TargetItemCount, int64(tgtVbItemCnt))

		dh.driver.MapLock.Lock()
		dh.driver.SrcVbItemCntMap[vbno] = srcVbItemCnt
		dh.driver.TgtVbItemCntMap[vbno] = tgtVbItemCnt
		dh.driver.MapLock.Unlock()
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
