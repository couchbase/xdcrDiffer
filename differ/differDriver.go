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
	"github.com/nelio2k/xdcrDiffer/base"
	fdp "github.com/nelio2k/xdcrDiffer/fileDescriptorPool"
	"github.com/nelio2k/xdcrDiffer/utils"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type DifferDriver struct {
	sourceFileDir    string
	targetFileDir    string
	diffFileDir      string
	diffKeysFileName string
	numberOfWorkers  int
	numberOfBuckets  int
	waitGroup        *sync.WaitGroup
	diffKeys         []string
	stateLock        *sync.RWMutex
	fileDescPool     *fdp.FdPool
	diffDetailsFile  *os.File
	vbCompleted      uint32
	finChan          chan bool
}

func NewDifferDriver(sourceFileDir, targetFileDir, diffFileDir, diffKeysFileName string, numberOfWorkers, numberOfBuckets int, numberOfFds int) *DifferDriver {
	var fdPool *fdp.FdPool
	if numberOfFds > 0 {
		fdPool = fdp.NewFileDescriptorPool(numberOfFds)
	}

	return &DifferDriver{
		sourceFileDir:    sourceFileDir,
		targetFileDir:    targetFileDir,
		diffFileDir:      diffFileDir,
		diffKeysFileName: diffKeysFileName,
		numberOfWorkers:  numberOfWorkers,
		numberOfBuckets:  numberOfBuckets,
		waitGroup:        &sync.WaitGroup{},
		stateLock:        &sync.RWMutex{},
		fileDescPool:     fdPool,
		finChan:          make(chan bool),
	}
}

func (dr *DifferDriver) Run() error {
	err := dr.initialize()
	if err != nil {
		return err
	}

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
		differHandler := NewDifferHandler(dr, i, dr.sourceFileDir, dr.targetFileDir, vbList, dr.numberOfBuckets, dr.waitGroup, dr.fileDescPool)
		go differHandler.run()
	}

	dr.waitGroup.Wait()

	dr.cleanup()

	return nil
}

func (dr *DifferDriver) cleanup() {
	close(dr.finChan)
	err := dr.writeDiffKeys()
	if err != nil {
		fmt.Printf("Error writing diff keys. err=%v\n", err)
	}
	dr.diffDetailsFile.Close()
}

func (dr *DifferDriver) initialize() error {
	diffDetailsFileName := dr.diffFileDir + base.FileDirDelimiter + base.DiffDetailsFileName
	diffDetailsFile, err := os.OpenFile(diffDetailsFileName, os.O_RDWR|os.O_CREATE, base.FileModeReadWrite)
	if err != nil {
		return err
	}
	dr.diffDetailsFile = diffDetailsFile
	return nil
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

func (dr *DifferDriver) addDiff(diffKeys []string, diffBytes []byte) {
	dr.addDiffKeys(diffKeys)

	dr.writeDiffBytes(diffBytes)
}

func (dr *DifferDriver) addDiffKeys(diffKeys []string) {
	dr.stateLock.Lock()
	defer dr.stateLock.Unlock()
	dr.diffKeys = append(dr.diffKeys, diffKeys...)
}

func (dr *DifferDriver) writeDiffKeys() error {
	dr.stateLock.RLock()
	defer dr.stateLock.RUnlock()

	diffKeysBytes, err := json.Marshal(dr.diffKeys)
	if err != nil {
		return err
	}

	diffKeysFileName := dr.diffFileDir + base.FileDirDelimiter + dr.diffKeysFileName
	diffKeysFile, err := os.OpenFile(diffKeysFileName, os.O_RDWR|os.O_CREATE, base.FileModeReadWrite)
	if err != nil {
		return err
	}

	defer diffKeysFile.Close()

	_, err = diffKeysFile.Write(diffKeysBytes)
	return err
}

func (dr *DifferDriver) writeDiffBytes(diffBytes []byte) error {
	_, err := dr.diffDetailsFile.Write(diffBytes)
	return err
}

type DifferHandler struct {
	driver          *DifferDriver
	index           int
	sourceFileDir   string
	targetFileDir   string
	vbList          []uint16
	numberOfBuckets int
	waitGroup       *sync.WaitGroup
	fileDescPool    *fdp.FdPool
}

func NewDifferHandler(driver *DifferDriver, index int, sourceFileDir, targetFileDir string, vbList []uint16, numberOfBuckets int, waitGroup *sync.WaitGroup, fdPool *fdp.FdPool) *DifferHandler {
	return &DifferHandler{
		driver:          driver,
		index:           index,
		sourceFileDir:   sourceFileDir,
		targetFileDir:   targetFileDir,
		vbList:          vbList,
		numberOfBuckets: numberOfBuckets,
		waitGroup:       waitGroup,
		fileDescPool:    fdPool,
	}
}

func (dh *DifferHandler) run() {
	//fmt.Printf("DiffHandler %v starting\n", dh.index)
	//defer fmt.Printf("DiffHandler %v stopping\n", dh.index)
	defer dh.waitGroup.Done()

	var vbno uint16
	for _, vbno = range dh.vbList {
		for bucketIndex := 0; bucketIndex < dh.numberOfBuckets; bucketIndex++ {
			sourceFileName := utils.GetFileName(dh.sourceFileDir, vbno, bucketIndex)
			targetFileName := utils.GetFileName(dh.targetFileDir, vbno, bucketIndex)

			filesDiffer, err := NewFilesDifferWithFDPool(sourceFileName, targetFileName, dh.fileDescPool)
			if err != nil {
				// Most likely FD overrun, program should exit. Print a msg just in case
				fmt.Printf("Creating file differ for files %v and %v resulted in error: %v\n",
					sourceFileName, targetFileName, err)
				return
			}

			diffKeys, diffBytes, err := filesDiffer.Diff()
			if err != nil {
				fmt.Printf("error getting diff from file differ. err=%v\n", err)
				continue
			}
			if len(diffKeys) > 0 {
				dh.driver.addDiff(diffKeys, diffBytes)
			}
		}
		atomic.AddUint32(&dh.driver.vbCompleted, 1)
	}
}
