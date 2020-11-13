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
	"xdcrDiffer/base"
	fdp "xdcrDiffer/fileDescriptorPool"
	"xdcrDiffer/utils"
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
	numberOfBins     int
	waitGroup        *sync.WaitGroup
	diffKeys         []string
	stateLock        *sync.RWMutex
	fileDescPool     *fdp.FdPool
	vbCompleted      uint32
	finChan          chan bool
	stopOnce         sync.Once
}

func NewDifferDriver(sourceFileDir, targetFileDir, diffFileDir, diffKeysFileName string, numberOfWorkers, numberOfBins int, numberOfFds int) *DifferDriver {
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
		numberOfBins:     numberOfBins,
		waitGroup:        &sync.WaitGroup{},
		stateLock:        &sync.RWMutex{},
		fileDescPool:     fdPool,
		finChan:          make(chan bool),
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
		differHandler := NewDifferHandler(dr, i, dr.sourceFileDir, dr.targetFileDir, vbList, dr.numberOfBins, dr.waitGroup, dr.fileDescPool)
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
		fmt.Printf("Error writing diff keys. err=%v\n", err)
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

type DifferHandler struct {
	driver          *DifferDriver
	index           int
	sourceFileDir   string
	targetFileDir   string
	vbList          []uint16
	diffDetailsFile *os.File
	numberOfBins    int
	waitGroup       *sync.WaitGroup
	fileDescPool    *fdp.FdPool
}

func NewDifferHandler(driver *DifferDriver, index int, sourceFileDir, targetFileDir string, vbList []uint16, numberOfBins int, waitGroup *sync.WaitGroup, fdPool *fdp.FdPool) *DifferHandler {
	return &DifferHandler{
		driver:        driver,
		index:         index,
		sourceFileDir: sourceFileDir,
		targetFileDir: targetFileDir,
		vbList:        vbList,
		numberOfBins:  numberOfBins,
		waitGroup:     waitGroup,
		fileDescPool:  fdPool,
	}
}

func (dh *DifferHandler) run() error {
	//fmt.Printf("DiffHandler %v starting\n", dh.index)
	//defer fmt.Printf("DiffHandler %v stopping\n", dh.index)
	defer dh.waitGroup.Done()

	err := dh.initialize()
	if err != nil {
		fmt.Printf("%v diff handler failed to initialize. err=%v\n", dh.index, err)
		return err
	}

	var vbno uint16
	for _, vbno = range dh.vbList {
		for bucketIndex := 0; bucketIndex < dh.numberOfBins; bucketIndex++ {
			sourceFileName := utils.GetFileName(dh.sourceFileDir, vbno, bucketIndex)
			targetFileName := utils.GetFileName(dh.targetFileDir, vbno, bucketIndex)

			filesDiffer, err := NewFilesDifferWithFDPool(sourceFileName, targetFileName, dh.fileDescPool)
			if err != nil {
				// Most likely FD overrun, program should exit. Print a msg just in case
				fmt.Printf("Creating file differ for files %v and %v resulted in error: %v\n",
					sourceFileName, targetFileName, err)
				return err
			}

			diffKeys, diffBytes, err := filesDiffer.Diff()
			if err != nil {
				fmt.Printf("error getting diff from file differ. err=%v\n", err)
				continue
			}
			if len(diffKeys) > 0 {
				//filesDiffer.PrettyPrintResult()
				dh.driver.addDiffKeys(diffKeys)
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
		fmt.Printf("Diff handler %v error writing diff details. err=%v\n", dh.index, err)
	}
	return err
}

func (dh *DifferHandler) cleanup() {
	dh.diffDetailsFile.Close()
}
