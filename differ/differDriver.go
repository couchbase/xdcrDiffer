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
	"fmt"
	"github.com/nelio2k/xdcrDiffer/base"
	fdp "github.com/nelio2k/xdcrDiffer/fileDescriptorPool"
	"github.com/nelio2k/xdcrDiffer/utils"
	"sync"
)

type DifferDriver struct {
	sourceFileDir   string
	targetFileDir   string
	numberOfWorkers int
	numberOfBuckets int
	waitGroup       *sync.WaitGroup
	diffKeyList     [][]byte
	stateLock       *sync.RWMutex
	fileDescPool    fdp.FdPoolIface
}

func NewDifferDriver(sourceFileDir, targetFileDir string, numberOfWorkers, numberOfBuckets int, numberOfFds int) *DifferDriver {
	var fdPool *fdp.FdPool
	if numberOfFds > 0 {
		fdPool = fdp.NewFileDescriptorPool(numberOfFds)
	}

	return &DifferDriver{
		sourceFileDir:   sourceFileDir,
		targetFileDir:   targetFileDir,
		numberOfWorkers: numberOfWorkers,
		numberOfBuckets: numberOfBuckets,
		waitGroup:       &sync.WaitGroup{},
		diffKeyList:     make([][]byte, 0),
		stateLock:       &sync.RWMutex{},
		fileDescPool:    fdPool,
	}
}

func (dr *DifferDriver) Run() [][]byte {
	loadDistribution := utils.BalanceLoad(dr.numberOfWorkers, base.NumberOfVbuckets)
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

	return dr.getDiffKeys()
}

func (dr *DifferDriver) addDiffKeys(diffKeys [][]byte) {
	dr.stateLock.Lock()
	defer dr.stateLock.Unlock()
	dr.diffKeyList = append(dr.diffKeyList, diffKeys...)
}

func (dr *DifferDriver) getDiffKeys() [][]byte {
	dr.stateLock.RLock()
	defer dr.stateLock.RUnlock()
	return dr.diffKeyList
}

type DifferHandler struct {
	driver          *DifferDriver
	index           int
	sourceFileDir   string
	targetFileDir   string
	vbList          []uint16
	numberOfBuckets int
	waitGroup       *sync.WaitGroup
	fileDescPool    fdp.FdPoolIface
}

func NewDifferHandler(driver *DifferDriver, index int, sourceFileDir, targetFileDir string, vbList []uint16, numberOfBuckets int, waitGroup *sync.WaitGroup, fdPool fdp.FdPoolIface) *DifferHandler {
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
	fmt.Printf("DiffHandler %v starting\n", dh.index)
	defer fmt.Printf("DiffHandler %v stopping\n", dh.index)
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
			match, diffKeys := filesDiffer.Diff()
			if !match {
				filesDiffer.PrettyPrintResult()
				err = filesDiffer.OutputToJsonFile(fmt.Sprintf("diffResult_%v_%v.json", sourceFileName, targetFileName))
				if err != nil {
					fmt.Printf("Error outputting JSON result file between %v and %v\n", sourceFileName, targetFileName)
				}
				dh.driver.addDiffKeys(diffKeys)
			}
		}
	}
}
