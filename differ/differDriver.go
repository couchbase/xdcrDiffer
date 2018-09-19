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
	"github.com/nelio2k/xdcrDiffer/utils"
	"sync"
)

type DifferDriver struct {
	sourceFileDir   string
	targetFileDir   string
	numberOfWorkers int
	waitGroup       *sync.WaitGroup
}

func NewDifferDriver(sourceFileDir, targetFileDir string, numberOfWorkers int) *DifferDriver {
	return &DifferDriver{
		sourceFileDir:   sourceFileDir,
		targetFileDir:   targetFileDir,
		numberOfWorkers: numberOfWorkers,
		waitGroup:       &sync.WaitGroup{},
	}
}

func (dr *DifferDriver) Run() {
	loadDistribution := utils.BalanceLoad(dr.numberOfWorkers, base.NumerOfVbuckets)
	for i := 0; i < dr.numberOfWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = uint16(j)
		}

		dr.waitGroup.Add(1)
		differHandler := NewDifferHandler(i, dr.sourceFileDir, dr.targetFileDir, vbList, dr.waitGroup)
		go differHandler.run()
	}

	dr.waitGroup.Wait()
}

type DifferHandler struct {
	index         int
	sourceFileDir string
	targetFileDir string
	vbList        []uint16
	waitGroup     *sync.WaitGroup
}

func NewDifferHandler(index int, sourceFileDir, targetFileDir string, vbList []uint16, waitGroup *sync.WaitGroup) *DifferHandler {
	return &DifferHandler{
		index:         index,
		sourceFileDir: sourceFileDir,
		targetFileDir: targetFileDir,
		vbList:        vbList,
		waitGroup:     waitGroup,
	}
}

func (dh *DifferHandler) run() {
	fmt.Printf("DiffHandler %v starting\n", dh.index)
	defer fmt.Printf("DiffHandler %v stopping\n", dh.index)
	defer dh.waitGroup.Done()

	var vbno uint16
	for _, vbno = range dh.vbList {
		for bucketIndex := 0; bucketIndex < base.NumberOfBucketsPerVbucket; bucketIndex++ {
			sourceFileName := utils.GetFileName(dh.sourceFileDir, vbno, bucketIndex)
			targetFileName := utils.GetFileName(dh.targetFileDir, vbno, bucketIndex)
			filesDiffer := NewFilesDiffer(sourceFileName, targetFileName)
			same := filesDiffer.Diff()
			if !same {
				filesDiffer.PrettyPrintResult()
			}
		}
	}
}
