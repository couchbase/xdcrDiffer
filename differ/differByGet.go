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
	"github.com/couchbase/gocb"
	"github.com/nelio2k/xdcrDiffer/utils"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const NumberOfDiffWorkers = 1
const KeyNotFoundErrMsg = "key not found"

type Differ struct {
	sourceUrl        string
	sourceBucketName string
	sourceUserName   string
	sourcePassword   string
	targetUrl        string
	targetBucketName string
	targetUserName   string
	targetPassword   string

	sourceBucket *gocb.Bucket
	targetBucket *gocb.Bucket

	// keys to do diff on
	keys [][]byte
}

type DifferWorker struct {
	// keys to do diff on
	keys              [][]byte
	sourceBucket      *gocb.Bucket
	targetBucket      *gocb.Bucket
	waitGroup         *sync.WaitGroup
	sourceResultCount uint32
	targetResultCount uint32
}

func NewDiffer(sourceUrl string,
	sourceBucketName string,
	sourceUserName string,
	sourcePassword string,
	targetUrl string,
	targetBucketName string,
	targetUserName string,
	targetPassword string,
	keys [][]byte) *Differ {
	return &Differ{
		sourceUrl:        sourceUrl,
		sourceBucketName: sourceBucketName,
		sourceUserName:   sourceUserName,
		sourcePassword:   sourcePassword,
		targetUrl:        targetUrl,
		targetBucketName: targetBucketName,
		targetUserName:   targetUserName,
		targetPassword:   targetPassword,
		keys:             keys,
	}
}

func (d *Differ) Diff() error {
	err := d.initialize()
	if err != nil {
		return err
	}

	loadDistribution := utils.BalanceLoad(NumberOfDiffWorkers, len(d.keys))
	waitGroup := &sync.WaitGroup{}
	for i := 0; i < NumberOfDiffWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		waitGroup.Add(1)
		diffWorker := NewDifferWorker(d.sourceBucket, d.targetBucket, d.keys[lowIndex:highIndex], waitGroup)
		sourceResults, targetResults := diffWorker.getResults()
		diffWorker.diff(sourceResults, targetResults)
	}

	waitGroup.Wait()

	return nil
}

func NewDifferWorker(sourceBucket, targetBucket *gocb.Bucket, keys [][]byte, waitGroup *sync.WaitGroup) *DifferWorker {
	return &DifferWorker{
		sourceBucket: sourceBucket,
		targetBucket: targetBucket,
		keys:         keys,
		waitGroup:    waitGroup,
	}
}

func (dw *DifferWorker) getResults() (map[string]*GetResult, map[string]*GetResult) {
	defer dw.waitGroup.Done()

	sourceResults := make(map[string]*GetResult)
	targetResults := make(map[string]*GetResult)
	for _, key := range dw.keys {
		sourceResults[string(key)] = &GetResult{}
		targetResults[string(key)] = &GetResult{}
	}

	for _, key := range dw.keys {
		dw.get(key, sourceResults, true /*isSource*/)
		dw.get(key, targetResults, false /*isSource*/)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			if atomic.LoadUint32(&dw.sourceResultCount) == uint32(len(dw.keys)) &&
				atomic.LoadUint32(&dw.targetResultCount) == uint32(len(dw.keys)) {
				goto done
			}
		case <-timer.C:
			fmt.Printf("get timed out\n")
			goto done
		}
	}
done:
	return sourceResults, targetResults
}

func (dw *DifferWorker) diff(sourceResults, targetResults map[string]*GetResult) {
	for key, sourceResult := range sourceResults {
		targetResult := targetResults[key]
		if isKeyNotFoundError(sourceResult.Error) && !isKeyNotFoundError(targetResult.Error) {
			fmt.Printf("%v exists on target and not on source\n", key)
			continue
		}
		if !isKeyNotFoundError(sourceResult.Error) && isKeyNotFoundError(targetResult.Error) {
			fmt.Printf("%v exists on source and not on target\n", key)
			continue
		}
		if !areGetResultsTheSame(sourceResult.Result, targetResult.Result) {
			fmt.Printf("Diff exists for %v. Source:%v, target:%v\n", key, sourceResult, targetResult)
		}

	}
}

func isKeyNotFoundError(err error) bool {
	return err != nil && err.Error() == KeyNotFoundErrMsg
}

func areGetResultsTheSame(result1, result2 *gocbcore.GetResult) bool {
	return reflect.DeepEqual(result1.Value, result2.Value) && result1.Flags == result2.Flags &&
		result1.Datatype == result2.Datatype && result1.Cas == result2.Cas
}

func (dw *DifferWorker) get(key []byte, resultsMap map[string]*GetResult, isSource bool) {
	getCallbackFunc := func(result *gocbcore.GetResult, err error) {
		resultsMap[string(key)].Result = result
		resultsMap[string(key)].Error = err
		if isSource {
			atomic.AddUint32(&dw.sourceResultCount, 1)
		} else {
			atomic.AddUint32(&dw.targetResultCount, 1)
		}
	}

	if isSource {
		dw.sourceBucket.IoRouter().GetEx(gocbcore.GetOptions{Key: key}, getCallbackFunc)
	} else {
		dw.targetBucket.IoRouter().GetEx(gocbcore.GetOptions{Key: key}, getCallbackFunc)
	}
}

type GetResult struct {
	Result *gocbcore.GetResult
	Error  error
}

func (r *GetResult) String() string {
	if r.Result == nil {
		return fmt.Sprintf("nil result")
	}
	return fmt.Sprintf("Cas=%v Datatype=%v Flags=%v Value=%v", r.Result.Cas, r.Result.Datatype, r.Result.Flags, r.Result.Value)
}

func (d *Differ) initialize() error {
	var err error
	d.sourceBucket, err = d.openBucket(d.sourceUrl, d.sourceBucketName, d.sourceUserName, d.sourcePassword)
	if err != nil {
		return err
	}
	d.targetBucket, err = d.openBucket(d.targetUrl, d.targetBucketName, d.targetUserName, d.targetPassword)
	if err != nil {
		return err
	}
	return nil
}

func (d *Differ) openBucket(url, bucketName, username, password string) (*gocb.Bucket, error) {
	cluster, err := gocb.Connect(url)
	if err != nil {
		fmt.Printf("Error connecting to cluster %v. err=%v\n", url, err)
		return nil, err
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	})

	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}

	return cluster.OpenBucket(bucketName, "")
}
