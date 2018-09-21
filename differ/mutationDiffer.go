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
	"github.com/couchbase/gocb"
	"github.com/nelio2k/xdcrDiffer/base"
	"github.com/nelio2k/xdcrDiffer/utils"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const KeyNotFoundErrMsg = "key not found"

type MutationDiffer struct {
	sourceUrl        string
	sourceBucketName string
	sourceUserName   string
	sourcePassword   string
	targetUrl        string
	targetBucketName string
	targetUserName   string
	targetPassword   string
	diffFileDir      string
	numberOfWorkers  int
	batchSize        int
	timeout          int

	sourceBucket *gocb.Bucket
	targetBucket *gocb.Bucket

	missingFromSource map[string]*gocbcore.GetMetaResult
	missingFromTarget map[string]*gocbcore.GetMetaResult
	diff              map[string][]*gocbcore.GetMetaResult
	stateLock         *sync.RWMutex
}

func NewMutationDiffer(sourceUrl string,
	sourceBucketName string,
	sourceUserName string,
	sourcePassword string,
	targetUrl string,
	targetBucketName string,
	targetUserName string,
	targetPassword string,
	diffFileDir string,
	numberOfWorkers int,
	batchSize int,
	timeout int) *MutationDiffer {
	return &MutationDiffer{
		sourceUrl:         sourceUrl,
		sourceBucketName:  sourceBucketName,
		sourceUserName:    sourceUserName,
		sourcePassword:    sourcePassword,
		targetUrl:         targetUrl,
		targetBucketName:  targetBucketName,
		targetUserName:    targetUserName,
		targetPassword:    targetPassword,
		diffFileDir:       diffFileDir,
		numberOfWorkers:   numberOfWorkers,
		batchSize:         batchSize,
		timeout:           timeout,
		missingFromSource: make(map[string]*gocbcore.GetMetaResult),
		missingFromTarget: make(map[string]*gocbcore.GetMetaResult),
		diff:              make(map[string][]*gocbcore.GetMetaResult),
		stateLock:         &sync.RWMutex{},
	}
}

func (d *MutationDiffer) Run() error {
	diffKeys, err := d.loadDiffKeys()
	if err != nil {
		return err
	}

	err = d.initialize()
	if err != nil {
		return err
	}

	loadDistribution := utils.BalanceLoad(d.numberOfWorkers, len(diffKeys))
	waitGroup := &sync.WaitGroup{}
	for i := 0; i < d.numberOfWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		if lowIndex == highIndex {
			// skip workers with 0 load
			continue
		}
		diffWorker := NewDifferWorker(d, d.sourceBucket, d.targetBucket, diffKeys[lowIndex:highIndex], waitGroup)
		waitGroup.Add(1)
		go diffWorker.run()
	}

	waitGroup.Wait()

	d.writeDiff()

	return nil
}

func (d *MutationDiffer) writeDiff() error {
	diffBytes, err := d.getDiffBytes()
	if err != nil {
		return err
	}

	return d.writeDiffBytesToFile(diffBytes)
}

func (d *MutationDiffer) getDiffBytes() ([]byte, error) {
	outputMap := map[string]interface{}{
		"Mismatch":          d.diff,
		"MissingFromSource": d.missingFromSource,
		"MissingFromTarget": d.missingFromTarget,
	}

	return json.Marshal(outputMap)
}

func (d *MutationDiffer) writeDiffBytesToFile(diffBytes []byte) error {
	diffFileName := d.diffFileDir + base.FileDirDelimiter + base.MutationDiffFileName
	diffFile, err := os.OpenFile(diffFileName, os.O_RDWR|os.O_CREATE, base.FileModeReadWrite)
	if err != nil {
		return err
	}

	defer diffFile.Close()

	_, err = diffFile.Write(diffBytes)
	return err

}

func (d *MutationDiffer) loadDiffKeys() ([]string, error) {
	diffKeysFileName := d.diffFileDir + base.FileDirDelimiter + base.DiffKeysFileName
	diffKeysBytes, err := ioutil.ReadFile(diffKeysFileName)
	if err != nil {
		return nil, err
	}

	diffKeys := make([]string, 0)
	err = json.Unmarshal(diffKeysBytes, &diffKeys)
	if err != nil {
		return nil, err
	}
	return diffKeys, nil
}

func (d *MutationDiffer) addDiff(missingFromSource map[string]*gocbcore.GetMetaResult,
	missingFromTarget map[string]*gocbcore.GetMetaResult,
	diff map[string][]*gocbcore.GetMetaResult) {
	d.stateLock.Lock()
	defer d.stateLock.Unlock()

	for key, result := range missingFromSource {
		d.missingFromSource[key] = result
	}
	for key, result := range missingFromTarget {
		d.missingFromTarget[key] = result
	}
	for key, results := range diff {
		d.diff[key] = results
	}
}

type DifferWorker struct {
	differ *MutationDiffer
	// keys to do diff on
	keys          []string
	sourceBucket  *gocb.Bucket
	targetBucket  *gocb.Bucket
	waitGroup     *sync.WaitGroup
	sourceResults map[string]*GetResult
	targetResults map[string]*GetResult
	resultsLock   sync.RWMutex
}

func NewDifferWorker(differ *MutationDiffer, sourceBucket, targetBucket *gocb.Bucket, keys []string, waitGroup *sync.WaitGroup) *DifferWorker {
	return &DifferWorker{
		differ:        differ,
		sourceBucket:  sourceBucket,
		targetBucket:  targetBucket,
		keys:          keys,
		waitGroup:     waitGroup,
		sourceResults: make(map[string]*GetResult),
		targetResults: make(map[string]*GetResult),
	}
}

func (dw *DifferWorker) run() {
	defer dw.waitGroup.Done()
	dw.getResults()
	dw.diff()
}

func (dw *DifferWorker) getResults() {
	index := 0
	for {
		if index >= len(dw.keys) {
			break
		}

		if index+dw.differ.batchSize < len(dw.keys) {
			dw.sendBatch(index, index+dw.differ.batchSize)
			index += dw.differ.batchSize
			continue
		}

		dw.sendBatch(index, len(dw.keys))
		break
	}

}

func (dw *DifferWorker) sendBatch(startIndex, endIndex int) {
	batch := NewBatch(dw, startIndex, endIndex)
	batch.send()
	dw.mergeResults(batch)
}

// merge results obtained by batch into dw
// no need to lock results in dw since it is never accessed concurrently
// need to lock results in batch since it could still be updated when mergeResults is called
func (dw *DifferWorker) mergeResults(b *batch) {
	for key, result := range b.sourceResults {
		dw.sourceResults[key] = result.Clone()
	}
	for key, result := range b.targetResults {
		dw.targetResults[key] = result.Clone()
	}

}

func (dw *DifferWorker) diff() {
	missingFromSource := make(map[string]*gocbcore.GetMetaResult)
	missingFromTarget := make(map[string]*gocbcore.GetMetaResult)
	diff := make(map[string][]*gocbcore.GetMetaResult)

	for key, sourceResult := range dw.sourceResults {
		if sourceResult.Key == "" {
			fmt.Printf("Skipping diff on %v since we did not get results from source\n", key)
			continue
		}

		targetResult := dw.targetResults[key]
		if targetResult.Key == "" {
			fmt.Printf("Skipping diff on %v since we did not get results from target\n", key)
			continue
		}
		if isKeyNotFoundError(sourceResult.Error) && !isKeyNotFoundError(targetResult.Error) {
			missingFromSource[key] = targetResult.Result
			continue
		}
		if !isKeyNotFoundError(sourceResult.Error) && isKeyNotFoundError(targetResult.Error) {
			missingFromTarget[key] = sourceResult.Result
			continue
		}
		if !areGetMetaResultsTheSame(sourceResult.Result, targetResult.Result) {
			diff[key] = []*gocbcore.GetMetaResult{sourceResult.Result, targetResult.Result}
		}
	}

	dw.differ.addDiff(missingFromSource, missingFromTarget, diff)
}

type batch struct {
	dw                *DifferWorker
	keys              []string
	waitGroup         sync.WaitGroup
	sourceResultCount uint32
	targetResultCount uint32
	sourceResults     map[string]*GetResult
	targetResults     map[string]*GetResult
	resultsLock       sync.RWMutex
}

func NewBatch(dw *DifferWorker, startIndex, endIndex int) *batch {
	b := &batch{
		dw:            dw,
		keys:          dw.keys[startIndex:endIndex],
		sourceResults: make(map[string]*GetResult),
		targetResults: make(map[string]*GetResult),
	}

	// initialize all entries in results map
	// update to *GetResult in map will not be treated as concurrent update to map itself
	for _, key := range b.keys {
		b.sourceResults[key] = &GetResult{}
		b.targetResults[key] = &GetResult{}
	}

	return b
}

func (b *batch) send() {
	b.waitGroup.Add(2)
	for _, key := range b.keys {
		b.get(key, true /*isSource*/)
		b.get(key, false /*isSource*/)
	}

	doneChan := make(chan bool, 1)
	go utils.WaitForWaitGroup(&b.waitGroup, doneChan)

	start := time.Now()

	timer := time.NewTimer(time.Duration(b.dw.differ.timeout) * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-doneChan:
			fmt.Printf("batch completed after %v\n", time.Since(start))
			goto done
		case <-timer.C:
			fmt.Printf("batch timed out\n")
			goto done
		}
	}
done:
}

func (b *batch) get(key string, isSource bool) {
	getCallbackFunc := func(result *gocbcore.GetMetaResult, err error) {
		var resultsMap map[string]*GetResult
		var newCount uint32
		if isSource {
			resultsMap = b.sourceResults
			newCount = atomic.AddUint32(&b.sourceResultCount, 1)
		} else {
			resultsMap = b.targetResults
			newCount = atomic.AddUint32(&b.targetResultCount, 1)
		}
		resultInMap := resultsMap[key]
		resultInMap.Lock.Lock()
		resultInMap.Key = string(key)
		resultInMap.Result = result
		resultInMap.Error = err
		resultInMap.Lock.Unlock()

		if newCount == uint32(len(b.keys)) {
			b.waitGroup.Done()
		}
	}

	if isSource {
		b.dw.sourceBucket.IoRouter().GetMetaEx(gocbcore.GetMetaOptions{Key: []byte(key)}, getCallbackFunc)
	} else {
		b.dw.targetBucket.IoRouter().GetMetaEx(gocbcore.GetMetaOptions{Key: []byte(key)}, getCallbackFunc)
	}
}

func isKeyNotFoundError(err error) bool {
	return err != nil && err.Error() == KeyNotFoundErrMsg
}

func areGetMetaResultsTheSame(result1, result2 *gocbcore.GetMetaResult) bool {
	if result1 == nil {
		return result2 == nil
	}
	if result2 == nil {
		return false
	}
	return reflect.DeepEqual(result1.Value, result2.Value) && result1.Flags == result2.Flags &&
		result1.Datatype == result2.Datatype && result1.Cas == result2.Cas && result1.Expiry == result2.Expiry &&
		result1.SeqNo == result2.SeqNo && result1.Deleted == result2.Deleted
}

type GetResult struct {
	Key    string
	Result *gocbcore.GetMetaResult
	Error  error
	Lock   sync.RWMutex
}

func (r *GetResult) Clone() *GetResult {
	r.Lock.RLock()
	defer r.Lock.RUnlock()

	// shallow copy is good enough to prevent race
	return &GetResult{
		Key:    r.Key,
		Result: r.Result,
		Error:  r.Error,
	}
}

func (d *MutationDiffer) initialize() error {
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

func (d *MutationDiffer) openBucket(url, bucketName, username, password string) (*gocb.Bucket, error) {
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
