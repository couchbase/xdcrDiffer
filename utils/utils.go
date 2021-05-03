// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	mrand "math/rand"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"xdcrDiffer/base"
)

func GetFileName(fileDir string, vbno uint16, bucketIndex int) string {
	var buffer bytes.Buffer
	buffer.WriteString(fileDir)
	buffer.WriteString(base.FileDirDelimiter)
	buffer.WriteString(base.FileNamePrefix)
	buffer.WriteString(base.FileNameDelimiter)
	buffer.WriteString(fmt.Sprintf("%v", vbno))
	buffer.WriteString(base.FileNameDelimiter)
	buffer.WriteString(fmt.Sprintf("%v", bucketIndex))
	return buffer.String()
}

func GetManifestFileName(fileDir string) string {
	var buffer bytes.Buffer
	buffer.WriteString(fileDir)
	buffer.WriteString(base.FileDirDelimiter)
	buffer.WriteString(base.FileNamePrefix)
	buffer.WriteString(base.FileNameDelimiter)
	buffer.WriteString(fmt.Sprintf("%v", base.ManifestFileName))
	return buffer.String()
}

// hash key into a bucket index in range [0, NumberOfBucketsPerVbucket)
func GetBucketIndexFromKey(key []byte, numberOfBins int) int {
	crc := crc32.ChecksumIEEE(key)
	return int(math.Mod(float64(crc), float64(numberOfBins)))
}

// evenly distribute load across workers
// assumes that num_of_worker <= num_of_load
// returns load_distribution [][]int, where
//     load_distribution[i][0] is the start index, inclusive, of load for ith worker
//     load_distribution[i][1] is the end index, exclusive, of load for ith worker
// note that load is zero indexed, i.e., indexed as 0, 1, .. N-1 for N loads
func BalanceLoad(num_of_worker int, num_of_load int) [][]int {
	load_distribution := make([][]int, 0)

	max_load_per_worker := int(math.Ceil(float64(num_of_load) / float64(num_of_worker)))
	num_of_worker_with_max_load := num_of_load - (max_load_per_worker-1)*num_of_worker

	index := 0
	var num_of_load_per_worker int
	for i := 0; i < num_of_worker; i++ {
		if i < num_of_worker_with_max_load {
			num_of_load_per_worker = max_load_per_worker
		} else {
			num_of_load_per_worker = max_load_per_worker - 1
		}

		load_for_worker := make([]int, 2)
		load_for_worker[0] = index
		index += num_of_load_per_worker
		load_for_worker[1] = index

		load_distribution = append(load_distribution, load_for_worker)
	}

	if index != num_of_load {
		panic(fmt.Sprintf("number of load processed %v does not match total number of load %v", index, num_of_load))
	}

	return load_distribution
}

func ParseHighSeqnoStat(statsMap map[string]map[string]string, highSeqnoMap map[uint16]uint64, vbuuidMap map[uint16]uint64, getHighSeqno bool) error {
	for _, statsMapPerServer := range statsMap {
		for vbno := 0; vbno < base.NumberOfVbuckets; vbno++ {
			uuidKey := fmt.Sprintf(base.VbucketUuidStatsKey, vbno)
			uuidStr, ok := statsMapPerServer[uuidKey]
			if ok && uuidStr != "" {
				uuid, err := strconv.ParseUint(uuidStr, 10, 64)
				if err != nil {
					err = fmt.Errorf("uuid for vbno=%v in stats map is not a valid uint64. uuid=%v\n", vbno, uuidStr)
					fmt.Printf("%v\n", err)
					return err
				}
				vbuuidMap[uint16(vbno)] = uuid
			}

			if getHighSeqno {
				highSeqnoKey := fmt.Sprintf(base.VbucketHighSeqnoStatsKey, vbno)
				highSeqnoStr, ok := statsMapPerServer[highSeqnoKey]
				if ok && highSeqnoStr != "" {
					highSeqno, err := strconv.ParseUint(highSeqnoStr, 10, 64)
					if err != nil {
						err = fmt.Errorf("high seqno for vbno=%v in stats map is not a valid uint64. high seqno=%v\n", vbno, highSeqnoStr)
						fmt.Printf("%v\n", err)
						return err
					}
					highSeqnoMap[uint16(vbno)] = highSeqno
				}
			}
		}
	}

	if len(vbuuidMap) != base.NumberOfVbuckets {
		err := fmt.Errorf("did not get all vb uuid. len(vbuuidMap) =%v\n", len(vbuuidMap))
		fmt.Printf("%v\n", err)
		return err
	}

	if getHighSeqno && len(highSeqnoMap) != base.NumberOfVbuckets {
		err := fmt.Errorf("did not get all high seqnos. len(highSeqnoMap) =%v\n", len(highSeqnoMap))
		fmt.Printf("%v\n", err)
		return err
	}

	return nil
}

func WaitForWaitGroup(waitGroup *sync.WaitGroup, doneChan chan bool) {
	waitGroup.Wait()
	close(doneChan)
}

type ExponentialOpFunc func() error

/**
 * Executes a anonymous function that returns an error. If the error is non nil, retry with exponential backoff.
 * Returns base.ErrorFailedAfterRetry + the last recorded error if operation times out, nil otherwise.
 * Max retries == the times to retry in additional to the initial try, should the initial try fail
 * initialWait == Initial time with which to start
 * Factor == exponential backoff factor based off of initialWait
 */
func ExponentialBackoffExecutor(name string, initialWait time.Duration, maxRetries int, factor int, maxBackoff time.Duration, op ExponentialOpFunc) error {
	waitTime := initialWait
	var opErr error
	for i := 0; i <= maxRetries; i++ {
		opErr = op()
		if opErr == nil {
			return nil
		} else if i != maxRetries {
			fmt.Printf("%v executor failed with %v. retry=%v\n", name, opErr, i)
			time.Sleep(waitTime)
			waitTime *= time.Duration(factor)
			if waitTime > maxBackoff {
				waitTime = maxBackoff
			}
		}
	}
	opErr = fmt.Errorf("%v Operation failed after max retries. Last error: %v", name, opErr.Error())
	return opErr
}

// add to error chan without blocking
func AddToErrorChan(errChan chan error, err error) {
	select {
	case errChan <- err:
	default:
		// some error already sent to errChan. no op
	}
}

func DeepCopyUint16Array(in []uint16) []uint16 {
	if in == nil {
		return nil
	}

	out := make([]uint16, len(in))
	copy(out, in)
	return out
}

func ShuffleVbList(list []uint16) {
	r := mrand.New(mrand.NewSource(time.Now().Unix()))
	// Start at the end of the slice, go backwards and scramble
	for i := len(list); i > 1; i-- {
		randIndex := r.Intn(i)
		// Swap values and continue until we're done
		if (i - 1) != randIndex {
			list[i-1], list[randIndex] = list[randIndex], list[i-1]
		}
	}
}

func GetRBACSupportedAndBucketPassword(remoteConnectStr, bucketName, remoteUsername, remotePassword string) (bool, string, error) {
	bucketInfo, err := GetBucketInfo(remoteConnectStr, bucketName, remoteUsername, remotePassword)
	if err != nil || bucketInfo == nil {
		return false, "", fmt.Errorf("Error retrieveing bucket info for %v. err=%v\n", remoteConnectStr, err)
	}

	clusterCompatibility, err := GetClusterCompatibilityFromBucketInfo(bucketInfo)
	if err != nil {
		return false, "", err
	}

	rbacSupported := IsClusterCompatible(clusterCompatibility, base.VersionForRBACSupport)
	bucketPassword := ""
	if !rbacSupported {
		// bucket password is needed when rbac is not supported
		bucketPassword, err = GetBucketPasswordFromBucketInfo(bucketName, bucketInfo)
		if err != nil {
			return false, "", err
		}
	}

	return rbacSupported, bucketPassword, nil
}

func GetBucketInfo(remoteConnectStr, bucketName, remoteUsername, remotePassword string) (map[string]interface{}, error) {
	bucketInfo := make(map[string]interface{})

	req, err := http.NewRequest(base.HttpGet, remoteConnectStr+base.PoolsDefaultBucketPath+bucketName, nil)
	if err != nil {
		return nil, nil
	}

	req.SetBasicAuth(remoteUsername, remotePassword)

	client := &http.Client{}

	res, err := client.Do(req)
	if err == nil && res != nil {
		err = ParseResponseBody(res, &bucketInfo)
		if err != nil {
			return nil, err
		}
		return bucketInfo, nil
	}

	return bucketInfo, err
}

func ParseResponseBody(res *http.Response,
	out interface{}) error {
	if res != nil && res.Body != nil {
		defer res.Body.Close()
		bod, err := ioutil.ReadAll(io.LimitReader(res.Body, res.ContentLength))
		if err != nil {
			fmt.Printf("Failed to read response body, err=%v\n res=%v\n", err, res)
			return err
		}
		if out != nil {
			err_marshal := json.Unmarshal(bod, out)
			if err_marshal != nil {
				fmt.Printf("Failed to unmarshal the response as json, err=%v, bod=%v\n res=%v\n", err_marshal, bod, res)
				out = bod
			}
		}
	}
	return nil
}

func GetClusterCompatibilityFromBucketInfo(bucketInfo map[string]interface{}) (int, error) {
	nodeList, err := GetNodeListFromInfoMap(bucketInfo)
	if err != nil {
		return 0, err
	}

	clusterCompatibility, err := GetClusterCompatibilityFromNodeList(nodeList)
	if err != nil {
		return 0, err
	}

	return clusterCompatibility, nil
}

func GetClusterCompatibilityFromNodeList(nodeList []interface{}) (int, error) {
	if len(nodeList) > 0 {
		firstNode, ok := nodeList[0].(map[string]interface{})
		if !ok {
			return 0, fmt.Errorf("node info is of wrong type. node info=%v", nodeList[0])
		}
		clusterCompatibility, ok := firstNode[base.ClusterCompatibilityKey]
		if !ok {
			return 0, fmt.Errorf("Can't get cluster compatibility info. node info=%v\n If replicating to ElasticSearch node, use XDCR v1.", nodeList[0])
		}
		clusterCompatibilityFloat, ok := clusterCompatibility.(float64)
		if !ok {
			return 0, fmt.Errorf("cluster compatibility is not of int type. type=%v", reflect.TypeOf(clusterCompatibility))
		}
		return int(clusterCompatibilityFloat), nil
	}

	return 0, fmt.Errorf("node list is empty")
}

func GetNodeListFromInfoMap(infoMap map[string]interface{}) ([]interface{}, error) {
	// get node list from the map
	nodes, ok := infoMap[base.NodesKey]
	if !ok {
		errMsg := fmt.Sprintf("info map contains no nodes. info map=%v", infoMap)
		return nil, errors.New(errMsg)
	}

	nodeList, ok := nodes.([]interface{})
	if !ok {
		errMsg := fmt.Sprintf("nodes is not of list type. type of nodes=%v", reflect.TypeOf(nodes))
		return nil, errors.New(errMsg)
	}

	// only return the nodes that are active
	activeNodeList := make([]interface{}, 0)
	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			errMsg := fmt.Sprintf("node info is not of map type. type=%v", reflect.TypeOf(node))
			return nil, errors.New(errMsg)
		}
		clusterMembershipObj, ok := nodeInfoMap[base.ClusterMembershipKey]
		if !ok {
			// this could happen when target is elastic search cluster (or maybe very old couchbase cluster?)
			// consider the node to be "active" to be safe
			fmt.Printf("node info map does not contain cluster membership. node info map=%v ", nodeInfoMap)
			activeNodeList = append(activeNodeList, node)
			continue
		}
		clusterMembership, ok := clusterMembershipObj.(string)
		if !ok {
			// play safe and return the node as active
			fmt.Printf("cluster membership is not string type. type=%v ", reflect.TypeOf(clusterMembershipObj))
			activeNodeList = append(activeNodeList, node)
			continue
		}
		if clusterMembership == "" || clusterMembership == base.ClusterMembership_Active {
			activeNodeList = append(activeNodeList, node)
		}
	}

	return activeNodeList, nil
}

func GetBucketPasswordFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error) {
	bucketPassword := ""
	bucketPasswordObj, ok := bucketInfo[base.SASLPasswordKey]
	if !ok {
		return "", fmt.Errorf("Error looking up password of bucket %v", bucketName)
	} else {
		bucketPassword, ok = bucketPasswordObj.(string)
		if !ok {
			return "", fmt.Errorf("Password of bucket %v is of wrong type", bucketName)
		}
	}
	return bucketPassword, nil
}

// check if a cluster (with specified clusterCompatibility) is compatible with version
func IsClusterCompatible(clusterCompatibility int, version []int) bool {
	return clusterCompatibility >= EncodeVersionToEffectiveVersion(version)
}

// encode version into an integer
func EncodeVersionToEffectiveVersion(version []int) int {
	majorVersion := 0
	minorVersion := 0
	if len(version) > 0 {
		majorVersion = version[0]
	}
	if len(version) > 1 {
		minorVersion = version[1]
	}

	effectiveVersion := majorVersion*0x10000 + minorVersion
	return effectiveVersion
}

// Diff tool by default allow users to enter "http://<addr>:<ns_serverPort>"
const httpPrefix = "http://"
const couchbasePrefix = "couchbase://"

func PopulateCCCPConnectString(url string) string {
	var cccpUrl string
	if strings.HasPrefix(url, httpPrefix) {
		cccpUrl = strings.TrimPrefix(url, httpPrefix)
	} else {
		cccpUrl = url
	}

	if !strings.HasPrefix(cccpUrl, couchbasePrefix) {
		cccpUrl = fmt.Sprintf("%v%v", couchbasePrefix, cccpUrl)
	}

	return cccpUrl
}
