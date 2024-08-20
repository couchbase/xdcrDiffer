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
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math"
	mrand "math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	xdcrBase "github.com/couchbase/goxdcr/v8/base"
	xdcrUtils "github.com/couchbase/goxdcr/v8/utils"
	"github.com/couchbase/xdcrDiffer/base"
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
//
//	load_distribution[i][0] is the start index, inclusive, of load for ith worker
//	load_distribution[i][1] is the end index, exclusive, of load for ith worker
//
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

func PopulateCCCPConnectString(url string) string {
	var cccpUrl string
	if strings.HasPrefix(url, base.HttpPrefix) {
		cccpUrl = strings.TrimPrefix(url, base.HttpPrefix)
	} else if strings.HasPrefix(url, base.HttpsPrefix) {
		cccpUrl = strings.TrimPrefix(url, base.HttpsPrefix)
	} else {
		cccpUrl = url
	}

	// For production environment (non-cluster-run), strip off the port
	portNo, portErr := xdcrBase.GetPortNumber(cccpUrl)
	if portErr == nil && (portNo < base.ClusterRunMinPortNo || portNo > base.ClusterRunMaxPortNo) {
		cccpUrl = xdcrBase.GetHostName(cccpUrl)
	}

	if !strings.HasPrefix(cccpUrl, base.CouchbasePrefix) {
		cccpUrl = fmt.Sprintf("%v%v", base.CouchbasePrefix, cccpUrl)
	}
	return cccpUrl
}

func DiffKeysFileName(isSource bool, diffFileDir, diffKeysFileName string) string {
	suffix := base.SourceClusterName
	if !isSource {
		suffix = base.TargetClusterName
	}
	return diffFileDir + base.FileDirDelimiter + diffKeysFileName + base.FileNameDelimiter + suffix
}

func GetCertificate(u xdcrUtils.UtilsIface, hostname string, username, password string, authMech xdcrBase.HttpAuthMech) ([]byte, error) {
	certificate := make([]byte, 0)

	userAuthMode := xdcrBase.UserAuthModeBasic
	req, host, err := u.ConstructHttpRequest(hostname, xdcrBase.DefaultPoolPath+"/certificate", false, username, password, authMech, userAuthMode, xdcrBase.MethodGet, xdcrBase.DefaultContentType, nil, nil)
	if err != nil {
		return nil, err
	}
	client, err := u.GetHttpClient(username, authMech, certificate, false, nil, nil, host, nil)
	if err != nil {
		return nil, err
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(res.Body)
}

// type to facilitate the sorting of uint16 lists
type Uint8List []uint8

func (u Uint8List) Len() int           { return len(u) }
func (u Uint8List) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u Uint8List) Less(i, j int) bool { return u[i] < u[j] }

func SortUint8List(list []uint8) []uint8 {
	sort.Sort(Uint8List(list))
	return list
}

func SearchUint8List(seqno_list []uint8, seqno uint8) (int, bool) {
	index := sort.Search(len(seqno_list), func(i int) bool {
		return seqno_list[i] >= seqno
	})
	if index < len(seqno_list) && seqno_list[index] == seqno {
		return index, true
	} else {
		return index, false
	}
}
