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
	"github.com/couchbase/xdcrDiffer/encryption"
)

func GetFileName(fileDir string, vbno uint16, bucketIndex int, encryptor encryption.FileOps) string {
	var buffer bytes.Buffer
	buffer.WriteString(fileDir)
	buffer.WriteString(base.FileDirDelimiter)
	buffer.WriteString(base.FileNamePrefix)
	buffer.WriteString(base.FileNameDelimiter)
	buffer.WriteString(fmt.Sprintf("%v", vbno))
	buffer.WriteString(base.FileNameDelimiter)
	buffer.WriteString(fmt.Sprintf("%v", bucketIndex))
	buffer.WriteString(encryptor.GetEncryptionFilenameSuffix())
	return buffer.String()
}

func GetManifestFileName(fileDir string, encryptor encryption.FileOps) string {
	var buffer bytes.Buffer
	buffer.WriteString(fileDir)
	buffer.WriteString(base.FileDirDelimiter)
	buffer.WriteString(base.FileNamePrefix)
	buffer.WriteString(base.FileNameDelimiter)
	buffer.WriteString(fmt.Sprintf("%v", base.ManifestFileName))
	buffer.WriteString(encryptor.GetEncryptionFilenameSuffix())
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

func ParseHighSeqnoStat(statsMap map[string]map[string]string, highSeqnoMap map[uint16]uint64, vbuuidMap map[uint16]uint64, getHighSeqno bool, numberOfVbs int) error {
	for _, statsMapPerServer := range statsMap {
		for vbno := 0; vbno < numberOfVbs; vbno++ {
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

	if len(vbuuidMap) != numberOfVbs {
		err := fmt.Errorf("did not get all vb uuid. len(vbuuidMap) =%v\n", len(vbuuidMap))
		fmt.Printf("%v\n", err)
		return err
	}

	if getHighSeqno && len(highSeqnoMap) != numberOfVbs {
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
	client, err := u.GetHttpClient(username, authMech, certificate, false, nil, nil, host, nil, nil)
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

var crc32tab = []uint32{
	0x00000000, 0x77073096, 0xee0e612c, 0x990951ba,
	0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3,
	0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
	0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
	0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de,
	0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
	0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec,
	0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5,
	0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
	0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,
	0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940,
	0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
	0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116,
	0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
	0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
	0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,
	0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a,
	0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
	0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818,
	0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01,
	0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
	0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457,
	0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c,
	0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
	0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2,
	0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb,
	0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
	0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9,
	0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086,
	0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
	0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4,
	0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad,
	0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
	0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
	0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8,
	0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
	0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe,
	0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7,
	0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
	0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5,
	0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252,
	0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
	0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60,
	0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
	0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
	0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f,
	0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04,
	0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
	0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a,
	0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713,
	0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
	0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21,
	0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e,
	0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
	0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c,
	0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
	0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
	0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db,
	0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0,
	0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
	0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6,
	0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
	0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
	0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d}

func cbCrc(key []byte) uint32 {
	crc := uint32(0xffffffff)
	for x := 0; x < len(key); x++ {
		crc = (crc >> 8) ^ crc32tab[(uint64(crc)^uint64(key[x]))&0xff]
	}
	return (^crc) >> 16 & 0x7fff
}

func CbcVbMap(key []byte, numVbs uint32) uint16 {
	return uint16(cbCrc(key) % numVbs)
}
