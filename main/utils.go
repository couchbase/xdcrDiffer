// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math"
	"strconv"
)

func GetFileName(fileDir string, vbno uint16, bucketIndex int) string {
	var buffer bytes.Buffer
	buffer.WriteString(fileDir)
	buffer.WriteString(FileDirDelimiter)
	buffer.WriteString(FileNamePrefix)
	buffer.WriteString(FileNameDelimiter)
	buffer.WriteString(fmt.Sprintf("%v", vbno))
	buffer.WriteString(FileNameDelimiter)
	buffer.WriteString(fmt.Sprintf("%v", bucketIndex))
	return buffer.String()
}

// hash key into a bucket index in range [0, NumberOfBucketsPerVbucket)
func GetBucketIndexFromKey(key []byte) int {
	crc := crc32.ChecksumIEEE(key)
	return int(math.Mod(float64(crc), float64(NumberOfBucketsPerVbucket)))
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

func ParseHighSeqnoStat(statsMap map[string]map[string]string, highSeqnoMap map[uint16]uint64) error {
	for _, statsMapPerServer := range statsMap {
		for vbno := 0; vbno < NumerOfVbuckets; vbno++ {
			statsKey := fmt.Sprintf(VbucketHighSeqnoStatsKey, vbno)
			highSeqnoStr, ok := statsMapPerServer[statsKey]
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

	if len(highSeqnoMap) != NumerOfVbuckets {
		err := fmt.Errorf("did not get all high seqnos. len(highSeqnoMap) =%v\n", len(highSeqnoMap))
		fmt.Printf("%v\n", err)
		return err
	}

	return nil
}
