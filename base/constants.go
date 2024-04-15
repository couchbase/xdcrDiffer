// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package base

const NumberOfVbuckets = 1024
const DcpHandlerChanSize = 100000
const FileNamePrefix = "diffTool"
const FileNameDelimiter = "_"
const FileDirDelimiter = "/"
const BucketBufferCapacity = 100000
const FileModeReadWrite = 0666
const StreamingBucketName = "xdcrDiffTool"
const VbucketSeqnoStatName = "vbucket-seqno"
const VbucketHighSeqnoStatsKey = "vb_%v:high_seqno"
const VbucketUuidStatsKey = "vb_%v:uuid"
const SourceFileDir = "source"
const TargetFileDir = "target"
const CheckpointFileDir = "checkpoint"
const FileDifferDir = "fileDiff"
const MutationDifferDir = "mutationDiff"
const DiffKeysFileName = "diffKeys"
const DiffDetailsFileName = "diffDetails"
const DiffKeysSrcMigrationHintSuffix = "hint"
const MutationDiffFileName = "mutationDiffDetails"
const MutationDiffColIdMapping = "mutationDiffColIdMapping"
const MutationDiffMigrationDetails = "mutationMigrationDetails"
const DiffErrorKeysFileName = "diffKeysWithError"
const StatsReportInterval = 5
const SourceClusterName = "source"
const TargetClusterName = "target"
const SelfReferenceName = "xdcrDifftoolSelfRef"
const ManifestFileName = "manifest"

const NodesKey = "nodes"
const PoolsDefaultBucketPath = "/pools/default/buckets/"
const SASLPasswordKey = "saslPassword"
const HttpGet = "GET"

// default values for configurable parameters if not specified by user
const BucketOpTimeout uint64 = 20
const GetStatsRetryInterval uint64 = 2
const GetStatsMaxBackoff uint64 = 10
const SendBatchRetryInterval uint64 = 500
const SendBatchMaxBackoff uint64 = 5
const GetStatsBackoffFactor = 2
const SendBatchBackoffFactor = 2
const MaxNumOfGetStatsRetry = 10
const MaxNumOfSendBatchRetry = 10
const DelayBetweenSourceAndTarget uint64 = 2
const CheckpointInterval = 600

const ClusterRunMinPortNo uint16 = 9000
const ClusterRunMaxPortNo uint16 = 9007

// length of mutation metadata + body, which consists of
//
//	seqno    - 8 bytes
//	revId    - 8 bytes
//	cas      - 8 bytes
//	flags    - 4 bytes
//	expiry   - 4 bytes
//	opCode   - 2 bytes
//	datatype - 2 byte
//	hash     - 64 bytes
//	collectionId - 4 bytes
//	migrationFilterLen - 2 bytes
//	(variable) - each filterID is 2 bytes
const BodyLength = 104
const KeyLenVariable = 2
const MigrationFilterLen = 2
const xattrSizeLen = 4 // To store the size of the individual Xattr Key-Value pair

const (
	JsonBody     = "Body"
	JsonMetadata = "Metadata"
	Updated      = "Updated"
)

// This function is used to calculate the length of the byte array for serializing a mutation
// @param keyLen denotes the length of the document key
// @param size denoted the combined length of importCAS and HLV
// @param colMigrationFilterMatched denotes the list of Migration Filters matched
func GetFixedSizeMutationLen(keyLen int, size uint32, colMigrationFilterMatched []uint8) int {
	return KeyLenVariable + keyLen + (2 * xattrSizeLen) + int(size) + BodyLength + MigrationFilterLen + len(colMigrationFilterMatched)*2 // (2*xattrSizeLen - to store the size of importCAS and HLV)

}

var VersionForRBACSupport = []int{5, 0}

var ClusterCompatibilityKey = "clusterCompatibility"
var ClusterMembershipKey = "clusterMembership"
var ClusterMembership_Active = "active"

type FilterResultType int

const (
	NotFilter      FilterResultType = iota
	Filtered       FilterResultType = iota
	UnableToFilter FilterResultType = iota
)

// Diff tool by default allow users to enter "http://<addr>:<ns_serverPort>"
const HttpPrefix = "http://"
const HttpsPrefix = "https://"
const CouchbasePrefix = "couchbase://"
const CouchbaseSecurePrefix = "couchbases://"

var SetupTimeoutSeconds int = 10

const JSONDataType = 1

const (
	MutationCompareTypeMetadata    = "meta" // This is the default
	MutationCompareTypeBodyAndMeta = "both" // This is the original method
	MutationCompareTypeBodyOnly    = "body"
)

var MutationDiffCompareType = []string{MutationCompareTypeMetadata, MutationCompareTypeBodyOnly, MutationCompareTypeBodyAndMeta}

const Uint32MaxVal uint32 = 1<<32 - 1
