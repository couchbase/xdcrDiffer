// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package base

import ()

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
const SourceCheckpointFileName = "SourceCheckpoint"
const TargetCheckpointFileName = "TargetCheckpoint"
const SourceClusterName = "source"
const TargetClusterName = "target"
const DiffKeysFileName = "diffKeys"
const DiffDetailsFileName = "diffDetails"
const MutationDiffFileName = "mutationDiffDetails"
const DiffErrorKeysFileName = "diffKeysWithError"
const StatsReportInterval = 5

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

// length of mutation metadata + body, which consists of
//  seqno   - 8 bytes
//  revId   - 8 bytes
//  cas     - 8 bytes
//  flags   - 4 bytes
//  expiry  - 4 bytes
//  hash    - 64 bytes
const BodyLength = 96
