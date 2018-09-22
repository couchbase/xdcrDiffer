// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package base

import (
	"time"
)

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
const CheckpointFileBufferSize = 200000
const SourceClusterName = "source"
const TargetClusterName = "target"
const DiffKeysFileName = "diffKeys"
const DiffDetailsFileName = "diffDetails"
const MutationDiffFileName = "mutationDiffDetails"
const DiffErrorKeysFileName = "diffKeysWithError"
const StatsReportInterval = 5

var BucketOpTimeout time.Duration = 10 * time.Second
var GetStatsRetryInterval time.Duration = 1 * time.Second
var SendBatchRetryInterval time.Duration = 500 * time.Millisecond

const MaxNumOfRetry = 10
const GetStatsBackoffFactor = 1
const SendBatchBackoffFactor = 2

// time to wait to stop processing of target cluster after processing of source cluster is completed
var DelayBetweenSourceAndTarget time.Duration = 2 * time.Second

// length of mutation metadata + body, which consists of
//  seqno   - 8 bytes
//  revId   - 8 bytes
//  cas     - 8 bytes
//  flags   - 4 bytes
//  expiry  - 4 bytes
//  hash    - 64 bytes
const BodyLength = 96
