# xdcrDiffer - a diffing tool used to confirm data consistency between XDCR clusters

This tool first contacts the specified clusters first the DCP protocol to extract data digest from both sides. It then compares the information to find if there are any data inconsistencies, lists them on screen, and also outputs the data to a JSON file.

If an XDCR is ongoing, it is quite possible that the tool will show documents as missing even though they are in transit. The tool then will do a second round of verification on these missing documents to make sure they are false positives.

## Getting Started

The tool can be built using golang into a single executable, and then run from the command line.


### Prerequisites

Golang version 1.8 or above.

### Compiling

First, clone the repository. Ensure that with respective to the defined GOPATH variable, that this directory is in, and execute go build under the main directory.

```
~/go/src/github.com/nelio2k/xdcrDiffer/main$ go build -o xdcrDiffer
```

### Running

To run the tool, use the options provided that can be found using "-h"

```
Usage of ./xdcrDiffer:
  -checkpointFileDir string
    	directory for checkpoint files (default "checkpoint")
  -completeByDuration uint
    	duration that the tool should run (default 1)
  -completeBySeqno
    	whether tool should automatically complete (after processing all mutations at start time) (default true)
  -diffFileDir string
    	 directory for storing diffs (default "diff")
  -newCheckpointFileName string
    	new checkpoint file to write to when tool shuts down
  -numberOfBins uint
    	number of buckets per vbucket (default 10)
  -numberOfFileDesc uint
    	number of file descriptors
  -numberOfWorkersForDcp uint
    	number of worker threads for dcp (default 10)
  -numberOfWorkersForFileDiffer uint
    	number of worker threads for file differ  (default 10)
  -numberOfWorkersForMutationDiffer uint
    	number of worker threads for mutation differ  (default 10)
  -oldCheckpointFileName string
    	old checkpoint file to load from when tool starts
  -sourceBucketName string
    	bucket name for source cluster (default "default")
  -sourceFileDir string
    	directory to store mutations in source cluster (default "source")
  -sourcePassword string
    	password for source cluster (default "welcome")
  -sourceUrl string
    	url for source cluster (default "http://localhost:9000")
  -sourceUsername string
    	username for source cluster (default "Administrator")
  -targetBucketName string
    	bucket name for target cluster (default "target")
  -targetFileDir string
    	directory to store mutations in target cluster (default "target")
  -targetPassword string
    	password for target cluster (default "welcome")
  -targetUrl string
    	url for target cluster (default "http://localhost:9000")
  -targetUsername string
    	username for target cluster (default "Administrator")
  -verifyDiffKeys
    	 whether to verify diff keys through aysnc Get on clusters (default true)
```

A few options worth noting:

- completeBySeqno - This flag will determine whether or not the tool will end by sequence number, or by time.
- checkpointDir - checkpointing allows the tool to resume from the last point in time when the tool was interrupted.
- oldCheckpointFileName - this is the flag to use to specify a last checkpoint from which to resume.
- verifyDiffKeys - By default this is enabled, which uses a non-stream based, key-by-key retrieval and validation. This is what is considered the second pass of verification after the first pass.
- numberOfBins - Each Couchbase bucket contains 1024 vbuckets. For optimizing sorting, each vbucket is also sub-divided into bins as the data are streamed before the diff operation.
- numberOfFileDesc - If the tool has exhausted all system file descriptors, this option allows the tool to limit the max number of concurently open file descriptors.

## License

Copyright 2018 Couchbase, Inc. All rights reserved.
