# xdcrDiffer - a diffing tool used to confirm data consistency between XDCR clusters

This tool first contacts the specified clusters first the DCP protocol to extract data digest from both sides. It then compares the information to find if there are any data inconsistencies, lists them on screen, and also outputs the data to a JSON file.

If an XDCR is ongoing, it is quite possible that the tool will show documents as missing even though they are in transit. The tool then will do a second round of verification on these missing documents to make sure they are false positives.

## Getting Started


### Prerequisites

Golang version 1.13 or above.

First, clone the repository to any preferred destination.
The build system will be using go modules, so it does not require special GOPATH configurations.

```
neil.huang@NeilsMacbookPro:~/go/src/github.com/couchbaselabs$ git clone git@github.com:couchbaselabs/xdcrDiffer.git
```

### Compiling

It can be compiled using the accompanied make file. If necessary, the `make deps` can accomplish that. It uses go modules and gathers the necessary dependencies.

```
neil.huang@NeilsMacbookPro:~/go/src/github.com/couchbaselabs/xdcrDiffer$ make deps
```

Then simply run make once the dependencies are satisfied:

```
neil.huang@NeilsMacbookPro:~/go/src/github.com/couchbaselabs/xdcrDiffer$ make
```

### Running
#### runDiffer

The `runDiffer.sh` shell script will ask for the minimum required information to run the difftool, and can be edited to add or modify detailed settings that are to be passed to the difftool itself. This is the *preferred* method.

The script also sets up the shell environment to allow the tool binary to be able to contact the source cluster's metakv given the user specified credentials to retrieve the `remote cluster reference` and `replication specification` in order to simulate the existing replication scenario (i.e. filtering). Note that credentials are sent unencrypted for now.

For example:
```
neil.huang@NeilsMacbookPro:~/go/src/github.com/couchbaselabs/xdcrDiffer$ ./runDiffer.sh -u Administrator -p password -h 127.0.0.1:9000 -r backupCluster -s beer-sample -t backupDumpster
```

#### Tool binary
The legacy method is to run the tool binary natively by using the options provided that can be found using "-h".
Note that running the tool natively will bypass the `remote cluster reference` and `replication specification` retrieval from the source node's metakv.

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

## DiffTool Process Flow
The difftool performs the following in order:
1. Data Retrieval from source and target buckets via DCP (can press Ctrl-C to move onto next phase)
2. Diff files retrieved from DCP to find differences
3. Verify differences from above using async Get (verifyDiffKeys) to rule out transitional mutations

## Output
Results from file diffing can be viewed as a summary file under `diffKeys`:
```
neil.huang@NeilsMacbookPro:~/go/src/github.com/couchbaselabs/xdcrDiffer$ cat fileDiff/diffKeys
null
```

Results from verifyDiffKeys can be viewed as JSON summary files under `mutationDiff`:
```
neil.huang@NeilsMacbookPro:~/go/src/github.com/couchbaselabs/xdcrDiffer/mutationDiff$ ls
diffKeysWithError	mutationBodyDiffDetails	mutationBodyDiffKeys	mutationDiffDetails	mutationDiffKeys

neil.huang@NeilsMacbookPro:~/go/src/github.com/couchbaselabs/xdcrDiffer/mutationDiff$ cat mutationDiffDetails
{"Mismatch":{},"MissingFromSource":{},"MissingFromTarget":{}}
```

## License

Copyright 2018-2020 Couchbase, Inc. All rights reserved.
