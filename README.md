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
#### Preparing Couchbase Clusters
Before running the differ to examine consistencies between two clusters, it is *highly recommended* to first set the Metadata Purge Interval to a low value, and then once that period has elapsed, run compaction on both clusters to ensure that tombstones are removed. Compaction will also ensure that the differ will only receive the minimum amount of data necessary, which will help minimize the storage requirement for the diff tool.

#### runDiffer

The `runDiffer.sh` shell script will ask for the minimum required information to run the difftool, and can be edited to add or modify detailed settings that are to be passed to the difftool itself. This is the *preferred* method.

The script also sets up the shell environment to allow the tool binary to be able to contact the source cluster's metakv given the user specified credentials to retrieve the `remote cluster reference` and `replication specification` in order to simulate the existing replication scenario (i.e. filtering). Note that credentials are sent unencrypted for now.

For example:
```
neil.huang@NeilsMacbookPro:~/go/src/github.com/couchbaselabs/xdcrDiffer$ ./runDiffer.sh -u Administrator -p password -h 127.0.0.1:9000 -r backupCluster -s beer-sample -t backupDumpster
```

#### Preparing xdcrDiffer host for running differ
While the differ can run on any machine that compiles the binary, one method of running the differ tool is to run on a non-KV couchbase node.
It is also possible to create a small Couchbase node that has only a simple non-impacting service enabled (i.e. Backup), and rebalance in to the cluster for running the differ, which will not trigger vb movement.
The node can then be removed once the differ has finished running.
The runDiffer script above will then allow the differ to access metadata information to enable various features, including using secure connections, or collections.

#### Tool binary
The legacy method is to run the tool binary natively by using the options provided that can be found using "-h".
Note that running the tool natively will bypass the `remote cluster reference` and `replication specification` retrieval from the source node's metakv.
And that this legacy method does not support features that are introduced _after_ Couchbase Server 6.0.

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
  -enforceTLS
    	 stops executing if pre-requisites are not in place to ensure TLS communications
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
  -mutationRetries
        Additional number of times to retry to resolve the mutation differences
  -mutationRetriesWaitSecs
        Seconds to wait in between retries for mutation differences
```

A few options worth noting:

- completeBySeqno - This flag will determine whether or not the tool will end by sequence number, or by time.
- checkpointDir - checkpointing allows the tool to resume from the last point in time when the tool was interrupted.
- oldCheckpointFileName - this is the flag to use to specify a last checkpoint from which to resume.
- verifyDiffKeys - By default this is enabled, which uses a non-stream based, key-by-key retrieval and validation. This is what is considered the second pass of verification after the first pass.
- numberOfBins - Each Couchbase bucket contains 1024 vbuckets. For optimizing sorting, each vbucket is also sub-divided into bins as the data are streamed before the diff operation.
- numberOfFileDesc - If the tool has exhausted all system file descriptors, this option allows the tool to limit the max number of concurently open file descriptors.
- mutationRetries - If there are differences, the tool will retry a specified amount of times to try to reconcile potential in-flight differences

#### Running with TLS encrypted traffic
The xdcrDiffer supports running with encrypted traffic such that no data (or metadata) is sent or received in plain text over the wire. To run TLS, the followings need to be in place:
1. The xdcrDiffer must be run using the runDiffer.sh
2. The xdcrDiffer must be run on a Couchbase Server node that is a part of the source cluster (the said node does not need KV service)
3. The `runDiffer.sh`'s `-h` argument (hostname to contact) must be a loopback address to the local node's ns_server (i.e. `127.0.0.1:8091`)
4. The specified remote cluster reference for `runDiffer.sh` must have Full-Encryption and the appropriate username/password already set up
5. (Optionally) The `-enforceTLS` flag passed to `xdcrDiffer` binary can be used to ensure the above pre-requisites are present, and will cause the program to exit if they are not in place (Not present in runDiffer.sh by default)
6. Obviously, that the loopback interface has not been tampered in any way

Once the above are in place, the xdcrDiffer will:
1. Retrieve the local cluster's Root Certificate from the local ns_server via loopback device to the ns_server's REST endpoint
2. Retrieve the remote cluster reference and replication spec from the local node's metakv, which contains the remote cluster's root certificate (Hopefully Node-To-Node encryption was already active)
3. Use the local cluster's root certificate to contact local cluster's ns_server (not necessary since using loopback, but does it anyway)
4. Use the local cluster's root certificate to contact local cluster's KV services over KV SSL ports
6. Use the remote cluster reference's root certificate to contact remote cluster's ns_server for any necessary information
5. Use the remote cluster reference's root certificate to contact remote cluster's KV services over KV SSL ports

## DiffTool Process Flow
The difftool performs the following in order:
1. Retrieve metadata from the specified node's metakv (if started via runDiffer.sh)
2. Data Retrieval from source and target buckets via DCP according to the specs' definitions (can press Ctrl-C to move onto next phase)
3. Diff files retrieved from DCP to find differences
4. Verify differences from above using async Get (verifyDiffKeys) to rule out transitional mutations

## Output
Results can be viewed as JSON summary files under `mutationDiff`:
```
neil.huang@NeilsMacbookPro:~/go/src/github.com/couchbaselabs/xdcrDiffer/mutationDiff$ ls
diffKeysWithError		mutationDiffColIdMapping	mutationDiffDetails

neil.huang@NeilsMacbookPro:~/go/src/github.com/nelio2k/xdcrDiffer/mutationDiff$ jsonpp mutationDiffDetails  | head
{
  "Mismatch": {},
  "MissingFromSource": {},
  "MissingFromTarget": {
    "0": {
      "xdcrProv_C10": {
        "Value": "eyJuYW1lIjogInhkY3JQcm92X0MxMCIsICJhZ2UiOiAwLCAiaW5kZXgiOiAiMCIsICJib2R5IjoiMDAwMDAwMDAwMCJ9",
        "Flags": 0,
        "Datatype": 1,
        "Cas": 1620776636481929216
        ...
```
If there is no differences, then the set will be empty. Otherwise, any differences will be shown as above via JSON.
The key of "0" represents the collection ID. For `MissingFromTarget`, the collection ID represents the target collection that the specific document should belong. For `MissingFromSource`, the collectionID would represent the collection ID under the source bucket.
For `Mismatch` column, the collection ID would represent collection ID for the source bucket.

### Manifests
Difftool will retrieve the manifests from both source and target buckets and store them under the corresponding source and target directories:
```
neil.huang@NeilsMacbookPro:~/go/src/github.com/nelio2k/xdcrDiffer$ find . -name diffTool_manifest
./target/diffTool_manifest
./source/diffTool_manifest
```

### Collection Mapping
The xdcrDiffer is going to compile various collection-to-collection mapping, and those are recorded as part of the differ log:
```
2021-05-11T17:03:49.564-07:00 INFO GOXDCR.xdcrDiffTool: Replication spec is using implicit mapping
2021-05-11T17:03:49.564-07:00 INFO GOXDCR.xdcrDiffTool: Collection namespace mapping: map[S1.col1:|Scope: S1 Collection: col1|  S1.col2:|Scope: S1 Collection: col2|  _default._default:|Scope: _default Collection: _default| ] idsMap: map[0:[0] 8:[8] 9:[9]]
```

## Detailed Q&A's
> Does the tool just match keys or the values of documents as well?

Each mutation from DCP is captured with the metadata as follows https://github.com/couchbaselabs/xdcrDiffer/blob/bc4b08afee4ff33424c8c8dfeab9d7cd3137be81/dcp/DcpHandler.go#L391
Essentially, it captures the metadata and translates a document’s value into a SHA-512 digest, which is 64 bytes.
Once all the data are captured from source and target, then it compares between the documents using document ID/Key, to figure out if a doc is missing from one of the two clusters. If none is missing, then it compares the metadata + body hash to see if they are the same.

> What is the largest data size that this tool can practically run on?

The limiting space factor here is the actual machine that is running the diff tool, since the diff tool receives data from the source and target clusters and then capture them for comparison. Each mutation the diff tool stores currently would be 102 bytes + key size. So, depending on how the customer’s docIDs are set up, the space could vary, but is calculable per situation.

> Does the tool always begin from sequence number 0? 

The diff tool has checkpointing mechanism built in in case of interruptions. The checkpointing mechanism is pretty much the same concept as XDCR checkpoints - that it knows where in the DCP stream it was last stopped and will try to resume from that point in time.

> I am hesitant to modify the purge interval or compact the bucket. What is the effect of not compacting beforehand?

Compacting and/or purging is to minimize the amount of data that the differ will receive from either source or target KV. Otherwise, it is possible for the differ to receive multiple versions of the same document as it mutates over time, and storing them all as part of the diffing operation.
It won’t affect the accuracy of the result, but it’ll cause differ to run longer and use more disk space.

## Known Limitations
1. No dynamic topology change support. If VBs are moved during runtime, the tool does not handle it well.
2. Strict security level is not supported at this time.

## License

Copyright 2018-2021 Couchbase, Inc. All rights reserved.
