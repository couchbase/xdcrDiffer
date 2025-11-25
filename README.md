# xdcrDiffer - a diffing tool used to confirm data consistency between XDCR clusters

This tool first contacts the specified clusters first the DCP protocol to extract data digest from both sides. It then compares the information to find if there are any data inconsistencies, lists them on screen, and also outputs the data to a JSON file.

If an XDCR is ongoing, it is quite possible that the tool will show documents as missing even though they are in transit. The tool then will do a second round of verification on these missing documents to make sure they are false positives.

# Table Of Contents
- [Getting Started](#getting-started)
    * [Prerequisites](#prerequisites)
    * [Compiling and Running](#compiling-and-running)
        + [Compiling Natively](#compiling-natively)
        + [Preparing Couchbase Clusters](#preparing-couchbase-clusters)
        + [runDiffer](#rundiffer)
        + [Compiling and Running with Docker](#compiling-and-running-with-docker)
        + [Preparing xdcrDiffer host for running differ](#preparing-xdcrdiffer-host-for-running-differ)
        + [Tool binary](#tool-binary)
        + [Running with TLS encrypted traffic](#running-with-tls-encrypted-traffic)
        + [Running with Encryption-At-Rest](#running-with-encryption-at-rest)
            - [Decrypt encrypted output files](#decrypt-encrypted-output-files)
- [DiffTool Process Flow](#difftool-process-flow)
- [Output](#output)
    * [Manifests](#manifests)
    * [Collection Mapping](#collection-mapping)
    * [Collection Migration Debugging](#collection-migration-debugging)
        + [How to interpret multi-target migration differ result](#how-to-interpret-multi-target-migration-differ-result)
- [Detailed Q&A's](#detailed-qas)
- [Known Limitations](#known-limitations)
- [License](#license)
## Getting Started


### Prerequisites

Couchbase Server version 7.1 and later

Golang version 1.23.0 or above.

First, clone the repository to any preferred destination.
The build system will be using go modules, so it does not require special GOPATH configurations.

```
~$ git clone https://github.com/couchbase/xdcrDiffer
```

### Compiling and Running

#### Compiling Natively
It can be compiled using the accompanying make file.

```
~/xdcrDiffer$ make
```

#### Preparing Couchbase Clusters
Before running the differ to examine consistencies between two clusters, it is *highly recommended* to first set the Metadata Purge Interval to a low value, and then once that period has elapsed, run compaction on both clusters to ensure that tombstones are removed. Compaction will also ensure that the differ will only receive the minimum amount of data necessary, which will help minimize the storage requirement for the diff tool.

#### runDiffer
The `runDiffer.sh` shell script will ask for the minimum required information to run the difftool. This information can either be passed through the command line or the script can also read from a YAML file. This is the *preferred* method.
Refer to `sampleConfig.yaml` for example.

The script also sets up the shell environment to allow the tool binary to be able to contact the source cluster's metakv given the user specified credentials to retrieve the `remote cluster reference` and `replication specification` in order to simulate the existing replication scenario (i.e. filtering). Note that credentials are sent unencrypted for now.

For example:
```
~/xdcrDiffer$ ./runDiffer.sh -u Administrator -p password -h 127.0.0.1:9000 -r backupCluster -s beer-sample -t backupDumpster -c
```
OR
```
~/xdcrDiffer$ ./runDiffer.sh -y ./sampleConfig.yaml
```

#### Compiling and Running with Docker

The tool can also be compiled and run using Docker. The following command will build the Docker image and run the tool with the specified parameters.
Note that this section will be outdated in the future as the `xdcrDiffer` should ultimately be run specifically on a Couchbase Server node.

```
docker build -t xdcr-differ:1.0.0 .
```

Create an output directory on the host machine to store the output files.

```
mkdir -p ./dockerOutput
```

(Optional) Docker buildx build and image push:
```
docker buildx build  --builder mycontext --platform "linux/amd64,linux/arm64" --push -t <myregistry>/xdcr-differ:1.0.0 .
```

Run the Docker container with the following command:
```
docker run -v `pwd`/dockerOutput:/outputs -t --network host xdcr-differ:1.0.0 -u <username> -p <password> -h <nodeIP>:8091 -s <srcBucket> -t <tgtBucket> -r <remClusterRefName> -o /outputs
```

In the above command, the container will be launched where the created `dockerOutput` directory is mounted to the container.
The host network will be used by the container to connect to the Couchbase cluster node.
The `nodeIP` should be an IP that is displayed by the Couchbase Server UI console (under the `Servers` tab)

#### Preparing xdcrDiffer host for running differ
While the differ can run on any couchbase node that compiles the binary, it is **strongly recommended** to run the differ tool on a **non-KV Couchbase node**. Running xdcrDiffer can cause noticeable CPU utilization spikes, so using a non-KV node helps avoid impacting cluster performance.

One method is to create a small Couchbase node that has only a simple non-impacting service enabled (ex Backup), and rebalance it into the cluster for running the differ, which will not trigger vBucket movement. The node can then be removed once the differ has finished running.

The runDiffer script above will then allow the differ to access metadata information to enable various features, including using secure connections, or collections.

While currently it could be run on a non-Couchbase node as an independent binary, this functionality will be deprecated in the future.
When that time comes, the binary must be run on a Couchbase Server node.

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

#### Running with Encryption-At-Rest
The xdcrDiffer supports encryption at rest such that sensitive that lands on disk is encrypted.
To run the xdcrDiffer in encryption mode, issue “-x” to the runDiffer shell script, and it will prompt at the command line prompt to enter an encryption passphrase:

```
./runDiffer.sh -x -u Administrator -p <password> -h 127.0.0.1:8091 -r C2 -s B1 -t B2 -c
…
Enter encryption passphrase:
Enter the same encryption passphrase again:
2025-11-24T14:12:56.877-08:00 INFO GOXDCR.differEncryption: Initializing encryption at rest with AES-GCM-256 and calculating key...
```

Output of the encrypted files will be encrypted with an .enc suffix.

##### Decrypt encrypted output files

To decrypt an encrypted file, use the differ as a decryptor tool with the following parameters:

```
$ ./xdcrDiffer -encryptionPassphrase -decrypt outputs/xdcrDiffer.log.enc
Enter encryption passphrase:
Enter the same encryption passphrase again:
<decrypted output to STDOUT>
```


## DiffTool Process Flow
The difftool performs the following in order:
1. Retrieve metadata from the specified node's metakv (if started via runDiffer.sh)
2. Data Retrieval from source and target buckets via DCP according to the specs' definitions (can press Ctrl-C to move onto next phase)
3. Diff files retrieved from DCP to find differences
4. Verify differences from above using async Get (verifyDiffKeys) to rule out transitional mutations

## Output
Results can be viewed as JSON summary files under `outputs/mutationDiff`:
```
~/xdcrDiffer/outputs/mutationDiff$ ls
diffKeysWithError       mutationDiffColIdMapping    mutationDiffDetails

~/xdcrDiffer/outputs/mutationDiff$ jsonpp mutationDiffDetails  | head
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
~/xdcrDiffer$ find . -name diffTool_manifest
./outputs/target/diffTool_manifest
./outputs/source/diffTool_manifest
```

### Collection Mapping
The xdcrDiffer is going to compile various collection-to-collection mapping, and those are recorded as part of the differ log:
```
2021-05-11T17:03:49.564-07:00 INFO GOXDCR.xdcrDiffTool: Replication spec is using implicit mapping
2021-05-11T17:03:49.564-07:00 INFO GOXDCR.xdcrDiffTool: Collection namespace mapping: map[S1.col1:|Scope: S1 Collection: col1|  S1.col2:|Scope: S1 Collection: col2|  _default._default:|Scope: _default Collection: _default| ] idsMap: map[0:[0] 8:[8] 9:[9]]
```

### Collection Migration Debugging
In certain scenarios, collections migration mode could lead to a single document being replicated to two or more target collections.
This is explained in the [official documentation](https://docs.couchbase.com/server/current/learn/clusters-and-availability/xdcr-with-scopes-and-collections.html#migration) page.
The xdcrDiffer can detect when these happen and showcase the information. The following will indicate how to read the output of a differ in this case.

#### How to interpret multi-target migration differ result
1. Refer to the `xdcrDiffer.log`. It shows a specific order of the migration filters that are used. The index is used as the key to interpret the results.
 ```
2023-03-30T14:45:30.512-07:00 INFO GOXDCR.xdcrDiffTool: 0 : type="brewery" -> S3.col3
2023-03-30T14:45:30.512-07:00 INFO GOXDCR.xdcrDiffTool: 1 : (country == "United States" OR country = "Canada") AND type="brewery" -> S3.col1
2023-03-30T14:45:30.512-07:00 INFO GOXDCR.xdcrDiffTool: 2 : country != "United States" AND country != "Canada" AND type="brewery" -> S3.col2
```
2. There is a file called `mutationMigrationDetails`. The file contains a map of `docKey` -> `indexes that match`. For example:
```
$ jsonpp outputs/mutationDiff/mutationMigrationDetails | head -n 30
{
  "21st_amendment_brewery_cafe": [
    0,
    1
  ],
  "357": [
    0,
    2
  ],
  "3_fonteinen_brouwerij_ambachtelijke_geuzestekerij": [
    0,
    2
  ],
  "512_brewing_company": [
    0,
    1
  ],
  ...
```

In the above example, a document named `512_brewing_company` had passed migration filters 0 and 1, and has been replicated to the corresponding target collections of `S3.col3` and `S3.col1`.


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
