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
	"flag"
	"fmt"
	"github.com/nelio2k/xdcrDiffer/base"
	"github.com/nelio2k/xdcrDiffer/dcp"
	"github.com/nelio2k/xdcrDiffer/differ"
	fdp "github.com/nelio2k/xdcrDiffer/fileDescriptorPool"
	"github.com/nelio2k/xdcrDiffer/utils"
	"os"
	"sync"
	"time"
)

var done = make(chan bool)

var options struct {
	sourceUrl                         string
	sourceUsername                    string
	sourcePassword                    string
	sourceBucketName                  string
	sourceFileDir                     string
	targetUrl                         string
	targetUsername                    string
	targetPassword                    string
	targetBucketName                  string
	targetFileDir                     string
	numberOfSourceDcpClients          uint64
	numberOfWorkersPerSourceDcpClient uint64
	numberOfTargetDcpClients          uint64
	numberOfWorkersPerTargetDcpClient uint64
	numberOfWorkersForFileDiffer      uint64
	numberOfWorkersForMutationDiffer  uint64
	numberOfBins                      uint64
	numberOfFileDesc                  uint64
	// the duration that the tools should be run, in minutes
	completeByDuration uint64
	// whether tool should complete after processing all mutations at tool start time
	completeBySeqno bool
	// directory for checkpoint files
	checkpointFileDir string
	// name of source cluster checkpoint file to load from when tool starts
	// if not specified, source cluster will start from 0
	oldSourceCheckpointFileName string
	// name of target cluster checkpoint file to load from when tool starts
	// if not specified, target cluster will start from 0
	oldTargetCheckpointFileName string
	// name of new checkpoint file to write to when tool shuts down
	// if not specified, tool will not save checkpoint files
	newCheckpointFileName string
	// directory for storing diffs
	diffFileDir string
	// name of file storing keys to be diffed
	diffKeysFileName string
	// name of file storing keys that encountered errors when being diffed
	diffErrorKeysFileName string
	// size of batch used by mutation differ
	mutationDifferBatchSize uint64
	// timeout, in seconds, used by mutation differ
	mutationDifferTimeout uint64
	// size of source dcp handler channel
	sourceDcpHandlerChanSize uint64
	// size of target dcp handler channel
	targetDcpHandlerChanSize uint64
	// timeout for bucket for stats collection, in seconds
	bucketOpTimeout uint64
	// max number of retry for get stats
	maxNumOfGetStatsRetry uint64
	// max number of retry for send batch
	maxNumOfSendBatchRetry uint64
	// retry interval for get stats, in seconds
	getStatsRetryInterval uint64
	// retry interval for send batch, in milliseconds
	sendBatchRetryInterval uint64
	// max backoff for get stats, in seconds
	getStatsMaxBackoff uint64
	// max backoff for send batch, in seconds
	sendBatchMaxBackoff uint64
	// delay between source cluster start up and target cluster start up, in seconds
	delayBetweenSourceAndTarget uint64
	//interval for periodical checkpointing, in seconds
	// value of 0 indicates no periodical checkpointing
	checkpointInterval uint64
	// whether to run data generation
	runDataGeneration bool
	// whether to run file differ
	runFileDiffer bool
	// whether to verify diff keys through aysnc Get on clusters
	runMutationDiffer bool
}

func argParse() {
	flag.StringVar(&options.sourceUrl, "sourceUrl", "",
		"url for source cluster")
	flag.StringVar(&options.sourceUsername, "sourceUsername", "",
		"username for source cluster")
	flag.StringVar(&options.sourcePassword, "sourcePassword", "",
		"password for source cluster")
	flag.StringVar(&options.sourceBucketName, "sourceBucketName", "",
		"bucket name for source cluster")
	flag.StringVar(&options.sourceFileDir, "sourceFileDir", base.SourceFileDir,
		"directory to store mutations in source cluster")
	flag.StringVar(&options.targetUrl, "targetUrl", "",
		"url for target cluster")
	flag.StringVar(&options.targetUsername, "targetUsername", "",
		"username for target cluster")
	flag.StringVar(&options.targetPassword, "targetPassword", "",
		"password for target cluster")
	flag.StringVar(&options.targetBucketName, "targetBucketName", "",
		"bucket name for target cluster")
	flag.StringVar(&options.targetFileDir, "targetFileDir", base.TargetFileDir,
		"directory to store mutations in target cluster")
	flag.Uint64Var(&options.numberOfSourceDcpClients, "numberOfSourceDcpClients", 4,
		"number of source dcp clients")
	flag.Uint64Var(&options.numberOfWorkersPerSourceDcpClient, "numberOfWorkersPerSourceDcpClient", 256,
		"number of workers for each source dcp client")
	flag.Uint64Var(&options.numberOfTargetDcpClients, "numberOfTargetDcpClients", 4,
		"number of target dcp clients")
	flag.Uint64Var(&options.numberOfWorkersPerTargetDcpClient, "numberOfWorkersPerTargetDcpClient", 256,
		"number of workers for each target dcp client")
	flag.Uint64Var(&options.numberOfWorkersForFileDiffer, "numberOfWorkersForFileDiffer", 30,
		"number of worker threads for file differ ")
	flag.Uint64Var(&options.numberOfWorkersForMutationDiffer, "numberOfWorkersForMutationDiffer", 30,
		"number of worker threads for mutation differ ")
	flag.Uint64Var(&options.numberOfBins, "numberOfBins", 10,
		"number of buckets per vbucket")
	flag.Uint64Var(&options.numberOfFileDesc, "numberOfFileDesc", 500,
		"number of file descriptors")
	flag.Uint64Var(&options.completeByDuration, "completeByDuration", 0,
		"duration that the tool should run")
	flag.BoolVar(&options.completeBySeqno, "completeBySeqno", true,
		"whether tool should automatically complete (after processing all mutations at start time)")
	flag.StringVar(&options.checkpointFileDir, "checkpointFileDir", base.CheckpointFileDir,
		"directory for checkpoint files")
	flag.StringVar(&options.oldSourceCheckpointFileName, "oldSourceCheckpointFileName", "",
		"old source checkpoint file to load from when tool starts")
	flag.StringVar(&options.oldTargetCheckpointFileName, "oldTargetCheckpointFileName", "",
		"old target checkpoint file to load from when tool starts")
	flag.StringVar(&options.newCheckpointFileName, "newCheckpointFileName", "",
		"new checkpoint file to write to when tool shuts down")
	flag.StringVar(&options.diffFileDir, "diffFileDir", base.DiffFileDir,
		" directory for storing diffs")
	flag.StringVar(&options.diffKeysFileName, "diffKeysFileName", base.DiffKeysFileName,
		" name of file for storing keys to be diffed")
	flag.StringVar(&options.diffErrorKeysFileName, "diffErrorKeysFileName", base.DiffErrorKeysFileName,
		" name of file for storing keys to be diffed")
	flag.Uint64Var(&options.mutationDifferBatchSize, "mutationDifferBatchSize", 100,
		"size of batch used by mutation differ")
	flag.Uint64Var(&options.mutationDifferTimeout, "mutationDifferTimeout", 30,
		"timeout, in seconds, used by mutation differ")
	flag.Uint64Var(&options.sourceDcpHandlerChanSize, "sourceDcpHandlerChanSize", base.DcpHandlerChanSize,
		"size of source dcp handler channel")
	flag.Uint64Var(&options.targetDcpHandlerChanSize, "targetDcpHandlerChanSize", base.DcpHandlerChanSize,
		"size of target dcp handler channel")
	flag.Uint64Var(&options.bucketOpTimeout, "bucketOpTimeout", base.BucketOpTimeout,
		" timeout for bucket for stats collection, in seconds")
	flag.Uint64Var(&options.maxNumOfGetStatsRetry, "maxNumOfGetStatsRetry", base.MaxNumOfGetStatsRetry,
		"max number of retry for get stats")
	flag.Uint64Var(&options.maxNumOfSendBatchRetry, "maxNumOfSendBatchRetry", base.MaxNumOfSendBatchRetry,
		"max number of retry for send batch")
	flag.Uint64Var(&options.getStatsRetryInterval, "getStatsRetryInterval", base.GetStatsRetryInterval,
		" retry interval for get stats, in seconds")
	flag.Uint64Var(&options.sendBatchRetryInterval, "sendBatchRetryInterval", base.SendBatchRetryInterval,
		"retry interval for send batch, in milliseconds")
	flag.Uint64Var(&options.getStatsMaxBackoff, "getStatsMaxBackoff", base.GetStatsMaxBackoff,
		"max backoff for get stats, in seconds")
	flag.Uint64Var(&options.sendBatchMaxBackoff, "sendBatchMaxBackoff", base.SendBatchMaxBackoff,
		"max backoff for send batch, in seconds")
	flag.Uint64Var(&options.delayBetweenSourceAndTarget, "delayBetweenSourceAndTarget", base.DelayBetweenSourceAndTarget,
		"delay between source cluster start up and target cluster start up, in seconds")
	flag.Uint64Var(&options.checkpointInterval, "checkpointInterval", base.CheckpointInterval,
		"interval for periodical checkpointing, in seconds")
	flag.BoolVar(&options.runDataGeneration, "runDataGeneration", true,
		" whether to run data generation")
	flag.BoolVar(&options.runFileDiffer, "runFileDiffer", true,
		" whether to file differ")
	flag.BoolVar(&options.runMutationDiffer, "runMutationDiffer", true,
		" whether to verify diff keys through aysnc Get on clusters")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()

	if options.runDataGeneration {
		err := generateDataFiles()
		if err != nil {
			fmt.Printf("Error generating data files. err=%v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("Skipping  generating data files since it has been disabled\n")
	}

	if options.runFileDiffer {
		err := diffDataFiles()
		if err != nil {
			fmt.Printf("Error running file differ. err=%v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("Skipping file differ since it has been disabled\n")
	}

	if options.runMutationDiffer {
		runMutationDiffer()
	} else {
		fmt.Printf("Skipping mutation diff since it has been disabled\n")
	}
}

func cleanUpAndSetup() error {
	err := os.MkdirAll(options.sourceFileDir, 0777)
	if err != nil {
		fmt.Printf("Error mkdir targetFileDir: %v\n", err)
	}
	err = os.MkdirAll(options.targetFileDir, 0777)
	if err != nil {
		fmt.Printf("Error mkdir targetFileDir: %v\n", err)
	}
	err = os.MkdirAll(options.checkpointFileDir, 0777)
	if err != nil {
		// it is ok for checkpoint dir to be existing, since we do not clean it up
		fmt.Printf("Error mkdir checkpointFileDir: %v\n", err)
	}
	return nil
}

func generateDataFiles() error {
	fmt.Printf("GenerateDataFiles routine started\n")
	defer fmt.Printf("GenerateDataFiles routine completed\n")

	if options.completeByDuration == 0 && !options.completeBySeqno {
		fmt.Printf("completeByDuration is required when completeBySeqno is false\n")
		os.Exit(1)
	}

	fmt.Printf("Tool started\n")

	if err := cleanUpAndSetup(); err != nil {
		fmt.Printf("Unable to clean and set up directory structure: %v\n", err)
		os.Exit(1)
	}

	errChan := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	var fileDescPool fdp.FdPoolIface
	if options.numberOfFileDesc > 0 {
		fileDescPool = fdp.NewFileDescriptorPool(int(options.numberOfFileDesc))
	}

	fmt.Printf("Starting source dcp clients\n")
	sourceDcpDriver := startDcpDriver(base.SourceClusterName, options.sourceUrl, options.sourceBucketName,
		options.sourceUsername, options.sourcePassword, options.sourceFileDir, options.checkpointFileDir,
		options.oldSourceCheckpointFileName, options.newCheckpointFileName, options.numberOfSourceDcpClients,
		options.numberOfWorkersPerSourceDcpClient, options.numberOfBins, options.sourceDcpHandlerChanSize,
		options.bucketOpTimeout, options.maxNumOfGetStatsRetry, options.getStatsRetryInterval,
		options.getStatsMaxBackoff, options.checkpointInterval, errChan, waitGroup, options.completeBySeqno, fileDescPool)

	delayDurationBetweenSourceAndTarget := time.Duration(options.delayBetweenSourceAndTarget) * time.Second
	fmt.Printf("Waiting for %v before starting target dcp clients\n", delayDurationBetweenSourceAndTarget)
	time.Sleep(delayDurationBetweenSourceAndTarget)

	fmt.Printf("Starting target dcp clients\n")
	targetDcpDriver := startDcpDriver(base.TargetClusterName, options.targetUrl, options.targetBucketName,
		options.targetUsername, options.targetPassword, options.targetFileDir, options.checkpointFileDir,
		options.oldTargetCheckpointFileName, options.newCheckpointFileName, options.numberOfTargetDcpClients,
		options.numberOfWorkersPerTargetDcpClient, options.numberOfBins, options.targetDcpHandlerChanSize,
		options.bucketOpTimeout, options.maxNumOfGetStatsRetry, options.getStatsRetryInterval,
		options.getStatsMaxBackoff, options.checkpointInterval, errChan, waitGroup, options.completeBySeqno, fileDescPool)

	var err error
	if options.completeBySeqno {
		err = waitForCompletion(sourceDcpDriver, targetDcpDriver, errChan, waitGroup)
	} else {
		err = waitForDuration(sourceDcpDriver, targetDcpDriver, errChan, options.completeByDuration, delayDurationBetweenSourceAndTarget)
	}

	return err
}

func diffDataFiles() error {
	fmt.Printf("DiffDataFiles routine started\n")
	defer fmt.Printf("DiffDataFiles routine completed\n")

	err := os.RemoveAll(options.diffFileDir)
	if err != nil {
		fmt.Printf("Error removing diffFileDir: %v\n", err)
	}
	err = os.MkdirAll(options.diffFileDir, 0777)
	if err != nil {
		return fmt.Errorf("Error mkdir diffFileDir: %v\n", err)
	}

	differDriver := differ.NewDifferDriver(options.sourceFileDir, options.targetFileDir, options.diffFileDir, options.diffKeysFileName, int(options.numberOfWorkersForFileDiffer), int(options.numberOfBins), int(options.numberOfFileDesc))
	err = differDriver.Run()
	if err != nil {
		fmt.Printf("Error from diffDataFiles = %v\n", err)
	}

	return err
}

func runMutationDiffer() {
	fmt.Printf("runMutationDiffer started\n")
	defer fmt.Printf("runMutationDiffer completed\n")

	differ := differ.NewMutationDiffer(options.sourceUrl, options.sourceBucketName, options.sourceUsername,
		options.sourcePassword, options.targetUrl, options.targetBucketName, options.targetUsername,
		options.targetPassword, options.diffFileDir, options.diffKeysFileName, options.diffErrorKeysFileName,
		int(options.numberOfWorkersForMutationDiffer), int(options.mutationDifferBatchSize), int(options.mutationDifferTimeout),
		int(options.maxNumOfSendBatchRetry), time.Duration(options.sendBatchRetryInterval)*time.Millisecond,
		time.Duration(options.sendBatchMaxBackoff)*time.Second)
	err := differ.Run()
	if err != nil {
		fmt.Printf("Error from runMutationDiffer = %v\n", err)
	}
}

func startDcpDriver(name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName,
	newCheckpointFileName string, numberOfDcpClients, numberOfWorkersPerDcpClient, numberOfBins,
	dcpHandlerChanSize, bucketOpTimeout, maxNumOfGetStatsRetry, getStatsRetryInterval, getStatsMaxBackoff,
	checkpointInterval uint64, errChan chan error, waitGroup *sync.WaitGroup, completeBySeqno bool,
	fdPool fdp.FdPoolIface) *dcp.DcpDriver {
	waitGroup.Add(1)
	dcpDriver := dcp.NewDcpDriver(name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName,
		newCheckpointFileName, int(numberOfDcpClients), int(numberOfWorkersPerDcpClient), int(numberOfBins),
		int(dcpHandlerChanSize), time.Duration(bucketOpTimeout)*time.Second, int(maxNumOfGetStatsRetry),
		time.Duration(getStatsRetryInterval)*time.Second, time.Duration(getStatsMaxBackoff)*time.Second,
		int(checkpointInterval), errChan, waitGroup, completeBySeqno, fdPool)
	// dcp driver startup may take some time. Do it asynchronously
	go startDcpDriverAysnc(dcpDriver, errChan)
	return dcpDriver
}

func startDcpDriverAysnc(dcpDriver *dcp.DcpDriver, errChan chan error) {
	err := dcpDriver.Start()
	if err != nil {
		fmt.Printf("Error starting dcp driver %v. err=%v\n", dcpDriver.Name, err)
		utils.AddToErrorChan(errChan, err)
	}
}

func waitForCompletion(sourceDcpDriver, targetDcpDriver *dcp.DcpDriver, errChan chan error, waitGroup *sync.WaitGroup) error {
	doneChan := make(chan bool, 1)
	go utils.WaitForWaitGroup(waitGroup, doneChan)

	select {
	case err := <-errChan:
		fmt.Printf("Stop diff generation due to error from dcp client %v\n", err)
		err1 := sourceDcpDriver.Stop()
		if err1 != nil {
			fmt.Printf("Error stopping source dcp client. err=%v\n", err1)
		}
		err1 = targetDcpDriver.Stop()
		if err1 != nil {
			fmt.Printf("Error stopping target dcp client. err=%v\n", err1)
		}
		return err
	case <-doneChan:
		fmt.Printf("Source cluster and target cluster have completed\n")
		return nil
	}

	return nil
}

func waitForDuration(sourceDcpDriver, targetDcpDriver *dcp.DcpDriver, errChan chan error, duration uint64, delayDurationBetweenSourceAndTarget time.Duration) (err error) {
	timer := time.NewTimer(time.Duration(duration) * time.Second)

	select {
	case err = <-errChan:
		fmt.Printf("Stop diff generation due to error from dcp client %v\n", err)
	case <-timer.C:
		fmt.Printf("Stop diff generation after specified processing duration\n")
	}

	err1 := sourceDcpDriver.Stop()
	if err1 != nil {
		fmt.Printf("Error stopping source dcp client. err=%v\n", err1)
	}

	time.Sleep(delayDurationBetweenSourceAndTarget)

	err1 = targetDcpDriver.Stop()
	if err1 != nil {
		fmt.Printf("Error stopping target dcp client. err=%v\n", err1)
	}

	return err
}
