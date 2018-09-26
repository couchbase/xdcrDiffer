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
	sourceUrl                        string
	sourceUsername                   string
	sourcePassword                   string
	sourceBucketName                 string
	sourceFileDir                    string
	targetUrl                        string
	targetUsername                   string
	targetPassword                   string
	targetBucketName                 string
	targetFileDir                    string
	numberOfDcpClients               uint64
	numberOfWorkersPerDcpClient      uint64
	numberOfWorkersForFileDiffer     uint64
	numberOfWorkersForMutationDiffer uint64
	numberOfBuckets                  uint64
	numberOfFileDesc                 uint64
	// the duration that the tools should be run, in minutes
	completeByDuration uint64
	// whether tool should complete after processing all mutations at tool start time
	completeBySeqno bool
	// directory for checkpoint files
	checkpointFileDir string
	// name of checkpoint file to load from when tool starts
	// if not specified, tool will start from 0
	oldCheckpointFileName string
	// name of new checkpoint file to write to when tool shuts down
	// if not specified, tool will not save checkpoint files
	newCheckpointFileName string
	// directory for storing diffs
	diffFileDir string
	// name of file storing keys to be diffed
	diffKeysFileName string
	// name of file storing keys that encountered errors when being diffed
	diffErrorKeysFileName string
	// whether to verify diff keys through aysnc Get on clusters
	runMutationDiffer bool
	// size of batch used by mutation differ
	mutationDifferBatchSize uint64
	// timeout, in seconds, used by mutation differ
	mutationDifferTimeout uint64
	// just run mutation differ and nothing else
	// this may be helpful when everything else succeeded and mutation differ ran into issues in last run
	mutationDifferOnly bool
	// size of dcp handler channel
	dcpHandlerChanSize uint64
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
}

func argParse() {
	flag.StringVar(&options.sourceUrl, "sourceUrl", "http://54.90.174.157:8091",
		"url for source cluster")
	flag.StringVar(&options.sourceUsername, "sourceUsername", "Administrator",
		"username for source cluster")
	flag.StringVar(&options.sourcePassword, "sourcePassword", "password",
		"password for source cluster")
	flag.StringVar(&options.sourceBucketName, "sourceBucketName", "default",
		"bucket name for source cluster")
	flag.StringVar(&options.sourceFileDir, "sourceFileDir", base.SourceFileDir,
		"directory to store mutations in source cluster")
	flag.StringVar(&options.targetUrl, "targetUrl", "http://13.56.246.168:8091",
		"url for target cluster")
	flag.StringVar(&options.targetUsername, "targetUsername", "Administrator",
		"username for target cluster")
	flag.StringVar(&options.targetPassword, "targetPassword", "password",
		"password for target cluster")
	flag.StringVar(&options.targetBucketName, "targetBucketName", "target",
		"bucket name for target cluster")
	flag.StringVar(&options.targetFileDir, "targetFileDir", base.TargetFileDir,
		"directory to store mutations in target cluster")
	flag.Uint64Var(&options.numberOfDcpClients, "numberOfDcpClients", 4,
		"number of dcp clients")
	flag.Uint64Var(&options.numberOfWorkersPerDcpClient, "numberOfWorkersPerDcpClient", 256,
		"number of workers for each dcp client")
	flag.Uint64Var(&options.numberOfWorkersForFileDiffer, "numberOfWorkersForFileDiffer", 10,
		"number of worker threads for file differ ")
	flag.Uint64Var(&options.numberOfWorkersForMutationDiffer, "numberOfWorkersForMutationDiffer", 30,
		"number of worker threads for mutation differ ")
	flag.Uint64Var(&options.numberOfBuckets, "numberOfBuckets", 10,
		"number of buckets per vbucket")
	flag.Uint64Var(&options.numberOfFileDesc, "numberOfFileDesc", 1000,
		"number of file descriptors")
	flag.Uint64Var(&options.completeByDuration, "completeByDuration", 60,
		"duration that the tool should run")
	flag.BoolVar(&options.completeBySeqno, "completeBySeqno", false,
		"whether tool should automatically complete (after processing all mutations at start time)")
	flag.StringVar(&options.checkpointFileDir, "checkpointFileDir", base.CheckpointFileDir,
		"directory for checkpoint files")
	flag.StringVar(&options.oldCheckpointFileName, "oldCheckpointFileName", "",
		"old checkpoint file to load from when tool starts")
	flag.StringVar(&options.newCheckpointFileName, "newCheckpointFileName", "",
		"new checkpoint file to write to when tool shuts down")
	flag.StringVar(&options.diffFileDir, "diffFileDir", base.DiffFileDir,
		" directory for storing diffs")
	flag.StringVar(&options.diffKeysFileName, "diffKeysFileName", base.DiffKeysFileName,
		" name of file for storing keys to be diffed")
	flag.StringVar(&options.diffErrorKeysFileName, "diffErrorKeysFileName", base.DiffErrorKeysFileName,
		" name of file for storing keys to be diffed")
	flag.BoolVar(&options.runMutationDiffer, "runMutationDiffer", true,
		" whether to verify diff keys through aysnc Get on clusters")
	flag.Uint64Var(&options.mutationDifferBatchSize, "mutationDifferBatchSize", 100,
		"size of batch used by mutation differ")
	flag.Uint64Var(&options.mutationDifferTimeout, "mutationDifferTimeout", 30,
		"timeout, in seconds, used by mutation differ")
	flag.BoolVar(&options.mutationDifferOnly, "mutationDifferOnly", false,
		"just run mutation differ and nothing else")
	flag.Uint64Var(&options.dcpHandlerChanSize, "dcpHandlerChanSize", base.DcpHandlerChanSize,
		"size of dcp handler channel")
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

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()

	if options.mutationDifferOnly {
		runMutationDiffer()
	} else {

		if options.completeByDuration == 0 && !options.completeBySeqno {
			fmt.Printf("completeByDuration is required when completeBySeqno is false\n")
			os.Exit(1)
		}

		fmt.Printf("Tool started\n")

		if err := cleanUpAndSetup(); err != nil {
			fmt.Printf("Unable to clean and set up directory structure: %v\n", err)
			os.Exit(1)
		}

		err := generateDataFiles()
		if err != nil {
			fmt.Printf("Error generating diff files. err=%v\n", err)
			os.Exit(1)
		}

		diffDataFiles()

		if options.runMutationDiffer {
			runMutationDiffer()
		} else {
			fmt.Printf("Skipping mutation diff since it has been disabled\n")
		}
	}
}

func cleanUpAndSetup() error {
	err := os.RemoveAll(options.sourceFileDir)
	if err != nil {
		return fmt.Errorf("Error removing sourceFileDir: %v\n", err)
	}
	err = os.RemoveAll(options.targetFileDir)
	if err != nil {
		return fmt.Errorf("Error removing targetFileDir: %v\n", err)
	}
	err = os.RemoveAll(options.diffFileDir)
	if err != nil {
		return fmt.Errorf("Error removing diffFileDir: %v\n", err)
	}
	err = os.MkdirAll(options.sourceFileDir, 0777)
	if err != nil {
		return fmt.Errorf("Error mkdir targetFileDir: %v\n", err)
	}
	err = os.MkdirAll(options.targetFileDir, 0777)
	if err != nil {
		return fmt.Errorf("Error mkdir targetFileDir: %v\n", err)
	}
	err = os.MkdirAll(options.diffFileDir, 0777)
	if err != nil {
		return fmt.Errorf("Error mkdir diffFileDir: %v\n", err)
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

	errChan := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	var fileDescPool fdp.FdPoolIface
	if options.numberOfFileDesc > 0 {
		fileDescPool = fdp.NewFileDescriptorPool(int(options.numberOfFileDesc))
	}

	fmt.Printf("Starting source dcp clients\n")
	sourceDcpDriver := startDcpDriver(base.SourceClusterName, options.sourceUrl, options.sourceBucketName,
		options.sourceUsername, options.sourcePassword, options.sourceFileDir, options.checkpointFileDir,
		options.oldCheckpointFileName, options.newCheckpointFileName, options.numberOfDcpClients,
		options.numberOfWorkersPerDcpClient, options.numberOfBuckets, options.dcpHandlerChanSize,
		options.bucketOpTimeout, options.maxNumOfGetStatsRetry, options.getStatsRetryInterval,
		options.getStatsMaxBackoff, errChan, waitGroup, options.completeBySeqno, fileDescPool)

	delayDurationBetweenSourceAndTarget := time.Duration(options.delayBetweenSourceAndTarget) * time.Second
	fmt.Printf("Waiting for %v before starting target dcp clients\n", delayDurationBetweenSourceAndTarget)
	time.Sleep(delayDurationBetweenSourceAndTarget)

	fmt.Printf("Starting target dcp clients\n")
	targetDcpDriver := startDcpDriver(base.TargetClusterName, options.targetUrl, options.targetBucketName,
		options.targetUsername, options.targetPassword, options.targetFileDir, options.checkpointFileDir,
		options.oldCheckpointFileName, options.newCheckpointFileName, options.numberOfDcpClients,
		options.numberOfWorkersPerDcpClient, options.numberOfBuckets, options.dcpHandlerChanSize,
		options.bucketOpTimeout, options.maxNumOfGetStatsRetry, options.getStatsRetryInterval,
		options.getStatsMaxBackoff, errChan, waitGroup, options.completeBySeqno, fileDescPool)

	var err error
	if options.completeBySeqno {
		err = waitForCompletion(sourceDcpDriver, targetDcpDriver, errChan, waitGroup)
	} else {
		err = waitForDuration(sourceDcpDriver, targetDcpDriver, errChan, options.completeByDuration, delayDurationBetweenSourceAndTarget)
	}

	return err
}

func diffDataFiles() {
	fmt.Printf("DiffDataFiles routine started\n")
	defer fmt.Printf("DiffDataFiles routine completed\n")

	differDriver := differ.NewDifferDriver(options.sourceFileDir, options.targetFileDir, options.diffFileDir, options.diffKeysFileName, int(options.numberOfWorkersForFileDiffer), int(options.numberOfBuckets), int(options.numberOfFileDesc))
	err := differDriver.Run()
	if err != nil {
		fmt.Printf("Error from diffDataFiles = %v\n", err)
	}
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
	newCheckpointFileName string, numberOfDcpClients, numberOfWorkersPerDcpClient, numberOfBuckets,
	dcpHandlerChanSize, bucketOpTimeout, maxNumOfGetStatsRetry, getStatsRetryInterval, getStatsMaxBackoff uint64,
	errChan chan error, waitGroup *sync.WaitGroup, completeBySeqno bool, fdPool fdp.FdPoolIface) *dcp.DcpDriver {
	waitGroup.Add(1)
	dcpDriver := dcp.NewDcpDriver(name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName,
		newCheckpointFileName, int(numberOfDcpClients), int(numberOfWorkersPerDcpClient), int(numberOfBuckets),
		int(dcpHandlerChanSize), time.Duration(bucketOpTimeout)*time.Second, int(maxNumOfGetStatsRetry),
		time.Duration(getStatsRetryInterval)*time.Second, time.Duration(getStatsMaxBackoff)*time.Second,
		errChan, waitGroup, completeBySeqno, fdPool)
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
