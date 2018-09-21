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
	// whether to verify diff keys through aysnc Get on clusters
	verifyDiffKeys bool
	// size of batch used by mutation differ
	mutationDifferBatchSize uint64
	// timeout, in seconds, used by mutation differ
	mutationDifferTimeout uint64
	// just run mutation differ and nothing else
	// this may be helpful when everything else succeeded and mutation differ ran into issues in last run
	mutationDifferOnly bool
}

func argParse() {
	flag.StringVar(&options.sourceUrl, "sourceUrl", "http://172.23.97.38:8091",
		"url for source cluster")
	flag.StringVar(&options.sourceUsername, "sourceUsername", "Administrator",
		"username for source cluster")
	flag.StringVar(&options.sourcePassword, "sourcePassword", "password",
		"password for source cluster")
	flag.StringVar(&options.sourceBucketName, "sourceBucketName", "msm",
		"bucket name for source cluster")
	flag.StringVar(&options.sourceFileDir, "sourceFileDir", "source",
		"directory to store mutations in source cluster")
	flag.StringVar(&options.targetUrl, "targetUrl", "http://172.23.97.38:8091",
		"url for target cluster")
	flag.StringVar(&options.targetUsername, "targetUsername", "Administrator",
		"username for target cluster")
	flag.StringVar(&options.targetPassword, "targetPassword", "password",
		"password for target cluster")
	flag.StringVar(&options.targetBucketName, "targetBucketName", "msm",
		"bucket name for target cluster")
	flag.StringVar(&options.targetFileDir, "targetFileDir", "target",
		"directory to store mutations in target cluster")
	flag.Uint64Var(&options.numberOfDcpClients, "numberOfDcpClients", 2,
		"number of dcp clients")
	flag.Uint64Var(&options.numberOfWorkersPerDcpClient, "numberOfWorkersPerDcpClient", 10,
		"number of workers for each dcp client")
	flag.Uint64Var(&options.numberOfWorkersForFileDiffer, "numberOfWorkersForFileDiffer", 100,
		"number of worker threads for file differ ")
	flag.Uint64Var(&options.numberOfWorkersForMutationDiffer, "numberOfWorkersForMutationDiffer", 20,
		"number of worker threads for mutation differ ")
	flag.Uint64Var(&options.numberOfBuckets, "numberOfBuckets", 1,
		"number of buckets per vbucket")
	flag.Uint64Var(&options.numberOfFileDesc, "numberOfFileDesc", 0,
		"number of file descriptors")
	flag.Uint64Var(&options.completeByDuration, "completeByDuration", 3,
		"duration that the tool should run")
	flag.BoolVar(&options.completeBySeqno, "completeBySeqno", false,
		"whether tool should automatically complete (after processing all mutations at start time)")
	flag.StringVar(&options.checkpointFileDir, "checkpointFileDir", "checkpoint",
		"directory for checkpoint files")
	flag.StringVar(&options.oldCheckpointFileName, "oldCheckpointFileName", "3",
		"old checkpoint file to load from when tool starts")
	flag.StringVar(&options.newCheckpointFileName, "newCheckpointFileName", "4",
		"new checkpoint file to write to when tool shuts down")
	flag.StringVar(&options.diffFileDir, "diffFileDir", "diff",
		" directory for storing diffs")
	flag.BoolVar(&options.verifyDiffKeys, "verifyDiffKeys", true,
		" whether to verify diff keys through aysnc Get on clusters")
	flag.Uint64Var(&options.mutationDifferBatchSize, "mutationDifferBatchSize", 500,
		"size of batch used by mutation differ")
	flag.Uint64Var(&options.mutationDifferTimeout, "mutationDifferTimeout", 10,
		"timeout, in seconds, used by mutation differ")
	flag.BoolVar(&options.mutationDifferOnly, "mutationDifferOnly", false,
		"just run mutation differ and nothing else")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()

	if options.mutationDifferOnly {
		verifyDiffKeysByGet()
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

		generateDataFiles()

		diffDataFiles()

		if options.verifyDiffKeys {
			verifyDiffKeysByGet()
		} else {
			fmt.Printf("Skipping mutation diff since it has been disabled\n")
		}
	}
}

func cleanUpAndSetup() error {
	err := os.RemoveAll(options.sourceFileDir)
	if err != nil {
		fmt.Errorf("Error removing sourceFileDir: %v\n", err)
		return err
	}
	err = os.RemoveAll(options.targetFileDir)
	if err != nil {
		fmt.Errorf("Error removing targetFileDir: %v\n", err)
		return err
	}
	err = os.RemoveAll(options.diffFileDir)
	if err != nil {
		fmt.Errorf("Error removing diffFileDir: %v\n", err)
		return err
	}
	err = os.MkdirAll(options.sourceFileDir, 0777)
	if err != nil {
		fmt.Errorf("Error mkdir targetFileDir: %v\n", err)
		return err
	}
	err = os.MkdirAll(options.targetFileDir, 0777)
	if err != nil {
		fmt.Errorf("Error mkdir targetFileDir: %v\n", err)
		return err
	}
	err = os.MkdirAll(options.diffFileDir, 0777)
	if err != nil {
		fmt.Errorf("Error mkdir diffFileDir: %v\n", err)
		return err
	}
	return nil
}

func generateDataFiles() {
	fmt.Printf("GenerateDataFiles routine started\n")
	defer fmt.Printf("GenerateDataFiles routine completed\n")

	errChan := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	var fileDescPool fdp.FdPoolIface
	if options.numberOfFileDesc > 0 {
		fileDescPool = fdp.NewFileDescriptorPool(int(options.numberOfFileDesc))
	}

	sourceDcpDriver, err := startDcpDriver(base.SourceClusterName, options.sourceUrl, options.sourceBucketName, options.sourceUsername, options.sourcePassword, options.sourceFileDir, options.checkpointFileDir, options.oldCheckpointFileName, options.newCheckpointFileName, options.numberOfDcpClients, options.numberOfWorkersPerDcpClient, options.numberOfBuckets, errChan, waitGroup, options.completeBySeqno, fileDescPool)
	if err != nil {
		fmt.Printf("Error starting source dcp client. err=%v\n", err)
		// TODO retry?
		os.Exit(1)
	}

	time.Sleep(base.DelayBetweenSourceAndTarget)

	targetDcpDriver, err := startDcpDriver(base.TargetClusterName, options.targetUrl, options.targetBucketName, options.targetUsername, options.targetPassword, options.targetFileDir, options.checkpointFileDir, options.oldCheckpointFileName, options.newCheckpointFileName, options.numberOfDcpClients, options.numberOfWorkersPerDcpClient, options.numberOfBuckets, errChan, waitGroup, options.completeBySeqno, fileDescPool)
	if err != nil {
		fmt.Printf("Error starting target dcp client. err=%v\n", err)
		sourceDcpDriver.Stop()
		// TODO retry?
		os.Exit(1)
	}

	if options.completeBySeqno {
		waitForCompletion(sourceDcpDriver, targetDcpDriver, errChan, waitGroup)
	} else {
		waitForDuration(sourceDcpDriver, targetDcpDriver, errChan, options.completeByDuration)
	}
}

func diffDataFiles() {
	fmt.Printf("DiffDataFiles routine started\n")
	defer fmt.Printf("DiffDataFiles routine completed\n")

	differDriver := differ.NewDifferDriver(options.sourceFileDir, options.targetFileDir, options.diffFileDir, int(options.numberOfWorkersForFileDiffer), int(options.numberOfBuckets), int(options.numberOfFileDesc))
	err := differDriver.Run()
	if err != nil {
		fmt.Printf("Error from diffDataFiles = %v\n", err)
	}
}

func verifyDiffKeysByGet() {
	fmt.Printf("VerifyDiffKeys routine started\n")
	defer fmt.Printf("VerifyDiffKeys routine completed\n")

	differ := differ.NewMutationDiffer(options.sourceUrl, options.sourceBucketName, options.sourceUsername, options.sourcePassword, options.targetUrl, options.targetBucketName, options.targetUsername, options.targetPassword, options.diffFileDir, int(options.numberOfWorkersForMutationDiffer), int(options.mutationDifferBatchSize), int(options.mutationDifferTimeout))
	err := differ.Run()
	if err != nil {
		fmt.Printf("Error from verifyDiffKeys = %v\n", err)
	}
}

func startDcpDriver(name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName, newCheckpointFileName string, numberOfDcpClients, numberOfWorkersPerDcpClient, numberOfBuckets uint64, errChan chan error, waitGroup *sync.WaitGroup, completeBySeqno bool, fdPool fdp.FdPoolIface) (*dcp.DcpDriver, error) {
	waitGroup.Add(1)
	dcpDriver := dcp.NewDcpDriver(name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName, newCheckpointFileName, int(numberOfDcpClients), int(numberOfWorkersPerDcpClient), int(numberOfBuckets), errChan, waitGroup, completeBySeqno, fdPool)
	err := dcpDriver.Start()
	if err == nil {
		return dcpDriver, nil
	} else {
		return nil, err
	}
}

func waitForCompletion(sourceDcpDriver, targetDcpDriver *dcp.DcpDriver, errChan chan error, waitGroup *sync.WaitGroup) {
	doneChan := make(chan bool, 1)
	go utils.WaitForWaitGroup(waitGroup, doneChan)

	select {
	case err := <-errChan:
		fmt.Printf("Exiting tool due to error from dcp client %v\n", err)
		err = sourceDcpDriver.Stop()
		if err != nil {
			fmt.Printf("Error stopping source dcp client. err=%v\n", err)
		}
		err = targetDcpDriver.Stop()
		if err != nil {
			fmt.Printf("Error stopping target dcp client. err=%v\n", err)
		}
	case <-doneChan:
		fmt.Printf("Source cluster and target cluster have completed\n")
	}
}

func waitForDuration(sourceDcpDriver, targetDcpDriver *dcp.DcpDriver, errChan chan error, duration uint64) {
	timer := time.NewTimer(time.Duration(duration) * time.Minute)

	select {
	case err := <-errChan:
		fmt.Printf("Exiting tool due to error from dcp client %v\n", err)
	case <-timer.C:
		fmt.Printf("Exiting tool after specified processing duration\n")
	}

	err := sourceDcpDriver.Stop()
	if err != nil {
		fmt.Printf("Error stopping source dcp client. err=%v\n", err)
	}

	time.Sleep(base.DelayBetweenSourceAndTarget)

	err = targetDcpDriver.Stop()
	if err != nil {
		fmt.Printf("Error stopping target dcp client. err=%v\n", err)
	}
}
