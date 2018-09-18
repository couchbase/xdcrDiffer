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
	"os"
	"sync"
	"time"
)

var done = make(chan bool)

var options struct {
	sourceUrl        string
	sourceUsername   string
	sourcePassword   string
	sourceBucketName string
	sourceFileDir    string
	targetUrl        string
	targetUsername   string
	targetPassword   string
	targetBucketName string
	targetFileDir    string
	numberOfWorkers  uint64
	// the duration that the tools should be run, in minutes
	completeByDuration uint64
	// whether tool should complete after processing all mutations at tool start time
	completeBySeqno bool
	// dir of old checkpoint files to load from when tool starts
	// if not specified, tool will start from 0
	oldCheckpointFileDir string
	// dir of new checkpoint files to write to when tool shuts down
	// if not specified, tool will not save checkpoint files
	newCheckpointFileDir string
}

func argParse() {
	flag.StringVar(&options.sourceUrl, "sourceUrl", "http://localhost:9000",
		"url for source cluster")
	flag.StringVar(&options.sourceUsername, "sourceUsername", "Administrator",
		"username for source cluster")
	flag.StringVar(&options.sourcePassword, "sourcePassword", "welcome",
		"password for source cluster")
	flag.StringVar(&options.sourceBucketName, "sourceBucketName", "default",
		"bucket name for source cluster")
	flag.StringVar(&options.sourceFileDir, "sourceFileDir", "source",
		"directory to store mutations in source cluster")
	flag.StringVar(&options.targetUrl, "targetUrl", "",
		"url for target cluster")
	flag.StringVar(&options.targetUsername, "targetUsername", "",
		"username for target cluster")
	flag.StringVar(&options.targetPassword, "targetPassword", "",
		"password for target cluster")
	flag.StringVar(&options.targetBucketName, "targetBucketName", "default",
		"bucket name for target cluster")
	flag.StringVar(&options.targetFileDir, "targetFileDir", "",
		"directory to store mutations in target cluster")
	flag.Uint64Var(&options.numberOfWorkers, "numberOfWorkers", 10,
		"number of worker threads")
	flag.Uint64Var(&options.completeByDuration, "completeByDuration", 2,
		"duration that the tool should run")
	flag.BoolVar(&options.completeBySeqno, "completeBySeqno", true,
		"whether tool should automatically complete (after processing all mutations at start time)")
	flag.StringVar(&options.oldCheckpointFileDir, "oldCheckpointFileDir", "",
		"dir of old checkpoint files to load from when tool starts")
	flag.StringVar(&options.newCheckpointFileDir, "newCheckpointFileDir", "",
		"dir of new checkpoint files to write to when tool shuts down")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()

	if options.completeByDuration == 0 && !options.completeBySeqno {
		fmt.Printf("completeByDuration is required when completeBySeqno is false\n")
		os.Exit(1)
	}

	fmt.Printf("tool started\n")

	errChan := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	sourceDcpClient, err := startDcpClient("source", options.sourceUrl, options.sourceBucketName, options.sourceUsername, options.sourcePassword, options.sourceFileDir, options.oldCheckpointFileDir, options.newCheckpointFileDir, options.numberOfWorkers, errChan, waitGroup, options.completeBySeqno)
	if err != nil {
		fmt.Printf("Error starting source dcp client. err=%v\n", err)
		// TODO retry?
		os.Exit(1)
	}

	time.Sleep(DelayBetweenSourceAndTarget)

	/*
		targetDcpClient, err := startDcpClient("target", options.targetUrl, options.targetBucketName, options.targetUsername, options.targetPassword, options.targetFileDir, options.oldCheckpointFileDir, options.newCheckpointFileDir, options.numberOfWorkers, errChan, waitGroup, options.completeBySeqno)
		if err != nil {
			fmt.Printf("Error starting target dcp client. err=%v\n", err)
			sourceDcpClient.Stop()
			// TODO retry?
			os.Exit(1)
		}
	*/

	if options.completeBySeqno {
		waitForCompletion(sourceDcpClient, nil /*targetDcpClient*/, errChan, waitGroup)
	} else {
		waitForDuration(sourceDcpClient, nil /*targetDcpClient*/, errChan, options.completeByDuration)
	}
}

func startDcpClient(name, url, bucketName, userName, password, fileDir, oldCheckpointFileDir, newCheckpointFileDir string, numberOfWorkers uint64, errChan chan error, waitGroup *sync.WaitGroup, completeBySeqno bool) (*DcpClient, error) {
	waitGroup.Add(1)
	dcpClient := NewDcpClient(name, url, bucketName, userName, password, fileDir, oldCheckpointFileDir, newCheckpointFileDir, int(numberOfWorkers), errChan, waitGroup, completeBySeqno)
	err := dcpClient.Start()
	if err == nil {
		return dcpClient, nil
	} else {
		return nil, err
	}
}

func waitForCompletion(sourceDcpClient *DcpClient, targetDcpClient *DcpClient, errChan chan error, waitGroup *sync.WaitGroup) {
	doneChan := make(chan bool, 1)
	go waitForWaitGroup(waitGroup, doneChan)

	select {
	case err := <-errChan:
		fmt.Printf("Exiting tool due to error from dcp client %v\n", err)
		err = sourceDcpClient.Stop()
		if err != nil {
			fmt.Printf("Error stopping source dcp client. err=%v\n", err)
		}
		/*err = targetDcpClient.Stop()
		if err != nil {
			fmt.Printf("Error stopping target dcp client. err=%v\n", err)
		}*/
	case <-doneChan:
		fmt.Printf("Source cluster and target cluster have completed\n")
	}
}

func waitForDuration(sourceDcpClient *DcpClient, targetDcpClient *DcpClient, errChan chan error, duration uint64) {
	timer := time.NewTimer(time.Duration(duration) * time.Minute)

	select {
	case err := <-errChan:
		fmt.Printf("Exiting tool due to error from dcp client %v\n", err)
	case <-timer.C:
		fmt.Printf("Exiting tool after specified processing duration\n")
	}

	err := sourceDcpClient.Stop()
	if err != nil {
		fmt.Printf("Error stopping source dcp client. err=%v\n", err)
	}

	time.Sleep(DelayBetweenSourceAndTarget)

	/*err = targetDcpClient.Stop()
	if err != nil {
		fmt.Printf("Error stopping target dcp client. err=%v\n", err)
	}*/
}

func waitForWaitGroup(waitGroup *sync.WaitGroup, doneChan chan bool) {
	waitGroup.Wait()
	close(doneChan)
}
