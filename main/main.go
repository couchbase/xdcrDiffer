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
	flag.BoolVar(&options.completeBySeqno, "completeBySeqno", false,
		"whether tool should automatically complete (after processing all mutations at start time)")

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
	doneChan := make(chan bool, 1)

	sourceDcpClient, err := startDcpClient("source", options.sourceUrl, options.sourceBucketName, options.sourceUsername, options.sourcePassword, options.sourceFileDir, options.numberOfWorkers, errChan, doneChan, options.completeBySeqno)
	if err != nil {
		fmt.Printf("Error starting source dcp client. err=%v\n", err)
		// TODO retry?
		os.Exit(1)
	}

	/*
		// target dcp client always has completeBySeqno set to false
		// it is always terminated 2 seconds after source cluster termination
		targetDcpClient, err := startDcpClient("target", options.targetUrl, options.targetBucketName, options.targetUsername, options.targetPassword, options.targetFileDir, options.numberOfWorkers, errChan, doneChan, false)
		if err != nil {
			fmt.Printf("Error starting target dcp client. err=%v\n", err)
			sourceDcpClient.Stop()
			// TODO retry?
			os.Exit(1)
		}
	*/

	if options.completeBySeqno {
		waitForSourceCompletion(sourceDcpClient, nil /*targetDcpClient*/, errChan, doneChan)
	} else {
		waitForDuration(sourceDcpClient, nil /*targetDcpClient*/, errChan, options.completeByDuration)
	}
}

func startDcpClient(name, url, bucketName, userName, password, fileDir string, numberOfWorkers uint64, errChan chan error, doneChan chan bool, completeBySeqno bool) (*DcpClient, error) {
	dcpClient := NewDcpClient(name, url, bucketName, userName, password, fileDir, int(numberOfWorkers), errChan, doneChan, completeBySeqno)
	err := dcpClient.Start()
	if err == nil {
		return dcpClient, nil
	} else {
		return nil, err
	}
}

//wait for source cluster to complete, then terminate target cluster
func waitForSourceCompletion(sourceDcpClient *DcpClient, targetDcpClient *DcpClient, errChan chan error, doneChan chan bool) {
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
		fmt.Printf("Source cluster has completed\n")
		time.Sleep(DelayBetweenSourceAndTarget)
		/*err = targetDcpClient.Stop()
		if err != nil {
			fmt.Printf("Error stopping target dcp client. err=%v\n", err)
		}*/
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
