// Copyright (c) 2013 Couchbase, Inc.
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
	flag.Uint64Var(&options.numberOfWorkers, "numberOfWorkers", 100,
		"number of worker threads")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()
	fmt.Printf("tool started\n")

	errChan := make(chan error, 1)
	doneChan := make(chan bool, 1)
	waitGroup := &sync.WaitGroup{}

	sourceDcpClient, err := startDcpClient(options.sourceUrl, options.sourceBucketName, options.sourceUsername, options.sourcePassword, options.sourceFileDir, options.numberOfWorkers, errChan, waitGroup)
	if err != nil {
		fmt.Printf("Error starting source dcp client. err=%v\n", err)
		// TODO retry?
		os.Exit(1)
	}

	/*
		targetDcpClient, err := startDcpClient(options.targetUrl, options.targetBucketName, options.targetUsername, options.targetPassword, options.targetFileDir, options.numberOfWorkers, errChan, waitGroup)
		if err != nil {
			fmt.Printf("Error starting target dcp client. err=%v\n", err)
			sourceDcpClient.Stop()
			// TODO retry?
			os.Exit(1)
		}
	*/

	go waitForCompletion(waitGroup, doneChan)

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
		fmt.Printf("Exiting tool since client has completed %v\n", err)
	}
}

func startDcpClient(url, bucketName, userName, password, fileDir string, numberOfWorkers uint64, errChan chan error, waitGroup *sync.WaitGroup) (*DcpClient, error) {
	waitGroup.Add(1)
	dcpClient := NewDcpClient(url, bucketName, userName, password, fileDir, int(numberOfWorkers), errChan, waitGroup)
	err := dcpClient.Start()
	if err == nil {
		return dcpClient, nil
	} else {
		return nil, err
	}
}

func waitForCompletion(waitGroup *sync.WaitGroup, doneChan chan bool) {
	waitGroup.Wait()
	doneChan <- true
}
