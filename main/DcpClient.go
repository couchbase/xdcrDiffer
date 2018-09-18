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
	"fmt"
	"github.com/couchbase/gocb"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
	"sync"
)

type DcpClient struct {
	name            string
	url             string
	bucketName      string
	userName        string
	password        string
	fileDir         string
	errChan         chan error
	doneChan        chan bool
	numberOfWorkers int
	completeBySeqno bool
	cluster         *gocb.Cluster
	bucket          *gocb.StreamingBucket
	endSeqnoMap     map[uint16]uint64
	dcpHandlers     []*DcpHandler
	vbHandlerMap    map[uint16]*DcpHandler
	// value = true if processing on the vb has been completed
	vbState   map[uint16]bool
	stopped   bool
	stateLock sync.RWMutex
}

func NewDcpClient(name, url, bucketName, userName, password, fileDir string, numberOfWorkers int, errChan chan error, doneChan chan bool, completeBySeqno bool) *DcpClient {
	return &DcpClient{
		name:            name,
		url:             url,
		bucketName:      bucketName,
		userName:        userName,
		password:        password,
		fileDir:         fileDir,
		numberOfWorkers: numberOfWorkers,
		errChan:         errChan,
		doneChan:        doneChan,
		completeBySeqno: completeBySeqno,
		endSeqnoMap:     make(map[uint16]uint64),
		dcpHandlers:     make([]*DcpHandler, numberOfWorkers),
		vbHandlerMap:    make(map[uint16]*DcpHandler),
		vbState:         make(map[uint16]bool),
	}
}

func (c *DcpClient) Start() error {
	fmt.Printf("Dcp client %v starting\n", c.name)
	defer fmt.Printf("Dcp client %v started\n", c.name)

	err := c.initialize()
	if err != nil {
		return err
	}

	return c.openStreams()
}

func (c *DcpClient) Stop() error {
	fmt.Printf("Dcp client %v stopping\n", c.name)
	defer fmt.Printf("Dcp client %v stopped\n", c.name)

	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	if c.stopped {
		fmt.Printf("Skipping stop() because client is alreasy dtopped\n")
		return nil
	}

	for i := 0; i < NumerOfVbuckets; i++ {
		_, err := c.bucket.IoRouter().CloseStream(uint16(i), c.closeStreamFunc)
		if err != nil {
			fmt.Printf("Error stopping dcp stream for vb %v. err=%v\n", i, err)
		}
	}

	err := c.bucket.IoRouter().Close()
	if err != nil {
		fmt.Printf("Error closing gocb agent. err=%v\n", err)
	}

	for _, dcpHandler := range c.dcpHandlers {
		dcpHandler.Stop()
	}

	c.stopped = true
	return nil
}

func (c *DcpClient) initialize() error {
	err := c.initializeCluster()
	if err != nil {
		return err
	}

	if c.completeBySeqno {
		err = c.initializeEndSeqnos()
		if err != nil {
			return err
		}
		fmt.Printf("c.endSeqnoMap=%v\n", c.endSeqnoMap)
	}

	err = c.initializeBucket()
	if err != nil {
		return err
	}

	return c.initializeDcpHandlers()

}

func (c *DcpClient) initializeCluster() (err error) {
	cluster, err := gocb.Connect(c.url)
	if err != nil {
		fmt.Printf("Error connecting to cluster. err=%v\n", err)
		return
	}
	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: c.userName,
		Password: c.password,
	})

	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	c.cluster = cluster
	return nil
}

func (c *DcpClient) initializeEndSeqnos() (err error) {
	statsBucket, err := c.cluster.OpenBucket(c.bucketName, "" /*password*/)
	if err != nil {
		return
	}

	statsMap, err := statsBucket.Stats(VbucketSeqnoStatName)
	if err != nil {
		return
	}

	return ParseHighSeqnoStat(statsMap, c.endSeqnoMap)
}

func (c *DcpClient) initializeBucket() (err error) {
	bucket, err := c.cluster.OpenStreamingBucket(StreamingBucketName, c.bucketName, "")
	if err != nil {
		fmt.Printf("Error opening streaming bucket. bucket=%v, err=%v\n", c.bucketName, err)
	}

	c.bucket = bucket

	return
}

func (c *DcpClient) initializeDcpHandlers() error {
	loadDistribution := BalanceLoad(c.numberOfWorkers, NumerOfVbuckets)
	for i := 0; i < c.numberOfWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = uint16(j)
		}

		dcpHandler, err := NewDcpHandler(c, c.fileDir, i, vbList)
		if err != nil {
			fmt.Printf("Error constructing dcp handler. err=%v\n", err)
			return err
		}

		err = dcpHandler.Start()
		if err != nil {
			fmt.Printf("Error starting dcp handler. err=%v\n", err)
			return err
		}

		c.dcpHandlers[i] = dcpHandler

		for j := lowIndex; j < highIndex; j++ {
			c.vbHandlerMap[uint16(j)] = dcpHandler
		}
	}
	return nil
}

func (c *DcpClient) openStreams() error {
	var vbno uint16
	for vbno = 0; vbno < NumerOfVbuckets; vbno++ {
		endSeqno := gocbcore.SeqNo(0xFFFFFFFFFFFFFFFF)
		if c.completeBySeqno {
			endSeqno = gocbcore.SeqNo(c.endSeqnoMap[vbno])
		}
		_, err := c.bucket.IoRouter().OpenStream(vbno, 0, 0, 0, endSeqno, 0, 0, c.vbHandlerMap[vbno], c.openStreamFunc)
		if err != nil {
			fmt.Printf("err opening dcp stream for vb %v. err=%v\n", vbno, err)
			return err
		}
	}

	return nil
}

// OpenStreamCallback
func (c *DcpClient) openStreamFunc(f []gocbcore.FailoverEntry, err error) {
	if err != nil {
		c.reportError(err)
	}
}

func (c *DcpClient) reportError(err error) {
	select {
	case c.errChan <- err:
	default:
		// some error already sent to errChan. no op
	}
}

// CloseStreamCallback
func (c *DcpClient) closeStreamFunc(err error) {
}

func (c *DcpClient) handleVbucketCompletion(vbno uint16, err error) {
	if err != nil {
		c.reportError(err)
	} else {
		c.stateLock.Lock()
		defer c.stateLock.Unlock()
		c.vbState[vbno] = true
		if len(c.vbState) == NumerOfVbuckets {
			fmt.Printf("all vbuckets have completed for dcp client %v\n", c.name)
			close(c.doneChan)
		}
	}
}
