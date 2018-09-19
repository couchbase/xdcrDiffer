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
	fdp "github.com/nelio2k/xdcrDiffer/fileDescriptorPool"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
	"sync"
)

type DcpClient struct {
	Name              string
	url               string
	bucketName        string
	userName          string
	password          string
	fileDir           string
	errChan           chan error
	waitGroup         *sync.WaitGroup
	numberOfWorkers   int
	numberOfBuckets   int
	cluster           *gocb.Cluster
	bucket            *gocb.StreamingBucket
	dcpHandlers       []*DcpHandler
	vbHandlerMap      map[uint16]*DcpHandler
	checkpointManager *CheckpointManager
	fdPool            *fdp.FdPool
	// value = true if processing on the vb has been completed
	vbState   map[uint16]bool
	stopped   bool
	stateLock sync.RWMutex
}

func NewDcpClient(name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName, newCheckpointFileName string, numberOfWorkers, numberOfBuckets int, errChan chan error, waitGroup *sync.WaitGroup, completeBySeqno bool, fdPool *fdp.FdPool) *DcpClient {
	return &DcpClient{
		checkpointManager: NewCheckpointManager(checkpointFileDir, oldCheckpointFileName, newCheckpointFileName, name, bucketName, completeBySeqno),
		Name:              name,
		url:               url,
		bucketName:        bucketName,
		userName:          userName,
		password:          password,
		fileDir:           fileDir,
		numberOfWorkers:   numberOfWorkers,
		numberOfBuckets:   numberOfBuckets,
		errChan:           errChan,
		waitGroup:         waitGroup,
		dcpHandlers:       make([]*DcpHandler, numberOfWorkers),
		vbHandlerMap:      make(map[uint16]*DcpHandler),
		vbState:           make(map[uint16]bool),
		fdPool:            fdPool,
	}
}

func (c *DcpClient) Start() error {
	fmt.Printf("Dcp client %v starting\n", c.Name)
	defer fmt.Printf("Dcp client %v started\n", c.Name)

	err := c.initialize()
	if err != nil {
		return err
	}

	// checkpointManager needs c.cluster and needs to be initialized after DcpClient initialization
	err = c.initializeAndStartCheckpointManager()
	if err != nil {
		return err
	}

	// openStreams() needs to be called after checkpointManager initialization
	return c.openStreams()
}

func (c *DcpClient) Stop() error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	if c.stopped {
		fmt.Printf("Skipping stop() because client is already stopped\n")
		return nil
	}

	fmt.Printf("Dcp client %v stopping\n", c.Name)
	defer fmt.Printf("Dcp client %v stopped\n", c.Name)

	defer c.waitGroup.Done()

	var err error
	for i := 0; i < NumerOfVbuckets; i++ {
		_, err = c.bucket.IoRouter().CloseStream(uint16(i), c.closeStreamFunc)
		if err != nil {
			fmt.Printf("%v error stopping dcp stream for vb %v. err=%v\n", c.Name, i, err)
		}
	}

	err = c.bucket.IoRouter().Close()
	if err != nil {
		fmt.Printf("%v error closing gocb agent. err=%v\n", c.Name, err)
	}

	fmt.Printf("Dcp client %v stopping handlers")
	for _, dcpHandler := range c.dcpHandlers {
		dcpHandler.Stop()
	}
	fmt.Printf("Dcp client %v done stopping handlers")

	err = c.checkpointManager.Stop()
	if err != nil {
		fmt.Printf("%v error stopping checkpoint manager. err=%v\n", c.Name, err)
	}

	c.stopped = true
	return nil
}

func (c *DcpClient) hasStopped() bool {
	c.stateLock.RLock()
	defer c.stateLock.RUnlock()
	return c.stopped
}

func (c *DcpClient) initialize() error {
	err := c.initializeCluster()
	if err != nil {
		return err
	}

	err = c.initializeBucket()
	if err != nil {
		return err
	}

	return c.initializeDcpHandlers()

}

func (c *DcpClient) initializeAndStartCheckpointManager() error {
	c.checkpointManager.SetCluster(c.cluster)
	return c.checkpointManager.Start()
}

func (c *DcpClient) initializeCluster() (err error) {
	cluster, err := gocb.Connect(c.url)
	if err != nil {
		fmt.Printf("Error connecting to cluster %v. err=%v\n", c.url, err)
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

		dcpHandler, err := NewDcpHandler(c, c.checkpointManager, c.fileDir, i, vbList, c.numberOfBuckets, c.fdPool)
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
		vbts := c.checkpointManager.GetStartVBTS(vbno)

		_, err := c.bucket.IoRouter().OpenStream(vbno, 0, gocbcore.VbUuid(vbts.Checkpoint.Vbuuid), gocbcore.SeqNo(vbts.Checkpoint.Seqno), gocbcore.SeqNo(vbts.EndSeqno), gocbcore.SeqNo(vbts.Checkpoint.SnapshotStartSeqno), gocbcore.SeqNo(vbts.Checkpoint.SnapshotEndSeqno), c.vbHandlerMap[vbno], c.openStreamFunc)
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
	// avoid printing spurious errors if we are stopping
	if !c.hasStopped() {
		fmt.Printf("%s dcp client encountered error=%v\n", c.Name, err)
	}

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
		c.vbState[vbno] = true
		numOfCompletedVb := len(c.vbState)
		c.stateLock.Unlock()

		if numOfCompletedVb == NumerOfVbuckets {
			fmt.Printf("all vbuckets have completed for dcp client %v\n", c.Name)
			c.Stop()
		}
	}
}
