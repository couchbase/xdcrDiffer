// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package dcp

import (
	"fmt"
	"github.com/couchbase/gocb"
	"github.com/nelio2k/xdcrDiffer/base"
	"github.com/nelio2k/xdcrDiffer/utils"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
	"sync"
	"sync/atomic"
	"time"
)

type DcpClient struct {
	Name               string
	dcpDriver          *DcpDriver
	vbList             []uint16
	cluster            *gocb.Cluster
	bucket             *gocb.StreamingBucket
	waitGroup          *sync.WaitGroup
	dcpHandlers        []*DcpHandler
	vbHandlerMap       map[uint16]*DcpHandler
	checkpointManager  *CheckpointManager
	numberClosing      uint32
	closeStreamsDoneCh chan bool
	activeStreams      uint32
	finChan            chan bool
}

func NewDcpClient(dcpDriver *DcpDriver, i int, vbList []uint16, waitGroup *sync.WaitGroup) *DcpClient {
	return &DcpClient{
		Name:               fmt.Sprintf("%v_%v", dcpDriver.Name, i),
		dcpDriver:          dcpDriver,
		vbList:             vbList,
		waitGroup:          waitGroup,
		dcpHandlers:        make([]*DcpHandler, dcpDriver.numberOfWorkers),
		vbHandlerMap:       make(map[uint16]*DcpHandler),
		closeStreamsDoneCh: make(chan bool),
		finChan:            make(chan bool),
	}
}

func (c *DcpClient) Start() error {
	fmt.Printf("Dcp client %v starting\n", c.Name)
	defer fmt.Printf("Dcp client %v started\n", c.Name)

	err := c.initialize()
	if err != nil {
		return err
	}

	go c.reportActiveStreams()

	// openStreams() needs to be called after checkpointManager initialization
	return c.openStreams()
}

func (c *DcpClient) reportActiveStreams() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			activeStreams := atomic.LoadUint32(&c.activeStreams)
			fmt.Printf("%v active streams=%v, expected=%v\n", c.Name, activeStreams, len(c.vbList))
			if activeStreams == uint32(len(c.vbList)) {
				fmt.Printf("%v all streams active. Stop reporting\n", c.Name)
				goto done
			}
		case <-c.finChan:
			goto done
		}
	}
done:
}

func (c *DcpClient) Stop() error {
	fmt.Printf("Dcp client %v stopping\n", c.Name)
	defer fmt.Printf("Dcp client %v stopped\n", c.Name)

	defer c.waitGroup.Done()

	c.numberClosing = uint32(len(c.vbList))
	var err error
	for _, i := range c.vbList {
		_, err = c.bucket.IoRouter().CloseStream(uint16(i), c.closeStreamFunc)
		if err != nil {
			fmt.Printf("%v error stopping dcp stream for vb %v. err=%v\n", c.Name, i, err)
		}
	}
	<-c.closeStreamsDoneCh

	// Close Stream should be enough
	//	fmt.Printf("Dcp client %v stopping IoRouter...\n", c.Name)
	//	err = c.bucket.IoRouter().Close()
	//	if err != nil {
	//		fmt.Printf("%v error closing gocb agent. err=%v\n", c.Name, err)
	//	}

	fmt.Printf("Dcp client %v stopping handlers\n", c.Name)
	for _, dcpHandler := range c.dcpHandlers {
		dcpHandler.Stop()
	}
	fmt.Printf("Dcp client %v done stopping handlers\n", c.Name)

	return nil
}

func (c *DcpClient) initialize() error {
	err := c.initializeCluster()
	if err != nil {
		fmt.Println("Error initializing cluster")
		return err
	}

	err = c.initializeBucket()
	if err != nil {
		fmt.Println("Error initializing bucket")
		return err
	}

	err = c.initializeDcpHandlers()
	if err != nil {
		fmt.Println("Error initializing DCP Handlers")
		return err
	}

	return nil
}

func (c *DcpClient) initializeCluster() (err error) {
	cluster, err := gocb.Connect(c.dcpDriver.url)
	if err != nil {
		fmt.Printf("Error connecting to cluster %v. err=%v\n", c.dcpDriver.url, err)
		return
	}
	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: c.dcpDriver.userName,
		Password: c.dcpDriver.password,
	})

	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	c.cluster = cluster
	return nil
}

func (c *DcpClient) initializeBucket() (err error) {
	bucket, err := c.cluster.OpenStreamingBucket(fmt.Sprintf("%v_%v", base.StreamingBucketName, c.Name), c.dcpDriver.bucketName, "")
	if err != nil {
		fmt.Printf("Error opening streaming bucket. bucket=%v, err=%v\n", c.dcpDriver.bucketName, err)
	}

	c.bucket = bucket

	return
}

func (c *DcpClient) initializeDcpHandlers() error {
	loadDistribution := utils.BalanceLoad(c.dcpDriver.numberOfWorkers, base.NumberOfVbuckets)
	for i := 0; i < c.dcpDriver.numberOfWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = uint16(j)
		}

		dcpHandler, err := NewDcpHandler(c, c.dcpDriver.checkpointManager, c.dcpDriver.fileDir, i, vbList, c.dcpDriver.numberOfBuckets, c.dcpDriver.fdPool)
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
	//randomize to evenly distribute [initial] load to handlers
	vbListCopy := utils.DeepCopyUint16Array(c.vbList)
	utils.ShuffleVbList(vbListCopy)
	for _, vbno := range vbListCopy {
		vbts := c.dcpDriver.checkpointManager.GetStartVBTS(vbno)

		openStreamFunc := func(f []gocbcore.FailoverEntry, err error) {
			if err != nil {
				c.reportError(err)
			} else {
				atomic.AddUint32(&c.activeStreams, 1)
			}

		}

		_, err := c.bucket.IoRouter().OpenStream(vbno, 0, gocbcore.VbUuid(vbts.Checkpoint.Vbuuid), gocbcore.SeqNo(vbts.Checkpoint.Seqno), gocbcore.SeqNo(vbts.EndSeqno), gocbcore.SeqNo(vbts.Checkpoint.SnapshotStartSeqno), gocbcore.SeqNo(vbts.Checkpoint.SnapshotEndSeqno), c.vbHandlerMap[vbno], openStreamFunc)
		if err != nil {
			fmt.Printf("err opening dcp stream for vb %v. err=%v\n", vbno, err)
			return err
		}
	}

	return nil
}

func (c *DcpClient) reportError(err error) {
	select {
	case c.dcpDriver.errChan <- err:
	default:
		// some error already sent to errChan. no op
	}
}

// CloseStreamCallback
func (c *DcpClient) closeStreamFunc(err error) {
	// (-1)
	streamsLeft := atomic.AddUint32(&c.numberClosing, ^uint32(0))
	if streamsLeft == 0 {
		c.closeStreamsDoneCh <- true
	}
}
