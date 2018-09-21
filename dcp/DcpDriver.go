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
	fdp "github.com/nelio2k/xdcrDiffer/fileDescriptorPool"
	"github.com/nelio2k/xdcrDiffer/utils"
	"sync"
)

type DcpDriver struct {
	Name              string
	url               string
	bucketName        string
	userName          string
	password          string
	fileDir           string
	errChan           chan error
	waitGroup         *sync.WaitGroup
	childWaitGroup    *sync.WaitGroup
	numberOfClients   int
	numberOfWorkers   int
	numberOfBuckets   int
	cluster           *gocb.Cluster
	checkpointManager *CheckpointManager
	fdPool            fdp.FdPoolIface
	clients           []*DcpClient
	// value = true if processing on the vb has been completed
	vbState map[uint16]bool
	// 0 - not started
	// 1 - started
	// 2 - stopped
	state     DriverState
	stateLock sync.RWMutex
}

type DriverState int

const (
	DriverStateNew     DriverState = iota
	DriverStateStarted DriverState = iota
	DriverStateStopped DriverState = iota
)

func NewDcpDriver(name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName, newCheckpointFileName string, numberOfClients, numberOfWorkers, numberOfBuckets int, errChan chan error, waitGroup *sync.WaitGroup, completeBySeqno bool, fdPool fdp.FdPoolIface) *DcpDriver {
	return &DcpDriver{
		checkpointManager: NewCheckpointManager(checkpointFileDir, oldCheckpointFileName, newCheckpointFileName, name, bucketName, completeBySeqno),
		Name:              name,
		url:               url,
		bucketName:        bucketName,
		userName:          userName,
		password:          password,
		fileDir:           fileDir,
		numberOfClients:   numberOfClients,
		numberOfWorkers:   numberOfWorkers,
		numberOfBuckets:   numberOfBuckets,
		errChan:           errChan,
		waitGroup:         waitGroup,
		vbState:           make(map[uint16]bool),
		fdPool:            fdPool,
		state:             DriverStateNew,
	}
}

func (d *DcpDriver) Start() error {
	err := d.initializeAndStartCheckpointManager()
	if err != nil {
		fmt.Printf("%v error starting checkpoint manager. err=%v\n", d.Name, err)
		return err
	}

	fmt.Printf("%v started checkpoint manager.\n", d.Name)

	err = d.initializeDcpClients()
	if err != nil {
		fmt.Printf("%v error initializing dcp clients. err=%v\n", d.Name, err)
		return err
	}

	d.setState(DriverStateStarted)
	return nil
}

func (d *DcpDriver) initializeAndStartCheckpointManager() error {
	err := d.initializeCluster()
	if err != nil {
		fmt.Printf("%v error initializing cluster. err=%v\n", d.Name, err)
		return err
	}

	fmt.Printf("%v initialized cluster for checkpoint manager.\n", d.Name)

	d.checkpointManager.SetCluster(d.cluster)
	return d.checkpointManager.Start()
}

func (d *DcpDriver) initializeCluster() (err error) {
	cluster, err := gocb.Connect(d.url)
	if err != nil {
		fmt.Printf("%v error connecting to cluster %v. err=%v\n", d.Name, d.url, err)
		return
	}
	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: d.userName,
		Password: d.password,
	})

	if err != nil {
		fmt.Printf("%v error authenticating cluster. err=%v\n", d.Name, err)
		return
	}

	d.cluster = cluster
	return nil
}

func (d *DcpDriver) Stop() error {
	d.stateLock.Lock()
	defer d.stateLock.Unlock()

	if d.state != DriverStateStarted {
		fmt.Printf("Skipping stop() because dcp driver is not started or is already stopped\n")
		return nil
	}

	fmt.Printf("Dcp driver %v stopping\n", d.Name)
	defer fmt.Printf("Dcp drive %v stopped\n", d.Name)
	defer d.waitGroup.Done()

	for i, dcpClient := range d.clients {
		err := dcpClient.Stop()
		if err != nil {
			fmt.Printf("Error stopping %vth dcp client. err=%v\n", i, err)
		}
	}

	d.childWaitGroup.Wait()

	err := d.checkpointManager.Stop()
	if err != nil {
		fmt.Printf("%v error stopping checkpoint manager. err=%v\n", d.Name, err)
	}

	d.state = DriverStateStopped

	return nil
}

func (d *DcpDriver) initializeDcpClients() error {
	loadDistribution := utils.BalanceLoad(d.numberOfClients, base.NumberOfVbuckets)
	d.clients = make([]*DcpClient, d.numberOfClients)
	d.childWaitGroup = &sync.WaitGroup{}
	for i := 0; i < d.numberOfClients; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = uint16(j)
		}

		d.childWaitGroup.Add(1)
		dcpClient := NewDcpClient(d, i, vbList, d.childWaitGroup)
		d.clients[i] = dcpClient

		err := dcpClient.Start()
		if err != nil {
			fmt.Printf("%v error starting dcp client. err=%v\n", d.Name, err)
			return err
		}
		fmt.Printf("%v started dcp client %v\n", d.Name, i)
	}
	return nil
}

func (d *DcpDriver) getState() DriverState {
	d.stateLock.RLock()
	defer d.stateLock.RUnlock()
	return d.state
}

func (d *DcpDriver) setState(state DriverState) {
	d.stateLock.Lock()
	defer d.stateLock.Unlock()
	d.state = state
}

func (d *DcpDriver) reportError(err error) {
	// avoid printing spurious errors if we are stopping
	if d.getState() != DriverStateStopped {
		fmt.Printf("%s dcp driver encountered error=%v\n", d.Name, err)
	}

	utils.AddToErrorChan(d.errChan, err)
}

func (d *DcpDriver) handleVbucketCompletion(vbno uint16, err error) {
	if err != nil {
		d.reportError(err)
	} else {
		d.stateLock.Lock()
		d.vbState[vbno] = true
		numOfCompletedVb := len(d.vbState)
		d.stateLock.Unlock()

		if numOfCompletedVb == base.NumberOfVbuckets {
			fmt.Printf("all vbuckets have completed for dcp driver %v\n", d.Name)
			d.Stop()
		}
	}
}
