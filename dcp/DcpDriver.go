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
	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/nelio2k/xdcrDiffer/base"
	fdp "github.com/nelio2k/xdcrDiffer/fileDescriptorPool"
	"github.com/nelio2k/xdcrDiffer/utils"
	"strings"
	"sync"
	"time"
)

type DcpDriver struct {
	Name               string
	url                string
	bucketName         string
	userName           string
	password           string
	rbacSupported      bool
	bucketPassword     string
	fileDir            string
	errChan            chan error
	waitGroup          *sync.WaitGroup
	childWaitGroup     *sync.WaitGroup
	numberOfClients    int
	numberOfWorkers    int
	numberOfBins       int
	dcpHandlerChanSize int
	completeBySeqno    bool
	checkpointManager  *CheckpointManager
	startVbtsDoneChan  chan bool
	fdPool             fdp.FdPoolIface
	clients            []*DcpClient
	// value = true if processing on the vb has been completed
	vbStateMap map[uint16]*VBStateWithLock
	// 0 - not started
	// 1 - started
	// 2 - stopped
	state     DriverState
	stateLock sync.RWMutex
	finChan   chan bool
	logger    *xdcrLog.CommonLogger
}

type VBStateWithLock struct {
	vbState VBState
	lock    sync.RWMutex
}

type VBState int

const (
	VBStateNormal       VBState = iota
	VBStateCompleted    VBState = iota
	VBStateStreamClosed VBState = iota
)

type DriverState int

const (
	DriverStateNew     DriverState = iota
	DriverStateStarted DriverState = iota
	DriverStateStopped DriverState = iota
)

func NewDcpDriver(logger *xdcrLog.CommonLogger, name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName,
	newCheckpointFileName string, numberOfClients, numberOfWorkers, numberOfBins, dcpHandlerChanSize int,
	bucketOpTimeout time.Duration, maxNumOfGetStatsRetry int, getStatsRetryInterval, getStatsMaxBackoff time.Duration,
	checkpointInterval int, errChan chan error, waitGroup *sync.WaitGroup, completeBySeqno bool,
	fdPool fdp.FdPoolIface) *DcpDriver {
	dcpDriver := &DcpDriver{
		Name:               name,
		url:                url,
		bucketName:         bucketName,
		userName:           userName,
		password:           password,
		fileDir:            fileDir,
		numberOfClients:    numberOfClients,
		numberOfWorkers:    numberOfWorkers,
		numberOfBins:       numberOfBins,
		dcpHandlerChanSize: dcpHandlerChanSize,
		completeBySeqno:    completeBySeqno,
		errChan:            errChan,
		waitGroup:          waitGroup,
		clients:            make([]*DcpClient, numberOfClients),
		childWaitGroup:     &sync.WaitGroup{},
		vbStateMap:         make(map[uint16]*VBStateWithLock),
		fdPool:             fdPool,
		state:              DriverStateNew,
		finChan:            make(chan bool),
		startVbtsDoneChan:  make(chan bool),
		logger:             logger,
	}

	var vbno uint16
	for vbno = 0; vbno < base.NumberOfVbuckets; vbno++ {
		dcpDriver.vbStateMap[vbno] = &VBStateWithLock{
			vbState: VBStateNormal,
		}
	}

	dcpDriver.checkpointManager = NewCheckpointManager(dcpDriver, checkpointFileDir, oldCheckpointFileName,
		newCheckpointFileName, name, bucketOpTimeout, maxNumOfGetStatsRetry,
		getStatsRetryInterval, getStatsMaxBackoff, checkpointInterval, dcpDriver.startVbtsDoneChan)

	if !strings.HasPrefix(dcpDriver.url, "http") {
		dcpDriver.url = fmt.Sprintf("http://%v", dcpDriver.url)
	}

	return dcpDriver

}

func (d *DcpDriver) Start() error {
	err := d.populateCredentials()
	if err != nil {
		d.logger.Errorf("%v error populating credentials. err=%v\n", d.Name, err)
		return err
	}

	err = d.checkpointManager.Start()
	if err != nil {
		d.logger.Errorf("%v error starting checkpoint manager. err=%v\n", d.Name, err)
		return err
	}

	d.logger.Infof("%v started checkpoint manager.\n", d.Name)

	d.initializeDcpClients()

	err = d.startDcpClients()
	if err != nil {
		d.logger.Errorf("%v error starting dcp clients. err=%v\n", d.Name, err)
		return err
	}

	d.setState(DriverStateStarted)

	go d.checkForCompletion()

	return nil
}

func (d *DcpDriver) checkForCompletion() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var numOfCompletedVb int
			var vbno uint16
			for vbno = 0; vbno < base.NumberOfVbuckets; vbno++ {
				vbState := d.getVbState(vbno)
				if vbState != VBStateNormal {
					numOfCompletedVb++
				}
			}
			if numOfCompletedVb == base.NumberOfVbuckets {
				d.logger.Infof("all vbuckets have completed for dcp driver %v\n", d.Name)
				d.Stop()
				return
			}
		case <-d.finChan:
			return
		}
	}
}

func (d *DcpDriver) populateCredentials() error {
	var err error
	d.rbacSupported, d.bucketPassword, err = utils.GetRBACSupportedAndBucketPassword(d.url, d.bucketName, d.userName, d.password)
	d.logger.Infof("%v rbacSupported=%v url=%v\n", d.Name, d.rbacSupported, d.url)
	return err
}

func (d *DcpDriver) Stop() error {
	d.stateLock.Lock()
	defer d.stateLock.Unlock()

	if d.state == DriverStateStopped {
		d.logger.Infof("Skipping stop() because dcp driver is already stopped\n")
		return nil
	}

	d.logger.Infof("Dcp driver %v stopping\n", d.Name)
	defer d.logger.Infof("Dcp driver %v stopped\n", d.Name)
	defer d.waitGroup.Done()

	close(d.finChan)

	for i, dcpClient := range d.clients {
		if dcpClient != nil {
			err := dcpClient.Stop()
			if err != nil {
				d.logger.Errorf("Error stopping %vth dcp client. err=%v\n", i, err)
			}
		}
	}

	d.childWaitGroup.Wait()

	err := d.checkpointManager.Stop()
	if err != nil {
		d.logger.Errorf("%v error stopping checkpoint manager. err=%v\n", d.Name, err)
	}

	d.state = DriverStateStopped

	return nil
}

func (d *DcpDriver) initializeDcpClients() {
	d.stateLock.Lock()
	defer d.stateLock.Unlock()

	loadDistribution := utils.BalanceLoad(d.numberOfClients, base.NumberOfVbuckets)
	for i := 0; i < d.numberOfClients; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = uint16(j)
		}

		d.childWaitGroup.Add(1)
		dcpClient := NewDcpClient(d, i, vbList, d.childWaitGroup, d.startVbtsDoneChan)
		d.clients[i] = dcpClient
	}
}

func (d *DcpDriver) startDcpClients() error {
	for i, dcpClient := range d.getDcpClients() {
		err := dcpClient.Start()
		if err != nil {
			d.logger.Errorf("%v error starting dcp client. err=%v\n", d.Name, err)
			return err
		}
		d.logger.Infof("%v started dcp client %v\n", d.Name, i)
	}
	return nil
}

func (d *DcpDriver) getDcpClients() []*DcpClient {
	d.stateLock.RLock()
	defer d.stateLock.RUnlock()

	clients := make([]*DcpClient, len(d.clients))
	copy(clients, d.clients)
	return clients
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
		d.logger.Infof("%s dcp driver encountered error=%v\n", d.Name, err)
	}

	utils.AddToErrorChan(d.errChan, err)
}

func (d *DcpDriver) handleVbucketCompletion(vbno uint16, err error, reason string) {
	if err != nil {
		d.reportError(err)
	} else {
		if d.completeBySeqno {
			vbStateWithLock := d.vbStateMap[vbno]
			vbStateWithLock.lock.Lock()
			defer vbStateWithLock.lock.Unlock()
			if vbStateWithLock.vbState == VBStateNormal {
				vbStateWithLock.vbState = VBStateCompleted
			}
		}
	}
}

func (d *DcpDriver) getVbState(vbno uint16) VBState {
	vbStateWithLock := d.vbStateMap[vbno]
	vbStateWithLock.lock.RLock()
	defer vbStateWithLock.lock.RUnlock()
	return vbStateWithLock.vbState
}

func (d *DcpDriver) setVbState(vbno uint16, vbState VBState) {
	vbStateWithLock := d.vbStateMap[vbno]
	vbStateWithLock.lock.Lock()
	defer vbStateWithLock.lock.Unlock()
	vbStateWithLock.vbState = vbState
}
