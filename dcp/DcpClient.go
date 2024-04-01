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
	"crypto/tls"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"xdcrDiffer/base"
	"xdcrDiffer/utils"

	gocb "github.com/couchbase/gocb/v2"
	gocbcore "github.com/couchbase/gocbcore/v10"
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	xdcrUtils "github.com/couchbase/goxdcr/utils"
)

type DcpClient struct {
	Name                string
	dcpDriver           *DcpDriver
	vbList              []uint16
	cluster             *gocb.Cluster
	dcpAgent            *gocbcore.DCPAgent
	waitGroup           *sync.WaitGroup
	dcpHandlers         []*DcpHandler
	vbHandlerMap        map[uint16]*DcpHandler
	numberClosing       uint32
	closeStreamsDoneCh  chan bool
	activeStreams       uint32
	finChan             chan bool
	startVbtsDoneChan   chan bool
	logger              *xdcrLog.CommonLogger
	capabilities        metadata.Capability
	collectionIds       []uint32
	colMigrationFilters []string
	bufferCap           int
	migrationMapping    metadata.CollectionNamespaceMapping

	gocbcoreDcpFeed *GocbcoreDCPFeed
	utils           xdcrUtils.UtilsIface

	kvSSLPortMap xdcrBase.SSLPortMap
	kvVbMap      map[string][]uint16
}

func NewDcpClient(dcpDriver *DcpDriver, i int, vbList []uint16, waitGroup *sync.WaitGroup, startVbtsDoneChan chan bool, capabilities metadata.Capability, collectionIds []uint32, colMigrationFilters []string, utils xdcrUtils.UtilsIface, bufferCap int, migrationMapping metadata.CollectionNamespaceMapping) *DcpClient {
	return &DcpClient{
		Name:                fmt.Sprintf("%v_%v", dcpDriver.Name, i),
		dcpDriver:           dcpDriver,
		vbList:              vbList,
		waitGroup:           waitGroup,
		dcpHandlers:         make([]*DcpHandler, dcpDriver.numberOfWorkers),
		vbHandlerMap:        make(map[uint16]*DcpHandler),
		closeStreamsDoneCh:  make(chan bool),
		finChan:             make(chan bool),
		startVbtsDoneChan:   startVbtsDoneChan,
		logger:              dcpDriver.logger,
		capabilities:        capabilities,
		collectionIds:       collectionIds,
		colMigrationFilters: colMigrationFilters,
		utils:               utils,
		bufferCap:           bufferCap,
		migrationMapping:    migrationMapping,
	}
}

func (c *DcpClient) Start() error {
	c.logger.Infof("Dcp client %v starting\n", c.Name)
	defer c.logger.Infof("Dcp client %v started\n", c.Name)

	err := c.initialize()
	if err != nil {
		return err
	}

	go c.handleDcpStreams()

	return nil
}

func (c *DcpClient) reportActiveStreams() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			activeStreams := atomic.LoadUint32(&c.activeStreams)
			c.logger.Infof("%v active streams=%v\n", c.Name, activeStreams)
			if activeStreams == uint32(len(c.vbList)) {
				c.logger.Infof("%v all streams active. Stop reporting\n", c.Name)
				goto done
			}
		case <-c.finChan:
			goto done
		}
	}
done:
}

func (c *DcpClient) closeCompletedStreams() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, vbno := range c.vbList {
				c.closeStreamIfCompleted(vbno)
			}
		case <-c.finChan:
			goto done
		}
	}
done:
}

func (c *DcpClient) closeStreamIfCompleted(vbno uint16) {
	vbState := c.dcpDriver.getVbState(vbno)
	if vbState == VBStateCompleted {
		err := c.closeStream(vbno)
		if err == nil {
			c.dcpDriver.setVbState(vbno, VBStateStreamClosed)
		}
	}
}

func (c *DcpClient) closeStreamIfOpen(vbno uint16) {
	vbState := c.dcpDriver.getVbState(vbno)
	if vbState != VBStateStreamClosed {
		err := c.closeStream(vbno)
		if err == nil {
			c.dcpDriver.setVbState(vbno, VBStateStreamClosed)
		}
	}

}

func (c *DcpClient) Stop() error {
	c.logger.Infof("Dcp client %v stopping\n", c.Name)
	defer c.logger.Infof("Dcp client %v stopped\n", c.Name)

	defer c.waitGroup.Done()

	close(c.finChan)

	c.numberClosing = uint32(len(c.vbList))
	for _, i := range c.vbList {
		c.closeStreamIfOpen(i)
	}

	c.logger.Infof("Dcp client %v stopping handlers\n", c.Name)
	for _, dcpHandler := range c.dcpHandlers {
		if dcpHandler != nil {
			dcpHandler.Stop()
		}
	}
	c.logger.Infof("Dcp client %v done stopping handlers\n", c.Name)

	return nil
}

func (c *DcpClient) initialize() error {
	err := c.initializeCluster()
	if err != nil {
		c.logger.Errorf("Error initializing cluster %v - %v", c.Name, err)
		return err
	}

	err = c.initializeBucket()
	if err != nil {
		c.logger.Errorf("Error initializing bucket %v - %v", c.Name, err)
		return err
	}

	err = c.initializeDcpHandlers()
	if err != nil {
		c.logger.Errorf("Error initializing DCP Handlers %v - %v", c.Name, err)
		return err
	}

	return nil
}

func (c *DcpClient) initializeCluster() (err error) {
	cluster, err := initializeClusterWithSecurity(c.dcpDriver)
	if err != nil {
		return err
	}

	c.cluster = cluster

	c.kvVbMap, err = initializeKVVBMap(c.dcpDriver)
	if err != nil {
		return err
	}

	if c.dcpDriver.ref.HttpAuthMech() == xdcrBase.HttpAuthMechHttps {
		c.kvSSLPortMap, err = initializeSSLPorts(c.dcpDriver)
		if err != nil {
			return err
		}
	}
	return nil
}

func initializeClusterWithSecurity(dcpDriver *DcpDriver) (*gocb.Cluster, error) {
	clusterOpts := gocb.ClusterOptions{}

	if dcpDriver.ref.HttpAuthMech() == xdcrBase.HttpAuthMechHttps {
		tlsCert := tls.Certificate{Certificate: [][]byte{dcpDriver.ref.Certificates()}}
		clusterOpts.Authenticator = gocb.CertificateAuthenticator{ClientCertificate: &tlsCert}
	} else {
		clusterOpts.Authenticator = gocb.PasswordAuthenticator{
			Username: dcpDriver.ref.UserName(),
			Password: dcpDriver.ref.Password(),
		}
	}

	cluster, err := gocb.Connect(utils.PopulateCCCPConnectString(dcpDriver.url), clusterOpts)
	if err != nil {
		dcpDriver.logger.Errorf("Error connecting to cluster %v. err=%v\n", dcpDriver.url, err)
		return nil, err
	}
	return cluster, nil
}

func (c *DcpClient) initializeBucket() (err error) {
	auth, bucketConnStr, err := initializeBucketWithSecurity(c.dcpDriver, c.kvVbMap, c.kvSSLPortMap, true)
	if err != nil {
		return err
	}

	c.gocbcoreDcpFeed, err = NewGocbcoreDCPFeed(c.Name, []string{bucketConnStr}, c.dcpDriver.bucketName, auth, c.capabilities.HasCollectionSupport())
	return
}

func initializeBucketWithSecurity(dcpDriver *DcpDriver, kvVbMap map[string][]uint16, kvSSLPortMap map[string]uint16, tagPrefix bool) (interface{}, string, error) {
	var auth interface{}
	pwAuth := base.PasswordAuth{
		Username: dcpDriver.ref.UserName(),
		Password: dcpDriver.ref.Password(),
	}

	var bucketConnStr string
	for k, _ := range kvVbMap {
		bucketConnStr = k
		break
	}

	if dcpDriver.ref.HttpAuthMech() == xdcrBase.HttpAuthMechHttps {
		auth = &base.CertificateAuth{
			PasswordAuth:     pwAuth,
			CertificateBytes: dcpDriver.ref.Certificates(),
		}

		sslPort, found := kvSSLPortMap[bucketConnStr]
		if !found {
			return nil, "", fmt.Errorf("Cannot find SSL port for %v in map %v", bucketConnStr, kvSSLPortMap)
		}
		bucketConnStr = xdcrBase.GetHostAddr(xdcrBase.GetHostName(bucketConnStr), sslPort)
		if tagPrefix {
			base.TagCouchbaseSecurePrefix(&bucketConnStr)
		}
	} else {
		auth = &pwAuth
		if tagPrefix {
			bucketConnStr = fmt.Sprintf("%v%v", base.CouchbasePrefix, bucketConnStr)
		}
	}
	return auth, bucketConnStr, nil
}

func (c *DcpClient) initializeDcpHandlers() error {
	loadDistribution := utils.BalanceLoad(c.dcpDriver.numberOfWorkers, len(c.vbList))
	for i := 0; i < c.dcpDriver.numberOfWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = c.vbList[j]
		}

		dcpHandler, err := NewDcpHandler(c, c.dcpDriver.fileDir, i, vbList, c.dcpDriver.numberOfBins,
			c.dcpDriver.dcpHandlerChanSize, c.dcpDriver.fdPool, c.dcpDriver.IncrementDocReceived,
			c.dcpDriver.IncrementSysOrUnsubbedEventReceived, c.colMigrationFilters, c.utils, c.bufferCap,
			c.migrationMapping, &xdcrBase.XattrIterator{})
		if err != nil {
			c.logger.Errorf("Error constructing dcp handler. err=%v\n", err)
			return err
		}

		err = dcpHandler.Start()
		if err != nil {
			c.logger.Errorf("Error starting dcp handler. err=%v\n", err)
			return err
		}

		c.dcpHandlers[i] = dcpHandler

		for j := lowIndex; j < highIndex; j++ {
			c.vbHandlerMap[c.vbList[j]] = dcpHandler
		}
	}
	return nil
}

func (c *DcpClient) handleDcpStreams() {
	// wait for start vbts done signal from checkpoint manager
	select {
	case <-c.startVbtsDoneChan:
	case <-c.finChan:
		return
	}

	err := c.openDcpStreams()
	if err != nil {
		wrappedErr := fmt.Errorf("%v: %v", c.Name, err.Error())
		c.reportError(wrappedErr)
		return
	}

	if c.dcpDriver.completeBySeqno {
		go c.closeCompletedStreams()
	}

	go c.reportActiveStreams()
}

func (c *DcpClient) openDcpStreams() error {
	//randomize to evenly distribute [initial] load to handlers
	vbListCopy := utils.DeepCopyUint16Array(c.vbList)
	utils.ShuffleVbList(vbListCopy)
	for _, vbno := range vbListCopy {
		vbts := c.dcpDriver.checkpointManager.GetStartVBTS(vbno)
		if vbts.NoNeedToStartDcpStream {
			c.dcpDriver.handleVbucketCompletion(vbno, nil, "no mutations to stream")
			continue
		}

		snapshotStartSeqno := vbts.Checkpoint.Seqno
		snapshotEndSeqno := vbts.Checkpoint.Seqno

		if c.dcpAgent == nil {
			c.dcpAgent = c.gocbcoreDcpFeed.dcpAgent
		}

		_, err := c.dcpAgent.OpenStream(vbno, 0, gocbcore.VbUUID(vbts.Checkpoint.Vbuuid), gocbcore.SeqNo(vbts.Checkpoint.Seqno),
			gocbcore.SeqNo(math.MaxUint64 /*vbts.EndSeqno*/), gocbcore.SeqNo(snapshotStartSeqno), gocbcore.SeqNo(snapshotEndSeqno), c.vbHandlerMap[vbno],
			c.getOpenStreamOptions(), c.openStreamFunc)

		if err != nil {
			c.logger.Errorf("err opening dcp stream for vb %v. err=%v\n", vbno, err)
			return err
		}
	}

	return nil
}

func (c *DcpClient) closeStream(vbno uint16) error {
	var err error
	if c.dcpAgent != nil {
		_, err = c.dcpAgent.CloseStream(vbno, gocbcore.CloseStreamOptions{}, c.closeStreamFunc)
		if err != nil {
			c.logger.Errorf("%v error stopping dcp stream for vb %v. err=%v\n", c.Name, vbno, err)
		}
	}
	return err
}

func (c *DcpClient) openStreamFunc(f []gocbcore.FailoverEntry, err error) {
	if err != nil {
		wrappedErr := fmt.Errorf("%v openStreamCallback reported err: %v", c.Name, err)
		c.reportError(wrappedErr)
	} else {
		atomic.AddUint32(&c.activeStreams, 1)
	}
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

func (c *DcpClient) getOpenStreamOptions() (streamOpts gocbcore.OpenStreamOptions) {
	if len(c.collectionIds) > 0 {
		filterOpts := &gocbcore.OpenStreamFilterOptions{CollectionIDs: c.collectionIds}
		streamOpts.FilterOptions = filterOpts
	}
	return
}

func initializeSSLPorts(dcpDriver *DcpDriver) (map[string]uint16, error) {
	var kvSSLPortMap map[string]uint16
	connStr, err := dcpDriver.ref.MyConnectionStr()
	if err != nil {
		return nil, err
	}
	// By default the url passed in should be ns_server
	kvSSLPortMap, err = dcpDriver.utils.GetMemcachedSSLPortMap(connStr, dcpDriver.ref.UserName(),
		dcpDriver.ref.Password(), dcpDriver.ref.HttpAuthMech(), dcpDriver.ref.Certificates(),
		dcpDriver.ref.SANInCertificate(), dcpDriver.ref.ClientCertificate(), dcpDriver.ref.ClientKey(),
		dcpDriver.bucketName, dcpDriver.logger, false)

	if err != nil {
		return nil, fmt.Errorf("getMemcachedSSLPortMap %v", err)
	}
	return kvSSLPortMap, nil
}

func initializeKVVBMap(dcpDriver *DcpDriver) (map[string][]uint16, error) {
	var kvVbMap map[string][]uint16
	connStr, err := dcpDriver.ref.MyConnectionStr()
	if err != nil {
		return nil, err
	}

	_, _, _, _, _, kvVbMap, err = dcpDriver.utils.BucketValidationInfo(connStr, dcpDriver.bucketName, dcpDriver.ref.UserName(),
		dcpDriver.ref.Password(), dcpDriver.ref.HttpAuthMech(), dcpDriver.ref.Certificates(),
		dcpDriver.ref.SANInCertificate(), dcpDriver.ref.ClientCertificate(), dcpDriver.ref.ClientKey(),
		dcpDriver.logger)

	return kvVbMap, nil
}
