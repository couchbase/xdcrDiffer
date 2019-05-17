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
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/metadata_svc"
	xdcrParts "github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_svc"
	"github.com/couchbase/goxdcr/service_def"
	service_def_mock "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/couchbase/goxdcr/service_impl"
	xdcrUtils "github.com/couchbase/goxdcr/utils"
	"github.com/nelio2k/xdcrDiffer/base"
	"github.com/nelio2k/xdcrDiffer/dcp"
	"github.com/nelio2k/xdcrDiffer/differ"
	fdp "github.com/nelio2k/xdcrDiffer/fileDescriptorPool"
	"github.com/nelio2k/xdcrDiffer/utils"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

type diffToolStateType int

const (
	finStateInitial diffToolStateType = iota
	dcpDriving      diffToolStateType = iota
	finStateFinal   diffToolStateType = iota
)

type difftoolState struct {
	state diffToolStateType
	mtx   sync.Mutex
}

type xdcrDiffTool struct {
	utils              xdcrUtils.UtilsIface
	metadataSvc        service_def.MetadataSvc
	remoteClusterSvc   service_def.RemoteClusterSvc
	replicationSpecSvc service_def.ReplicationSpecSvc
	logger             *xdcrLog.CommonLogger
	throughSeqSvc      service_def.ThroughSeqnoTrackerSvc

	// XDCR related
	specifiedRef           *metadata.RemoteClusterReference
	specifiedSpec          *metadata.ReplicationSpecification
	filter                 xdcrParts.FilterIface
	statsMgr               pipeline_svc.StatsMgrIface
	replicationStatus      *pipeline.ReplicationStatus
	targetXdcrTopologyMock *service_def_mock.XDCRCompTopologySvc
	clusterInfoSvc         *service_impl.ClusterInfoSvc

	sourceDcpDriver *dcp.DcpDriver
	targetDcpDriver *dcp.DcpDriver

	curState difftoolState

	// Mocks
	uiLogSvcMock     *service_def_mock.UILogSvc
	xdcrTopologyMock *service_def_mock.XDCRCompTopologySvc
}

func NewDiffTool() *xdcrDiffTool {
	difftool := &xdcrDiffTool{
		utils:                  xdcrUtils.NewUtilities(),
		uiLogSvcMock:           &service_def_mock.UILogSvc{},
		xdcrTopologyMock:       &service_def_mock.XDCRCompTopologySvc{},
		targetXdcrTopologyMock: &service_def_mock.XDCRCompTopologySvc{},
		replicationStatus:      &pipeline.ReplicationStatus{},
	}
	difftool.metadataSvc, _ = metadata_svc.NewMetaKVMetadataSvc(nil, difftool.utils)

	difftool.logger = xdcrLog.NewLogger("xdcrDiffTool", nil)

	difftool.clusterInfoSvc = service_impl.NewClusterInfoSvc(nil, difftool.utils)
	difftool.remoteClusterSvc, _ = metadata_svc.NewRemoteClusterService(difftool.uiLogSvcMock, difftool.metadataSvc, difftool.xdcrTopologyMock,
		difftool.clusterInfoSvc, xdcrLog.DefaultLoggerContext, difftool.utils)

	difftool.replicationSpecSvc, _ = metadata_svc.NewReplicationSpecService(difftool.uiLogSvcMock, difftool.remoteClusterSvc,
		difftool.metadataSvc, difftool.xdcrTopologyMock, difftool.clusterInfoSvc,
		nil, difftool.utils)

	difftool.throughSeqSvc = service_impl.NewThroughSeqnoTrackerSvc(nil)

	// Capture any Ctrl-C for continuing to next steps or cleanup
	go difftool.monitorInterruptSignal()

	difftool.retrieveReplicationSpecInfo()
	return difftool
}

func (difftool *xdcrDiffTool) createFilterIfNecessary() error {
	var ok bool
	var expr string
	if expr, ok = difftool.specifiedSpec.Settings.Values[metadata.FilterExpressionKey].(string); !ok {
		return nil
	}

	var filterVersion xdcrBase.FilterVersionType
	if filterVersion, ok = difftool.specifiedSpec.Settings.Values[metadata.FilterVersionKey].(xdcrBase.FilterVersionType); !ok {
		err := fmt.Errorf("Unable to find filter version given filter expression %v\nsettings:%v\n", expr, difftool.specifiedSpec.Settings)
		return err
	}

	if filterVersion == xdcrBase.FilterVersionKeyOnly {
		expr = xdcrBase.UpgradeFilter(expr)
	}
	difftool.logger.Infof("Found filtering expression: %v\n", expr)

	filter, err := xdcrParts.NewFilter("XDCRDiffToolFilter", expr, difftool.utils)
	difftool.filter = filter
	return err
}

func (difftool *xdcrDiffTool) generateDataFiles() error {
	difftool.logger.Infof("GenerateDataFiles routine started\n")
	defer difftool.logger.Infof("GenerateDataFiles routine completed\n")

	if options.completeByDuration == 0 && !options.completeBySeqno {
		difftool.logger.Infof("completeByDuration is required when completeBySeqno is false\n")
		os.Exit(1)
	}

	difftool.logger.Infof("Tool started\n")

	if err := cleanUpAndSetup(); err != nil {
		difftool.logger.Errorf("Unable to clean and set up directory structure: %v\n", err)
		os.Exit(1)
	}

	errChan := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	var fileDescPool fdp.FdPoolIface
	if options.numberOfFileDesc > 0 {
		fileDescPool = fdp.NewFileDescriptorPool(int(options.numberOfFileDesc))
	}

	if err := difftool.createFilterIfNecessary(); err != nil {
		os.Exit(1)
	}

	difftool.logger.Infof("Starting source dcp clients on %v\n", options.sourceUrl)
	difftool.sourceDcpDriver = startDcpDriver(difftool.logger, base.SourceClusterName, options.sourceUrl, difftool.specifiedSpec.SourceBucketName,
		options.sourceUsername, options.sourcePassword, options.sourceFileDir, options.checkpointFileDir,
		options.oldSourceCheckpointFileName, options.newCheckpointFileName, options.numberOfSourceDcpClients,
		options.numberOfWorkersPerSourceDcpClient, options.numberOfBins, options.sourceDcpHandlerChanSize,
		options.bucketOpTimeout, options.maxNumOfGetStatsRetry, options.getStatsRetryInterval,
		options.getStatsMaxBackoff, options.checkpointInterval, errChan, waitGroup, options.completeBySeqno, fileDescPool, difftool.filter,
		difftool.xdcrTopologyMock, difftool.specifiedSpec, difftool.clusterInfoSvc)

	delayDurationBetweenSourceAndTarget := time.Duration(options.delayBetweenSourceAndTarget) * time.Second
	difftool.logger.Infof("Waiting for %v before starting target dcp clients\n", delayDurationBetweenSourceAndTarget)
	time.Sleep(delayDurationBetweenSourceAndTarget)

	difftool.logger.Infof("Starting target dcp clients\n")
	difftool.targetDcpDriver = startDcpDriver(difftool.logger, base.TargetClusterName, difftool.specifiedRef.HostName_, difftool.specifiedSpec.TargetBucketName,
		difftool.specifiedRef.UserName_, difftool.specifiedRef.Password_, options.targetFileDir, options.checkpointFileDir,
		options.oldTargetCheckpointFileName, options.newCheckpointFileName, options.numberOfTargetDcpClients,
		options.numberOfWorkersPerTargetDcpClient, options.numberOfBins, options.targetDcpHandlerChanSize,
		options.bucketOpTimeout, options.maxNumOfGetStatsRetry, options.getStatsRetryInterval,
		options.getStatsMaxBackoff, options.checkpointInterval, errChan, waitGroup, options.completeBySeqno, fileDescPool, difftool.filter,
		difftool.targetXdcrTopologyMock, difftool.specifiedSpec /*TODO REVERSE*/, difftool.clusterInfoSvc)

	difftool.curState.mtx.Lock()
	difftool.curState.state = dcpDriving
	difftool.curState.mtx.Unlock()

	var err error
	if options.completeBySeqno {
		err = difftool.waitForCompletion(difftool.sourceDcpDriver, difftool.targetDcpDriver, errChan, waitGroup)
	} else {
		err = difftool.waitForDuration(difftool.sourceDcpDriver, difftool.targetDcpDriver, errChan, options.completeByDuration, delayDurationBetweenSourceAndTarget)
	}

	return err
}

func (difftool *xdcrDiffTool) diffDataFiles() error {
	difftool.logger.Infof("DiffDataFiles routine started\n")
	defer difftool.logger.Infof("DiffDataFiles routine completed\n")

	err := os.RemoveAll(options.fileDifferDir)
	if err != nil {
		difftool.logger.Errorf("Error removing fileDifferDir: %v\n", err)
	}
	err = os.MkdirAll(options.fileDifferDir, 0777)
	if err != nil {
		return fmt.Errorf("Error mkdir fileDifferDir: %v\n", err)
	}

	difftoolDriver := differ.NewDifferDriver(options.sourceFileDir, options.targetFileDir, options.fileDifferDir, base.DiffKeysFileName, int(options.numberOfWorkersForFileDiffer), int(options.numberOfBins), int(options.numberOfFileDesc))
	err = difftoolDriver.Run()
	if err != nil {
		difftool.logger.Errorf("Error from diffDataFiles = %v\n", err)
	}

	return err
}

func (difftool *xdcrDiffTool) runMutationDiffer() {
	difftool.logger.Infof("runMutationDiffer started\n")
	defer difftool.logger.Infof("runMutationDiffer completed\n")

	err := os.RemoveAll(options.mutationDifferDir)
	if err != nil {
		difftool.logger.Errorf("Error removing mutationDifferDir: %v\n", err)
	}
	err = os.MkdirAll(options.mutationDifferDir, 0777)
	if err != nil {
		err = fmt.Errorf("Error mkdir mutationDifferDir: %v\n", err)
		return
	}

	mutationDiffer := differ.NewMutationDiffer(options.sourceUrl, difftool.specifiedSpec.SourceBucketName, options.sourceUsername,
		options.sourcePassword, difftool.specifiedRef.HostName_, difftool.specifiedSpec.TargetBucketName, difftool.specifiedRef.UserName_,
		difftool.specifiedRef.Password_, options.fileDifferDir, options.mutationDifferDir, options.inputDiffKeysFileDir,
		int(options.numberOfWorkersForMutationDiffer), int(options.mutationDifferBatchSize), int(options.mutationDifferTimeout),
		int(options.maxNumOfSendBatchRetry), time.Duration(options.sendBatchRetryInterval)*time.Millisecond,
		time.Duration(options.sendBatchMaxBackoff)*time.Second, difftool.logger)
	err = mutationDiffer.Run()
	if err != nil {
		difftool.logger.Errorf("Error from runMutationDiffer = %v\n", err)
	}
}

func startDcpDriver(logger *xdcrLog.CommonLogger, name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName,
	newCheckpointFileName string, numberOfDcpClients, numberOfWorkersPerDcpClient, numberOfBins,
	dcpHandlerChanSize, bucketOpTimeout, maxNumOfGetStatsRetry, getStatsRetryInterval, getStatsMaxBackoff,
	checkpointInterval uint64, errChan chan error, waitGroup *sync.WaitGroup, completeBySeqno bool,
	fdPool fdp.FdPoolIface, filter xdcrParts.FilterIface, xdcrTopology service_def.XDCRCompTopologySvc, spec *metadata.ReplicationSpecification,
	clusterInfoSvc service_def.ClusterInfoSvc) *dcp.DcpDriver {
	waitGroup.Add(1)
	dcpDriver := dcp.NewDcpDriver(logger, name, url, bucketName, userName, password, fileDir, checkpointFileDir, oldCheckpointFileName,
		newCheckpointFileName, int(numberOfDcpClients), int(numberOfWorkersPerDcpClient), int(numberOfBins),
		int(dcpHandlerChanSize), time.Duration(bucketOpTimeout)*time.Second, int(maxNumOfGetStatsRetry),
		time.Duration(getStatsRetryInterval)*time.Second, time.Duration(getStatsMaxBackoff)*time.Second,
		int(checkpointInterval), errChan, waitGroup, completeBySeqno, fdPool, filter, xdcrTopology, spec,
		clusterInfoSvc)
	// dcp driver startup may take some time. Do it asynchronously
	go startDcpDriverAysnc(dcpDriver, errChan, logger)
	return dcpDriver
}

func startDcpDriverAysnc(dcpDriver *dcp.DcpDriver, errChan chan error, logger *xdcrLog.CommonLogger) {
	err := dcpDriver.Start()
	if err != nil {
		logger.Errorf("Error starting dcp driver %v. err=%v\n", dcpDriver.Name, err)
		utils.AddToErrorChan(errChan, err)
	}
}

func (difftool *xdcrDiffTool) waitForCompletion(sourceDcpDriver, targetDcpDriver *dcp.DcpDriver, errChan chan error, waitGroup *sync.WaitGroup) error {
	doneChan := make(chan bool, 1)
	go utils.WaitForWaitGroup(waitGroup, doneChan)

	select {
	case err := <-errChan:
		difftool.logger.Errorf("Stop diff generation due to error from dcp client %v\n", err)
		err1 := sourceDcpDriver.Stop()
		if err1 != nil {
			difftool.logger.Errorf("Error stopping source dcp client. err=%v\n", err1)
		}
		err1 = targetDcpDriver.Stop()
		if err1 != nil {
			difftool.logger.Errorf("Error stopping target dcp client. err=%v\n", err1)
		}
		return err
	case <-doneChan:
		difftool.logger.Infof("Source cluster and target cluster have completed\n")
		return nil
	}

	return nil
}

func (difftool *xdcrDiffTool) waitForDuration(sourceDcpDriver, targetDcpDriver *dcp.DcpDriver, errChan chan error, duration uint64, delayDurationBetweenSourceAndTarget time.Duration) (err error) {
	timer := time.NewTimer(time.Duration(duration) * time.Second)

	select {
	case err = <-errChan:
		difftool.logger.Errorf("Stop diff generation due to error from dcp client %v\n", err)
	case <-timer.C:
		difftool.logger.Infof("Stop diff generation after specified processing duration\n")
	}

	err1 := sourceDcpDriver.Stop()
	if err1 != nil {
		difftool.logger.Errorf("Error stopping source dcp client. err=%v\n", err1)
	}

	time.Sleep(delayDurationBetweenSourceAndTarget)

	err1 = targetDcpDriver.Stop()
	if err1 != nil {
		difftool.logger.Errorf("Error stopping target dcp client. err=%v\n", err1)
	}

	return err
}

func (difftool *xdcrDiffTool) retrieveReplicationSpecInfo() error {
	// CBAUTH has already been setup
	rcMap, err := difftool.remoteClusterSvc.RemoteClusters()
	if err != nil {
		difftool.logger.Errorf("Error retrieving remote clusters: %v\n", err)
		return err
	}

	specMap, err := difftool.replicationSpecSvc.AllReplicationSpecs()
	if err != nil {
		difftool.logger.Errorf("Error retrieving specs: %v\n", err)
	}

	for _, ref := range rcMap {
		if ref.Name_ == options.remoteClusterName {
			difftool.specifiedRef = ref
			break
		}
	}

	for _, spec := range specMap {
		if spec.SourceBucketName == options.sourceBucketName && spec.TargetBucketName == options.targetBucketName {
			difftool.specifiedSpec = spec
			break
		}
	}

	var errStrs []string
	if difftool.specifiedRef == nil {
		errStrs = append(errStrs, fmt.Sprintf("Unable to find Remote cluster %v\n", options.remoteClusterName))
	}
	if difftool.specifiedSpec == nil {
		errStrs = append(errStrs, fmt.Sprintf("Unable to find Replication Spec with source %v target %v\n", options.sourceBucketName, options.targetBucketName))
	}
	if len(errStrs) > 0 {
		err := fmt.Errorf(strings.Join(errStrs, " and "))
		difftool.logger.Errorf(err.Error())
		return err
	}

	difftool.logger.Infof("Found Remote Cluster: %v and Replication Spec: %v\n", difftool.specifiedRef.String(), difftool.specifiedSpec.String())

	return nil
}

func (difftool *xdcrDiffTool) populateTemporarySpecAndRef() {
	difftool.specifiedSpec, _ = metadata.NewReplicationSpecification(options.sourceBucketName, "", /*sourceBucketUUID*/
		"" /*targetClusterUUID*/, options.targetBucketName, "" /*targetBucketUUID*/)

	difftool.specifiedRef, _ = metadata.NewRemoteClusterReference("" /*uuid*/, "" /*name*/, options.targetUrl, options.targetUsername, options.targetPassword,
		false /*demandEncryption*/, "" /*encryptionType*/, nil, nil, nil)

}

func (difftool *xdcrDiffTool) monitorInterruptSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			if sig.String() == "interrupt" {
				difftool.curState.mtx.Lock()
				switch difftool.curState.state {
				case finStateInitial:
					os.Exit(0)
				case dcpDriving:
					difftool.logger.Warnf("Received interrupt. Closing DCP drivers")
					difftool.sourceDcpDriver.Stop()
					difftool.targetDcpDriver.Stop()
					difftool.curState.state = finStateFinal
				case finStateFinal:
					os.Exit(0)
				}
				difftool.curState.mtx.Unlock()
			}
		}
	}()
}

func (d *xdcrDiffTool) setupXDCRCompTopologyMock() {
	d.xdcrTopologyMock.On("MyConnectionStr").Return(options.sourceUrl, nil)
	d.xdcrTopologyMock.On("MyCredentials").Return(options.sourceUsername, options.sourcePassword, xdcrBase.HttpAuthMechPlain, nil, false, nil, nil, nil)
	d.logger.Infof("Local XDCR Topology returns url: %v username: %v pw: %v\n", options.sourceUrl, options.sourceUsername, options.sourcePassword)

	d.targetXdcrTopologyMock.On("MyConnectionStr").Return(d.specifiedRef.HostName(), nil)
	d.targetXdcrTopologyMock.On("MyCredentials").Return(d.specifiedRef.UserName(), d.specifiedRef.Password(), xdcrBase.HttpAuthMechPlain, nil, false, nil, nil, nil)
	d.logger.Infof("Target XDCR Topology returns url: %v username: %v pw: %v\n", d.specifiedRef.HostName(), d.specifiedRef.UserName(), d.specifiedRef.Password())
}
