package difftool

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	filter2 "github.com/couchbase/goxdcr/base/filter"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/utils"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"sync"
	"time"
	base2 "xdcrDiffer/base"
	"xdcrDiffer/dcp"
	"xdcrDiffer/differ"
	"xdcrDiffer/differCommon"
	"xdcrDiffer/fileDescriptorPool"
	utils2 "xdcrDiffer/utils"
)

type DiffToolStateType int

const (
	StateInitial    DiffToolStateType = iota
	StateDcpStarted DiffToolStateType = iota
	StateFinal      DiffToolStateType = iota
)

type DifftoolState struct {
	state DiffToolStateType
	mtx   sync.Mutex
}

type XdcrDiffTool struct {
	*differCommon.XdcrDependencies

	sourceDcpDriver *dcp.DcpDriver
	targetDcpDriver *dcp.DcpDriver

	curState DifftoolState

	legacyMode bool
}

func NewDiffTool(legacyMode bool) (*XdcrDiffTool, error) {
	var err error
	difftool := &XdcrDiffTool{
		legacyMode: legacyMode,
	}

	if !legacyMode {
		difftool.XdcrDependencies, err = differCommon.NewXdcrDependencies()
		if err != nil {
			return nil, err
		}
	}

	return difftool, err
}

func (difftool *XdcrDiffTool) CreateFilter() error {
	var ok bool
	var expr string
	expr, ok = difftool.SpecifiedSpec.Settings.Values[metadata.FilterExpressionKey].(string)
	filterMode := difftool.SpecifiedSpec.Settings.GetExpDelMode()
	if ok && len(expr) > 0 {
		var filterVersion base.FilterVersionType
		if filterVersion, ok = difftool.SpecifiedSpec.Settings.Values[metadata.FilterVersionKey].(base.FilterVersionType); !ok {
			err := fmt.Errorf("Unable to find filter version given filter expression %v\nsettings:%v\n", expr, difftool.SpecifiedSpec.Settings)
			return err
		}

		if filterVersion == base.FilterVersionKeyOnly {
			expr = base.UpgradeFilter(expr)
		}
		difftool.Logger().Infof("Found filtering expression: %v\n", expr)
	}

	filter, err := filter2.NewFilter("XDCRDiffToolFilter", expr, difftool.Utils, filterMode.IsSkipReplicateUncommittedTxnSet())
	difftool.Filter = filter
	return err
}

func (difftool *XdcrDiffTool) GenerateDataFiles() error {
	difftool.Logger().Infof("GenerateDataFiles routine started\n")
	defer difftool.Logger().Infof("GenerateDataFiles routine completed\n")

	if viper.GetUint64(base2.CompleteByDurationKey) == 0 && !viper.GetBool(base2.CompleteBySeqnoKey) {
		difftool.Logger().Infof("completeByDuration is required when completeBySeqno is false\n")
		os.Exit(1)
	}

	errChan := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	var fileDescPool fileDescriptorPool.FdPoolIface
	if viper.GetInt(base2.NumberOfFileDescKey) > 0 {
		fileDescPool = fileDescriptorPool.NewFileDescriptorPool(viper.GetInt(base2.NumberOfFileDescKey))
	}

	if err := difftool.CreateFilter(); err != nil {
		difftool.Logger().Errorf("Error creating filter: %v", err.Error())
		os.Exit(1)
	}

	difftool.sourceDcpDriver = startDcpDriver(difftool.Logger(), base2.SourceClusterName, viper.GetString(base2.SourceUrlKey),
		difftool.SpecifiedSpec.SourceBucketName,
		difftool.SelfRef, viper.GetString(base2.SourceFileDirKey), viper.GetString(base2.CheckpointFileDirKey),
		viper.GetString(base2.OldSourceCheckpointFileNameKey), viper.GetString(base2.NewCheckpointFileNameKey),
		viper.GetUint64(base2.NumberOfSourceDcpClientsKey),
		viper.GetUint64(base2.NumberOfWorkersPerSourceDcpClientKey), viper.GetUint64(base2.NumberOfBinsKey),
		viper.GetUint64(base2.SourceDcpHandlerChanSizeKey),
		viper.GetUint64(base2.BucketOpTimeoutKey), viper.GetUint64(base2.MaxNumOfGetStatsRetryKey),
		viper.GetUint64(base2.GetStatsRetryIntervalKey),
		viper.GetUint64(base2.GetStatsMaxBackoffKey), viper.GetUint64(base2.CheckpointIntervalKey), errChan, waitGroup,
		viper.GetBool(base2.CompleteBySeqnoKey), fileDescPool, difftool.Filter,
		difftool.SrcCapabilities, difftool.SrcCollectionIds, difftool.ColFilterOrderedKeys, difftool.Utils,
		viper.GetInt(base2.BucketBufferCapacityKey))

	delayDurationBetweenSourceAndTarget := time.Duration(viper.GetUint64(base2.DelayBetweenSourceAndTargetKey)) * time.Second
	difftool.Logger().Infof("Waiting for %v before starting target dcp clients\n", delayDurationBetweenSourceAndTarget)
	time.Sleep(delayDurationBetweenSourceAndTarget)

	difftool.Logger().Infof("Starting target dcp clients\n")
	difftool.targetDcpDriver = startDcpDriver(difftool.Logger(), base2.TargetClusterName, difftool.SpecifiedRef.HostName_,
		difftool.SpecifiedSpec.TargetBucketName, difftool.SpecifiedRef,
		viper.GetString(base2.TargetFileDirKey), viper.GetString(base2.CheckpointFileDirKey),
		viper.GetString(base2.OldTargetCheckpointFileNameKey), viper.GetString(base2.NewCheckpointFileNameKey),
		viper.GetUint64(base2.NumberOfTargetDcpClientsKey), viper.GetUint64(base2.NumberOfWorkersPerTargetDcpClientKey),
		viper.GetUint64(base2.NumberOfBinsKey), viper.GetUint64(base2.TargetDcpHandlerChanSizeKey),
		viper.GetUint64(base2.BucketOpTimeoutKey), viper.GetUint64(base2.MaxNumOfGetStatsRetryKey),
		viper.GetUint64(base2.GetStatsRetryIntervalKey), viper.GetUint64(base2.GetStatsMaxBackoffKey),
		viper.GetUint64(base2.CheckpointIntervalKey), errChan, waitGroup,
		viper.GetBool(base2.CompleteBySeqnoKey), fileDescPool, difftool.Filter,
		difftool.TgtCapabilities, difftool.TgtCollectionIds, difftool.ColFilterOrderedKeys, difftool.Utils,
		viper.GetInt(base2.BucketBufferCapacityKey))

	difftool.curState.mtx.Lock()
	difftool.curState.state = StateDcpStarted
	difftool.curState.mtx.Unlock()

	var err error
	if viper.GetBool(base2.CompleteBySeqnoKey) {
		err = difftool.WaitForCompletion(difftool.sourceDcpDriver, difftool.targetDcpDriver, errChan, waitGroup)
	} else {
		err = difftool.WaitForDuration(difftool.sourceDcpDriver, difftool.targetDcpDriver, errChan,
			viper.GetUint64(base2.CompleteByDurationKey), delayDurationBetweenSourceAndTarget)
	}

	return err
}

func (difftool *XdcrDiffTool) DiffDataFiles() error {
	difftool.Logger().Infof("DiffDataFiles routine started\n")
	defer difftool.Logger().Infof("DiffDataFiles routine completed\n")

	err := os.RemoveAll(viper.GetString(base2.FileDifferDirKey))
	if err != nil {
		difftool.Logger().Errorf("Error removing fileDifferDir: %v\n", err)
	}
	err = os.MkdirAll(viper.GetString(base2.FileDifferDirKey), 0777)
	if err != nil {
		return fmt.Errorf("Error mkdir fileDifferDir: %v\n", err)
	}

	difftoolDriver := differ.NewDifferDriver(viper.GetString(base2.SourceFileDirKey),
		viper.GetString(base2.TargetFileDirKey), viper.GetString(base2.FileDifferDirKey),
		base2.DiffKeysFileName, viper.GetInt(base2.NumberOfWorkersPerSourceDcpClientKey),
		viper.GetInt(base2.NumberOfBinsKey), viper.GetInt(base2.NumberOfFileDescKey),
		difftool.SrcToTgtColIdsMap, difftool.ColFilterOrderedKeys, difftool.ColFilterOrderedTargetColId)
	err = difftoolDriver.Run()
	if err != nil {
		difftool.Logger().Errorf("Error from DiffDataFiles = %v\n", err)
	}
	difftoolDriver.MapLock.RLock()
	if difftool.ColFilterOrderedKeys == nil {
		difftool.Logger().Infof("Source vb to item count map: %v", difftoolDriver.SrcVbItemCntMap)
	}
	difftool.Logger().Infof("Target vb to item count map: %v", difftoolDriver.TgtVbItemCntMap)
	difftoolDriver.MapLock.RUnlock()
	if difftool.ColFilterOrderedKeys == nil {
		difftool.Logger().Infof("Source bucket item count including tombstones is %v (excluding %v filtered mutations)", difftoolDriver.SourceItemCount, difftool.sourceDcpDriver.FilteredCount())
	} else {
		difftool.Logger().Infof("Replication is in migration mode from the source bucket")
	}
	difftool.Logger().Infof("Target bucket item count including tombstones is %v (excluding %v filtered mutations)", difftoolDriver.TargetItemCount, difftool.targetDcpDriver.FilteredCount())
	if difftool.ColFilterOrderedKeys == nil && difftoolDriver.SourceItemCount != difftoolDriver.TargetItemCount {
		difftool.Logger().Infof("Here are the vbuckets with different item counts:")
		for vb, c1 := range difftoolDriver.SrcVbItemCntMap {
			c2 := difftoolDriver.TgtVbItemCntMap[vb]
			if c1 != c2 {
				difftool.Logger().Infof("vb:%v source count %v, target count %v", vb, c1, c2)
			}
		}
	}
	return err
}

func (difftool *XdcrDiffTool) RunMutationDiffer() {
	difftool.Logger().Infof("RunMutationDiffer started with compareBody=%v\n", viper.GetBool(base2.CompareBodyKey))
	defer difftool.Logger().Infof("RunMutationDiffer completed\n")

	err := os.RemoveAll(viper.GetString(base2.MutationDifferDirKey))
	if err != nil {
		difftool.Logger().Errorf("Error removing mutationDifferDir: %v\n", err)
	}
	err = os.MkdirAll(viper.GetString(base2.MutationDifferDirKey), 0777)
	if err != nil {
		err = fmt.Errorf("Error mkdir mutationDifferDir: %v\n", err)
		return
	}

	mutationDiffer := differ.NewMutationDiffer(difftool.SpecifiedSpec.SourceBucketName,
		difftool.SelfRef, difftool.SpecifiedSpec.TargetBucketName, difftool.SpecifiedRef,
		viper.GetString(base2.FileDifferDirKey), viper.GetString(base2.MutationDifferDirKey),
		int(viper.GetUint64(base2.NumberOfWorkersForMutationDifferKey)),
		int(viper.GetUint64(base2.MutationDifferBatchSizeKey)), int(viper.GetUint64(base2.MutationDifferTimeoutKey)),
		int(viper.GetUint64(base2.MaxNumOfSendBatchRetryKey)),
		time.Duration(viper.GetUint64(base2.SendBatchRetryIntervalKey))*time.Millisecond,
		time.Duration(viper.GetUint64(base2.SendBatchMaxBackoffKey))*time.Second, viper.GetBool(base2.CompareBodyKey),
		difftool.Logger(), difftool.SrcToTgtColIdsMap, difftool.SrcCapabilities, difftool.TgtCapabilities, difftool.Utils,
		viper.GetInt(base2.MutationRetriesKey), viper.GetInt(base2.MutationRetriesWaitSecsKey))
	err = mutationDiffer.Run()
	if err != nil {
		difftool.Logger().Errorf("Error from RunMutationDiffer = %v\n", err)
	}
}

func startDcpDriver(logger *log.CommonLogger, name, url, bucketName string, ref *metadata.RemoteClusterReference, fileDir, checkpointFileDir, oldCheckpointFileName, newCheckpointFileName string, numberOfDcpClients, numberOfWorkersPerDcpClient, numberOfBins, dcpHandlerChanSize, bucketOpTimeout, maxNumOfGetStatsRetry, getStatsRetryInterval, getStatsMaxBackoff, checkpointInterval uint64, errChan chan error, waitGroup *sync.WaitGroup, completeBySeqno bool, fdPool fileDescriptorPool.FdPoolIface, filter filter2.Filter, capabilities metadata.Capability, collectionIDs []uint32, colMigrationFilters []string, utils utils.UtilsIface, bucketBufferCap int) *dcp.DcpDriver {
	waitGroup.Add(1)
	dcpDriver := dcp.NewDcpDriver(logger, name, url, bucketName, ref, fileDir, checkpointFileDir, oldCheckpointFileName,
		newCheckpointFileName, int(numberOfDcpClients), int(numberOfWorkersPerDcpClient), int(numberOfBins),
		int(dcpHandlerChanSize), time.Duration(bucketOpTimeout)*time.Second, int(maxNumOfGetStatsRetry),
		time.Duration(getStatsRetryInterval)*time.Second, time.Duration(getStatsMaxBackoff)*time.Second,
		int(checkpointInterval), errChan, waitGroup, completeBySeqno, fdPool, filter, capabilities, collectionIDs, colMigrationFilters,
		utils, bucketBufferCap)
	// dcp driver startup may take some time. Do it asynchronously
	go startDcpDriverAysnc(dcpDriver, errChan, logger)
	return dcpDriver
}

func startDcpDriverAysnc(dcpDriver *dcp.DcpDriver, errChan chan error, logger *log.CommonLogger) {
	err := dcpDriver.Start()
	if err != nil {
		logger.Errorf("Error starting dcp driver %v. err=%v\n", dcpDriver.Name, err)
		utils2.AddToErrorChan(errChan, err)
	}
}

func (difftool *XdcrDiffTool) WaitForCompletion(sourceDcpDriver, targetDcpDriver *dcp.DcpDriver, errChan chan error, waitGroup *sync.WaitGroup) error {
	doneChan := make(chan bool, 1)
	go utils2.WaitForWaitGroup(waitGroup, doneChan)

	select {
	case err := <-errChan:
		difftool.Logger().Errorf("Stop diff generation due to error from dcp client %v\n", err)
		err1 := sourceDcpDriver.Stop()
		if err1 != nil {
			difftool.Logger().Errorf("Error stopping source dcp client. err=%v\n", err1)
		}
		err1 = targetDcpDriver.Stop()
		if err1 != nil {
			difftool.Logger().Errorf("Error stopping target dcp client. err=%v\n", err1)
		}
		return err
	case <-doneChan:
		difftool.Logger().Infof("Source cluster and target cluster have completed\n")
		return nil
	}

	return nil
}

func (difftool *XdcrDiffTool) WaitForDuration(sourceDcpDriver, targetDcpDriver *dcp.DcpDriver, errChan chan error, duration uint64, delayDurationBetweenSourceAndTarget time.Duration) (err error) {
	timer := time.NewTimer(time.Duration(duration) * time.Second)

	select {
	case err = <-errChan:
		difftool.Logger().Errorf("Stop diff generation due to error from dcp client %v\n", err)
	case <-timer.C:
		difftool.Logger().Infof("Stop diff generation after specified processing duration\n")
	}

	err1 := sourceDcpDriver.Stop()
	if err1 != nil {
		difftool.Logger().Errorf("Error stopping source dcp client. err=%v\n", err1)
	}

	time.Sleep(delayDurationBetweenSourceAndTarget)

	err1 = targetDcpDriver.Stop()
	if err1 != nil {
		difftool.Logger().Errorf("Error stopping target dcp client. err=%v\n", err1)
	}

	return err
}

func (difftool *XdcrDiffTool) PopulateTemporarySpecAndRef() error {
	var err error
	difftool.SpecifiedSpec, err = metadata.NewReplicationSpecification(viper.GetString(base2.SourceBucketNameKey),
		"" /*sourceBucketUUID*/, "" /*targetClusterUUID*/, viper.GetString(base2.TargetBucketNameKey), "" /*targetBucketUUID*/)
	if err != nil {
		return fmt.Errorf("PopulateTemporarySpecAndRef() - %v", err)
	}

	difftool.SpecifiedRef, err = metadata.NewRemoteClusterReference("" /*uuid*/, viper.GetString(base2.RemoteClusterNameKey), /*name*/
		viper.GetString(base2.TargetUrlKey), viper.GetString(base2.TargetUsernameKey), viper.GetString(base2.TargetPasswordKey),
		"", false, "", nil, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("PopulateTemporarySpecAndRef() - %v", err)
	}

	err = difftool.XdcrDependencies.PopulateSelfRef()
	if err != nil {
		return fmt.Errorf("PopulateTemporarySpecAndRef() - %v", err)
	}
	return err
}

func (difftool *XdcrDiffTool) MonitorInterruptSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for sig := range c {
		if sig.String() == "interrupt" {
			difftool.curState.mtx.Lock()
			switch difftool.curState.state {
			case StateInitial:
				os.Exit(0)
			case StateDcpStarted:
				difftool.Logger().Warnf("Received interrupt. Closing DCP drivers")
				difftool.sourceDcpDriver.Stop()
				difftool.targetDcpDriver.Stop()
				difftool.curState.state = StateFinal
			case StateFinal:
				os.Exit(0)
			}
			difftool.curState.mtx.Unlock()
		}
	}
}

func (difftool *XdcrDiffTool) SetupDirectories() error {
	err := os.MkdirAll(viper.GetString(base2.SourceFileDirKey), 0777)
	if err != nil {
		fmt.Printf("Error mkdir sourceFileDir: %v\n", err)
	}
	err = os.MkdirAll(viper.GetString(base2.TargetFileDirKey), 0777)
	if err != nil {
		fmt.Printf("Error mkdir targetFileDir: %v\n", err)
	}
	err = os.MkdirAll(viper.GetString(base2.CheckpointFileDirKey), 0777)
	if err != nil {
		// it is ok for checkpoint dir to be existing, since we do not clean it up
		fmt.Printf("Error mkdir checkpointFileDir: %v\n", err)
	}
	return nil
}
