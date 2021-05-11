package dcp

import (
	"encoding/json"
	"fmt"
	gocb "github.com/couchbase/gocb"
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/rcrowley/go-metrics"
	"io/ioutil"
	"math"
	"os"
	"sync"
	"time"
	"xdcrDiffer/base"
	"xdcrDiffer/utils"
)

type CheckpointManager struct {
	dcpDriver             *DcpDriver
	clusterName           string
	oldCheckpointFileName string
	newCheckpointFileName string
	cluster               *gocb.Cluster
	startVBTS             map[uint16]*VBTS
	vbuuidMap             map[uint16]uint64
	seqnoMap              map[uint16]*SeqnoWithLock
	snapshots             map[uint16]*Snapshot
	endSeqnoMap           map[uint16]uint64
	filteredCnt           map[uint16]metrics.Counter
	failedFilterCnt       map[uint16]metrics.Counter
	finChan               chan bool
	// channel to signal the completion of start vbts computation
	startVbtsDoneChan     chan bool
	bucketOpTimeout       time.Duration
	maxNumOfGetStatsRetry int
	getStatsRetryInterval time.Duration
	getStatsMaxBackoff    time.Duration
	checkpointInterval    int
	started               bool
	stateLock             sync.RWMutex
	logger                *xdcrLog.CommonLogger
	completeBySeqno       bool
	logOnceCount          uint64
	lastRemainingMap      map[uint16]uint64
}

func NewCheckpointManager(dcpDriver *DcpDriver, checkpointFileDir, oldCheckpointFileName, newCheckpointFileName, clusterName string,
	bucketOpTimeout time.Duration, maxNumOfGetStatsRetry int, getStatsRetryInterval, getStatsMaxBackoff time.Duration,
	checkpointInterval int, startVbtsDoneChan chan bool, logger *xdcrLog.CommonLogger, completeBySeqno bool) *CheckpointManager {
	cm := &CheckpointManager{
		dcpDriver:             dcpDriver,
		clusterName:           clusterName,
		startVBTS:             make(map[uint16]*VBTS),
		seqnoMap:              make(map[uint16]*SeqnoWithLock),
		snapshots:             make(map[uint16]*Snapshot),
		finChan:               make(chan bool),
		endSeqnoMap:           make(map[uint16]uint64),
		filteredCnt:           make(map[uint16]metrics.Counter),
		failedFilterCnt:       make(map[uint16]metrics.Counter),
		bucketOpTimeout:       bucketOpTimeout,
		maxNumOfGetStatsRetry: maxNumOfGetStatsRetry,
		getStatsRetryInterval: getStatsRetryInterval,
		getStatsMaxBackoff:    getStatsMaxBackoff,
		checkpointInterval:    checkpointInterval,
		startVbtsDoneChan:     startVbtsDoneChan,
		logger:                logger,
		completeBySeqno:       completeBySeqno,
	}

	if checkpointFileDir != "" {
		if oldCheckpointFileName != "" {
			cm.oldCheckpointFileName = checkpointFileDir + base.FileDirDelimiter + clusterName + base.FileNameDelimiter + oldCheckpointFileName
		}

		if newCheckpointFileName != "" {
			cm.newCheckpointFileName = checkpointFileDir + base.FileDirDelimiter + clusterName + base.FileNameDelimiter + newCheckpointFileName
		}
	}

	var vbno uint16
	for vbno = 0; vbno < base.NumberOfVbuckets; vbno++ {
		cm.seqnoMap[vbno] = &SeqnoWithLock{}
		cm.snapshots[vbno] = &Snapshot{}
		cm.filteredCnt[vbno] = metrics.NewCounter()
		cm.failedFilterCnt[vbno] = metrics.NewCounter()
	}

	return cm
}

func (cm *CheckpointManager) CloneSeqnoMap() map[uint16]uint64 {
	clonedMap := make(map[uint16]uint64)
	for k, v := range cm.seqnoMap {
		clonedMap[k] = v.getSeqno()
	}
	return clonedMap
}

func (cm *CheckpointManager) OutputEndSeqnoMapDiff() map[uint16]uint64 {
	currentSeqnoMap := cm.CloneSeqnoMap()
	endSeqnoMap := cm.endSeqnoMap
	diffMap := make(map[uint16]uint64)

	for vb, curSeqno := range currentSeqnoMap {
		endSeqno, ok := endSeqnoMap[vb]
		if ok {
			diff := endSeqno - curSeqno
			if diff > 0 {
				diffMap[vb] = diff
			}
		}
	}

	return diffMap
}

func (cm *CheckpointManager) Start() error {
	err := cm.initialize()
	if err != nil {
		return err
	}

	if cm.newCheckpointFileName != "" && cm.checkpointInterval > 0 {
		go cm.periodicalCheckpointing()
	}

	go cm.reportStatus()

	cm.setStarted()

	return nil
}

func (cm *CheckpointManager) setStarted() {
	cm.stateLock.Lock()
	defer cm.stateLock.Unlock()
	cm.started = true
}

func (cm *CheckpointManager) isStarted() bool {
	cm.stateLock.RLock()
	defer cm.stateLock.RUnlock()
	return cm.started
}

func (cm *CheckpointManager) Stop() error {
	cm.logger.Infof("CheckpointManager stopping\n")
	defer cm.logger.Infof("CheckpointManager stopped\n")

	if cm.isStarted() {
		err := cm.SaveCheckpoint()
		if err != nil {
			cm.logger.Errorf("%v error saving checkpoint. err=%v\n", cm.clusterName, err)
		}
	}

	close(cm.finChan)

	return nil
}

func (cm *CheckpointManager) periodicalCheckpointing() {
	cm.logger.Infof("%v starting periodical checkpointing routine\n", cm.clusterName)

	ticker := time.NewTicker(time.Duration(cm.checkpointInterval) * time.Second)
	defer ticker.Stop()

	// periodical checkpointing iteration
	// it is appended to checkpoint file Name to make file Name unique
	iter := 0

	for {
		select {
		case <-ticker.C:
			cm.checkpointOnce(iter)
			iter++
		case <-cm.finChan:
			return
		}
	}
}

func (cm *CheckpointManager) checkpointOnce(iter int) error {
	checkpointFileName := cm.newCheckpointFileName + base.FileNameDelimiter + fmt.Sprintf("%v", iter)
	err := cm.saveCheckpoint(checkpointFileName)
	if err != nil {
		cm.logger.Errorf("%v error saving checkpoint %v. err=%v\n", cm.clusterName, checkpointFileName, err)
	}
	return err
}

func (cm *CheckpointManager) reportStatus() {
	ticker := time.NewTicker(time.Duration(base.StatsReportInterval) * time.Second)
	defer ticker.Stop()

	var prevSum uint64 = math.MaxUint64

	for {
		select {
		case <-ticker.C:
			prevSum = cm.reportStatusOnce(prevSum)
		case <-cm.finChan:
			return
		}
	}
}

func (cm *CheckpointManager) reportStatusOnce(prevSum uint64) uint64 {
	var vbno uint16
	var sum uint64
	var filtered int64
	var failedFilter int64
	cm.logOnceCount++
	for vbno = 0; vbno < base.NumberOfVbuckets; vbno++ {
		sum += cm.seqnoMap[vbno].getSeqno()
		filtered += cm.filteredCnt[vbno].Count()
		failedFilter += cm.failedFilterCnt[vbno].Count()
	}
	if prevSum != math.MaxUint64 {
		cm.logger.Infof("%v %v processed %v mutations, filtered %v mutations, %v failed filtering. processing rate=%v mutation/second\n",
			time.Now(), cm.clusterName, sum, filtered, failedFilter, (sum-prevSum)/base.StatsReportInterval)
	} else {
		cm.logger.Infof("%v %v processed %v mutations, filtered %v mutations, %v failed filtering.\n",
			time.Now(), cm.clusterName, sum, filtered, failedFilter)
	}
	if cm.completeBySeqno && cm.logOnceCount%10 == 0 {
		diffMap := cm.OutputEndSeqnoMapDiff()
		cm.logger.Infof("%v remaining seqnomap: %v\n", cm.clusterName, diffMap)
		var stuckVBs []uint16
		for vb, seqnoLeft := range diffMap {
			if lastSeqnoLeft, ok := cm.lastRemainingMap[vb]; ok && lastSeqnoLeft == seqnoLeft {
				stuckVBs = append(stuckVBs, vb)
			}
		}
		if len(stuckVBs) > 0 {
			cm.logger.Warnf("These VBs have not move since last time: %v", xdcrBase.SortUint16List(stuckVBs))
		}
		cm.lastRemainingMap = diffMap
	}
	return sum
}

func (cm *CheckpointManager) initialize() error {
	err := cm.initializeCluster()
	if err != nil {
		return err
	}

	err = cm.getVbuuidsAndHighSeqnos()
	if err != nil {
		return err
	}

	if cm.completeBySeqno {
		cm.logger.Infof("%v endSeqno map retrieved %v\n", cm.clusterName, cm.endSeqnoMap)
	} else {
		cm.logger.Infof("%v endSeqno map retrieved\n", cm.clusterName)
	}

	return cm.setStartVBTS()
}

func (cm *CheckpointManager) initializeCluster() error {
	cluster, err := gocb.Connect(cm.dcpDriver.url)
	if err != nil {
		cm.logger.Errorf("%v error connecting to cluster %v. err=%v\n", cm.clusterName, cm.dcpDriver.url, err)
		return err
	}

	if cm.dcpDriver.rbacSupported {
		err = cluster.Authenticate(gocb.PasswordAuthenticator{
			Username: cm.dcpDriver.userName,
			Password: cm.dcpDriver.password,
		})

		if err != nil {
			cm.logger.Errorf("%v error authenticating cluster. err=%v\n", cm.clusterName, err)
			return err
		}
	}

	cm.cluster = cluster
	return nil
}

func (cm *CheckpointManager) getVbuuidsAndHighSeqnos() error {
	statsBucket, err := cm.cluster.OpenBucket(cm.dcpDriver.bucketName, cm.dcpDriver.bucketPassword)
	if err != nil {
		cm.logger.Errorf("%v error opening bucket. err=%v\n", cm.clusterName, err)
		return err
	}
	defer statsBucket.Close()

	statsBucket.SetOperationTimeout(cm.bucketOpTimeout)
	statsBucket.SetBulkOperationTimeout(cm.bucketOpTimeout)

	statsMap, err := cm.getStatsWithRetry(statsBucket)
	if err != nil {
		return err
	}

	vbuuidMap := make(map[uint16]uint64)
	endSeqnoMap := make(map[uint16]uint64)
	err = utils.ParseHighSeqnoStat(statsMap, endSeqnoMap, vbuuidMap, true)
	if err != nil {
		return err
	}

	var sum uint64
	for _, seqno := range endSeqnoMap {
		sum += seqno
	}
	cm.logger.Infof("%v total docs=%v\n", cm.clusterName, sum)

	cm.vbuuidMap = vbuuidMap

	if cm.dcpDriver.completeBySeqno {
		cm.endSeqnoMap = endSeqnoMap
		// For end Seqno 0's, mark them as completed
		for vb, seqno := range endSeqnoMap {
			if seqno == 0 {
				cm.dcpDriver.handleVbucketCompletion(vb, nil, "end Seqno reached")
			}
		}
	} else {
		cm.endSeqnoMap = make(map[uint16]uint64)
		// set endSeqno to maxInt
		var vbno uint16
		for vbno = 0; vbno < base.NumberOfVbuckets; vbno++ {
			cm.endSeqnoMap[vbno] = math.MaxUint64
		}
	}

	return nil
}

// get stats is likely to time out. add retry
func (cm *CheckpointManager) getStatsWithRetry(statsBucket *gocb.Bucket) (map[string]map[string]string, error) {
	var statsMap map[string]map[string]string
	var err error
	getStatsFunc := func() error {
		statsMap, err = statsBucket.Stats(base.VbucketSeqnoStatName)
		return err
	}

	opErr := utils.ExponentialBackoffExecutor("getStatsWithRetry", cm.getStatsRetryInterval, cm.maxNumOfGetStatsRetry,
		base.GetStatsBackoffFactor, cm.getStatsMaxBackoff, getStatsFunc)
	if opErr != nil {
		return nil, opErr
	} else {
		return statsMap, nil
	}
}

func (cm *CheckpointManager) setStartVBTS() error {

	var sum uint64 = 0
	var totalFiltered uint64
	var totalFailedFilter uint64

	if cm.oldCheckpointFileName != "" {
		checkpointDoc, err := cm.loadCheckpoints()
		if err != nil {
			return err
		}

		for vbno, checkpoint := range checkpointDoc.Checkpoints {
			cm.startVBTS[vbno] = &VBTS{
				Checkpoint: checkpoint,
				EndSeqno:   cm.endSeqnoMap[vbno],
			}
			if cm.dcpDriver.completeBySeqno && checkpoint.Seqno >= cm.endSeqnoMap[vbno] {
				cm.startVBTS[vbno].NoNeedToStartDcpStream = true
			}

			// update start Seqno as that in checkpoint doc
			cm.seqnoMap[vbno].setSeqno(checkpoint.Seqno)
			sum += checkpoint.Seqno
			totalFiltered += checkpoint.FilteredCnt
			totalFailedFilter += checkpoint.FailedFilterCnt

			// Resume previous counters
			cm.filteredCnt[vbno].Inc(int64(checkpoint.FilteredCnt))
			cm.failedFilterCnt[vbno].Inc(int64(checkpoint.FailedFilterCnt))
		}
	} else {
		var vbno uint16
		for vbno = 0; vbno < base.NumberOfVbuckets; vbno++ {
			// if we are not loading checkpoints, it is ok to leave all fields in Checkpoint with default values, 0
			cm.startVBTS[vbno] = &VBTS{
				Checkpoint: &Checkpoint{},
				EndSeqno:   cm.endSeqnoMap[vbno],
			}
		}
	}

	cm.logger.Infof("%v starting from %v filtered %v unableToFilter %v\n", cm.clusterName, sum, totalFiltered, totalFailedFilter)

	close(cm.startVbtsDoneChan)

	return nil
}

func (cm *CheckpointManager) GetStartVBTS(vbno uint16) *VBTS {
	return cm.startVBTS[vbno]
}

func (cm *CheckpointManager) loadCheckpoints() (*CheckpointDoc, error) {
	checkpointFileBytes, err := ioutil.ReadFile(cm.oldCheckpointFileName)
	if err != nil {
		cm.logger.Errorf("Error opening checkpoint file. err=%v\n", err)
		return nil, err
	}

	checkpointDoc := &CheckpointDoc{}
	err = json.Unmarshal(checkpointFileBytes, checkpointDoc)
	if err != nil {
		cm.logger.Errorf("Error unmarshalling checkpoint file. err=%v\n", err)
		return nil, err
	}

	if len(checkpointDoc.Checkpoints) < base.NumberOfVbuckets {
		return nil, fmt.Errorf("checkpoint file %v has less than 1024 vbuckets.", cm.oldCheckpointFileName)
	}

	return checkpointDoc, nil
}

func (cm *CheckpointManager) SaveCheckpoint() error {
	if cm.newCheckpointFileName == "" {
		// checkpointing disabled
		cm.logger.Infof("Skipping checkpointing for %v since checkpointing has been disabled\n", cm.clusterName)
		return nil
	}
	return cm.saveCheckpoint(cm.newCheckpointFileName)
}

func (cm *CheckpointManager) saveCheckpoint(checkpointFileName string) error {
	cm.logger.Infof("%v starting to save checkpoint %v\n", cm.clusterName, checkpointFileName)
	defer cm.logger.Infof("%v completed saving checkpoint %v\n", cm.clusterName, checkpointFileName)

	// delete existing file if exists
	os.Remove(checkpointFileName)

	checkpointDoc := &CheckpointDoc{
		Checkpoints: make(map[uint16]*Checkpoint),
	}

	var vbno uint16
	var total uint64
	var totalFiltered uint64
	var totalFailedFilter uint64
	for vbno = 0; vbno < base.NumberOfVbuckets; vbno++ {
		vbuuid := cm.vbuuidMap[vbno]
		seqno := cm.seqnoMap[vbno].getSeqno()
		total += seqno
		var snapshotStartSeqno uint64
		var snapshotEndSeqno uint64
		filteredCnt := uint64(cm.filteredCnt[vbno].Count())
		totalFiltered += filteredCnt
		failedFilterCnt := uint64(cm.failedFilterCnt[vbno].Count())
		totalFailedFilter += failedFilterCnt

		curStartVBTS := cm.startVBTS[vbno].Checkpoint
		if seqno != curStartVBTS.Seqno {
			snapshotStartSeqno, snapshotEndSeqno = cm.getSnapshot(vbno)
		} else {
			// if we have not made any progress since start VBTS, use the same snapshotSeqnos as those in start VBTS
			snapshotStartSeqno = curStartVBTS.SnapshotStartSeqno
			snapshotEndSeqno = curStartVBTS.SnapshotEndSeqno
		}
		checkpointDoc.Checkpoints[vbno] = &Checkpoint{
			Vbuuid:             vbuuid,
			Seqno:              seqno,
			SnapshotStartSeqno: snapshotStartSeqno,
			SnapshotEndSeqno:   snapshotEndSeqno,
			FilteredCnt:        filteredCnt,
			FailedFilterCnt:    failedFilterCnt,
		}
	}

	value, err := json.Marshal(checkpointDoc)
	if err != nil {
		return err
	}

	checkpointFile, err := os.OpenFile(checkpointFileName, os.O_RDWR|os.O_CREATE, base.FileModeReadWrite)
	if err != nil {
		return err
	}

	defer checkpointFile.Close()

	numOfBytes, err := checkpointFile.Write(value)
	if err != nil {
		return err
	}
	if numOfBytes != len(value) {
		return fmt.Errorf("Incomplete write. expected=%v, actual=%v", len(value), numOfBytes)
	}

	cm.logger.Infof("----------------------------------------------------------------\n")
	cm.logger.Infof("%v saved checkpoints to %v. totalMutationsChecked=%v filtered=%v filterErr=%v\n",
		cm.clusterName, checkpointFileName, total, totalFiltered, totalFailedFilter)
	return nil
}

// Returns false if mutation is filtered (should not be recorded into bucket)
func (cm *CheckpointManager) RecordFilterEvent(vbno uint16, filterResult base.FilterResultType) bool {
	switch filterResult {
	case base.Filtered:
		cm.filteredCnt[vbno].Inc(1)
		return false
	case base.UnableToFilter:
		cm.failedFilterCnt[vbno].Inc(1)
		return false
	}
	return true
}

// no need to lock seqoMap since
// 1. MutationProcessedEvent on a Vbno are serialized
// 2. checkpointManager reads seqnoMap when it saves checkpoints.
//    This is done after all DcpHandlers are stopped and MutationProcessedEvent cease to happen
func (cm *CheckpointManager) HandleMutationEvent(mut *Mutation, filterResult base.FilterResultType) bool {
	if cm.dcpDriver.completeBySeqno {
		endSeqno := cm.endSeqnoMap[mut.Vbno]
		if mut.Seqno >= endSeqno {
			cm.dcpDriver.handleVbucketCompletion(mut.Vbno, nil, "end Seqno reached")
		}
		if mut.Seqno <= endSeqno {
			cm.seqnoMap[mut.Vbno].setSeqno(mut.Seqno)
			return cm.RecordFilterEvent(mut.Vbno, filterResult)
		} else {
			return false
		}
	} else {
		cm.seqnoMap[mut.Vbno].setSeqno(mut.Seqno)
		return cm.RecordFilterEvent(mut.Vbno, filterResult)
	}
}

func (cm *CheckpointManager) updateSnapshot(vbno uint16, startSeqno, endSeqno uint64) {
	snapshot := cm.snapshots[vbno]
	snapshot.lock.Lock()
	defer snapshot.lock.Unlock()

	snapshot.startSeqno = startSeqno
	snapshot.endSeqno = endSeqno
}

func (cm *CheckpointManager) getSnapshot(vbno uint16) (startSeqno, endSeqno uint64) {
	snapshot := cm.snapshots[vbno]
	snapshot.lock.RLock()
	defer snapshot.lock.RUnlock()

	return snapshot.startSeqno, snapshot.endSeqno
}

type Snapshot struct {
	startSeqno uint64
	endSeqno   uint64
	lock       sync.RWMutex
}

type SeqnoWithLock struct {
	Seqno uint64
	Lock  sync.RWMutex
}

func (s *SeqnoWithLock) setSeqno(seqno uint64) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	s.Seqno = seqno
}

func (s *SeqnoWithLock) getSeqno() uint64 {
	s.Lock.RLock()
	defer s.Lock.RUnlock()
	return s.Seqno
}
