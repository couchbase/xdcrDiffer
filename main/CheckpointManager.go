package main

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/gocb"
	"os"
	"sync"
)

type CheckpointManager struct {
	clusterName           string
	oldCheckpointFileName string
	newCheckpointFileName string
	bucketName            string
	completeBySeqno       bool
	cluster               *gocb.Cluster
	startVBTS             map[uint16]*VBTS
	vbuuidMap             map[uint16]uint64
	seqnoMap              map[uint16]*SeqnoWithLock
	snapshots             map[uint16]*Snapshot
}

func NewCheckpointManager(oldCheckpointFileDir, newCheckpointFileDir, clusterName, bucketName string, completeBySeqno bool) *CheckpointManager {
	cm := &CheckpointManager{
		clusterName:     clusterName,
		bucketName:      bucketName,
		completeBySeqno: completeBySeqno,
		startVBTS:       make(map[uint16]*VBTS),
		seqnoMap:        make(map[uint16]*SeqnoWithLock),
		snapshots:       make(map[uint16]*Snapshot),
	}

	if oldCheckpointFileDir != "" {
		cm.oldCheckpointFileName = oldCheckpointFileDir + FileDirDelimiter + clusterName
	}

	if newCheckpointFileDir != "" {
		cm.newCheckpointFileName = newCheckpointFileDir + FileDirDelimiter + clusterName
	}

	var vbno uint16
	for vbno = 0; vbno < NumerOfVbuckets; vbno++ {
		cm.seqnoMap[vbno] = &SeqnoWithLock{}
		cm.snapshots[vbno] = &Snapshot{}
	}

	return cm
}

func (cm *CheckpointManager) SetCluster(cluster *gocb.Cluster) {
	cm.cluster = cluster
}

func (cm *CheckpointManager) Start() error {
	return cm.initialize()
}

func (cm *CheckpointManager) Stop() error {
	return cm.SaveCheckpoint()
}

func (cm *CheckpointManager) initialize() error {
	endSeqnoMap, err := cm.getVbuuidsAndHighSeqnos()
	if err != nil {
		return err
	}

	return cm.setStartVBTS(endSeqnoMap)
}

func (cm *CheckpointManager) getVbuuidsAndHighSeqnos() (map[uint16]uint64, error) {
	statsBucket, err := cm.cluster.OpenBucket(cm.bucketName, "" /*password*/)
	if err != nil {
		return nil, err
	}

	defer statsBucket.Close()

	statsMap, err := statsBucket.Stats(VbucketSeqnoStatName)
	if err != nil {
		return nil, err
	}

	vbuuidMap := make(map[uint16]uint64)
	endSeqnoMap := make(map[uint16]uint64)
	err = ParseHighSeqnoStat(statsMap, endSeqnoMap, vbuuidMap, cm.completeBySeqno)
	if err != nil {
		return nil, err
	}

	cm.vbuuidMap = vbuuidMap

	if !cm.completeBySeqno {
		// set endSeqno to maxInt
		var vbno uint16
		for vbno = 0; vbno < NumerOfVbuckets; vbno++ {
			endSeqnoMap[vbno] = 0xFFFFFFFFFFFFFFFF
		}
	}

	return endSeqnoMap, nil
}

func (cm *CheckpointManager) setStartVBTS(endSeqnoMap map[uint16]uint64) error {
	if cm.oldCheckpointFileName != "" {
		checkpointDoc, err := cm.loadCheckpoints()
		if err != nil {
			return err
		}
		for vbno, checkpoint := range checkpointDoc.Checkpoints {
			cm.startVBTS[vbno] = &VBTS{
				Checkpoint: checkpoint,
				EndSeqno:   endSeqnoMap[vbno],
			}
			// update start seqno as that in checkpoint doc
			cm.seqnoMap[vbno].setSeqno(checkpoint.Seqno)
		}
	} else {
		var vbno uint16
		for vbno = 0; vbno < NumerOfVbuckets; vbno++ {
			// if we are not loading checkpoints, it is ok to leave all fields in Checkpoint with default values, 0
			cm.startVBTS[vbno] = &VBTS{
				Checkpoint: &Checkpoint{},
				EndSeqno:   endSeqnoMap[vbno],
			}
		}
	}

	return nil
}

func (cm *CheckpointManager) GetStartVBTS(vbno uint16) *VBTS {
	return cm.startVBTS[vbno]
}

func (cm *CheckpointManager) loadCheckpoints() (*CheckpointDoc, error) {
	checkpointFile, err := os.Open(cm.oldCheckpointFileName)
	if err != nil {
		fmt.Printf("Error opening checkpoint file. err=%v\n", err)
		return nil, err
	}

	buffer := make([]byte, CheckpointFileBufferSize)
	bufferBytes, err := checkpointFile.Read(buffer)
	if err != nil {
		fmt.Printf("Error reading checkpoint file. err=%v\n", err)
		return nil, err
	}

	checkpointDoc := &CheckpointDoc{}
	err = json.Unmarshal(buffer[:bufferBytes], checkpointDoc)
	if err != nil {
		fmt.Printf("Error unmarshalling checkpoint file. err=%v\n", err)
		return nil, err
	}

	if len(checkpointDoc.Checkpoints) < NumerOfVbuckets {
		return nil, fmt.Errorf("checkpoint file %v has less than 1024 vbuckets.", cm.oldCheckpointFileName)
	}

	return checkpointDoc, nil
}

func (cm *CheckpointManager) SaveCheckpoint() error {
	if cm.newCheckpointFileName == "" {
		// checkpointing disabled
		fmt.Printf("Skipping checkpointing for %v since checkpointing has been disabled", cm.clusterName)
		return nil
	}

	// delete existing file if exists
	os.Remove(cm.newCheckpointFileName)

	checkpointDoc := &CheckpointDoc{
		Checkpoints: make(map[uint16]*Checkpoint),
	}

	var vbno uint16
	for vbno = 0; vbno < NumerOfVbuckets; vbno++ {
		vbuuid := cm.vbuuidMap[vbno]
		seqno := cm.seqnoMap[vbno].getSeqno()
		var snapshotStartSeqno uint64
		var snapshotEndSeqno uint64

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
		}
	}

	value, err := json.Marshal(checkpointDoc)
	if err != nil {
		return err
	}

	checkpointFile, err := os.OpenFile(cm.newCheckpointFileName, os.O_RDWR|os.O_CREATE, FileModeReadWrite)
	if err != nil {
		return err
	}

	numOfBytes, err := checkpointFile.Write(value)
	if err != nil {
		return err
	}
	if numOfBytes != len(value) {
		return fmt.Errorf("Incomplete write. expected=%v, actual=%v", len(value), numOfBytes)
	}
	return nil
}

// no need to lock seqoMap since
// 1. MutationProcessedEvent on a vbno are serialized
// 2. checkpointManager reads seqnoMap when it saves checkpoints.
//    This is done after all DcpHandlers are stopped and MutationProcessedEvent cease to happen
func (cm *CheckpointManager) HandleMutationProcessedEvent(mut *Mutation) {
	cm.seqnoMap[mut.vbno].setSeqno(mut.seqno)
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
