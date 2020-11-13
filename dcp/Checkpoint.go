package dcp

type Checkpoint struct {
	Vbuuid             uint64
	Seqno              uint64
	SnapshotStartSeqno uint64
	SnapshotEndSeqno   uint64
	FilteredCnt        uint64
	FailedFilterCnt    uint64
}

// vbucket timestamp required by dcp
type VBTS struct {
	Checkpoint *Checkpoint
	EndSeqno   uint64
	// whether a dcp stream needs to be started
	NoNeedToStartDcpStream bool
}

type CheckpointDoc struct {
	Checkpoints map[uint16]*Checkpoint
}
