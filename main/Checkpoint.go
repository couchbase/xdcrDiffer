package main

import ()

type Checkpoint struct {
	Vbuuid             uint64
	Seqno              uint64
	SnapshotStartSeqno uint64
	SnapshotEndSeqno   uint64
}

// vbucket timestamp required by dcp
type VBTS struct {
	Checkpoint *Checkpoint
	EndSeqno   uint64
}

type CheckpointDoc struct {
	Checkpoints map[uint16]*Checkpoint
}
