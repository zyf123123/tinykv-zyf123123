// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"reflect"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	PrevSoftSt *SoftState
	PrevHardSt pb.HardState
	ReadyState Ready
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	if config.ID == 0 {
		panic("config.ID must not be zero")
	}
	r := newRaft(config)
	rn := &RawNode{
		Raft: r,
	}
	lastIndex, err := config.Storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// If the log is empty, this is a new RawNode (like StartNode); otherwise it's
	// restoring an existing RawNode (like RestartNode).
	// TODO(bdarnell): rethink RawNode initialization and whether the application needs
	// to be able to tell us when it expects the RawNode to exist.
	if lastIndex == 0 {
		r.becomeFollower(1, None)
		ents := make([]pb.Entry, len(config.peers))
		for i, peer := range config.peers {
			cc := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: peer}
			data, err := cc.Marshal()
			if err != nil {
				panic("unexpected marshal error")
			}

			ents[i] = pb.Entry{EntryType: pb.EntryType_EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data}
		}
		r.RaftLog.append(ents...)
		r.RaftLog.committed = uint64(len(ents))
		for _, peer := range config.peers {
			r.addNode(peer)
		}
	}

	// Set the initial hard and soft states after performing all initialization.
	rn.PrevSoftSt = r.softState()
	if lastIndex == 0 {
		rn.PrevHardSt = pb.HardState{}
	} else {
		rn.PrevHardSt = r.hardState()
	}

	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	rn.ReadyState = Ready{
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
		Messages:         rn.Raft.msgs,
	}
	if rn.PrevSoftSt.Lead != rn.Raft.softState().Lead || rn.PrevSoftSt.RaftState != rn.Raft.softState().RaftState {
		rn.ReadyState.SoftState = rn.Raft.softState()
	} else {
		rn.ReadyState.SoftState = nil
	}
	if !reflect.DeepEqual(rn.PrevHardSt, rn.Raft.hardState()) {
		rn.ReadyState.HardState = rn.Raft.hardState()
	}
	if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		rn.ReadyState.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}
	return rn.ReadyState
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	rn.ReadyState = rn.Ready()

	return rn.ReadyState.SoftState != nil ||
		!IsEmptyHardState(rn.ReadyState.HardState) ||
		len(rn.ReadyState.Entries) > 0 ||
		len(rn.ReadyState.CommittedEntries) > 0 ||
		len(rn.ReadyState.Messages) > 0 ||
		!IsEmptySnap(&rn.ReadyState.Snapshot)
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	if rn.HasReady() {
		rd = rn.Ready()
		rn.ReadyState = Ready{}
		rn.Raft.RaftLog.stabled = rn.Raft.RaftLog.LastIndex()
		rn.Raft.RaftLog.applied = rn.Raft.RaftLog.committed
		rn.Raft.msgs = nil
		rn.Raft.RaftLog.pendingSnapshot = nil
		rn.Raft.RaftLog.maybeCompact()
	}
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
