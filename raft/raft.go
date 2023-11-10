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
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization. Only the methods needed by the code are exposed
// (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomizedElectionTimeout is a random number between electionTimeout and 2 * electionTimeout - 1.
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	step stepFunc
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftPeer := Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		State:            StateFollower,
		votes:            map[uint64]bool{},
		msgs:             []pb.Message{},
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}

	for _, p := range c.peers {
		raftPeer.Prs[p] = &Progress{Next: 1, Match: 0}
	}

	return &raftPeer
}

// send persists state to stable storage and then sends to its mailbox.
func (r *Raft) send(m pb.Message) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.send(m)
	return true
}

func (r *Raft) quorum() int { return len(r.Prs)/2 + 1 }

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.send(m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed++
	r.electionElapsed++
	switch r.State {
	case StateFollower:
		// election timeout
		if r.pastElectionTimeout() {
			// start new election
			r.Step(pb.Message{
				From:    r.id,
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case StateCandidate:
		// election timeout
		if r.pastElectionTimeout() {
			r.electionElapsed = 0
			// start new election
			r.Step(pb.Message{
				From:    r.id,
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case StateLeader:
		// heartbeat timeout
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// send heartbeat
			for peerId := range r.Prs {
				if peerId != r.id {
					r.sendHeartbeat(peerId)
				}
			}
		}
	}

}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// 重置raft的一些状态
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		// 如果是新的任期，那么保存任期号，同时将投票节点置空
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.votes = make(map[uint64]bool)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.step = stepFollower
	r.State = StateFollower
	r.reset(term)
	r.Lead = lead
	log.Infof("%d become follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.State = StateCandidate
	r.reset(r.Term + 1)
	r.Vote = r.id
	log.Infof("%d become candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.State = StateLeader
	r.reset(r.Term)
	r.Lead = r.id
	// NOTE: Leader should propose a noop entry on its term

	log.Infof("%d become leader at term %d", r.id, r.Term)
}

func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		log.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		log.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if r.quorum() <= r.poll(r.id, pb.MessageType_MsgRequestVote, true) {
		r.becomeLeader()
		return
	}
	for peerId := range r.Prs {
		if peerId != r.id {
			log.Infof("%x sent %s request to %x at term %d",
				r.id, pb.MessageType_MsgRequestVote, peerId, r.Term)
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peerId,
				From:    r.id,
				Term:    r.Term,
			})
		}
	}
}

func (r *Raft) EvaluateStep(state StateType) {
	switch state {
	case StateFollower:
		r.step = stepFollower
	case StateCandidate:
		r.step = stepCandidate
	case StateLeader:
		r.step = stepLeader
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if r.step == nil {
		r.EvaluateStep(r.State)
	}
	log.Infof("%d receive message from %d", r.id, m.From)
	switch {
	case m.Term == 0:
	// local message
	case m.Term > r.Term:
		log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		r.becomeFollower(m.Term, m.From)
	case m.Term < r.Term:
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// start election
		if r.State != StateLeader {
			log.Infof("%d start election at term %d", r.id, r.Term)
			r.campaign()
		} else {
			log.Infof("%d ignore election because already leader at term %d", r.id, r.Term)
		}
	case pb.MessageType_MsgRequestVote:
		// handle vote request
		if r.Vote == None || m.Term > r.Term || r.Vote == m.From {
			// 如果当前没有给任何节点投票（r.Vote == None）或者投票的节点term大于本节点的（m.Term > r.Term）
			// 或者是之前已经投票的节点（r.Vote == m.From）
			log.Infof("%d vote for %d at term %d", r.id, m.From, m.Term)
			r.Vote = m.From
			r.electionElapsed = 0
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
			})
		} else {
			// 否则拒绝投票
			log.Infof("%d reject vote for %d at term %d", r.id, m.From, m.Term)
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		}
	default:
		// handle other message
		r.step(r, m)
	}
	return nil
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

type stepFunc func(r *Raft, m pb.Message)

func stepFollower(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
		r.Lead = m.From
		r.electionElapsed = 0
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
		r.Lead = m.From
		r.electionElapsed = 0
	}
}

func stepCandidate(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr := r.poll(m.From, m.MsgType, !m.Reject)
		log.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.MsgType, len(r.votes)-gr)
		if r.quorum() <= gr {
			r.becomeLeader()
		} else {
			// r.becomeFollower(r.Term, None)
		}
	}
}

func stepLeader(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	log.Infof("%d handle append entries from %d", r.id, m.From)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Infof("%d handle heartbeat from %d", r.id, m.From)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
