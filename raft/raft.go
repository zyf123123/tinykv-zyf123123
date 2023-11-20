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
	"sort"
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

// maybeUpdate returns false if the given n index comes from an outdated message.
// Otherwise it updates the progress and returns true.
// 收到appresp的成功应答之后，leader更新节点的索引数据
// 如果传入的n小于等于当前的match索引，则索引就不会更新，返回false；否则更新索引返回true
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
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

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) loadState(state pb.HardState) {
	r.Term = state.Term
	r.Vote = state.Vote
	r.RaftLog.committed = state.Commit
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
		msgs:             []pb.Message(nil),
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}

	for _, p := range c.peers {
		raftPeer.Prs[p] = &Progress{Next: 1}
	}

	hardstate, _, _ := c.Storage.InitialState()
	// 如果不是第一次启动而是从之前的数据进行恢复
	if !IsEmptyHardState(hardstate) {
		raftPeer.loadState(hardstate)
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
	pr := r.Prs[to]
	// 从该节点的Next的上一条数据获取term
	term, errt := r.RaftLog.Term(pr.Next - 1)
	// 获取从该节点的Next-1之后的entries
	ents, erre := r.RaftLog.GetEntries(pr.Next)
	entries := make([]*pb.Entry, len(ents))
	for i := range ents {
		entries[i] = &ents[i]
	}
	// 构造消息
	m := pb.Message{
		To:   to,
		From: r.id,
		Term: r.Term,
	}

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		// 如果前面过程中有错，那说明之前的数据都写到快照里了，尝试发送快照数据过去
	} else {
		// 如果没有错，那么就发送entries数据
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = entries
		m.Commit = r.RaftLog.committed
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

	// 似乎对于非leader节点来说，重置progress数组状态没有太多的意义？
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
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
	r.appendEntry(pb.Entry{Data: nil})
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

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
// 尝试commit当前的日志，如果commit日志索引发生变化了就返回true
func (r *Raft) maybeCommit() bool {
	mis := make(uint64Slice, 0, len(r.Prs))
	// 拿到当前所有节点的Match到数组中
	for id := range r.Prs {
		mis = append(mis, r.Prs[id].Match)
	}
	// 逆序排列
	sort.Sort(sort.Reverse(mis))
	// 排列之后拿到中位数的Match，因为如果这个位置的Match对应的Term也等于当前的Term
	// 说明有过半的节点至少comit了mci这个索引的数据，这样leader就可以以这个索引进行commit了
	mci := mis[r.quorum()-1]
	// raft日志尝试commit
	return r.RaftLog.maybeCommit(mci, r.Term)
}

// 批量append一堆entries
func (r *Raft) appendEntry(es ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		// 设置这些entries的Term以及index
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.RaftLog.append(es...)
	// 更新本节点的Next以及Match索引
	r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	// append之后，尝试一下是否可以进行commit
	r.maybeCommit()
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
				Index:   r.RaftLog.LastIndex(),
				LogTerm: r.RaftLog.LastTerm(),
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
	log.Infof("%d receive message %s from %d term %d", r.id, m.MsgType, m.From, m.Term)
	switch {
	case m.Term == 0:
	// local message
	case m.Term > r.Term:
		log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		r.becomeFollower(m.Term, 0)
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
		if (r.Vote == None || m.Term > r.Term || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			// 如果当前没有给任何节点投票（r.Vote == None）或者投票的节点term大于本节点的（m.Term > r.Term）
			// 或者是之前已经投票的节点（r.Vote == m.From）
			// 同时还满足该节点的消息是最新的（r.raftLog.isUpToDate(m.Index, m.LogTerm)），那么就接收这个节点的投票
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

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
// 向所有follower发送append消息
func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
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
			r.bcastAppend()
		} else if r.quorum() <= len(r.votes)-gr {
			r.becomeFollower(r.Term, None)
		}
	}
}

func stepLeader(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			log.Panicf("%x stepped empty MsgProp", r.id)
		}
		// 检查是否在集群中
		if _, ok := r.Prs[r.id]; !ok {
			// 这里检查本节点是否还在集群以内，如果已经不在集群中了，不处理该消息直接返回。
			// 这种情况出现在本节点已经通过配置变化被移除出了集群的场景。
			return
		}
		// 添加数据到log中
		entries := make([]pb.Entry, len(m.Entries))
		for i := range m.Entries {
			entries[i] = *m.Entries[i]
		}
		r.appendEntry(entries...)
		// 向集群其他节点广播append消息
		r.bcastAppend()
		return
	}

	// All other message types require a progress for m.From (pr).
	// 检查消息发送者当前是否在集群中
	pr, prOk := r.Prs[m.From]
	if !prOk {
		log.Debugf("%x no progress available for %x", r.id, m.From)
		return
	}
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			log.Debugf("%x received msgAppend rejection from %x for index %d", r.id, m.From, m.Index)
			// 尝试回退关于该节点的Match、Next索引
			r.Prs[m.From].Next = m.Index
			log.Debugf("%x decreased progress of %x to [%d]", r.id, m.From, r.Prs[m.From].Next)
			// 再次发送append消息
			r.sendAppend(m.From)

		} else {
			// 更新该节点的Match、Next索引
			// 如果更新成功，那么就尝试commit
			if pr.maybeUpdate(m.Index) {
				// 尝试commit
				if r.maybeCommit() {
					// 如果commit成功，那么就向集群其他节点广播append消息
					r.bcastAppend()
				}
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		// 该节点的match节点小于当前最大日志索引，可能已经过期了，尝试添加日志
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 先检查消息消息的合法性
	if m.Index < r.RaftLog.committed { // 传入的消息索引是已经commit过的索引
		// 返回commit日志索引
		r.send(pb.Message{To: m.From, From: r.id, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed, Term: r.Term})
		return
	}

	log.Infof("%d handle append entries from %d", r.id, m.From)
	entries := make([]pb.Entry, len(m.Entries))
	for i := range m.Entries {
		entries[i] = *m.Entries[i]
	}
	// 尝试添加到日志模块中
	if mlastIndex, ok := r.RaftLog.MaybeAppend(m.Index, m.LogTerm, m.Commit, entries); ok {
		// 添加成功，返回的index是添加成功之后的最大index
		r.send(pb.Message{To: m.From, From: r.id, MsgType: pb.MessageType_MsgAppendResponse, Index: mlastIndex, Term: r.Term})
	} else {
		// 添加失败
		log.Debugf("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			r.id, r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		// 添加失败的时候，返回的Index是传入过来的Index，RejectHint是该节点当前日志的最后索引
		r.send(pb.Message{To: m.From, From: r.id, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Term: r.Term, Reject: true})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Infof("%d handle heartbeat from %d", r.id, m.From)
	r.RaftLog.CommitTo(m.Commit)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
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
