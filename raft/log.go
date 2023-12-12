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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// 添加数据，返回最后一条日志的索引
func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	// 没有数据，直接返回最后一条日志索引
	if len(ents) == 0 {
		return l.LastIndex()
	}
	// 如果索引小于committed，则说明该数据是非法的
	if after := ents[0].Index - 1; after < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	// 先拿到这些数据的第一个索引
	after := ents[0].Index
	switch {
	case after == l.FirstIndex()+uint64(len(l.entries)):
		// 如果正好是紧接着当前数据的，就直接append
		l.entries = append(l.entries, ents...)
	case after <= l.FirstIndex():
		log.Infof("replace the entries from index %d", after)
		// 如果比当前偏移量小，那用新的数据替换当前数据
		l.entries = ents
		l.stabled = 0
	default:
		// 新的entries需要拼接而成
		log.Infof("truncate the entries before index %d", after)
		l.entries, _ = l.Slice(l.FirstIndex(), after)
		l.entries = append(l.entries, ents...)
		l.stabled = min(after-1, l.stabled)
	}
	return l.LastIndex()
}

// MaybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
// 尝试添加一组日志，如果不能添加则返回(0,false)，否则返回(新的日志的索引,true)
// index：从哪里开始的日志条目
// logTerm：这一组日志对应的term
// committed：leader上的committed索引
// ents：需要提交的一组日志，因此这组数据的最大索引为index+len(ents)
func (l *RaftLog) MaybeAppend(index, logTerm, committed uint64, ents []pb.Entry) (lastnewi uint64, ok bool) {
	if l.MatchTerm(index, logTerm) {
		// 首先需要保证传入的index和logTerm能匹配的上才能走入这里，否则直接返回false

		// 首先得到传入数据的最后一条索引
		lastnewi = index + uint64(len(ents))
		// 查找传入的数据从哪里开始找不到对应的Term了
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
			// 没有冲突，忽略
		case ci <= l.committed:
			// 找到的数据索引小于committed，都说明传入的数据是错误的
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			// ci > 0的情况下来到这里
			offset := index + 1
			// 从查找到的数据索引开始，将这之后的数据放入到unstable存储中
			l.append(ents[ci-offset:]...)
			log.Infof("entry %d conflict with uncommitted entry [last committed index(%d), last committed term(%d)]", ci, l.LastIndex(), l.LastTerm())
			log.Infof("log append %d [%d:%d] to unstable storage at %d, now len %d", ci-offset, ci, len(ents), l.LastIndex(), len(l.entries))
		}
		// 选择committed和lastnewi中的最小者进行commit
		log.Infof("committing index %d lastnewi %d", committed, lastnewi)
		l.CommitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

// CommitTo 将raftlog的commit索引，修改为tocommit
func (l *RaftLog) CommitTo(tocommit uint64) {
	// never decrease commit
	// 首先需要判断，commit索引绝不能变小
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			// 传入的值如果比lastIndex大则是非法的
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
		log.Infof("log commit to %d", tocommit)
	}
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	// 只有在传入的index大于当前commit索引，以及maxIndex对应的term与传入的term匹配时，才使用这些数据进行commit
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.CommitTo(maxIndex)
		return true
	}
	return false
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	raftlog := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	// Initialize our committed and applied pointers to the time of the last compaction.
	// committed和applied从持久化的第一个index的前一个开始
	raftlog.committed = firstIndex - 1
	raftlog.applied = firstIndex - 1
	raftlog.stabled, _ = storage.LastIndex()
	raftlog.entries = entries
	return raftlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	ents, err := l.GetEntries(l.FirstIndex())
	if err == nil {
		return ents
	}
	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	// TODO: handle error?
	panic(err)
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	ents, _ := l.Slice(l.unstableOffset(), l.LastIndex()+1)
	if ents == nil {
		return []pb.Entry{}
	}
	return ents
}

func (l *RaftLog) unstableOffset() uint64 {
	return max(l.stabled+1, l.FirstIndex())
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	off := max(l.applied+1, l.FirstIndex())
	// log.Infof("nextEnts: applied(%d), off(%d), committed(%d), lastIndex(%d)", l.applied, off, l.committed, l.LastIndex())
	if l.committed+1 > off { // 如果commit索引比前面得到的值还大，说明还有没有commit了但是还没apply的数据，将这些数据返回
		ents, err := l.Slice(off, l.committed+1)
		if err != nil {
			log.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// 如果entries不为空，则返回最后一条日志的索引
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	return l.zeroTermOnErrCompacted(l.storage.LastIndex())
}

func (l *RaftLog) FirstIndex() uint64 {
	// 否则才返回持久化数据的firstIndex
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

// LastTerm 返回最后一个索引的term
func (l *RaftLog) LastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
// 判断是否比当前节点的日志更新：1）term是否更大 2）term相同的情况下，索引是否更大
func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && lasti >= l.LastIndex())
}

// 判断传入的lo，hi是否超过范围了
// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.FirstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.LastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}

// Slice returns a slice of log entries from lo through hi-1, inclusive.
// 返回[lo,hi-1]之间的数据，这些数据的大小总和不超过maxSize
func (l *RaftLog) Slice(lo, hi uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry

	start, end := 0, len(l.entries)-1
	for i := 0; i < len(l.entries); i++ {
		if l.entries[i].Index == lo {
			start = i
		}
		if l.entries[i].Index == hi {
			end = i - 1
		}
	}
	ents = l.entries[start : end+1]

	return ents, nil
}

// GetEntries 获取从i开始的entries返回
func (l *RaftLog) GetEntries(i uint64) ([]pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}
	return l.Slice(i, l.LastIndex()+1)
}

// 返回第一个在entry数组中，index中的term与当前存储的数据不同的索引
// 如果没有冲突数据，而且当前存在的日志条目包含所有传入的日志条目，返回0；
// 如果没有冲突数据，而且传入的日志条目有新的数据，则返回新日志条目的第一条索引
// 一个日志条目在其索引值对应的term与当前相同索引的term不相同时认为是有冲突的数据。
func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	// 遍历传入的ents数组
	for _, ne := range ents {
		// 找到第一个任期号不匹配的，即当前在raftLog存储的该索引数据的任期号，不是ent数据的任期号
		if !l.MatchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				// 如果不匹配任期号的索引数据，小于当前最后一条日志索引，就打印错误日志
				log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.Term(ne.Index)), ne.Term)
			}
			// 返回第一条任期号与索引号不匹配的数据索引
			return ne.Index
		}
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// the valid term range is [index of dummy entry, last index]
	// 先判断范围是否正确
	if i > l.LastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}

	unstable := l.unstableEntries()
	// 尝试从unstable中查询term
	if len(unstable) > 0 && i >= unstable[0].Index {
		return unstable[i-unstable[0].Index].Term, nil
	}

	// 尝试从storage中查询term
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	// 只有这两种错可以接受
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	return l.entries[i].Term, nil
}

// MatchTerm 判断索引i的term是否和term一致
func (l *RaftLog) MatchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

// 如果传入的err是nil，则返回t；如果是ErrCompacted则返回0，其他情况都panic
func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if errors.Is(err, ErrCompacted) {
		return 0
	}
	log.Panicf("unexpected error (%v)", err)
	return 0
}
