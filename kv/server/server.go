package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	keys := [][]byte{req.GetKey()}
	keys = append(keys, req.GetKey())
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts <= req.GetVersion() {
		return &kvrpcpb.GetResponse{Error: &kvrpcpb.KeyError{Locked: lock.Info(req.GetKey())}}, nil
	}
	val, err := txn.GetValue(req.GetKey())
	if err != nil {
		return nil, err
	}
	if val == nil {
		return &kvrpcpb.GetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.GetResponse{Value: val}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	keys := [][]byte{}
	for _, m := range req.Mutations {
		keys = append(keys, m.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, m := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(m.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback && ts > req.StartVersion {
			conflict := &kvrpcpb.WriteConflict{
				StartTs:    req.StartVersion,
				ConflictTs: ts,
				Key:        m.Key,
				Primary:    req.PrimaryLock,
			}
			return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{{Conflict: conflict}}}, nil
		}
	}
	for _, m := range req.Mutations {
		lock, err := txn.GetLock(m.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{{Locked: lock.Info(m.Key)}}}, nil
		}
	}
	// 可以执行prewrite操作
	for _, m := range req.Mutations {
		txn.PutLock(m.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(m.Op),
		})
		txn.PutValue(m.Key, m.Value)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, k := range req.Keys {
		write, _, err := txn.CurrentWrite(k)
		if err != nil {
			return nil, err
		}
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			return &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{Abort: "true"}}, nil
		}
		// 重复提交
		if write != nil && write.Kind != mvcc.WriteKindRollback && write.StartTS == req.StartVersion {
			return &kvrpcpb.CommitResponse{}, nil
		}
		// 检测lock
		lock, err := txn.GetLock(k)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			continue
		}
		if lock.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{Retryable: "true"}}, nil
		}

		txn.PutWrite(k, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(k)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	var pairs []*kvrpcpb.KvPair
	for i := uint32(0); i < req.Limit; {
		key, val, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts <= req.GetVersion() {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
				Key: key,
			})
			i++
			continue
		}
		if val != nil {
			i++
			pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: val})
		}
	}
	return &kvrpcpb.ScanResponse{Pairs: pairs}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		return &kvrpcpb.CheckTxnStatusResponse{CommitVersion: commitTs}, nil
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		resp := &kvrpcpb.CheckTxnStatusResponse{}
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		return &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_TTLExpireRollback}, nil
	}
	return &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_NoAction}, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, k := range req.Keys {
		// 1. 获取 key 的 Write，如果已经是 WriteKindRollback 则跳过这个 key
		write, _, err := txn.CurrentWrite(k)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				return &kvrpcpb.BatchRollbackResponse{Error: &kvrpcpb.KeyError{Abort: "true"}}, nil
			}
		}
		// 获取 Lock，如果 Lock 被清除或者 Lock 不是当前事务的 Lock，则中止操作
		// 这个时候说明 key 被其他事务占用
		// 否则的话移除 Lock、删除 Value，写入 WriteKindRollback 的 Write
		lock, err := txn.GetLock(k)
		if err != nil {
			return nil, err
		}
		if lock == nil || lock.Ts != req.StartVersion {
			txn.PutWrite(k, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		txn.DeleteLock(k)
		txn.DeleteValue(k)
		txn.PutWrite(k, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	var keys [][]byte
	resp := &kvrpcpb.ResolveLockResponse{}
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return resp, nil
	}
	// 根据 request.CommitVersion 提交或者 Rollback
	if req.CommitVersion == 0 {
		resp1, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		resp.Error, resp.RegionError = resp1.Error, resp1.RegionError
		return resp, err
	} else {
		resp1, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		resp.Error, resp.RegionError = resp1.Error, resp1.RegionError
		return resp, err
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
