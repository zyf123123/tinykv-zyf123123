package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	storageReader, _ := server.storage.Reader(req.Context)
	val, readerErr := storageReader.GetCF(req.Cf, req.Key)
	if val == nil {
		server.storage.Stop()
		return &kvrpcpb.RawGetResponse{RegionError: nil, Error: "", Value: nil, NotFound: true}, readerErr
	}

	return &kvrpcpb.RawGetResponse{Value: val}, readerErr
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	modify := storage.Modify{Data: put}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	modify := storage.Modify{Data: delete}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.GetCf())
	startKey := req.GetStartKey()
	var nums uint32 = 0
	kvPairs := make([]*kvrpcpb.KvPair, 0)
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		if nums >= req.GetLimit() {
			break
		}
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{Key: item.Key(), Value: val})
		nums++
	}
	iter.Close()
	return &kvrpcpb.RawScanResponse{Kvs: kvPairs}, nil
}
