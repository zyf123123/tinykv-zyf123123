package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	option := badger.DefaultOptions
	option.Dir = s.conf.DBPath
	option.ValueDir = s.conf.DBPath
	s.db, _ = badger.Open(option)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// .db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &standaloneReader{s, 0, s.db, s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// txn := db.NewTransaction(true)
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.db, data.Cf, data.Key, data.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.db, data.Cf, data.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// standaloneReader is a StorageReader which reads from a StandaloneStorage.
type standaloneReader struct {
	inner     *StandAloneStorage
	iterCount int
	db        *badger.DB
	txn       *badger.Txn
}

func (s standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	//TODO implement me
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil && err.Error() == "Key not found" {
		return nil, nil
	}
	return val, err
}

func (s standaloneReader) IterCF(cf string) engine_util.DBIterator {
	//TODO implement me
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s standaloneReader) Close() {
	//TODO implement me
	s.txn.Discard()
}
