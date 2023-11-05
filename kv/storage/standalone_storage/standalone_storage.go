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
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	db, err := badger.Open(badger.Options{Dir: s.conf.DBPath})
	return &standaloneReader{s, 0, db}, err
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	db, _ := badger.Open(badger.Options{Dir: s.conf.DBPath})
	// txn := db.NewTransaction(true)
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(db, data.Cf, data.Key, data.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(db, data.Cf, data.Key)
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
}

func (s standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	//TODO implement me
	txn := s.db.NewTransaction(false)
	return engine_util.GetCFFromTxn(txn, cf, key)
}

func (s standaloneReader) IterCF(cf string) engine_util.DBIterator {
	//TODO implement me
	txn := s.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (s standaloneReader) Close() {
	//TODO implement me
	err := s.db.Close()
	if err != nil {
		return
	}
}
