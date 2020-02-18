package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const DeleteLimit = 70000

// Priority - global list of all pruners priorities. Higher priority means faster delete.
type Priority uint8

const (
	History Priority = iota
	SelfDestructedAccount
)

type BlockChainer interface {
	CurrentBlock() *types.Block
}

type FeaturePruner interface {
	Init(ctx context.Context) error
	Step(ctx context.Context) error
	SaveProgress(ctx context.Context) error
}

func NewPruningManager(db ethdb.Database, chainer BlockChainer, config *CacheConfig) (*PruningManager, error) {
	//if config.BlocksToPrune == 0 || config.PruneTimeout.Seconds() < 1 {
	//	return nil, fmt.Errorf("incorrect config BlocksToPrune - %v, PruneTimeout - %v", config.BlocksToPrune, config.PruneTimeout.Seconds())
	//}

	p := &PruningManager{
		config:          config,
		wg:              &sync.WaitGroup{},
		exclusiveStepCh: make(chan struct{}, 1),
		pruners:         []FeaturePruner{},
	}

	hPruner, err := NewHistoryPruner(db, chainer, config)
	if err != nil {
		return nil, fmt.Errorf("pruner creation: %w", err)
	}
	p.pruners = append(p.pruners, hPruner)

	sdPruner, err := NewSelfDestructPruner(db)
	if err != nil {
		return nil, fmt.Errorf("pruner creation: %w", err)
	}
	p.pruners = append(p.pruners, sdPruner)

	return p, nil
}

// PruningManager manages db pressure.
// It limits amount of read/deletes and can orchestrate multiple pruners - runs one at a time.
type PruningManager struct {
	config          *CacheConfig
	currentStep     uint64
	wg              *sync.WaitGroup
	exclusiveStepCh chan struct{} // guarantee that only one step running at a time
	cancel          context.CancelFunc
	pruners         []FeaturePruner
}

func (p *PruningManager) Start(ctx context.Context) error {
	ctx, p.cancel = context.WithCancel(ctx)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		for _, pruner := range p.pruners {
			if err := pruner.Init(ctx); err != nil {
				log.Error("pruner init progress err", "err", err, "pruner", pruner)
				return
			}
		}

		prunerRun := time.NewTicker(p.config.PruneTimeout)
		defer prunerRun.Stop()
		saveProgress := time.NewTicker(time.Minute * 5)
		defer saveProgress.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-saveProgress.C:
				for _, pruner := range p.pruners {
					if err := pruner.SaveProgress(ctx); err != nil {
						log.Error("pruner save progress err", "err", err)
					}
				}
			case <-prunerRun.C:
				p.currentStep++
				pruner := p.pruners[p.currentStep%uint64(len(p.pruners))]
				err := pruner.Step(ctx)
				if err != nil {
					log.Error("pruner step err", "err", err, "pruner", pruner)
				}
			}

		}
	}()
	log.Info("Pruner started")

	return nil
}

func (p *PruningManager) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	log.Info("Pruning stopped")
}

func NewHistoryPruner(db ethdb.Database, chain BlockChainer, config *CacheConfig) (*HistoryPruner, error) {
	if config.BlocksToPrune == 0 || config.PruneTimeout.Seconds() < 1 {
		return nil, fmt.Errorf("incorrect config BlocksToPrune - %v, PruneTimeout - %v", config.BlocksToPrune, config.PruneTimeout.Seconds())
	}

	return &HistoryPruner{
		db:     db,
		chain:  chain,
		config: config,
	}, nil
}

type HistoryPruner struct {
	lastPrunedBlockNum uint64
	db                 ethdb.Database
	chain              BlockChainer
	config             *CacheConfig
}

func (p *HistoryPruner) Step(ctx context.Context) error {
	cb := p.chain.CurrentBlock()
	if cb == nil || cb.Number() == nil {
		return nil
	}
	from, to, ok := calculateNumOfPrunedBlocks(cb.Number().Uint64(), p.lastPrunedBlockNum, p.config.BlocksBeforePruning, p.config.BlocksToPrune)
	if !ok {
		return nil
	}
	log.Debug("Pruning history", "from", from, "to", to)
	if err := PruneHistory(ctx, p.db, from, to); err != nil {
		return fmt.Errorf("history pruning error: %w", err)
	}
	p.lastPrunedBlockNum = to

	return nil
}

func calculateNumOfPrunedBlocks(currentBlock, lastPrunedBlock uint64, blocksBeforePruning uint64, blocksBatch uint64) (uint64, uint64, bool) {
	//underflow see https://github.com/ledgerwatch/turbo-geth/issues/115
	if currentBlock <= lastPrunedBlock {
		return lastPrunedBlock, lastPrunedBlock, false
	}

	diff := currentBlock - lastPrunedBlock
	if diff <= blocksBeforePruning {
		return lastPrunedBlock, lastPrunedBlock, false
	}
	diff = diff - blocksBeforePruning
	switch {
	case diff >= blocksBatch:
		return lastPrunedBlock, lastPrunedBlock + blocksBatch, true
	case diff < blocksBatch:
		return lastPrunedBlock, lastPrunedBlock + diff, true
	default:
		return lastPrunedBlock, lastPrunedBlock, false
	}
}

var LastPrunedBlockKey = []byte("History.LastPrunedBlock")

func (p *HistoryPruner) Init(ctx context.Context) error {
	data, err := p.db.Get(dbutils.PrunerProgressBucket, LastPrunedBlockKey)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		p.lastPrunedBlockNum = 0
		return nil
	}
	p.lastPrunedBlockNum = binary.LittleEndian.Uint64(data)
	return nil
}

// SaveProgress stores the head block's hash.
func (p *HistoryPruner) SaveProgress(ctx context.Context) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, p.lastPrunedBlockNum)
	if err := p.db.Put(dbutils.PrunerProgressBucket, LastPrunedBlockKey, b); err != nil {
		return fmt.Errorf("failed to store last pruned block's num: %w", err)
	}
	return nil
}

func PruneHistory(ctx context.Context, db ethdb.Database, blockNumFrom uint64, blockNumTo uint64) error {
	keysToRemove := newKeysToRemove()
	err := db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(key, v []byte) (b bool, e error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		timestamp, _ := dbutils.DecodeTimestamp(key)
		if timestamp < blockNumFrom {
			return true, nil
		}
		if timestamp > blockNumTo {
			return false, nil
		}

		keysToRemove.ChangeSet = append(keysToRemove.ChangeSet, key)

		err := dbutils.Walk(v, func(cKey, _ []byte) error {
			compKey, _ := dbutils.CompositeKeySuffix(cKey, timestamp)
			if bytes.HasSuffix(cKey, dbutils.AccountsHistoryBucket) {
				keysToRemove.AccountHistoryKeys = append(keysToRemove.AccountHistoryKeys, compKey)
			}
			if bytes.HasSuffix(cKey, dbutils.StorageHistoryBucket) {
				keysToRemove.StorageHistoryKeys = append(keysToRemove.StorageHistoryKeys, compKey)
			}
			return nil
		})
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	err = batchDelete(ctx, db, keysToRemove)
	if err != nil {
		return err
	}

	return nil
}

type SelfDestructPruner struct {
	lastKey []byte

	db ethdb.Database
}

func NewSelfDestructPruner(db ethdb.Database) (*SelfDestructPruner, error) {
	return &SelfDestructPruner{
		db: db,
	}, nil
}

func (p *SelfDestructPruner) Step(ctx context.Context) error {
	if !debug.IsIntermediateTrieHash() {
		return nil
	}
	return PruneSelfDestructedStorage(ctx, p.db)
}

func PruneSelfDestructedStorage(ctx context.Context, db ethdb.Database) error {
	keysToRemove := newKeysToRemove()
	if err := db.Walk(dbutils.IntermediateTrieHashBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		if len(v) > 0 && len(k) != common.HashLength { // marker of self-destructed account is - empty value
			return true, nil
		}

		if err := db.Walk(dbutils.StorageBucket, k, common.HashLength*8, func(k, _ []byte) (b bool, e error) {
			keysToRemove.StorageKeys = append(keysToRemove.StorageKeys, k)
			return true, nil
		}); err != nil {
			return false, err
		}

		if err := db.Walk(dbutils.IntermediateTrieHashBucket, k, common.HashLength*8, func(k, _ []byte) (b bool, e error) {
			keysToRemove.IntermediateTrieHashKeys = append(keysToRemove.IntermediateTrieHashKeys, k)
			return true, nil
		}); err != nil {
			return false, err
		}

		return true, nil
	}); err != nil {
		return err
	}

	//return batchDelete(ctx, db, keysToRemove)
	log.Debug("SelfDestructPruner can remove rows amount", "storage_bucket", len(keysToRemove.StorageKeys), "intermediate_bucket", len(keysToRemove.IntermediateTrieHashKeys))
	return nil
}

var LastPrunedSelfDestructedAccountKey = []byte("SelfDestruct.LastPrunedAccount")

// SaveProgress
func (p *SelfDestructPruner) Init(ctx context.Context) error {
	val, err := p.db.Get(dbutils.PrunerProgressBucket, LastPrunedSelfDestructedAccountKey)
	if err != nil {
		return fmt.Errorf("failed to get last pruned key: %w", err)
	}
	p.lastKey = val
	return nil
}

func (p *SelfDestructPruner) SaveProgress(ctx context.Context) error {
	if err := p.db.Put(dbutils.PrunerProgressBucket, LastPrunedSelfDestructedAccountKey, common.CopyBytes(p.lastKey)); err != nil {
		return fmt.Errorf("failed to store last pruned key: %w", err)
	}
	return nil
}

func batchDelete(ctx context.Context, db ethdb.Database, keys *keysToRemove) error {
	log.Debug("Removing: ", "accounts", len(keys.AccountHistoryKeys), "storage", len(keys.StorageHistoryKeys), "suffix", len(keys.ChangeSet))
	iterator := LimitIterator(keys, DeleteLimit)
	for iterator.HasMore() {
		iterator.ResetLimit()
		batch := db.NewBatch()
		for {
			select {
			case <-ctx.Done():
				batch.Rollback()
				return ctx.Err()
			default:
			}

			key, bucketKey, ok := iterator.GetNext()
			if !ok {
				break
			}
			err := batch.Delete(bucketKey, key)
			if err != nil {
				log.Warn("Unable to remove", "bucket", bucketKey, "addr", common.Bytes2Hex(key), "err", err)
				continue
			}
		}
		_, err := batch.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func newKeysToRemove() *keysToRemove {
	return &keysToRemove{
		AccountHistoryKeys:       make(Keys, 0),
		StorageHistoryKeys:       make(Keys, 0),
		ChangeSet:                make(Keys, 0),
		StorageKeys:              make(Keys, 0),
		IntermediateTrieHashKeys: make(Keys, 0),
	}
}

type Keys [][]byte
type Batch struct {
	bucket []byte
	keys   Keys
}

type keysToRemove struct {
	AccountHistoryKeys       Keys
	StorageHistoryKeys       Keys
	ChangeSet                Keys
	StorageKeys              Keys
	IntermediateTrieHashKeys Keys
}

func LimitIterator(k *keysToRemove, limit int) *limitIterator {
	i := &limitIterator{
		k:     k,
		limit: limit,
	}
	i.batches = []Batch{
		{bucket: dbutils.AccountsHistoryBucket, keys: i.k.AccountHistoryKeys},
		{bucket: dbutils.StorageHistoryBucket, keys: i.k.StorageHistoryKeys},
		{bucket: dbutils.StorageBucket, keys: i.k.StorageKeys},
		{bucket: dbutils.ChangeSetBucket, keys: i.k.ChangeSet},
		{bucket: dbutils.IntermediateTrieHashBucket, keys: i.k.IntermediateTrieHashKeys},
	}

	return i
}

type limitIterator struct {
	k             *keysToRemove
	counter       uint64
	currentBucket []byte
	currentNum    int
	limit         int
	batches       []Batch
}

func (i *limitIterator) GetNext() ([]byte, []byte, bool) {
	if i.limit <= i.currentNum {
		return nil, nil, false
	}
	i.updateBucket()
	if !i.HasMore() {
		return nil, nil, false
	}
	defer func() {
		i.currentNum++
		i.counter++
	}()

	for batchIndex, batch := range i.batches {
		if batchIndex == len(i.batches)-1 {
			break
		}
		if bytes.Equal(i.currentBucket, batch.bucket) {
			return batch.keys[i.currentNum], batch.bucket, true
		}
	}
	return nil, nil, false
}

func (i *limitIterator) ResetLimit() {
	i.counter = 0
}

func (i *limitIterator) HasMore() bool {
	lastBatch := i.batches[len(i.batches)-1]
	if bytes.Equal(i.currentBucket, lastBatch.bucket) && len(lastBatch.keys) == i.currentNum {
		return false
	}
	return true
}

func (i *limitIterator) updateBucket() {
	if i.currentBucket == nil {
		i.currentBucket = i.batches[0].bucket
	}

	for batchIndex, batch := range i.batches {
		if batchIndex == len(i.batches)-1 {
			break
		}

		if bytes.Equal(i.currentBucket, batch.bucket) && len(batch.keys) == i.currentNum {
			i.currentBucket = i.batches[batchIndex+1].bucket
			i.currentNum = 0
		}
	}
}
