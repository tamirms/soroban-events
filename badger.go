package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/dgraph-io/badger/v3"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/stellar/go/support/log"
)

func parseBlockFromBloom(key string) uint64 {
	block, err := strconv.ParseUint(key[len("b:"):], 10, 32)
	if err != nil {
		log.Fatalf("could not extract block %v", err)
	}
	return block
}

func bloomKey(block uint64) string {
	return fmt.Sprintf("b:%010d", block)
}

func eventKey(block uint64, index uint) string {
	id := (block << 32) | uint64(index)
	return fmt.Sprintf("e:%020d", id)
}

type badgerEventsDB struct {
	db        *badger.DB
	bloomSize uint
}

func newBadgerEventsDB(dbFilePath string) (eventsDB, error) {
	opts := badger.DefaultOptions(dbFilePath)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &badgerEventsDB{
		db:        db,
		bloomSize: 2500,
	}, nil
}

func (e *badgerEventsDB) query(query eventsQuery) (time.Duration, error) {
	var found []types.Log
	start := time.Now()
	err := e.db.View(func(txn *badger.Txn) error {
		bloomIt := txn.NewIterator(badger.DefaultIteratorOptions)
		defer bloomIt.Close()
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		startBloomKey := bloomKey(query.startBlock)
		endBloomKey := bloomKey(query.endBlock)
		filter := newBlockBloomFilter(e.bloomSize)
		for bloomIt.Seek([]byte(startBloomKey)); bloomIt.ValidForPrefix([]byte("b:")); bloomIt.Next() {
			item := bloomIt.Item()
			k := item.Key()
			if string(k[:len(endBloomKey)]) > endBloomKey {
				break
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err = filter.GobDecode(val); err != nil {
				return err
			}
			if filter.Test(query.target.Bytes()) {
				block := parseBlockFromBloom(string(k))
				found, err = findEvents(it, eventsQuery{
					target:     query.target,
					startBlock: block,
					endBlock:   block,
					limit:      query.limit,
				}, found)
				if err != nil {
					return err
				}
				if len(found) >= query.limit {
					break
				}
			}
		}

		return nil
	})

	duration := time.Now().Sub(start)

	return duration, err
}

func findEvents(it *badger.Iterator, query eventsQuery, found []types.Log) ([]types.Log, error) {
	startKey := eventKey(query.startBlock, 0)
	endKey := eventKey(query.endBlock+1, 0)
	for it.Seek([]byte(startKey)); it.ValidForPrefix([]byte("e:")); it.Next() {
		item := it.Item()
		k := item.Key()
		if string(k[:len(endKey)]) >= endKey {
			break
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		var event types.Log
		if err := rlp.DecodeBytes(val, &event); err != nil {
			return nil, err
		}
		if event.Address == query.target {
			found = append(found, event)
		}
		if len(found) >= query.limit {
			break
		}
	}
	return found, nil
}

func (e *badgerEventsDB) ingest(file io.Reader) (time.Duration, error) {
	start := time.Now()

	batch := e.db.NewWriteBatch()
	decoder := json.NewDecoder(file)
	var curBlock uint64
	filter := newBlockBloomFilter(e.bloomSize)

	curItemsPerBlock, curEventsPerBlock, maxItemsPerBlock, maxEventsPerBlock := 0, 0, 0, 0
	numEvents := 0

	for {
		var event types.Log
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				if err := writeBloomFilter(curBlock, filter, batch); err != nil {
					return 0, err
				}
				break
			}
			return 0, err
		}
		numEvents++
		if event.BlockNumber > curBlock {
			if err := writeBloomFilter(curBlock, filter, batch); err != nil {
				return 0, err
			}
			filter = newBlockBloomFilter(e.bloomSize)
			curBlock = event.BlockNumber

			if curItemsPerBlock > maxItemsPerBlock {
				maxItemsPerBlock = curItemsPerBlock
			}
			curItemsPerBlock = 0

			if curEventsPerBlock > maxEventsPerBlock {
				maxEventsPerBlock = curEventsPerBlock
			}
			curEventsPerBlock = 0
		}
		key := eventKey(event.BlockNumber, event.Index)
		var buf bytes.Buffer
		if err := event.EncodeRLP(&buf); err != nil {
			return 0, err
		}

		if err := batch.Set([]byte(key), buf.Bytes()); err != nil {
			return 0, err
		}
		filter.Add(event.Address.Bytes())
		curItemsPerBlock++
		curEventsPerBlock++
		for i, topic := range event.Topics {
			filter.Add(append(topic.Bytes(), byte(i)))
			curItemsPerBlock++
		}
	}
	if curItemsPerBlock > maxItemsPerBlock {
		maxItemsPerBlock = curItemsPerBlock
	}
	if curEventsPerBlock > maxEventsPerBlock {
		maxEventsPerBlock = curEventsPerBlock
	}
	err := batch.Flush()
	duration := time.Since(start)
	//fmt.Printf("maxItemsPerBlock %v maxEventsPerBlock %v numEvents %v duration %v\n", maxItemsPerBlock, maxEventsPerBlock, numEvents, duration)
	return duration, err
}

func (e *badgerEventsDB) close() error {
	return e.db.Close()
}

func newBlockBloomFilter(size uint) *bloom.BloomFilter {
	return bloom.NewWithEstimates(size, 0.01)
}

func writeBloomFilter(curBlock uint64, filter *bloom.BloomFilter, batch *badger.WriteBatch) error {
	if curBlock == 0 {
		return nil
	}
	payload, err := filter.GobEncode()
	if err != nil {
		return err
	}
	if err := batch.Set([]byte(bloomKey(curBlock)), payload); err != nil {
		return err
	}
	return nil
}
