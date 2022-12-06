package main

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/core/types"
	"io"
	"os"
	"sort"
	"time"
)

type memoryEventsDB struct {
	events            []types.Log
	ingestionDuration time.Duration
}

func newMemoryEventsDB(jsonFilePath string) (eventsDB, error) {
	file, err := os.Open(jsonFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	db := &memoryEventsDB{}
	if _, err := db.ingest(file); err != nil {
		return nil, err
	}
	fmt.Printf("ingestion duration %v\n", db.ingestionDuration)
	return db, nil
}

func (e *memoryEventsDB) ingest(inputJSON io.Reader) (time.Duration, error) {
	if e.ingestionDuration > 0 {
		return e.ingestionDuration, nil
	}
	startTime := time.Now()
	decoder := json.NewDecoder(inputJSON)

	for {
		var event types.Log
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		e.events = append(e.events, event)
	}
	e.ingestionDuration = time.Since(startTime)

	return e.ingestionDuration, nil
}

func (e *memoryEventsDB) query(q eventsQuery) (time.Duration, error) {
	startTime := time.Now()

	var found []types.Log
	startIndex := sort.Search(len(e.events), func(i int) bool {
		return e.events[i].BlockNumber >= q.startBlock
	})
	for i := startIndex; i < len(e.events); i++ {
		if e.events[i].BlockNumber > q.endBlock {
			break
		}
		if e.events[i].Address == q.target {
			found = append(found, e.events[i])
			if len(found) >= q.limit {
				break
			}
		}
	}

	return time.Since(startTime), nil
}

func (e *memoryEventsDB) close() error {
	return nil
}
