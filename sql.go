package main

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	_ "github.com/mattn/go-sqlite3"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/stellar/go/support/db"
)

//go:embed migrations/*.sql
var migrations embed.FS

type sqlEventsDB struct {
	session            *db.Session
	maxIngestBatchSize int
	maxQueryBatchSize  int
}

func newSQLiteEventsDB(dbFilePath string) (eventsDB, error) {
	session, err := db.Open("sqlite3", dbFilePath)
	if err != nil {
		return nil, err
	}

	return &sqlEventsDB{
		session:            session,
		maxIngestBatchSize: 150,
		maxQueryBatchSize:  999,
	}, nil
}

func runMigrations(db *sql.DB) error {
	m := &migrate.AssetMigrationSource{
		Asset: migrations.ReadFile,
		AssetDir: func() func(string) ([]string, error) {
			return func(path string) ([]string, error) {
				dirEntry, err := migrations.ReadDir(path)
				if err != nil {
					return nil, err
				}
				entries := make([]string, 0)
				for _, e := range dirEntry {
					entries = append(entries, e.Name())
				}

				return entries, nil
			}
		}(),
		Dir: "migrations",
	}
	_, err := migrate.ExecMax(db, "postgres", m, migrate.Up, 0)
	return err
}
func (e *sqlEventsDB) ingest(inputJSON io.Reader) (time.Duration, error) {
	if err := runMigrations(e.session.DB.DB); err != nil {
		return 0, err
	}

	start := time.Now()
	numEvents := 0
	decoder := json.NewDecoder(inputJSON)

	builder := db.BatchInsertBuilder{
		Table:        e.session.GetTable("contract_events"),
		MaxBatchSize: e.maxIngestBatchSize,
	}
	topicBuilder := db.BatchInsertBuilder{
		Table:        e.session.GetTable("contract_event_topics"),
		MaxBatchSize: e.maxIngestBatchSize,
	}

	for {
		var event types.Log
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		numEvents++
		id := (event.BlockNumber << 32) | uint64(event.Index)
		var buf bytes.Buffer
		if err := event.EncodeRLP(&buf); err != nil {
			return 0, err
		}
		err := builder.Row(context.Background(), map[string]interface{}{
			"id":   id,
			"body": hex.EncodeToString(buf.Bytes()),
		})
		if err != nil {
			return 0, err
		}

		row := map[string]interface{}{
			"event_id":    id,
			"contract_id": event.Address.String(),
		}
		for i := 0; i < len(event.Topics); i++ {
			row["topic"+strconv.Itoa(i)] = event.Topics[i].String()
		}
		for i := len(event.Topics); i < 4; i++ {
			row["topic"+strconv.Itoa(i)] = nil
		}
		err = topicBuilder.Row(context.Background(), row)
		if err != nil {
			return 0, err
		}
	}

	if err := builder.Exec(context.Background()); err != nil {
		return 0, err
	}

	if err := topicBuilder.Exec(context.Background()); err != nil {
		return 0, err
	}

	duration := time.Since(start)
	return duration, nil
}

func (e *sqlEventsDB) query(q eventsQuery) (time.Duration, error) {
	startTime := time.Now()

	startEventID := q.startBlock << 32
	endEventID := (q.endBlock + 1) << 32

	sql := sq.Select("event_id").From("contract_event_topics").
		Where("contract_id = ?", q.target.String()).
		Where("event_id >= ?", startEventID).
		Where("event_id < ?", endEventID).
		OrderBy("event_id ASC").
		Limit(uint64(q.limit))

	var results []uint64
	if err := e.session.Select(context.Background(), &results, sql); err != nil {
		return 0, err
	}
	parsed := make([]types.Log, len(results))
	offset := 0

	for i := 0; i < len(results); i += e.maxQueryBatchSize {
		end := i + e.maxQueryBatchSize
		if end > len(results) {
			end = len(results)
		}
		subset := results[i:end]
		if len(subset) == 0 {
			break
		}

		sql = sq.Select("body").From("contract_events").Where(map[string]interface{}{
			"id": subset,
		})

		var events []string
		if err := e.session.Select(context.Background(), &events, sql); err != nil {
			return 0, err
		}
		for j, event := range events {
			if raw, err := hex.DecodeString(event); err != nil {
				return 0, err
			} else if err := rlp.DecodeBytes(raw, &parsed[offset+j]); err != nil {
				return 0, err
			}
		}
		offset += len(events)
	}
	duration := time.Since(startTime)

	return duration, nil
}

func (e *sqlEventsDB) close() error {
	return e.session.Close()
}
