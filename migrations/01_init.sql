-- +migrate Up
CREATE TABLE contract_events (
                                 id bigint NOT NULL PRIMARY KEY,
                                 body TEXT
);

CREATE TABLE contract_event_topics (
                                       contract_id TEXT NOT NULL,
                                       event_id bigint NOT NULL,
                                       topic0 TEXT,
                                       topic1 TEXT,
                                       topic2 TEXT,
                                       topic3 TEXT
);
CREATE INDEX events_by_contract_id ON contract_event_topics (contract_id, event_id);
CREATE INDEX events_by_topic0 ON contract_event_topics (topic0, event_id);
CREATE INDEX events_by_topic1 ON contract_event_topics (topic1, event_id);
CREATE INDEX events_by_topic2 ON contract_event_topics (topic2, event_id);
CREATE INDEX events_by_topic3 ON contract_event_topics (topic3, event_id);


-- +migrate Down
drop table contract_event_topics cascade;
drop table contract_events cascade;