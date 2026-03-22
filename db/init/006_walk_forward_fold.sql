BEGIN;

SET search_path TO nseml, public;

CREATE TABLE IF NOT EXISTS nseml.walk_forward_fold (
    fold_id          serial PRIMARY KEY,
    wf_session_id    varchar(128) NOT NULL
        REFERENCES nseml.paper_session(session_id) ON DELETE CASCADE,
    fold_index       int NOT NULL,
    train_start      date NOT NULL,
    train_end        date NOT NULL,
    test_start       date NOT NULL,
    test_end         date NOT NULL,
    exp_id           varchar(64),
    status           text,
    total_return_pct numeric(10, 4),
    max_drawdown_pct numeric(10, 4),
    profit_factor    numeric(10, 4),
    total_trades     int,
    created_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_walk_forward_fold_session_index UNIQUE (wf_session_id, fold_index)
);

CREATE INDEX IF NOT EXISTS idx_walk_forward_fold_session
    ON nseml.walk_forward_fold (wf_session_id);

COMMIT;
