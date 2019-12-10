-- +migrate Up

CREATE TABLE exp_history_transactions (
    LIKE history_transactions
    including defaults
    including constraints
    including indexes
);

-- +migrate Down

DROP TABLE exp_history_transactions cascade;
