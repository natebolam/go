package history

import (
	"database/sql"
	"encoding/hex"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/guregu/null"
	"github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/services/horizon/internal/db2/sqx"
	"github.com/stellar/go/services/horizon/internal/test"
	"github.com/stellar/go/services/horizon/internal/toid"
	"github.com/stellar/go/xdr"
)

func TestTransactionQueries(t *testing.T) {
	tt := test.Start(t).Scenario("base")
	defer tt.Finish()
	q := &Q{tt.HorizonSession()}

	// Test TransactionByHash
	var tx Transaction
	real := "2374e99349b9ef7dba9a5db3339b78fda8f34777b1af33ba468ad5c0df946d4d"
	err := q.TransactionByHash(&tx, real)
	tt.Assert.NoError(err)

	fake := "not_real"
	err = q.TransactionByHash(&tx, fake)
	tt.Assert.Equal(err, sql.ErrNoRows)
}

// TestTransactionSuccessfulOnly tests if default query returns successful
// transactions only.
// If it's not enclosed in brackets, it may return incorrect result when mixed
// with `ForAccount` or `ForLedger` filters.
func TestTransactionSuccessfulOnly(t *testing.T) {
	tt := test.Start(t).Scenario("failed_transactions")
	defer tt.Finish()

	var transactions []Transaction

	q := &Q{tt.HorizonSession()}
	query := q.Transactions().
		ForAccount("GA5WBPYA5Y4WAEHXWR2UKO2UO4BUGHUQ74EUPKON2QHV4WRHOIRNKKH2")

	err := query.Select(&transactions)
	tt.Assert.NoError(err)

	tt.Assert.Equal(3, len(transactions))

	for _, transaction := range transactions {
		tt.Assert.True(*transaction.Successful)
	}

	sql, _, err := query.sql.ToSql()
	tt.Assert.NoError(err)
	// Note: brackets around `(ht.successful = true OR ht.successful IS NULL)` are critical!
	tt.Assert.Contains(sql, "WHERE htp.history_account_id = ? AND (ht.successful = true OR ht.successful IS NULL)")
}

// TestTransactionIncludeFailed tests `IncludeFailed` method.
func TestTransactionIncludeFailed(t *testing.T) {
	tt := test.Start(t).Scenario("failed_transactions")
	defer tt.Finish()

	var transactions []Transaction

	q := &Q{tt.HorizonSession()}
	query := q.Transactions().
		ForAccount("GA5WBPYA5Y4WAEHXWR2UKO2UO4BUGHUQ74EUPKON2QHV4WRHOIRNKKH2").
		IncludeFailed()

	err := query.Select(&transactions)
	tt.Assert.NoError(err)

	var failed, successful int
	for _, transaction := range transactions {
		if *transaction.Successful {
			successful++
		} else {
			failed++
		}
	}

	tt.Assert.Equal(3, successful)
	tt.Assert.Equal(1, failed)

	sql, _, err := query.sql.ToSql()
	tt.Assert.NoError(err)
	tt.Assert.Equal("SELECT ht.id, ht.transaction_hash, ht.ledger_sequence, ht.application_order, ht.account, ht.account_sequence, ht.max_fee, COALESCE(ht.fee_charged, ht.max_fee) as fee_charged, ht.operation_count, ht.tx_envelope, ht.tx_result, ht.tx_meta, ht.tx_fee_meta, ht.created_at, ht.updated_at, ht.successful, array_to_string(ht.signatures, ',') AS signatures, ht.memo_type, ht.memo, lower(ht.time_bounds) AS valid_after, upper(ht.time_bounds) AS valid_before, hl.closed_at AS ledger_close_time FROM history_transactions ht LEFT JOIN history_ledgers hl ON ht.ledger_sequence = hl.sequence JOIN history_transaction_participants htp ON htp.history_transaction_id = ht.id WHERE htp.history_account_id = ?", sql)
}

func TestExtraChecksTransactionSuccessfulTrueResultFalse(t *testing.T) {
	tt := test.Start(t).Scenario("failed_transactions")
	defer tt.Finish()

	// successful `true` but tx result `false`
	_, err := tt.HorizonDB.Exec(
		`UPDATE history_transactions SET successful = true WHERE transaction_hash = 'aa168f12124b7c196c0adaee7c73a64d37f99428cacb59a91ff389626845e7cf'`,
	)
	tt.Require.NoError(err)

	var transactions []Transaction

	q := &Q{tt.HorizonSession()}
	query := q.Transactions().
		ForAccount("GA5WBPYA5Y4WAEHXWR2UKO2UO4BUGHUQ74EUPKON2QHV4WRHOIRNKKH2").
		IncludeFailed()

	err = query.Select(&transactions)
	tt.Assert.Error(err)
	tt.Assert.Contains(err.Error(), "Corrupted data! `successful=true` but returned transaction is not success")
}

func TestExtraChecksTransactionSuccessfulFalseResultTrue(t *testing.T) {
	tt := test.Start(t).Scenario("failed_transactions")
	defer tt.Finish()

	// successful `false` but tx result `true`
	_, err := tt.HorizonDB.Exec(
		`UPDATE history_transactions SET successful = false WHERE transaction_hash = 'a2dabf4e9d1642722602272e178a37c973c9177b957da86192a99b3e9f3a9aa4'`,
	)
	tt.Require.NoError(err)

	var transactions []Transaction

	q := &Q{tt.HorizonSession()}
	query := q.Transactions().
		ForAccount("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON").
		IncludeFailed()

	err = query.Select(&transactions)
	tt.Assert.Error(err)
	tt.Assert.Contains(err.Error(), "Corrupted data! `successful=false` but returned transaction is success")
}

func insertTransaction(
	tt *test.T, q *Q, tableName string, transaction io.LedgerTransaction, sequence int32,
) {
	m, err := transactionToMap(transaction, uint32(sequence))
	tt.Assert.NoError(err)
	insertSQL := sq.Insert(tableName).SetMap(m)
	_, err = q.Exec(insertSQL)
	tt.Assert.NoError(err)
}

func TestCheckExpTransactions(t *testing.T) {
	tt := test.Start(t)
	defer tt.Finish()
	test.ResetHorizonDB(t, tt.HorizonDB)
	q := &Q{tt.HorizonSession()}

	transaction := io.LedgerTransaction{
		Index:      1,
		Envelope:   xdr.TransactionEnvelope{},
		Result:     xdr.TransactionResultPair{},
		FeeChanges: xdr.LedgerEntryChanges{},
		Meta:       xdr.TransactionMeta{},
	}
	err := xdr.SafeUnmarshalBase64(
		"AAAAACiSTRmpH6bHC6Ekna5e82oiGY5vKDEEUgkq9CB//t+rAAAAZAEXUhsAADDGAAAAAQAAAAAAAAAAAAAAAF3v3WAAAAABAAAACjEwOTUzMDMyNTAAAAAAAAEAAAAAAAAAAQAAAAAOr5CG1ax6qG2fBEgXJlF0sw5W0irOS6N/NRDbavBm4QAAAAAAAAAAE32fwAAAAAAAAAABf/7fqwAAAEAkWgyAgV5tF3m1y1TIDYkNXP8pZLAwcxhWEi4f3jcZJK7QrKSXhKoawVGrp5NNs4y9dgKt8zHZ8KbJreFBUsIB",
		&transaction.Envelope,
	)
	tt.Assert.NoError(err)
	err = xdr.SafeUnmarshalBase64(
		"AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAA=",
		&transaction.Result.Result,
	)
	tt.Assert.NoError(err)
	err = xdr.SafeUnmarshalBase64(
		"AAAAAAAAAAMAAAACAAAAAAAAAAMAAAAAAAAAABbxCy3mLg3hiTqX4VUEEp60pFOrJNxYM1JtxXTwXhY2AAAAAAvrwgAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAMAAAAAAAAAAAGUcmKO5465JxTSLQOQljwk2SfqAJmZSG6JH6wtqpwhDeC2s5t4PNQAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAwAAAAEAAAADAAAAAAAAAAABlHJijueOuScU0i0DkJY8JNkn6gCZmUhuiR+sLaqcIQAAAAAL68IAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAMAAAADAAAAAAAAAAAW8Qst5i4N4Yk6l+FVBBKetKRTqyTcWDNSbcV08F4WNgAAAAAL68IAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAAADAAAAAAAAAAAW8Qst5i4N4Yk6l+FVBBKetKRTqyTcWDNSbcV08F4WNg3gtrObeDzUAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAAABAAAAAwAAAAAAAAAAAZRyYo7njrknFNItA5CWPCTZJ+oAmZlIbokfrC2qnCEAAAAAC+vCAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		&transaction.Meta,
	)
	tt.Assert.NoError(err)

	_, err = hex.Decode(transaction.Result.TransactionHash[:], []byte("ea1e96dd4aa8a16e357842b0fcdb66c1ab03fade7dcd3a99f88ed28ea8c30f6a"))
	tt.Assert.NoError(err)

	otherTransaction := io.LedgerTransaction{
		Index:      2,
		Envelope:   xdr.TransactionEnvelope{},
		Result:     xdr.TransactionResultPair{},
		FeeChanges: xdr.LedgerEntryChanges{},
		Meta:       xdr.TransactionMeta{},
	}
	err = xdr.SafeUnmarshalBase64(
		"AAAAAAGUcmKO5465JxTSLQOQljwk2SfqAJmZSG6JH6wtqpwhAAABLAAAAAAAAAABAAAAAAAAAAEAAAALaGVsbG8gd29ybGQAAAAAAwAAAAAAAAAAAAAAABbxCy3mLg3hiTqX4VUEEp60pFOrJNxYM1JtxXTwXhY2AAAAAAvrwgAAAAAAAAAAAQAAAAAW8Qst5i4N4Yk6l+FVBBKetKRTqyTcWDNSbcV08F4WNgAAAAAN4Lazj4x61AAAAAAAAAAFAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABLaqcIQAAAEBKwqWy3TaOxoGnfm9eUjfTRBvPf34dvDA0Nf+B8z4zBob90UXtuCqmQqwMCyH+okOI3c05br3khkH0yP4kCwcE",
		&otherTransaction.Envelope,
	)
	tt.Assert.NoError(err)
	err = xdr.SafeUnmarshalBase64(
		"AAAAAAAAASwAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAFAAAAAAAAAAA=",
		&otherTransaction.Result.Result,
	)
	tt.Assert.NoError(err)
	err = xdr.SafeUnmarshalBase64(
		"AAAAAAAAAAMAAAACAAAAAAAAHqEAAAAAAAAAAB+lHtRjj4+h2/0Tj8iBQiaUDzLo4oRCLyUnytFHzAyIAAAAAAvrwgAAAB6hAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAQAAHqEAAAAAAAAAABbxCy3mLg3hiTqX4VUEEp60pFOrJNxYM1JtxXTwXhY2DeC2s4+MeHwAAAADAAAAAgAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAB6hAAAAAAAAAACzMOD+8iU8qo+qbTYewT8lxKE/s1cE3FOCVWxsqJ74GwAAAAAL68IAAAAeoQAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAB6hAAAAAAAAAAAW8Qst5i4N4Yk6l+FVBBKetKRTqyTcWDNSbcV08F4WNg3gtrODoLZ8AAAAAwAAAAIAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAeoQAAAAAAAAAASZcLtOTqf+cdbsq8HmLMkeqU06LN94UTWXuSBem5Z88AAAAAC+vCAAAAHqEAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAeoQAAAAAAAAAAFvELLeYuDeGJOpfhVQQSnrSkU6sk3FgzUm3FdPBeFjYN4Lazd7T0fAAAAAMAAAACAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAA=",
		&otherTransaction.Meta,
	)
	tt.Assert.NoError(err)

	_, err = hex.Decode(otherTransaction.Result.TransactionHash[:], []byte("3389e9f0f1a65f19736cacf544c2e825313e8447f569233bb8db39aa607c8889"))
	tt.Assert.NoError(err)

	sequence := int32(123)
	valid, err := q.CheckExpTransactions(sequence)
	tt.Assert.True(valid)
	tt.Assert.NoError(err)

	insertTransaction(tt, q, "exp_history_transactions", transaction, sequence)
	insertTransaction(tt, q, "exp_history_transactions", otherTransaction, sequence)
	insertTransaction(tt, q, "history_transactions", transaction, sequence)
	insertTransaction(tt, q, "history_transactions", otherTransaction, sequence)

	ledger := Ledger{
		Sequence:                   sequence,
		LedgerHash:                 "4db1e4f145e9ee75162040d26284795e0697e2e84084624e7c6c723ebbf80118",
		PreviousLedgerHash:         null.NewString("4b0b8bace3b2438b2404776ce57643966855487ba6384724a3c664c7aa4cd9e4", true),
		TotalOrderID:               TotalOrderID{toid.New(int32(69859), 0, 0).ToInt64()},
		ImporterVersion:            321,
		TransactionCount:           12,
		SuccessfulTransactionCount: new(int32),
		FailedTransactionCount:     new(int32),
		OperationCount:             23,
		TotalCoins:                 23451,
		FeePool:                    213,
		BaseReserve:                687,
		MaxTxSetSize:               345,
		ProtocolVersion:            12,
		BaseFee:                    100,
		ClosedAt:                   time.Now().UTC().Truncate(time.Second),
		LedgerHeaderXDR:            null.NewString("temp", true),
	}
	*ledger.SuccessfulTransactionCount = 12
	*ledger.FailedTransactionCount = 3
	insertSQL := sq.Insert("history_ledgers").SetMap(ledgerToMap(ledger))
	_, err = q.Exec(insertSQL)
	tt.Assert.NoError(err)

	valid, err = q.CheckExpTransactions(sequence)
	tt.Assert.True(valid)
	tt.Assert.NoError(err)

	for fieldName, value := range map[string]interface{}{
		"id":               999,
		"transaction_hash": "hash",
		"account":          "account",
		"account_sequence": "999",
		"max_fee":          999,
		"fee_charged":      999,
		"operation_count":  999,
		"tx_envelope":      "envelope",
		"tx_result":        "result",
		"tx_meta":          "meta",
		"tx_fee_meta":      "fee_meta",
		"signatures":       sqx.StringArray([]string{"sig1", "sig2"}),
		"time_bounds":      sq.Expr("int8range(?,?)", 123, 456),
		"memo_type":        "invalid",
		"memo":             "invalid-memo",
		"successful":       false,
	} {
		updateSQL := sq.Update("history_transactions").
			Set(fieldName, value).
			Where(
				"ledger_sequence = ? AND application_order = ?",
				sequence, otherTransaction.Index,
			)
		_, err = q.Exec(updateSQL)
		tt.Assert.NoError(err)

		valid, err = q.CheckExpTransactions(sequence)
		tt.Assert.NoError(err)
		tt.Assert.False(valid)

		_, err = q.Exec(sq.Delete("history_transactions").
			Where(
				"ledger_sequence = ? AND application_order = ?",
				sequence, otherTransaction.Index,
			))
		tt.Assert.NoError(err)

		insertTransaction(tt, q, "history_transactions", otherTransaction, sequence)

		valid, err := q.CheckExpTransactions(ledger.Sequence)
		tt.Assert.NoError(err)
		tt.Assert.True(valid)
	}
}
