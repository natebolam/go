package history

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/guregu/null"
	"github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/services/horizon/internal/db2/sqx"
	"github.com/stellar/go/services/horizon/internal/toid"
	"github.com/stellar/go/services/horizon/internal/utf8"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

func (t *Transaction) IsSuccessful() bool {
	if t.Successful == nil {
		return true
	}

	return *t.Successful
}

// TransactionByHash is a query that loads a single row from the
// `history_transactions` table based upon the provided hash.
func (q *Q) TransactionByHash(dest interface{}, hash string) error {
	sql := selectTransaction.
		Limit(1).
		Where("ht.transaction_hash = ?", hash)

	return q.Get(dest, sql)
}

// TransactionsByIDs fetches transactions from the `history_transactions` table
// which match the given ids
func (q *Q) TransactionsByIDs(ids ...int64) (map[int64]Transaction, error) {
	if len(ids) == 0 {
		return nil, errors.New("no id arguments provided")
	}

	sql := selectTransaction.Where(map[string]interface{}{
		"ht.id": ids,
	})

	var transactions []Transaction
	if err := q.Select(&transactions, sql); err != nil {
		return nil, err
	}

	byID := map[int64]Transaction{}
	for _, transaction := range transactions {
		byID[transaction.TotalOrderID.ID] = transaction
	}

	return byID, nil
}

// Transactions provides a helper to filter rows from the `history_transactions`
// table with pre-defined filters.  See `TransactionsQ` methods for the
// available filters.
func (q *Q) Transactions() *TransactionsQ {
	return &TransactionsQ{
		parent:        q,
		sql:           selectTransaction,
		includeFailed: false,
	}
}

// ForAccount filters the transactions collection to a specific account
func (q *TransactionsQ) ForAccount(aid string) *TransactionsQ {
	var account Account
	q.Err = q.parent.AccountByAddress(&account, aid)
	if q.Err != nil {
		return q
	}

	q.sql = q.sql.
		Join("history_transaction_participants htp ON htp.history_transaction_id = ht.id").
		Where("htp.history_account_id = ?", account.ID)

	return q
}

// ForLedger filters the query to a only transactions in a specific ledger,
// specified by its sequence.
func (q *TransactionsQ) ForLedger(seq int32) *TransactionsQ {
	var ledger Ledger
	q.Err = q.parent.LedgerBySequence(&ledger, seq)
	if q.Err != nil {
		return q
	}

	start := toid.ID{LedgerSequence: seq}
	end := toid.ID{LedgerSequence: seq + 1}
	q.sql = q.sql.Where(
		"ht.id >= ? AND ht.id < ?",
		start.ToInt64(),
		end.ToInt64(),
	)

	return q
}

// IncludeFailed changes the query to include failed transactions.
func (q *TransactionsQ) IncludeFailed() *TransactionsQ {
	q.includeFailed = true
	return q
}

// Page specifies the paging constraints for the query being built by `q`.
func (q *TransactionsQ) Page(page db2.PageQuery) *TransactionsQ {
	if q.Err != nil {
		return q
	}

	q.sql, q.Err = page.ApplyTo(q.sql, "ht.id")
	return q
}

// Select loads the results of the query specified by `q` into `dest`.
func (q *TransactionsQ) Select(dest interface{}) error {
	if q.Err != nil {
		return q.Err
	}

	if !q.includeFailed {
		q.sql = q.sql.
			Where("(ht.successful = true OR ht.successful IS NULL)")
	}

	q.Err = q.parent.Select(dest, q.sql)
	if q.Err != nil {
		return q.Err
	}

	transactions, ok := dest.(*[]Transaction)
	if !ok {
		return errors.New("dest is not *[]Transaction")
	}

	for _, t := range *transactions {
		var resultXDR xdr.TransactionResult
		err := xdr.SafeUnmarshalBase64(t.TxResult, &resultXDR)
		if err != nil {
			return err
		}

		if !q.includeFailed {
			if !t.IsSuccessful() {
				return errors.Errorf("Corrupted data! `include_failed=false` but returned transaction is failed: %s", t.TransactionHash)
			}

			if resultXDR.Result.Code != xdr.TransactionResultCodeTxSuccess {
				return errors.Errorf("Corrupted data! `include_failed=false` but returned transaction is failed: %s %s", t.TransactionHash, t.TxResult)
			}
		}

		// Check if `successful` equals resultXDR
		if t.IsSuccessful() && resultXDR.Result.Code != xdr.TransactionResultCodeTxSuccess {
			return errors.Errorf("Corrupted data! `successful=true` but returned transaction is not success: %s %s", t.TransactionHash, t.TxResult)
		}

		if !t.IsSuccessful() && resultXDR.Result.Code == xdr.TransactionResultCodeTxSuccess {
			return errors.Errorf("Corrupted data! `successful=false` but returned transaction is success: %s %s", t.TransactionHash, t.TxResult)
		}
	}

	return nil
}

// TransactionBatchInsertBuilder is used to insert transactions into the
// exp_history_transactions table
type TransactionBatchInsertBuilder interface {
	Add(transaction io.LedgerTransaction, sequence uint32) error
	Exec() error
}

// transactionBatchInsertBuilder is a simple wrapper around db.BatchInsertBuilder
type transactionBatchInsertBuilder struct {
	builder db.BatchInsertBuilder
}

// NewTransactionBatchInsertBuilder constructs a new TransactionBatchInsertBuilder instance
func (q *Q) NewTransactionBatchInsertBuilder(maxBatchSize int) TransactionBatchInsertBuilder {
	return &transactionBatchInsertBuilder{
		builder: db.BatchInsertBuilder{
			Table:        q.GetTable("exp_history_transactions"),
			MaxBatchSize: maxBatchSize,
		},
	}
}

// QTransactions defines transaction related queries.
type QTransactions interface {
	NewTransactionBatchInsertBuilder(maxBatchSize int) TransactionBatchInsertBuilder
}

// Add adds a new transaction to the batch
func (i *transactionBatchInsertBuilder) Add(transaction io.LedgerTransaction, sequence uint32) error {
	m, err := transactionToMap(transaction, sequence)
	if err != nil {
		return err
	}

	return i.builder.Row(m)
}

func (i *transactionBatchInsertBuilder) Exec() error {
	return i.builder.Exec()
}

func formatTimeBounds(transaction io.LedgerTransaction) interface{} {
	if transaction.Envelope.Tx.TimeBounds == nil {
		return nil
	}

	if transaction.Envelope.Tx.TimeBounds.MaxTime == 0 {
		return sq.Expr("int8range(?,?)", transaction.Envelope.Tx.TimeBounds.MinTime, nil)
	}

	maxTime := transaction.Envelope.Tx.TimeBounds.MaxTime
	if maxTime > math.MaxInt64 {
		maxTime = math.MaxInt64
	}

	return sq.Expr("int8range(?,?)", transaction.Envelope.Tx.TimeBounds.MinTime, maxTime)
}

func signatures(transaction io.LedgerTransaction) []string {
	signatures := make([]string, len(transaction.Envelope.Signatures))
	for i, sig := range transaction.Envelope.Signatures {
		signatures[i] = base64.StdEncoding.EncodeToString(sig.Signature)
	}
	return signatures
}

func memoType(transaction io.LedgerTransaction) string {
	switch transaction.Envelope.Tx.Memo.Type {
	case xdr.MemoTypeMemoNone:
		return "none"
	case xdr.MemoTypeMemoText:
		return "text"
	case xdr.MemoTypeMemoId:
		return "id"
	case xdr.MemoTypeMemoHash:
		return "hash"
	case xdr.MemoTypeMemoReturn:
		return "return"
	default:
		panic(fmt.Errorf("invalid memo type: %v", transaction.Envelope.Tx.Memo.Type))
	}
}

func memo(transaction io.LedgerTransaction) null.String {
	var (
		value string
		valid bool
	)
	switch transaction.Envelope.Tx.Memo.Type {
	case xdr.MemoTypeMemoNone:
		value, valid = "", false
	case xdr.MemoTypeMemoText:
		scrubbed := utf8.Scrub(transaction.Envelope.Tx.Memo.MustText())
		notnull := strings.Join(strings.Split(scrubbed, "\x00"), "")
		value, valid = notnull, true
	case xdr.MemoTypeMemoId:
		value, valid = fmt.Sprintf("%d", transaction.Envelope.Tx.Memo.MustId()), true
	case xdr.MemoTypeMemoHash:
		hash := transaction.Envelope.Tx.Memo.MustHash()
		value, valid =
			base64.StdEncoding.EncodeToString(hash[:]),
			true
	case xdr.MemoTypeMemoReturn:
		hash := transaction.Envelope.Tx.Memo.MustRetHash()
		value, valid =
			base64.StdEncoding.EncodeToString(hash[:]),
			true
	default:
		panic(fmt.Errorf("invalid memo type: %v", transaction.Envelope.Tx.Memo.Type))
	}

	return null.NewString(value, valid)
}

func transactionToMap(transaction io.LedgerTransaction, sequence uint32) (map[string]interface{}, error) {
	envelopeBase64, err := xdr.MarshalBase64(transaction.Envelope)
	if err != nil {
		return nil, err
	}
	resultBase64, err := xdr.MarshalBase64(transaction.Result.Result)
	if err != nil {
		return nil, err
	}
	metaBase64, err := xdr.MarshalBase64(transaction.Meta)
	if err != nil {
		return nil, err
	}
	feeMetaBase64, err := xdr.MarshalBase64(transaction.FeeChanges)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"id":                toid.New(int32(sequence), int32(transaction.Index), 0).ToInt64(),
		"transaction_hash":  hex.EncodeToString(transaction.Result.TransactionHash[:]),
		"ledger_sequence":   sequence,
		"application_order": int32(transaction.Index),
		"account":           transaction.Envelope.Tx.SourceAccount.Address(),
		"account_sequence":  strconv.FormatInt(int64(transaction.Envelope.Tx.SeqNum), 10),
		"max_fee":           int32(transaction.Envelope.Tx.Fee),
		"fee_charged":       int32(transaction.Result.Result.FeeCharged),
		"operation_count":   int32(len(transaction.Envelope.Tx.Operations)),
		"tx_envelope":       envelopeBase64,
		"tx_result":         resultBase64,
		"tx_meta":           metaBase64,
		"tx_fee_meta":       feeMetaBase64,
		"signatures":        sqx.StringArray(signatures(transaction)),
		"time_bounds":       formatTimeBounds(transaction),
		"memo_type":         memoType(transaction),
		"memo":              memo(transaction),
		"created_at":        time.Now().UTC(),
		"updated_at":        time.Now().UTC(),
		"successful":        transaction.Result.Result.Result.Code == xdr.TransactionResultCodeTxSuccess,
	}, nil
}

var selectTransaction = sq.Select(
	"ht.id, " +
		"ht.transaction_hash, " +
		"ht.ledger_sequence, " +
		"ht.application_order, " +
		"ht.account, " +
		"ht.account_sequence, " +
		"ht.max_fee, " +
		// `fee_charged` is NULL by default, DB needs to be reingested
		// to populate the value. If value is not present display `max_fee`.
		"COALESCE(ht.fee_charged, ht.max_fee) as fee_charged, " +
		"ht.operation_count, " +
		"ht.tx_envelope, " +
		"ht.tx_result, " +
		"ht.tx_meta, " +
		"ht.tx_fee_meta, " +
		"ht.created_at, " +
		"ht.updated_at, " +
		"ht.successful, " +
		"array_to_string(ht.signatures, ',') AS signatures, " +
		"ht.memo_type, " +
		"ht.memo, " +
		"lower(ht.time_bounds) AS valid_after, " +
		"upper(ht.time_bounds) AS valid_before, " +
		"hl.closed_at AS ledger_close_time").
	From("history_transactions ht").
	LeftJoin("history_ledgers hl ON ht.ledger_sequence = hl.sequence")
