package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --------------------------------------------
// Notifier interface (implemented elsewhere)
// --------------------------------------------
const DefaultDataID = "0000000"

type TxNotifier interface {
	// Create posts a transaction record to the Transactions service.
	// - userID: the account/user id whose balance changed (MUST be UUID)
	// - dataID: an event identifier (e.g., "recharge:<id>:<ts>", "use:<id>:<ts>", "transfer:out:...") — should be unique(ish) per event
	// - coinID: logical coin/currency id, e.g., the coins row id
	// - platformName: the source system, e.g., "coin-service"
	// - coinUsed: positive amount of coins affected (we keep it positive for both inflow/outflow)
	// - ts/expiry: timestamps; expiry may be zero if you don’t use it
	Create(ctx context.Context, userID, dataID, coinID, platformName string, coinUsed float64, ts time.Time, expiry time.Time) error
}

// --------------------------------------------
// Store & Initialization
// --------------------------------------------

type Store struct {
	Pool     *pgxpool.Pool
	Notifier TxNotifier // optional; nil means notifications disabled
	Logger   *slog.Logger
}

// logger returns a usable logger.
func (s *Store) logger() *slog.Logger {
	if s == nil || s.Logger == nil {
		return slog.Default()
	}
	return s.Logger
}

// New creates a new Store from a Postgres connection string.
func New(ctx context.Context, connURL string) (*Store, error) {
	log := slog.Default()
	start := time.Now()
	log.Info("db.New: start")
	if connURL == "" {
		err := errors.New("db.New: connection url is empty")
		log.Error("db.New: invalid config", slog.String("error", err.Error()))
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(connURL)
	if err != nil {
		log.Error("db.New: parse config", slog.String("error", err.Error()))
		return nil, fmt.Errorf("db.New: parse config: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Error("db.New: connect", slog.String("error", err.Error()))
		return nil, fmt.Errorf("db.New: connect: %w", err)
	}
	log.Info("db.New: success", slog.Duration("dur", time.Since(start)))
	return &Store{Pool: pool, Logger: log}, nil
}

// NewFromPool lets you inject an existing pool (handy for tests).
func NewFromPool(pool *pgxpool.Pool) *Store {
	return &Store{Pool: pool}
}

// Close closes the underlying pool.
func (s *Store) Close() {
	log := s.logger()
	start := time.Now()
	if s.Pool != nil {
		log.Info("db.Close: closing pool")
		s.Pool.Close()
	}
	log.Info("db.Close: done", slog.Duration("dur", time.Since(start)))
}

// --------------------------------------------
// Schema & Models
// --------------------------------------------

// EnsureSchema creates the coins table if it doesn't exist.
func (s *Store) EnsureSchema(ctx context.Context) error {
	log := s.logger()
	start := time.Now()
	log.Info("EnsureSchema: ensure coins table")
	_, err := s.Pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS public.coins (
			id TEXT PRIMARY KEY,
			coins BIGINT NOT NULL DEFAULT 0,
			last_recharge_date TIMESTAMPTZ NULL,
			last_usage_date TIMESTAMPTZ NULL
		);
	`)
	if err != nil {
		log.Error("EnsureSchema: failed", slog.String("error", err.Error()))
		return err
	}
	log.Info("EnsureSchema: ok", slog.Duration("dur", time.Since(start)))
	return nil
}

// Account is assumed to exist elsewhere in this package.
// type Account struct {
// 	ID                string
// 	Coins             int64
// 	LastRechargeDate  *time.Time
// 	LastUsageDate     *time.Time
// }

// --------------------------------------------
// Helpers
// --------------------------------------------

func canonicalUUID(s string) (string, error) {
	u, err := uuid.Parse(strings.TrimSpace(s))
	if err != nil {
		return "", fmt.Errorf("invalid userID (must be UUID): %w", err)
	}
	return u.String(), nil
}

// internal helper to send transaction notifications
func (s *Store) notify(ctx context.Context, userID, coinID, dataID string, coinUsed float64, when time.Time) {
	l := s.logger()
	if l == nil {
		l = slog.Default()
	}
	l.Debug("notify: called",
		slog.String("userID_in", userID),
		slog.String("coinID", coinID),
		slog.String("dataID", dataID),
		slog.Float64("coinUsed", coinUsed),
		slog.Time("when", when),
	)

	if s.Notifier == nil {
		l.Debug("notify: notifier nil; skipping",
			slog.String("userID", userID),
			slog.String("dataID", dataID),
		)
		return
	}

	const platform = "coin-service"

	if err := s.Notifier.Create(ctx, userID, dataID, coinID, platform, coinUsed, when.UTC(), time.Time{}); err != nil {
		l.Error("notify: failed",
			slog.String("userID", userID),
			slog.String("dataID", dataID),
			slog.String("coinID", coinID),
			slog.Float64("coinUsed", coinUsed),
			slog.Time("when_utc", when.UTC()),
			slog.String("error", err.Error()),
		)
		return
	}

	l.Debug("notify: sent",
		slog.String("userID", userID),
		slog.String("dataID", dataID),
		slog.String("coinID", coinID),
		slog.Float64("coinUsed", coinUsed),
		slog.Time("when_utc", when.UTC()),
	)
}

// --------------------------------------------
// CRUD & Business Operations
// --------------------------------------------

func (s *Store) GetAccount(ctx context.Context, id string) (*Account, error) {
	log := s.logger()
	start := time.Now()
	log.Debug("GetAccount: query", slog.String("id", id))
	row := s.Pool.QueryRow(ctx, `
		SELECT id, coins, last_recharge_date, last_usage_date
		FROM public.coins WHERE id=$1
	`, id)
	var a Account
	if err := row.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Info("GetAccount: not found", slog.String("id", id), slog.Duration("dur", time.Since(start)))
			return nil, nil
		}
		log.Error("GetAccount: scan failed", slog.String("id", id), slog.String("error", err.Error()))
		return nil, err
	}
	log.Debug("GetAccount: ok", slog.String("id", id), slog.Int64("coins", a.Coins), slog.Duration("dur", time.Since(start)))
	return &a, nil
}

func (s *Store) ListAccounts(ctx context.Context, limit, offset int) ([]*Account, error) {
	log := s.logger()
	start := time.Now()
	origLimit, origOffset := limit, offset
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	log.Debug("ListAccounts: query", slog.Int("limit", limit), slog.Int("offset", offset), slog.Int("origLimit", origLimit), slog.Int("origOffset", origOffset))
	rows, err := s.Pool.Query(ctx, `
		SELECT id, coins, last_recharge_date, last_usage_date
		FROM public.coins
		ORDER BY id
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		log.Error("ListAccounts: query failed", slog.String("error", err.Error()))
		return nil, err
	}
	defer rows.Close()

	var out []*Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
			log.Error("ListAccounts: scan failed", slog.String("error", err.Error()))
			return nil, err
		}
		out = append(out, &a)
	}
	if err := rows.Err(); err != nil {
		log.Error("ListAccounts: rows err", slog.String("error", err.Error()))
		return nil, err
	}
	log.Debug("ListAccounts: ok", slog.Int("count", len(out)), slog.Duration("dur", time.Since(start)))
	return out, nil
}

func (s *Store) ListAccountsByCoinsRange(ctx context.Context, min, max *int64) ([]*Account, error) {
	log := s.logger()
	start := time.Now()
	log.Debug("ListAccountsByCoinsRange: start", slog.Any("min", min), slog.Any("max", max))
	q := `
		SELECT id, coins, last_recharge_date, last_usage_date
		FROM public.coins
		WHERE 1=1
	`
	args := []any{}
	i := 1
	if min != nil {
		q += fmt.Sprintf(" AND coins >= $%d", i)
		args = append(args, *min)
		i++
	}
	if max != nil {
		q += fmt.Sprintf(" AND coins <= $%d", i)
		args = append(args, *max)
		i++
	}
	q += " ORDER BY coins DESC, id ASC"

	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		log.Error("ListAccountsByCoinsRange: query failed", slog.String("error", err.Error()))
		return nil, err
	}
	defer rows.Close()
	var out []*Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
			log.Error("ListAccountsByCoinsRange: scan failed", slog.String("error", err.Error()))
			return nil, err
		}
		out = append(out, &a)
	}
	if err := rows.Err(); err != nil {
		log.Error("ListAccountsByCoinsRange: rows err", slog.String("error", err.Error()))
		return nil, err
	}
	log.Debug("ListAccountsByCoinsRange: ok", slog.Int("count", len(out)), slog.Duration("dur", time.Since(start)))
	return out, nil
}

func (s *Store) ListRecentRecharges(ctx context.Context, since time.Time) ([]*Account, error) {
	log := s.logger()
	start := time.Now()
	log.Debug("ListRecentRecharges: start", slog.Time("since", since))
	rows, err := s.Pool.Query(ctx, `
		SELECT id, coins, last_recharge_date, last_usage_date
		FROM public.coins
		WHERE last_recharge_date IS NOT NULL AND last_recharge_date >= $1
		ORDER BY last_recharge_date DESC
	`, since)
	if err != nil {
		log.Error("ListRecentRecharges: query failed", slog.String("error", err.Error()))
		return nil, err
	}
	defer rows.Close()

	var out []*Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
			log.Error("ListRecentRecharges: scan failed", slog.String("error", err.Error()))
			return nil, err
		}
		out = append(out, &a)
	}
	if err := rows.Err(); err != nil {
		log.Error("ListRecentRecharges: rows err", slog.String("error", err.Error()))
		return nil, err
	}
	log.Debug("ListRecentRecharges: ok", slog.Int("count", len(out)), slog.Duration("dur", time.Since(start)))
	return out, nil
}

func (s *Store) ListInactiveSince(ctx context.Context, before time.Time) ([]*Account, error) {
	log := s.logger()
	start := time.Now()
	log.Debug("ListInactiveSince: start", slog.Time("before", before))
	rows, err := s.Pool.Query(ctx, `
		SELECT id, coins, last_recharge_date, last_usage_date
		FROM public.coins
		WHERE last_usage_date IS NULL OR last_usage_date < $1
		ORDER BY last_usage_date NULLS FIRST, id
	`, before)
	if err != nil {
		log.Error("ListInactiveSince: query failed", slog.String("error", err.Error()))
		return nil, err
	}
	defer rows.Close()

	var out []*Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
			log.Error("ListInactiveSince: scan failed", slog.String("error", err.Error()))
			return nil, err
		}
		out = append(out, &a)
	}
	if err := rows.Err(); err != nil {
		log.Error("ListInactiveSince: rows err", slog.String("error", err.Error()))
		return nil, err
	}
	log.Debug("ListInactiveSince: ok", slog.Int("count", len(out)), slog.Duration("dur", time.Since(start)))
	return out, nil
}

func (s *Store) CountAccounts(ctx context.Context) (int64, error) {
	log := s.logger()
	start := time.Now()
	log.Debug("CountAccounts: start")
	var n int64
	if err := s.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.coins`).Scan(&n); err != nil {
		log.Error("CountAccounts: failed", slog.String("error", err.Error()))
		return 0, err
	}
	log.Debug("CountAccounts: ok", slog.Int64("count", n), slog.Duration("dur", time.Since(start)))
	return n, nil
}

func (s *Store) SumCoins(ctx context.Context) (int64, error) {
	log := s.logger()
	start := time.Now()
	log.Debug("SumCoins: start")
	var sum int64
	if err := s.Pool.QueryRow(ctx, `SELECT COALESCE(SUM(coins),0) FROM public.coins`).Scan(&sum); err != nil {
		log.Error("SumCoins: failed", slog.String("error", err.Error()))
		return 0, err
	}
	log.Debug("SumCoins: ok", slog.Int64("sum", sum), slog.Duration("dur", time.Since(start)))
	return sum, nil
}

func (s *Store) UserExists(ctx context.Context, id string) (bool, error) {
	log := s.logger()
	start := time.Now()
	log.Debug("UserExists: start", slog.String("id", id))
	var exists bool
	if err := s.Pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM public.coins WHERE id=$1)
	`, id).Scan(&exists); err != nil {
		log.Error("UserExists: failed", slog.String("id", id), slog.String("error", err.Error()))
		return false, err
	}
	log.Debug("UserExists: ok", slog.String("id", id), slog.Bool("exists", exists), slog.Duration("dur", time.Since(start)))
	return exists, nil
}

func (s *Store) CreateAccount(ctx context.Context, id string, coins *int64) (*Account, error) {
	log := s.logger()
	start := time.Now()
	initial := int64(0)
	if coins != nil {
		initial = *coins
	}
	log.Info("CreateAccount: start", slog.String("id", id), slog.Int64("initial", initial))
	if _, err := s.Pool.Exec(ctx, `
		INSERT INTO public.coins (id, coins) VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING
	`, id, initial); err != nil {
		log.Error("CreateAccount: insert failed", slog.String("id", id), slog.String("error", err.Error()))
		return nil, err
	}
	acc, err := s.GetAccount(ctx, id)
	if err != nil {
		log.Error("CreateAccount: readback failed", slog.String("id", id), slog.String("error", err.Error()))
		return nil, err
	}
	log.Info("CreateAccount: ok", slog.String("id", id), slog.Int64("coins", acc.Coins), slog.Duration("dur", time.Since(start)))
	return acc, nil
}

func (s *Store) DeleteAccount(ctx context.Context, id string) (bool, error) {
	log := s.logger()
	start := time.Now()
	log.Info("DeleteAccount: start", slog.String("id", id))
	tag, err := s.Pool.Exec(ctx, `DELETE FROM public.coins WHERE id=$1`, id)
	if err != nil {
		log.Error("DeleteAccount: failed", slog.String("id", id), slog.String("error", err.Error()))
		return false, err
	}
	ok := tag.RowsAffected() > 0
	log.Info("DeleteAccount: done", slog.String("id", id), slog.Bool("deleted", ok), slog.Duration("dur", time.Since(start)))
	return ok, nil
}

// SetCoinsExact sets the balance to an exact value and emits a transaction using the caller-provided userID (UUID) and dataID.
func (s *Store) SetCoinsExact(ctx context.Context, coinID string, coins int64, userID, dataID string) (*Account, error) {
	log := s.logger()
	start := time.Now()
	log.Info("SetCoinsExact: start",
		slog.String("coinID", coinID),
		slog.Int64("target", coins),
		slog.String("userID_in", userID),
		slog.String("dataID_in", dataID),
	)

	if strings.TrimSpace(userID) == "" {
		return nil, errors.New("userID is required (UUID)")
	}
	uid, err := canonicalUUID(userID)
	if err != nil {
		return nil, err
	}
	userID = uid

	// fetch current to compute delta
	cur, _ := s.GetAccount(ctx, coinID)

	if _, err := s.Pool.Exec(ctx, `
		UPDATE public.coins SET coins=$2 WHERE id=$1
	`, coinID, coins); err != nil {
		log.Error("SetCoinsExact: update failed", slog.String("coinID", coinID), slog.String("error", err.Error()))
		return nil, err
	}
	acc, err := s.GetAccount(ctx, coinID)
	if err != nil {
		log.Error("SetCoinsExact: readback failed", slog.String("coinID", coinID), slog.String("error", err.Error()))
		return nil, err
	}

	// emit transaction for the delta (positive number)
	if cur != nil && acc.Coins != cur.Coins {
		delta := acc.Coins - cur.Coins
		if delta < 0 {
			delta = -delta
		}
		if strings.TrimSpace(dataID) == "" {
			dataID = fmt.Sprintf("setexact:%s:%d", coinID, time.Now().UnixNano())
		}
		s.notify(ctx, userID, coinID, dataID, float64(delta), time.Now().UTC())
	}

	log.Info("SetCoinsExact: ok",
		slog.String("coinID", coinID),
		slog.Int64("coins", acc.Coins),
		slog.Duration("dur", time.Since(start)),
	)
	return acc, nil
}

// Recharge increases balance and emits a transaction using caller-provided userID (UUID) and dataID.
func (s *Store) Recharge(ctx context.Context, coinID string, amount int64, userID, dataID string) (*Account, error) {
	log := s.logger()
	start := time.Now()
	log.Info("Recharge: start",
		slog.String("coinID", coinID),
		slog.Int64("amount", amount),
		slog.String("userID_in", userID),
		slog.String("dataID_in", dataID),
	)
	if amount <= 0 {
		return nil, errors.New("recharge: amount must be > 0")
	}
	if strings.TrimSpace(userID) == "" {
		return nil, errors.New("userID is required (UUID)")
	}
	uid, err := canonicalUUID(userID)
	if err != nil {
		return nil, err
	}
	userID = uid

	if _, err := s.Pool.Exec(ctx, `
		UPDATE public.coins
		SET coins = coins + $2,
		    last_recharge_date = NOW()
		WHERE id=$1
	`, coinID, amount); err != nil {
		log.Error("Recharge: update failed", slog.String("coinID", coinID), slog.String("error", err.Error()))
		return nil, err
	}
	acc, err := s.GetAccount(ctx, coinID)
	if err != nil {
		log.Error("Recharge: readback failed", slog.String("coinID", coinID), slog.String("error", err.Error()))
		return nil, err
	}

	// Notify (positive amount)
	if strings.TrimSpace(dataID) == "" {
		dataID = fmt.Sprintf("recharge:%s:%d", coinID, time.Now().UnixNano())
	}
	s.notify(ctx, userID, coinID, dataID, float64(amount), time.Now().UTC())

	log.Info("Recharge: ok",
		slog.String("coinID", coinID),
		slog.Int64("coins", acc.Coins),
		slog.Duration("dur", time.Since(start)),
	)
	return acc, nil
}

// BatchRecharge increases balances for many coinIDs and emits per-id notifications using caller-provided userID (UUID) and baseDataID.
func (s *Store) BatchRecharge(ctx context.Context, coinIDs []string, amount int64, userID, baseDataID string) (int64, error) {
	log := s.logger()
	start := time.Now()
	log.Info("BatchRecharge: start",
		slog.Int("ids", len(coinIDs)),
		slog.Int64("amount", amount),
		slog.String("userID_in", userID),
		slog.String("baseDataID_in", baseDataID),
	)
	if amount <= 0 {
		return 0, errors.New("batchRecharge: amount must be > 0")
	}
	if strings.TrimSpace(userID) == "" {
		return 0, errors.New("userID is required (UUID)")
	}
	uid, err := canonicalUUID(userID)
	if err != nil {
		return 0, err
	}
	userID = uid

	tag, err := s.Pool.Exec(ctx, `
		UPDATE public.coins
		SET coins = coins + $2,
		    last_recharge_date = NOW()
		WHERE id = ANY($1)
	`, coinIDs, amount)
	if err != nil {
		log.Error("BatchRecharge: update failed", slog.String("error", err.Error()))
		return 0, err
	}

	// Per-id notifications
	now := time.Now().UTC()
	for _, cid := range coinIDs {
		dataID := baseDataID
		if strings.TrimSpace(dataID) == "" {
			dataID = fmt.Sprintf("batchrecharge:%s:%d", cid, now.UnixNano())
		} else {
			// make it unique-ish per id
			dataID = fmt.Sprintf("%s:%s", baseDataID, cid)
		}
		s.notify(ctx, userID, cid, dataID, float64(amount), now)
	}

	rows := tag.RowsAffected()
	log.Info("BatchRecharge: ok",
		slog.Int64("rowsAffected", rows),
		slog.Duration("dur", time.Since(start)),
	)
	return rows, nil
}

// Use decreases balance (depletion) and emits a transaction using caller-provided userID (UUID) and dataID.
func (s *Store) Use(ctx context.Context, coinID string, amount int64, userID, dataID string) (*Account, error) {
	log := s.logger()
	start := time.Now()
	log.Info("Use: start",
		slog.String("coinID", coinID),
		slog.Int64("amount", amount),
		slog.String("userID_in", userID),
		slog.String("dataID_in", dataID),
	)
	if amount <= 0 {
		return nil, errors.New("use: amount must be > 0")
	}
	if strings.TrimSpace(userID) == "" {
		return nil, errors.New("userID is required (UUID)")
	}
	uid, err := canonicalUUID(userID)
	if err != nil {
		return nil, err
	}
	userID = uid

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		log.Error("Use: begin tx failed", slog.String("error", err.Error()))
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var coins int64
	if err := tx.QueryRow(ctx, `
		SELECT coins FROM public.coins WHERE id=$1 FOR UPDATE
	`, coinID).Scan(&coins); err != nil {
		log.Error("Use: select failed", slog.String("coinID", coinID), slog.String("error", err.Error()))
		return nil, err
	}
	if coins < amount {
		return nil, fmt.Errorf("use: insufficient balance (have %d, need %d)", coins, amount)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE public.coins
		SET coins = coins - $2,
		    last_usage_date = NOW()
		WHERE id=$1
	`, coinID, amount); err != nil {
		log.Error("Use: update failed", slog.String("coinID", coinID), slog.String("error", err.Error()))
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		log.Error("Use: commit failed", slog.String("error", err.Error()))
		return nil, err
	}

	acc, err := s.GetAccount(ctx, coinID)
	if err != nil {
		log.Error("Use: readback failed", slog.String("coinID", coinID), slog.String("error", err.Error()))
		return nil, err
	}

	// Notify (positive amount)
	if strings.TrimSpace(dataID) == "" {
		dataID = fmt.Sprintf("use:%s:%d", coinID, time.Now().UnixNano())
	}
	s.notify(ctx, userID, coinID, dataID, float64(amount), time.Now().UTC())

	log.Info("Use: ok",
		slog.String("coinID", coinID),
		slog.Int64("coins", acc.Coins),
		slog.Duration("dur", time.Since(start)),
	)
	return acc, nil
}

// Transfer moves coins between ids and emits two notifications using caller-provided userID (UUID) and dataID.
func (s *Store) Transfer(ctx context.Context, fromID, toID string, amount int64, userID, dataID string) (*Account, *Account, error) {
	log := s.logger()
	start := time.Now()
	log.Info("Transfer: start",
		slog.String("from", fromID),
		slog.String("to", toID),
		slog.Int64("amount", amount),
		slog.String("userID_in", userID),
		slog.String("dataID_in", dataID),
	)
	if amount <= 0 {
		return nil, nil, errors.New("transfer: amount must be > 0")
	}
	if strings.TrimSpace(userID) == "" {
		return nil, nil, errors.New("userID is required (UUID)")
	}
	uid, err := canonicalUUID(userID)
	if err != nil {
		return nil, nil, err
	}
	userID = uid

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		log.Error("Transfer: begin tx failed", slog.String("error", err.Error()))
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var fromCoins int64
	if err := tx.QueryRow(ctx, `
		SELECT coins FROM public.coins WHERE id=$1 FOR UPDATE
	`, fromID).Scan(&fromCoins); err != nil {
		log.Error("Transfer: select from failed", slog.String("from", fromID), slog.String("error", err.Error()))
		return nil, nil, err
	}
	if fromCoins < amount {
		return nil, nil, fmt.Errorf("transfer: insufficient balance on %s", fromID)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE public.coins
		SET coins = coins - $2,
		    last_usage_date = NOW()
		WHERE id=$1
	`, fromID, amount); err != nil {
		log.Error("Transfer: debit failed", slog.String("from", fromID), slog.String("error", err.Error()))
		return nil, nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE public.coins
		SET coins = coins + $2,
		    last_recharge_date = NOW()
		WHERE id=$1
	`, toID, amount); err != nil {
		log.Error("Transfer: credit failed", slog.String("to", toID), slog.String("error", err.Error()))
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		log.Error("Transfer: commit failed", slog.String("error", err.Error()))
		return nil, nil, err
	}

	from, err := s.GetAccount(ctx, fromID)
	if err != nil {
		log.Error("Transfer: readback from failed", slog.String("from", fromID), slog.String("error", err.Error()))
		return nil, nil, err
	}
	to, err := s.GetAccount(ctx, toID)
	if err != nil {
		log.Error("Transfer: readback to failed", slog.String("to", toID), slog.String("error", err.Error()))
		return nil, nil, err
	}

	// Notifications (both positive coinUsed), keep event ids distinct
	now := time.Now().UTC()
	outDataID := dataID
	inDataID := dataID
	if strings.TrimSpace(outDataID) == "" {
		outDataID = fmt.Sprintf("transfer:out:%s->%s:%d", fromID, toID, now.UnixNano())
	}
	if strings.TrimSpace(inDataID) == "" {
		inDataID = fmt.Sprintf("transfer:in:%s->%s:%d", fromID, toID, now.UnixNano())
	} else {
		// suffix to avoid identical data ids for two events
		inDataID = inDataID + ":in"
		outDataID = outDataID + ":out"
	}

	s.notify(ctx, userID, fromID, outDataID, float64(amount), now)
	s.notify(ctx, userID, toID, inDataID, float64(amount), now)

	log.Info("Transfer: ok",
		slog.String("from", fromID),
		slog.String("to", toID),
		slog.Int64("amount", amount),
		slog.Duration("dur", time.Since(start)),
	)
	return from, to, nil
}

func (s *Store) TouchUsage(ctx context.Context, id string) (*Account, error) {
	log := s.logger()
	start := time.Now()
	log.Debug("TouchUsage: start", slog.String("id", id))
	if _, err := s.Pool.Exec(ctx, `
		UPDATE public.coins SET last_usage_date = NOW() WHERE id=$1
	`, id); err != nil {
		log.Error("TouchUsage: update failed", slog.String("id", id), slog.String("error", err.Error()))
		return nil, err
	}
	acc, err := s.GetAccount(ctx, id)
	if err != nil {
		log.Error("TouchUsage: readback failed", slog.String("id", id), slog.String("error", err.Error()))
		return nil, err
	}
	log.Debug("TouchUsage: ok", slog.String("id", id), slog.Duration("dur", time.Since(start)))
	return acc, nil
}
