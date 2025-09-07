package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a pgx connection pool and exposes DB operations.
type Store struct {
	Pool *pgxpool.Pool
}

// New creates a new Store from a Postgres connection string.
func New(ctx context.Context, connURL string) (*Store, error) {
	if connURL == "" {
		return nil, errors.New("db.New: connection url is empty")
	}
	cfg, err := pgxpool.ParseConfig(connURL)
	if err != nil {
		return nil, fmt.Errorf("db.New: parse config: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db.New: connect: %w", err)
	}
	return &Store{Pool: pool}, nil
}

// NewFromPool lets you inject an existing pool (handy for tests).
func NewFromPool(pool *pgxpool.Pool) *Store {
	return &Store{Pool: pool}
}

// Close closes the underlying pool.
func (s *Store) Close() {
	if s.Pool != nil {
		s.Pool.Close()
	}
}

// EnsureSchema creates the accounts table if it doesn't exist.
func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS public.coins (
			id TEXT PRIMARY KEY,
			coins BIGINT NOT NULL DEFAULT 0,
			last_recharge_date TIMESTAMPTZ NULL,
			last_usage_date TIMESTAMPTZ NULL
		);
	`)
	return err
}

// ---------- CRUD & Business Operations ----------

func (s *Store) GetAccount(ctx context.Context, id string) (*Account, error) {
	row := s.Pool.QueryRow(ctx, `
		SELECT id, coins, last_recharge_date, last_usage_date
		FROM public.coins WHERE id=$1
	`, id)
	var a Account
	if err := row.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &a, nil
}

func (s *Store) ListAccounts(ctx context.Context, limit, offset int) ([]*Account, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT id, coins, last_recharge_date, last_usage_date
		FROM public.coins
		ORDER BY id
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
			return nil, err
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}

func (s *Store) ListAccountsByCoinsRange(ctx context.Context, min, max *int64) ([]*Account, error) {
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
		return nil, err
	}
	defer rows.Close()
	var out []*Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
			return nil, err
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}

func (s *Store) ListRecentRecharges(ctx context.Context, since time.Time) ([]*Account, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, coins, last_recharge_date, last_usage_date
		FROM public.coins
		WHERE last_recharge_date IS NOT NULL AND last_recharge_date >= $1
		ORDER BY last_recharge_date DESC
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
			return nil, err
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}

func (s *Store) ListInactiveSince(ctx context.Context, before time.Time) ([]*Account, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, coins, last_recharge_date, last_usage_date
		FROM public.coins
		WHERE last_usage_date IS NULL OR last_usage_date < $1
		ORDER BY last_usage_date NULLS FIRST, id
	`, before)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Coins, &a.LastRechargeDate, &a.LastUsageDate); err != nil {
			return nil, err
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}

func (s *Store) CountAccounts(ctx context.Context) (int64, error) {
	var n int64
	if err := s.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.coins`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *Store) SumCoins(ctx context.Context) (int64, error) {
	var sum int64
	if err := s.Pool.QueryRow(ctx, `SELECT COALESCE(SUM(coins),0) FROM public.coins`).Scan(&sum); err != nil {
		return 0, err
	}
	return sum, nil
}

func (s *Store) UserExists(ctx context.Context, id string) (bool, error) {
	var exists bool
	if err := s.Pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM public.coins WHERE id=$1)
	`, id).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *Store) CreateAccount(ctx context.Context, id string, coins *int64) (*Account, error) {
	initial := int64(0)
	if coins != nil {
		initial = *coins
	}
	if _, err := s.Pool.Exec(ctx, `
		INSERT INTO public.coins (id, coins) VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING
	`, id, initial); err != nil {
		return nil, err
	}
	return s.GetAccount(ctx, id)
}

func (s *Store) DeleteAccount(ctx context.Context, id string) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM public.coins WHERE id=$1`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *Store) SetCoinsExact(ctx context.Context, id string, coins int64) (*Account, error) {
	if _, err := s.Pool.Exec(ctx, `
		UPDATE public.coins SET coins=$2 WHERE id=$1
	`, id, coins); err != nil {
		return nil, err
	}
	return s.GetAccount(ctx, id)
}

func (s *Store) Recharge(ctx context.Context, id string, amount int64) (*Account, error) {
	if amount <= 0 {
		return nil, errors.New("recharge: amount must be > 0")
	}
	if _, err := s.Pool.Exec(ctx, `
		UPDATE public.coins
		SET coins = coins + $2,
		    last_recharge_date = NOW()
		WHERE id=$1
	`, id, amount); err != nil {
		return nil, err
	}
	return s.GetAccount(ctx, id)
}

func (s *Store) BatchRecharge(ctx context.Context, ids []string, amount int64) (int64, error) {
	if amount <= 0 {
		return 0, errors.New("batchRecharge: amount must be > 0")
	}
	tag, err := s.Pool.Exec(ctx, `
		UPDATE public.coins
		SET coins = coins + $2,
		    last_recharge_date = NOW()
		WHERE id = ANY($1)
	`, ids, amount)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) Use(ctx context.Context, id string, amount int64) (*Account, error) {
	if amount <= 0 {
		return nil, errors.New("use: amount must be > 0")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var coins int64
	if err := tx.QueryRow(ctx, `
		SELECT coins FROM public.coins WHERE id=$1 FOR UPDATE
	`, id).Scan(&coins); err != nil {
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
	`, id, amount); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.GetAccount(ctx, id)
}

func (s *Store) Transfer(ctx context.Context, fromID, toID string, amount int64) (*Account, *Account, error) {
	if amount <= 0 {
		return nil, nil, errors.New("transfer: amount must be > 0")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	var fromCoins int64
	if err := tx.QueryRow(ctx, `
		SELECT coins FROM public.coins WHERE id=$1 FOR UPDATE
	`, fromID).Scan(&fromCoins); err != nil {
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
		return nil, nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE public.coins
		SET coins = coins + $2,
		    last_recharge_date = NOW()
		WHERE id=$1
	`, toID, amount); err != nil {
		return nil, nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	from, err := s.GetAccount(ctx, fromID)
	if err != nil {
		return nil, nil, err
	}
	to, err := s.GetAccount(ctx, toID)
	if err != nil {
		return nil, nil, err
	}
	return from, to, nil
}

func (s *Store) TouchUsage(ctx context.Context, id string) (*Account, error) {
	if _, err := s.Pool.Exec(ctx, `
		UPDATE public.coins SET last_usage_date = NOW() WHERE id=$1
	`, id); err != nil {
		return nil, err
	}
	return s.GetAccount(ctx, id)
}
