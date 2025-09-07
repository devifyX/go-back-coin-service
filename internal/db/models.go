package db

import "time"

// Account mirrors the `public.coins` table in Postgres/Supabase.
type Account struct {
	ID               string     `json:"id"`
	Coins            int64      `json:"coins"`
	LastRechargeDate *time.Time `json:"lastRechargeDate"`
	LastUsageDate    *time.Time `json:"lastUsageDate"`
}
