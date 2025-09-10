package db

import "time"

// Account represents a row in public.coins
type Account struct {
	ID               string     `db:"id" json:"id"`
	Coins            int64      `db:"coins" json:"coins"`
	LastRechargeDate *time.Time `db:"last_recharge_date" json:"lastRechargeDate"`
	LastUsageDate    *time.Time `db:"last_usage_date" json:"lastUsageDate"`
}
