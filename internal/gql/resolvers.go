package gql

import (
	"context"
	"errors"
	"time"

	"github.com/graphql-go/graphql"

	dbpkg "github.com/devifyX/go-back-coin-service/internal/db"
)

// Resolvers holds dependencies used by GraphQL resolvers.
type Resolvers struct {
	Store *dbpkg.Store

	// Optional: default timeouts per op
	QueryTimeout    time.Duration
	MutationTimeout time.Duration
}

func NewResolvers(store *dbpkg.Store) *Resolvers {
	return &Resolvers{
		Store:           store,
		QueryTimeout:    10 * time.Second,
		MutationTimeout: 10 * time.Second,
	}
}

func (r *Resolvers) qctx(p graphql.ResolveParams) (context.Context, context.CancelFunc) {
	timeout := r.QueryTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return context.WithTimeout(p.Context, timeout)
}

func (r *Resolvers) mctx(p graphql.ResolveParams) (context.Context, context.CancelFunc) {
	timeout := r.MutationTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return context.WithTimeout(p.Context, timeout)
}

// -------- Query resolvers --------

func (r *Resolvers) GetUser() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.qctx(p)
		defer cancel()
		id := p.Args["id"].(string)
		return r.Store.GetAccount(ctx, id)
	}
}

func (r *Resolvers) ListUsers() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.qctx(p)
		defer cancel()
		limit, _ := p.Args["limit"].(int)
		offset, _ := p.Args["offset"].(int)
		return r.Store.ListAccounts(ctx, limit, offset)
	}
}

func (r *Resolvers) GetBalance() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.qctx(p)
		defer cancel()
		id := p.Args["id"].(string)
		acct, err := r.Store.GetAccount(ctx, id)
		if err != nil || acct == nil {
			return nil, err
		}
		return int(acct.Coins), nil
	}
}

func (r *Resolvers) GetUsersByCoinsRange() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.qctx(p)
		defer cancel()
		var minPtr, maxPtr *int64
		if v, ok := p.Args["min"].(int); ok {
			vv := int64(v)
			minPtr = &vv
		}
		if v, ok := p.Args["max"].(int); ok {
			vv := int64(v)
			maxPtr = &vv
		}
		return r.Store.ListAccountsByCoinsRange(ctx, minPtr, maxPtr)
	}
}

func (r *Resolvers) GetRecentRecharges() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.qctx(p)
		defer cancel()
		since := p.Args["since"].(time.Time)
		return r.Store.ListRecentRecharges(ctx, since)
	}
}

func (r *Resolvers) GetInactiveSince() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.qctx(p)
		defer cancel()
		before := p.Args["before"].(time.Time)
		return r.Store.ListInactiveSince(ctx, before)
	}
}

func (r *Resolvers) CountUsers() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.qctx(p)
		defer cancel()
		n, err := r.Store.CountAccounts(ctx)
		return int(n), err
	}
}

func (r *Resolvers) TotalCoins() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.qctx(p)
		defer cancel()
		s, err := r.Store.SumCoins(ctx)
		return int(s), err
	}
}

func (r *Resolvers) ExistsUser() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.qctx(p)
		defer cancel()
		id := p.Args["id"].(string)
		return r.Store.UserExists(ctx, id)
	}
}

// -------- Mutation resolvers --------

func (r *Resolvers) CreateUser() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.mctx(p)
		defer cancel()
		id := p.Args["id"].(string)
		var coinsPtr *int64
		if v, ok := p.Args["coins"].(int); ok {
			vv := int64(v)
			coinsPtr = &vv
		}
		return r.Store.CreateAccount(ctx, id, coinsPtr)
	}
}

// RechargeCoins(id: ID!, amount: Int!, userId: ID!, dataId: String)
func (r *Resolvers) RechargeCoins() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.mctx(p)
		defer cancel()

		id := p.Args["id"].(string)
		amount := int64(p.Args["amount"].(int))

		userIDv, ok := p.Args["userId"].(string)
		if !ok || userIDv == "" {
			return nil, errors.New("userId (UUID) is required")
		}
		var dataID string
		if v, ok := p.Args["dataId"].(string); ok {
			dataID = v
		}

		return r.Store.Recharge(ctx, id, amount, userIDv, dataID)
	}
}

// BatchRecharge(ids: [ID!]!, amount: Int!, userId: ID!, dataId: String)
func (r *Resolvers) BatchRecharge() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.mctx(p)
		defer cancel()

		raw := p.Args["ids"].([]any)
		ids := make([]string, 0, len(raw))
		for _, v := range raw {
			ids = append(ids, v.(string))
		}
		amount := int64(p.Args["amount"].(int))

		userIDv, ok := p.Args["userId"].(string)
		if !ok || userIDv == "" {
			return nil, errors.New("userId (UUID) is required")
		}
		var baseDataID string
		if v, ok := p.Args["dataId"].(string); ok {
			baseDataID = v
		}

		n, err := r.Store.BatchRecharge(ctx, ids, amount, userIDv, baseDataID)
		return int(n), err
	}
}

// UseCoins(id: ID!, amount: Int!, userId: ID!, dataId: String)
func (r *Resolvers) UseCoins() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.mctx(p)
		defer cancel()

		id := p.Args["id"].(string)
		amount := int64(p.Args["amount"].(int))

		userIDv, ok := p.Args["userId"].(string)
		if !ok || userIDv == "" {
			return nil, errors.New("userId (UUID) is required")
		}
		var dataID string
		if v, ok := p.Args["dataId"].(string); ok {
			dataID = v
		}

		return r.Store.Use(ctx, id, amount, userIDv, dataID)
	}
}

// TransferCoins(fromId: ID!, toId: ID!, amount: Int!, userId: ID!, dataId: String)
func (r *Resolvers) TransferCoins() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.mctx(p)
		defer cancel()

		fromID := p.Args["fromId"].(string)
		toID := p.Args["toId"].(string)
		amount := int64(p.Args["amount"].(int))

		userIDv, ok := p.Args["userId"].(string)
		if !ok || userIDv == "" {
			return nil, errors.New("userId (UUID) is required")
		}
		var dataID string
		if v, ok := p.Args["dataId"].(string); ok {
			dataID = v
		}

		from, to, err := r.Store.Transfer(ctx, fromID, toID, amount, userIDv, dataID)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"from": from,
			"to":   to,
		}, nil
	}
}

// SetCoins(id: ID!, coins: Int!, userId: ID!, dataId: String)
func (r *Resolvers) SetCoins() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.mctx(p)
		defer cancel()

		id := p.Args["id"].(string)
		coins := int64(p.Args["coins"].(int))

		userIDv, ok := p.Args["userId"].(string)
		if !ok || userIDv == "" {
			return nil, errors.New("userId (UUID) is required")
		}
		var dataID string
		if v, ok := p.Args["dataId"].(string); ok {
			dataID = v
		}

		return r.Store.SetCoinsExact(ctx, id, coins, userIDv, dataID)
	}
}

func (r *Resolvers) TouchUsage() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.mctx(p)
		defer cancel()
		id := p.Args["id"].(string)
		return r.Store.TouchUsage(ctx, id)
	}
}

func (r *Resolvers) DeleteUser() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ctx, cancel := r.mctx(p)
		defer cancel()
		id := p.Args["id"].(string)
		return r.Store.DeleteAccount(ctx, id)
	}
}
