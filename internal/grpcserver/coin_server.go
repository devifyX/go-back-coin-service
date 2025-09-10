package grpcserver

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	coinsv1 "github.com/devifyX/go-back-coin-service/api/coinsv1"
	dbpkg "github.com/devifyX/go-back-coin-service/internal/db"
)

type CoinsServer struct {
	coinsv1.UnimplementedCoinsServiceServer
	Store *dbpkg.Store
}

func NewCoinsServer(store *dbpkg.Store) *CoinsServer {
	return &CoinsServer{Store: store}
}

func (s *CoinsServer) CreateAccount(ctx context.Context, req *coinsv1.CreateRequest) (*coinsv1.AccountReply, error) {
	if strings.TrimSpace(req.GetId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var initPtr *int64
	if req.Initial != 0 {
		v := req.Initial
		initPtr = &v
	}
	acct, err := s.Store.CreateAccount(ctx, req.Id, initPtr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	return toReply(acct), nil
}

func (s *CoinsServer) Deplete(ctx context.Context, req *coinsv1.DepleteRequest) (*coinsv1.AccountReply, error) {
	if strings.TrimSpace(req.GetId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if req.Amount <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be > 0")
	}
	if strings.TrimSpace(req.GetUserId()) == "" {
		// DB layer also validates UUID, but we fail fast if empty.
		return nil, status.Error(codes.InvalidArgument, "user_id (UUID) is required")
	}

	// Pass through to DB; it will validate user_id as UUID and use data_id (optional).
	acct, err := s.Store.Use(ctx, req.Id, req.Amount, req.GetUserId(), req.GetDataId())
	if err != nil {
		switch {
		case isInsufficient(err):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "deplete: %v", err)
		}
	}
	return toReply(acct), nil
}

func toReply(a *dbpkg.Account) *coinsv1.AccountReply {
	if a == nil {
		return &coinsv1.AccountReply{}
	}
	var lr, lu string
	if a.LastRechargeDate != nil {
		lr = a.LastRechargeDate.UTC().Format(time.RFC3339)
	}
	if a.LastUsageDate != nil {
		lu = a.LastUsageDate.UTC().Format(time.RFC3339)
	}
	return &coinsv1.AccountReply{
		Id:               a.ID,
		Coins:            a.Coins,
		LastRechargeDate: lr,
		LastUsageDate:    lu,
	}
}

func isInsufficient(err error) bool {
	// crude check for the error we return in db.Use()
	return err != nil && strings.Contains(err.Error(), "insufficient balance")
}
