package txnotify

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	transactionsv1 "github.com/devifyX/go-back-transaction-service/proto"
)

// Notifier is the interface your db.Store expects (matches TxNotifier).
type Notifier interface {
	Create(ctx context.Context, userID, dataID, coinID, platformName string, coinUsed float64, ts time.Time, expiry time.Time) error
	Close() error
}

type GRPCNotifier struct {
	conn   *grpc.ClientConn
	client transactionsv1.TransactionsClient

	// Optional defaults
	DefaultCoinID      string
	DefaultPlatform    string
	DefaultExpiryAfter time.Duration
}

// NewGRPC dials the Transactions gRPC service at addr (e.g. "localhost:9090").
// For secure comms, pass grpc.WithTransportCredentials(...). For local/dev, grpc.WithInsecure() is fine.
func NewGRPC(addr string, opts ...grpc.DialOption) (*GRPCNotifier, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithInsecure()}
	}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &GRPCNotifier{
		conn:               conn,
		client:             transactionsv1.NewTransactionsClient(conn),
		DefaultExpiryAfter: 24 * time.Hour,
	}, nil
}

func (n *GRPCNotifier) Close() error {
	if n.conn != nil {
		return n.conn.Close()
	}
	return nil
}

func (n *GRPCNotifier) Create(ctx context.Context, userID, dataID, coinID, platformName string, coinUsed float64, ts time.Time, expiry time.Time) error {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	if expiry.IsZero() {
		expiry = ts.Add(n.DefaultExpiryAfter)
	}
	if coinID == "" {
		coinID = n.DefaultCoinID
	}
	if platformName == "" {
		platformName = n.DefaultPlatform
	}

	// Idempotency key (optional, helpful if server enforces uniqueness)
	src := fmt.Sprintf("%s|%s|%s|%.8f|%d", userID, dataID, coinID, coinUsed, ts.UnixNano())
	sum := sha1.Sum([]byte(src))
	idemKey := hex.EncodeToString(sum[:])

	md := metadata.Pairs(
		"x-user-id", "coin-service",
		"x-idempotency-key", idemKey,
	)
	ctx = metadata.NewOutgoingContext(ctx, md)

	_, err := n.client.CreateTransaction(ctx, &transactionsv1.CreateTransactionRequest{
		Coinid:               coinID,
		Userid:               userID,
		Dataid:               dataID,
		Coinused:             coinUsed,
		TransactionTimestamp: timestamppb.New(ts.UTC()),
		ExpiryDate:           timestamppb.New(expiry.UTC()),
		PlatformName:         platformName,
	})
	return err
}
