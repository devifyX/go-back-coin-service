package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/graphql-go/handler"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"

	dbpkg "github.com/devifyX/go-back-coin-service/internal/db"
	gqlpkg "github.com/devifyX/go-back-coin-service/internal/gql"
	mw "github.com/devifyX/go-back-coin-service/internal/middleware"

	coinsv1 "github.com/devifyX/go-back-coin-service/api/coinsv1"
	grpcserver "github.com/devifyX/go-back-coin-service/internal/grpcserver"
)

func mustGetEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s not set", key)
	}
	return v
}

func main() {
	// --- Load env
	if err := godotenv.Load(); err != nil {
		log.Printf("warning: no .env file found")
	}

	// --- DB setup
	ctx := context.Background()
	connURL := mustGetEnv("DATABASE_URL") // uses .env
	store, err := dbpkg.New(ctx, connURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer store.Close()

	if err := store.EnsureSchema(ctx); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}

	// --- GraphQL setup
	resolvers := gqlpkg.NewResolvers(store)
	resolvers.QueryTimeout = 10 * time.Second
	resolvers.MutationTimeout = 10 * time.Second

	schema, err := gqlpkg.NewSchema(resolvers)
	if err != nil {
		log.Fatalf("build schema: %v", err)
	}

	gqlHandler := handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: true, // GET /graphql shows GraphiQL
	})

	// --- HTTP rate limit middleware configuration
	rl := mw.NewRateLimiter()
	defaultQueryCfg := mw.RateCfg{PerMinute: 60, Burst: 30}
	defaultMutationCfg := mw.RateCfg{PerMinute: 20, Burst: 10}
	apiOverrides := map[string]mw.RateCfg{
		"deleteUser":    {PerMinute: 5, Burst: 2},
		"rechargeCoins": {PerMinute: 30, Burst: 15},
		"useCoins":      {PerMinute: 60, Burst: 30},
		"batchRecharge": {PerMinute: 10, Burst: 5},
		"transferCoins": {PerMinute: 20, Burst: 10},
	}
	rateLimited := mw.GraphQLRateLimit(rl, defaultQueryCfg, defaultMutationCfg, apiOverrides)(gqlHandler)

	// --- HTTP routes (GraphQL + health)
	mux := http.NewServeMux()
	mux.Handle("/graphql", rateLimited)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// Optional: redirect root to /graphql
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/graphql", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})

	// --- gRPC server (CreateAccount, Deplete)
	grpcSrv := grpc.NewServer()
	coinsSvc := grpcserver.NewCoinsServer(store)
	coinsv1.RegisterCoinsServiceServer(grpcSrv, coinsSvc)

	// Start gRPC in background on :7090
	go func() {
		lis, err := net.Listen("tcp", ":7090")
		if err != nil {
			log.Fatalf("gRPC listen: %v", err)
		}
		log.Printf("gRPC listening on :7090")
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("gRPC serve: %v", err)
		}
	}()

	// --- Start HTTP server on :7080
	addr := ":7080"
	log.Printf("HTTP listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
