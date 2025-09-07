// main_test.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/graphql-go/handler"
	"github.com/joho/godotenv"

	dbpkg "coin-service/internal/db"
	gqlpkg "coin-service/internal/gql"
	mw "coin-service/internal/middleware"
)

// ---------- Test scaffolding ----------

type gqlResp struct {
	Data   map[string]any `json:"data"`
	Errors any            `json:"errors"`
}

func doGQL(t *testing.T, srv *httptest.Server, query string, vars map[string]any) gqlResp {
	t.Helper()
	body := map[string]any{"query": query}
	if vars != nil {
		body["variables"] = vars
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(srv.URL+"/graphql", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /graphql: %v", err)
	}
	defer resp.Body.Close()
	var out gqlResp
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func setupServer(t *testing.T) (*httptest.Server, *dbpkg.Store) {
	t.Helper()

	_ = godotenv.Load() // ok if not present

	conn := os.Getenv("DATABASE_URL")
	if conn == "" {
		t.Skip("DATABASE_URL not set; skipping integration test")
	}

	ctx := context.Background()
	store, err := dbpkg.New(ctx, conn)
	if err != nil {
		t.Fatalf("db connect: %v", err)
	}

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Clean slate for test run
	if _, err := store.Pool.Exec(ctx, `TRUNCATE TABLE public.coins`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Build GraphQL schema + handler
	resolvers := gqlpkg.NewResolvers(store)
	resolvers.QueryTimeout = 10 * time.Second
	resolvers.MutationTimeout = 10 * time.Second

	schema, err := gqlpkg.NewSchema(resolvers)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	gqlHandler := handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: false, // not needed for test
	})

	// Rate limit middleware (generous for tests to avoid 429)
	rl := mw.NewRateLimiter()
	defaultQueryCfg := mw.RateCfg{PerMinute: 600, Burst: 300}
	defaultMutationCfg := mw.RateCfg{PerMinute: 300, Burst: 150}
	apiOverrides := map[string]mw.RateCfg{} // no special limits for tests

	rateLimited := mw.GraphQLRateLimit(rl, defaultQueryCfg, defaultMutationCfg, apiOverrides)(gqlHandler)

	mux := http.NewServeMux()
	mux.Handle("/graphql", rateLimited)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := httptest.NewServer(mux)
	return srv, store
}

// ---------- The actual test ----------

func TestGraphQL_AllAPIs(t *testing.T) {
	srv, store := setupServer(t)
	defer srv.Close()
	defer store.Close()

	// 1) Create users u1 and u2
	create := `
	  mutation($id:ID!,$coins:Int){
	    createUser(id:$id, coins:$coins){ id coins }
	  }
	`
	_ = doGQL(t, srv, create, map[string]any{"id": "u1", "coins": 100})
	_ = doGQL(t, srv, create, map[string]any{"id": "u2", "coins": 50})

	// 2) getUser
	getUser := `query($id:ID!){ getUser(id:$id){ id coins lastRechargeDate lastUsageDate } }`
	res := doGQL(t, srv, getUser, map[string]any{"id": "u1"})
	if res.Data == nil || res.Data["getUser"] == nil {
		t.Fatalf("expected getUser data, got: %#v", res)
	}

	// 3) listUsers
	list := `query{ listUsers(limit:10, offset:0){ id coins } }`
	lr := doGQL(t, srv, list, nil)
	if lr.Data == nil || lr.Data["listUsers"] == nil {
		t.Fatalf("expected listUsers data")
	}

	// 4) getBalance
	bal := doGQL(t, srv, `query($id:ID!){ getBalance(id:$id) }`, map[string]any{"id": "u1"})
	if bal.Data == nil || bal.Data["getBalance"] == nil {
		t.Fatalf("expected getBalance data")
	}

	// 5) rechargeCoins u2 +25
	rec := doGQL(t, srv, `mutation($id:ID!,$amt:Int!){ rechargeCoins(id:$id, amount:$amt){ id coins lastRechargeDate } }`,
		map[string]any{"id": "u2", "amt": 25})
	if rec.Data == nil || rec.Data["rechargeCoins"] == nil {
		t.Fatalf("expected rechargeCoins data")
	}

	// 6) useCoins u1 -10
	use := doGQL(t, srv, `mutation($id:ID!,$amt:Int!){ useCoins(id:$id, amount:$amt){ id coins lastUsageDate } }`,
		map[string]any{"id": "u1", "amt": 10})
	if use.Data == nil || use.Data["useCoins"] == nil {
		t.Fatalf("expected useCoins data")
	}

	// 7) transferCoins 40 u1 -> u2
	tr := doGQL(t, srv, `mutation($f:ID!,$t:ID!,$a:Int!){
	  transferCoins(fromId:$f, toId:$t, amount:$a){ from{ id coins } to{ id coins } }
	}`, map[string]any{"f": "u1", "t": "u2", "a": 40})
	if tr.Data == nil || tr.Data["transferCoins"] == nil {
		t.Fatalf("expected transferCoins data")
	}

	// 8) batchRecharge +5 for u1,u2
	br := doGQL(t, srv, `mutation($ids:[ID!]!,$amt:Int!){
	  batchRecharge(ids:$ids, amount:$amt)
	}`, map[string]any{"ids": []string{"u1", "u2"}, "amt": 5})
	if br.Data == nil || br.Data["batchRecharge"] == nil {
		t.Fatalf("expected batchRecharge data")
	}

	// 9) setCoins u2 = 7
	sc := doGQL(t, srv, `mutation($id:ID!,$c:Int!){
	  setCoins(id:$id, coins:$c){ id coins }
	}`, map[string]any{"id": "u2", "c": 7})
	if sc.Data == nil || sc.Data["setCoins"] == nil {
		t.Fatalf("expected setCoins data")
	}

	// 10) touchUsage u1
	tu := doGQL(t, srv, `mutation($id:ID!){
	  touchUsage(id:$id){ id lastUsageDate }
	}`, map[string]any{"id": "u1"})
	if tu.Data == nil || tu.Data["touchUsage"] == nil {
		t.Fatalf("expected touchUsage data")
	}

	// 11) getUsersByCoinsRange(min:5)
	cr := doGQL(t, srv, `query{
	  getUsersByCoinsRange(min:5){ id coins }
	}`, nil)
	if cr.Data == nil || cr.Data["getUsersByCoinsRange"] == nil {
		t.Fatalf("expected getUsersByCoinsRange data")
	}

	// 12) getRecentRecharges(since: now - 2h)
	since := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339)
	rr := doGQL(t, srv, `query($s:DateTime!){
	  getRecentRecharges(since:$s){ id lastRechargeDate }
	}`, map[string]any{"s": since})
	if rr.Data == nil || rr.Data["getRecentRecharges"] == nil {
		t.Fatalf("expected getRecentRecharges data")
	}

	// 13) getInactiveSince(before: now + 1s)
	before := time.Now().Add(1 * time.Second).UTC().Format(time.RFC3339)
	ina := doGQL(t, srv, `query($b:DateTime!){
	  getInactiveSince(before:$b){ id }
	}`, map[string]any{"b": before})
	if ina.Data == nil || ina.Data["getInactiveSince"] == nil {
		t.Fatalf("expected getInactiveSince data")
	}

	// 14) countUsers & totalCoins
	stats := doGQL(t, srv, `query{ countUsers totalCoins }`, nil)
	if stats.Data == nil || stats.Data["countUsers"] == nil || stats.Data["totalCoins"] == nil {
		t.Fatalf("expected stats data")
	}

	// 15) existsUser
	ex := doGQL(t, srv, `query{ existsUser(id:"u1") }`, nil)
	if ex.Data == nil || ex.Data["existsUser"] == nil {
		t.Fatalf("expected existsUser data")
	}

	// 16) deleteUser u2
	del := doGQL(t, srv, `mutation{ deleteUser(id:"u2") }`, nil)
	if del.Data == nil || del.Data["deleteUser"] == nil {
		t.Fatalf("expected deleteUser data")
	}
}
