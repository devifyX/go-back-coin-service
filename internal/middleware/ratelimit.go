package middleware

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"golang.org/x/time/rate"
)

// RateCfg controls a token-bucket limiter.
type RateCfg struct {
	PerMinute int // tokens replenished per minute
	Burst     int // bucket size
}

type rateKey struct {
	Client string // client identifier (IP or X-Forwarded-For)
	API    string // top-level GraphQL field name (e.g., getUser, rechargeCoins)
}

// RateLimiter stores per-(client,api) token buckets.
type RateLimiter struct {
	mu       sync.Mutex
	limiters map[rateKey]*rate.Limiter
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{limiters: make(map[rateKey]*rate.Limiter)}
}

func (rl *RateLimiter) limiterFor(k rateKey, cfg RateCfg) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if l, ok := rl.limiters[k]; ok {
		return l
	}
	perSec := rate.Limit(float64(cfg.PerMinute) / 60.0)
	l := rate.NewLimiter(perSec, cfg.Burst)
	rl.limiters[k] = l
	return l
}

// gqlRequest is a minimal GraphQL HTTP payload shape.
type gqlRequest struct {
	Query string `json:"query"`
}

// Extract top-level field names from a GraphQL document (best-effort).
var spaceRE = regexp.MustCompile(`\s+`)

func extractAPIs(query string) (opType string, fields []string) {
	q := spaceRE.ReplaceAllString(query, " ")
	q = strings.TrimSpace(q)
	brace := strings.Index(q, "{")
	if brace == -1 {
		return "query", nil
	}
	head := strings.ToLower(strings.TrimSpace(q[:brace]))
	if strings.HasPrefix(head, "mutation") {
		opType = "mutation"
	} else {
		opType = "query"
	}

	depth := 0
	var b strings.Builder
	flush := func() {
		if b.Len() == 0 {
			return
		}
		s := b.String()
		// remove alias if present: alias: field
		if idx := strings.Index(s, ":"); idx != -1 {
			s = strings.TrimSpace(s[idx+1:])
		}
		s = strings.TrimSpace(s)
		if s != "" &&
			s != "query" &&
			s != "mutation" &&
			s != "subscription" &&
			s != "fragment" &&
			s != "on" {
			fields = append(fields, s)
		}
		b.Reset()
	}

	inName := false
	for _, r := range q[brace:] {
		switch {
		case r == '{':
			depth++
			inName = false
			flush()
		case r == '}':
			depth--
			inName = false
			flush()
		case depth == 1 && (r == '(' || r == ' ' || r == '\n' || r == '\t' || r == '{' || r == ',' ):
			inName = false
			flush()
		case depth == 1:
			if !inName {
				inName = true
				b.Reset()
			}
			b.WriteRune(r)
		}
	}
	flush()

	// de-dup
	seen := map[string]struct{}{}
	uniq := fields[:0]
	for _, f := range fields {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			uniq = append(uniq, f)
		}
	}
	fields = uniq
	return
}

// Identify the client for rate-limiting (trusts first X-Forwarded-For hop if present).
func clientKey(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	h, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return h
	}
	return r.RemoteAddr
}

// GraphQLRateLimit returns a middleware that applies per-API, per-client rate limits.
// Usage:
//   rl := middleware.NewRateLimiter()
//   mw := middleware.GraphQLRateLimit(rl, queryCfg, mutationCfg, overrides)
//   http.Handle("/graphql", mw(yourGraphQLHandler))
func GraphQLRateLimit(
	rl *RateLimiter,
	defaultQuery RateCfg,
	defaultMutation RateCfg,
	apiOverrides map[string]RateCfg,
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Only gate POST GraphQL requests (allow GET for GraphiQL).
			if r.Method != http.MethodPost {
				next.ServeHTTP(w, r)
				return
			}

			// Read and clone the body so we can pass it downstream.
			var body bytes.Buffer
			if _, err := body.ReadFrom(http.MaxBytesReader(w, r.Body, 1<<20)); err != nil {
				http.Error(w, "request too large or unreadable", http.StatusBadRequest)
				return
			}
			r.Body = ioNopCloser(bytes.NewReader(body.Bytes()))

			// Parse query for rate buckets.
			var req gqlRequest
			_ = json.Unmarshal(body.Bytes(), &req)
			opType, fields := extractAPIs(req.Query)
			if len(fields) == 0 {
				// No fields detected, allow through.
				next.ServeHTTP(w, r)
				return
			}

			client := clientKey(r)
			denied := make([]string, 0, len(fields))
			for _, f := range fields {
				cfg, ok := apiOverrides[f]
				if !ok {
					if opType == "mutation" {
						cfg = defaultMutation
					} else {
						cfg = defaultQuery
					}
				}
				k := rateKey{Client: client, API: f}
				if !rl.limiterFor(k, cfg).Allow() {
					denied = append(denied, f)
				}
			}

			if len(denied) > 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":       "rate limit exceeded",
					"deniedAPIs":  denied,
					"retryAdvice": "retry later or contact server admin for higher limits",
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// Small shim to avoid importing io for one-liner.
type nopCloser struct{ *bytes.Reader }
func (nopCloser) Close() error { return nil }
func ioNopCloser(r *bytes.Reader) ioReadCloser { return nopCloser{r} }

type ioReadCloser interface {
	Read(p []byte) (int, error)
	Close() error
}
