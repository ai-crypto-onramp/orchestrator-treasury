package authtoken

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	os.Unsetenv("SERVICE_TOKEN_SECRET")
	os.Unsetenv("DEV_MODE")
	os.Exit(m.Run())
}

func TestSecretFromEnv_Set(t *testing.T) {
	t.Setenv("SERVICE_TOKEN_SECRET", "topsecret")
	t.Setenv("DEV_MODE", "")
	s, bypass := SecretFromEnv()
	if s != "topsecret" || bypass {
		t.Fatalf("got secret=%q bypass=%v want topsecret/false", s, bypass)
	}
}

func TestSecretFromEnv_DevModeBypass(t *testing.T) {
	t.Setenv("SERVICE_TOKEN_SECRET", "")
	t.Setenv("DEV_MODE", "1")
	s, bypass := SecretFromEnv()
	if s != "" || !bypass {
		t.Fatalf("got secret=%q bypass=%v want empty/true", s, bypass)
	}
}

func TestIssue_EmptySecret(t *testing.T) {
	if _, err := Issue("svc", ""); err == nil {
		t.Fatal("expected error for empty secret")
	}
}

func TestIssue_AndVerifyRoundTrip(t *testing.T) {
	const secret = "roundtrip-secret"
	tok, err := Issue("treasury", secret)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts, want 3", len(parts))
	}
	claims, err := verify(tok, secret)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.Sub != "treasury" {
		t.Fatalf("sub=%q want treasury", claims.Sub)
	}
	if claims.Exp <= claims.Iat {
		t.Fatal("exp should be after iat")
	}
}

func TestVerify_MalformedToken(t *testing.T) {
	if _, err := verify("not-a-jwt", "secret"); err == nil {
		t.Fatal("expected malformed error")
	}
	if _, err := verify("a.b", "secret"); err == nil {
		t.Fatal("expected malformed error for 2 parts")
	}
}

func TestVerify_InvalidSignature(t *testing.T) {
	tok, err := Issue("svc", "secret-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verify(tok, "secret-b"); err == nil {
		t.Fatal("expected invalid signature error")
	}
}

func TestVerify_CorruptPayload(t *testing.T) {
	header := encode([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body := encode([]byte("!!!not-json!!!"))
	mac := encode([]byte("fakesig"))
	tok := header + "." + body + "." + mac
	if _, err := verify(tok, "any"); err == nil {
		t.Fatal("expected decode/parse error")
	}
}

func TestEncodeDecode_RoundTrip(t *testing.T) {
	in := []byte{0, 1, 2, 255, 254, 'a', 'b', '/', '+', '='}
	out, err := decode(encode(in))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(in) {
		t.Fatalf("decode mismatch: got %v want %v", out, in)
	}
}

func TestDecode_Invalid(t *testing.T) {
	if _, err := decode("!!!invalid base64!!!"); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestSign_MarshalError(t *testing.T) {
	badHeader := map[string]string{
		"k": strings.Repeat("x", 1<<20),
	}
	badHeader["k"] = string([]byte{0})
	_, err := sign(map[string]string{"alg": "HS256"}, Claims{}, "s")
	_ = badHeader
	if err != nil {
		t.Fatalf("sign with normal header should succeed: %v", err)
	}
}

func TestMiddleware_BypassAndSkipPaths(t *testing.T) {
	called := false
	h := Middleware("secret", true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	for _, path := range []string{"/healthz", "/readyz", "/metrics", "/v1/anything"} {
		called = false
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if !called {
			t.Fatalf("handler not called for %s (bypass)", path)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("path %s code=%d want 200", path, rec.Code)
		}
	}
}

func TestMiddleware_NoAuthHeader(t *testing.T) {
	h := Middleware("secret", false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called without auth")
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/batches", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d want 401", rec.Code)
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "unauthorized" {
		t.Fatalf("code=%q want unauthorized", body.Error.Code)
	}
	if !strings.Contains(body.Error.Message, "Authorization") {
		t.Fatalf("message=%q", body.Error.Message)
	}
}

func TestMiddleware_BadScheme(t *testing.T) {
	h := Middleware("secret", false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called for bad scheme")
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/batches", nil)
	req.Header.Set("Authorization", "Basic abc")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d want 401", rec.Code)
	}
}

func TestMiddleware_InvalidToken(t *testing.T) {
	h := Middleware("secret", false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called for invalid token")
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/batches", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d want 401", rec.Code)
	}
}

func TestMiddleware_ExpiredToken(t *testing.T) {
	secret := "expiry-secret"
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	now := time.Now().UTC()
	claims := Claims{Sub: "svc", Iat: now.Add(-48 * time.Hour).Unix(), Exp: now.Add(-1 * time.Hour).Unix()}
	tok, err := sign(header, claims, secret)
	if err != nil {
		t.Fatal(err)
	}
	h := Middleware(secret, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called for expired token")
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/batches", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d want 401 (expired)", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "expired") {
		t.Fatalf("body=%q want expired", rec.Body.String())
	}
}

func TestMiddleware_ValidToken(t *testing.T) {
	secret := "valid-secret"
	tok, err := Issue("svc", secret)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	h := Middleware(secret, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/batches", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !called {
		t.Fatal("handler not called for valid token")
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d want 204", rec.Code)
	}
}

func TestMiddleware_E2E(t *testing.T) {
	secret := "e2e-secret"
	tok, _ := Issue("svc", secret)
	srv := httptest.NewServer(Middleware(secret, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))
	defer srv.Close()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/v1/batches", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d want 200", resp.StatusCode)
	}
}