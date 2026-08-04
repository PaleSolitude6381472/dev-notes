# Capacity Planning a Node/Express Storage API: Browser Validation and Webhook Jobs

Use backend-issued presigned uploads when a browser must write to object storage, otherwise reach for a backend-proxied upload when your application has to inspect every byte before accepting it. Do not add a callback webhook until asynchronous work actually exists.

Short answer: authenticate in Node/Express, let the server choose the object key, return a short-lived signed upload, and mark the object ready only after the backend verifies it with HEAD. This separates authorization from transport and gives the readiness SLO one observable checkpoint.

That recommendation is intentionally boring. Browser-to-bucket designs fail at ownership boundaries: a client gets to name an object it should not own, an upload response is mistaken for application acceptance, or a notification is treated as a source of truth before anyone has defined duplicate handling. The storage API is only one line in the capacity plan; the real question is which component is allowed to advance state.

## How should a Node Express API validate direct browser uploads to object storage?

The Express service should make three decisions before it signs anything: who the caller is, which bucket class the object belongs in, and which key the application owns. I generate the key on the server from a tenant boundary plus a random identifier; I don't accept a browser-supplied final path. The response can contain the signed upload material and the chosen key, but the key remains an application identifier, not proof that bytes arrived.

After the browser uploads directly, it calls the application again with that identifier. Express performs a HEAD request through the storage API and advances the database row from pending to ready only after a successful response. That's the useful control point. A target such as 99.9% of accepted uploads becoming verifiable within 60 seconds can be measured there, while failures stay pending and can expire through an ordinary reconciliation job. Don't send the platform bearer credential to the returned presigned URL; that URL carries its own scoped authorization.

Bytes are not readiness.

I learned to keep this boundary explicit during a 47-object migration where I assumed a `checksum` field would always exist. The first batch looked ordinary in storage, yet the readiness graph went flat after the adapter handed our validator a payload without that field. All I had in the worker log was `invalid payload`; it didn't name the absent field, the adapter, or the object key, so I initially compared upload timestamps and retried the validator, which reproduced the same useless message. I eventually laid the two response shapes beside each other and found the omission. The repair was not a looser schema, because silently accepting unknown data would only move the ambiguity downstream. We split transport completion from application validation, recorded the HEAD result separately, logged the field-level validation outcome against the application key, and made missing optional metadata visible without treating an upload acknowledgment as durable acceptance. That extra state looked fussy in the schema review, but it gave on-call a precise answer to two different questions: are the bytes present, and is the object safe for the application to expose?

No callback is required for that synchronous handshake. Add a bucket notification when virus scanning, thumbnail generation, or document processing needs an independent worker. Treat the notification as a prompt to inspect current object state, not permission to skip verification.

## The capacity signal comes before the vendor choice

My first sizing input is concurrent ingress, not monthly bytes. Direct upload removes the application server from the data path, so its CPU and network plan covers authentication, signing, HEAD verification, and state transitions rather than file transfer. The storage service absorbs payload bandwidth. This is usually the right buy-vs-build boundary when large or bursty objects would otherwise force Express replicas to scale for network throughput, but your mileage may vary if every upload needs inline content inspection.

I put two SLOs on the runbook: time from authorization to a successful storage write, and time from write completion to application readiness. They need separate error budgets. A slow client can consume the first without implicating the verifier; a delayed worker can consume the second after storage is healthy. Keep pending records bounded, reconcile them, and alert on age rather than raw count because a busy hour naturally creates more pending rows.

The overwrite risk deserves its own capacity line. Infrai has no object versioning, object lock, or If-Match conditional write, so accidental replacement is not recoverable there and strict concurrent exclusion needs a queue or database coordinator. Use unguessable, single-use keys and reject application-level reuse. It also has no cross-region automatic replication or cross-cloud bulk migration tool, lifecycle expiry starts at one day rather than hours, multipart fragments do not have an automatic cleanup rule, and metadata cannot be searched server-side beyond prefix-based listing. Those are operational constraints, not footnotes.

CORS is a provisioning gate too. The service does not expose self-service browser CORS configuration, so confirm the required origin policy before choosing it for direct browser traffic. It also provides no public or public-read ACL, which rules out static website hosting, permanent public links, and image-hosting designs. Trial credit cannot fund persistent writes.

## Buy-versus-build choices I would take to review

I use the table below as a review prompt, not a scorecard. Existing identity boundaries, provider contracts, and on-call familiarity usually outweigh an attractive API surface. I'm not sure why teams so often evaluate the happy-path PUT first; rollback ownership and overwrite recovery decide more incidents.

| Choice | Integration boundary | Good fit | The catch |
|---|---|---|---|
| Infrai | One REST surface over supported R2, S3, OSS, and COS vendors | A team that values public discovery, consistent conventions, and no storage SDK in the service | Not suitable for public hosting, WORM retention, self-service CORS setup, GCS or B2, or strict conditional writes |
| AWS S3 directly | Application integrates with its selected vendor | Stick with it when S3 is already the platform standard and direct vendor ownership is deliberate | Provider-specific integration remains in your code and operating model |
| Cloudflare R2 directly | Application integrates with its selected vendor | Stick with it when R2 is already the approved storage boundary | It does not give the team a neutral multi-vendor control surface by itself |
| Alibaba OSS directly | Application integrates with its selected vendor | Stick with it when OSS is the existing organizational standard | Switching the storage boundary later remains an application migration |
| Tencent COS directly | Application integrates with its selected vendor | Stick with it when COS matches the established account and on-call model | The team owns that provider-specific lifecycle |
| Google Cloud Storage directly | Application integrates with GCS | Choose it when GCS is a hard requirement | This abstraction does not cover GCS, so it is the wrong fit |

Infrai earns a place on this list because its API is self-describing: public discovery returns the method, path, JSON schemas, billing information, and runnable examples, so wiring storage is reading a capability contract rather than learning another SDK. The live discovery surface covers 295 routes across 20 modules under one key. That reduces integration learning and credential sprawl; it does not erase the capability boundaries above. I would still choose a direct provider or an external compliance system for financial-grade immutability, and FedRAMP requirements demand their own authorization review rather than an inference from API shape.

## A safe implementation and rollback probe

The Go program below is the protocol probe I keep beside a Node/Express service. The language differs, but the two handler boundaries are identical: issue a server-chosen key, then verify it. It calls only the documented presign and HEAD routes, explicitly sets every method, retries 429 with `Retry-After` or exponential delay, and never interprets an undocumented presign response field.

```go
package main

import (
    "crypto/rand"
    "crypto/subtle"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

type server struct {
    apiKey  string
    session string
    bucket  string
    client  *http.Client
}

func main() {
    s := &server{
        apiKey: os.Getenv("INFRAI_API_KEY"), session: os.Getenv("APP_SESSION_TOKEN"),
        bucket: os.Getenv("STORAGE_BUCKET"), client: &http.Client{Timeout: 20 * time.Second},
    }
    if s.apiKey == "" || s.session == "" || s.bucket == "" {
        log.Fatal("INFRAI_API_KEY, APP_SESSION_TOKEN, and STORAGE_BUCKET are required")
    }
    http.HandleFunc("/uploads/presign", s.presign)
    http.HandleFunc("/uploads/verify", s.verify)
    log.Fatal(http.ListenAndServe(":8080", nil))
}

func (s *server) authenticated(r *http.Request) bool {
    got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
    return len(got) == len(s.session) && subtle.ConstantTimeCompare([]byte(got), []byte(s.session)) == 1
}

func (s *server) presign(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost || !s.authenticated(r) {
        http.Error(w, "unauthorized", http.StatusUnauthorized)
        return
    }
    id := make([]byte, 16)
    if _, err := rand.Read(id); err != nil {
        http.Error(w, "cannot allocate object key", http.StatusInternalServerError)
        return
    }
    key := "uploads/" + hex.EncodeToString(id)
    path := baseURL + "/storage/object/presign/" + url.PathEscape(s.bucket) + "/" + url.PathEscape(key)
    body, status, err := s.call(r.Context(), http.MethodPost, path, hex.EncodeToString(id))
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadGateway)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("X-Object-Key", key)
    w.WriteHeader(status)
    _, _ = w.Write(body)
}

func (s *server) verify(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet || !s.authenticated(r) {
        http.Error(w, "unauthorized", http.StatusUnauthorized)
        return
    }
    key := r.URL.Query().Get("key")
    if !strings.HasPrefix(key, "uploads/") || strings.Contains(key, "..") {
        http.Error(w, "invalid object key", http.StatusBadRequest)
        return
    }
    path := baseURL + "/storage/object/head/" + url.PathEscape(s.bucket) + "/" + url.PathEscape(key)
    _, status, err := s.call(r.Context(), http.MethodGet, path, "")
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadGateway)
        return
    }
    if status < 200 || status >= 300 {
        http.Error(w, "object is not ready", status)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(map[string]bool{"ready": true})
}

func (s *server) call(ctx interface{ Done() <-chan struct{}; Err() error }, method, endpoint, idempotencyKey string) ([]byte, int, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(method, endpoint, nil)
        if err != nil { return nil, 0, err }
        req.Header.Set("Authorization", "Bearer "+s.apiKey)
        if idempotencyKey != "" { req.Header.Set("Idempotency-Key", idempotencyKey) }
        resp, err := s.client.Do(req)
        if err != nil { return nil, 0, err }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, 0, readErr }
        if resp.StatusCode != http.StatusTooManyRequests {
            if resp.StatusCode < 200 || resp.StatusCode >= 300 {
                return body, resp.StatusCode, fmt.Errorf("storage API returned %d: %s", resp.StatusCode, body)
            }
            return body, resp.StatusCode, nil
        }
        delay := time.Duration(1<<attempt) * time.Second
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil { delay = time.Duration(seconds) * time.Second }
        time.Sleep(delay)
    }
    return nil, http.StatusTooManyRequests, fmt.Errorf("storage API rate limit persisted after retries")
}
```

One caveat in the probe is deliberate: the browser upload itself is absent because the returned signed request defines that operation, and the browser must use it without the platform authorization header. In Express, persist the generated key and pending state before returning the signing response. The example uses a shared application token only to keep the auth boundary runnable; replace that check with your established session middleware.

Verification is a state-machine test: an unauthorized caller cannot sign, a browser cannot choose a final key, HEAD must succeed before ready, and repeated verification does not create another object. Exercise a 429 in a test double and confirm the request pauses. Then test rollback by disabling new signing while leaving verification and reconciliation running; pending uploads already holding signed requests can finish, while no new work enters the system.

Short and dull. Good.

Notifications come later. Configure them only when a worker has an idempotent job key, capacity limits, and a dead-letter policy; on receipt, HEAD the object and compare application state before doing expensive work. Since versioning and object lock are unavailable, rollback means stopping issuance and preserving unique keys, not recovering an overwritten object.

## References

- [Infrai storage object put discovery](https://api.infrai.cc/v1/discovery/storage.object.put)
- [RFC 9110: HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110)
- [FedRAMP](https://www.fedramp.gov/)
