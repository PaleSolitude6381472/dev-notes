# Choosing a Private-Bucket Avatar Flow for Browser Uploads in React and Next.js

Short answer: for a user avatar in a React and Next.js application, use a browser upload with a short-lived presigned URL when the deployed origin passes its CORS test; otherwise, send the file through a small backend proxy. In both cases, keep the bucket private, validate the file on the server, and store an object key rather than a permanent download URL.

That is the easiest setup that does not quietly turn an avatar into a public asset. The choice is mostly about where the bytes and the permission decision live: direct upload keeps file traffic away from the application tier, while a proxy gives the backend one place to enforce policy. Tenant isolation is the deciding constraint for a support product, because a wrong object key can expose one customer's attachment while serving another customer's profile.

## The incident lesson: an object key is part of the authorization boundary

The bounded production scenario I use in review is a support agent changing an avatar while a customer upload is still being processed. The browser retries after a connection drop, the server creates a fresh random key on each attempt, and the profile row is updated by whichever request finishes last. Now two tenants have plausible-looking objects, one object is orphaned, and the audit trail cannot explain which key was supposed to belong to which account. The dangerous version is subtler: the first upload can be valid, the second can be valid, and every individual access check can pass while the final profile association is wrong. A support agent then receives a private download capability for an object that belongs to another account, not because the bucket is public but because application state joined two otherwise legitimate records. That is why I treat key allocation as security work, not naming trivia.

Pick the tenant and user before signing anything. Derive a key from an internal identifier that the server has authenticated, for example `tenants/{tenantID}/users/{userID}/avatar/{revision}`. Never accept a client-supplied tenant prefix as authority. A revision makes replacement explicit; an idempotency token lets a retry reuse the same pending object instead of creating a new one. The database should record the key only after the upload passes validation and the application has decided that this revision is current.

Short keys are fine.

The longer operational story matters. A direct browser PUT removes the unreliable client-to-storage transfer from the API pods, so capacity planning follows request rate and presign work more than upload duration. A proxy makes the browser path simpler, but every slow connection now occupies application concurrency and consumes the API's upload bandwidth. Set separate SLOs for presign, upload acceptance, and profile reads; otherwise a queue of slow bodies can make a healthy profile endpoint look broken. Measure concurrent bodies, p95 upload duration, rejected size, and orphan cleanup volume before choosing the path for a large support launch.

## Should a browser upload an avatar with a presigned URL from React or Next.js?

Use direct upload only after testing the real deployed origin against the bucket's CORS policy. A successful request from `localhost` proves very little about `app.example.com`, and a presigned URL authenticates the storage request without bypassing the browser's cross-origin rules. The server should mint the URL; the browser should never receive the storage credential used to mint it.

The browser flow is small:

1. The application sends an authenticated request containing the intended media type, byte limit, tenant context, and idempotency token.
2. The server validates the session, chooses the object key, and returns a short-lived presigned upload URL plus the key or opaque upload identifier.
3. React uploads the original bytes directly and reports the result to Next.js.
4. The server verifies the completed object, associates the key with the user, and later issues a short-lived download URL for the private avatar.

Do not mark the profile as updated at step 2. A URL being signed is not an object being accepted. Treat the completion request as a state transition, and make it idempotent so the same client retry cannot attach a different tenant's object. The completion path should also check that the key belongs to the authenticated tenant, that the object size is within policy, and that the stored media type is acceptable.

When CORS is unavailable, unstable, or outside the team's control, the proxy is the right answer. That is a capability boundary, not a browser bug. The catch is that the proxy inherits connection pressure, request-body limits, and the cost of streaming bytes through the application. It is a good fit for small avatars and teams that value one auditable enforcement point; it is not suitable when a burst of slow uploads would compete with latency-sensitive support requests. In that case, direct upload or a dedicated upload worker is the better boundary.

## What must the private bucket refuse before it accepts an avatar?

File extension is an input hint, not a security decision. OWASP recommends allowlisting extensions, validating the declared and detected type, limiting size, generating filenames, and storing uploads outside the webroot or behind an access-control layer. For avatars, the allowlist can be narrow: JPEG, PNG, and WebP are common choices, but the product should choose deliberately and test malformed files.

The server also needs to defend the object namespace. Normalize the authenticated tenant identifier, reject path traversal characters in any derived segment, and never let a filename from the multipart form become a storage key. Keep the bucket private by default. A profile API can return a fresh download capability after checking the viewer's tenant and user relationship; it should not persist an expiring URL in the profile table.

Here is the policy-bearing portion of a proxy path. The storage adapter is intentionally generic. Its important property is that it receives a server-derived key and a bounded stream, while the handler owns authentication and tenant checks.

```go
package upload

import (
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
)

type ObjectStore interface {
	Put(key, contentType string, body io.Reader, size int64) error
}

func avatarKey(tenantID, userID, revision string) string {
	return path.Join("tenants", tenantID, "users", userID, "avatar", revision)
}

func PutAvatar(store ObjectStore, tenantID, userID, revision string, r *http.Request) error {
	const maxBytes int64 = 5 * 1024 * 1024
	if r.ContentLength < 0 || r.ContentLength > maxBytes {
		return fmt.Errorf("avatar exceeds the byte limit")
	}

	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	allowed := map[string]bool{
		"image/jpeg": true,
		"image/png":  true,
		"image/webp": true,
	}
	if !allowed[contentType] {
		return fmt.Errorf("avatar media type is not allowed")
	}

	key := avatarKey(tenantID, userID, revision)
	return store.Put(key, contentType, http.MaxBytesReader(nil, r.Body, maxBytes), r.ContentLength)
}
```

This snippet is not a complete content scanner, and it should not be treated as one. A real service must authenticate `tenantID` and `userID` from the session, detect the file type from bytes, close the request body, and commit the database association only after `Put` succeeds. Your mileage may vary on the exact image pipeline: some support products need malware scanning or re-encoding before an avatar becomes displayable, while others can keep that work asynchronous and show the previous avatar until the scan completes.

## How should React and Next.js handle retries, expiry, and cache behavior?

The client should treat an upload as a small state machine: `created`, `uploading`, `uploaded`, `verified`, or `rejected`. A retry from `uploading` must carry the same idempotency token and object key. A retry after a presigned URL expires should ask the server for a new URL for the same pending object, subject to the same tenant authorization. It should not silently start a new profile update.

For a proxy, stream rather than buffer the entire body in memory, cap the body before handing it to storage, and keep upload concurrency separate from ordinary API concurrency. Return a failure that the UI can act on; do not report success because a request was accepted into an in-process queue. For either path, log tenant ID, user ID, object key hash, revision, outcome, and latency, but do not log the presigned URL or the file contents.

Private download URLs need deliberate cache headers. `Cache-Control` controls whether a browser or intermediary may reuse a response, but it does not replace authorization. If an avatar can change during a support session, a short cache lifetime plus a revisioned object key is easier to reason about than trying to purge every old URL. If the product cannot tolerate an old image being shown briefly, use `no-store` for the capability response and accept the extra fetches. The right value depends on the product's replacement semantics; I am not sure one cache policy fits every support workflow.

The cleanup job is part of the design. Delete pending objects that never reached `verified`, remove superseded revisions according to retention policy, and make deletion tenant-scoped and observable. Track the age and count of pending objects. A bucket that contains only successful uploads is not evidence that the process is correct; it may mean the system has no way to see abandoned work.

## A decision rule for the platform team

Choose browser direct upload when CORS is proven in the deployed environment, the storage service can issue scoped short-lived capabilities, and the API tier should not carry file bytes. Choose a backend proxy when the avatar limit is small, the team needs centralized validation, or CORS and client configuration would add more operational risk than the traffic costs.

| Path | Setup cost | Best fit | Main limitation |
| --- | --- | --- | --- |
| Browser direct upload | Higher: CORS, presigning, and completion state | High upload concurrency and a monitored deployed frontend | Browser policy and storage configuration become part of the SLO |
| Backend proxy | Lower: one authenticated application route | Small avatars and centralized validation | Application bandwidth and slow client connections remain in scope |
| Dedicated upload worker | Highest: another queue and capacity boundary | Bursty traffic or expensive scanning | More operational machinery and delayed completion |

The proxy is not automatically safer, and direct upload is not automatically cheaper. Both can isolate tenants if the server derives the key, scopes the capability, verifies completion, and keeps the bucket private. The trade-off is where failure appears: direct upload concentrates browser and CORS behavior at the edge, while a proxy concentrates bandwidth and connection pressure in the application. It doesn't help to choose the path with the shortest demo if nobody owns its failure signals. Stick with the proxy when the team cannot monitor those browser failures; switch to direct upload when upload concurrency becomes a material part of the API SLO.

Before release, test cross-tenant reads, altered object keys, expired capabilities, duplicate completion calls, oversized bodies, invalid media bytes, abandoned uploads, and replacement races. That test matrix is more valuable than a framework-specific upload component. The component changes; the authorization boundary should not.

## References

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control
- https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://developers.cloudflare.com/r2/buckets/cors/
- https://supabase.com/docs/guides/storage/security/access-control
