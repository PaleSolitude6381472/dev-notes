# Expired Presigned Download URL: AI-Generated Images in a SaaS Retention Runbook

Short answer: when an expired presigned download URL leaves AI-generated images not loading in a SaaS app, keep the object key and retention decision durable in your database, treat every signed link as disposable, and make deletion a versioned workflow that can be audited independently of image delivery.

That is the design I would use for a customer-support SaaS storing AI-generated training images. A support lead may need an image for a scheduled review, while a retention rule may require the same artifact to disappear after a fixed period. Those are separate contracts. A URL expiring means a temporary authorization has ended; it does not, by itself, say anything about the object or the policy.

This distinction matters because the tempting fix is usually the wrong one: make the bucket public, lengthen every link, or let the browser keep a storage URL as if it were the record. All three blur authorization, retention, and diagnosis. Keep the browser on a short leash.

## What should a support SaaS retain when a training image link expires?

Store an immutable artifact record, not a link. The record should contain a tenant identifier, an object-storage location, the artifact type, the generation timestamp, the retention class, the policy version, and the deletion state. The presigned URL belongs in neither the durable record nor an audit event; it is derived state with a bounded lifetime.

Do not refresh blindly.

Suppose a support trainer opens a case-review set after the first links have expired. The app should resolve each image through its tenant-scoped record, check whether the artifact is still available under the recorded policy, and mint a fresh download authorization for the unchanged key. If the record says deletion-pending, the UI must not turn a stale preview into a reason to extend retention; if the record says deleted, a second URL cannot restore the object. That sequence makes a support ticket diagnosable: an authorization lifetime, a policy decision, and an object lookup are three different observations, even when the browser reports each one as “image failed.”

For example, an artifact record for a support training set might identify `tenant-42`, `training/2026-08/case-817/image-03.png`, and a policy such as `support-review-30d`. The policy engine decides when the object becomes eligible for deletion. A read endpoint checks tenant authorization, resolves the record, and creates a fresh download authorization. It does not accept an arbitrary bucket and key supplied by the browser.

There are three useful states: available, deletion-pending, and deleted. A failed image load should not transition an available object to deleted, and a deletion job should not infer that an object is gone merely because its old URL returned an authorization failure. Record the state change with the policy version and a request identifier.

Short links. Durable records.

The SLO should reflect the split. Measure download authorization latency and success separately from object availability, policy evaluation latency, deletion completion, and audit-event delivery. Otherwise a link-renewal incident can look like data loss, while a deletion backlog can hide behind healthy image previews. I want those signals on different dashboards before I put an on-call rotation behind the system.

Capacity planning belongs here too. A training review that opens hundreds of artifacts at once can create a signing burst even if the object store is quiet. Bound concurrent refreshes per tenant, coalesce requests for the same artifact, and leave enough headroom for policy workers to process deletion eligibility. Your mileage may vary on exact limits; traffic shape, not a universal request count, should set them.

## How should a Node.js SaaS troubleshoot expired signed downloads without breaking retention?

Start with the artifact record and the response status, then follow the identity through the layers. A stale URL is one branch of the investigation, not the conclusion.

1. Confirm that the authenticated user may read the tenant's artifact and that the record points to the expected object key.
2. If the temporary URL has passed its usable lifetime, issue a new one from the application boundary. Do not change bucket visibility or the retention class.
3. If a fresh authorization still cannot load the image, compare the recorded key with the generation job's committed key and inspect content type, response headers, and browser cache behavior.
4. If the artifact is deletion-pending, show the policy state to the support operator instead of silently resurrecting it.
5. If the object is deleted, preserve the deletion audit record and return a domain-level “expired by policy” result rather than trying another URL.

The browser should replace `src` only after an authorized application response. It should not retry forever: an ordinary rendering failure can be a cache issue, a revoked session, a missing object, or a policy decision. A retry loop makes all of those more expensive and can turn a gallery refresh into a signing thundering herd.

Deletion is a workflow.

For production verification, keep the read path and the deletion path independent. A fresh URL proves that the application authorized a read at one point in time; it does not prove that a future policy check will allow another read. The deletion worker needs its own queue age, retry, and reconciliation signals, while the image endpoint needs its own authorization and latency SLO. This separation is tedious until the first retention exception arrives, at which point it is the difference between a support explanation and a guess.

Do not log the full signed query string or a bearer credential. Log a tenant-safe artifact identifier, policy version, operation, status class, and correlation ID. OWASP's secrets guidance is useful here because operational logs are often copied into tickets and retained longer than the request that created them. I am not sure which cache layer owns a failure until those fields are visible; that uncertainty is a reason to improve the evidence, not to weaken access controls.

## How can a Go storage adapter separate download authorization from deletion?

The adapter should expose domain operations rather than leak provider-specific URLs into the rest of the service. The example below is deliberately an interface and a state transition model. It contains no undocumented endpoint, SDK assumption, or pretend response field. The concrete implementation can use the storage system selected by the platform team, while the policy and audit behavior stays testable.

```go
package retention

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrAlreadyDeleted = errors.New("artifact already deleted")

type Artifact struct {
	TenantID    string
	Key         string
	Policy      string
	PolicyVer   int
	DeleteAfter time.Time
	State       string
}

type ObjectStore interface {
	PresignDownload(ctx context.Context, key string, ttl time.Duration) (string, error)
	Delete(ctx context.Context, key string) error
}

type AuditLog interface {
	Write(ctx context.Context, event string, artifact Artifact) error
}

type Service struct {
	Store ObjectStore
	Audit AuditLog
}

func (s Service) DownloadURL(ctx context.Context, a Artifact, now time.Time) (string, error) {
	if a.State != "available" || !now.Before(a.DeleteAfter) {
		return "", fmt.Errorf("artifact is not available under policy")
	}
	return s.Store.PresignDownload(ctx, a.Key, 10*time.Minute)
}

func (s Service) DeleteArtifact(ctx context.Context, a Artifact) error {
	if a.State == "deleted" {
		return ErrAlreadyDeleted
	}
	if err := s.Store.Delete(ctx, a.Key); err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	if err := s.Audit.Write(ctx, "artifact.deleted", a); err != nil {
		return fmt.Errorf("write deletion audit: %w", err)
	}
	return nil
}
```

The ten-minute value is an example application choice, not a storage standard. Choose a lifetime shorter than the exposure window and long enough for the actual support workflow; the record's retention deadline must still be enforced by the service and its deletion worker. A URL lifetime cannot substitute for deletion. Conversely, deleting the object should not require waiting for every previously issued URL to age out.

Test the boundary conditions directly: an available artifact before its deadline can receive a URL; the same artifact at or after the deadline cannot; a deleted artifact cannot receive a new authorization; and a repeated deletion request is idempotent at the domain layer. Test that the browser never supplies the key used by `PresignDownload`. Those tests protect the authorization boundary more effectively than a snapshot of one provider response.

## What are the retention and deletion trade-offs in object storage?

The buy-vs-build decision is mostly about control and on-call load. A managed object store supplies durable storage primitives, but the application still owns tenant authorization, policy versioning, deletion orchestration, and the evidence that proves what happened. Self-hosting can offer deeper control over placement and lifecycle behavior, while adding capacity, replication, upgrade, and incident responsibilities. Neither option removes the need for an explicit state machine.

| Decision | Good fit | Cost or risk to carry |
|---|---|---|
| Provider lifecycle rules | Fixed, simple expiry classes | Policy changes may need a migration plan and careful verification |
| Application deletion worker | Tenant-aware rules, legal holds, and audit requirements | Queue depth, retries, and idempotency become your team's SLO |
| Versioned artifact manifest | Reproducible training sets and policy history | Metadata must be protected and reconciled with stored objects |
| Self-hosted storage | Required placement or operational control | Your team owns capacity, replication, upgrades, and recovery |

The catch is that object storage is not a database transaction. A manifest update and an object deletion can be observed at different times, so the worker needs an idempotency key, a retry policy, and a reconciliation pass. Multipart uploads deserve a separate check: AWS documents that incomplete multipart uploads consume storage until completed or aborted, which makes cleanup rules part of the retention design rather than a footnote.

This workflow is not suitable when operators need permanent public links, instant global deletion with no propagation window, or immutable evidence that cannot be removed by the application. Use a dedicated immutability or records-management design when those controls are requirements. Stick with a simpler lifecycle rule when every artifact has one fixed expiry and there is no tenant-specific hold. The right answer is the one whose failure modes the team can actually observe and operate.

## How should verification and rollback protect the retention SLO?

Verify the whole path in a non-production bucket with synthetic training artifacts. Create a record, request a short-lived URL, download the object, wait past the URL lifetime, request a second URL, and confirm that the object key and policy version did not change. Then mark the record deletion-pending, run the worker twice, and check that the second run does not create a second destructive action or a misleading audit event.

For operational review, graph deletion eligibility age, deletion completion age, queue depth, retry count, reconciliation mismatches, fresh-URL success, and download latency. Define an SLO for deletion completion separately from the image-loading SLO. A deletion backlog is a policy risk even when every current preview loads; a preview failure is a user-facing issue even when deletion is on schedule.

Rollback should happen at the policy and application boundaries. Version the retention policy, pause new deletions while preserving their eligibility records, and disable proactive URL refresh independently of the worker. Never roll back by broadening public access. Before resuming, reconcile manifests against objects, inspect the audit stream, and replay only idempotent work. Three words matter: prove, then resume.

## References

- [AWS S3 multipart upload overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
