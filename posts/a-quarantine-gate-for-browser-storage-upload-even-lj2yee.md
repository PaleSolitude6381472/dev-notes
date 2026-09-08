# A Quarantine Gate for Browser Storage Upload Events and Private Bucket Scans

**Short answer:** for a browser-to-storage upload, keep the private bucket unreadable, let a verified object event create durable work, and release the object only after virus scanning and thumbnail processing record an explicit verdict. A browser callback is useful UI feedback; it is not the authority for an upload webhook notification or an access decision.

The least complex design that survives ordinary failure is a private intake prefix, a queue between the storage notification and workers, and a small status record keyed by the immutable object identity. It may feel fussy for a first implementation. It prevents a far more expensive ambiguity later: an object can exist while the browser has vanished, a worker can retry, and a thumbnail can become a new event unless the pipeline makes those states distinct.

## The incident lesson: an upload is not a release

Consider the production review that tends to expose this design flaw. A user selects a file, the browser sends it directly to storage using a narrowly scoped upload grant, and the page receives a success response. The tempting next step is to treat that response as permission to show the file. It isn't. The tab can close after bytes reach storage but before the application records anything; client metadata can describe the wrong type; and the content has not yet passed the checks that determine whether anyone should read it.

Keep it private.

The invariant is simple: a successful transfer creates an **untrusted intake object**, while a clean scan verdict creates a readable application asset. Those are separate state transitions, with different owners and different SLOs. OWASP recommends allowlisting extensions, checking type rather than trusting the `Content-Type` header, generating server-side filenames, setting size limits, and storing files outside the webroot or behind controlled access. Those recommendations remain useful even when the browser never sends file bytes through the application server, because direct upload changes the network path, not the trust boundary.

An upload-complete webhook has a narrower role than many teams give it. It should authenticate the sender, reject malformed envelopes quickly, and enqueue a reference to an object version. It should not download the object, run the antivirus engine, render images, and then hold an HTTP request open while a burst of browser uploads arrives. A queue supplies a measurable backlog, retry policy, and a place to apply capacity planning; an inline webhook supplies none of those unless the team quietly rebuilds them in the handler.

There is a catch. A small internal workflow with a handful of known documents and a contractual requirement for immediate accept-or-reject feedback may be better served by a synchronous scan gate, with an object event retained as reconciliation. Event-driven processing is **not suitable when** the product promise is a final decision before the user leaves the page; stick with a synchronous gate in that case. For long-running media jobs, use a job state machine with progress and cancellation rather than pretending a single notification is the whole workflow.

## How should browser-to-storage upload events protect a private bucket before thumbnail processing?

Start with namespaces, not worker code. Put original files in an intake prefix or bucket that is private by policy. Put generated thumbnails in a distinct output prefix or bucket that does not emit the same input event. A notification rule that watches both original files and its own derivatives turns a normal resize into recursive fan-out. The symptom can look like excess queue depth, duplicate scans, or a worker pool that never catches up; the cause is often merely that output was classified as input.

The event consumer also needs an idempotency key. Use the bucket, object key, and version identifier where the storage system exposes one; a content digest can complement that identity when the application computes it during scanning. Do not use a browser-generated name as the dedupe key. Names get reused, retries happen, and a later upload can legitimately occupy the same logical path.

| Design decision | Failure it contains | Operational cost |
| --- | --- | --- |
| Browser success callback starts processing | Closed tabs and forged client state | Low initial code, silent gaps |
| Storage event calls an inline webhook | Burst concurrency and long processing | Retries exist, but no durable backpressure |
| Storage event enqueues an object reference | Redelivery and worker saturation | Queue, alerting, and replay procedures |
| Worker records scan status before access | Premature reads of unreviewed files | A small state store and authorization check |

DigitalOcean's Spaces documentation describes Spaces as object storage with an S3-compatible API. Compatibility can make direct browser upload and object-event patterns portable, but teams should still validate their chosen service's event payload, delivery semantics, object version behavior, and prefix filtering before setting a production SLO. The differences are operationally important. A design that assumes exactly-once delivery will eventually produce duplicate work somewhere.

Size the pipeline from the peak, not the daily average: peak uploads per minute multiplied by the maximum fan-out per object gives the first queue-arrival estimate. Then measure the slowest bounded stage, usually scanning or image decode, and select worker concurrency from the desired time-to-verdict. A credible review traces the whole recovery path: a notification is accepted; the queue records its age; a worker acquires the versioned object; inspection either stores a terminal status or returns the job for a bounded retry; the authorization layer refuses reads until the terminal status is `clean`; and the on-call engineer can distinguish a growing arrival rate from a worker that is merely slow on large files. The alerts follow those transitions rather than a vendor dashboard's default graph: queue age, oldest unscanned object age, worker failures by reason, rejected-object count, retry count, and the count of objects stuck in `pending` beyond the promised processing window. CPU utilization alone won't show that a poisoned job, a blocked download path, or a downstream status-store delay is violating the user-facing SLO. Replay should be rehearsed too. The procedure needs an immutable event identity, a known retry boundary, and an audit record showing why a file was released or quarantined, because a manual replay without those controls can accidentally process a newer object under an older filename. This is the unglamorous part of direct browser uploads, and it determines whether the system can be operated during a burst instead of merely demonstrated on a quiet day.

I'm not sure a universal queue depth threshold exists; it depends on file size distribution, scanner throughput, and how much recovery time the service objective permits. The threshold should come from a tested drain-rate calculation and be reviewed when upload limits or media types change.

## A narrow handler and an explicit worker contract

The handler below is deliberately generic Go. It validates an authenticated event, rejects keys outside the intake namespace, claims an object version, and writes a job. The interfaces stand in for the storage notification verifier, idempotency store, and queue already operated by the platform team. No storage-provider endpoint is assumed.

```go
package intake

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
)

type Event struct {
	Bucket  string
	Key     string
	Version string
	Size    int64
}

type Verifier interface {
	Verify(*http.Request) (Event, error)
}

type Claims interface {
	Claim(context.Context, string, time.Duration) (bool, error)
}

type Queue interface {
	Enqueue(context.Context, Event) error
}

type Handler struct {
	Verifier Verifier
	Claims   Claims
	Queue    Queue
	Intake   string
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	event, err := h.Verifier.Verify(r)
	if err != nil {
		http.Error(w, "invalid event", http.StatusUnauthorized)
		return
	}
	if !strings.HasPrefix(event.Key, h.Intake) || event.Version == "" {
		http.Error(w, "unexpected object", http.StatusBadRequest)
		return
	}

	claim := event.Bucket + ":" + event.Key + ":" + event.Version
	fresh, err := h.Claims.Claim(r.Context(), claim, 24*time.Hour)
	if err != nil {
		http.Error(w, "temporary dependency failure", http.StatusServiceUnavailable)
		return
	}
	if !fresh {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if err := h.Queue.Enqueue(r.Context(), event); err != nil {
		http.Error(w, "job not accepted", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

var ErrUnsafe = errors.New("object did not pass inspection")
```

The 24-hour claim period is an example policy, not a magic number. It must outlast the queue's total retry window, or a delayed delivery can acquire a second claim and trigger redundant work. For a worker, the safe sequence is: fetch the referenced version under least-privilege credentials; enforce the allowlist, size policy, and content checks; scan it; write only clean derivatives to the output namespace; persist `clean`, `rejected`, or `quarantined`; then let the application mint a time-limited read grant. A failed stage should leave the intake object private and produce a retryable or reviewable state, never an implied success.

This path has a real buy-versus-build decision. A managed scanning system can move signature operations and service ownership out of the on-call rotation, while a self-hosted scanner keeps the runtime, update process, and capacity reserve with the team. Choose using the recovery objective, data-handling constraints, expected object sizes, and the people who will carry the pager. Price is not the deciding variable when an unbounded queue or an unclear quarantine policy is the source of risk.

## What to test before calling the upload pipeline ready

Test the state transitions rather than just the happy-path upload. Send a valid event twice and verify one job is created. Send an event for a generated thumbnail and verify it is rejected by the intake prefix rule. Upload a file whose extension and detected type disagree, a file above the stated size limit, and a file that fails scanning; each must remain private and appear with an actionable status. Finally, pause workers during a controlled load test, observe queue age and recovery, then verify that the drain rate meets the declared SLO after workers resume.

The browser should poll or subscribe to application status, not infer safety from its own transfer result. This keeps the UI honest: “uploaded” can mean bytes arrived, while “available” means policy checks and event-driven processing completed. Those words should remain separate in the API and in support runbooks.

## Further reading

- OWASP File Upload Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- DigitalOcean Spaces documentation: https://docs.digitalocean.com/products/spaces/
