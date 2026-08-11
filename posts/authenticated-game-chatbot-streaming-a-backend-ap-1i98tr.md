# Authenticated Game Chatbot Streaming: A Backend API Runbook Beyond an SDK

Short answer: for an authenticated web app chatbot, put a small backend API in front of the model service, stream through an application-owned contract, and choose the runtime that gives you per-tenant cost visibility without making the browser responsible for provider behavior. For a gaming company summarizing sales calls into CRM actions, that boundary matters more than an SDK-shaped method name.

The output is operational data, not a toy chat bubble. A transcript can become a follow-up task, an account note, or a field update. The service therefore needs authentication, tenant attribution, cancellation, bounded concurrency, and a way to tell a complete stream from a connection that merely ended politely. Keep the model credential server-side. Always.

Ship less.

## How should an authenticated web app chatbot use a streaming backend API without an SDK?

Start with a stable application request: tenant ID from the authenticated session, conversation ID, transcript text, and an idempotency key. The browser should never be allowed to choose the tenant or send a provider credential. The backend checks authorization, attaches a request ID, records the tenant dimension, and forwards a normalized request to the selected runtime.

The response contract should be yours. It can carry `start`, `delta`, `tool_result`, `complete`, and `error` events over a normal HTTP stream, but it must define which event means the CRM action is safe to persist. A closed TCP connection is not that event. If the browser cancels, propagate cancellation upstream and release the stream slot; otherwise a user who navigated away still consumes capacity.

For this workload, streaming is useful for perceived latency while the summary is being assembled, but the CRM write should happen only after validation. Buffer the structured action, validate account identifiers and allowed fields, then commit it once the terminal event has arrived. A partial sentence must never become a partial sales record.

## What failure modes make simple chatbot APIs expensive?

The first failure is usually attribution. If usage is recorded only against an API key, a platform team can see a bill but cannot explain which game, region, or tenant generated it. Carry the tenant ID through logs, metrics, traces, and usage records, then restrict its cardinality to a controlled identifier. Do not put transcript text in labels.

The second failure is capacity blindness. A stream occupies a connection for its whole lifetime, so requests per second alone is a poor sizing signal. Measure active streams, time to first event, total stream duration, terminal-event rate, cancellation rate, and upstream tokens or bytes where available. Set a concurrency limit per instance and a tenant-level budget. CPU autoscaling cannot infer those limits from a quiet process that is holding open sockets.

The practical trap is a busy launch-day tenant. Imagine a publisher importing a backlog of sales calls while account managers are also using the live chat: the same tenant can create a queue of long transcript requests, keep connections open, and trigger retries from a browser that has already timed out. Without a tenant counter and a stream admission budget, the aggregate dashboard reports acceptable CPU while CRM actions arrive late and the most valuable account absorbs the queue. With those controls, the service can reject or defer new work for that tenant, preserve capacity for other tenants, and show the operator whether the problem is admission, model latency, or CRM commit latency. That distinction is the difference between a capacity decision and a guess.

The third failure is unsafe retrying. Retry an admission failure before anything reaches the browser; do not replay a response after a delta has already been delivered. For a CRM action, idempotency belongs at the commit boundary, not only on the model request. A retry can then re-run inference without creating two follow-up tasks.

Here is the cost record I want beside every request. It is intentionally boring: the point is to make a tenant-level report possible even when the underlying runtime changes.

The record is the control plane.

```go
package costlog

import (
	"context"
	"log/slog"
	"time"
)

type Usage struct {
	TenantID   string
	RequestID  string
	InputUnits int64
	OutputUnits int64
	Duration   time.Duration
	Complete   bool
}

func Record(ctx context.Context, logger *slog.Logger, u Usage) {
	logger.InfoContext(ctx, "model_stream_finished",
		"tenant_id", u.TenantID,
		"request_id", u.RequestID,
		"input_units", u.InputUnits,
		"output_units", u.OutputUnits,
		"duration_ms", u.Duration.Milliseconds(),
		"complete", u.Complete,
	)
}
```

The fields are not a price calculator. They are the evidence needed to join runtime usage to a tenant contract later. I’m not sure a single unit definition will survive every model or provider, so keep the raw counts and the runtime name rather than pretending all tokens have identical meaning. Your mileage may vary when the service exposes only aggregate usage.

## Buy or build: which backend boundary survives an SLO review?

The decision is less about finding a universally best API and more about assigning work to the team that can meet the SLO. A managed runtime reduces serving work but adds a dependency and a translation boundary. Self-hosting buys control over placement and model inventory but creates GPU capacity, upgrade, and on-call obligations.

| Path | What the platform team owns | Good fit | Change course when |
|---|---|---|---|
| Managed model service behind an adapter | Auth relay, quotas, event translation, observability | The team needs to ship the sales-call workflow and has a clear external dependency budget | Tenant isolation or regional placement cannot meet the SLO |
| Self-hosted inference runtime | Hardware, model rollout, scheduling, scaling, incident response | Predictable traffic and a team able to operate the serving fleet | Utilization is spiky or GPU on-call exceeds the product value |
| Split path with a queue for CRM writes | Stream lifecycle plus durable action processing | A reply may be shown immediately while CRM mutation needs stronger delivery guarantees | The product requires a synchronous, low-latency record update |

The useful abstraction is a narrow Go interface: submit a request, receive typed events, and expose usage metadata. Keep provider-specific fields behind the adapter. That gives the application a portable API without claiming that models, safety controls, or streaming semantics are interchangeable.

Do not choose on advertised unit price alone. Per-tenant cost visibility is the primary axis here: define a tenant budget, attach usage to the authenticated subject, and alert on both spend and stream occupancy. A low nominal rate is irrelevant if the team cannot find the tenant that caused a retry storm.

## How do you verify, canary, and roll back the streaming path?

The release probe should use a synthetic tenant and a fixed transcript fixture. Verify rejected application authentication, cross-tenant access denial, an allowed summary, an oversized transcript, client cancellation, an upstream deadline, a retry before first output, and a duplicate commit with the same idempotency key. Verify the terminal event separately from HTTP status.

Then canary by tenant, not by a random percentage alone. A game publisher may have one large account whose sales-call volume dominates the distribution; hiding it inside a percentage can make the average look healthy while that tenant burns its budget. Watch first-event latency, completion rate, active streams, action-commit latency, and per-tenant usage during the canary.

Rollback must preserve the application contract. Keep the model selection in backend configuration, stop admitting new streams to the candidate, drain existing connections, and route new work to the last known-good adapter. If streaming itself is the suspect, return a complete response through the same authenticated endpoint and defer the CRM mutation until validation finishes.

The catch is that this architecture is not suitable when the product needs a provider-native realtime session, specialized moderation workflow, or strict on-premise data residency that the selected runtime cannot satisfy. In those cases, use the dedicated capability or self-hosted path and accept its ownership cost. A portable API reduces migration work; it does not erase capability boundaries.

That is the runbook: authenticate at the application edge, attribute every stream to a tenant, commit CRM actions only after a validated terminal event, and make capacity and rollback observable before launch. The simplest backend is the one whose failure behavior the on-call engineer can explain at 03:00.

## References

- [OpenAI Embeddings guide](https://platform.openai.com/docs/guides/embeddings)
- [Prompt Engineering Guide](https://www.promptingguide.ai)
