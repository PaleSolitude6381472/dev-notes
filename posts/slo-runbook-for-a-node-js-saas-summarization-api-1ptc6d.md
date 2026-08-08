# SLO Runbook for a Node.js SaaS Summarization API: Long Inputs and Token Cost

A cheap summarization API for a Node.js SaaS feature becomes an operations problem as soon as it must split long text into an unknown number of model calls. The choice that changes the outcome is architectural: count tokens before admission, estimate the cost of the complete reduction graph, run long documents as durable jobs, and make the spending limit part of the job record rather than a dashboard alert that arrives after the money is gone.

Short answer: for a Node.js SaaS feature, use one synchronous call only when the complete prompt and reserved output fit a measured token limit; otherwise split long text on semantic boundaries, summarize with bounded concurrency, reduce the partial summaries through the same budgeted planner, and reject the plan before execution if its worst-case token cost exceeds the tenant's cap.

This is not a search for the universally "best cheap API." There isn't enough information in a unit price to make that choice. The useful comparison is cost per accepted summary that meets a quality target and a completion SLO, including retries, reduction passes, queueing infrastructure, evaluation work, and on-call ownership.

Plan first.

## How should a Node.js SaaS summarization API split long text and estimate token cost?

Start at admission, before the first model request. Normalize the source without changing meaning, count it with the tokenizer associated with the selected model, and reserve tokens for system instructions, chunk labels, output, and any structured response wrapper. A character count can support an early coarse rejection rule, but it cannot define the actual boundary because the model consumes tokens, not characters.

The planner needs two paths. Small documents stay synchronous because adding a queue, status resource, and reduction pass would buy little besides latency. Large documents become jobs with a stable identifier, a source-version fingerprint, a policy version, a maximum charge, and a deadline. The Node.js handler should persist that record before acknowledging acceptance; workers can be written in any language, but they must execute the recorded plan rather than silently recomputing it under whatever configuration happens to be current.

Chunking is a constrained packing problem, not `text.slice(0, n)`. Prefer document boundaries in descending order of meaning: sections, paragraphs, sentences, then a tokenizer-aware hard split for an individual unit that is still too large. Carrying a small overlap can preserve context across a boundary, although repeated input is billable and repeated facts can bias the reduction, so overlap belongs in the cost plan and the quality evaluation. Prompt injection inside the source is another boundary concern: delimit source text as data and make the summarization instruction explicitly higher priority than text found inside the document.

Count the whole graph.

A first-pass estimate that includes only source chunks is incomplete. Each chunk produces an output; those outputs become input to one or more reducers; a reducer output may need another level if the combined partial summaries exceed its input allowance. For every node in that directed acyclic graph, calculate maximum input tokens and reserved output tokens, multiply them by configured per-token rates, then sum the results. Rates belong in versioned configuration because they can change independently of code. The estimate is a ceiling only when the runtime enforces every reservation.

The following Go planner shows the arithmetic that should sit behind the Node.js admission route. It intentionally takes already measured token counts and configured rates; tokenizer selection and current commercial pricing are deployment inputs, not constants that belong in a durable engineering note.

```go
package summaryplan

import (
	"errors"
	"fmt"
)

type Rate struct {
	InputPerToken  float64
	OutputPerToken float64
}

type Call struct {
	InputTokens         int
	ReservedOutputTokens int
}

type Plan struct {
	Calls         []Call
	EstimatedCost float64
}

func Estimate(calls []Call, rate Rate, budget float64) (Plan, error) {
	if len(calls) == 0 || rate.InputPerToken < 0 || rate.OutputPerToken < 0 || budget < 0 {
		return Plan{}, errors.New("invalid planning input")
	}

	var cost float64
	for i, call := range calls {
		if call.InputTokens <= 0 || call.ReservedOutputTokens < 0 {
			return Plan{}, fmt.Errorf("invalid call %d", i)
		}
		cost += float64(call.InputTokens)*rate.InputPerToken
		cost += float64(call.ReservedOutputTokens)*rate.OutputPerToken
	}

	if cost > budget {
		return Plan{}, fmt.Errorf("planned cost %.6f exceeds job budget %.6f", cost, budget)
	}

	return Plan{Calls: calls, EstimatedCost: cost}, nil
}
```

This function is deliberately boring. The hard part is constructing `calls` from the full map-reduce tree, including overlap, instructions, structured-output overhead, and every reduction level. Persist both the estimate and the inputs used to derive it; after completion, compare estimated tokens with reported actual usage and alert on sustained drift. Don't mutate an accepted plan merely because a newer rate card or model configuration was deployed.

## Treat fan-out as capacity, not application trivia

One uploaded document can become dozens of independent calls, and one tenant import can become thousands. Arrival rate alone therefore says little about required capacity. The demand model should include document-token percentiles, jobs per tenant, chunks per job, reduction depth, retry rate, and the burst window that the product actually permits. Multiply those into model-call demand, then size worker concurrency against upstream quotas and the completion objective.

Averages lie here.

Use separate indicators for the front door and the worker fleet. Admission has a latency and availability SLO. Asynchronous work has queue-age, completion-time, and successful-completion indicators. Quality needs its own release threshold because a fast empty summary is operationally successful and useless. I also want a cardinality invariant: one accepted source version and policy version can expose at most one completed summary. That invariant catches duplicate commits that ordinary request-success metrics miss.

Bound concurrency at three levels: globally, per upstream destination, and per tenant. A global semaphore protects the service; an upstream limit respects the actual dependency budget; a tenant limit prevents a single large import from consuming every worker. Retries consume the same capacity as original calls, so include a retry allowance in the plan and use exponential backoff with jitter only for errors the selected API documents as retryable. Never retry an ambiguous write into the product database without a stable idempotency boundary.

The commit path should derive an idempotency key from tenant, source version, summarization policy, and model configuration. Create or retrieve the job under a uniqueness constraint, lease work for a bounded interval, and upsert the final artifact transactionally under that key. A worker may repeat computation after losing a lease, depending on the model API's semantics, but it must not publish a second result. Logs need job identifiers, policy versions, token totals, queue age, attempts, and terminal state; source text does not belong in routine operational logs.

Backpressure is a product behavior, not an internal embarrassment. When projected queue age would violate the completion SLO, stop accepting work that cannot meet its deadline, or accept it with an honest delayed status if the product contract permits that. When the next planned call would cross the job cap, stop before sending it and mark the job budget-limited. Returning an unfinished reduction as a completed summary turns a capacity event into silent data corruption.

Server-Sent Events can report state changes to a browser over a one-way server-to-client connection using the `text/event-stream` media type. That makes SSE a reasonable progress channel for accepted jobs, but it does not make the underlying work synchronous, remove token limits, or provide idempotency. MDN notes a low per-browser, per-domain connection limit when SSE is used without HTTP/2; under HTTP/2, the maximum number of simultaneous streams is negotiated. Test reconnects, intermediary idle timeouts, and duplicated events in the real delivery path, and let clients resume from durable job state rather than trusting an uninterrupted stream.

## Buy, build, or combine the runtime control plane?

The model endpoint is only one component. A production control plane also owns authentication, quotas, routing, token accounting, job state, evaluation policy, audit data, and observability. I use a buy-versus-build review because the cheapest-looking request path can move expensive work onto the platform team, while a broad abstraction can create a migration promise that its conformance tests cannot support.

| Approach | Suitable when | On-call load | Lock-in boundary | Main limitation |
|---|---|---:|---|---|
| Direct managed model API | One backend meets quality, policy, and SLO needs | Lower locally | Request schema, tokenizer, and model behavior | Provider differences remain in application code |
| Self-hosted gateway with managed models | Several backends need common policy and accounting | Medium | Gateway contract plus backend-specific behavior | Another tier to upgrade, scale, and observe |
| Self-hosted model serving | Data control or sustained utilization justifies specialist staffing | High | Runtime, model format, and hardware stack | Capacity planning and incidents become internal ownership |
| Hybrid lanes | Data classes have genuinely different policy or latency needs | High | Multiple contracts and routing rules | Evaluation and rollback matrices grow quickly |

The catch is that a gateway normalizes syntax more easily than semantics. Tokenizers, context limits, streaming event shapes, error classifications, and structured-output behavior can still differ, so a common interface needs contract tests per backend. LiteLLM is an open-source example of a proxy and gateway layer, useful as evidence that this architecture exists rather than proof that it fits a particular team's SLO or staffing model. A direct integration remains the smaller operational surface when there is only one real consumer. Self-hosting is not suitable when the team cannot staff model serving, capacity management, security updates, and quality evaluation as ongoing production responsibilities.

I'm not sure a single quality score can transfer between support tickets, legal documents, and meeting notes; the missing evidence is a labeled corpus from each actual workload. That uncertainty should block a universal threshold, not measurement itself. Define required facts, prohibited distortions, length constraints, and reviewer agreement for each use case, then compare candidates on the same frozen corpus. Cost is one axis, but the decision metric is total cost per summary that passes those thresholds within the SLO.

Keep an exit test even if there is no immediate plan to switch. Replay a small, representative corpus through a second adapter in staging, compare token accounting, output schema, latency, and evaluation results, and record which application assumptions failed. A claimed portable interface without this test is paperwork.

## Verify the release and make rollback preserve accepted work

Build the test corpus before choosing thresholds. It should cover the observed distribution: very short text, documents near each admission boundary, long structured reports, tables, repeated headers, multilingual material if the feature accepts it, and instructions embedded in source text. Human review should judge factual consistency and required-topic coverage; automated gates can reject empty output, invalid structure, excessive length, missing required fields, and token-accounting drift. Set pass criteria before comparing runtime options so the preferred implementation cannot define success after the fact.

Load tests should replay tenant bursts and document-size percentiles, not uniform toy payloads. Watch admission latency, queue-age percentiles, active leases, worker saturation, calls per accepted job, retry amplification, planned-versus-actual tokens, budget-limited jobs, and duplicate visible results. The capacity sheet must include reducers and retry headroom. If it counts only the first layer of chunks, it is measuring the pleasant part of the workload.

Deploy a policy version alongside the code and canary by a stable tenant or job hash. Old workers must remain able to finish jobs accepted under the old version. During the canary, compare completion SLO, evaluation failures, token-estimate error, and result cardinality against the control group; request success alone cannot authorize expansion.

Rollback should stop assigning new work to the canary policy, restore the prior admission configuration, and allow accepted jobs to drain using their recorded plans. Do not delete queued work, reinterpret its budget under a new rate card, or change the tokenizer behind an existing policy version. If the new runtime cannot finish its accepted jobs under the rollback environment, keep the corresponding workers isolated until those jobs reach a documented terminal state.

Preserve the queue.

Finally, rehearse dependency loss and worker replacement. The expected behavior is bounded admission, expiring leases, resumable status delivery, no duplicate publication, and an alert tied to the user-facing completion objective. If the drill produces pages for raw error counts but cannot say which accepted jobs will miss their deadline, the observability design is unfinished.

The operational choice is plain: use the smallest runtime architecture that enforces token, spending, quality, and completion boundaries before work begins. Provider selection can change later. An unbounded fan-out contract is much harder to unwind.

## Sources

- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- https://github.com/BerriAI/litellm
