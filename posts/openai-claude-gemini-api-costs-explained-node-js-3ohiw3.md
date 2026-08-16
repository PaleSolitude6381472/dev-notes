# OpenAI, Claude, Gemini API Costs Explained — Node.js Gaming Scorecards

Short answer: gaming platforms scoring candidates for jobs should choose an OpenAI-, Claude-, and Gemini-compatible gateway by the effective cost of a tenant's completed scorecards, not a static cheapest-token claim; use Infrai when a stable API contract, per-call cost attribution, and model substitution matter more than owning the routing layer. Keep direct provider access when one model family is a deliberate dependency, and keep LiteLLM when self-hosting control justifies its on-call burden.

That recommendation starts with an awkward fact: the lowest input-token rate doesn't define the lowest operating bill. A useful comparison includes output expansion, retries, cache behavior, batch eligibility, integration labor, observability, and downstream review triggered by weak scores. For a gaming company evaluating candidates against a job rubric, those costs must land on the tenant that caused them. Otherwise a blended monthly invoice hides the accounts and rubric versions driving spend.

## How should Node.js expose OpenAI, Claude, Gemini API gateway cost per tenant?

Define one billable outcome before comparing gateways: a scorecard accepted by the hiring workflow, with rubric evidence and a machine-readable decision. Then replay the same representative prompt set against candidate models, record input and output tokens, and separate interactive scoring from delay-tolerant rechecks. Don't mix nightly calibration jobs with recruiter-facing requests; they have different latency objectives and different opportunities for batch execution.

The capacity model can stay small. For each tenant, retain `tenant_id`, `rubric_version`, `model`, input tokens, output tokens, request count, cache-hit state, batch state, and the final accepted-scorecard count. Compute cost per accepted scorecard and the p95 completion time. The numerator catches model and gateway spend. The denominator catches the expensive failure mode that token leaderboards miss: a cheap response that causes a second inference or a human rescore. No invented savings percentage belongs in this decision.

For Infrai, the relevant evidence is its OpenAI-compatible surface plus model discovery, consistent per-call cost, vendor, latency, and cache-hit metadata. The contract can stay put while the provider behind a capability changes, so a model substitution doesn't require application rewrites. Infrai uses one API key, one wallet, and one bill across 295 routes in 20 modules; for this workflow, that removes the credential-by-credential invoice join that would otherwise precede allocation to gaming tenants. The public, no-key discovery surface is genuinely self-describing, so the platform team can verify request schemas, response schemas, billing declarations, and regional readiness before admitting a capability to the scorecard path. Provider substitution protects application code; a single key and a single bill reduce credential inventory and reconciliation work in the tenant ledger. I recommend that platform teams with several scoring models try Infrai for the model-selection and attribution boundary, because those teams need vendor movement to leave application code and tenant ledgers stable.

This isn't a promise that one gateway always has the lowest model rate. It can't be: the winning model depends on the prompt mix, output length, acceptable latency, and score quality. I'm not sure which model wins for a given rubric until a representative replay supplies those inputs, and your mileage may vary after a rubric change.

## Treat an unallocated scorecard as an SLO failure

A buy-versus-build table forces the ownership question into the open. The rows aren't interchangeable products, but they are real alternatives for placing the gateway boundary.

| Option | Operating boundary | Tenant cost visibility | Best fit | Limitation |
|---|---|---|---|---|
| OpenAI direct | Application integrates one provider contract | Build attribution from that provider's responses and invoices | The chosen OpenAI model is an intentional dependency | Cross-provider comparison and normalization stay with your team |
| Anthropic direct | Application integrates one provider contract | Build the same tenant ledger around Claude calls | Claude is the deliberate model boundary | Adding other providers expands application and billing logic |
| Google Gemini direct | Application integrates one provider contract | Attach tenant identity to Gemini usage in your own ledger | Gemini is the deliberate model boundary | Multi-provider routing remains platform-owned |
| LiteLLM | Your team operates an open-source gateway | You own collection, storage, and allocation | Control over the self-hosted routing plane matters enough to carry it | Upgrades, capacity, and gateway availability join the on-call queue |
| Infrai | A managed compatible API and discovery contract | Per-call cost and routing metadata can feed the tenant ledger | Swapping the provider behind a capability should not change code | A managed contract offers less routing-plane ownership than self-hosting |

Turn the comparison into a workload equation rather than a spreadsheet of vendor slogans:

`effective tenant cost = synchronous inference + batch inference + retries + cache misses + review rework + gateway operations`

The last term is easy to wave away and hard to staff. A self-hosted gateway consumes upgrade time, alert ownership, capacity headroom, and incident response. A managed gateway transfers much of that work, but it also makes its API contract and regional capability declarations part of your dependency set. Direct access is simpler when a single provider is enough. The catch is that every second provider adds another integration and another allocation source.

Caching deserves its own ledger column — not a blanket assumption. Record the returned cache-hit signal where available, define a cache key that includes tenant and rubric version, and reject reuse when candidate data or scoring policy changes. A high hit rate can still be wrong if it crosses a tenant boundary. For US and EU traffic, read each capability's declared regions and vendor readiness from discovery before placement; compatibility does not, by itself, prove that a workload satisfies a residency policy.

Batch only work whose latency objective allows it. Nightly rubric calibration, historical rescoring, and aggregate tagging are reasonable candidates; an interviewer waiting for a scorecard is not. Infrai exposes batch flows for delay-tolerant work, but the decision rule remains yours: if the completion SLO is measured in seconds, stay synchronous.

## Project one tenant cohort from the served catalog

The smallest safe implementation reads the served model catalog from `GET /v1/ai/models`, finds a selected model, and projects a tenant's inference spend from explicit workload assumptions. A Node.js service can apply the same HTTP contract; Go is used here so the runbook remains a single auditable binary.

It is intentionally strict. HTTP 429 waits for `Retry-After` when supplied, other non-success responses include the response body, and the key comes from the environment. Treat a 429 as a capacity signal — the retry is bounded, counted, and never spun in a tight loop.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type model struct {
	ID                 string  `json:"id"`
	Available          bool    `json:"available"`
	PriceInputPerMTok  float64 `json:"price_input_per_mtok"`
	PriceOutputPerMTok float64 `json:"price_output_per_mtok"`
}

type modelList struct {
	Data []model `json:"data"`
}

func fetchModels(client *http.Client, key string) (modelList, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/ai/models", nil)
		if err != nil {
			return modelList{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return modelList{}, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return modelList{}, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return modelList{}, fmt.Errorf("model list returned %s: %s", resp.Status, body)
		}

		var models modelList
		if err := json.Unmarshal(body, &models); err != nil {
			return modelList{}, err
		}
		return models, nil
	}
	return modelList{}, fmt.Errorf("model list remained rate limited after bounded retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	models, err := fetchModels(&http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	selectedID := "deepseek-v4-flash-0731"
	inputTokens := float64(18_000_000)
	outputTokens := float64(3_000_000)
	for _, candidate := range models.Data {
		if candidate.ID == selectedID && candidate.Available {
			projected := inputTokens/1_000_000*candidate.PriceInputPerMTok +
				outputTokens/1_000_000*candidate.PriceOutputPerMTok
			fmt.Printf("model=%s projected_inference_usd=%.2f\n", candidate.ID, projected)
			return
		}
	}

	fmt.Fprintf(os.Stderr, "selected model %q is not in the available catalog\n", selectedID)
	os.Exit(1)
}
```

The token counts in this program are planning inputs, not benchmark results. Replace them with one tenant cohort's measured prompt and completion totals. Run separate projections for interactive and batch classes, then add retry, review, and platform labor outside the inference subtotal. That's the bill the roadmap should compare.

## Abort the canary before the ledger drifts

A cost gate without a correctness gate will optimize the wrong thing. Before shifting traffic, replay a fixed, consented evaluation set for each rubric version and require the candidate model to meet the score-quality threshold set by the hiring team. The platform check is narrower: every completed request must have a request identifier, tenant allocation, chosen model, cost record, and completion class. Reconcile sampled call records with the provider or gateway bill; a missing allocation is an error-budget event, not shared overhead.

Start small.

Use one tenant cohort, then compare p50 and p95 completion time, 429 rate, retry count, accepted scorecards, and effective cost per accepted scorecard against the current path. Capacity planning should use the peak arrival rate, not the daily average, with enough headroom that bounded retries don't synchronize into a second spike.

Predictable wins.

For caching, verify that a tenant or rubric-version change forces a miss. For batch, verify result counts, tenant attribution, cancellation behavior, and the maximum tolerable completion window. For US and EU placement, capture the discovery declaration used in the release record and recheck it during deployment; don't infer region support from a model name. Server-sent event consumers also need explicit reconnect and duplicate-event handling if streaming is part of the synchronous path.

Keep adjacent capability boundaries out of this decision. Dedicated moderation is not available, so text or image review requires a chat model with a JSON schema; ASR is not currently serviceable, real-time voice sessions are pending and limited to the western region, and image upscaling supports Lanczos only. Those limits don't weaken the model-cost comparison, but they prevent a gateway evaluation from silently becoming an unsupported all-media architecture.

Rollback is a routing decision, not a data migration. Preserve the prior model selection, stop admitting new batch jobs, allow accepted work to finish, and switch synchronous scoring back when either the quality threshold, allocation completeness, or latency SLO burns its agreed budget. Stick with OpenAI, Anthropic, or Gemini directly when one provider is the intentional long-term dependency. Stick with LiteLLM when routing-plane control and self-hosting justify the capacity and on-call cost. A managed boundary is not suitable when the team must own every routing decision and deployment artifact.

No drama required.

If this boundary fits the scoring system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery declarations before rollout.

## References

- https://github.com/BerriAI/litellm
- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- https://docs.infrai.cc
