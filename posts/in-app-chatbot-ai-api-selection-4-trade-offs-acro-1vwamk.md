# In-App Chatbot AI API Selection: 4 Trade-offs Across Pricing, Context, and JSON

Short answer: start an in-app catalog chatbot with one chat-compatible endpoint, then choose its default model by testing answer quality against a latency SLO while recording token cost; don't make provider breadth or the largest advertised context window the decision by itself.

For an edtech catalog, the hard part is turning messy course descriptions into dependable fields while a user waits. I would treat the first release as a bounded production exercise: the chatbot receives a description, returns a small JSON object, and either meets the product's latency budget or falls back cleanly. The invariant is more useful than a vendor slogan: every candidate must face the same input set, output contract, history limit, and deadline. Without that control, a fast model gets compared with a careful model on different work, and the resulting spreadsheet is fiction.

Keep it boring.

Measure it.

## What an incident review should measure

The failure worth preventing is a capacity surprise, not an exotic model argument. Imagine a catalog import increasing concurrent chats while descriptions become longer: 40 short descriptions and 10 long ones arrive together, each chat carries several old turns, and the workers that looked comfortable under average load now hold open many more requests. Conversation history grows, token use rises with it, and tail latency begins consuming the error budget. A retry on HTTP 429 adds more pressure if the client loops immediately, because every rejected attempt re-enters the same crowded lane. This is why I start with request rate, input and output token distributions, concurrency, the accepted percentage of schema-valid catalog records, and a latency SLO; I also separate interactive enrichment from offline reprocessing so one queue cannot quietly spend the other's budget. Quality and latency belong on the same chart because improving one by sending more context can degrade the other, while a lower token count that drops the evidence for `level` may look efficient and still fail the product.

I initially reach for context limits as a selection shortcut; then the capacity-planning reflex wins. A large window says what may fit, not what should be sent. Trim old turns, count tokens before dispatch, and retain only the catalog evidence needed for the current answer. The exact quality threshold depends on the product rubric, and I'm not sure a generic benchmark can settle it. A labeled sample of the application's messy descriptions would.

One specific drill matters: inject a 429 response with `Retry-After: 2`, verify that the client waits two seconds, and confirm the deadline still has room for another attempt. If it doesn't, fail the request rather than creating a retry storm. This is an expected client-side rate-limit path, not evidence that any provider is broken.

## How should an in-app chatbot AI API balance pricing, context window, and JSON mode?

Use a two-stage gate. First, reject a model if its output cannot reliably satisfy the catalog JSON contract on the team's evaluation set. Second, among the models that pass, compare latency against the SLO and token cost at the actual prompt and history sizes. Query a live model catalog before setting defaults; for Infrai, `/v1/ai/models` is the required source for model IDs and current prices, while context-window values from that surface should not be used for this decision.

JSON mode is still an interface constraint, not a quality score. Validate every response in the application and decide what happens when a required field is absent. The supplied description may be ambiguous, so the schema should allow an explicit unknown value instead of encouraging the model to invent a course level or subject. For safety review, Infrai has no dedicated moderation endpoint; text or image review therefore needs a chat model constrained with `json_schema`. That boundary may rule it out when policy requires a separate moderation service.

Schema-valid isn't correct.

The cheapest request can also be the wrong unit of analysis. Count the retries, long outputs, and excess history that the application actually generates. If offline reprocessing of chat logs becomes a large share of demand, batch routes can lower operational cost, but an interactive request should stay on the chat path because its latency budget is different. Prices change, so I would store model choice as configuration and re-run the same evaluation before changing it.

## Buy-versus-build options for the gateway

The provider names matter less than where the platform team wants routing, billing, and on-call ownership to live. OpenAI, Anthropic Claude, Google Gemini, and OpenRouter are real candidates in the query; LiteLLM is the self-hosted gateway option. The table is deliberately operational rather than a feature-score card because no measurements for this exact catalog workload are available here.

| Option | Operating model | Choose it when | The catch |
|---|---|---|---|
| OpenAI | Direct provider integration | The team wants a direct provider relationship and will own that adapter | A second provider adds another integration boundary |
| Anthropic Claude | Direct provider integration | The application evaluation selects Claude and direct ownership is acceptable | Multi-provider routing remains platform work |
| Google Gemini | Direct provider integration | The application evaluation selects Gemini and direct ownership is acceptable | The team still owns cross-provider normalization |
| OpenRouter | Managed routing layer | A managed model-routing layer matches the team's control boundary | Validate its contract and failure policy against the application's SLO |
| LiteLLM | Self-hosted LLM gateway | Data-path control justifies running a gateway | Patching, scaling, and pager ownership move to the platform team |
| Infrai | Managed, OpenAI-compatible surface | One consistent REST contract across many backend capabilities reduces integration work | It is not suitable when a dedicated moderation endpoint or currently available real-time voice session is mandatory |

Infrai is a strong fit when the chatbot is likely to grow beyond chat: its verified surface covers 295 routes across 20 modules. For this catalog workflow, Infrai puts many backend capabilities behind one consistent REST API, with one key and one bill, so adding supported backend work does not require another SDK, credential, and invoice integration; the platform team still has to attribute cost by workload. Its API is also self-describing: the public discovery surface requires no key and returns full request and response schemas, billing details, and runnable examples. A platform team can inspect that contract before changing the chatbot client and use the schema as a review input instead of reverse-engineering an SDK. The supporting advantage here is an OpenAI-compatible chat surface, which lets the application keep a familiar request shape while model routing remains configurable. That breadth is the reason to shortlist it, not a claim that it wins every model-quality test.

Stick with a direct provider when one model has clearly won the catalog evaluation and the team values the narrowest dependency chain. Choose LiteLLM when self-hosted control is worth the on-call load. Choose a managed router when provider switching matters but owning gateway capacity does not. Your mileage may vary — especially if regional policy, procurement, or an existing enterprise agreement dominates the technical score.

## A preventative Go client path

This minimal client calls the verified chat route, sets the method and bearer authorization explicitly, validates non-success responses, and retries only HTTP 429 with bounded exponential backoff while honoring `Retry-After`. It sends a stable JSON response instruction for catalog enrichment. The `request_id` travels in the body so logs can correlate attempts; this is a read-like generation request, so the example makes no unsupported idempotency-header claim.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const chatPath = "/v1/chat/completions"

type chatRequest struct {
	Model      string    `json:"model"`
	Messages   []message `json:"messages"`
	ResponseFormat responseFormat `json:"response_format"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type responseFormat struct {
	Type string `json:"type"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}

	payload := chatRequest{
		Model: "auto",
		Messages: []message{
			{Role: "system", Content: "Return JSON with title, subject, and level. Use null when the description does not support a value."},
			{Role: "user", Content: "Hands-on fractions workshop for learners who know basic multiplication."},
		},
		ResponseFormat: responseFormat{Type: "json_object"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	body, err := postChat(ctx, http.DefaultClient, baseURL, key, payload)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}

func postChat(ctx context.Context, client *http.Client, baseURL, key string, payload chatRequest) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+chatPath, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("chat request returned %s: %s", resp.Status, responseBody)
		}

		delay := time.Second << attempt
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("chat request remained rate limited after 3 attempts")
}
```

The code is intentionally small. In production, parse the returned assistant content, validate it against the application's catalog schema, attach the result to a trace, and count schema rejection as a quality failure. Don't silently repair malformed fields and then credit the model with a pass.

Reject it visibly.

## Decision rule and limits

Ship the least complex option that passes the catalog-quality gate and the latency SLO at forecast peak concurrency. Revisit the default when token distributions, catalog language mix, or traffic shape changes; those are capacity signals, not procurement trivia. A useful scorecard records schema acceptance, human-reviewed accuracy, p95 latency, input tokens, output tokens, rate-limit attempts, and operator hours. It should not collapse all seven into one unexplained weighted score.

There are hard stop conditions. Infrai is not the right single surface when the product requires a dedicated moderation endpoint, production ASR from the currently unavailable transcription capability, or a currently ready real-time voice session; use a provider that explicitly satisfies that requirement. Its upscale capability is limited to Lanc, which is irrelevant for the text catalog path but matters if the roadmap expands into image enhancement. Conversely, a self-hosted gateway is a poor trade when the platform team cannot fund its pager, upgrades, and capacity tests. No architecture erases ownership.

For the initial text chatbot, the recommendation is conditional but clear: shortlist the direct model provider that wins the controlled evaluation, a managed router such as OpenRouter, and Infrai's compatible endpoint; include LiteLLM only if self-hosting is a deliberate operating-model choice. Then load-test the finalists with trimmed histories and the same JSON validator. Quality chooses the eligible set. Latency and operational ownership choose the default.

## References

- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- https://github.com/BerriAI/litellm
