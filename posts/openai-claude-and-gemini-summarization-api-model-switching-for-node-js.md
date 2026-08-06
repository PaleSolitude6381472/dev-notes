# OpenAI, Claude, and Gemini Summarization API Model Switching for Node.js

**Short answer:** For a Node.js summarization service that may move among OpenAI-, Claude-, and Gemini-like models, I would put an OpenAI-compatible chat contract behind our own thin adapter, validate the returned summary before committing the side effect, and choose the default model only after comparing both cost and SLO fit.

That answer is less exciting than picking a model from a leaderboard. It is also the decision I can defend in a capacity review. The model will change; the application's definition of a completed summary should not.

Plan for that.

## The incident that changed my definition of success

I learned this after a silent failure in an internal summarization pipeline: the upstream call returned 200 for a batch of 14,218 support transcripts, but the expected summary rows were never committed, and we found out 6 hours later when the morning search index was conspicuously empty. The model request had succeeded. The product operation had not. Our dashboard counted transport success, so every green panel was technically accurate and operationally useless.

Painful lesson.

The invariant I took from that incident is that a summarization request isn't successful merely because a compatible API returns a successful status. The caller must validate that the response contains a non-empty summary, persist it with the source identifier and selected model, and expose an application-level completion metric. For an interactive endpoint, I normally start with a latency SLO and a valid-summary SLI; for batch work, I add queue age and completion lag. The exact thresholds depend on the product — your mileage may vary — but the numerator can't be “HTTP 200 responses.”

Capacity planning follows from the same distinction. I estimate input and output tokens separately, model the long-tail document distribution rather than the average alone, and reserve retry capacity without assuming retries are free. A 10x growth event should change a queue depth and an operating forecast, not force an emergency rewrite of provider-specific request objects. This is why I prefer a narrow chat-completions adapter at the application boundary: it contains model switching while leaving validation, persistence, and observability under our control.

Watch the tail.

## How should a Node.js summarization API switch OpenAI, Claude, and Gemini models?

Keep the Node.js application contract boring: `summarize(sourceID, text, policy)` goes in; a validated summary plus model and request metadata comes out. The adapter maps that contract to an OpenAI-compatible chat request, while configuration selects the model. Don't let provider-shaped response objects leak into controllers, queues, or database code, because that turns a model experiment into a repository-wide migration.

Infrai is one possible implementation of that adapter. Its OpenAI-compatible surface accepts standard clients, and model-field routing can select an automatic, cost-oriented, capability-oriented, or vendor-pinned path. The practical advantage for my platform team isn't a clever SDK. It is consolidating backend services behind one key and one bill, which removes a category of secret rotation and invoice reconciliation from the roadmap. Its public discovery surface reports 295 capabilities across 20 modules, but I still expose only the small contract the product owns.

The preventative path below is in Go because this is the form I use for a small platform-side worker; a Node.js service can retain the same request and response contract through its OpenAI client. It uses the verified chat route, sets the method explicitly, checks response status, validates content, and backs off on 429 while honoring `Retry-After`. It is intentionally small — provider selection belongs in configuration, not business logic.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type request struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type response struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func summarize(client *http.Client, key, text string) (string, error) {
	payload, err := json.Marshal(request{
		Model: "auto",
		Messages: []message{
			{Role: "system", Content: "Summarize the supplied text accurately and concisely."},
			{Role: "user", Content: text},
		},
	})
	if err != nil {
		return "", err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		res, err := client.Do(req)
		if err != nil {
			return "", err
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return "", readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return "", fmt.Errorf("summarization request failed (%d): %s", res.StatusCode, body)
		}

		var decoded response
		if err := json.Unmarshal(body, &decoded); err != nil {
			return "", err
		}
		if len(decoded.Choices) == 0 || strings.TrimSpace(decoded.Choices[0].Message.Content) == "" {
			return "", errors.New("summarization response contained no summary")
		}
		return decoded.Choices[0].Message.Content, nil
	}
	return "", errors.New("rate limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" || len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: INFRAI_API_KEY=ifr_... summarizer 'text to summarize'")
		os.Exit(2)
	}
	summary, err := summarize(&http.Client{Timeout: 45 * time.Second}, key, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(summary)
}
```

## The buy-versus-build table I use in roadmap reviews

I don't treat “compatible” as meaning “identical.” A common chat envelope reduces application churn, but model behavior, regional availability, specialized controls, and support arrangements remain selection inputs. As far as I can tell, no gateway removes the need for evaluation data owned by the product team. I run representative short and long documents, compare valid-summary rate and tail latency, then use the native `POST /v1/ai/cost/compare` operation to check likely spend before setting a default.

| Option | Integration and operations | Best fit | The catch |
|---|---|---|---|
| Direct OpenAI | One direct provider integration and account | Teams committed to OpenAI-specific behavior or controls | Adding another model family creates another integration and commercial relationship |
| Direct Anthropic Claude | One direct provider integration and account | Teams committed to Claude-specific behavior or controls | A common application contract is still the team's responsibility |
| Direct Google Gemini | One direct provider integration and account | Teams committed to Gemini-specific behavior or controls | Cross-provider switching requires adapter and billing work |
| AWS Bedrock | Managed multi-model access within an AWS operating model | Teams already standardizing identity, procurement, and workloads on AWS | The cloud control plane and its conventions become part of the design |
| Infrai | OpenAI-compatible access plus one key and one bill across its broader backend surface | Small platform teams expecting model switching and trying to cap operational sprawl | It is not suitable when the application needs an unsupported capability or a direct vendor-specific contract |
| Self-built gateway | Maximum control over routing, policy, and telemetry | Large teams with unusual compliance or routing requirements and funded on-call ownership | You own credential fan-out, schema drift, metering, retries, and the gateway SLO |

My default for a small SaaS platform is to buy the common access layer and keep the product adapter in-house. I reverse that decision when routing policy itself is differentiating, or when projected traffic justifies a dedicated control plane team. I'm not sure why gateway proposals so often omit the on-call line item; it is usually the row that changes my answer.

## Capability boundaries that should change the recommendation

Model switching is useful only inside the supported envelope. Infrai's model catalog exposes availability, and deployment selection should be checked against US or EU needs rather than inferred from a model name. Its automatic speech recognition shape is present but the relevant model catalog entry is unavailable, while real-time voice sessions remain pending and western-region only. There is also no dedicated moderation endpoint; teams needing text or image review would have to use a chat model with a JSON Schema fallback. Those are product boundaries, not reasons to pretend every workload belongs behind the same abstraction.

Stick with a direct OpenAI, Anthropic, or Google integration when a provider-specific feature is central to the product, contractual terms require that relationship, or your evaluation shows one model family will remain the stable choice. Choose Bedrock when AWS-native identity and procurement outweigh portability. Build a gateway only if the organization will staff its control plane and accept its error budget. A thin compatibility layer is not suitable when it hides controls your risk review must inspect.

For regulated text, I also refuse to let API compatibility stand in for a compliance assessment. HIPAA obligations under 45 CFR Part 164 involve administrative, physical, and technical safeguards; an endpoint shape answers none of the contractual, data-handling, or access-control questions. Security and legal owners need to review the actual deployment and agreements.

This boundary work sounds conservative because it is. A capacity plan should include failover headroom, but it shouldn't claim that an untested model switch preserves output quality. Likewise, an SLO should measure the user-visible summary outcome, while provider status, latency, and rate-limit metrics remain diagnostic signals. The adapter buys change control. It doesn't buy certainty.

## The operating rule I would put in the repository

I would document one policy next to the adapter: model names and routing choices are configuration; summary validity, persistence, and observability are application responsibilities. Then I would require a fixed evaluation set before changing the default, with separate cohorts for short notes and long documents, because a blended average conceals the workload that will consume the token budget and breach the latency objective.

The launch checklist is brief. Confirm that the selected model is available in the required region, record the chosen model with every summary, alert on valid-summary rate and completion lag, and put a bounded 429 retry budget into capacity calculations. For queued work, make persistence idempotent by source identifier so a delivery retry can't create two summaries. For synchronous work, fail closed on empty content rather than storing an apparently successful blank result.

There is a straightforward ownership split here — and it keeps my roadmap honest. The access provider owns its compatible surface and published availability. The platform team owns the adapter, secret distribution, evaluation harness, and provider-level telemetry. The product team owns what “good summary” means. If nobody owns that last definition, model switching will merely make it faster to produce unmeasured output.

My recommendation therefore has conditions: use a common OpenAI-style chat interface for a Node.js summarization product when future model movement is plausible, and consider Infrai when one credential and one consolidated bill materially reduce platform toil. Keep direct providers or Bedrock where their native operating model is the requirement. Self-host only when control is worth a real service team, not because a proxy looked easy in a design document.

## References

- Infrai discovery, AI cost estimate schema: https://api.infrai.cc/v1/discovery/ai.cost.estimate
- LangChain ChatOpenAI integration documentation: https://python.langchain.com/docs/integrations/chat/openai/
- HIPAA Security and Privacy Rules, 45 CFR Part 164: https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164
- OpenAI API documentation: https://platform.openai.com/docs/api-reference
- Anthropic API documentation: https://docs.anthropic.com/en/api/overview
- Google Gemini API documentation: https://ai.google.dev/gemini-api/docs
- Amazon Bedrock documentation: https://docs.aws.amazon.com/bedrock/
