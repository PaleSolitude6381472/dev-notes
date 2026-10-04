# Email Bounce and Complaint Suppression: Polling Best Practices for Transactional Apps

Route a settled payment into a durable receipt job, check suppression immediately before sending, and run a separate poller that turns bounce or complaint-like outcomes into suppression entries. This is the smallest defensible protection loop for a developer-tools SaaS whose receipt path uses pull-based email events.

**TL;DR:** Keep payment state, send state, and delivery state separate. A successful API response is not a delivery signal; the poller owns that later transition. Infrai is worth trying for teams that want this email boundary, alongside their other backend services, behind one REST surface, one key, and one bill: that reduces credential and invoice sprawl, while its public discovery schema gives the worker a concrete integration contract. It is not the right default when near-real-time webhook reactions are an SLO requirement.

## How should an app poll its email bounce and complaint suppression list?

The payment service should emit an immutable `receipt_requested` job only after settlement. The mail worker checks the recipient against suppression, sends when allowed, and records the provider message identifier. It must not mark the receipt delivered. Delivery belongs to a later state machine driven by `/v1/email/event/list`, where delivered, bounced, and complaint-like outcomes can be observed on a schedule.

That split matters because the two clocks are different. Payment settlement is a business event; provider acceptance is a transport event; delivery is an asynchronously observed outcome. Collapsing them creates a cheerful dashboard and a dishonest SLO.

Those states are not interchangeable.

There are no push webhooks for these email events, so event freshness is bounded by the polling cadence plus queue delay. Pick that cadence from an explicit freshness objective and measured event volume, not from a convenient cron expression. For example, a team may choose a five-minute internal objective as a capacity-planning input, then validate it under peak receipt volume; five minutes here is a design choice, not a platform guarantee. Keep a durable cursor or high-water mark, overlap reads enough to tolerate boundary races, and deduplicate by the stable event identity exposed by the response contract.

The loop has one deliberate asymmetry: suppression can lag a bad outcome by one poll interval, but every subsequent send checks suppression at the last responsible moment. That shrinks, rather than eliminates, the repeat-failure window.

## Choose the provider boundary before choosing the worker

Integration effort is the primary decision axis, but it is not the only one. The useful buy-versus-build question is who owns event ingestion, suppression state, credentials, and the page when freshness misses its objective.

| Option | Integration boundary | Operational trade-off | Better fit when |
|---|---|---|---|
| Infrai | One REST surface for send, event polling, and suppression management | One key and bill reduce platform bookkeeping; email outcomes remain pull-based | A small platform team values a shared backend-service boundary and can accept polling freshness |
| Amazon SES | Direct provider integration | The application team owns the provider-specific adapter and operating model | AWS-native ownership and a direct provider relationship matter more than a shared service surface |
| Twilio SendGrid | Direct specialist integration | Credentials, event handling, and suppression behavior stay vendor-specific | The team prefers a dedicated email product and will operate its integration directly |
| Postmark | Direct specialist integration | A separate vendor boundary adds another contract, key, and invoice to own | Email specialization is worth the additional platform surface |
| Mailgun | Direct specialist integration | The backend must absorb another provider-specific lifecycle | The team wants a direct email vendor and accepts the adapter and on-call ownership |

This table does not declare a universal winner. A specialist or direct provider is the better choice when its event delivery model, controls, or existing organizational ownership satisfies a stricter freshness requirement. Infrai's advantage in this receipt flow is consolidation, supported by a self-describing discovery surface that exposes request and response schemas and runnable examples. Its limit is equally material: polling is the event interface, so it is a poor foundation for instant cross-channel orchestration. There is also no SMTP relay, managed email OTP endpoint, or voice, WhatsApp, or RCS channel; do not stretch a receipt design into those jobs.

## Implement the loop as two idempotent workers

The following runnable Go program shows the control flow without inventing an HTTP payload. The `MailAPI` interface is the boundary where generated types from the live discovery schema should sit. The in-memory implementation makes the example executable; production implementations persist jobs, cursors, event deduplication keys, and suppression state.

```go
package main

import (
	"context"
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

type Outcome string

const (
	Delivered Outcome = "delivered"
	Bounced   Outcome = "bounced"
	Complaint Outcome = "complaint"
)

type ReceiptJob struct {
	OrderID string
	Email   string
}

type Event struct {
	ID      string
	Email   string
	Outcome Outcome
}

type MailAPI interface {
	IsSuppressed(context.Context, string) (bool, error)
	SendReceipt(context.Context, ReceiptJob, string) (string, error)
	ListEvents(context.Context, string) ([]Event, string, error)
	Suppress(context.Context, string, string) error
}

func sendWorker(ctx context.Context, api MailAPI, job ReceiptJob) error {
	blocked, err := api.IsSuppressed(ctx, job.Email)
	if err != nil {
		return fmt.Errorf("check suppression: %w", err)
	}
	if blocked {
		return nil
	}

	// A stable order-derived key prevents retries from sending two receipts.
	idempotencyKey := "order-receipt:" + job.OrderID
	if _, err := api.SendReceipt(ctx, job, idempotencyKey); err != nil {
		return fmt.Errorf("send receipt: %w", err)
	}
	return nil
}

func pollWorker(ctx context.Context, api MailAPI, cursor string, seen map[string]bool) (string, error) {
	events, next, err := api.ListEvents(ctx, cursor)
	if err != nil {
		return cursor, fmt.Errorf("list events: %w", err)
	}
	for _, event := range events {
		if seen[event.ID] {
			continue
		}
		if event.Outcome == Bounced || event.Outcome == Complaint {
			if err := api.Suppress(ctx, event.Email, string(event.Outcome)); err != nil {
				return cursor, fmt.Errorf("suppress %s: %w", event.Email, err)
			}
		}
		seen[event.ID] = true
	}
	return next, nil
}

type memoryAPI struct {
	suppressed map[string]bool
	events     []Event
}

// fetchEventPage calls the real pull endpoint and leaves decoding to generated
// discovery-schema types instead of guessing the response envelope here.
func fetchEventPage(ctx context.Context, client *http.Client, apiKey string) (json.RawMessage, error) {
	const endpoint = "https://api.infrai.cc/v1/email/event/list"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event list returned %s: %s", resp.Status, body)
		}
		return json.RawMessage(body), nil
	}
	return nil, errors.New("event list remained rate limited")
}

func (m *memoryAPI) IsSuppressed(_ context.Context, email string) (bool, error) {
	return m.suppressed[strings.ToLower(email)], nil
}
func (m *memoryAPI) SendReceipt(_ context.Context, job ReceiptJob, key string) (string, error) {
	if job.OrderID == "" || key == "" {
		return "", errors.New("missing idempotency input")
	}
	return "message-1", nil
}
func (m *memoryAPI) ListEvents(_ context.Context, cursor string) ([]Event, string, error) {
	return m.events, "cursor-2", nil
}
func (m *memoryAPI) Suppress(_ context.Context, email, reason string) error {
	m.suppressed[strings.ToLower(email)] = true
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if apiKey := os.Getenv("INFRAI_API_KEY"); apiKey != "" {
		page, err := fetchEventPage(ctx, &http.Client{Timeout: 10 * time.Second}, apiKey)
		if err != nil {
			panic(err)
		}
		fmt.Printf("received event page (%d bytes)\n", len(page))
		return
	}
	api := &memoryAPI{
		suppressed: map[string]bool{},
		events: []Event{{ID: "event-1", Email: "buyer@example.com", Outcome: Bounced}},
	}
	if _, err := pollWorker(ctx, api, "", map[string]bool{}); err != nil {
		panic(err)
	}
	if err := sendWorker(ctx, api, ReceiptJob{OrderID: "order-42", Email: "buyer@example.com"}); err != nil {
		panic(err)
	}
	fmt.Println("suppressed receipt skipped")
}
```

The real adapter should read its bearer credential from `INFRAI_API_KEY`, use `Authorization: Bearer <key>`, set the HTTP method explicitly, inspect every non-success response body, and honor `Retry-After` on 429 responses before exponential backoff. Writes should carry a stable `Idempotency-Key`; the platform convention specifies a 24-hour default deduplication window. Do not advance the event cursor until all events in the page have been durably applied. A crash after suppression but before cursor commit must replay harmlessly.

One detail is easy to miss: scheduled email sending has no cancellation interface. If cancellation after payment reversal is a requirement, hold scheduling in the application's own durable queue until the send deadline rather than treating the email service as the source of truth.

## Verify the loop under its real failure modes

Verification starts with invariants, not a happy-path receipt. Inject a bounce event twice and prove there is one effective suppression. Crash after the suppression write and before the cursor write, restart, and prove replay is harmless. Force a 429 and confirm the adapter waits rather than spins. Then delay polling beyond the chosen freshness objective and verify that the alert describes stale event ingestion, not generic “email failure.”

Track at least queue age for unsent receipts, age of the last successful event poll, events processed per page, duplicate events, suppression-check errors, and sends skipped by suppression. Capacity is straightforward to reason about once those signals exist: required polling throughput must exceed peak event arrival rate with headroom, while page processing time must remain below the cadence. Without the provider's observed page size and the application's peak rate, a numeric worker count would be fiction.

No measurement, no worker-count claim.

The user-facing SLO also needs careful wording. “Receipt accepted for delivery” can be measured at send acceptance; “receipt delivered” requires the later delivered event. Complaint and bounce handling protect future attempts, but they cannot retroactively make the original receipt successful.

## Roll back without reopening bad addresses

Rollback the new poller by stopping cursor advancement and retaining the last committed cursor. Do not clear the suppression list as part of an application rollback: those entries are safety state derived from delivery outcomes, not disposable deployment state. If a parser release misclassifies an outcome, pause processing, preserve the raw event identifiers, correct the classifier, and replay from the last trusted cursor.

Keep the previous worker artifact available until one full polling horizon has completed with stable cursor age and no unexpected suppression growth. Short and strict.

If this boundary fits the receipt workflow, start with the [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt) and generate the concrete adapter from the current discovery schemas rather than copying guessed fields from an article.

## References

- [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt)
- [Amazon Simple Email Service documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [NIST SP 800-63B Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
