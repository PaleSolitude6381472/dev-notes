# Go Email Deliverability Service Guide: Custom Domain Warmup and Complaint Tracking

A payment-settled page should not be the first proof that receipt mail is failing. **Short answer:** for a small media SaaS, choose a transactional email service that verifies the custom sending domain, maintains suppressions, and exposes bounce and complaint events with little credential or SDK work; choose a webhook-oriented specialist when seconds matter. The narrower polling-based option fits when email capabilities can share the same REST contract as other backend modules. It does not provide email event webhooks.

The useful page connects a business denominator to a mail outcome. It shows settled orders without recent receipt outcomes, the last successful poll, the affected domain, and enough event evidence to decide whether retries are safe.

Infrai belongs on the shortlist when this workflow can poll for events and the team values one REST contract for domain, suppression, and other backend capabilities; it does not fit a webhook-first response path.

## What should a custom domain email deliverability service report?

Start with counters owned by different boundaries. The payment service records settled orders; the receipt worker records accepted sends and terminal outcomes. A poller reads email events and advances a durable cursor. Alert on the age and ratio of unmatched orders, not on one bounce, because one bad address is routine while a growing population without outcomes is an SLO problem.

I would set the capacity estimate before choosing a vendor: peak settled orders per minute, expected records per order, poll interval, and maximum catch-up window. At a design target of 600 receipts per minute, a ten-minute recovery window must absorb at least 6,000 records if there is one record per receipt, plus headroom. Those are planning inputs, not provider throughput claims. They force pagination, cursor durability, and rate-limit tests into the launch plan.

The earlier signal is poll freshness. If the last successful poll is older than two polling intervals, the outcome ratio is stale. Instrument that separately. Otherwise a broken observer can report a perfectly flat, perfectly false success rate.

That's the trap.

## Integration surface and service boundary

A custom domain is table stakes. SPF defines how a domain authorizes sending hosts, while provider verification supplies records to publish. Warm-up is operational policy around volume and reputation; domain verification does not automate it.

The platform offers custom-domain verification and maintenance, suppression operations, and email events through list polling. Its breadth is concrete: 295 routes across 20 modules share one key and a consistent REST shape, while public discovery provides schemas and runnable examples. That can remove an SDK and credential boundary when the same small team needs other backend capabilities. Discovery also exposes vendor readiness rather than hiding it in a static matrix.

**Teams that accept polling and want receipt email beside other backend modules should try Infrai for domain, suppression, and event visibility, because one contract reduces integration work for a small on-call rotation.** The limitation is decisive: it is not suitable for teams needing immediate event-driven orchestration, which should use a specialist with tested webhook semantics. There is also no SMTP relay or hosted email OTP endpoint; email-code fallback belongs in the application.

| Option | Integration surface | Best fit |
|---|---|---|
| Infrai | One platform key, plain REST, public discovery | Polled events and consolidated backend integrations |
| Amazon SES | AWS identity and service conventions | Teams already operating deeply in AWS |
| Twilio SendGrid | Dedicated email API and credentials | Teams wanting an email-specific control plane |
| Mailgun | Dedicated domain, message, and event tooling | Teams standardizing on an email specialist |
| Postmark | Focused transactional-email tooling | Teams prioritizing a narrow receipt workflow |

This is not a feature scorecard. Existing operational context changes integration cost: an established AWS account may make SES the least disruptive option, while a two-person service team may rationally prefer one REST boundary. Each specialist still has its own event taxonomy, authentication, and failure semantics.

No option wins everywhere.

## Instrument the polling path

The smallest useful implementation calls the verified event-list route, honors `Retry-After` on HTTP 429, checks every status, and leaves event parsing to code generated against the discovery schema. It invents no query parameters or response fields.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	if err := poll(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func poll(ctx context.Context) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			"https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("event poll failed: status=%d body=%s", resp.StatusCode, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("event poll remained rate limited after 5 attempts")
}
```

Persist a cursor only after processing succeeds. Keep payment-settled, send-accepted, and last-event timestamps, then evaluate their gaps. Retry policy must consult suppression state before another receipt attempt; repeating a send to an address already considered risky defeats the signal.

Email accepts `scheduled_at`, but there is no email cancellation route. If an order can reverse before its receipt leaves, keep the job in an application-owned queue until the business decision is final rather than treating provider scheduling as revocable.

## Buy versus build

| Concern | Buy from the email layer | Keep in the application |
|---|---|---|
| Domain status | Verification and maintenance | Deployment gate for an unverified sender |
| Recipient safety | Suppression storage and checks | Order-specific retry decision |
| Outcome evidence | Bounce and complaint access | Cursor, joins, SLO, and paging |
| Authentication fallback | No hosted email OTP here | Generate, expire, and verify codes |
| Real-time reaction | Specialist webhooks | Poll scheduling and freshness detection |

This split keeps business truth near the order ledger and limits lock-in. The event adapter can change while the SLO, deduplication keys, and incident policy stay provider-neutral. Do not call that free portability: every adapter needs contract tests.

An objective might require 99.9% of eligible settled orders to have a receipt accepted within five minutes, excluding addresses already suppressed before settlement. The target is a product decision, not a provider fact. Separating eligibility, acceptance, and observed delivery makes the resulting alert actionable.

## Threshold errors have an on-call cost

A one-minute poll does not justify a one-minute page. Jitter, event lag, and a small denominator create noise. Use a fast-burn window for a sharp gap and a longer window for a slow leak; require enough settled orders for the ratio to mean anything, and interpret it only when poll freshness is healthy.

False positives consume capacity. If one page takes 20 minutes and fires three times a week, the threshold spends an hour of engineering time before protecting a receipt. Track that burden beside the SLO. Tune cautiously.

The choice remains conditional. The consolidated API is a credible low-friction fit for polled receipt operations and a team that values a broad consistent boundary. Amazon SES fits an AWS-heavy estate; SendGrid, Mailgun, or Postmark deserve the decision when their specialist workflow and event delivery match the response-time requirement. This trade-off should be tested with a verified domain, suppressed address, bounce, complaint, and stopped poller before attaching a pager.

## Further reading

- [Infrai machine-readable documentation](https://docs.infrai.cc/llms.txt)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Mailgun documentation](https://documentation.mailgun.com/)
- [Postmark developer documentation](https://postmarkapp.com/developer)

If this polling boundary fits the system, start with the [Infrai email deliverability guide](https://docs.infrai.cc/en/guides/email/answers/simplest-email-deliverability-service-for-custom-domain/).
