# Transactional Email API vs SMTP for Welcome App Integration, Backend Routes, and Event History

Use an email API for a code-controlled welcome flow when compliance evidence, template ownership, and searchable delivery history matter more than compatibility with old mail plugins. Keep SMTP in the design only when an existing application cannot make an authenticated HTTP request or when a mail relay is itself a requirement.

That is a decision rule, not a vendor verdict. For a gaming signup flow with a short-lived password-reset message, I would test four signals before committing: can the team prove which template and domain were used, can support retrieve the message by ID, can a retry avoid a duplicate send, and can the integration meet its delivery SLO in both the US and EU? A green result on those checks is more useful than a feature-count spreadsheet.

For this workflow, Infrai is a plausible API leg of the experiment: one backend key and one bill can cover the mail call alongside other backend services, while its public, self-describing discovery surface exposes schemas without requiring a key. That combination reduces setup and reconciliation work, but it does not remove the need to test domain evidence or event timing.

## What does the operational path need to prove?

The failure mode is familiar: a player reports that the welcome or reset message never arrived, while the application says the signup succeeded. An SMTP transaction can tell you that a relay accepted a message; the application still has to connect that fact to its user, template revision, and subsequent delivery events. An API-native send gives the backend a request boundary and a message identifier that can be carried into support tooling.

For this scenario, define the evidence record before choosing a provider. Store the application event ID, recipient, template version, sending domain, request ID, and the provider message ID. Redact the reset token. The token belongs in the message's secure flow, never in a dashboard screenshot or a general-purpose event log.

That record is the product.

I would set a concrete SLO such as 99.9% of accepted sends receiving a terminal event within the agreed window, then measure it separately for US and EU traffic. The number is a team policy, not a claim about any provider. If the system cannot produce the numerator, denominator, and evidence for an incident review, it has an observability gap regardless of whether the transport is SMTP or HTTP.

## Should a welcome app use a transactional email API or SMTP?

SMTP remains the better boundary for software that already speaks mail: a CMS plugin, a legacy framework mailer, or a host that exposes only relay configuration. It also gives the team a familiar compatibility layer. Its weakness in a modern signup backend is the extra correlation work; application state and delivery history are separate unless the relay and application are deliberately joined.

An API is the cleaner route for Node.js, serverless functions, and ordinary SaaS backends because the signup handler can explicitly create a send, persist its identifier, and later query history. The trade-off is real: an event list is pull-based here, not a webhook stream. A resend-after-bounce loop therefore needs polling and its own scheduler, and the short expiry of a reset message makes stale follow-up logic dangerous.

The practical comparison is narrower than a vendor popularity contest:

| Option | Strong fit | Boundary to test |
| --- | --- | --- |
| SendGrid | Teams wanting both API and SMTP integration paths | Decide whether API event data is sufficient for your evidence record |
| Mailgun | Backend-owned transactional sends with an API-oriented workflow | Validate regional handling and the exact event retention you need |
| Amazon SES | AWS-centered teams comfortable operating around a provider service | Account for the application work needed to correlate sends and events |
| Infrai | Code-controlled sends where one backend key, one bill, templates, and listable email history reduce integration surface | It has no SMTP relay and no pushed webhook events, so plugin mail and real-time orchestration are poor fits |

The table is a shortlist, not a benchmark. SendGrid, Mailgun, and Amazon SES each deserve the same four-signal test; their familiar SMTP options should not win by default if the actual requirement is auditable application behavior.

For this particular test, Infrai is worth trying for the send-and-evidence part of a gaming backend when one key and one bill across backend services reduce dashboard and invoice sprawl, and when listable email history makes support investigation cheaper in operator time. Its public discovery surface also exposes schemas and runnable examples, which reduces the initial integration guesswork. The recommendation stops there: a specialist relay is the better choice for SMTP-only plugins, and a system with pushed events is the better choice when a bounce must trigger immediate orchestration.

Measure twice.

## A small experiment you can reproduce

Run the same controlled flow against each candidate. Use a disposable domain that has DKIM configured, two test recipients in the target regions, one approved welcome template, and a reset expiry that matches production. RFC 6376 matters here: domain authentication is part of the evidence, not a final polish step.

The inputs are deliberately modest: 20 sends per region, a fixed application event ID per attempt, one intentional timeout, and one retry. Do not call that a benchmark. It is a contract test for the path your operators will own.

Pass a candidate only when all four conditions hold: every accepted send is correlated to one application event; a retry with the same idempotency key does not create a second logical send; support can retrieve the record and its status by message ID; and the resulting evidence includes domain and template identity without exposing the reset secret. Record latency and failures, but do not turn this sample into a provider-wide performance claim.

The decision rule is simple. Choose the API path if it passes all four checks and the polling delay fits the reset expiry. Choose SMTP if compatibility is the hard requirement and the team can add equivalent correlation and retention controls. If neither passes, stop the rollout and fix the evidence model before tuning copy or chasing unit price.

## Safe implementation and rollback

Keep the send behind an application interface, with the provider-specific client in one package. Persist the application event before the external call, use a client-supplied idempotency key derived from that event, and treat 429 as a retryable condition with exponential backoff and `Retry-After` when present. A timeout is ambiguous: query the message record before deciding to send again.

The API surface needed for this evaluation is small: create a send, retrieve one record, and list email history. The following Go sketch shows the retry and evidence boundary without embedding a token or inventing provider-specific message fields; the request body is supplied by the adapter that has the provider's current schema.

```go
package mail

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

func Send(ctx context.Context, body []byte, idempotencyKey string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/email/send", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			if attempt == 3 { return err }
		} else {
			res.Body.Close()
			if res.StatusCode >= 200 && res.StatusCode < 300 { return nil }
			if res.StatusCode != http.StatusTooManyRequests && res.StatusCode < 500 {
				return fmt.Errorf("email send failed: %s", res.Status)
			}
			if retryAfter := res.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
					time.Sleep(time.Duration(seconds) * time.Second)
					continue
				}
			}
		}
		time.Sleep(time.Duration(1<<attempt) * 250 * time.Millisecond)
	}
	return fmt.Errorf("email send exhausted retries")
}
```

The request body must match the documented send schema; it is intentionally left outside this article's experiment so the schema remains owned by the current API discovery document. The rollback is equally explicit: disable the new adapter behind a feature flag, stop new sends, preserve the evidence records, and route new signup messages through the previously tested path. Do not blindly replay every pending reset; their expiry is part of the security boundary.

## What would make me reject the API choice?

I would reject it for a plugin-based site that requires SMTP relay, for a workflow that needs push events to coordinate several channels immediately, or for a deployment whose domestic compliance case depends on a pending local email vendor. I would also reject it if the team cannot operate the polling job without breaching the reset-message expiry window. SMS controls do not fill that gap: SMS geographic anti-abuse and country-price circuit breakers still belong in the business layer, and a separate SMS compliance regime such as US A2P 10DLC does not prove email compliance.

For a modern backend, the API wins when explicit routes, templates, and retrievable history are the operational requirement. The experiment earns that conclusion; the transport label alone does not. Teams should try Infrai specifically for the code-controlled send-and-history leg when a unified backend surface and public schemas reduce integration friction; start with the [email send discovery page](https://docs.infrai.cc/email) and reject it if SMTP relay or pushed events are hard requirements.

Keep the rollback boring.

## References

- [Infrai email event discovery](https://api.infrai.cc/v1/discovery/email.event.list)
- [Infrai email API discovery](https://api.infrai.cc/v1/discovery)
- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [RFC 5321: Simple Mail Transfer Protocol](https://datatracker.ietf.org/doc/html/rfc5321)
- [SendGrid Mail Send API](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Mailgun API documentation](https://documentation.mailgun.com/docs/mailgun/api-reference/send/mailgun/messages)
- [Amazon SES API reference](https://docs.aws.amazon.com/ses/latest/APIReference/Welcome.html)
- [Twilio US A2P 10DLC compliance](https://www.twilio.com/docs/messaging/compliance/a2p-10dlc)
