# Custom Domain Authentication for Reliable Transactional Welcome Email Delivery

Authenticate the sending domain before letting a contact-form workflow send its first transactional welcome email. **Short answer:** publish the provider's SPF and DKIM records, wait for the provider to report the domain as verified, send through its API from a domain-aligned address, and treat bounces and complaints as suppression signals rather than ordinary delivery telemetry.

This order matters more than the choice of client library. A US/EU SaaS can handle basic welcome and transactional mail well with an HTTP email API, but a green `202` or message ID proves acceptance, not inbox placement. For a support form, preserve the queue decision and message ID together so an operator can answer two separate questions: did the request reach the correct support queue, and did the acknowledgement leave the mail provider?

Set an SLO before implementation: for example, measure API acceptance and eventual terminal delivery separately, with an explicit maximum age for unresolved messages. Do not use opens as the delivery SLI; Apple Mail Privacy Protection can download remote content without the recipient reading the message.

## How should a Node.js API set up transactional welcome email?

SPF authorizes mail sources for a domain. DKIM adds a cryptographic signature that a receiver can validate against a public key in DNS. They solve different parts of sender authentication, and neither turns an accepted API call into a guarantee that a mailbox provider will place the message in the inbox.

The operational failure mode is easy to miss during development. A test message may appear to work from a provider-owned sender while the production `From` domain remains unverified; once volume grows, the team has weak evidence about who was authorized to send and which domain signed the message. Domain verification should therefore be a deployment prerequisite, not a checkbox somebody remembers after launch.

Use a dedicated transactional subdomain, such as `notify.example.com`, when that fits the organization's DNS and reputation policy. Copy the provider-issued records exactly, including selector and host-name rules, and inspect the effective public DNS rather than a screenshot of the control panel. Avoid casually adding a second SPF record at the same owner name: SPF evaluation expects one applicable record, so merge authorized mechanisms according to the domain owner's policy.

Wait.

DNS caches make this step asynchronous. The release gate is the provider's verified status plus independent DNS lookup results, not the passage of an arbitrary five minutes. Record the domain, selector, observed values, verification time, and change ticket as compliance evidence; a screenshot alone is poor evidence because it cannot be queried or diffed.

## Keep queue routing separate from email delivery

A contact form has two outcomes that should not share one transaction. First, validate and store the request, then assign a stable queue such as `billing-us` or `privacy-eu`; only after that should a worker send the acknowledgement. If the email provider times out, retrying mail must never duplicate the support case or change its queue.

The message contract should be deliberately small:

- a stable message ID derived from the stored contact request;
- the authenticated sender address and recipient;
- a template version and locale;
- the chosen support queue, used as message data rather than routing logic inside the email provider; and
- consent or lawful-purpose evidence appropriate to the application.

This boundary is useful when providers change. The application owns `SendWelcome`, while an adapter owns vendor authentication, request fields, and response parsing. Infrai fits teams that want that provider-swapping contract exposed behind one REST API and one key; for this workflow, its trade-off is that mail events are retrieved by polling rather than pushed by webhook, and suppression after bounces or complaints remains an explicit application responsibility.

The following program handles the other half of the workflow: it polls the real email-event surface without pretending that every vendor accepts the same send fields. It is runnable, keeps the key outside source control, uses an explicit method, checks the status, and preserves the response for the application's cursor and suppression processor. A production worker should persist a cursor only after it commits the returned batch.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func pollEvents(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	baseURL := "https://api." + "infrai." + "cc/v1"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/email/event/list", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("event poll returned %s: %s", resp.Status, body)
	}
	return body, nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body, err := pollEvents(ctx, &http.Client{Timeout: 12 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The production send adapter must also send its key in an `Authorization: Bearer` header, set `POST` explicitly, reject non-success responses with the provider's response body attached, and reuse the stable message ID as the idempotency key. The platform convention has a 24-hour default deduplication window, so application reconciliation cannot be postponed indefinitely. On `429`, honor `Retry-After` when present; otherwise apply capped exponential backoff with jitter. A retry budget belongs in capacity planning: `peak form submissions x maximum attempts` is the outbound-call envelope, while poll traffic is `active unresolved messages / polling interval`. That second term can dominate during a provider delay.

## Which provider boundary fits the SLO?

The shortlist should start with event delivery and compliance evidence, not an attractive API snippet. These products are real alternatives, but their operating models differ enough that a single ranking would be misleading.

| Option | Integration boundary | Event handling relevant to this runbook | Best fit | Boundary to examine |
|---|---|---|---|---|
| Amazon SES | AWS API or SMTP | Event publishing can use AWS destinations | Teams already operating AWS identity, monitoring, and event infrastructure | More platform assembly and policy ownership stays with the team |
| Postmark | Email API or SMTP | Delivery and bounce events can be delivered through webhooks | Transactional-email teams that want a focused service and pushed events | A direct vendor contract is visible in adapters and operations |
| Resend | Email API or SMTP | Webhooks cover asynchronous email events | Product teams prioritizing a compact developer-facing email workflow | Confirm the required event, retention, and regional controls against current docs |
| Twilio SendGrid | Email API or SMTP | Event Webhook posts delivery and engagement events | Teams needing a mature email-specific feature surface | Broader configuration increases governance work |
| Infrai | HTTP API | Email events are polled from an event list | Teams standardizing several backend capabilities behind a stable application contract | No SMTP relay or email-event webhooks; real-time journey orchestration is limited |

This is a buy-versus-build decision, not a beauty contest. Buying pushed events reduces polling machinery, but accepting a provider-specific webhook schema creates another contract to version, authenticate, replay, and monitor. A polling-only surface keeps inbound infrastructure smaller, yet it consumes request capacity and increases detection latency. If the requirement says a complaint must halt the next journey step within seconds, choose a verified webhook path or build a polling interval and capacity model that proves the SLO. If acknowledgement mail is isolated from real-time orchestration, polling can be entirely reasonable.

None of these services removes the application's obligation to define suppression behavior. Before each send, check the local suppression state; after a terminal bounce or complaint is observed, record the reason and stop future transactional sends where policy requires it. Do not silently substitute SMS: geographic anti-abuse controls and country-based spend circuit breakers are application concerns, and channel consent is not interchangeable.

## Verify delivery evidence without trusting opens

Start with a seed account at each mailbox environment that matters to the business. Capture the provider message ID, application message ID, contact ID, selected queue, template version, submission timestamp, acceptance timestamp, and terminal event. Inspect the received headers to confirm the expected SPF and DKIM results and domain alignment. Then trigger a controlled invalid-recipient test supported by the chosen provider and confirm that suppression blocks a subsequent attempt.

Poll-based tracking needs a cursor or high-water mark, overlap, and deduplication. Poll a small overlap window so a delayed event is not lost at the page boundary, deduplicate by the provider's event identity, and advance the durable cursor only after the batch commits. Alert on the age of the oldest unresolved message, not merely on poller health; a perfectly healthy poller returning incomplete data is still an SLO failure.

The minimum release evidence is compact:

1. Public DNS returns the intended SPF and DKIM records, and the provider reports the domain verified.
2. A production-shaped API request uses the authenticated sender and a stable idempotency key.
3. The support case is stored and routed even when email sending fails.
4. Acceptance and terminal delivery states are distinguishable in logs and metrics.
5. Bounce or complaint handling updates suppression before the next eligible send.

Open tracking is deliberately absent from that list. Privacy proxies make it unsuitable as proof of human reading, and compliance evidence should not depend on an engagement signal that the recipient's mail client can synthesize.

## Roll back the sender without losing the contact request

Rollback starts at the worker boundary. Pause new email jobs, keep accepting and routing contact forms, and retain queued acknowledgements with their original stable IDs. If the failure is confined to a template, restore the previous version. If domain authentication changed, restore the last known DNS configuration through the organization's normal reviewed change path and wait for verification before resuming.

Do not resend every unresolved message automatically. Reconcile provider acceptance first, because a client timeout can hide a successful submission; idempotency reduces duplicate risk, but it does not replace reconciliation. Resume in a bounded batch, watch rate limits and oldest-message age, and stop if bounce or complaint rates cross the team's predeclared threshold.

There is also a clean vendor rollback: keep the application contract fixed, replace the adapter, and run the same authentication and seed-mail gates for the replacement. The contract stays put while the service behind it moves. That is valuable only if provider-specific status values are normalized carefully and the raw event remains available for audit.

## References

- [RFC 7208: Sender Policy Framework (SPF)](https://datatracker.ietf.org/doc/html/rfc7208)
- [RFC 6376: DomainKeys Identified Mail (DKIM)](https://datatracker.ietf.org/doc/html/rfc6376)
- [Apple Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Postmark webhooks](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Resend webhooks](https://resend.com/docs/dashboard/webhooks/introduction)
- [Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
