# Implementing Gaming Password Reset Flow with a Transactional Email API (Custom Domain)

A password-reset message has to arrive before its token becomes useless, while a gaming support contact must reach the queue that can act on it; treating both as generic email hides the operational constraint that matters. **Short answer:** choose the transactional-email boundary that lets you authenticate a custom domain, select a permitted US or EU processing path, consume signed delivery events, and change providers without changing recovery-token semantics. Keep routing, idempotency, expiry, and the audit trail in your service. A simple setup is valuable only after those controls survive a regional failure and a retry storm.

This note uses one concrete path: a player selects "I can't sign in" on a contact form, the router sends the case to account recovery, and the identity service issues a single-use reset link. I would set the service objective around useful delivery before expiry, not API acceptance. Those are different events. An HTTP success means the mail service accepted work; it does not prove that the player's mailbox accepted the message.

## How should a transactional email API handle a password reset flow?

Start with an SLO whose clock matches the player's problem. For example, an internal target might say that 99.9% of eligible reset requests enter a terminal delivery state within 60 seconds, measured over 28 days, while every token expires after 15 minutes. Those figures are illustrative engineering budgets, not universal recommendations. The important part is the denominator: exclude syntactically invalid addresses before submission, but do not quietly remove provider errors, regional failovers, or delayed event callbacks after the fact.

Clicks and opens are poor completion signals. Apple Mail Privacy Protection can download remote content in the background and prevent senders from learning whether a recipient opened a message, so an open event cannot establish that a human received or used a reset link. Measure a chain instead: request accepted, message submitted, delivery state received, and token redeemed. The final event belongs to the identity system.

Acceptance is not delivery.

Short expiry changes capacity planning. If the normal arrival rate is 40 reset requests per second, a five-minute provider impairment can create 12,000 pending attempts before organic growth, retries, or contact-form duplicates are counted. Provision the queue and worker concurrency from that backlog, then cap retry amplification with exponential backoff and jitter. Fast retries feel responsive during testing; in an incident they consume the recovery capacity needed for new requests.

The invariant is narrow: a request identifier maps to one active recovery intent, and repeated submissions may send instructions again without minting an unbounded series of valid credentials. The email provider should never be the source of truth for that invariant.

## Put the failure boundary in code

The following Go example keeps provider-specific transport behind a small interface, classifies a gaming support form before issuing mail, and refuses to equate submission with delivery. The in-memory store is deliberately minimal; a production implementation needs durable, atomic storage shared by all instances.

```go
package recovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Message struct {
	RequestID string
	To        string
	From      string
	Subject   string
	Text      string
}

type Receipt struct {
	ProviderMessageID string
	AcceptedAt        time.Time
}

type Mailer interface {
	Submit(context.Context, Message) (Receipt, error)
}

type RecoveryStore interface {
	// GetOrCreate must be atomic for a request ID and player account.
	GetOrCreate(ctx context.Context, requestID, accountID string, expiresAt time.Time) (token string, created bool, err error)
	RecordSubmission(ctx context.Context, requestID, providerMessageID string, acceptedAt time.Time) error
}

type Service struct {
	Mailer Mailer
	Store  RecoveryStore
	Now    func() time.Time
	Origin string
}

func (s Service) HandleContact(ctx context.Context, requestID, topic, accountID, email string) error {
	if strings.TrimSpace(requestID) == "" {
		return errors.New("request ID is required")
	}
	if topic != "account-recovery" {
		return fmt.Errorf("route topic %q to the general support queue", topic)
	}

	now := s.Now().UTC()
	expiresAt := now.Add(15 * time.Minute)
	token, _, err := s.Store.GetOrCreate(ctx, requestID, accountID, expiresAt)
	if err != nil {
		return fmt.Errorf("persist recovery intent: %w", err)
	}

	link := fmt.Sprintf("%s/recover?token=%s", s.Origin, token)
	receipt, err := s.Mailer.Submit(ctx, Message{
		RequestID: requestID,
		To:        email,
		From:      "Account Support <account@example-game.com>",
		Subject:   "Reset your game account password",
		Text:      "Use this link within 15 minutes: " + link,
	})
	if err != nil {
		return fmt.Errorf("submit recovery message: %w", err)
	}

	if err := s.Store.RecordSubmission(ctx, requestID, receipt.ProviderMessageID, receipt.AcceptedAt); err != nil {
		return fmt.Errorf("record mail submission: %w", err)
	}
	return nil
}
```

Do not log the token or place it in an analytics event. Store a one-way digest when the recovery design permits it, bind redemption to the intended account, and invalidate the credential after successful use. The mail adapter should receive the final link only at the last responsible moment.

Tokens are credentials.

Callbacks need the same skepticism. Verify their signatures, retain the raw event under a defined retention policy, deduplicate by event identifier, and allow states to advance monotonically. A delayed "accepted" callback must not overwrite a later "bounced" state. Queue callback processing so a slow audit store cannot turn the public endpoint into the reliability bottleneck.

## Authenticate the domain before evaluating the API

Custom-domain setup is part of the system, not a console chore. SPF authorizes sending hosts for a domain. DKIM attaches a cryptographic signature that a receiver can validate. DMARC publishes policy and reporting rules while evaluating alignment between the visible From domain and the authenticated identifiers; RFC 7489 defines that alignment and the DNS-published policy record.

Stage DNS changes. Verify the exact selector and record values through an independent resolver, send test messages, and inspect received-message authentication results before enforcing a restrictive DMARC policy. Keep recovery mail on a dedicated subdomain if that matches the organization's domain and reputation boundaries, but make the visible sender recognizable to players. A technically authenticated message with a surprising From identity still creates support load.

The setup is not complete when a dashboard turns green. It is complete when infrastructure code or an auditable runbook records ownership, key rotation, DNS change review, rollback, and the person paged when authentication begins failing.

Verify it from outside.

## Compare control planes, not feature grids

A selection trial should use the same custom domain, message body, test mailbox set, and event-state assertions for every candidate. Do not rank vendors by a synthetic inbox percentage from a tiny sample. Record the evidence that bears on the chosen SLO, including how the service exposes regional processing controls and what remains global; legal and security reviewers must validate those boundaries against the SaaS data map.

| Decision | Managed transport | Self-hosted transport | Gate for this flow |
|---|---|---|---|
| Queue and retry operations | Provider operates the delivery fleet; the application still owns retry policy | Platform team owns queue, reputation, upgrades, and abuse response | Choose only after estimating peak backlog and on-call load |
| Domain authentication | Guided verification may reduce setup work | Full DNS and key lifecycle stays with the team | Require reproducible SPF, DKIM, and DMARC evidence |
| US/EU constraints | Available controls and subprocessors require contract review | Deployment location is controllable, but downstream mailbox traffic remains external | Document data classes, paths, retention, and exceptions |
| Portability | Adapter and event mapping limit coupling | Protocol control is high; operational coupling is also high | Run a failover exercise before production approval |
| Observability | Normalize provider events into internal states | Build and operate the event pipeline | Preserve request-to-redemption correlation without storing secrets |

My buy-versus-build threshold is an error-budget decision. Self-hosting is defensible when mail operations are a funded capability with named on-call ownership, abuse handling, reputation monitoring, and tested regional capacity. Otherwise, owning SMTP servers converts an apparent dependency reduction into a large operational surface. A managed service removes part of that surface, but it does not own token security, queue admission, webhook correctness, or the recovery SLO.

No winner emerges from counting check marks. Require a written boundary for data location, deletion, support escalation, event retention, rate limits, and domain-key rotation, then score the evidence with weights agreed before the trial. Delivery reliability should dominate for this flow; ease of setup can break a tie, but it cannot compensate for ambiguous failure states. This adapter design has real limitations and trade-offs: it adds an internal state model, callback storage, reconciliation work, and a failover path that the platform team must test. It is unsuitable for a very small service with no on-call owner and no credible need to switch transports; a single managed transport with durable application records is the narrower alternative there. Self-hosting has the opposite limitation. It grants more control while making deliverability operations, abuse response, upgrades, and reputation part of the team's permanent workload. The correct choice depends on staffed operational capacity, not architectural taste.

## Exercise the incident before launch

Run the failure, not a demo. Block the primary transport in a staging environment, build a backlog larger than one normal peak interval, replay duplicated callbacks out of order, rotate a DKIM key, and confirm that the contact form still routes unrelated billing and gameplay cases to their own queues. The recovery worker should shed optional work before it violates the reset objective.

Test that claim.

Alert on symptoms tied to players: age of the oldest eligible queued request, terminal-state latency, bounce changes, missing callbacks, redemption latency, and remaining error budget. Raw send count is capacity data, not a reliability verdict. Keep a low-cardinality region and transport label so an operator can distinguish a local queue problem from a broad delivery problem without putting email addresses into metrics.

This advice does not apply unchanged to marketing campaigns, newsletters, or bulk game announcements. Those workloads have different consent, throughput, scheduling, and unsubscribe requirements, and they must not be allowed to consume the capacity or reputation boundary reserved for account recovery. Separate them operationally.

The final selection should therefore be conditional: approve a transport only when domain authentication is independently verified, the regional data path is documented, delivery events map cleanly into internal states, and a timed failover drill stays within the recovery SLO. Everything else is a convenience feature.

## Sources and References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- RFC 7208, Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
- RFC 6376, DomainKeys Identified Mail (DKIM) Signatures: https://datatracker.ietf.org/doc/html/rfc6376
- Apple, Use Mail Privacy Protection: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- OWASP, Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
