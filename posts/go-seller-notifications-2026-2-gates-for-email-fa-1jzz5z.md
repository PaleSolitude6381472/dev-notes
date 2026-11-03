# Go Seller Notifications 2026: 2 Gates for Email Fallback and SMS Status Polling

Keep seller order templates under an explicit release owner, send email first, and use status polling to decide whether SMS fallback is still justified for event notifications. For a media marketplace notifying a seller about a new order, the deciding constraint is who can change the message while a notification is waiting: a template edit must not silently change the meaning of an already committed order alert.

TL;DR: Pin the template revision and order identity, persist channel outcomes, and poll for an acceptable email delivery signal before sending SMS at the business deadline. Two release gates matter: approve content before enqueueing, then recheck suppression and notification state before fallback. This is practical for SaaS notifications with approximate escalation timing; a sub-minute real-time requirement needs a different event contract.

## How should email fallback and SMS status polling preserve seller templates?

Consider a proposed notification for order `media-order-2048`, seller `seller-73`, and template revision `seller-order-r7`. These are example identifiers, not a customer incident. The email can explain which media asset was ordered, while the SMS should carry the minimum useful order context; both should identify the same order and use the content revision approved for that notification. A seller shouldn't receive an initial message saying an order needs review and a fallback saying it has already been accepted merely because someone edited a shared template between polling cycles.

Freeze the meaning.

The content owner approves wording and variables; the platform owner controls the state machine, suppression policy, polling budget, and rollback switch. Treat those as separate release decisions even when one team does both jobs. Mustache supplies a documented template syntax, but adopting a syntax doesn't decide who owns a revision or how long it must remain renderable. Store the revision with the notification and validate its required order fields before any send.

This makes template ownership a useful buy-versus-build boundary. Managed delivery can carry the message, but a new-order alert still needs an application decision about whether the seller should hear from another channel. An accepted email request isn't evidence that the seller has seen the order.

## Watch for delayed evidence before escalating

Persist a per-notification state machine: queued, emailed, sms_fallback, then delivered or failed. Record the email message ID after sending, schedule a delayed worker, and use subsequent event reads to determine whether your documented delivery condition has been met. Store the SMS ID if fallback occurs. These are application states, not a claim that every provider returns those exact strings.

Watch for an aging emailed state with no acceptable delivery evidence.

There are two clocks: the business deadline and the age of the most recent successful poll. If polling has stopped, the absence of a delivery event is weak evidence; automatically turning that absence into SMS traffic can convert an observation problem into a burst of duplicate seller contacts. Keep stale observations visible. Consider the proposed order at its deadline: the content revision is approved, the email send has an ID, but the last successful event read predates a worker interruption. The correct next action is to refresh the evidence, while retaining the original order and channel identity, rather than manufacture a delivery failure from missing observations. That distinction must survive worker restarts and template releases.

For a planning example, choose a 5-minute fallback deadline and a 30-second polling interval. These are proposed settings, not measured service performance. With 600 active notifications, one individual read per notification per interval implies about 20 reads per second before retries; batching or event-list reads require a separate calculation using their actual retrieval semantics. A delayed worker also contributes queue lag, so a nominal deadline cannot become a promise of fallback at exactly 5 minutes.

Define the SLO around an observable application outcome, such as the proportion of eligible orders reaching a recorded channel outcome within the agreed window. Track poll age and worker lag alongside it. Delivery evidence doesn't prove the seller read the message. SMS isn't an acknowledgment mechanism.

## Implement the decision before wiring the transports

The following runnable Go program reads the status of an existing SMS message after fallback. A Node.js application can use the same read-and-record boundary in its delayed worker. Save it as `main.go` and run `go run main.go` with `INFRAI_API_KEY`, `INFRAI_API_BASE_URL` (the configured HTTPS API origin, without a path), and `SMS_MESSAGE_ID` set in the environment. It prints the response for inspection rather than guessing undocumented response fields or declaring delivery from HTTP success alone. It does not create a message.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    base := strings.TrimRight(os.Getenv("INFRAI_API_BASE_URL"), "/")
    id := os.Getenv("SMS_MESSAGE_ID")
    origin, err := url.Parse(base)
    if err != nil || origin.Scheme != "https" || origin.Host == "" ||
        origin.User != nil || origin.Path != "" || origin.RawQuery != "" ||
        origin.Fragment != "" || key == "" || id == "" {
        return fmt.Errorf("set a trusted HTTPS API origin, API key, and message ID")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            base+strings.Replace("/v1/sms/status/{id}", "{id}", url.PathEscape(id), 1), nil)
        if err != nil { return err }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil { return err }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if readErr != nil { return readErr }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            fmt.Println(string(body))
            return nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return fmt.Errorf("status %d: %s", resp.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
            if seconds > 45 { return fmt.Errorf("retry exceeds worker budget") }
            if d := time.Duration(seconds)*time.Second; d > delay { delay = d }
        } else if deadline, err := http.ParseTime(retry); err == nil {
            if d := time.Until(deadline); d > delay { delay = d }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

A fallback decision isn't a send authorization that can sit in memory indefinitely. Re-read current state, refresh suppression checks on both channels, and atomically claim the transition so two pollers cannot both initiate SMS. Commit a durable send intent with a stable per-notification channel identity, then execute it using the provider's verified idempotency contract. Keep that identity stable across retries. A random replacement key on each attempt defeats deduplication.

The difficult boundary is between sending and recording the result: a timeout can leave the application uncertain whether the provider accepted the request. Preserve that uncertainty, reconcile it using available status evidence, and retry only under a verified deduplication policy. Database locking alone cannot prevent a second external send after a process dies between acceptance and commit.

The example uses an explicit method, environment-based authentication, real response errors, and bounded exponential backoff that honors Retry-After. The read is safe to repeat; any sending adapter needs its own verified idempotency contract. Suppression checks must be refreshed before fallback, not treated as permanent permission to contact a recipient.

Budget retries separately.

## Buy delivery, but assign the content release work

This buy-versus-build table compares implementation candidates by the work this runbook would assign them. Its evaluation scopes are proposed boundaries, not assertions that the products have identical template, polling, or orchestration features. Verify those details in the referenced documentation before selecting an adapter.

| Candidate | Proposed buy boundary | Template ownership gate and trade-off |
| --- | --- | --- |
| SendGrid with Twilio | Evaluate SendGrid for email and Twilio for SMS | Require one application revision across two adapters; accept the integration work if separate channel control matters. |
| Amazon SES with Twilio | Evaluate SES for email and Twilio for SMS | Require the same revision and delivery-evidence checks; favor this candidate when its fit with the team's operating model justifies the adapter work. |
| Infrai | Evaluate email and SMS behind one contract spanning 295 routes across 20 modules under one key | Additional backend capabilities reuse the API surface, and public self-describing schemas help review adapters; the application still owns polling, revisions, and escalation. |
| Application-owned dispatcher | Build the revision ledger, worker, and state transitions around the chosen transports | Direct control of content release and rollback requires the team to carry reconciliation and on-call work. |

SendGrid, Twilio, and Amazon SES are alternatives to assess, rather than interchangeable labels in a feature-score table. Require evidence: can an adapter expose the delivery signal your SLO uses, can retries preserve send identity, and can the content owner reproduce the exact revision used for a disputed seller alert? Without those answers, a feature count is a poor reason to commit the roadmap.

For the unified-contract candidate, email events and SMS status use pull-based observation, so it fits this delayed worker pattern when approximate timing is acceptable. Require a different contract for sub-minute escalation. This design assumes API-based email and SMS delivery; SMTP relay, voice, WhatsApp, and RCS are outside its scope. Keep order alerts separate from OTP workflows, and retain unsent scheduled work in the application dispatcher so rollback doesn't depend on canceling an already scheduled provider message.

Don't infer domestic compliance from a vendor label. Establish regional and regulatory evidence independently, and put SMS geographic abuse controls and country-level spending circuit breakers in application policy. Neither a shared API key nor template approval supplies those controls.

## Verify the revision, then rehearse rollback

Before enabling fallback, exercise late email delivery, duplicate worker execution, suppression changes after email, stale polls, and a timeout after send acceptance. These are proposed verification cases. Check both the recorded transition and externally reconciled outcome; a passing state-machine check cannot establish delivery.

Start with fallback disabled and compare recorded decisions against the intended deadline and template revision. A content preview should include actual required order variables and a seller destination intended for verification. Treat sender requirements, including Yahoo's guidance, as a delivery prerequisite rather than treating successful rendering as permission to launch.

Rollback should stop new fallback claims first. Retain message IDs and continue reconciliation for attempted sends, because stopping the worker cannot retract a received message. If revision r7 has incorrect wording, approve a replacement for new notifications while retaining r7 and its audit record for existing ones; explicitly decide which unsent records may migrate. Don't replay completed orders to distribute a template fix.

The content owner should be able to identify the revision behind each seller contact, and the on-call engineer should be able to explain why a specific order escalated.

Keep both answers available.

## References

- Mustache template syntax manual: https://mustache.github.io/mustache.5.html
- Yahoo sender best practices and requirements: https://senders.yahooinc.com/best-practices/
- SendGrid documentation for adapter evaluation: https://www.twilio.com/docs/sendgrid
- Twilio messaging documentation for adapter evaluation: https://www.twilio.com/docs/messaging
- Amazon SES documentation for adapter evaluation: https://docs.aws.amazon.com/ses/
