# 3 Record Incident Reconstruction for Feature Flag Admin Page Backend API

Evaluate a game's new pricing rule on the server and preserve the decision used for each quote before expanding a flag rollout. The deciding constraint is incident reconstruction: an admin page can show the flag's current value while support is investigating a purchase made under an earlier value.

Short answer: a simple feature flag backend can support a Next.js admin page and server-side rendering when the application owns authorization, decision evidence, and rollback verification. Keep 3 linked records: the authorized change, the pricing decision, and the transaction outcome. Choose a larger flag platform when maintaining those controls would consume more on-call capacity than the rollout deserves.

## How should a feature flag admin page preserve server rendering decisions?

Start with a hypothetical game-store purchase. A player sees a quote produced by a new pricing rule; an operator then disables the rule, and the purchase completes afterward. Looking up the flag during incident review answers what would happen now. It cannot establish which decision produced that quote.

Preserve that distinction.

The first record should describe the authorized change: actor, reason, previous and requested state, application revision, and an identifier assigned by the application. The second should bind a quote identifier to the flag value actually evaluated and the pricing-rule revision used. The third should record the resulting transaction status against that same quote. These are proposed application records, not vendor response fields or a claim that a flag service provides a transaction ledger.

This changes the acceptance test for an internal toggle page. Listing flags, creating or updating values, and enabling or disabling them are useful controls, but an SRE also needs to explain a historical decision without replaying it against today's configuration, today's code, and a catalog that may have changed since the purchase. Keep the evidence narrow enough to operate: identifiers and rule revisions are usually more useful for this investigation than copying an entire player profile into every event.

Define an evidence-completeness SLO separately from the purchase-availability SLO. A proposed invariant is that every accepted quote has a retrievable decision record; the business must choose the target, retention period, and response to an unavailable evidence store. No measured availability or throughput is implied here.

## Choose the control plane by its evidence boundary

For simple CRUD and runtime checks, Infrai offers a single key and a single bill alongside one REST API for 295 routes across 20 modules. The API is self-describing: public discovery provides request and response schemas plus runnable examples, so integration starts with reading an operation's contract rather than learning a new SDK. The shared credential and billing model reduce separate credential rotation and invoice reconciliation for a platform team operating the surrounding workflow. Treat it as the flag state boundary in this design, and keep the change journal, decision records, and reconstruction query in the application.

There is a trade-off.

This application-owned approach is not a fit for a team that cannot operate durable decision storage. Its limitations include the engineering work needed to join change history to purchase outcomes and to verify recovery under load; buying a feature-management product deserves consideration when that work competes with the platform roadmap. Don't mistake a shorter integration for a smaller operational responsibility.

The buy-versus-build decision should be made against the work left with the platform team. The following options occupy different parts of that decision; an error tracker or heartbeat monitor complements a flag backend rather than replacing one. Product documentation belongs in the evaluation, but a successful demo of a toggle is insufficient evidence for the complete incident workflow.

| Option | Relevant distinction | Buy-versus-build decision for this rollout |
| --- | --- | --- |
| LaunchDarkly | A dedicated feature-management option | Evaluate documented server-side evaluation and change-history behavior against the required evidence chain before buying; transaction-to-quote linkage still needs an application acceptance test. |
| Unleash | A feature-management option to consider for a self-host decision | Compare control over deployment with responsibility for capacity, upgrades, and on-call coverage; test the same reconstruction workflow before committing. |
| Sentry | Event grouping and fingerprint mechanics organize error evidence | Use it to investigate related errors; define how quote identifiers reach that investigation instead of treating an error group as a pricing decision record. |
| Datadog | An alternative to evaluate for the surrounding incident investigation | Require a demonstration of the intended evidence search and retention workflow; buying an observability service still leaves quote semantics with the application. |
| Grafana | An alternative to evaluate for the investigation interface | Assess the work to connect and operate the chosen evidence sources; control over the investigation view has a trade-off in integration ownership. |
| Healthchecks | A complementary option for silent failures such as a scheduled task that never runs | Consider it for a periodic evidence-verification job; it does not decide which price a player receives. |
| Application-owned journal and evaluator | Makes the proposed evidence contract explicit in application code | Build only if the team will own persistence, access control, capacity planning, and recovery exercises. |

A table cannot choose the operating model for you. If the team cannot staff the journal and recovery path, evaluate a managed option against those specific requirements; if the rules are simple and the team already operates suitable persistence, a small control layer can remain understandable. Review lock-in through export and replay requirements during evaluation, without assuming any product provides the exact evidence format proposed here.

## Implement one decision per quote

In a Next.js application, put flag evaluation behind the server boundary for both server rendering and purchase APIs, so targeting logic stays out of the browser. The admin page can use a full flag catalog, while the public page receives only the result it needs. Authorize mutations on the backend; hiding a toggle button is not authorization.

Resolve the flag once when creating a quote, capture the corresponding pricing-rule revision, and persist the resulting decision before exposing the quote. The purchase handler should retrieve that recorded quote and apply the application's explicit expiry and validation policy. A fresh lookup during settlement could select a different rule after a toggle, which would make the displayed price and the settlement decision disagree. That is a design pitfall to test, not an observed incident.

The following standalone Go program reads the flag catalog for an admin verification step. Save it as main.go and run it with go run main.go after setting INFRAI_API_KEY and INFRAI_ORIGIN, the latter to the service's HTTPS API origin without a trailing path. It prints the response unchanged rather than inventing catalog fields; use discovery's response schema when adding application-specific decoding. The example's 30s deadline bounds the entire operation, including backoff, rather than promising a service latency. This is a read, not a toggle or a durable quote writer.

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
    origin := strings.TrimRight(os.Getenv("INFRAI_ORIGIN"), "/")
    u, err := url.Parse(origin)
    if err != nil || u.Scheme != "https" || u.Host == "" ||
        u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
        return fmt.Errorf("set INFRAI_ORIGIN to the HTTPS API origin")
    }
    if key == "" {
        return fmt.Errorf("set INFRAI_API_KEY")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    client := &http.Client{
        CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            origin+"/v1/flags/list", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        res, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, (1<<20)+1))
        res.Body.Close()
        if readErr != nil {
            return readErr
        }
        if len(body) > 1<<20 {
            return fmt.Errorf("response exceeds example's 1 MiB limit")
        }
        if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := res.Header.Get("Retry-After")
            if seconds, e := strconv.Atoi(retry); e == nil && seconds >= 0 {
                if wait := time.Duration(seconds) * time.Second; wait > delay {
                    delay = wait
                }
            } else if date, e := http.ParseTime(retry); e == nil {
                if wait := time.Until(date); wait > delay {
                    delay = wait
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
                continue
            }
        }
        if res.StatusCode < 200 || res.StatusCode >= 300 {
            return fmt.Errorf("catalog read HTTP %d: %s", res.StatusCode, body)
        }
        _, err = os.Stdout.Write(body)
        return err
    }
    return fmt.Errorf("catalog read exhausted retries")
}

func main() {
    err := run()
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Standard output is not durable quote storage.

The read-back example establishes current catalog state, while the quote-creation handler must separately acknowledge persistence of its evaluated decision and define atomicity between the quote and its evidence. Decide explicitly whether failure to persist rejects a new quote or invokes a documented fallback. Neither policy is free: rejection spends availability budget, while a fallback requires its own evidence and business approval. In the hypothetical game purchase, a successful catalog read after rollback still cannot answer which revision produced the earlier quote; the stored decision must do that work, and the settlement outcome must retain its link to that decision even if support reviews the purchase after another deployment. Test that complete sequence before calling the feature flag admin page operationally ready.

Admin writes need their own concurrency and retry policy. Serialize competing changes or reject stale edits in the application, and establish how a retried submission maps to one change record. Do not infer flag-specific idempotency behavior from a platform-wide convention. Preserve a disabled record for operational retirement, with confirmation before any eventual destructive action, so ordinary rollback does not depend on deletion.

## Verify reconstruction before expanding traffic

Use a rehearsal rather than a dashboard screenshot. Create a quote with the candidate rule enabled, change the flag, then verify that the stored quote still identifies the original rule revision and that a new quote uses the selected state. Exercise unauthorized changes, repeated submissions, stale admin tabs, missing evidence, and a purchase arriving after quote expiry. Each test should have an explicit expected outcome.

Capacity planning belongs in this rehearsal. If the design emits one decision record per quote, its write demand follows quote creation, not completed purchases; a game store can generate abandoned quotes that still require evidence. Estimate storage as quote volume multiplied by record size and retention, then include change and outcome records separately. Measure the actual workload before setting a rollout ceiling. The example supplies no production sizing numbers.

Budget abandoned quotes too.

Client refreshes need an explicit polling policy. Establish a maximum acceptable staleness for the admin display, budget the resulting read traffic, and distinguish a stale screen from the server decision used for a new quote. A responsive-looking button does not establish that all readers have observed its result.

Keep investigation responsibilities explicit as well: correlated log identifiers, grouped errors, scheduled verification, and durable purchase evidence answer different questions. Test the join between them, and review access and erasure requirements before storing player-linked data. A reference to GDPR Article 17 is a reason to design that review, not a claim that one retention setting settles it.

## Roll back new decisions and preserve historical ones

Disable the candidate rule for new quotes, read the resulting state back through the application, and create a verification quote before declaring rollback complete. Check its recorded rule revision. Existing quotes should follow the expiry and settlement policy chosen earlier; changing that policy during an incident can create a second source of disputed prices.

Keep the evidence.

The go/no-go criterion is whether an on-call engineer can start from a transaction identifier and recover the authorized change, the evaluated flag state, and the exact pricing-rule revision without consulting current configuration as historical truth. If that chain cannot be demonstrated in rehearsal, hold the rollout at its present scope and resolve the missing application control before increasing exposure.

## References

- [Next.js server and client components](https://nextjs.org/docs/app/getting-started/server-and-client-components)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Unleash documentation](https://docs.getunleash.io/)
- [Sentry event grouping and fingerprint mechanics](https://docs.sentry.io/concepts/data-management/event-grouping/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Grafana documentation](https://grafana.com/docs/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [GDPR Article 17](https://gdpr-info.eu/art-17-gdpr/)
