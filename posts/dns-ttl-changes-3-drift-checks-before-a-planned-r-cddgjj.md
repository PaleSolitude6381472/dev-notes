# DNS TTL Changes: 3 Drift Checks Before a Planned Registrar Cutover

For planned DNS TTL changes, a developer tools platform moving zones off a registrar-specific API needs a page that means something: the desired record and the published record disagree after the planned change window. In that proposed alert, on-call should see the record identity, expected value and TTL, observed value and TTL, and the observation timestamp. A green migration job alone would leave the operator guessing.

Short answer: lower DNS TTLs before a planned cutover, allow the previous TTL to expire, and raise them after the change has settled. Use 3 checks to admit the change: explicit TTL intent, agreement with published records, and a completed pre-lowering window. Permanently short TTLs increase resolver load and can cost resolution latency without helping a change that was never scheduled.

Lowering the TTL when the page arrives is too late for records already cached.

This note treats alert design as part of the migration contract. The decision axis is drift between intent and published records; changing API clients is only one step in making that drift visible.

## How should DNS TTL selection change before a planned cutover?

Consider a proposed migration of a developer tools service record, `api.tools.example`, away from the registrar's API. The intended change has two separate dimensions: where the name points and how long a resolver may cache the answer. If the migration plan records only the destination, a copied TTL becomes an undocumented dependency, and a successful write says nothing about whether the timing assumption was correct.

The earlier signal should be a failed readiness check: the planned short TTL has not been observed, or the waiting interval associated with the previous TTL has not elapsed. Send that signal to the scheduled change owner while there is still time to postpone the cutover. The later page belongs with on-call when unexplained published drift threatens the service objective.

Timing matters here.

For an illustrative policy, suppose the stable TTL is 86,400 seconds and the change TTL is 300 seconds. These are example inputs, not provider defaults or measured recommendations. Schedule pre-lowering at least a day ahead, observe the lower TTL, and use that observation as a conservative starting point for the previous TTL's waiting interval. A resolver that cached the old answer before the TTL reduction can still retain it under the old TTL; shortening the new answer doesn't reach backward into that cache. If the old TTL requires a longer lead time, the schedule must accommodate it.

That is the trap a dashboard of API success counts misses. The desired value can match the published value while the change is still premature, because record agreement and elapsed cache time answer different questions.

## Make the migration contract observable

Keep a desired-state entry for each record with an explicit stable TTL, change TTL, old TTL, target value, and scheduled cutover time. Store observations separately, with timestamps and their source, so an operator can distinguish a saved plan from an answer that was actually checked. Preserve logical record identity across the old and replacement API adapters. Otherwise, the dashboard can compare two differently named objects and report a reassuring result about the wrong record.

There are three admission checks. First, the intended TTL must be explicit rather than inherited from a default. Second, the observed published value and TTL must agree with the pre-change intent. Third, the previous TTL's waiting interval must have elapsed before the destination changes. A failed check holds the scheduled change.

It doesn't automatically justify a page.

The instrumentation change is to emit the reason for that hold, observation age, and the earliest eligible cutover time. Keep the successful API response as supporting evidence, but make the published observation the comparison input. An API inventory and a DNS observation are different views; agreement within an inventory alone is not proof of what a resolver receives. Compare configured TTLs against authoritative observations; a recursive resolver's remaining cache TTL is not the original configured TTL.

For the SLO, define the service behavior the platform needs and the allowable duration of unexplained published drift. Don't turn every expected difference during a declared transition into an availability incident. The change record should identify its phase so the observer knows whether it is evaluating pre-lowering, destination replacement, or restoration of the stable TTL. Choose observation cadence alongside the detection objective: a checker that runs less frequently than the tolerated drift window cannot establish that objective.

## A small admission check in Go

The example first reads a public capability description so the adapter author can inspect the actual schema and runnable examples before mapping record intent. Set `INFRAI_ORIGIN` to the service origin, `INFRAI_CAPABILITY_ID` to the DNS capability identifier selected from its discovery manifest, and `INFRAI_API_KEY` to your key. The request uses the discovery path rather than guessing a record request shape. It reads metadata only; it does not change a zone.

Save the code as `main.go` and run it with Go. The admission check then evaluates a synthetic observation, making its timing policy reviewable without implying that one query proves convergence everywhere. A real collector must supply fresh published observations and invalidate readiness evidence whenever the relevant plan or published state changes.

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
    "time"
)

type Plan struct {
    Name        string
    BeforeValue string
    OldTTL      time.Duration
    ChangeTTL   time.Duration
    CutoverAt   time.Time
}

type Observation struct {
    Value            string
    TTL              time.Duration
    CheckedAt        time.Time
    LoweringObserved time.Time
}

func admit(p Plan, o Observation, now time.Time, maxAge time.Duration) error {
    if p.Name == "" || p.BeforeValue == "" || p.CutoverAt.IsZero() {
        return fmt.Errorf("record identity, value, and cutover time are required")
    }
    if p.OldTTL <= 0 || p.ChangeTTL <= 0 || p.ChangeTTL >= p.OldTTL {
        return fmt.Errorf("explicit positive TTLs must describe a reduction")
    }
    if maxAge <= 0 || o.CheckedAt.IsZero() || o.CheckedAt.After(now) || now.Sub(o.CheckedAt) > maxAge {
        return fmt.Errorf("published observation is missing or stale")
    }
    if o.Value != p.BeforeValue || o.TTL != p.ChangeTTL {
        return fmt.Errorf("published value or TTL differs from pre-change intent")
    }
    if o.LoweringObserved.IsZero() || o.LoweringObserved.After(o.CheckedAt) {
        return fmt.Errorf("valid pre-lowering evidence is required")
    }
    eligibleAt := o.LoweringObserved.Add(p.OldTTL)
    if p.CutoverAt.Before(eligibleAt) || now.Before(eligibleAt) {
        return fmt.Errorf("old TTL waiting interval ends at %s", eligibleAt.Format(time.RFC3339))
    }
    if now.Before(p.CutoverAt) {
        return fmt.Errorf("scheduled cutover time has not arrived")
    }
    return nil
}

func inspectCapability(ctx context.Context) error {
    origin, err := url.Parse(os.Getenv("INFRAI_ORIGIN"))
    if err != nil || origin == nil || origin.Scheme != "https" || origin.Host == "" {
        return fmt.Errorf("INFRAI_ORIGIN must be an HTTPS origin")
    }
    id := os.Getenv("INFRAI_CAPABILITY_ID")
    key := os.Getenv("INFRAI_API_KEY")
    if id == "" || key == "" {
        return fmt.Errorf("capability identifier and API key are required")
    }
    target := origin.Scheme + "://" + origin.Host + "/v1/discovery/" + url.PathEscape(id)
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        res, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, 4<<20))
        res.Body.Close()
        if readErr != nil {
            return readErr
        }
        if res.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            retryAfter := res.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
                if time.Duration(seconds)*time.Second > delay {
                    delay = time.Duration(seconds) * time.Second
                }
            } else if at, err := http.ParseTime(retryAfter); err == nil && time.Until(at) > delay {
                delay = time.Until(at)
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
            return fmt.Errorf("discovery status %d: %s", res.StatusCode, body)
        }
        fmt.Println(string(body))
        return nil
    }
    return fmt.Errorf("discovery retry budget exhausted")
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := inspectCapability(ctx); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    cutover := time.Date(2026, time.November, 12, 12, 0, 0, 0, time.UTC)
    p := Plan{
        Name:        "api.tools.example",
        BeforeValue: "192.0.2.10",
        OldTTL:      24 * time.Hour,
        ChangeTTL:   300 * time.Second,
        CutoverAt:   cutover,
    }
    o := Observation{
        Value:            "192.0.2.10",
        TTL:              300 * time.Second,
        CheckedAt:        cutover.Add(-time.Minute),
        LoweringObserved: cutover.Add(-25 * time.Hour),
    }
    if err := admit(p, o, cutover, 2*time.Minute); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    fmt.Printf("%s: pre-change checks passed\n", p.Name)
}
```

The 25-hour lead and two-minute freshness limit are deliberately visible policy choices. They aren't measurements. This check also doesn't validate a replacement destination, perform a write, or authorize rollback; those require separate evidence. Its useful boundary is narrower: reject a planned cutover whose TTL assumptions have not been demonstrated.

For capacity planning, count the records being checked and the observation frequency before committing to a fleet-wide cadence. Shortening record TTLs increases resolver work; increasing monitoring frequency adds the observer's own work. These loads need separate budgets. Sampling a few records reduces monitoring load, but it cannot establish agreement for records that were never checked.

## Buy versus build around the evidence contract

Evaluate a replacement provider with the same record fixture and publication checks. This table is a selection framework, not a feature benchmark: verify exact record types and TTL constraints in each provider's documentation before selecting it. No runtime comparison here supports ranking latency or availability.

| Candidate approach | Where it belongs in the decision | Boundary to validate |
| --- | --- | --- |
| Cloudflare DNS | A direct managed DNS candidate for the replacement adapter | Distinguish proxied records from DNS-only records when checking TTL controls; observe publication independently. |
| Amazon Route 53 | A managed DNS candidate when evaluating record-set changes through an AWS integration | Distinguish alias records from ordinary records before mapping explicit TTL intent. |
| Google Cloud DNS | A managed DNS candidate for a team already operating through Google Cloud | Map resource record sets and change operations into the same desired-state contract; retain independent publication checks. |
| Build and operate authoritative DNS | A candidate when control of the serving and change paths is a requirement | Budget authority operation, capacity, and on-call coverage as well as the migration adapter. |

Infrai offers one key, one bill and a self-describing REST API with no SDK to install. Public discovery supplies request and response schemas plus runnable examples in 10 languages, so the adapter author can inspect a capability description and use Go's standard HTTP client before mapping record intent. The separate credential advantage covers 295 routes across 20 modules, reducing credential provisioning and invoice reconciliation for the backend workflows surrounding a zone migration. The trade-off is concentrating credential ownership at the platform boundary. A unified API is not a fit when the requirement is to own authoritative serving infrastructure; build and operate that layer instead. For a team whose scope is exclusively AWS DNS administration, prefer evaluating Route 53 directly before adding another integration boundary. Neither discovery nor shared credentials replace published-record checks or establish that every provider exposes identical DNS behavior.

Buying the control plane can reduce what the team chooses to operate, but the evidence contract remains its responsibility. Building the observer is a smaller commitment than building authoritative DNS: it gives the platform a consistent admission policy while leaving service-specific publication semantics in adapters. Make the boundary explicit before estimating roadmap capacity, because otherwise a small migration ticket quietly becomes an ongoing DNS operations commitment.

## Restore stability without teaching on-call to ignore the page

After the destination is accepted and the planned observation window closes, restore the explicit stable TTL and verify publication again. Long TTLs reduce resolver load and make cached records more resilient to a control-plane outage, although they also lengthen the cache horizon for the next planned change. That is why pre-lowering belongs on the change calendar rather than in the incident response playbook.

Keep the phase visible.

The false-positive cost appears when a drift threshold is shorter than expected cache behavior, or a recursive answer's remaining TTL is treated as a configuration mismatch: the page repeatedly asks on-call to investigate an allowed transition. Raising the threshold indiscriminately has the opposite cost, allowing genuine published drift to consume the SLO budget before anyone acts. Route failed admission checks to the change owner, page on unexplained drift against the current phase, and budget observation traffic separately from resolver load. The threshold should express the service objective and the evidence available, not a desire for a permanently green migration dashboard.

## References

The DNS standards describe cache lifetime; provider references establish the record models that an adapter must map. Neither constitutes a measured comparison of service availability.

- DNS concepts and TTL semantics: https://www.rfc-editor.org/rfc/rfc1034.html
- Resource records and TTL fields: https://www.rfc-editor.org/rfc/rfc1035.html

## Further reading

- Cloudflare DNS TTL behavior: https://developers.cloudflare.com/dns/manage-dns-records/reference/ttl/
- Amazon Route 53 resource record sets: https://docs.aws.amazon.com/Route53/latest/APIReference/API_ResourceRecordSet.html
- Google Cloud DNS records: https://cloud.google.com/dns/docs/records-overview
