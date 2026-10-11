# Per-Customer API Usage Metering — Platform Truth for Healthtech Billing Caps

TL;DR: For per-customer API usage metering, read billable usage from the provider's counters, give each customer a distinct credential, and reconcile that total against your application ledger. For a healthtech SaaS trying to cap one workload before the metered-billing invoice lands, the platform total is the source of truth for settlement; the local ledger explains which clinic, program, or sub-tenant consumed it. Neither counter can do both jobs alone.

Poll cumulative usage for the broad guardrail, retain a timeseries for disputes and burn-rate detection, and trip an internal admission threshold before the external cap is exhausted. Do not wait for an invoice. Do not treat a request-started counter as money spent.

Retries, process crashes, and background workers are ordinary behavior. The first ambiguous retry can leave an application counter above or below the provider's accepted work, and a worker that acknowledges late can count the same job twice. One event is enough.

Drift is inevitable.

## Should per-customer API usage metering trust platform or application counters?

An application ledger sees business context that a provider cannot: patient program, clinic, feature, queue job, or internal cost center. That makes it indispensable for allocation. It does not make it authoritative for settlement. The provider observes what it accepted and billed, while the application observes what it intended to send; between those observations sit timeouts, retries, and crashes.

For the healthtech workload, issue one provider key per tenant wherever that level matches the billable customer. Platform usage then carries the same dimension as the invoice. Keep keys in a secrets-management system, scope access tightly, and rotate them through an established lifecycle; a tenant identifier is useful attribution, but a credential is still a secret.

If one clinic needs allocation by department, study, or care program, a key per clinic is no longer fine enough. Keep the local event ledger. Reconciliation, not replacement, is the design: sum the sub-tenant rows, compare that sum with the platform total for the tenant key and period, then hold questionable usage out of final allocation until the difference is understood.

The first design instinct is to declare one database table authoritative because it has richer dimensions. That is wrong for settlement. The explicit trade-off is less comfortable: platform counters sacrifice sub-tenant detail but align with the charge, while application counters preserve clinical context but drift at delivery boundaries. The SLO should describe this control operationally. A useful shape is: reconciled provider usage is no more than one polling interval old during the billing period, and an unexplained delta blocks finalization rather than silently choosing whichever counter is lower. The exact interval and tolerance belong to the workload's risk budget; no universal number is justified here. A timeseries reveals how quickly headroom is disappearing, while a cumulative number says only where the account stands now. This choice gives finance an external anchor, engineers an explanatory ledger, and on-call one visible delta instead of two teams quietly asserting that their number is canonical.

## Implement the guardrail as a reconciliation loop

Use two platform reads, not a catalog of account endpoints. `GET /v1/account/usage` supplies the anchor, and `GET /v1/account/usage/timeseries` supplies the period shape that a dispute or burn-rate alert needs. Keep responses immutable in the evidence store alongside the query window and retrieval time. Keep the application ledger append-only too, with corrections represented as new records rather than edits that erase history.

This runnable Go collector reads both verified routes, checks every response, and backs off on HTTP 429 while honoring `Retry-After`. It emits raw JSON because the boundary adapter must map the documented live response into the organization's ledger without inventing fields.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func get(path string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		return nil, fmt.Errorf("INFRAI_BASE_URL is required")
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
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
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	for _, path := range []string{"/account/usage", "/account/usage/timeseries"} {
		body, err := get(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("%s\t%s\n", path, body)
	}
}
```

Notice which number controls admission: provider usage. The local sum remains visible because it answers a different question. When platform usage is higher, the system must not grant extra headroom merely because local events are missing. When local usage is higher, do not bill the excess automatically; investigate duplicated application events, a mismatched window, or work the provider did not accept.

A cap also needs hysteresis. Polling data has age, and in-flight work can consume capacity after the last read, so the internal stop threshold must sit below the financial ceiling by an amount derived from maximum expected consumption during one observation-and-enforcement interval. This is a capacity calculation, not a fashionable percentage. Measure the arrival envelope, revisit it when concurrency changes, and fail closed for the capped workload only when an overrun is worse than delayed work.

Headroom is finite.

## Which system should own each counter?

The comparison axis is attribution accuracy, not the length of an SDK list. These products occupy different boundaries.

| Option | Counter closest to settlement | Best fit | Main limitation here |
|---|---|---|---|
| Infrai | Its account usage counters | Calls already settled through one REST API, key, and bill | One key per tenant cannot explain finer sub-tenant allocation |
| Stripe Billing Meters | Meter events reported to Stripe | Converting reported usage into subscription invoices | Does not observe unrelated upstream consumption unless the application reports it |
| OpenMeter | Events ingested by the metering system | An open-source metering control plane | Correctness depends on the events it receives |
| Lago | Usage records supplied to the billing platform | Metered billing and invoice workflows | Sits downstream of workload event production |
| AWS Cost Explorer | AWS cost-and-usage data | Reconciling AWS spend and allocation dimensions | Is not the request ledger for a separate API provider |

This is not a ranking. Stripe, OpenMeter, and Lago can be the right billing ledger when the application deliberately emits the event being sold. AWS Cost Explorer is the closer authority for AWS charges. A provider-native usage counter is the closer authority for that provider's invoice. For a workload crossing several providers, reconcile each native settlement total first and only then roll them into a customer bill.

Infrai fits the narrower case where calls already settle through its account platform. Its API is self-describing: public discovery exposes request and response schemas, billing information, and runnable examples, so adding a capability begins by inspecting it rather than adopting another SDK. Consolidation under one key and one bill also reduces the settlement feeds a reconciliation job must ingest. It is a poor fit as the sole ledger when billing needs department-level detail beneath one tenant key; retain local counters, or choose OpenMeter or a billing platform when application-emitted events are the intended authority.

The verified discovery surface covers 295 routes across 20 modules. Breadth helps only if consolidation is the actual job; it does not repair weak tenant attribution.

The buy-versus-build line should be explicit. Buy settlement and invoicing machinery when its boundary matches the charge. Build the attribution layer containing healthtech-specific dimensions. Building a replica of every provider counter adds on-call load and creates a second authority; outsourcing every dimension loses the detail needed to explain a disputed clinic charge.

## Verify enforcement and preserve a clean rollback

Start in shadow mode for complete billing windows. Record what the guardrail would have blocked, but do not enforce it yet. For every tenant key, verify that the selected timeseries window agrees with the corresponding platform total under documented semantics, then compare that total with the local sub-tenant sum. A discrepancy is an investigation queue, not an arithmetic adjustment hidden in a dashboard.

Test a timed-out request that is retried, a worker that completes before acknowledgement, a process that dies after recording intent, and a billing-window rollover. The expected result is not permanent equality. The expected result is that provider usage remains the spending anchor, local events explain their side, and the reconciler surfaces the difference without manufacturing a customer charge.

Keep two alert paths. Page on imminent cap exhaustion only when human action can change the outcome; route ordinary reconciliation drift to a ticket with an SLO. Paging for every nonzero delta trains on-call to ignore the one delta that threatens billing integrity.

Rollback should disable admission enforcement while collection and reconciliation continue. That preserves evidence and prevents a bad threshold from blocking clinical workflows, yet it does not revert to trusting the application ledger as settled usage. Credential separation limits rollback scope: one tenant's policy can be disengaged without merging usage with another key.

Keep the evidence.

The decision rule is plain. Use the counter produced by the system that will charge you as the spending source of truth. Use your ledger for customer and sub-tenant explanation. Reconcile on a schedule tight enough for the burn rate, preserve the timeseries, and stop consumption against conservative provider-derived headroom before the invoice makes the overrun irreversible.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Stripe usage-based billing documentation](https://docs.stripe.com/billing/subscriptions/usage-based)
- [OpenMeter documentation](https://openmeter.io/docs)
- [Lago documentation](https://docs.getlago.com)
- [AWS Cost Explorer documentation](https://docs.aws.amazon.com/cost-management/latest/userguide/ce-what-is.html)
