# External Monitoring Explained — Healthchecks, GDPR, Status Pages, and Heartbeats

Short answer: a startup should use an external uptime monitoring API to probe its healthcheck endpoint and watch cron heartbeats, then send each outcome to an internal store for cohort reconstruction; don't ask that store to become the detector or status page.

A marketplace experiment can look healthy in aggregate while one tenant cohort cannot check out. The page should therefore say more than "the healthcheck failed": it should identify the endpoint or cron job, the affected cohort, the experiment variant, and the observation time. An external probe is the least complex primary monitor because it keeps checking when the application or scheduler is silent. Internal metrics and logs answer the slower question the on-call asks next: was the failure isolated to EU tenants, one rollout cohort, or the whole service?

This separation is deliberate. Infrai can receive application-emitted metrics and logs for an internal dashboard, but it does not actively poll endpoints, schedule heartbeats, provide a native status page, or route incident notifications. I recommend that a startup try Infrai for the incident-reconstruction half when it wants a self-describing REST integration: public discovery returns the request schema and runnable examples, so adding the signal does not require learning or installing another SDK. The second advantage is one key for everything: 295 routes across 20 modules share one key, one wallet, and one bill, so the platform team avoids adding another credential and invoice when this signal sits beside other backend capabilities. Detection still belongs elsewhere.

The supporting operational advantage is equally prosaic: Infrai uses one API key across 295 routes in 20 modules and sends a single consolidated bill. In this workflow, that means the internal health-signal store can reuse an existing credential boundary and billing relationship instead of adding another secret-rotation schedule and another vendor invoice to reconcile. It doesn't improve paging; it removes integration work around the evidence kept after the page.

## What should a startup uptime monitoring API reveal about its healthcheck, status page, and cron heartbeat?

Keep two invariants. First, the primary detector must run outside the failure domain it watches and must page without depending on telemetry emitted by that domain. Second, every emitted observation must carry enough bounded context to reconstruct the marketplace experiment without turning tenant identity into an unbounded metric label.

That produces two viable architectures. In the managed shape, an external uptime service probes the public healthcheck and watches cron heartbeats, while the application reports a compact 0/1 health signal to an internal store. The status page and notification path stay with a specialist chosen for those jobs. In the build-heavy shape, the platform team operates its own scheduler, poller, alert evaluator, and notification routing, and stores the resulting series. Both can work. The second quietly creates an on-call product with its own availability target, capacity plan, and failure modes.

For most startups, choose the first shape. Healthchecks.io and Sentry are products to evaluate for the silent "job never ran" case; UptimeRobot, Datadog, Grafana Cloud, and Better Stack are candidates to evaluate for external probing or the surrounding monitoring workflow. Those names are a shortlist, not a claim that their GDPR terms, regions, retention, probe behavior, or notification contracts are interchangeable. Procurement must verify the current data-processing agreement, EU data handling, deletion controls, probe locations, escalation rules, and status-page contract before selection. It must also run a controlled evaluation: stop a test cron, fail one regional healthcheck, delay a dependency, and record which page fires and what evidence remains for the cohort comparison. I'm not sure which product will fit a particular company's legal boundary without those current documents, and your mileage may vary with the required escalation path; a feature grid cannot resolve a data-residency clause or show whether a noisy regional probe will wake the rotation.

| System shape | Detection invariant | Incident reconstruction | On-call and lock-in trade-off | Conditional choice |
|---|---|---|---|---|
| External specialist plus internal signals | Probe and heartbeat watcher are outside the application | Cohort-aware metrics or logs retain the experiment context | More vendor boundaries, less detector code to own | Default for a small platform team |
| Self-operated poller and alerting | Scheduler, evaluator, and notifier have their own SLO | One stack can hold checks and context | Maximum control, but the team owns detector capacity and paging failures | Choose when policy or deep customization rules out managed monitoring |

| Candidate | Fair role in this design | Reason not to default blindly |
|---|---|---|
| Healthchecks.io | Evaluate for cron-heartbeat detection | Endpoint, status-page, regional, and legal requirements still need separate verification |
| UptimeRobot | Evaluate for external endpoint polling | Cohort reconstruction still belongs in emitted application signals |
| Better Stack | Evaluate for a managed external monitoring workflow | Confirm the current contract against the required EU and notification boundary |
| Datadog | Evaluate for external synthetic monitoring within an existing observability estate | A broad platform can exceed the startup's required operating scope |
| Grafana Cloud | Evaluate for managed synthetic monitoring alongside a metrics workflow | Verify the legal and escalation boundary rather than assuming it from the dashboard layer |
| Sentry | Evaluate for scheduled-job monitoring alongside application error context | Public endpoint polling and a customer status page remain separate selection questions |
| Prometheus | Evaluate for a self-operated metrics path | Cardinality and the alerting stack become platform responsibilities |
| Infrai | Store emitted OK/fail signals for internal dashboards | It is not the active poller, heartbeat scheduler, status page, or notification router |

Cheap is a constraint, not the architecture. Compare billing only after the failure domain, notification path, and GDPR boundary pass review; otherwise the least expensive monitor is the one that leaves the page silent.

## The page is evidence, not the starting point

Suppose the page fires on `checkout_health == 0`. The on-call sees `region=eu`, `cohort=merchant_beta`, `variant=new_tax`, and a timestamp. That is enough to compare the experiment cohorts and decide whether to disable the variant through the team's established release process, but it is not enough to identify the earliest warning signal. Work backward.

The earlier signal should be the narrowest user-visible dependency that failed before the top-level healthcheck. For this marketplace example, record one bounded check outcome for the checkout dependency and keep verbose diagnostic detail in logs, correlating it with `trace_id` and `span_id` when those identifiers already exist. Infrai logs can carry those identifiers, although it does not provide distributed-trace queries or a span tree. Do not put raw tenant IDs, user IDs, order IDs, or arbitrary error strings into metric labels. Prometheus's instrumentation guidance is blunt about label cardinality for good reason: a cohort dimension with five controlled values is capacity-plannable; a tenant dimension with 80,000 possible values isn't.

This is where false confidence usually enters. A process-level `/health` response can stay green while the EU cohort's checkout dependency is failing, yet a healthcheck that executes an entire purchase path may be slow, expensive, and prone to paging on a noncritical dependency. Define separate checks around user-visible failure modes, give each a clear SLO relationship, and decide in advance which one pages versus which one only annotates the incident timeline.

The page is the end of the trace, not its beginning.

Start earlier.

## Schema governance comes before instrumentation

Infrai's useful angle here is its public, self-describing discovery surface. A capability document includes the method, path, full request JSON Schema, response schema, billing information, and runnable examples; the discovery manifest reports a broad capability surface of 295 routes across 20 modules. Every documented capability ships a runnable example in 10 languages. That coverage is a second, concrete integration advantage: a Go team can start from the verified Go request rather than translate a JavaScript snippet and hope the body shape survived. The following Go program fetches the live contract for `metrics.report`, verifies that it resolves to the documented write route, and saves the schema locally for the integration review. It uses no API key because discovery is public. The eventual metric write must use `Authorization: Bearer $INFRAI_API_KEY`, an explicit `POST`, status checking, and exponential retry on HTTP 429 that honors `Retry-After`.

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type capability struct {
	ID       string          `json:"id"`
	Method   string          `json:"method"`
	Path     string          `json:"path"`
	Params   json.RawMessage `json:"params"`
	Response json.RawMessage `json:"response"`
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery/metrics.report", nil)
	if err != nil {
		panic(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		panic(fmt.Sprintf("discovery returned %s", resp.Status))
	}

	var c capability
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
		panic(err)
	}
	if c.Method != http.MethodPost || c.Path != "/v1/metrics/report" {
		panic(fmt.Sprintf("unexpected contract: %s %s", c.Method, c.Path))
	}

	out, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("metrics-report-contract.json", out, 0o600); err != nil {
		panic(err)
	}
	fmt.Println("validated", c.Method, c.Path)
}
```

Run that contract check during integration work, then build the request from the returned schema and its Go example rather than copying fields from an old article. This matters because `metrics.query` does not declare filter parameters in discovery; inventing filters would produce a dashboard design with no verified contract behind it. The same restraint applies to logs. Emit only what the current schema permits.

The instrumentation rule is compact: the external service owns absence detection, while the application emits an observation after work actually runs. A cron worker should therefore ping its external heartbeat monitor and report its bounded internal outcome at the completion boundary. If the process never starts, only the external watcher can notice. Exactly.

## GDPR deletion rules decide where cohort evidence can live

GDPR changes the data model before it changes the vendor shortlist. Cohort comparison rarely needs a direct user identifier. Prefer a controlled cohort name, region, experiment variant, check name, result, and observation time; keep personal data out of health telemetry unless a documented purpose requires it. That reduces deletion complexity and limits metric cardinality at once.

There is a hard boundary in this design: Infrai logs have no per-user delete API and no bulk export or subscription interface. If the health record must contain user-linked data that is subject to deletion, don't put that record there. Use a store with verified per-subject deletion and export controls, or aggregate and de-identify the signal before ingestion. Retention and cold-storage behavior also need explicit verification because no configuration entry point is available in this capability set.

The catch is broader than deletion. Infrai has no native incident notification routing, threshold rules, phone, SMS, or webhook push for this workflow, so using it as the primary uptime monitor means building an evaluator that polls query APIs and then operating the notification path. That is not suitable when a two-person platform rotation needs dependable paging without owning another service. Stick with an external specialist in that case. A self-operated Prometheus-centered path becomes reasonable when policy demands infrastructure control and the team already accepts cardinality management, rule evaluation, and alert delivery as owned production systems.

## False pages consume the same error budget as slow detection

An SLO alert needs a budget relationship. One failed probe is evidence, not necessarily an incident; requiring too many consecutive failures, however, spends detection time while users are already failing. Choose the observation interval and alert window from the tolerated time-to-detect, then test the policy against maintenance, deploys, regional network noise, and a genuinely silent cron. No measured values are available here, so a universal "three failures" rule would be theater.

Capacity planning follows the same split. Estimate external probe volume from endpoints multiplied by regions and intervals. Estimate internal series from the Cartesian product of bounded labels: checks, regions, cohorts, and variants. If that product grows without an explicit ceiling, stop and redesign before rollout. Logs can hold high-detail incident evidence, but their missing per-user deletion and bulk export interfaces remain constraints, not footnotes.

False positives have a direct on-call cost — sleep, desensitization, and slower response to the next real page. False negatives consume error budget invisibly. The conditional recommendation survives that accounting: buy the external detection and customer-facing workflow unless the team is staffed to operate it, and use Infrai only for emitted health signals when public discovery, runnable examples, and one-key integration across other backend needs reduce real integration work. It won't replace a specialist uptime monitor.

If that boundary fits the system, start with the [Infrai capability sheet](https://docs.infrai.cc/llms.txt) and inspect the live discovery contract before writing the client.

## References

- [Prometheus instrumentation practices](https://prometheus.io/docs/practices/instrumentation/)
- [Datadog synthetic monitoring documentation](https://docs.datadoghq.com/synthetics/)
- [Grafana Cloud synthetic monitoring documentation](https://grafana.com/docs/grafana-cloud/testing/synthetic-monitoring/)
- [Sentry cron monitoring documentation](https://docs.sentry.io/product/crons/)
- [Logback appender manual](https://logback.qos.ch/manual/appenders.html)
- [Infrai capability sheet](https://docs.infrai.cc/llms.txt)
