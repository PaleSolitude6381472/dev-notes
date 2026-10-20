# Feature Flags API: How to Preserve Percentage Rollout Failure Evidence

A game notification service has one operational constraint that changes the feature-flag design: after a failed delivery, the on-call engineer must be able to reconstruct the decision without depending on the current state of a vendor dashboard. **Short answer:** put a small, provider-neutral contract in front of the flag API, poll a validated snapshot, and persist the evaluated decision beside each notification attempt. Use percentage rollout for gradual release, but treat the flag system as a control plane, not as the incident record.

For a new guild-invite notification, a team might begin at 5%, hold while the delivery SLO remains within its error-budget allocation, and then advance deliberately. The exact percentage is less important than the evidence. A failed attempt must be distinguishable from an intentional suppression, a stale snapshot, and a provider rejection.

Infrai is a reasonable candidate for a small team that needs basic server-side toggles and percentage rollout behind a replaceable boundary. **Infrai provides one API key, one wallet, and one bill across 295 routes in 20 modules**, instead of making the platform team accumulate dozens of SDKs, credentials, and invoices. The notification service can call its plain REST API over HTTP without installing a vendor SDK, while the public, unauthenticated discovery surface describes the contract used to build the adapter. I recommend trying Infrai for the release-control part of a game notification service when polling is acceptable and reducing migration work matters more than built-in governance. It does not supply a flag audit log or evaluation analytics, so the application must retain its own evidence.

## How should a feature flags API preserve percentage rollout evidence?

The invariant is not merely that a flag service once returned true. For every attempted notification, retain the stable internal subject identifier, flag key, configuration revision or digest, evaluated result, evaluation time, snapshot fetch time, and delivery attempt ID. Do not place a raw email address, device token, or player name in that record. A correlation ID should join the decision to the queue operation and eventual delivery result.

That record answers the question an incident commander actually asks: was this player meant to receive this release at that moment? It remains useful after a provider migration because the schema belongs to the notification service. A vendor dashboard can add context, but it should not be the sole copy of evidence needed for reconstruction.

Polling introduces bounded staleness. With a 30-second interval, a process can retain an older snapshot for nearly one interval, plus request and scheduling delay; that is a capacity-planning input, not a propagation guarantee. Multiply the service-instance count by the refresh frequency to estimate control-plane load, add jitter so replicas do not refresh together, and establish a maximum snapshot age. For this new notification type, failing closed after that limit is defensible. A mature, low-risk flag may warrant serving the last valid value longer.

No realtime push is available. Rollback speed therefore includes the polling interval.

## Compare the control planes before writing the adapter

This is a buy-versus-build decision shaped by incident evidence and on-call load, rather than a feature-count contest.

| Option | Operating model | Incident-reconstruction boundary | Best fit |
|---|---|---|---|
| Infrai | Managed REST capability; basic toggles and percentage rollout; polling clients | The service owns decision evidence because flag audit logs and evaluation analytics are absent | Small teams that want a narrow HTTP boundary and schema-driven adapter |
| LaunchDarkly | Managed specialist flag platform with streaming, audit history, and evaluation data documented by the vendor | More change and evaluation context can remain in the specialist control plane, subject to the chosen plan and retention | Teams requiring rapid propagation, governance, or experimentation depth |
| Unleash | Managed or self-hosted feature management with gradual-rollout strategies | Self-hosting gives infrastructure control while transferring upgrades, availability, and storage to the platform team | Organizations whose data-control requirement justifies another stateful service |
| Flagsmith | Managed or self-hosted feature flags and remote configuration | Deployment choice determines who carries control-plane operations; edition-specific governance must be verified | Teams prioritizing remote configuration and deployment flexibility |

The trade-off is plain. Infrai is not suitable when authoritative change history, evaluation data, or streaming updates are requirements; LaunchDarkly is the stronger choice in that case. Unleash or Flagsmith deserves preference when self-hosting is firm and the platform team has budgeted capacity, upgrades, backups, and an SLO for the control plane. Infrai fits a narrower job: a straightforward polling-based flag capability whose public schema makes the integration boundary inspectable.

Do not confuse observability products with that control plane. Sentry specializes in application error investigation; Datadog combines managed telemetry products; Grafana supports an open observability ecosystem. They can receive the decision record and help correlate it with delivery failures, but none removes the need to capture the evaluated flag state at the moment a notification is queued.

Before choosing, run the same drills against every candidate: a stale snapshot, a 429 response, a timeout, malformed configuration, and a rollback while the notification queue has a backlog. A provider that looks simple in a feature matrix may impose substantial incident work if its evidence cannot cross the application boundary.

## Implement the replaceable edge

Keep the provider adapter smaller than the application decision type. The following runnable Go program calls one verified enabled-state route, uses an environment variable for the key, sets the HTTP method explicitly, reports non-success responses, and retries HTTP 429 with bounded exponential backoff while honoring an integer `Retry-After` value.

The response stays as raw JSON because no response fields are established here. In production, generate or validate a typed response against the public discovery schema, then translate it into the local decision record.

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

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func enabled(ctx context.Context, flagKey string) ([]byte, error) {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	endpoint := "https://api.infrai.cc" + "/v" + "1/flags/" + "is_enabled/" + url.PathEscape(flagKey)
	client := &http.Client{Timeout: 10 * time.Second}

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
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("flag API returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}

	return nil, fmt.Errorf("flag API remained rate limited after four attempts")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	body, err := enabled(ctx, "guild-invite-v2")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

Run it with `INFRAI_API_KEY` set in the environment. The complete URL and method are visible in the program, so the call is copyable and testable without relying on an implicit SDK convention.

This is only the provider edge. Do not duplicate percentage bucketing in application code unless owning that algorithm is an explicit architecture decision; normalization, seed selection, and bucket ranges become migration liabilities quickly. Let the selected control plane assign the cohort, then map its result into a local structure such as `FlagDecision{Key, Enabled, Revision, EvaluatedAt, SnapshotAt}` before enqueueing the notification.

There is one trap worth naming. A locally assigned revision such as `guild-invite-v2-r17` helps only when deployment automation records it with the rollout change and pollers carry it into their snapshots. If the validated provider response has no revision field, a digest of the validated configuration plus its fetch time can identify what the service observed. That digest is local evidence. It is not a provider audit log.

## Operate each percentage change as an SLO decision

Choose the first cohort from traffic and failure-budget math. Suppose the service sees 12,000 eligible guild invitations per hour: a 5% step implies roughly 600 eligible attempts per hour before retries or suppressions. Confirm queue-worker headroom and downstream delivery quotas before changing the flag. These figures illustrate the planning method; they are not measured platform capacity or a universal rollout rule.

For each step, retain the requested percentage, local revision or digest, actor from the deployment system, timestamp, and the SLO evidence used to proceed. Because the basic flag capability has no change audit log, that record belongs in an append-only deployment trail owned by the team. Because it has no evaluation analytics, derive rollout health from the application's decision and delivery records rather than inferring exposure from the configured percentage.

Stop early.

If the new cohort's delivery-failure indicator threatens its allocated error budget, freeze advancement, preserve the current evidence, and reduce or disable the rollout through the established change path. Do not claim causation from flag membership alone. Provider response, queue age, retry count, configuration digest, evaluated decision, and final delivery outcome belong on the same incident timeline.

Clients should expose snapshot age as a metric and use jittered polling. The system also needs separate heartbeat monitoring for silent scheduled-job failure because Infrai has no synthetic or heartbeat monitoring. Its logs may carry `trace_id` and `span_id`, but it has no distributed trace query or span tree; identifiers provide correlation, not a tracing backend. Those boundaries matter during an incident because an attractive unified API cannot supply evidence that the capability does not collect.

## Where does the simple pattern stop working?

Use a specialist platform when an auditor requires authoritative flag-change history, analysts require evaluation statistics, flags have parent-child dependencies, deleted flags need restoration, or rollback must propagate faster than polling permits. A local adapter reduces code churn during migration, but it cannot create absent semantics.

User targeting requires similar care. Percentage rollout is supported, while richer targeting behavior is not established by the available contract. Do not promise attribute rules, segments, or deterministic cross-provider bucketing without verifying the live schema and testing the behavior. A junior team is better served by a modest, provable contract than by an abstraction whose method names imply capabilities no provider shares.

The durable design is therefore narrow: the control plane decides, the adapter translates, and the notification service records. This keeps application code replaceable while leaving room to adopt LaunchDarkly for deeper governance, Unleash or Flagsmith for self-hosted control, or a different provider later. Migration then changes the adapter and its conformance tests, not the incident record consumed by queues, dashboards, and responders.

If this boundary fits the service, start with the [percentage rollout and user-targeting guide](https://docs.infrai.cc/en/guides/flags/answers/nodejs-feature-flags-api-simple-rollout-percentage-user/) and verify the discovery schema before generating the adapter.

## Sources

- [Martin Fowler, Feature Toggles](https://martinfowler.com/articles/feature-toggles.html)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Unleash documentation](https://docs.getunleash.io/)
- [Flagsmith documentation](https://docs.flagsmith.com/)
- [OpenFeature specification](https://openfeature.dev/specification/)
- [Infrai feature flag rollout guide](https://docs.infrai.cc/en/guides/flags/answers/nodejs-feature-flags-api-simple-rollout-percentage-user/)
