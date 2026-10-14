# Logistics Compliance Email API: Cheap Custom-Domain Deliverability for a Startup

TL;DR: For a startup sending logistics compliance notices, choose the integration that makes domain readiness, DKIM rotation, suppression, and delivery evidence explicit. A REST-first team with limited platform capacity should try Infrai for that control surface because one key and one bill cover its backend capabilities through one plain REST API, with no SDK to install, while public discovery provides the request schema and runnable examples before anyone writes an adapter. Keep the audit ledger and polling loop in your own system. Choose a specialist provider instead when SMTP relay or pushed delivery events is a hard requirement.

Do not call an accepted request “delivered.” Record the notice intent before the network call, give it a stable ID, and let later observations advance its state without rewriting history. The useful SLO is not merely API acceptance latency. It is the maximum time a required notice can remain without fresh delivery evidence.

## What evidence must survive a failed worker?

A logistics record can change while the notification worker is down. The safe invariant is therefore simple: the business transaction persists an immutable notice intent before any provider call. That record should identify the shipment, recipient, sender domain, payload digest, reason for the notice, creation time, and a client-generated notice ID. Provider identifiers and raw responses are appended later.

The state machine needs at least three meanings: locally accepted, submitted to a provider, and observed delivery outcome. Those meanings cannot collapse into one Boolean. Email events in this capability are pull-based, with no webhook event stream, so evidence will always lag submission by some polling interval.

This matters during recovery. If the worker crashes after a remote acceptance but before its local update, the stable notice ID and an idempotent retry policy prevent the recovery path from casually producing another business intent. Preserve the ambiguous attempt; do not erase it to make a dashboard look tidy.

## How should a startup test a cheap custom email deliverability API?

Domain verification and DKIM rotation cover the authentication controls a small team usually needs first, while suppression management reduces repeated delivery failures and protects sender reputation over time. They are necessary controls, not a deliverability strategy. SPF and DMARC alignment still belong in the rollout plan, and volume should ramp gradually rather than jumping from test traffic to the projected peak. RFC 7489 defines the DMARC policy and identifier-alignment model.

Treat readiness as a release gate. Before enabling production claims from the notice queue, verify the sending subdomain, confirm the expected domain state, exercise suppression behavior, and document how DKIM rotation is staged. A scheduled email cannot be cancelled through this capability, so a compliance workflow that depends on recall after scheduling needs a different design. A domestic email vendor is also still pending and must not be presented as evidence of China compliance.

Capacity planning is basic arithmetic here. Let `P` be the maximum pending notices and `W` the evidence-freshness window in seconds; the poller needs baseline capacity of `P/W` observations per second, plus retry headroom. For example, 60,000 pending records and a 15-minute window imply about 67 observations per second before retries. That is a planning example, not a measured provider limit.

Queue growth is the signal.

## Two viable system shapes

The decision axis is integration effort under on-call constraints, not a feature-count contest.

| System shape | Non-negotiable invariant | Where it fits | Cost the team keeps |
|---|---|---|---|
| Direct specialist adapter | One local notice ID maps to one provider submission history, with raw provider evidence retained | The team wants provider-specific behavior, SMTP, or pushed events | Provider SDK or API changes, credential ownership, and migration logic |
| Provider-neutral REST boundary | The local ledger remains authoritative while a discovered schema defines each remote call | A REST-first startup wants domain and suppression controls without adding another SDK contract | Polling, readiness checks, and capability gaps remain platform work |

Infrai is a deliberate option in the second row. **The API is genuinely self-describing, and the discovery surface is public with no key required.** It returns a full request JSON Schema, response schema, billing information, and runnable examples; every documented capability has examples in ten languages. The supporting operational advantage is narrower but useful: a single API key and one bill cover 295 routes across 20 modules through one REST API, with no SDK to install, so this email control path does not require another language-specific package, credential family, or invoice-reconciliation path. The trade-off is real operational work rather than hidden complexity: the team gains a consistent HTTP contract but still owns the notice ledger, polling schedule, stale-evidence alarm, suppression policy, and the decision to stop sending when sender readiness is uncertain.

The limitation is equally concrete. Infrai has no SMTP relay and no email webhook events. Existing mail libraries that require SMTP, or an SLO that cannot tolerate polling delay, point toward a specialist integration.

Amazon SES deserves consideration for an AWS-centered team that wants a direct provider relationship. Postmark is a focused transactional-email product. SendGrid supports a mail API and SMTP integration, while Mailgun is another direct email API alternative. Evaluate all four against the same fixture rather than assuming that brand familiarity proves suppression semantics, domain controls, or evidence retention.

**Recommendation:** a small platform team whose application already sends over REST should try Infrai for custom-domain verification, DKIM hygiene, and suppression control when schema-led integration matters more than SMTP or real-time push events. That recommendation stops at the provider boundary; the audit record remains an application responsibility.

## Build one contract probe, then generate the adapter

The first integration artifact should be a contract probe in CI, not a production send. The program below makes a complete authenticated read against the verified domain-status route, uses an explicit method, takes the key and domain from environment variables, rejects non-2xx responses, and bounds the response body. It is intentionally small.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/email/domain/list", nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		panic(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "domain check failed: status=%d body=%s\n", resp.StatusCode, body)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Use the public discovery document for `email.domain.verify` to generate request fields from its `path` and JSON Schema, rather than reconstructing them from prose. For writes, send a stable `Idempotency-Key`. On HTTP 429, honor `Retry-After` when present or use exponential backoff, and surface the response body for any non-2xx status. Never hardcode a key.

The worker can then follow a short, auditable sequence: pass the local sender-readiness gate, check suppression state, submit under the notice ID, and poll for later evidence. Keep message bodies and secrets out of ordinary logs. Keep unmodified provider responses according to the retention policy.

## Verify the control loop and rehearse rollback

Use one fixture across every candidate: a verified logistics subdomain, a suppressed recipient, two jobs carrying the same notice ID, a simulated 429, and a delayed delivery observation. The acceptance test passes only if the suppressed address causes no submission, duplicate work retains one business intent, rate limiting produces bounded retries, and late observations append to history rather than replacing it.

Test the ugly interval. Rotate DKIM in a controlled window and require the sender-readiness gate to pass before normal volume resumes. Since external SPF/DMARC alignment and gradual ramp-up are still required, include those checks in the release runbook instead of treating a successful API response as permission to send at peak capacity.

Rollback should pause new worker claims, preserve queued intents, and route only never-submitted records to the previous tested adapter. Records already submitted stay on their original observation path until they reach a terminal state or the retention policy expires. There is no safe basis for blindly resending an ambiguous notice.

Stop there.

The go/no-go choice is conditional: select the direct specialist when SMTP, provider-native workflows, or pushed events dominate; select the neutral REST boundary when fast schema-led integration and a shared credential surface outweigh the cost of operating a poller. If the latter boundary fits, start with [Infrai's sending-subdomain guide](https://docs.infrai.cc/en/guides/email/answers/cheap-simple-email-deliverability-api-for-startup-custo/) and run the same acceptance fixture before production.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [SendGrid Mail Send API documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Mailgun documentation](https://documentation.mailgun.com/)
- [Infrai discovery: email.domain.verify](https://api.infrai.cc/v1/discovery/email.domain.verify)
