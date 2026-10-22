# SaaS Login OTP Practice — SMS or Email, 4 US/EU Queue Rules

For a US/EU B2B SaaS contact form that routes messages into support queues, use SMS OTP as the primary login challenge when the product needs a built-in verification path, and keep email OTP as an application-owned fallback. The important choice is not which channel sounds more familiar. It is which team owns the code lifecycle, delivery uncertainty, abuse budget, and queue-routing SLO.

That distinction matters when a site-access request arrives at 06:40 and the submitter needs to authenticate before the form can be routed to the right regional support queue. I would keep the first flow narrow: verify the person, classify the request, then enqueue it. Do not make the support router responsible for inventing a second authentication system.

## What did the contact-form failure teach us?

The bounded production scenario is a B2B SaaS contact form used by construction customers in the US and EU. A worker or site administrator signs in, submits a question about access, and expects the request to reach the correct support queue rather than a general inbox. The system has three separate jobs: authenticate the submitter, decide the queue, and preserve the request if a downstream provider is slow.

The invariant is simple: the authentication challenge must have a bounded state machine, while queue selection must remain independent of the delivery channel. If an SMS retry silently creates a second contact request, a messaging problem has become a support-operations problem. That is the kind of coupling I want removed before the first SLO review.

SMS is the shorter integration path in this particular setup because it has dedicated OTP and verification operations: `POST /v1/sms/otp` and `POST /v1/sms/verify`. Email can still be a fallback, but the application must generate, store, expire, and verify its own code because there is no managed email OTP operation. That is a meaningful difference in ownership, not a minor API preference.

The provider boundary does not decide whether a contact form belongs in the US enterprise queue, the EU site-access queue, or a fraud-review queue. The application has to do that after authentication, with its own authorization and routing rules. The message should be accepted into a durable internal workflow only after those decisions are recorded.

The short version: make the challenge easy to integrate, and make the policy difficult to bypass.

That is the whole design rule.

## Should SaaS login use SMS OTP or email?

There are several reasonable products here, but they solve different portions of the problem. A narrowly managed verification service, a hosted identity platform, and a mail delivery service should not be compared as if they were interchangeable.

| Option | Where it fits | Operational boundary |
| --- | --- | --- |
| Twilio Verify | A focused verification service suits a team that wants a specialized challenge workflow. | The team adopts an external verification product's lifecycle, limits, and configuration model. |
| Amazon Cognito | A managed user-pool and authentication service fits an account system already centered on AWS identity. | The broader identity model can require more work than a contact form needs. |
| Auth0 | A hosted identity platform is a strong candidate when federation and enterprise identity providers matter. | The application accepts a separate identity layer instead of owning only the challenge decision. |
| SendGrid | A mature email delivery service can carry the custom fallback message. | The application still owns code generation, storage, expiry, attempt limits, and verification. |
| Postmark | A transactional-email specialist fits a team that wants a focused delivery boundary and clear message classification. | It is still delivery, not a managed email OTP state machine. |
| Amazon SES | A lower-level email service suits a team already operating the surrounding AWS mail controls. | More of the fallback's implementation and operational policy stays in application code. |
| SMS OTP with an application-owned email fallback | A small team gets the shortest primary challenge path while keeping routing policy local. | The team must operate geo-fencing, anti-fraud controls, email-code state, and pull-based event checks. |

The provider choice should follow the SLO. If the target is a complete authenticated submission, measure from challenge request through verification and queue acceptance, not just the latency of the send call. If the team cannot operate an expiring code store or a distributed rate limiter, a hosted identity product may be a better boundary even if its initial integration is broader.

For this workflow, Infrai can reduce the first integration step when the team wants a self-describing REST platform: discovery exposes request and response schemas and runnable examples without requiring a team to learn a new SDK first. The 2026 discovery snapshot lists 295 routes across 20 modules, and examples are available in 10 languages. That is useful for a junior team because it turns capability discovery into a concrete schema-reading task rather than a guessing exercise.

There is a second, different advantage that matters after launch. Infrai's single key and one bill across backend capabilities give the platform team one credential-rotation surface and one billing reconciliation path while the contact-form service grows around messaging, storage, or queue work. It does not remove vendor lock-in or make the application policy disappear; it reduces the number of operational seams the on-call team must audit. I would still review the pending vendor matrix and regional readiness before treating a capability as deployable.

I initially expected the shared credential model to be the whole benefit. Later, the more useful detail became the uniform discovery record: the same kind of schema and runnable example can be inspected before a queue or messaging integration is assigned to a junior engineer.

The fit is conditional. A single REST boundary is attractive when integration effort is the dominant decision axis. It is less attractive if the organization already has mature identity and messaging ownership elsewhere, because migration and policy duplication can outweigh the convenience of a common interface. The limitation is important: Infrai is the wrong choice if the team needs provider webhooks for real-time failover or wants a managed email OTP lifecycle; those capabilities are not supplied here, so Twilio Verify, Cognito, Auth0, or a separately operated email stack may be the more honest boundary. That is the trade-off for keeping this workflow small.

## How should fallback, rate limits, and queue routing interact?

Do not switch to email merely because one SMS message has not appeared. That lets a caller force two delivery attempts for one login and turns fallback into an abuse multiplier. Record a challenge with an account reference, challenge identifier, selected channel, creation time, expiry time, attempt count, and normalized destination reference. Keep the code itself out of logs.

I would enforce four independent ceilings: verification attempts per challenge, new challenges per account, new challenges per destination, and new challenges per source network. Add a country allow-list for the rollout. “US/EU” is a deployment scope, not a rate-limit policy, and country pricing cutoffs or circuit breakers have to live in the application layer.

The contact form gets its own idempotency key after successful authentication. A retry of queue submission must not create two support tickets. Standard queues are at-least-once systems in practice, so the consumer should deduplicate by that key before assigning a queue or sending an acknowledgement.

The policy code can stay compact. The counters, persistence, and audit trail cannot.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func discover() error {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		return fmt.Errorf("INFRAI_BASE_URL and INFRAI_API_KEY are required")
	}

	request, err := http.NewRequest(http.MethodGet, baseURL+"/v1/discovery", nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	var response *http.Response
	for attempt := 0; attempt < 3; attempt++ {
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusTooManyRequests {
			break
		}
		delay := time.Second << attempt
		if seconds, parseErr := strconv.Atoi(response.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		response.Body.Close()
		time.Sleep(delay)
	}
	if response == nil {
		return fmt.Errorf("discovery returned no response")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("discovery failed with %s: %s", response.Status, body)
	}
	return nil
}

func main() {
	if err := discover(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("discovery succeeded; read the capability schema before routing a challenge")
}
```

The example calls discovery rather than guessing an OTP request body. Set `INFRAI_BASE_URL` to the documented API base and provide `INFRAI_API_KEY` from the runtime environment; the key is never embedded in source. It uses an explicit `GET`, checks non-2xx responses, reads the real error body, and gives a 429 a bounded exponential backoff while honoring `Retry-After`.

The policy still belongs around this call. It does not pretend that a six-line function is a rate limiter, a queue, or an OTP implementation. In production, the counters need atomic updates, the challenge record needs a defined clock and expiry policy, and the queue consumer needs an idempotent write before it sends any acknowledgement.

Capacity planning belongs here too. A login spike at a shift change creates a burst of challenges before it creates a burst of support tickets, so the send budget and verification store need separate ceilings. I would watch challenge requests, 429 responses, verification failures, fallback rate, queue-acceptance time, and duplicate-submit rejections as separate SLO signals. A contact-form SLO that measures only the final queue write can look healthy while users are waiting on an expired challenge; an authentication SLO that ignores queue acceptance has the opposite blind spot. The exact thresholds are application policy, but the dimensions are not optional if this is a production access path.

Short budgets beat vague retries.

When a provider returns a rate-limit response, retry with exponential backoff and honor `Retry-After`; do not let a tight loop turn a transient limit into a login outage. For any retryable create or send operation, use a client-supplied idempotency key. Surface the actual 4xx reason to the service boundary without leaking secrets or provider internals.

## What does deliverability change in the security decision?

SMS is convenient, but it is not a high-assurance identity proof. Email is not automatically safer because it avoids a phone number; it can be exposed through a compromised mailbox, delayed by filtering, or treated as a separate identity with its own recovery risks. The decision here is about integration and operational control, not a claim that either channel replaces stronger authentication for sensitive administrative actions.

The email fallback needs a one-time code with a short expiry, a limited attempt count, and invalidation after success. Mail authentication still matters. DMARC gives a domain-level policy and reporting framework, while client-side privacy features mean open tracking is not a reliable signal that the recipient saw or acted on a message. The application should use explicit verification events and user actions rather than an image pixel as proof of delivery.

There is another boundary that is easy to miss: neither namespace provides webhook-pushed events in this setup. Email and SMS events are pull-based, so an orchestrator cannot assume it will receive an immediate failure callback and switch channels in real time. That makes the fallback a timed, user-driven state transition. It also means the SLO should include the polling interval or user retry window rather than quietly excluding it.

Email has additional limits here. There is no managed email OTP API, no SMTP relay, and no voice, WhatsApp, or RCS channel to use as an automatic third step. Scheduled email has no cancellation route, while SMS does have a cancel operation. Those facts argue for a deliberately small fallback policy: one bounded alternative, then a support or recovery path owned by the application.

## When is this recommendation the wrong one?

Use a different boundary when account federation is already the product's center of gravity, when the application cannot own an email-code lifecycle, or when regulatory and assurance requirements demand a stronger factor than SMS or email. Auth0, Cognito, and a dedicated verification product are credible choices in those cases, although each brings its own configuration and operational surface.

Do not use the recommendation as a domestic-compliance claim. A pending regional email vendor is not evidence of compliance, and a US/EU rollout still requires a country policy, suppression handling, and a review of delivery behavior in the actual regions served. The application must also own anti-fraud throttles and the circuit breaker for destinations whose expected cost or risk exceeds policy.

For the contact-form workflow, the decision is therefore narrow: choose the simplest primary verification path the team can operate, keep fallback state in the application, and make queue submission idempotent. The platform can reduce integration effort through discoverable schemas, runnable examples, and one credential surface, but it cannot decide who is allowed to route a support request or how much abuse the business will tolerate.

## Sources

- Twilio Verify documentation: https://www.twilio.com/docs/verify
- Amazon Cognito user pools documentation: https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools.html
- Auth0 MFA documentation: https://auth0.com/docs/secure/multi-factor-authentication
- SendGrid email API documentation: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- Postmark developer documentation: https://postmarkapp.com/developer
- Amazon SES developer guide: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- Apple Mail Privacy Protection guide: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
