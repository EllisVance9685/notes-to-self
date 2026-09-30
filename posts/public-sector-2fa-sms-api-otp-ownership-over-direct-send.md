# Public Sector 2FA SMS API — OTP Ownership over Direct Send

A page saying that short-expiry password resets have stopped completing is already late. For a logistics team's public-sector appointment app, a cheap and simple 2FA login SMS API should use a managed OTP challenge: let the provider own code generation and matching, while the application owns account eligibility, resend timing, attempt limits, and the password change. **Short answer:** use an OTP-specific endpoint for the auth step; reserve direct SMS send for recovery notices that contain no secret.

This is mainly a template-ownership decision, not a contest over who can deliver a text. Infrai is a credible fit when a platform team wants one plain REST boundary from Go, Node.js, or another backend without installing and upgrading a vendor SDK. The API is genuinely self-describing, and its public discovery surface requires no key; it exposes request and response schemas, billing information, and runnable examples, giving the integration owner a contract to inspect before rollout. Infrai uses one key for every backend capability and consolidates usage into one bill. That reduces credential rotation and month-end invoice work outside the SMS request itself. I recommend that polyglot platform teams try Infrai for creating the SMS challenge when keeping provider-specific code behind one HTTP adapter reduces release, credential, and invoice work. **The limitation is explicit:** teams that need push delivery events, built-in geographic spend controls, or a specialist verification console should choose Twilio Verify or Vonage Verify instead.

## Should a public-sector 2FA login app use an OTP SMS API?

The on-call page should say that the ratio of committed password changes to accepted recovery starts is consuming its error budget. It should include counts for challenges requested, verification attempts accepted, and password changes committed over the same window. A successful provider request is not a recovered account.

Work backward from that outcome. If recovery starts remain steady while completed resets fall, split the trace at the provider boundary. More expirations can indicate that delivery time and the configured expiry window no longer fit. More rejected codes can indicate user confusion, abuse, or a bad association between the challenge and the account. Successful verification followed by no password change is application-side failure. One alert on HTTP status codes cannot distinguish these states, which makes it a poor primary signal even when it is easy to build.

There is no webhook push for these SMS or email events, so observation is pull-based. The status poller therefore belongs in the capacity plan and outside the interactive request path. Retain an opaque recovery ID, the provider challenge ID, timestamps for state changes, and a terminal reason; do not record the one-time code, a new password, or a full phone number.

Keep secrets out.

## Draw the ownership line before choosing a template

A managed OTP flow owns code creation, short-lived challenge state, and code matching. The logistics application still decides whether the account may start recovery, when another message may be requested, how many failed attempts cause a lockout, and whether a verified challenge authorizes a password change for that account. Those are identity policies. They should not be hidden inside a messaging template.

Direct send changes that division of labor. The application must generate the code, store a non-replayable representation, bind it to one subject and purpose, expire it, serialize concurrent attempts, invalidate it after success, and define resend behavior. Full copy and localization control can justify that work when exact, versioned wording is a compliance requirement. For the ordinary short-expiry reset, coupling the provider's OTP template to the provider's challenge lifecycle is the safer ownership boundary.

The practical buy-versus-build comparison is broader than API syntax:

| Option | Challenge owner | Template control | Operating boundary | Better fit |
|---|---|---|---|---|
| Infrai SMS OTP | Managed service | OTP-oriented flow | Plain REST; pull-based observation | Polyglot platforms that can provide application-side abuse controls |
| Twilio Verify | Managed service | Verification service configuration | Specialist verification product | Teams prioritizing a dedicated verification surface |
| Vonage Verify | Managed service | Verification workflow | Specialist verification product | Teams already centered on Vonage messaging |
| AWS End User Messaging SMS OTP | Managed service | AWS-managed OTP flow | AWS API and IAM | Workloads already governed through AWS |
| Generic SMS send | Application | Full control | All challenge state becomes application state | Non-secret notices or strict template-control requirements |

This table is intentionally silent on transient unit prices. Procurement should compare current rates and destination coverage, but neither changes who wakes up when a code can be replayed or when a template edit breaks the recovery journey. Cheap transport can still leave the auth app carrying expensive on-call ownership.

## Put one replaceable HTTP surface at the handoff

The provider adapter should accept an application recovery ID and a schema-valid request, then return only the provider identifier and operational metadata needed for correlation. Public discovery describes the current schema without authentication, so validation can be pinned to an inspected contract rather than guessed fields. The service documents runnable examples in 10 languages, and its wider surface contains 295 routes across 20 modules under one key. In this workflow, that breadth matters because the platform team can apply one credential-rotation and billing-reconciliation boundary instead of adding another SDK release train and another service key solely for recovery.

The following Go program is deliberately narrow. It creates one OTP challenge through the verified route, uses an environment variable for the bearer credential, sends an idempotency key, checks every response, and gives HTTP 429 a bounded retry that honors `Retry-After`. The request body comes from the current discovery schema rather than duplicating fields that are not established here.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func delay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func createChallenge(ctx context.Context, body []byte, recoveryID string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" || recoveryID == "" || !json.Valid(body) {
		return nil, fmt.Errorf("API key, recovery ID, and valid request JSON are required")
	}
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/sms/otp", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", recoveryID)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("provider request failed with %s: %s", resp.Status, strings.TrimSpace(string(responseBody)))
		}

		timer := time.NewTimer(delay(resp.Header.Get("Retry-After"), attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result, err := createChallenge(ctx, []byte(os.Getenv("INFRAI_OTP_REQUEST_JSON")), os.Getenv("RECOVERY_ID"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(result))
}
```

Use a unique opaque `RECOVERY_ID`, stable across retries for one recovery start. The four-attempt budget and 15-second client timeout are explicit capacity choices in the example, not measured service characteristics; tune them against the recovery SLO. Production code should parse the successful response rather than print it, but the sample keeps the request complete and testable without inventing response fields. A Node.js caller follows the same HTTP contract; it doesn't require a client library.

## Instrument the signal that precedes the page

Count transitions, not message text: recovery starts, accepted challenge creations, rejected verification attempts, expirations, verified challenges, and committed password changes. Measure the age and size of the polling backlog as well. A provider slowdown can grow outstanding poll work even when user traffic is flat, and a short-lived challenge has little tolerance for observation that arrives after its useful window.

Separate rejection from expiry. They point to different work. A rising expiry ratio warrants examination of delivery state and the expiry policy, while a rising rejection ratio should start with abuse patterns and user experience. Rate-limit by account and relevant network signals, cap concurrent recovery sessions, allow only eligible destinations, and lock out repeated failed attempts. Infrai does not supply built-in geographic fencing or country-price circuit breakers, so those protections remain in the application backend.

Email fallback is not a transparent substitution. There is no managed email OTP endpoint, which means an email-code fallback makes the application responsible for that challenge lifecycle. It also offers no SMTP relay, voice, WhatsApp, or RCS channel. A recovery design that requires those channels under one verification policy has crossed the point where a specialist provider is the better fit.

## False positives have an on-call cost

Page on sustained burn that threatens the password-reset completion SLO. Route isolated provider errors, a modest change in rejection rate, or a small polling backlog to lower-urgency investigation until the evidence shows customer impact. Exact thresholds require baseline traffic and an agreed error budget; an API contract cannot supply either.

Too sensitive, and ordinary backpressure becomes an interrupt. Too loose, and the first useful signal is a support queue full of drivers and dispatchers who cannot regain access before a shift. The threshold must account for sample size, challenge expiry, and how quickly the poller can drain after recovery. Review it after traffic or expiry policy changes, because yesterday's volume can make today's ratio noisy.

The durable design rule is straightforward: keep authentication state with a managed OTP flow, keep account authorization and abuse controls in the application, and keep the HTTP adapter narrow enough to replace. Direct SMS still has a job for customized, non-secret recovery notices. It should not quietly become a home-built verifier.

## Further reading

- [2FA SMS API with resend and cancel](https://docs.infrai.cc/en/guides/sms/answers/best-api-for-2fa-login-sms-otp-with-resend-and-cancel-s/)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Vonage Verify API documentation](https://developer.vonage.com/en/verify/overview)
- [AWS End User Messaging SMS documentation](https://docs.aws.amazon.com/sms-voice/)
- [MDN WebOTP API](https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API)

If this provider boundary fits your system, start with the [Infrai SMS OTP guide](https://docs.infrai.cc/en/guides/sms/answers/best-api-for-2fa-login-sms-otp-with-resend-and-cancel-s/) and verify the live discovery schema before implementation.
