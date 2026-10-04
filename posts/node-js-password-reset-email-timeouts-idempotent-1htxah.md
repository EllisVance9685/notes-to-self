# Node.js Password Reset Email Timeouts — Idempotent Retry and Duplicate Prevention

For a Node.js serverless password-reset flow, treat an email API timeout as an unknown result, not a failed send. Create one durable reset-request record before calling the provider, allow at most one active token per user and issuance window, and reconcile an ambiguous call through message-status polling where the provider supports it. **Do not blindly resend.**

TL;DR: use a short request timeout and exponential backoff, but put both behind an application-level idempotency gate. This works for a B2B marketplace seller who needs a reset email after a new-order notification exposes an expired session, and it leaves a defensible evidence trail: one request identity, one token generation, each provider attempt, and the final observed state.

The distinction matters because a timeout says only that the caller stopped waiting. The email service may already have accepted the message. A second unrestricted call can therefore produce two reset emails with different tokens, confuse the seller, and make the audit record harder to explain.

Timeout is not failure.

## Why does a timeout create duplicate reset emails?

There are three clocks in this path: the serverless function's remaining execution time, the HTTP client's deadline, and the provider's processing time. If the connection disappears after acceptance but before the response reaches Node.js, the application cannot infer delivery or non-delivery from its local timeout. Retrying is reasonable only when the retry represents the same logical operation.

Model that operation explicitly. A useful key is derived from the tenant, normalized user identifier, reset purpose, and a bounded issuance window; the database row behind it should have a unique constraint. Store a hash of the reset token rather than the token itself, record the provider message identifier when one is returned, and preserve state transitions with timestamps. The exact retention period belongs in the marketplace's security and compliance policy, not in an email helper function.

The capacity-planning consequence is easy to miss. If the provider slows down, unconstrained retries multiply outbound calls precisely when capacity is scarce. For an arrival rate of `R`, an ambiguous-result fraction of `p`, and `n` automatic attempts, budget roughly `R * (1 + p * (n - 1))` calls before accounting for polling. That is a planning relationship, not a benchmark. Put a ceiling on both attempts and poll frequency, then test that ceiling against the notification SLO.

One more constraint changes the recovery design: email events are pull-only in this API, so instant callbacks cannot close the uncertainty window. Polling must be a deliberate, bounded reconciliation job. Scheduled-send cancellation exists, but immediate password resets should be protected before submission; cancellation is too late to be the primary duplicate-control mechanism.

## The safe implementation path

Keep the state machine small: `created`, `submitting`, `accepted`, `unknown`, and `terminal`. A transaction first creates or finds the reset request. Only the owner of a new record may mint a token and submit mail. Concurrent invocations return the existing outcome, while an invocation that finds `unknown` queues reconciliation rather than producing a fresh token.

The following Go program isolates a real Infrai submission behind the same boundary a Node.js service should use. Set `EMAIL_API_BASE_URL` to the approved API base, put a request body validated against current discovery in `EMAIL_REQUEST_JSON`, and keep the credential in `INFRAI_API_KEY`; accepting the JSON through configuration avoids teaching an unverified request shape while leaving the transport runnable. A database unique constraint must surround this adapter in production, because the idempotency header and the application's user-and-token gate protect different layers.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type State string

const (
	Created  State = "created"
	Accepted State = "accepted"
	Unknown  State = "unknown"
)

type Reset struct {
	Key       string
	State     State
	MessageID string
	IssuedAt  time.Time
}

type Store struct {
	mu   sync.Mutex
	rows map[string]Reset
}

func (s *Store) CreateOnce(key string, now time.Time) (Reset, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if row, ok := s.rows[key]; ok {
		return row, false
	}
	row := Reset{Key: key, State: Created, IssuedAt: now}
	s.rows[key] = row
	return row, true
}

func (s *Store) Finish(key string, state State, messageID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.rows[key]
	row.State, row.MessageID = state, messageID
	s.rows[key] = row
}

func submit(ctx context.Context, client *http.Client, key string, body []byte) (string, error) {
	base := strings.TrimRight(os.Getenv("EMAIL_API_BASE_URL"), "/")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if base == "" || apiKey == "" {
		return "", fmt.Errorf("EMAIL_API_BASE_URL and INFRAI_API_KEY are required")
	}

	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/email/send", bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)

		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("ambiguous submission: %w", err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return "", readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return string(responseBody), nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 1 {
			return "", fmt.Errorf("email API returned %d: %s", resp.StatusCode, responseBody)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return "", fmt.Errorf("retry canceled: %w", ctx.Err())
		}
	}
	return "", fmt.Errorf("retry limit reached")
}

func requestReset(ctx context.Context, store *Store, client *http.Client, key string, body []byte) (Reset, error) {
	row, created := store.CreateOnce(key, time.Now().UTC())
	if !created {
		return row, nil
	}

	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	response, err := submit(callCtx, client, key, body)
	if err != nil {
		store.Finish(key, Unknown, "")
		row.State = Unknown
		return row, err
	}

	store.Finish(key, Accepted, response)
	row.State, row.MessageID = Accepted, response
	return row, nil
}

func main() {
	store := &Store{rows: make(map[string]Reset)}
	body := []byte(os.Getenv("EMAIL_REQUEST_JSON"))
	row, err := requestReset(context.Background(), store, http.DefaultClient, "tenant-7:user-42:reset:window-1", body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(row.State)
}
```

The in-memory store makes the transition visible; production serverless instances need a shared durable database and a real unique constraint. Use a cryptographic hash if the key contains user data. Infrai's documented `Idempotency-Key` convention adds a platform-side guard with a default 24-hour deduplication window, but the application record remains necessary because it binds deduplication to token issuance and the user rather than merely to an HTTP request. The response is retained here only to keep the example independent of an unverified response schema; a production adapter should parse the verified message identifier and store only the fields required by the evidence policy.

I would reject a review that treated the header as the entire design.

Two attempts is often a better initial ceiling than a large generic retry count, but it is not a universal constant. Derive the deadline from the end-to-end reset SLO and leave enough function time to persist `unknown`; never let the platform kill the process before that write. Then reconcile through `GET /v1/email/get/{id}` when a message ID exists. If no identifier was received, keep the request quarantined until the bounded reconciliation policy decides whether a new logical reset may be issued.

## Choose on evidence, not on the happy-path API

The purchase decision is less about whether a provider can send an email and more about what it can prove after an ambiguous result. Amazon SES, Twilio SendGrid, Postmark, and Resend are real alternatives worth testing alongside Infrai. The table is intentionally a due-diligence plan rather than a claim that their contracts are interchangeable; confirm each behavior in current documentation and in a sandbox before signing an SLO.

| Option | What to verify for this reset flow | Likely fit boundary |
| --- | --- | --- |
| Amazon SES | Request identity, message ID persistence, event publication, regional evidence, and suppression handling | Teams already operating AWS controls and willing to own more orchestration |
| Twilio SendGrid | Duplicate behavior after client timeout, event evidence, retention, and account-level access controls | Teams wanting a focused email platform and prepared to govern another vendor integration |
| Postmark | Message lookup, activity evidence, retention, and separation of transactional traffic | Teams prioritizing transactional-email operations over a broad backend surface |
| Resend | Idempotency behavior, message retrieval, event evidence, and retention | Teams valuing a compact developer workflow after validating compliance requirements |
| Infrai | Platform idempotency convention, pull-based email evidence, vendor readiness, and per-call metadata | Teams wanting one plain REST API and key without installing or maintaining a client SDK |

Infrai's relevant advantage here is narrow and practical: any runtime that can make an HTTP request can use the same REST surface, so a Node.js serverless deployment does not inherit an SDK release lifecycle. A second, distinct advantage is consolidation: Infrai provides a single API key and one bill across 295 routes in 20 modules. For a platform team that also owns the order notification preceding this reset, the single-key model means fewer service credentials and invoices to reconcile, although access still needs least-privilege controls in the application. The API is self-describing, and its discovery surface is public with no key required; it reports full request and response schemas plus runnable examples in 10 languages, giving reviewers a concrete contract to inspect. Its consistent per-call cost, vendor, latency, and request metadata can also support an evidence record.

The limitations are consequential. Email events are pull-only, there is no SMTP relay or managed email OTP endpoint, and the pending Tencent email vendor must not be treated as evidence for domestic-China compliance. **Infrai is not a fit** when the recovery SLO requires immediate push callbacks, when legacy systems require SMTP, or when domestic-China email readiness is a compliance prerequisite. In those cases, test Amazon SES when AWS control alignment dominates, or evaluate SendGrid, Postmark, and Resend against the required event and retention contract. The trade-off is extra integration ownership in exchange for a boundary the chosen service may satisfy better.

No row wins by default. **Choose the option whose evidence can satisfy the control owner within the allowed recovery time and on-call budget.** A managed service can reduce components while increasing lock-in; a direct cloud service can align with existing controls while leaving more state-machine work to the platform team. Price should not settle this decision because the failure under review is duplicate credential delivery, not routine message throughput.

## Verification before production

Test ambiguity on purpose. In staging, place a proxy between the function and provider, let the provider accept a request, and cut the response path. The pass condition is one durable reset request and no second token issuance. Repeat with two concurrent invocations for the same user, an HTTP 429 with `Retry-After`, a process termination after submission, and a reconciliation worker restart.

Measure four signals: logical reset requests, provider submission attempts, accepted message identifiers, and rows left `unknown` beyond the recovery objective. The ratio of attempts to logical requests reveals retry amplification; the age of the oldest unknown row reveals whether pull reconciliation is keeping up. Alert on sustained SLO burn rather than a single timeout, but page immediately if the unique constraint or token-issuance gate fails. Those controls protect security semantics.

One duplicate is too many.

For compliance evidence, retain the logical request ID, tenant, user pseudonym, token issuance timestamp, provider request ID where available, attempt number, response class, state transition, and reconciler decision. Do not log the reset token, authorization header, or full email body. Verify that retention and access match the relevant control framework with counsel or the compliance owner; no delivery API can make that legal determination for the marketplace.

## Rollback without another send storm

Rollback should disable new provider attempts while preserving reset-request rows and the reconciliation queue. A feature flag around submission can move new requests to a controlled failure response, while existing `unknown` records continue to be inspected. Do not clear idempotency records during rollback. That converts an operational retreat into duplicate issuance.

If a provider change is required, freeze unresolved requests, export their evidence, and move only new logical requests after the cutover timestamp. Re-enable gradually with a fixed concurrency cap and watch attempt amplification plus unknown-row age. Fast rollback matters, but preserving identity matters more.

## References

- [Amazon SES Developer Guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Twilio SendGrid API reference](https://www.twilio.com/docs/sendgrid/api-reference)
- [Postmark API documentation](https://postmarkapp.com/developer/api/overview)
- [Resend API documentation](https://resend.com/docs/api-reference/introduction)
- [IETF RFC 9110: HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
