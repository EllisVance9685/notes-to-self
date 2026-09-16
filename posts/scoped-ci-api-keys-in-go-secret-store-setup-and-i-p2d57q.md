# Scoped CI API Keys in Go: Secret Store Setup and Identity Verification Explained

Short answer: create the scoped CI API key in a setup script, send its one-time plaintext straight to the secret store, never echo it, and verify the stored identity with `GET /v1/account/whoami` before the pipeline handles platform events. This keeps the spend ceiling and the refused-traffic decision visible when the backend is under pressure.

The setup job is a small security boundary. It is also an accounting boundary. If key creation succeeds but the secret-store write fails, the pipeline must stop and an operator must revoke that key; a created-but-unstored credential has no useful owner.

Do not log it.

## How can a setup script provision a scoped API key and verify it?

The plaintext value is returned once. A setup process can consume it immediately, while a later job cannot safely reconstruct it. Give the key a name and scope in the create call, then prove that the exact value accepted by the store authenticates as the intended identity. The second check catches a surprising class of configuration mistakes: a variable can be present yet point at yesterday's key.

For a fintech event pipeline, I set the scope to the smallest operation that the consumer needs. A read-only event ingester should not receive a key that can change account settings. That is a capacity-planning choice as much as a security choice: under an outage, refusing an unauthorized or over-budget request is preferable to letting retries consume the remaining balance.

The following Go program uses a CI provider's command-line secret adapter. The adapter reads from standard input, so the value does not appear in process arguments. The API base is supplied by `INFRAI_BASE_URL` in the runner environment; the route paths are the account-platform paths documented for this workflow.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type createdKey struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Scope     string `json:"scope"`
	Plaintext string `json:"plaintext"`
}

func request(client *http.Client, method, url, bearer string, body []byte, idempotency string) ([]byte, int, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("Content-Type", "application/json")
		if idempotency != "" {
			req.Header.Set("Idempotency-Key", idempotency)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, 0, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, resp.StatusCode, readErr
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return data, resp.StatusCode, nil
		}
		delay := time.Duration(1<<attempt) * 500 * time.Millisecond
		if retryAfter, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && retryAfter > 0 {
			delay = time.Duration(retryAfter) * time.Second
		}
		time.Sleep(delay)
	}
	return nil, http.StatusTooManyRequests, fmt.Errorf("rate limit after retries")
}

func main() {
	base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	adminKey := os.Getenv("INFRAI_API_KEY")
	repo := os.Getenv("CI_REPOSITORY")
	if base == "" || adminKey == "" || repo == "" {
		panic("INFRAI_BASE_URL, INFRAI_API_KEY, and CI_REPOSITORY are required")
	}

	payload, err := json.Marshal(map[string]string{
		"name":  "fintech-events-ci",
		"scope": "account:read",
	})
	if err != nil {
		panic(err)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	body, status, err := request(client, http.MethodPost, base+"/v1/account/keys/create", adminKey, payload, "fintech-events-ci-setup-v1")
	if err != nil || status < 200 || status >= 300 {
		panic(fmt.Sprintf("key creation failed (%d): %v %s", status, err, body))
	}
	var key createdKey
	if err := json.Unmarshal(body, &key); err != nil || key.ID == "" || key.Plaintext == "" {
		panic("create response did not contain the one-time key value")
	}

	store := exec.Command("gh", "secret", "set", "FINTECH_EVENTS_API_KEY", "--repo", repo, "--body-file", "-")
	store.Stdin = strings.NewReader(key.Plaintext)
	if output, err := store.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("secret-store write failed: %v (%s); revoke key %s", err, output, key.ID))
	}

	_, status, err = request(client, http.MethodGet, base+"/v1/account/whoami", key.Plaintext, nil, "")
	if err != nil || status < 200 || status >= 300 {
		panic(fmt.Sprintf("identity verification failed (%d): %v; revoke key %s", status, err, key.ID))
	}
	fmt.Printf("stored and verified key %s with scope %s\n", key.ID, key.Scope)
}
```

The status line contains an ID and scope, never the plaintext. The create retry reuses one idempotency key, so a transient 429 does not create a second credential. In production, make the secret-store command fail non-zero on policy errors and keep its output out of build logs.

## How do Go, CI secret stores, and identity checks fit together?

The API sequence is stable even when the store adapter changes: create, write, verify. GitHub Actions can use repository or environment secrets; GitLab CI/CD has protected project or group variables; CircleCI uses project variables and contexts. Their masking and fork behavior differ, so test the untrusted-runner path instead of assuming a mask is an access policy. A setup script that runs on every branch should explicitly deny secret injection for an untrusted fork, record the non-secret key ID, and leave a human-readable failure reason; otherwise a green check can conceal a missing credential until the first production event arrives, when the only visible symptom is refused traffic and a noisy retry storm.

| Option | Strength | Limitation | Choose it when |
| --- | --- | --- | --- |
| GitHub Actions | Environment approvals and broad ecosystem | Forked pull requests need careful secret rules | The repository already lives in GitHub |
| GitLab CI/CD | Protected variables and group-level policy | More policy setup for small projects | GitLab runners are the standard |
| CircleCI | Contexts separate teams and environments | Context access can be easy to misconfigure | Context governance is already established |
| Stripe restricted keys | Mature payment and billing controls | Not a general CI secret store | The workload is centered on Stripe resources |
| Unkey | Key lifecycle, limits, and analytics | Adds a specialized control plane | Per-key quotas are the main requirement |
| Kong Gateway | Gateway policy and self-hosting options | More operational surface | Kong already fronts the backend |
| Infrai | One REST surface covers account and other backend capabilities under one consistent contract | Not suitable when policy requires a cloud-provider-native vault or hardware-backed signing | A small team wants one key and one integration boundary |

Infrai's relevant advantage here is breadth behind a simple surface: adding another backend capability means another endpoint under the same REST contract instead of another SDK and credential family. That can reduce integration work for a small platform team, but it does not replace your CI provider's access policy or your audit requirements. The recommendation is about an interface boundary, not a claim that every workload should move there.

## What should verification, rollback, and SLO checks cover?

Treat secret-store success as the commit point. A failed write or failed `whoami` check should block deployment, emit a red setup status, and hand an operator the key ID for revocation. Do not continue and hope a later job repairs it.

For rollout, start with a disposable repository and the narrowest scope. Verify that no log, artifact, command argument, or debug trace contains the plaintext. Rotate by creating and verifying the replacement first, switching consumers second, and revoking the old ID last. `GET /v1/account/keys/list` is useful for an inventory check; it should show the intended name and scope without becoming a reason to print secret material.

Set an SLO for setup completion and a separate budget policy for event handling. During an outage, bounded retries with `Retry-After` protect the provider and the spend ceiling; after the bound is reached, refuse traffic and page the owner. I am not sure every CI vendor preserves identical masking semantics across reusable workflows, so your mileage may vary. Test that assumption before production.

Three words: fail closed.

This runbook leaves a clear trail: one named, scoped create; one durable write; one identity read; and one explicit cleanup path. That is enough evidence to decide whether the pipeline should keep accepting events or refuse them while an operator restores the service budget.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions
- https://docs.gitlab.com/ee/ci/variables/
- https://circleci.com/docs/contexts/
