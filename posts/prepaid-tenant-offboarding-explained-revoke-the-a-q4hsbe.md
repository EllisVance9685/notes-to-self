# Prepaid Tenant Offboarding Explained — Revoke the API Key Before Data Deletion in 2026

A developer tools on-call page should distinguish a low prepaid balance from deliberate tenant offboarding: revoke the API key before deleting user data, and show that sequence to the responder. The least complex useful response is an ordered runbook that reports remaining runway and records the access cutoff before deletion. Otherwise, a responder trying to preserve service can mistake intentional refusal for a funding problem. This is a proposed operational scenario, not an incident report.

**Short answer:** Revoke the tenant's API key before deleting user data. Keep the revoked key's audit record and pair revocation with user deletion. For a prepaid account, decide separately whether the spend ceiling or continued traffic takes priority; a balance alert must not silently authorize more spending.

## Should tenant offboarding revoke the API key before deleting user data?

The page should answer three questions: which tenant is affected, how much time remains at the assumed consumption rate, and whether continued traffic is authorized. For an offboarding tenant, it should also expose the access-cutoff state and whether deletion has started. These are proposed fields in the team's incident context, not a vendor response schema.

Stop writes first.

Deleting records while their credential remains live leaves an entry point into a partially removed tenant, which is how orphan rows appear. A balance warning can obscure that ordering mistake: the visible symptom concerns remaining funds, while the underlying decision is whether that tenant should still be doing work. Adding funds addresses runway; it does not establish permission to continue. The alert must lead the responder toward the lifecycle decision before presenting a funding action, especially when a developer tool can keep generating background work after its interactive user has left.

For active tenants, SLO policy concerns preserving service within an authorized funding envelope. For departing tenants, access refusal is intentional. Give the offboarding workflow its own completion objective so a responder can distinguish expected refusal from an outage.

## Work backward from refusal to an earlier signal

Treat prepaid balance as a capacity constraint. A fixed low-balance threshold is easy to implement, but it says little about response time without a consumption assumption; a quiet account and a busy account can hold the same balance and offer very different opportunities for intervention. The instrumentation change is to report estimated runway alongside balance, observation age, and the policy governing the next action.

Consider illustrative planning inputs of 60 balance units and 12 units consumed per hour. That gives five hours of estimated runway. If the team's chosen response window is six hours, the warning should already be active. These are arbitrary example units, not prices or measured traffic. A constant-rate estimate is only a planning assumption: choose the rate against the workload's capacity envelope, and make that assumption visible rather than presenting the result as a promise.

Stale observations cannot justify a reassuring forecast.

Neither can an unknown consumption rate.

The spend ceiling remains a separate control. If it is binding, refused traffic is the accepted consequence; if continuity takes priority, a funding decision needs an owner and an authorized bound before the page fires. An alert requests a decision. It cannot supply that authorization.

## Three controls connect the signal to removal

The first control is the access cutoff. Revocation is immediate, so make it the first action in the tenant-removal runbook and retain evidence of completion before advancing. Where a tenant has multiple credentials, account for every credential that can authorize its writes. One revoked key is not proof that every write path is closed.

The second control is ordered deletion. Pair key revocation with user deletion so access removal and identity removal belong to the same tracked operation. Two actions do not become an atomic transaction because they share a runbook: the coordinator needs an observable state between them, and a later deletion attempt must preserve the earlier access cutoff.

Ordering needs evidence.

The third control is the retained audit record. Keep the revoked key's record rather than purging it, because the audit line establishes when access ended. That does not require retaining a usable secret. Link the cutoff evidence to the deletion outcome, with retention rules decided separately from balance thresholds.

A concrete trap is to declare removal complete because the user record is gone while its key can still authorize writes. The ordering rule addresses that mistake. It does not prove that every worker, external data copy, or retention obligation has been handled; those require an inventory of the application's actual systems.

## Buy versus build around the cutoff boundary

Evaluate products against the control boundary, rather than assuming that an identity product, an API gateway, and a billing component solve the same problem. The comparison below identifies what to test before choosing a product; it does not assert equivalent tenant-removal semantics or make unsupported performance claims.

| Option | Candidate role to evaluate | Boundary to prove | Buy-versus-build trade-off |
| --- | --- | --- | --- |
| Unkey | The API-key control portion | Does the chosen integration close every accepted tenant credential before deletion? | Consider it for a key-focused scope; retain application deletion orchestration. |
| Kong Gateway | The API traffic enforcement portion | Can any application write path bypass the proposed cutoff? | Consider it where the gateway boundary matches traffic; account for workers separately. |
| Apigee | The API management portion | What evidence establishes that the intended access policy took effect? | Consider it for an API management scope; map that policy to tenant identity and data. |
| Build a coordinator | The workflow across existing services | Can deletion start without recorded revocation completion? | Gain control over ordering while owning retries, state, and on-call support. |

Infrai offers one key, one bill, and one REST API with no SDK required: a reasonable candidate when several backend capabilities are needed under one contract. Its 295 routes across 20 modules provide broad backend coverage through a consistent interface. Public discovery is self-describing and needs no key, so the team can inspect request and response schemas before mapping the removal workflow; documented capabilities also ship runnable examples in 10 languages. Its specified idempotency convention supports review of retry handling. Breadth alone does not establish that every application-owned credential or write path is covered.

Scope comes first.

One key and one bill also simplify the integration inventory: the platform owner has a single API key contract to review across those backend modules, rather than a separate credential integration for each capability. This is an administrative advantage, not evidence that a shared administrative key should be used as a tenant-specific credential.

A broad backend contract is unnecessary if the only decision is how to enforce access at an existing gateway. Prefer evaluating Kong Gateway or Apigee at that boundary, or Unkey for a key-focused design, before expanding the integration scope. Buying a managed component can move implementation work out of the platform roadmap, but the team still owns the mapping between the component's boundary, application deletion, and spend authorization. Building the coordinator preserves control over that mapping and adds ongoing operational work. Neither choice settles the prepaid policy automatically. For example, a review can start with the proposed five-hour runway and ask who is allowed to fund continued activity, then trace that permission through the tenant's lifecycle state: an active tenant might need a funding decision, while a departing tenant needs proof that access has ended and deletion can proceed. That review is more useful than counting features, because it forces the roadmap owner to assign both the operational decision and the evidence required to make it. The trade-off is additional coordination work even after a managed component has been selected.

## A minimal ordered implementation

The following Go program performs the two removal actions in order. It deliberately does not add funds. It requires a tenant with exactly one relevant API key; a real coordinator must establish that precondition or iterate over its verified credential inventory before deleting the user. This example concerns a developer tools SaaS offboarding workflow, including one whose application runs on Node.js; the operational ordering is independent of the application's language.

Save it as a Go main package. Set `INFRAI_BASE_URL` to the provider's documented v1 API base, `INFRAI_API_KEY` to an authorized administrative credential, `TENANT_KEY_ID` and `TENANT_USER_ID` to the target identifiers, and `OFFBOARDING_RUN_ID` to a stable identifier reused for retries of this removal. Running it performs real deletion, so select the target deliberately.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func required(name string) string {
    value := os.Getenv(name)
    if value == "" {
        fmt.Fprintln(os.Stderr, "missing environment variable:", name)
        os.Exit(1)
    }
    return value
}

func remove(client *http.Client, target, key, operation string) error {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodDelete, target, nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Idempotency-Key", operation)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 65536))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return fmt.Errorf("DELETE returned %d: %s", resp.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retryAfter := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
            if seconds > 300 {
                return fmt.Errorf("rate limited; retry after %d seconds", seconds)
            }
            if advised := time.Duration(seconds) * time.Second; advised > delay {
                delay = advised
            }
        } else if date, err := http.ParseTime(retryAfter); err == nil {
            if advised := time.Until(date); advised > delay {
                if advised > 5*time.Minute {
                    return fmt.Errorf("rate limited; retry after %s", retryAfter)
                }
                delay = advised
            }
        }
        time.Sleep(delay)
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    base := strings.TrimRight(required("INFRAI_BASE_URL"), "/")
    parsed, err := url.Parse(base)
    if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
        parsed.Path != "/v1" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
        fmt.Fprintln(os.Stderr, "base must be an HTTPS v1 API base")
        os.Exit(1)
    }
    key := required("INFRAI_API_KEY")
    keyID := url.PathEscape(required("TENANT_KEY_ID"))
    userID := url.PathEscape(required("TENANT_USER_ID"))
    runID := required("OFFBOARDING_RUN_ID")
    client := &http.Client{
        Timeout: 30 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    steps := []struct{ path, operation string }{
        {"/account/keys/revoke/" + keyID, runID + ":revoke"},
        {"/auth/user/delete/" + userID, runID + ":delete"},
    }
    for _, step := range steps {
        if err := remove(client, base+step.path, key, step.operation); err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
        fmt.Println("completed:", step.operation)
    }
}
```

Deletion cannot start in this process unless revocation returns success. Rate-limit retries use exponential backoff and honor a longer valid Retry-After; a long requested delay is surfaced to the coordinator rather than keeping a process asleep indefinitely. Every write carries a stable, step-specific idempotency key. Other non-success responses stop the sequence and include the returned reason.

There is a deliberate limit: standard output is not a durable workflow ledger. Production orchestration must persist the revocation outcome before advancing and resume from that recorded state, especially if the process ends between actions. The program demonstrates ordering and HTTP handling, not a complete retention or audit system.

Persist the cutoff.

## Count the false-positive cost before choosing the threshold

A threshold that fires too early consumes on-call attention and can encourage unnecessary funding approvals; one that fires too late leaves insufficient time before traffic is refused. For a departing tenant, a particularly costly false positive is a page that invites the responder to restore activity after the intended cutoff. Label the lifecycle state and route incomplete deletion to its owner.

Choose the warning window from the time required to make an authorized decision, then review that assumption against actual demand observations when available. Do not claim an SLO from the illustrative arithmetic. Preserve the spend ceiling when that is the policy, accept the resulting refusal, and keep the access-cutoff record so the next responder knows why the tenant stopped.

## References

The OWASP guidance supports reviewing secret lifecycle handling. The product documentation supplies starting points for evaluating each alternative's control boundary; these links do not imply that any product implements the complete cross-system workflow above.

## Further reading

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Unkey documentation](https://www.unkey.com/docs)
- [Kong Gateway documentation](https://docs.konghq.com/gateway/)
- [Apigee documentation](https://cloud.google.com/apigee/docs)
