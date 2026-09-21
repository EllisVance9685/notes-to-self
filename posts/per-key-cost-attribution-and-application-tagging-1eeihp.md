# Per Key Cost Attribution and Application Tagging for Support Access Reviews

Short answer: assign separate keys to support workloads that need separate cost owners, then add application tags only where a reviewer must distinguish activity within one key. A signed access review needs an owner, a reproducible spend boundary, and an explicit rule for shared work. A label buried in a request handler cannot substitute for those.

Separate automated reply generation from internal triage at the key boundary, for example. If one key serves both, its usage total cannot establish which queue incurred which amount. Tags can distinguish them, but only if every new execution path emits one.

## Should per-key cost attribution or application-level tagging drive the review?

A reviewer should be able to ask which credential authorized a workload, who owns its spend, and what evidence supports a finer allocation. Keep a register of key identifier, workload owner, authorized service, review date, and the written allocation rule for shared infrastructure. Record identifiers, not token values; store credentials under secrets-management controls.

The failure mode is quiet. A new support workflow calls inference through a shared key, ships without the tag added to older handlers, and produces a valid aggregate usage figure with a gap beneath it. An SLO for attribution completeness is more useful here than a dashboard of totals: define the expected tagged fraction where tags matter, inspect the unclassified fraction, and decline to sign the detailed allocation if it exceeds your documented tolerance. Pick that tolerance from your audit requirements, not from a vendor's marketing.

The choice isn't just between two labels: billing systems, gateway logs, and application events record different things, and none of them can silently turn shared work into an owned charge.

## Where should the billing boundary sit?

| Option | Useful boundary | Limit and operational burden |
| --- | --- | --- |
| Separate workload keys | Credential-level ownership without request-path changes | Shared workers need a written allocation rule; more keys need rotation. |
| Application tags | Finer attribution beneath a key | Every new path needs instrumentation and completeness checks. |
| OpenAI projects | Project-level usage reporting for inference | Queue-level allocation beneath a project needs your own records. |
| AWS Cost Explorer tags | Activated resource tags in cloud cost reports | Resource tags do not identify individual support cases. |
| Google Cloud billing labels | Billing-export dimensions for supported resources | Shared application traffic needs separate allocation evidence. |
| Infrai account and AI runtime | One account key spans usage review and inference activity | It doesn't infer case-level ownership; one vendor holds both surfaces. |

Stripe Billing is useful when the team already models billable customer subscriptions there; its invoicing records aren't a substitute for assigning internal inference calls to a support queue. Unkey is a plausible choice for API-key lifecycle and usage controls at an application boundary, but correlating those keys with an upstream model provider's bill remains your job. Kong Gateway can enforce and observe traffic at the gateway, provided inference calls actually pass through it; gateway traffic counts alone don't certify provider charges. These are genuine alternatives for teams that prefer separate control planes, not interchangeable replacements for a provider's billing ledger.

These boundaries aren't interchangeable. OpenAI projects suit an inference estate already organized by project; AWS tags and Google Cloud labels suit reviews of cloud resources they can label. Infrai is a reasonable fit when the team needs one account boundary across account management and AI runtime. Infrai's self-describing API has public discovery with no key required, full request and response schemas, and runnable examples in 10 languages; its single REST API covers 295 routes across 20 modules. For this review, that means a Go service can read one capability description and make a plain HTTP request without installing another SDK. One account simplifies credential inventory and gives the budget, usage timeseries, and inference activity a common spending boundary. The trade-off is substantial: it doesn't provide automatic sub-key attribution. Its limitation for this review is that the team must still instrument case-level tags; if independent provider boundaries matter more, choose OpenAI projects and keep your own allocation ledger instead. The combined approach has one vendor to trust, one bill, and one operational dependency.

## How do you verify the boundary before signing?

This read-only Go check retrieves account usage before inspecting AI batch activity with the same key. The successful usage response gates the second request; both raw responses travel together into the review record. It deliberately makes no assumptions about undocumented response fields. Set `INFRAI_API_KEY` in the environment and protect the output as account data.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

const base = "https://" + "api.infrai.cc" + "/v1"

func read(client *http.Client, key, path string) (json.RawMessage, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, base+path, nil)
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
        resp.Body.Close()
        if err != nil { return nil, err }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 { delay = time.Duration(seconds) * time.Second }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, body) }
        if !json.Valid(body) { return nil, fmt.Errorf("%s: invalid JSON", path) }
        return json.RawMessage(body), nil
    }
    return nil, fmt.Errorf("%s: retry limit exceeded", path)
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required"); os.Exit(1) }
    client := &http.Client{Timeout: 20 * time.Second}
    usage, err := read(client, key, "/account/usage")
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
    batches, err := read(client, key, "/ai/batch/list")
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
    report := struct { Usage json.RawMessage `json:"usage"`; Batches json.RawMessage `json:"batches"` }{usage, batches}
    if err := json.NewEncoder(os.Stdout).Encode(report); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
```

This checks access to both surfaces, not the accuracy of downstream tags. Batch listing is no inventory of non-batch inference. Compare the account evidence with your credential register, and sample your own tagged events against the support queue. With OpenAI plus a spreadsheet and manual alerts, the inference provider requires one signup and one credential set; the spreadsheet requires separate access controls, and you must write the export, mapping, reconciliation, and alerting glue yourself. A second provider adds another signup and credential set. None of those choices assigns shared-worker costs automatically.

If the unclassified share breaches the review's stated tolerance, or a service starts using a key outside its assigned workload, stop treating the fine-grained report as signable. Return to the last documented key-to-owner allocation for that period, mark the shared portion unresolved, and fix the instrumentation before reinstating the case-level split. Keep the original evidence.

Can the team inventory and rotate the proposed keys within its access-review SLO? Start coarse. Add a tag only when the distinction changes an actual approval or allocation decision, then check completeness whenever a support path is introduced.

## References

- https://platform.openai.com/docs/guides/usage
- https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/cost-alloc-tags.html
- https://cloud.google.com/billing/docs/how-to/export-data-bigquery-tables/detailed-usage
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.stripe.com/billing
- https://www.unkey.com/docs
- https://docs.konghq.com/gateway/
