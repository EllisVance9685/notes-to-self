# Tenant Domain Limits — Enforce Many Customer Adds Using Reservation State

**Short answer:** enforce the per-tenant domain limit in the application transaction that reserves intent, then let an asynchronous publisher and reconciler make DNS converge to that state.

A per-tenant domain limit belongs in the administrative write path, but that is only the first gate. For an e-commerce console, the durable rule is to reserve a name in the tenant's own database, publish it through a reconciler, and count only records that have passed the desired-state workflow. Otherwise a retry, a deleted checkout, or a stale DNS provider response can turn a modest quota into an unreliable promise. The practical consequence is that the console can give a customer a deterministic answer even while authoritative publication is pending, and the SRE team can measure the pending state instead of guessing from resolver results. That distinction becomes important during a sales event, when a burst of domain-add requests and a slow provider response arrive together.

The quota is a database decision.

## The limit is a reservation, not a DNS observation

The failure mode is easy to miss. An operator submits `shop.example.com`, the console accepts it, and a worker has not published the record yet. A second request arrives before the worker runs. If the API counts only live DNS records, both requests can pass. If it counts rows without a uniqueness constraint, both can still pass. The customer sees one storefront; the control plane now has two claims and no clear owner.

The invariant I use is narrower: a tenant may have at most N active or reserved aliases, and every reservation has an owner, a lifecycle state, and an idempotency key. DNS is an external projection of that state. It is evidence for health checks, not the quota ledger.

This is a bounded incident pattern, not a reason to pretend that every DNS operation is synchronous. The API should reject the N+1 reservation atomically, while the publisher may remain asynchronous. That split keeps the user-facing SLO about accepted intent measurable even when propagation is outside the application team's timing control.

## How should a tenant enforce domain limits for many customer adds?

There are five useful gates, and they answer different questions. The console validates syntax and tenant authorization. The database transaction reserves capacity. The publisher verifies that the reservation is still current before writing. The reconciler compares desired and observed state. Monitoring reports age, failure, and drift. Putting all five in the provider adapter creates a race; putting all five in the browser creates a suggestion instead of a control.

| Gate | Question answered | Failure it contains |
| --- | --- | --- |
| Request validation | Is this alias acceptable for this tenant? | malformed or unauthorized input |
| Atomic reservation | Does capacity remain? | concurrent N+1 creates |
| Publication | Is this still the desired state? | stale retries and deletes |
| Reconciliation | Does observed state match intent? | drift after an external change |
| SLO monitoring | Is repair within the budget? | silent backlog and aging reservations |

The exact quota is a product policy, so the number must be configuration data with an audit trail, not a constant hidden in a worker. A useful decision record includes the counting unit: hostname, verified registrable domain, or a platform-specific alias. Those are not interchangeable.

## Can the write path stay small and still be safe?

Yes, if the transaction owns the decision and the worker owns delivery. The example below shows the shape of the boundary; `ReserveAlias` must execute in a transaction whose count and insert are serialized for the tenant. The endpoint is intentionally generic because the important contract is the state transition, not a provider API.

```go
package aliases

import (
    "context"
    "fmt"
)

type Store interface {
    ReserveAlias(ctx context.Context, tenantID, hostname, requestID string, limit int) error
}

func AddAlias(ctx context.Context, store Store, tenantID, hostname, requestID string, limit int) error {
    if tenantID == "" || hostname == "" || requestID == "" {
        return fmt.Errorf("tenant, hostname, and request ID are required")
    }
    if limit < 1 {
        return fmt.Errorf("alias limit must be positive")
    }
    return store.ReserveAlias(ctx, tenantID, hostname, requestID, limit)
}
```

The database implementation should make retries return the original reservation for the same request ID, reject a conflicting hostname according to the ownership policy, and make the quota decision under the same isolation boundary as the reservation. A queue acknowledgement is not publication success. Record both.

## What changes when the control plane is managed?

The boundary still matters even when another system publishes the record. A managed DNS service can provide authoritative storage and change APIs; it does not know whether the tenant has used its last allowed storefront alias, whether a checkout was decommissioned, or whether an old request is superseded. A self-hosted authoritative stack gives more control over reconciliation and failure injection, but transfers patching, availability, and operational ownership to the platform team.

For comparison, Amazon Route 53 exposes hosted-zone and record APIs, Cloudflare exposes zone and DNS-record APIs, and Google Cloud DNS exposes managed zones and resource-record-set APIs. Their object models and authentication boundaries differ, so an adapter should normalize intent and status rather than leak provider fields into quota logic. That is an engineering trade-off, not a ranking: the right boundary depends on who carries the authoritative-state and on-call obligations.

I would put the SLO on the internal workflow: for example, 99.9% of accepted reservations reach a terminal published or rejected state within the agreed window, with a separate alert for observed DNS drift. Do not claim that window for recursive resolver visibility unless the measurement includes the resolver population and cache behavior you actually care about.

Use the application database as the source of truth for tenant capacity, reserve before enqueueing publication, and make reconciliation delete or quarantine stale intent according to an explicit policy. If the business needs customers to add unlimited names, remove the quota policy deliberately and scale the reservation and reconciliation paths for that load; hiding an unlimited policy behind a small provider limit is worse than stating the boundary.

The five gates are useful only if their ownership is visible in dashboards and audit records. The number itself is easy. Keeping intent and published records aligned is the real system.

## Sources

- https://datatracker.ietf.org/doc/html/rfc7489
- https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html
- https://developers.cloudflare.com/api/operations/dns-records-for-a-zone-dns-records/
- https://cloud.google.com/dns/docs/reference/rest
