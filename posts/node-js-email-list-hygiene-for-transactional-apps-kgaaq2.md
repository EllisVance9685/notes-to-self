# Node.js Email List Hygiene for Transactional Apps with Suppression Table Sync

Short answer: a Node.js transactional app should check a local suppression table in the same decision path that queues each marketplace order email, while a separate idempotent poller imports bounce and unsubscribe events, advances a durable cursor, and alerts on synchronization lag before sellers can be repeatedly mailed after opting out or hard bouncing.

The page is blunt: `order_email_acceptance` has fallen below its SLO, and the on-call view shows new marketplace orders whose email notification never reached the accepted state. That page is late. By the time an order alert misses its objective, the useful earlier signals were the age of the suppression-event cursor, the count of unmapped event types, and the interval between an unsubscribe event and the corresponding local row becoming enforceable.

This is a consistency problem disguised as an email feature. Treating it as a nightly “list hygiene” batch leaves a gap in which the application can keep selecting an address that has already produced a hard bounce or an unsubscribe. Treating every send as a synchronous call to an external suppression API moves a deliverability control into the order checkout failure domain. Neither extreme gives an on-call engineer a clean place to set an SLO.

## What should a Node.js transactional app monitor before bounced users are retried?

Split collection from enforcement. The event collector polls the email event source, normalizes only states the team has deliberately mapped, and upserts a local record keyed by a canonical recipient identifier. The Node.js order-notification path reads that local state immediately before enqueueing the email. The queue consumer checks it again immediately before submission because a seller can unsubscribe after the order event is created but before a worker handles it. Don't assume that a queue preserves the eligibility decision made at enqueue time; it preserves a task, and the facts around that task can change. A short delay is enough to cross the unsubscribe boundary.

Check twice.

Use monotonic state transitions. A late “delivered” event must not erase a later unsubscribe, and replaying the same hard-bounce event must have no additional effect. Store the source event ID for deduplication, the source event time for ordering, the normalized reason, and the ingestion time for lag measurement. If an event type is unknown, retain it for inspection and increment a metric, but don't guess that it means suppressed or active. That uncertainty is operationally useful: the mapping should change only after the source's event contract resolves it.

The application still needs a policy distinction. A seller's marketing unsubscribe may not apply to a strictly transactional new-order notice, depending on the consent model and applicable rules, while a hard bounce says that sending to that address is futile regardless of message category. Encode that distinction as data, not as a conditional scattered through handlers. The resulting row is an auditable policy decision: it records which category is blocked, why it is blocked, which source event caused the transition, when that event occurred, and when the application learned about it. That is more useful during an incident than a boolean named `do_not_email`, which cannot explain whether the writer meant consent, address validity, a temporary operational choice, or every message category forever.

## The alert arrived one dependency too late

Start at the customer-visible objective: for an eligible seller, an order notification should enter the email provider's accepted state within the chosen latency budget. “Eligible” is part of the denominator. Suppressed recipients should produce an intentional `suppressed` terminal state, not inflate a failed-delivery counter and not disappear from telemetry.

Then walk backward. The worker can submit only if the queue is moving and its last suppression check returns eligible. That check can be trusted only while the local table is fresh. Freshness depends on the poller advancing its cursor and on every fetched event reaching a committed row. The earliest actionable alert is therefore not a rising bounce count; it is suppression-sync lag approaching the maximum delay allowed by the team's risk budget. For capacity planning, make the arithmetic explicit. If the marketplace peaks at an assumed 50 order events per second and the poll interval is 10 seconds, a worker may face 500 order candidates between polls. That is an input assumption, not a benchmark. The useful question is whether the business accepts that exposure window; if it doesn't, shorten the interval, consume pushed events, or block the send path on a more current source. A one-minute poll may be cheap to operate yet plainly incompatible with a ten-second suppression-freshness objective. Instrument four points: event-source watermark, committed local watermark, suppression decision, and provider submission result. Record counts and latency histograms by normalized reason, but keep recipient addresses out of metric labels because high-cardinality personal data makes the monitoring system expensive and turns routine dashboards into a privacy liability. The alert should compare age with a budget, not fire merely because one poll returned no events. Empty polls are normal. Page when the last successfully committed source watermark is old enough to threaten the suppression SLO; open a lower-urgency ticket for unknown event mappings or a slowly growing dead-letter set. This separation keeps an informational anomaly from waking someone while preserving evidence for contract drift.

Lag is the signal.

## Why must the event cursor move in the same database commit?

The collector's critical invariant is compact: never advance the durable cursor past an event that has not been reflected in the suppression table or deliberately quarantined. The source may return duplicate events, events with the same timestamp, or pages that are retried after a process restart. Cursor state therefore needs the source's stable pagination token or a compound position defined by that source; a bare timestamp is unsafe when several events can share it.

The following Go sketch shows the transaction boundary even when the marketplace application itself is Node.js. It assumes the event adapter has already translated a source-specific payload into a small internal contract. The Node.js service and this poller share the same database schema and suppression policy; keeping the adapter out of the send handler limits integration effort and gives replay one owner.

```go
package suppression

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Event struct {
	ID        string
	Recipient string
	Kind      string
	Occurred  time.Time
}

type Page struct {
	Events     []Event
	NextCursor string
}

func ApplyPage(ctx context.Context, db *sql.DB, page Page) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, event := range page.Events {
		reason, suppress := normalize(event.Kind)
		if !suppress {
			return fmt.Errorf("unmapped suppression event %q", event.Kind)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO email_suppressions
				(recipient, reason, source_event_id, source_occurred_at, ingested_at)
			VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
			ON CONFLICT (recipient) DO UPDATE SET
				reason = EXCLUDED.reason,
				source_event_id = EXCLUDED.source_event_id,
				source_occurred_at = EXCLUDED.source_occurred_at,
				ingested_at = EXCLUDED.ingested_at
			WHERE email_suppressions.source_occurred_at < EXCLUDED.source_occurred_at`,
			event.Recipient, reason, event.ID, event.Occurred)
		if err != nil {
			return err
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE ingestion_cursors
		SET cursor = $1, committed_at = CURRENT_TIMESTAMP
		WHERE stream = 'email-suppressions'`, page.NextCursor)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func normalize(kind string) (string, bool) {
	switch kind {
	case "hard_bounce":
		return "hard_bounce", true
	case "unsubscribe":
		return "unsubscribe", true
	default:
		return "", false
	}
}
```

Production code should also deduplicate on source event ID and normalize recipient identity according to an explicit policy. Be conservative with email-address rewriting: case folding and provider-specific aliases can merge identities the business considers distinct. The safest normalization rule is the narrowest one supported by the application's account model. Also make replay a normal operator action, not an emergency script: it should accept a bounded cursor range, emit the same metrics as live ingestion, and preserve monotonic transitions. A replay that follows different code paths is a second implementation of the hardest invariant and will age badly.

There is a deliberate hard stop on an unknown event in this sketch. A different team might quarantine the event and continue, but that decision changes the failure mode: stopping favors correctness and visible lag; quarantining favors throughput and requires a tightly monitored review path. I'm not sure one policy fits every event contract. The deciding evidence is the source's ordering guarantee and whether later events can be interpreted safely after an unknown predecessor.

## How can a test expose the race between policy and queued orders?

An end-to-end test should create an eligible seller, queue a new-order notification, apply an unsubscribe before the worker submits it, and assert an intentional suppressed outcome with no submission attempt. Repeat the test with a hard bounce, a duplicate event, an older event arriving after a newer one, a restart after the page is fetched but before commit, and two events sharing a timestamp. These cases exercise the consistency boundary; a test that only confirms JSON decoding does not. Run the same suite against every event adapter because normalization is where source-specific vocabulary becomes application policy, then run the worker test without a network dependency so a provider sandbox cannot hide a local race. The deploy check is equally concrete: migrate the table before starting the collector, start the collector before enforcing the read, observe lag until the cursor is current, and only then enable suppression as a send gate. Reversing that order can classify every seller against an empty or stale table even though each component passes its isolated test.

Race it on purpose.

Open tracking is a poor substitute for this outcome model. Apple's Mail Privacy Protection can prevent senders from learning whether a recipient opened a message, so an “open rate dropped” page cannot reliably diagnose a suppression-sync problem. Measure the states the system owns: event ingestion, policy decision, queue progress, and submission acceptance. Delivery beyond that boundary needs separate signals and careful language.

Google's sender guidance also makes authentication, wanted mail, and easy unsubscription part of deliverability practice. A clean local table does not compensate for weak authentication or mail recipients did not request. List hygiene is one control in a larger system.

## A freshness budget chooses the operating model

Integration effort is more than the first adapter. It includes schema migrations, replay tooling, event-contract review, dashboards, privacy handling, and the on-call path when the cursor ages. The buy-vs-build decision should assign those responsibilities explicitly. Start with the maximum acceptable time between a source-side suppression event and local enforcement, then eliminate models that cannot meet it without making the order path less reliable. Only after that should implementation estimates decide among the surviving options; otherwise a low initial estimate can disguise a permanent mismatch between the design and the deliverability objective.

| Operating model | Integration work | Failure ownership | Best fit | Not suitable when |
| --- | --- | --- | --- | --- |
| Poll source events into a local table | Adapter, cursor, replay, schema, and alerts | Application team owns freshness and correctness | The send path needs a fast local decision and the team can operate ingestion | The event source cannot provide a durable ordering or cursor |
| Consume pushed events into a local table | Receiver authentication, deduplication, replay, and alerts | Application team owns receiver availability and correctness | The suppression window must be shorter than a polling interval | The source cannot replay missed deliveries |
| Query a remote suppression service before each send | Thin local integration plus timeout policy | Shared with the remote service | Traffic is low and current remote state matters more than isolation | Checkout or order processing cannot tolerate that dependency |
| Run a self-hosted mail and suppression stack | Full mail operations, abuse controls, storage, and on-call | Internal platform team | Regulatory or control requirements justify dedicated ownership | The team lacks specialist deliverability capacity |

The catch is that the local-table design creates bounded staleness. Stick with a synchronous decision when even the smallest practical poll or push delay exceeds the policy window, and choose pushed ingestion when the source offers authenticated delivery plus replay. Self-hosting can reduce one form of vendor dependence, but it also transfers mail reputation, abuse handling, and a much larger on-call surface to the platform team. Lock-in has several shapes.

Finally, tune the alert against both risk and human cost. A threshold comfortably below the business's suppression window leaves time to act, but a threshold below ordinary event and commit jitter produces pages with no customer action attached. Those false positives train responders to distrust the only early warning this architecture has. The right threshold comes from observed lag percentiles, the documented event contract, and the accepted exposure window; until those measurements exist, label the initial value as provisional and review it rather than presenting it as an SLO backed by evidence.

## References

- Google: Email sender guidelines — https://support.google.com/a/answer/81126
- Apple: Mail Privacy Protection guide — https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
