# Tenant Isolation for Object Storage Signed URLs and Document Image Resizing

The deletion deadline changes the architecture. For signed e-commerce documents, keep each tenant's source objects and preview images behind the same authorization boundary, generate bounded derivatives in an asynchronous worker, and ensure that no thumbnail or signed URL can outlive its source document's retention deadline.

Short answer: object storage holds private originals and deterministic thumbnails, a Go worker performs resizing after an authorized queue event, and the application issues a short-lived signed URL only after checking tenant ownership and the document's deletion deadline.

This isn't a generic image CDN pattern. A receipt preview may look harmless while still revealing a name, address, order contents, or signature, so treating derivatives as a public cache quietly defeats the private bucket. The operational target has two parts: an authorized user can retrieve a ready preview within the delivery SLO, and every byte derived from a document becomes unreachable and eligible for deletion by the explicit deadline.

## What breaks tenant isolation in private object storage image resizing?

Use the application database as the authority for tenant ownership, document state, source generation, and deletion time; use object storage for bytes. A request should never translate an untrusted tenant ID and object key directly into a signed URL. It should authenticate the caller, load the document through a tenant-scoped query, reject documents at or beyond their deletion deadline, and then locate a derivative whose key is computed by trusted code.

A useful key shape is `tenants/{tenantID}/documents/{documentID}/g/{sourceGeneration}/previews/w{width}.{format}`. The generation component matters because overwriting a source without changing the derivative namespace can serve a stale preview. The width and format must come from a small server-side allowlist; arbitrary dimensions turn one upload into an unbounded CPU and storage multiplier. On a cache miss, enqueue one job identified by tenant, document, source generation, transform policy, and deadline, then deduplicate on that complete identity. The worker rechecks the database record before reading the source and again before publishing the result, because a deletion request can race a queued resize. Consider a test fixture in which tenant `shop-17` uploads document `order-8842`, generation `3`, with deletion at `14:00:00Z`; a preview job starts at `13:59:57Z`, but a deletion transition commits at `13:59:58Z`. It doesn't matter that the JPEG encoder finished at `13:59:59Z`, or that the output key looks correct. The catalog state has invalidated the claim, so the worker must not commit the object, the signing path must reject it, and reconciliation must remove any uncommitted bytes. Now replay the fixture with tenant `shop-18` and the same document ID: every catalog lookup, object operation, queue deduplication key, and metric label must stay in the second tenant's scope. This exercise catches a more dangerous class of mistake than a broken resize: a globally unique-looking document ID that persuaded one layer to omit the tenant boundary. Keep signing separate from transformation throughout. The worker writes a private object and marks the exact generation ready; the request path then chooses a URL expiry no later than the document deadline. A signed URL is bearer access for a limited time, not proof that the holder still belongs to the tenant, so don't mint one before authorization and don't log its query string.

The deadline wins.

Tenant isolation is the primary decision axis here. A shared bucket with tenant-prefixed keys can work when every read, write, queue message, metric, and reconciliation query carries a verified tenant scope. Separate buckets can reduce the blast radius of a policy mistake, but bucket counts, policy distribution, lifecycle configuration, and reconciliation work then grow with the tenant population. Neither layout repairs an authorization bug in application code; the isolation boundary must be tested at the storage adapter and job-consumer interfaces.

| Layout | Isolation benefit | Operational cost | Prefer it when |
|---|---|---|---|
| Shared bucket, tenant prefixes | One fleet-wide policy surface; deterministic inventory partitions | A broad credential or missing prefix check has a larger blast radius | The adapter can enforce tenant scope centrally and the team continuously tests cross-tenant denial |
| Bucket per tenant | Policies and credentials can be narrowed per tenant | Provisioning, policy drift, lifecycle rollout, and inventory become fleet problems | Contractual isolation or tenant-specific retention justifies the control-plane load |
| Dedicated account or project | Stronger administrative boundary | Highest onboarding and on-call burden; aggregation becomes harder | A regulatory or enterprise boundary requires separate administration |

The catch is plain: this pattern is not suitable when the team cannot operate a queue, reconcile derived objects, or prove tenant-scoped authorization. Use a managed image transformation service when dynamic crops and many formats matter more than control of the worker, provided its privacy and deletion semantics satisfy the same deadline. Keep transformation in an audited application service when signed documents require tighter policy control than an image delivery product exposes.

## Run the deletion clock before the resizing worker

The resize call is the easy line. The difficult part is preventing an old or cross-tenant job from publishing bytes after the document has changed, while keeping retries idempotent and memory bounded under a miss burst.

The following Go core makes those boundaries visible without assuming a vendor API. `Claim` must atomically allow only the current source generation to move from pending to processing for the named tenant, and `Commit` must reject a result when the generation or deadline is no longer current. The storage implementation must apply the same tenant scope to both keys. A production decoder should also impose pixel and input-size limits before allocating the full image; those limits depend on the workload, so I'm not sure a universal number would be honest.

```go
package preview

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"io"
	"time"
)

var ErrStaleJob = errors.New("preview job is no longer current")

type Job struct {
	TenantID        string
	DocumentID      string
	SourceGeneration string
	SourceKey       string
	PreviewKey      string
	Width           int
	DeleteAt        time.Time
}

type Catalog interface {
	Claim(ctx context.Context, job Job, now time.Time) (bool, error)
	Commit(ctx context.Context, job Job, now time.Time) error
}

type Store interface {
	Open(ctx context.Context, tenantID, key string) (io.ReadCloser, error)
	PutJPEG(ctx context.Context, tenantID, key string, body io.Reader) error
	Delete(ctx context.Context, tenantID, key string) error
}

func Process(ctx context.Context, catalog Catalog, store Store, job Job, now time.Time) error {
	if job.TenantID == "" || job.DocumentID == "" || job.Width <= 0 || !now.Before(job.DeleteAt) {
		return ErrStaleJob
	}

	claimed, err := catalog.Claim(ctx, job, now)
	if err != nil {
		return fmt.Errorf("claim preview job: %w", err)
	}
	if !claimed {
		return nil
	}

	source, err := store.Open(ctx, job.TenantID, job.SourceKey)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer source.Close()

	decoded, _, err := image.Decode(source)
	if err != nil {
		return fmt.Errorf("decode source: %w", err)
	}
	preview := resizeNearest(decoded, job.Width)

	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, preview, &jpeg.Options{Quality: 82}); err != nil {
		return fmt.Errorf("encode preview: %w", err)
	}
	if !time.Now().Before(job.DeleteAt) {
		return ErrStaleJob
	}
	if err := store.PutJPEG(ctx, job.TenantID, job.PreviewKey, &encoded); err != nil {
		return fmt.Errorf("write preview: %w", err)
	}
	if err := catalog.Commit(ctx, job, time.Now()); err != nil {
		_ = store.Delete(ctx, job.TenantID, job.PreviewKey)
		return fmt.Errorf("commit preview: %w", err)
	}
	return nil
}

func resizeNearest(src image.Image, width int) *image.RGBA {
	b := src.Bounds()
	height := b.Dy() * width / b.Dx()
	dst := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			dst.Set(x, y, src.At(b.Min.X+x*b.Dx()/width, b.Min.Y+y*b.Dy()/height))
		}
	}
	return dst
}
```

Nearest-neighbor resizing keeps the example dependency-free, not visually ideal. The production adapter can use a reviewed image library while preserving this control flow: validate, claim, read through tenant scope, transform within resource limits, publish privately, and commit only if current. Don't let a library choice erase the state machine.

Order matters.

There is still a narrow race between the final deadline check and the object write. Close it in the catalog and cleanup design: `Commit` rejects stale work, the worker immediately removes an uncommitted derivative, and a deadline-driven sweeper treats both the source prefix and every derivative prefix as one retention unit. Object lifecycle rules are useful as a backstop, but their timing and minimum granularity must be verified against the explicit deletion promise rather than assumed; the application-owned deadline remains the control signal.

## Prove capacity and deletion with adversarial fixtures

Start rollout in shadow mode. Compute the expected key, generation, and tenant scope without writing a preview, then compare those decisions with the current delivery path. The first active slice should allow one width and one format, because each additional variant increases cold-generation demand and complicates proof that deletion covered every derivative.

Verification needs negative cases, not just a successful image. Tenant A must be unable to request, enqueue, sign, inventory, or delete Tenant B's document even when it knows every identifier. A job created before a source replacement must not publish into the new generation. A job finishing after the deletion deadline must leave no committed preview. A URL expiry must be bounded by both the normal delivery window and `DeleteAt`, and logs must retain only a redacted object identity rather than the signed query.

For capacity planning, measure peak unique cache misses per second, transform CPU time by source megapixels and output format, memory high-water mark, queue age, and derivative write latency. Concurrency should follow measured CPU and memory headroom, not request concurrency. Set separate objectives for ready-preview delivery and generation freshness: a cache-hit path can meet availability while the oldest queued preview exceeds its freshness objective. Watch that oldest age. It tells the truth.

Deletion deserves its own SLO and evidence trail. On deadline, stop signing first, invalidate pending jobs through catalog state, delete the source and known derivatives, then reconcile the tenant/document prefix for strays. Record object identifiers, generations, policy versions, attempt timestamps, and final disposition without retaining document content or live URLs. Lifecycle management can expire objects and clean up storage according to configured rules, but it should provide defense in depth rather than substitute for the application workflow described in the object lifecycle documentation linked below.

## Roll back the queue, never the privacy boundary

Rollback changes routing and scheduling; it doesn't make the bucket public. Stop new preview jobs, keep authorization checks in place, and route eligible requests to the last known current derivative or to an authenticated document response. Preserve source objects only until their existing deletion deadlines. No emergency flag should extend retention or mint a URL beyond `DeleteAt`.

If the new worker raises queue age or saturation, reduce admitted variant widths and worker concurrency according to the measured bottleneck, then drain only jobs whose catalog generation remains current. Reconciliation should remove uncommitted outputs before re-enabling the path. Recovery is complete when cross-tenant denial tests pass, the oldest eligible job is back within the freshness objective, and deadline reconciliation has no unexplained objects.

This design buys a clear ownership model, not a free operating model. The database owns authorization and time, the queue owns bounded asynchronous work, object storage owns private bytes, and the signing layer grants temporary delivery. If the team can't staff those boundaries and test deletion as a first-class workflow, it should choose a service whose documented controls can meet them.

## Sources

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
