# Tenant Archives: Large Image Uploads with Multipart Node.js and Sharp Thumbnail Pipeline

Short answer: for a property-management system handling large tenant photos, make the completed private object the only input to Sharp, and optimize throughput around that boundary rather than resizing inside the multipart upload loop. Keep the original immutable by convention, write thumbnails to separate keys, and measure completion-to-thumbnail availability as its own SLO.

That decision sounds narrow until a move-in inspection produces hundreds of high-resolution images while several buildings share the same upload workers. A thumbnail that appears before the original is durable is not a faster result; it is an ambiguous result that complicates retries, audit trails, and restore operations.

## The failure mode: transport progress is not image readiness

The incident pattern is familiar in shape, even when the details differ: a client sends several multipart parts, loses its connection, and leaves an upload session open. A worker watching part activity starts a resize job, but there is no complete object for the decoder to consume. A second retry then creates another job, and an operator sees a thumbnail with no trustworthy relationship to the original. In a property-management archive, that ambiguity reaches beyond an image grid: a leasing or maintenance workflow may attach the derivative to the wrong inspection record, while the original remains a set of unowned parts. The repair is not a clever Sharp option. The repair is to make the asset state explicit, make the completion operation the only event that can advance it, and give the cleanup worker enough metadata to distinguish a slow upload from an abandoned one without guessing from object-listing results.

The invariant is simple: **thumbnail eligibility begins after multipart completion succeeds**. Accepted parts describe a transport session. A completed object is the pipeline input. Those states must have different names in the database and different metrics in the dashboard.

Abandoned sessions are also a capacity problem. Budget three byte pools: completed originals, generated derivatives, and parts belonging to uploads that never reached completion. Track opened, completed, and explicitly aborted sessions separately, then alert on the age and bytes of the open pool. A lifecycle policy may help clean up incomplete parts, but the service that owns the upload still needs a deadline and an abort path; otherwise a tenant who closes a browser tab can leave storage capacity assigned to a forgotten session.

Short boundary. Big consequence.

No shortcut.

## How should a Node.js Sharp pipeline handle private tenant photos?

The application should create the multipart upload, transfer parts with bounded concurrency, retain each successful part result, and call completion exactly once for the logical asset. Only after that operation succeeds should it enqueue a thumbnail job containing the asset ID, source key, target key, and transform version. The worker reads the completed source, passes it to Sharp, and writes a derivative such as `derivatives/320/<asset-id>.jpg` without changing the original key.

The browser does not need a public bucket to display the result. Keep both original and derivative objects private, and issue short-lived presigned read URLs after authorization has checked the tenant and property relationship. OWASP's upload guidance still matters before the worker accepts bytes: constrain size, validate the detected file type, use generated storage names, and keep user-controlled files away from a trusted executable or public-serving path. Those controls reduce exposure; they do not replace the completion gate.

Node.js and Sharp are implementation choices, not state transitions. Put Sharp in a worker that consumes completion-triggered jobs, where memory limits and concurrency can be tuned independently of request handling. At-least-once delivery is workable when the job is deterministic, targets a stable derivative key, and carries a transform version. If two workers can produce different bytes for the same key, coordinate that ownership in a queue or database instead of assuming object storage provides a lock.

For large files, measure the things that affect the real decision: part size, upload concurrency, connection interruption rate, source-read throughput, Sharp memory per image, queue delay, and derivative write latency. I'm not sure any generic part-size recommendation survives a different camera mix or network shape; a representative sample of tenant images and a defined completion-to-thumbnail SLO should settle it.

## A small controller makes the gate explicit

The following Go example represents the control-plane decision without inventing a provider-specific API. The same rule belongs in the Node.js service: no resize job for an open or aborted upload, and no derivative that aliases its source.

```go
package main

import (
	"errors"
	"fmt"
)

type uploadState string

const (
	stateOpen      uploadState = "open"
	stateCompleted uploadState = "completed"
	stateAborted   uploadState = "aborted"
)

type asset struct {
	id            string
	state         uploadState
	originalKey   string
	thumbnailKey  string
}

type resizeJob struct {
	id        string
	sourceKey string
	targetKey string
}

func makeResizeJob(a asset) (resizeJob, error) {
	if a.state != stateCompleted {
		return resizeJob{}, fmt.Errorf("asset %s is %s", a.id, a.state)
	}
	if a.originalKey == a.thumbnailKey {
		return resizeJob{}, errors.New("source and derivative keys must differ")
	}

	return resizeJob{
		id:        "thumbnail:" + a.id + ":320:v1",
		sourceKey: a.originalKey,
		targetKey: a.thumbnailKey,
	}, nil
}

func main() {
	a := asset{
		id:           "inspection-42",
		state:        stateCompleted,
		originalKey:  "originals/inspection-42.jpg",
		thumbnailKey: "derivatives/320/inspection-42.jpg",
	}

	job, err := makeResizeJob(a)
	if err != nil {
		panic(err)
	}
	fmt.Printf("enqueue %s: %s -> %s\n", job.id, job.sourceKey, job.targetKey)
}
```

In production, persist the state transition and the enqueue intent so a process restart cannot leave a completed asset without work or create unbounded duplicate jobs. Make the worker idempotent by deriving its job ID from the asset and transform version. Retries need backoff and a bounded attempt count; a retry storm is a capacity incident wearing an error-handling costume. The worker should also reject an unexpectedly large decoded image before Sharp consumes unbounded memory, with limits chosen from the actual image corpus and worker budget.

## Buy versus build is an ownership decision

The storage choice should follow the control you need to own. A direct object-storage account keeps provider settings and data-plane behavior explicit. A self-hosted S3-compatible layer can fit an organization that already operates stateful storage, but it adds upgrades, durability planning, replication, and another on-call surface. A managed abstraction can reduce credential and operational sprawl, but it may not expose every provider feature your retention or migration policy requires.

| Option | Strong fit | The catch |
| --- | --- | --- |
| Direct object storage | A team wants the provider's native controls and account boundary | Provider-specific credentials, policies, and billing remain part of platform ownership |
| Self-hosted S3-compatible storage | Storage operations are an intentional internal capability | The team owns durability, upgrades, capacity, and recovery testing |
| Managed storage abstraction | The application benefits from one consistent HTTP integration across backends | Missing controls, migration limits, or opaque provider behavior can matter more than integration effort |

This is not a leaderboard. Stick with direct storage when object lock, versioning, cross-region recovery, browser CORS policy, or a particular compliance control is non-negotiable. Choose a managed boundary only after verifying those requirements, the supported regions, the egress model, and the restore workflow. Your mileage may vary because a photo archive's operational risk is shaped as much by retention and recovery objectives as by upload throughput.

## The review checklist ends at restore, not upload

Before shipping, test an interrupted part transfer, an expired presigned URL, a client retry after completion, a duplicate completion notification, a corrupt image, and a worker restart after the source is available. Confirm that an authorized user can restore the selected original and its expected derivatives without making the bucket public.

Then review the SLO dashboard: upload completion latency, open-session age, abandoned-part bytes, completion-to-worker-start latency, completion-to-thumbnail availability, and restore success rate. If the system can resize an object before its commit boundary, the dashboard is already telling you the wrong story.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- https://docs.digitalocean.com/products/spaces/
