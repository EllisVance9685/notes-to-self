# Edtech Throughput: Object Storage for SaaS User Documents, Private Files, Signed Links

Short answer: for an edtech SaaS storing large training artifacts, keep objects private, authorize every access in the application, issue short-lived signed links, and choose a storage backend only after its throughput, retention, recovery, and browser path pass the same workload test.

No shortcut.

The cheapest storage rate is a weak decision rule. A platform team still owns tenant isolation, document indexing, deletion, recovery, and the pager when a learner cannot fetch a course recording. I would first set the service objectives, then reject any design that cannot meet them, and only then compare S3, Cloudflare R2, DigitalOcean Spaces, Backblaze B2, or a self-hosted alternative.

The budget is the decision.

## The authorization boundary for training artifacts

Start with one invariant: a database authorization decision grants access; possession of an object URL does not. The request flow should authenticate the user, look up the tenant and artifact record, authorize that particular object, and return a narrowly scoped signed URL. The bucket or container stays private while the storage service transfers the bytes directly instead of making the application process every large download.

The object key should not be the policy. A document record needs, at minimum, the tenant identifier, owner, object key, content type, logical state, and retention decision. That record gives the application one place to enforce authorization before it signs a download. It also supports a deletion job that can distinguish an expired training artifact from an object covered by a legal hold or an active course.

Presigned URLs are temporary bearer credentials. AWS describes them as granting time-limited access to a specific object operation, so their lifetime and scope belong in the threat model rather than in a UI convenience setting. Never put a permanent application credential in a browser request. The browser gets the signed URL after the application has made its decision, and the storage request carries the signature rather than a broad platform token.

Browser-direct uploads add a different concern. CORS is enforced by browsers, and it is not authentication. Test the exact production origin, method, and headers against the chosen endpoint; a Go client succeeding on the server proves nothing about a browser upload. For an artifact upload, the application should authorize the intended tenant and object key first, issue a scoped upload URL, and mark the database record complete only after it verifies that the write finished.

Capacity planning needs more than monthly gigabytes. Record object count, median and tail object size, concurrent uploads, download requests per second, retry behavior, monthly bytes retrieved, and the burst caused by a class-wide export. For large training videos, the tail size and concurrent range or multipart activity usually matter more than the average file. A useful SLO set includes signed-link issuance latency, successful download rate, upload completion rate, and recovery time for accidental deletion.

## The retention failure that changes the design

Consider a bounded production scenario: a deploy accidentally writes a new revision over 10,000 training statements, the mistake is detected 30 minutes later, and support needs the original bytes restored within the recovery objective. A signed URL does not restore data. A private access policy does not prevent an overwrite. The relevant question is which independent control can recover the prior object.

That is the invariant. Authorization, write concurrency, and recoverability are separate controls.

If the storage contract has object versioning, test how versions are listed, retained, restored, and eventually deleted. If it has object lock, test the retention mode and the authority that can change it. If neither is available, use unique object keys and a database pointer to reduce overwrite risk, then provide an external backup and a restore drill for deletion recovery. A compliance requirement for immutable retention should reject a design whose only promise is “we have backups.”

The test must include an expired-link request, a cross-tenant download denial, two writers competing for the same logical revision, and a restore measured against the recovery time objective. The right number depends on support and compliance requirements; I'm not sure what RTO is defensible for a particular school until those owners state the consequence of lost access. The uncertainty is a reason to run the exercise, not a reason to omit it.

Retention also has a clock granularity problem. A lifecycle rule that removes objects after one day does not implement hourly expiry. Multipart uploads need cleanup as well, because abandoned parts can consume capacity even when no completed object is visible. The policy should therefore name completed objects, incomplete uploads, backups, legal holds, and the operator responsible for each transition.

## How should SaaS object storage handle private files and large-file throughput?

Put AWS S3, Cloudflare R2, DigitalOcean Spaces, and Backblaze B2 through one evidence sheet rather than comparing isolated price pages. The shortlist comes from the reader's question; the approval criteria come from the workload. A provider that handles a small test file tells us very little about a 99th-percentile training artifact under an export burst.

| Candidate path | Test conditions | Work that remains with the platform team |
| --- | --- | --- |
| Managed object storage service | Private access, signed upload and download, large-file concurrency, lifecycle behavior, recovery controls, CORS, and egress trace | Authorization, tenant index, retention state machine, restore drill, and migration plan |
| Several managed services evaluated side by side | Identical object corpus, request trace, regions, retry policy, and SLO thresholds | Normalized capacity model, evidence date, exit criteria, and operational runbook |
| Self-hosted object storage | Failure-domain capacity, upgrade rehearsal, node loss, repair time, backup restore, and staffing coverage | Data plane, patching, on-call, capacity reserve, and every failure test |

The cost model should include stored bytes, request counts, retrieved bytes, egress destination, multipart overhead, backup copies, support, and the engineering time required to operate the system. I would keep the object corpus representative: tiny PDFs, medium assignments, and the largest training media, with realistic tenant skew. Run the same trace against each candidate and record p50 and p99 transfer behavior, failed requests, retries, and recovery timing.

This is where the roadmap gets honest. Managed storage buys time and a defined service boundary, while self-hosting can make residency or integration constraints easier to satisfy but adds capacity reserve and operational ownership. Neither choice removes the application team's responsibility for policy. Your mileage may vary when learners download across regions, because the network path can dominate a storage calculation that looked attractive on a spreadsheet.

Measure it.

## Go code for the storage data path

The following client accepts a URL that the application has already authorized and signed. It checks response status, bounds error output, and honors `Retry-After` for a throttled request. It does not add an application bearer token to the storage request.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	signedURL := os.Getenv("SIGNED_DOWNLOAD_URL")
	if signedURL == "" {
		fmt.Fprintln(os.Stderr, "SIGNED_DOWNLOAD_URL is required")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	var lastErr error

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, signedURL, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				fmt.Fprintln(os.Stderr, ctx.Err())
				os.Exit(1)
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			fmt.Fprintf(os.Stderr, "download failed: status=%d body=%q\n", resp.StatusCode, body)
			os.Exit(1)
		}

		if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
			resp.Body.Close()
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		resp.Body.Close()
		return
	}

	fmt.Fprintf(os.Stderr, "download failed after retries: %v\n", lastErr)
	os.Exit(1)
}
```

Production code still needs a maximum object size, content-type policy, checksum handling, audit events, and a download stream that does not buffer a large artifact in memory. The storage transfer is only one leg of the operation. The application must also record who requested the link, which tenant was authorized, when the link expires, and whether the final download met the SLO.

## Where is this storage choice a bad fit?

The catch is that a managed service with a narrow abstraction can hide controls the workload eventually needs. Use a directly evaluated managed service when the workload needs native retention controls, object versioning, immutable retention, cross-region recovery, browser-upload configuration, metadata queries beyond prefix listing, or a provider-specific transfer feature. Those requirements are valid reasons to accept a broader API surface and a provider adapter.

Choose self-hosting only when hard residency, integration, or control requirements justify owning capacity, upgrades, repair procedures, and round-the-clock response. That is a legitimate outcome, but it needs a staffing model and a restore rehearsal before the system carries learner data. This is not suitable when the team cannot reserve capacity or cover repair work; stick with a managed service when those operational duties would weaken the SLO.

The recommendation changes again when the objects are public media, static-site assets, or permanent share links. Private signed access is the wrong default for those cases. Conversely, a compliance workload should not proceed until the retention authority, deletion exception, backup scope, and measured restore time are written down.

The durable decision is not a vendor name. It is a tested contract: private objects, application authorization, scoped temporary links, predictable large-file transfer, explicit retention transitions, and a recovery path that has been timed. Record the assumptions and evidence date so a later migration is an engineering exercise rather than an emergency rewrite.

## References

- [MDN: Cross-Origin Resource Sharing](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS)
- [AWS S3: Presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)
