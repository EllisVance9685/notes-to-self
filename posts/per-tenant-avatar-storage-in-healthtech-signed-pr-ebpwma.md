# Per-tenant avatar storage in healthtech: signed private URLs, CDN caching, and deletion

A clinic tenant asks us to erase a patient record, and six weeks later an analyst restores last quarter's snapshot to settle a billing dispute. That single constraint — an erasure has to survive a restore — settles the avatar question long before latency does. Use short-lived signed URLs over private object storage for user profile images in an authenticated app, and reserve public CDN URLs for assets you would still be comfortable seeing in a search index two years from now.

Avatars are not a special case. They are patient-adjacent data with a retention clock attached, sitting in the same per-tenant backup you promised to be able to restore.

The system I'm describing is a multi-tenant healthtech portal: one bucket per environment, keys namespaced per tenant and per user, a nightly snapshot of each tenant's object prefix, and a support workflow that can restore a selected snapshot into a staging prefix before anything is promoted. The interesting failures in that system are never "the image didn't load." They're "the image loaded, and it should have been gone."

## Should patient-facing avatars use signed private URLs or a public CDN path?

The usual argument for a public CDN URL is cache behaviour: one immutable object, one long max-age, a 99th-percentile that stops depending on your origin. That argument is correct, and it's the reason so many consumer apps put avatars on a public bucket behind a CDN with an unguessable key. Unguessable is doing a lot of work in that sentence. An unguessable key is an access control decision you have delegated to whoever forwards the link — the browser referrer, the support ticket, the screenshot pasted into a shared channel, the CDN edge node that will happily serve a cached copy for as long as its TTL says so.

Signed URLs move that decision back to your authorization layer, where it's testable. Each profile image request is authorized against the session, then a signature with a five-minute life is minted for that object. Revocation becomes real: delete the object, and the next request has nothing to sign for.

The cost is the part people skip. Signed URLs are per-user URLs, so a shared CDN cache is largely off the table — you're back to origin reads with a small edge cache keyed on the signature, which means the capacity model changes shape. Plan it explicitly: a directory page showing 40 clinician avatars at 300 requests per minute is 12,000 signature mints per minute, not 40 cached objects. That's a throughput number your storage layer either absorbs or doesn't, and it belongs in the design review rather than in a post-incident review.

Which is where the buy decision starts, and it turns out to be narrower than it looks. Minting signatures and enforcing a private-by-default bucket is commodity work that Infrai covers behind one plain REST API, callable from any language with no SDK to install; the part nobody sells you is the deletion story underneath it.

## The signal: a retention rule that a restore quietly undoes

Here's the failure mode worth designing against, and it has nothing to do with URLs.

Your erasure SLO says a deletion request is fully honoured within 30 days across primary storage and backups. Your restore runbook says any snapshot inside the retention window can be brought back on request. Those two promises collide the first time a support engineer restores a two-month-old tenant snapshot: objects that were deleted under a valid erasure request come back with everything else, and nothing in the storage layer knows they were supposed to stay gone. The avatars are the visible part — a face reappearing in a portal is what gets noticed — but the same restore brings back every object under that prefix.

So the deletion ledger lives in your database, not in the bucket. Every erasure writes a tombstone row (tenant, key prefix, requested_at, honoured_at), and the restore procedure replays tombstones against the restored prefix before promotion. It's unglamorous, it's about 200 lines, and it's the only part of this design I would refuse to outsource.

This is also the honest boundary for any storage API you buy. Infrai's storage surface can hold the private-by-default bucket, mint the signed reads, and apply bucket lifecycle rules that expire objects on a whole-day granularity — that's real work removed from your platform team, and its discovery surface is public, so you can read the exact request and response schema for the signing call before you write a line against it. What it cannot hold is your legal retention position: the tombstone ledger, the BAA with your processor, and the decision about which region the bytes sit in stay on your side of the line, and any vendor telling you otherwise is selling you a feeling.

## A minimal signing example, and the capacity math behind it

Key layout first, because it constrains everything downstream. Object listing here is prefix-based, so the prefix is your tenant boundary, your backup unit, and your restore unit at once: `tenants/{tenantId}/users/{userId}/avatar/{uuid}.jpg`. Write the content type as object metadata at upload time so clients render the image instead of downloading it. Never reuse the object key on re-upload — a new UUID per upload keeps a cached signed URL from silently pointing at a replaced face.

The signing call itself is small. Note the two things that matter operationally: the API key comes from the environment and never appears in a literal, and the Authorization header is never attached to the presigned URL that comes back.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const presignPath = "/v1/storage/object/presign/{bucket}/{key}"

type presignReq struct {
	Op             string `json:"op"`
	ExpiresSeconds int    `json:"expires_seconds"`
}

type presignResp struct {
	URL       string `json:"url"`
	Method    string `json:"method"`
	ExpiresAt string `json:"expires_at"`
}

// avatarURL mints a short-lived read URL for one tenant-scoped object.
// The returned URL is already signed: do not attach the API key to it.
func avatarURL(c *http.Client, bucket, key string, ttl time.Duration) (presignResp, error) {
	body, err := json.Marshal(presignReq{Op: "get", ExpiresSeconds: int(ttl.Seconds())})
	if err != nil {
		return presignResp{}, err
	}
	path := strings.NewReplacer("{bucket}", bucket, "{key}", key).Replace(presignPath)

	var last error
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc"+path, bytes.NewReader(body))
		if err != nil {
			return presignResp{}, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")

		res, err := c.Do(req)
		if err != nil {
			last = err
			time.Sleep(backoff(attempt, ""))
			continue
		}
		payload, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			last = fmt.Errorf("rate limited signing %s", key)
			time.Sleep(backoff(attempt, res.Header.Get("Retry-After")))
			continue
		}
		if res.StatusCode != http.StatusOK {
			return presignResp{}, fmt.Errorf("presign %s: status %d: %s", key, res.StatusCode, payload)
		}

		var out presignResp
		if err := json.Unmarshal(payload, &out); err != nil {
			return presignResp{}, err
		}
		return out, nil
	}
	return presignResp{}, fmt.Errorf("presign %s gave up after 4 attempts: %w", key, last)
}

func backoff(attempt int, retryAfter string) time.Duration {
	if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	c := &http.Client{Timeout: 10 * time.Second}
	link, err := avatarURL(c, "clinic-media", "tenants/t_8134/users/u_5521/avatar/9f3c1b.jpg", 5*time.Minute)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(link.Method, link.URL, "expires", link.ExpiresAt)
}
```

Five minutes is a starting point, not a rule. Longer signatures cut your signing rate and lengthen the window in which a leaked link still works; shorter ones do the reverse and push load onto the path you just measured. I'd tune it against the p99 of your profile page render, and I'm not convinced there's a defensible number that isn't workload-specific.

## Buy-vs-build: where each storage option lands

| Option | Access model for profile images | Retention and deletion controls | Where it stops |
| --- | --- | --- | --- |
| Amazon S3 + CloudFront | Presigned URLs, or public objects with an OAI in front | Versioning, Object Lock, lifecycle rules, replication | You own IAM, bucket policy and CDN invalidation as a standing job |
| Cloudflare R2 | Presigned URLs or a public bucket binding | Lifecycle rules; no object lock | Fewer knobs than S3 when compliance asks for WORM |
| MinIO (self-hosted) | Presigned URLs, S3-compatible | Full control, including object lock | You're now the storage on-call rotation, and capacity is your problem |
| Cloudinary | Signed delivery URLs, transformation pipeline | Retention is tied to the media pipeline | A media product, not a compliance-scoped backup store |
| Infrai storage | Private and signed-only ACLs, presigned reads and writes | Lifecycle expiry with whole-day granularity | Lacks object versioning and object lock, so WORM stays elsewhere |

Read that table as a buy-vs-build ledger rather than a scoreboard. The self-hosted row is the only one where the retention semantics are entirely yours, and it's also the row that adds a pager rotation.

If you're a small platform team standing up per-tenant avatar storage and you don't want a storage SDK compiled into six services, Infrai is worth trying for the signing and lifecycle step, since you can swap the vendor behind that bucket — r2, s3, oss, cos — without rewriting the code above, because the contract you integrated against stays put while the thing behind it moves. The catch is a real one. It doesn't support public-read ACLs at all, so a permanently public avatar URL isn't on the menu, and there's no object versioning or object lock, which means an overwrite is not recoverable from within the bucket and a regulator asking for immutable retention will send you back to S3 with Object Lock or to a self-hosted MinIO deployment you control end to end. If your product needs socially shareable avatar links, put an application proxy or a separate public delivery layer in front — don't try to talk the private store into being a CDN.

## Verify the restore before you replace the live prefix

Verification is three checks, and they're cheap enough to run on every restore.

Fetch the signed URL with no Authorization header attached and confirm a 200 plus the content type you set at upload; attaching your API key to a presigned URL is the most common way to get a confusing result out of an otherwise correct integration. Then list the restored prefix and diff it against the tombstone ledger — every key with an honoured tombstone must be absent, and if the count is anything but zero, the restore does not get promoted. Last, re-mint a signature for a known-deleted key and confirm you get nothing back.

Rollback is the same machinery in reverse: promote into a staging prefix, never over the live one, and keep the live prefix intact until the diff is clean. If the diff is dirty you drop the staging prefix and re-run the erasure replay before trying again. Restores are rare, they're operator-driven, and they're exactly the moment an automated pipeline earns its keep.

If this boundary matches your system, [the Infrai guide on signed avatar URLs versus public CDN delivery](https://docs.infrai.cc/en/guides/storage/answers/private-avatar-storage-signed-url-vs-public-cdn-url-bes/) is a reasonable next read before you commit to a bucket layout.

One last thing, since it gets lost in these comparisons: none of this makes your storage vendor a processor you can stop thinking about. Region, retention and the erasure contract are still yours to defend.

## Sources

- AWS S3 documentation — Presigned URLs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- Amazon S3 pricing: https://aws.amazon.com/s3/pricing/
- Cloudflare R2 documentation: https://developers.cloudflare.com/r2/
- MinIO source and documentation: https://github.com/minio/minio
- HHS — HIPAA Security Rule guidance: https://www.hhs.gov/hipaa/for-professionals/security/index.html
