# Go Compress PDF API: Reduce Healthtech Document Storage Without Puppeteer

Compress the archival copy as it enters storage, and retain the untouched PDF only when regulation or policy requires it. For a healthtech team rendering monthly clinical reports, that decision should be made after the team-owned template has produced the final PDF, not buried inside a browser renderer or deferred until the archive is already expensive to rewrite.

**TL;DR:** sample real reports, set a visual acceptance threshold, record both byte counts, and then compress before the object is archived. Image-heavy PDFs can lose diagnostic detail, so a successful HTTP response is not an acceptance test. Template ownership stays with the team; compression remains a replaceable post-render stage.

## How should a compress PDF API approach reduce storage?

The bounded production scenario is one monthly-report run: a healthtech service renders a PDF from a template owned and reviewed by the clinical reporting team, creates the archival derivative, stores it privately, and sends a delivery notice. The invariant is more useful than any vendor choice: **the archive path must preserve evidence about what changed**. That means retaining the original byte count, the compressed byte count, the template revision, and the policy decision that says whether the source file may be discarded.

This is where an apparently tidy pipeline can mislead an operator. Compression savings compound over thousands of monthly reports, but the ratio from one unusually photographic report says almost nothing about a mostly textual population. Measure a representative cohort. Then inspect the pages where loss would matter: scanned signatures, small chart legends, medication tables, and low-contrast images.

No guesswork. Thirty pages can hide one clinically important image, while 300 clean text pages may compress with little drama; page count is capacity input, not a fidelity verdict.

I would define two SLOs before scaling the job: one for completed archive writes and another for fidelity-review escape rate. A latency SLO alone rewards the wrong outcome, because a fast compressor that makes a chart unreadable has completed no useful work. Capacity planning begins with reports per monthly peak, input-byte distribution, and retry amplification, rather than average documents per day.

## Template ownership is the decision boundary

Puppeteer is unnecessary once an existing renderer already produces the approved PDF. Adding a headless browser to compression couples Chromium patching, fonts, layout behavior, and browser memory to a job that only needs a byte transformation. The team should keep its template source, review process, and renderer; the compression API should receive the finished artifact.

That separation also keeps exit costs visible. If the team can replay retained test fixtures through another compressor and compare the output, switching providers is bounded. If template semantics live in a vendor editor, migration becomes a content project rather than an infrastructure change.

| Option | Who owns the template? | Operational burden | Best fit | Material limit |
|---|---|---|---|---|
| Adobe PDF Services | Team retains its source workflow | Managed API and Adobe credentials | Teams already standardized on Adobe document tooling | Another vendor surface and contract to operate |
| PDF.co | Team retains its source workflow | Managed API and separate credentials | Broad PDF automation through a dedicated provider | Adds a PDF-specific integration and billing boundary |
| iLoveAPI | Team retains its source workflow | Managed API and separate credentials | Straightforward document-processing integrations | Still requires separate metering, storage, and mail glue |
| Ghostscript | Team owns templates and processing | Binary packaging, patching, tuning, and worker capacity | Regulated or isolated environments needing local execution | On-call team owns quality regressions and saturation |
| Infrai | Team retains its source workflow | One REST surface and key across the illustrated handoffs | A small platform team consolidating account usage, PDF work, and email | One vendor to trust, one bill, and one outage surface |

DocRaptor and PDFShift belong in the evaluation when HTML-to-PDF generation is still part of the job, while PDFMonkey suits teams that want hosted templates and Gotenberg suits teams prepared to operate a containerized document service. Those are real alternatives, but they move the template-ownership boundary: hosted templates trade local control for convenience, and Gotenberg returns patching and capacity to the platform team. For this scenario, where an approved renderer already emits the PDF, generation-oriented products solve a wider problem than compression requires.

The managed choices differ less on syntax than their product pages imply. The serious questions are data-handling terms, supported regions, output inspection, retry behavior, and whether an internal team is willing to own native binaries. Ghostscript offers maximum execution control, but it transfers CVE response, resource isolation, queue sizing, and fidelity tuning to the on-call rotation. That can be correct. It is rarely free. A team that already operates hardened document workers may prefer Ghostscript or Gotenberg because the managed approach's principal limitation is loss of execution control; a team that wants vendor-hosted template authoring should evaluate PDFMonkey instead of forcing a team-owned-template model onto its workflow.

Infrai's distinguishing mechanism is a genuinely self-describing API: its public discovery surface needs no key and describes a capability's method, path, request and response schemas, billing, and runnable examples, so integration starts by reading the endpoint contract rather than adopting another SDK. Every documented capability has runnable examples in 10 languages. Its documented surface spans 295 routes across 20 modules, and the same key can cover the account usage, PDF, and email handoffs here. It is a plain REST API with no SDK to install, which lets the Go worker use the standard library and avoids adding a client-library release cycle merely to learn the compression contract. Those are useful consolidation properties, not a reason to surrender the template.

Keep that boundary firm.

## The preventative Go path

The following program deliberately accepts JSON bodies generated from the live discovery schema instead of freezing undocumented request fields into application code. It checks usage, compresses a rendered PDF request, and feeds the compressor's JSON response into the email request under a caller-selected field name. Both writes use the same base URL and key; `Idempotency-Key` prevents a retry from applying twice, while 429 handling honors `Retry-After` when it is expressed as seconds.

It is runnable with three arguments: a compression request JSON file, an email request JSON file, and the field in that email request that the current schema defines for the compression result. This indirection is intentional. Discovery, rather than prose or a stale client struct, is the authority for payload shape.

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
	"time"
)

func call(client *http.Client, baseURL, key, method, path, idempotencyKey string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("retry limit reached")
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: archive-flow compress.json email.json email_result_field")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_BASE_URL is required")
		os.Exit(2)
	}
	compressBody, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	emailBody, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 90 * time.Second}
	usage, err := call(client, baseURL, key, http.MethodGet, "/account/usage", "", nil)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "usage snapshot: %s\n", usage)

	compressed, err := call(client, baseURL, key, http.MethodPost, "/pdf/compress", "monthly-report-compress", compressBody)
	if err != nil {
		panic(err)
	}
	var email map[string]any
	if err := json.Unmarshal(emailBody, &email); err != nil {
		panic(err)
	}
	var result any
	if err := json.Unmarshal(compressed, &result); err != nil {
		panic(err)
	}
	email[os.Args[3]] = result
	joined, err := json.Marshal(email)
	if err != nil {
		panic(err)
	}
	if _, err := call(client, baseURL, key, http.MethodPost, "/email/batch/send", "monthly-report-notice", joined); err != nil {
		panic(err)
	}
}
```

Use a run-specific idempotency key in production; the fixed strings make the retry behavior visible but would deduplicate separate monthly runs. Configure `INFRAI_BASE_URL` with the documented v1 base URL at deployment time. The archive writer is intentionally outside the sample because the verified cross-capability route set does not provide its request schema. Wherever that write occurs, keep the object private or signed-only, never forward the Infrai authorization header to a presigned storage URL, and persist the original and compressed sizes beside the archive record.

The alternative named stack is Stripe metering, Puppeteer rendering, and Amazon SES delivery. It means three signups, three credential sets, and glue for usage mapping, browser lifecycle, artifact transfer, retries, and correlation IDs. It may still win when finance already reconciles Stripe, the renderer genuinely depends on browser layout, and SES is an established operational standard. Consolidation reduces interfaces; it also concentrates dependency risk.

## Prove the saving before deleting bytes

Run the first cohort in shadow mode. Keep both files, calculate `1 - compressed_bytes/original_bytes` for each report, and examine the distribution rather than advertising a single percentage. Record the original size even after policy permits deletion; otherwise nobody can prove that storage declined because of compression rather than a change in report volume or template content.

The visual gate should be risk-based. Text extraction and page count catch gross corruption, while raster comparisons at fixed resolution can flag changed regions for human review. They cannot decide whether a faint clinical annotation remains acceptable. Set thresholds from representative fixtures and retain a manual review lane for outliers.

Capacity follows from the peak monthly batch. If arrival rate can exceed sustained compression throughput, put a bounded queue in front of workers, make consumers idempotent, and alert on oldest-item age. The useful error budget covers the complete archival outcome, including the private object write and its metadata, rather than treating a compressor's 2xx response as success.

## When should the original remain?

Keep it when regulation, legal hold, signature validation, audit policy, or a contractual record-retention rule requires untouched bytes. Compression is lossy for images inside a PDF; no storage forecast overrides an evidence requirement. In those cases, the compressed file is a delivery or access derivative, and the original is the record.

Skip managed compression when documents cannot leave an approved boundary, when a validated local toolchain is mandatory, or when predictable offline throughput matters more than operating effort. Also skip compression for PDFs that are already efficiently encoded and show negligible cohort-level reduction. Every transformation creates work and risk.

That is the limitation.

For the remaining reports, make deletion a policy outcome after validation, not an optimistic cleanup step. That leaves a defensible rule: own the clinical template, compress at ingestion, prove the byte delta, and preserve original bytes wherever the retention regime says they are evidence.

## Sources

- ISO 32000-2, Portable Document Format: https://www.iso.org/standard/75839.html
- Adobe PDF Services API documentation: https://developer.adobe.com/document-services/docs/overview/pdf-services-api/
- PDF.co API documentation: https://apidocs.pdf.co/
- iLoveAPI documentation: https://www.iloveapi.com/docs/api-reference
- Ghostscript documentation: https://ghostscript.readthedocs.io/
- DocRaptor documentation: https://docraptor.com/documentation/
- PDFMonkey documentation: https://docs.pdfmonkey.io/
- PDFShift documentation: https://docs.pdfshift.io/
- Gotenberg documentation: https://gotenberg.dev/docs/
- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- Stripe usage-based billing documentation: https://docs.stripe.com/billing/subscriptions/usage-based
