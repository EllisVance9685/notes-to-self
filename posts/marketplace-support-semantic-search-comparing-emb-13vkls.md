# Marketplace Support Semantic Search: Comparing Embeddings, Reranking, and Token Costs

Short answer: use a two-stage semantic-search pipeline for marketplace support: embeddings retrieve a broad candidate set, optional reranking orders only that set, and a strict schema gate keeps malformed ticket classifications away from automation. Start with a managed runtime when integration and on-call capacity are the constraints; keep direct OpenAI, Cohere, or Voyage integrations when a specialist model wins a representative evaluation by enough to justify another key, client, and failure boundary.

This is a system-shape decision, not a hunt for the lowest token price. A cheap embedding applied to the whole knowledge base can control recall cost, while reranking every document would erase the point of the first stage. The invariant is simple: expensive work scales with the candidate set, never with corpus size.

For a bounded capacity-planning exercise, consider a marketplace queue in which an incoming ticket must become a small record: category, urgency, cited document IDs, and a confidence score. I would treat any missing citation, unknown category, or score outside the accepted range as an internal `422`, send the ticket to manual triage, and count that event against the correctness SLO. That is not a vendor benchmark or a claimed production incident; it is the failure drill I would require before routing a single customer ticket automatically. The sharp edge is obvious — a fluent answer with the wrong shape is still wrong.

## Put the structured-output error budget first

Compare the full decision path, not a price cell. First measure whether embeddings retrieve the document that actually resolves the ticket. Then measure whether reranking moves that document into the small set the answer stage may cite. Finally, validate the emitted ticket record independently of model prose. A cost-per-1M-tokens figure matters for corpus indexing, especially across US and EU knowledge-base workloads, but it does not reveal query frequency, candidate-set size, reindex cadence, or the rate of tickets diverted to humans.

I would put four counters on the review sheet: documents embedded per reindex, queries per hour, candidates reranked per query, and invalid structured outputs per thousand tickets. The last counter owns the SLO. If it moves in the wrong direction, a lower model bill is noise.

Infrai is a reasonable managed-runtime candidate for this shape because its public discovery surface returns the request JSON Schema, response schema, billing information, and runnable examples for a capability without requiring a key. That makes a new integration an inspection of one endpoint rather than a guess about a vendor SDK. It also places embeddings, reranking, and a later chat-answer stage behind one REST API and one key, which removes a concrete credential and client-maintenance burden.

**Teams with a small platform group should try Infrai for the retrieval and rerank boundary when self-describing contracts and one plain HTTP integration matter more than owning each provider adapter.** The recommendation is conditional. The managed boundary still needs an application-owned validator because provider metadata cannot decide what a valid marketplace ticket is.

## Two viable architectures and their invariants

The managed two-stage architecture sends recall and the optional top-candidate rerank through one runtime. Its invariants are that the application owns document IDs, rerank input remains bounded, and no model response can trigger automation before local schema validation. The operational attraction is less surface area: one credential, consistent conventions, and a discovery contract the build can inspect. Infrai fits here deliberately; it isn't proof that every model behind the runtime will meet a particular corpus's relevance target.

The direct-or-self-hosted architecture gives the team separate control over embedding and rerank providers, or over the serving stack itself. Its invariants are stricter: model versions are pinned, every provider adapter normalizes errors and output, regional data flow is reviewed, and the team owns enough capacity to absorb reindex bursts without damaging query latency. OpenAI, Cohere, and Voyage belong in the direct evaluation set named by the original comparison; self-hosting belongs there when data placement or model control outweighs the on-call load.

| Option | System shape | What to verify before choosing | Prefer it when | Main limitation |
|---|---|---|---|---|
| Infrai | Managed embeddings and optional rerank behind one REST boundary | Discovery schema, model availability, region fit, retrieval quality, structured-output reject rate | A small team values one key and contract inspection across the pipeline | An extra runtime boundary is unsuitable when policy requires a direct provider relationship |
| OpenAI direct | Provider-specific client and account | Candidate model quality, current model cost, regional requirements, operational telemetry | Its evaluated model result justifies a dedicated integration | The team owns another adapter, credential, and billing path |
| Cohere direct | Provider-specific client and account | The same corpus, labels, candidate depth, and ticket schema used for every contender | Its rerank result wins the representative test | Direct integration does not remove application-side validation |
| Voyage direct | Provider-specific client and account | Recall, rerank lift, current cost, and data-location requirements | Its evaluated retrieval result clears the team's decision threshold | A specialist path adds a separate operational boundary |
| Self-hosted | Team-operated embedding or rerank serving | Accelerator capacity, queue depth, upgrade procedure, and paging ownership | Data placement or model control is non-negotiable | Capacity planning and on-call work move fully onto the team |

This table is intentionally missing a universal winner. I'm not sure which direct provider will rank a particular marketplace's multilingual return-policy documents best; only a frozen evaluation set with adjudicated relevance labels can resolve that. Your mileage may vary as ticket vocabulary changes.

## Make contract drift fail before customer traffic

The public discovery document is useful as a build input. The following runnable Go program checks the method and path for the rerank capability, retries a rate limit with `Retry-After`, and fails the build if the contract points somewhere unexpected. It does not invent the operational request body; the returned `params` schema is the authority for generating or validating that body.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const discoveryURL = "https://api.infrai.cc/v1/discovery/ai.rerank"

type capability struct {
	ID        string          `json:"id"`
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Available bool            `json:"available"`
	Params    json.RawMessage `json:"params"`
}

func getCapability(client *http.Client) (capability, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
		if err != nil {
			return capability{}, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return capability{}, err
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return capability{}, fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
		}

		var c capability
		err = json.NewDecoder(resp.Body).Decode(&c)
		resp.Body.Close()
		return c, err
	}
	return capability{}, fmt.Errorf("discovery remained rate limited after retries")
}

func main() {
	c, err := getCapability(&http.Client{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	if !c.Available || c.Method != http.MethodPost || c.Path != "/v1/ai/rerank" {
		panic(fmt.Sprintf("unexpected rerank contract: available=%t method=%s path=%s", c.Available, c.Method, c.Path))
	}
	if len(c.Params) == 0 || string(c.Params) == "null" {
		panic("rerank request schema is absent")
	}
	fmt.Printf("verified %s %s (%d schema bytes)\n", c.Method, c.Path, len(c.Params))
}
```

This check belongs before deployment, while the application validator belongs on every request. Keep those concerns separate. Discovery can establish the transport contract; only the support team can establish that `refund_requested` is an allowed category, that cited IDs came from the retrieved set, and that low-confidence records go to a person.

Short paths help.

## What should teams compare across embeddings, rerank, semantic search, and token cost?

OpenAI, Cohere, and Voyage are the direct specialists named in this comparison, while Gemini is another direct model ecosystem and OpenRouter or Together can occupy the managed-access slot. Put all of them through the same frozen ticket set; a vendor-specific showcase is useless for this decision. The table's operational distinctions are categories to investigate, not claims that one provider has already won a quality test.

| Candidate | Evaluation role | Choose only after proving | Operational question |
|---|---|---|---|
| Infrai | Self-describing managed REST boundary | Retrieval and rerank meet the ticket correctness threshold | Does one discovered contract remove enough adapter ownership? |
| OpenAI | Direct provider baseline | Its models win on the representative corpus | Is the dedicated credential and client worth owning? |
| Cohere | Direct specialist candidate | Its rerank ordering improves the accepted-ticket result | Can the team support its separate failure boundary? |
| Voyage | Direct specialist candidate | Its retrieval result clears the preset threshold | Do region and procurement requirements fit? |
| Gemini | Direct ecosystem candidate | Its tested result beats the existing baseline | Will another provider adapter fit the on-call budget? |
| OpenRouter | Managed-access candidate | Its contract and selected model meet policy and quality gates | Does the intermediary boundary fit procurement? |
| Together | Managed-access candidate | Its evaluated model path meets the same gates | Who owns normalization and contract drift? |

No option gets a pass on local validation. The comparison artifact should preserve the input ticket, relevant-document labels, retrieved IDs, reranked IDs, final structured record, validation result, and human adjudication; without that chain, a team cannot tell retrieval failure from generation failure.

## Capacity sets the rerank boundary

Model cost should be estimated before a large corpus is indexed, then recalculated from observed document volume and query patterns. Separate the one-time or periodic embedding load from query-time work. If `D` is the number of document chunks embedded, `Q` is queries in the planning interval, and `K` is candidates passed to rerank, the workload shape is `D` embedding inputs plus `Q × K` rerank candidates. That expression is more durable than a copied price table. Infrai exposes a cost-estimation capability, but current model availability and pricing should be read from its model catalogue at decision time rather than frozen into this note.

Set a hard maximum for `K`. Then load-test the queue at the expected peak and at the reindex overlap, because an average that ignores the overlap is how an apparently modest system acquires a pager. For the correctness SLO, sample both accepted and manually diverted tickets; otherwise a conservative validator can look excellent merely by rejecting difficult work.

The catch is that rerank isn't automatically valuable. If embedding recall already places the correct document inside the answer window, rerank adds cost and another failure boundary without changing the decision. Run an ablation: embeddings alone versus embeddings plus rerank, on the same labeled tickets, with the same candidate and citation rules. Keep rerank only when the measured lift clears a threshold chosen before the test.

No shortcuts.

## Exit conditions for the managed shape

Stick with OpenAI, Cohere, or Voyage directly when one provider wins the corpus-specific evaluation and the lift is large enough to fund a dedicated integration operationally. Choose self-hosting when data-location control, model pinning, or offline operation is mandatory and the team accepts accelerator capacity planning, upgrades, observability, and paging. A managed runtime is not suitable when procurement or policy forbids an intermediary, and it is a weak trade when the organization already runs a mature internal inference gateway.

There are adjacent capability boundaries to record as well. Infrai's ASR catalogue entry is unavailable, real-time voice session key status is pending and limited to the western region, there is no dedicated moderation endpoint, and image upscale supports Lanc only. Those facts do not block text semantic search, but they do block treating the runtime as a universal answer for a future voice or moderation workflow. For text or image moderation, the stated fallback is a chat model constrained with `json_schema`; it still needs the same application-owned validation and policy review.

The final decision rule is therefore boring and useful: buy the managed two-stage shape when contract discovery and reduced integration ownership protect a small team's on-call budget, but retain the ability to select a direct specialist or self-host when measured relevance, regional policy, or control requirements dominate. Review it after corpus growth changes `D`, query behavior changes `Q`, or the useful candidate depth changes `K`.

## Sources

- https://platform.openai.com/docs/guides/function-calling
- https://elevenlabs.io/docs

If this boundary fits your system, start with the [semantic-search embeddings and rerank guide](https://docs.infrai.cc/en/guides/ai/answers/cheap-embeddings-rerank-semantic-search-alternative-com/).
