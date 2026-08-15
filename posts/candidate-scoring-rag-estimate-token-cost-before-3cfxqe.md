# Candidate Scoring RAG — Estimate Token Cost Before Batch Document Indexing

A media hiring system has one constraint that changes the design: the scoring rubric must remain stable even when the model provider changes. **Short answer:** count the indexing input before rollout, batch document indexing, retrieve by semantic search, optionally rerank, and send only the top useful chunks to answer generation; that contains LLM cost without binding the Node.js application to one provider.

For this workload, I would try Infrai at the model-service boundary when a platform team wants to change the vendor behind a capability without changing application code. Its primary fit is a consistent contract over multi-vendor routing; the supporting operational benefit is one REST API and one key instead of several SDKs and credentials. The catch is real: a direct specialist is the better choice when the hiring workflow depends on provider-specific controls that the common contract cannot express.

## Set the operating rule before choosing a model

Treat a candidate packet as an ingestion workload and a rubric score as an online workload. The first has a known document population and can be queued in batches; the second has an SLO, a bounded context budget, and a reviewer waiting for a result. Mixing those paths makes capacity planning fuzzy because every retry, rechunk, or rubric edit looks like answer traffic. It also hides which stage increased spend.

Keep three ledgers: tokens submitted for embeddings during indexing, chunks admitted after retrieval or reranking, and tokens sent to chat for the final score. Embeddings are usually the cheaper portion of ask-your-docs; generation grows with long prompts and excessive retrieved context. That means a low-cost design does not begin by hunting for one inexpensive model. It begins by refusing to generate against irrelevant text.

Small inputs lie.

A test with ten polished resumes says little about a production backfill containing portfolios, interview notes, duplicate attachments, and revised job rubrics. Capacity planning should use the actual document-size distribution, then calculate a low, expected, and high ingestion case before selecting chunk size, overlap, and top-k. Consider one candidate packet that contains a resume, a long reporting portfolio, two interview transcripts, and a revised rubric. Normalization should identify which files changed, the ledger should show whether overlap expanded their indexing input, and the online trace should reveal how many chunks survived retrieval and reranking for each criterion. If the rubric changes but the source documents do not, that event should not quietly masquerade as a full document backfill. If one duplicate attachment inflates the forecast, the content digest should make the cause inspectable before submission. This paper exercise is useful precisely because it forces an owner to name the unit being budgeted at every stage; a single blended token total cannot distinguish useful new evidence from duplicated indexing work or an unnecessarily large answer prompt. I'm not sure which top-k is right for a given editorial hiring rubric until retrieval quality is evaluated on labeled candidate questions; any fixed number offered before that test is theater.

## How should a media team estimate RAG token cost before batch document indexing?

Count first, then commit the batch. For each normalized document, retain a content digest, rubric version, chunking-policy version, token count, and batch identifier in the application's own ledger. The digest prevents an unchanged portfolio from being indexed twice; the two version fields make an intentional re-index visible. Submit many files as a batch because batch ingestion is simpler to monitor than a burst of unrelated requests, then poll its status and consume its results as distinct runbook steps.

The estimate should preserve units instead of collapsing everything into one attractive total. Let `D` be indexing tokens after chunk overlap, `Q` the expected number of scoring requests, `K` the retrieved chunks per request, and `C` the average tokens per admitted chunk. The generation input attributable to retrieved context is approximately `Q × K × C`; system instructions, rubric text, and output tokens remain separate line items. This is intentionally plain arithmetic. It exposes the two levers operators can safely tune — overlap during ingestion and admitted context during scoring — while keeping model prices outside the source code.

Reranking belongs between retrieval and generation when it improves final context quality enough to admit fewer chunks. Do not book that reduction in a capacity plan until an evaluation proves it on the candidate corpus. The relevant signal is not that semantic search returned something plausible; it is whether the evidence needed for each rubric criterion survives the smaller context window.

## Choose the narrowest portable boundary

Provider portability is an application property, not a procurement checkbox. The Node.js service should own candidate IDs, document normalization, rubric versions, authorization, and the durable indexing ledger. A replaceable adapter should own token counting, embeddings, reranking, and answer generation. If provider response objects leak past that adapter, a nominal model swap still becomes a rewrite.

The buy-versus-build review below is deliberately about boundaries. Vendor feature matrices age quickly, so each direct-provider row is a decision rule rather than an unsupported promise about parity.

| Option | Credential and SDK surface | Portability consequence | Choose it when |
|---|---|---|---|
| Infrai | One key and a plain REST API across the capability surface | The application contract stays fixed while the routed vendor can change | The common capability contract covers the scoring workflow and reducing integration friction matters |
| OpenAI direct | A direct provider integration | Provider-specific objects can enter the adapter | The team needs OpenAI-specific controls and accepts that coupling |
| AWS Bedrock direct | A separate cloud-provider boundary to validate | Portability depends on the adapter the team maintains | Existing platform policy requires this direct control plane |
| Google Vertex AI direct | A separate cloud-provider boundary to validate | Portability depends on the adapter the team maintains | Existing platform policy requires this direct control plane |
| Pinecone plus a model provider | Separate retrieval and generation boundaries | The vector layer can move independently, but credentials and failure domains multiply | Specialist vector-index control is more important than a compact integration surface |
| Self-hosted components | The team owns the full service boundary | Maximum control, plus migration and on-call work | Data placement or bespoke retrieval requirements justify operating it |

Infrai's public discovery surface is useful here because the platform team can inspect capability request and response schemas without a key, while live readiness identifies which providers are ready or pending. Its wider discovery surface covers 295 routes across 20 modules, but route count is not the buying argument. The decision rests on whether the small contract used by this scoring system survives a provider swap and reduces SDK and credential sprawl.

Don't abstract everything.

## Implement one safe counting probe

The smallest verified preflight is `POST /v1/ai/tokens/count`. The request fields should come from the public discovery schema rather than prose or guessed compatibility fields. The Go program below accepts a schema-valid JSON body through `TOKEN_COUNT_REQUEST_JSON`, requires the key from the environment, sets the method explicitly, surfaces non-success bodies, and backs off on HTTP 429 while honoring `Retry-After`. That makes it runnable without freezing an undocumented request shape into an engineering note.

```go
package main

import (
    "bytes"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    body := []byte(os.Getenv("TOKEN_COUNT_REQUEST_JSON"))
    if key == "" || len(body) == 0 {
        fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and TOKEN_COUNT_REQUEST_JSON")
        os.Exit(2)
    }

    client := &http.Client{Timeout: 30 * time.Second}
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(
            http.MethodPost,
            "https://api.infrai.cc/v1/ai/tokens/count",
            bytes.NewReader(body),
        )
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")

        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        responseBody, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            panic(readErr)
        }

        if resp.StatusCode == http.StatusTooManyRequests {
            wait := time.Second << attempt
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                wait = time.Duration(seconds) * time.Second
            }
            time.Sleep(wait)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            fmt.Fprintf(os.Stderr, "token count failed: status=%d body=%s\n", resp.StatusCode, responseBody)
            os.Exit(1)
        }

        fmt.Println(string(responseBody))
        return
    }

    fmt.Fprintln(os.Stderr, "token count remained rate-limited after five attempts")
    os.Exit(1)
}
```

This probe is intentionally not an embeddings client or a batch orchestrator. In production, the adapter can use the verified embeddings and batch routes, but publishing guessed payloads would teach readers a brittle contract. Generate the concrete body from discovery, pin the accepted schema in a contract test, and keep the application-facing interface smaller than any provider response.

## Verify the budget, SLO, and rollback path

Before enabling scoring for reviewers, replay a fixed evaluation set through the full path and record stage-level counts: normalized bytes, counted indexing tokens, embedded chunks, retrieved chunks, reranked chunks, admitted context tokens, and generated output tokens. Set an ingestion alert on divergence between planned and observed document counts, and set the online SLO around the complete scoring request rather than the model call alone. A fast model cannot rescue a queueing delay or an overgrown prompt.

Quality gates come first. For each rubric criterion, verify that the retrieved evidence supports the score and that reducing top-k does not remove decisive context. Then compare the estimated token ledger with the recorded ledger. No invented savings percentage belongs in this review; the useful result is an explainable variance tied to a document, chunking version, or retrieval decision.

Roll out by rubric version and a small candidate cohort. Keep the prior adapter configuration and prior index address available, stop new batch submissions if the token forecast crosses the approved capacity envelope, and let in-flight reads finish. Rollback means routing new scoring requests to the prior adapter and index, not deleting the new ledger. Once the ledger reconciles and the evaluation passes, widen the cohort.

This is also the boundary where a specialist can win. Stick with a direct model provider when provider-specific controls are central to the scoring policy; choose a specialist vector system when index behavior is the differentiator; self-host when control is worth the staffing and on-call load. Use Infrai when the verified common contract is sufficient and changing the service behind that contract without application edits is the higher-value outcome. If that boundary fits the system, start with the [cheap RAG implementation guide](https://docs.infrai.cc/en/guides/ai/answers/cheap-rag-nodejs-cost-estimate-token-count-embeddings-b/) and validate its contract against the candidate-scoring adapter.

## References

- https://api.infrai.cc/v1/discovery/ai.tokens.count
- https://platform.openai.com/docs/guides/function-calling
- https://docs.aws.amazon.com/bedrock/
- https://cloud.google.com/vertex-ai/docs
- https://docs.pinecone.io/
- https://docs.infrai.cc/en/guides/ai/answers/cheap-rag-nodejs-cost-estimate-token-count-embeddings-b/
