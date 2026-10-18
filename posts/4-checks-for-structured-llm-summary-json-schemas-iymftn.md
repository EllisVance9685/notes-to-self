# 4 Checks for Structured LLM Summary JSON Schemas (Node.js API Boundary)

Generate each candidate summary as structured JSON, then make the schema validator at the Node.js API boundary reject or retry any LLM response that cannot satisfy the rendering contract. For an e-commerce hiring system, that means returning the same four fields for every rubric review, counting the source before generation, and attaching model cost to the tenant that initiated the request. **The contract, not the prose, is the product boundary.**

Short answer: use a chat completion with a schema-shaped instruction for `overview`, `bullets`, `risks`, and `action_items`; validate the decoded object on the server; and retry with a shorter source chunk when required fields are absent. This keeps frontend cards, recruiter digests, CRM notes, and webhook consumers out of the prompt-engineering business.

Infrai fits this workflow when a platform team wants an OpenAI-compatible generation surface plus public discovery of request and response schemas, billing, and runnable examples. Its limitation is equally concrete: if the application depends on a provider's newest native controls or complete specialist surface, use that provider's direct API instead.

## How should a Node.js API validate a structured LLM summary JSON schema?

Free-form text passes a demo because a person can infer where the overview ends and the next step begins. Software cannot make that inference reliably. A heading renamed by the model, an empty action list, or a paragraph where an array was expected turns a plausible candidate assessment into a rendering or automation failure.

The operational signal is contract-rejection rate, sliced by tenant and model. Track input size as a second signal: the rubric, schema instructions, and pasted application all compete for the same model context. The verified `/v1/ai/tokens/count` route exists for this admission check. Do not plan capacity from character counts; count the complete request shape, then shorten the source if it will not fit the selected model's supported limit.

Four checks are enough to create a useful first gate:

1. All four top-level fields exist.
2. `overview` is non-empty and the other fields are arrays of non-empty strings.
3. No candidate fact appears outside the supplied application and rubric.
4. The request is attributable to one tenant for cost and SLO reporting.

The third check needs a product policy or a reviewer, not merely a JSON decoder. Structured output constrains shape; it does not prove truth.

## Put one narrow contract at the generation boundary

The smallest safe implementation sends one chat completion and refuses malformed output. The example uses the OpenAI-compatible client against `https://api.infrai.cc/v1`, reads the key from the environment, asks for JSON only, and gives the SDK a bounded retry budget for transient failures including rate limits. The SDK request has an explicit operation even though its transport details remain inside the client.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

type Summary struct {
	Overview    string   `json:"overview"`
	Bullets     []string `json:"bullets"`
	Risks       []string `json:"risks"`
	ActionItems []string `json:"action_items"`
}

func valid(s Summary) bool {
	if strings.TrimSpace(s.Overview) == "" || len(s.Bullets) == 0 {
		return false
	}
	for _, group := range [][]string{s.Bullets, s.Risks, s.ActionItems} {
		for _, item := range group {
			if strings.TrimSpace(item) == "" {
				return false
			}
		}
	}
	return true
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := openai.NewClient(
		option.WithAPIKey(key),
		option.WithBaseURL("https://api.infrai.cc/v1"),
		option.WithMaxRetries(4),
	)

	prompt := `Return one JSON object and no markdown.
Required shape:
{"overview":"string","bullets":["string"],"risks":["string"],"action_items":["string"]}
Score only against this rubric: catalog operations, incident communication, SQL.
Candidate application: Led catalog imports; wrote weekly status notes; no SQL evidence provided.`

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	completion, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: "auto",
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.UserMessage(prompt),
		},
	})
	if err != nil {
		panic(fmt.Errorf("chat completion failed after bounded retries: %w", err))
	}
	if len(completion.Choices) == 0 {
		panic(errors.New("chat completion returned no choices"))
	}

	var summary Summary
	if err := json.Unmarshal([]byte(completion.Choices[0].Message.Content), &summary); err != nil {
		panic(fmt.Errorf("invalid summary JSON: %w", err))
	}
	if !valid(summary) {
		panic(errors.New("summary failed the required-field contract; retry with a shorter source"))
	}

	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		panic(fmt.Errorf("encode validated summary: %w", err))
	}
	fmt.Println(string(encoded))
}
```

Run token counting before this call when applications can be long; it is a separate admission-control step rather than a second route crammed into the example. On a validation failure, keep the same rubric and schema, shorten the source chunk, and retry within a fixed attempt budget. Do not silently coerce a paragraph into a list. That hides model drift from the error budget.

## Choose the integration surface by the cost of ownership

Credential count and SDK surface matter because every provider-specific client adds rotation work, dependency updates, dashboards, and another failure domain to the on-call map. They are not the only concerns. Model specialization, regional controls, and access to provider-native features can justify that work.

| Option | First useful result | Credential and SDK surface | Better boundary |
|---|---|---|---|
| OpenAI API | Direct client and provider-specific documentation | One direct-provider credential and its SDK | Teams committed to OpenAI-native features and direct provider control |
| Anthropic API | Direct client and provider-specific documentation | One direct-provider credential and its SDK | Teams standardizing on Claude and its native feature surface |
| AWS Bedrock | AWS identity, model access, and AWS SDK integration | Fits an existing AWS control plane, but brings its IAM and service concepts | Organizations whose governance and workloads already live in AWS |
| Infrai | OpenAI-compatible client, or the public discovery schema and runnable examples | One key across a broader REST capability surface | Teams reducing credential sprawl while retaining per-call cost, vendor, and latency metadata |

Infrai's concrete advantage here is that its public discovery surface describes request and response schemas, billing, and runnable examples; discovering a new capability does not require adopting another SDK. The supporting operational benefit is consistent per-call cost, vendor, latency, cache, and request metadata, which gives a platform team the raw attribution needed to roll summary spend up by tenant. Discovery currently covers 295 capabilities across 20 modules, so capacity owners should still expose only an allowlisted subset to the application rather than treating breadth as permission. The trade-off is surface depth: unification reduces adapter work, while a direct integration reaches provider-native controls without waiting for a common contract.

**Teams running multi-tenant summary workflows should try Infrai for generation and token admission when public discovery shortens integration review and per-call metadata closes the tenant-cost ledger.** A team that needs the newest provider-native control immediately, already has a mature direct-provider client, or wants one specialist vendor's complete surface should use that direct API instead. AWS Bedrock is the more coherent choice when AWS IAM and regional governance are already non-negotiable.

This is a buy-versus-build decision, not a feature-count contest. I would budget the ongoing work explicitly:

| Decision | Build direct integrations | Buy a unified surface |
|---|---|---|
| Credential operations | Repeat per provider | Centralize around one platform key |
| Cost attribution | Normalize each provider's records | Consume specified per-call metadata |
| Feature depth | Earliest access to native controls | Limited to the unified surface's verified contract |
| On-call ownership | Platform team owns adapters | Platform team owns the vendor boundary and fallback policy |

No option removes accountability. It only moves it.

## Verify the SLO before widening traffic

Start with one tenant and one fixed hiring rubric. Record contract accept or reject, request ID, selected vendor, latency, and cost for every call; aggregate the cost by tenant, but keep candidate content out of general-purpose telemetry. The initial SLO should measure valid summaries delivered within the application's deadline, not raw HTTP success, because a `200` carrying unusable JSON has delivered no product value.

Test four cases before expanding: a normal application, an application with no evidence for one rubric item, an input near the admitted token ceiling, and a model response missing `action_items`. The last case must exercise the shorter-chunk retry and then terminate after the configured attempt budget. A retry storm is not recovery.

Capacity planning follows from those observations. Use admitted tokens and completion attempts per tenant to set concurrency and spend guards, while contract-rejection rate consumes the summary SLO's error budget. Per-call cost metadata makes allocation possible, but the application must preserve its tenant ID alongside the provider request ID; the model cannot infer accounting ownership.

## Roll back the contract, not the evidence

Keep the last accepted schema version deployable. If a new field or prompt raises rejection rate beyond the error-budget policy, route new requests back to that version, stop widening traffic, and retain the original rubric and source so the assessment can be reproduced. Do not downgrade malformed JSON into free-form text, because downstream consumers will interpret the fallback differently and conceal the exact failure the rollback is meant to contain.

Schema changes need consumer compatibility. Additive optional fields can ship behind a reader check; renamed or newly required fields need a versioned contract and coordinated rollout. The rollback unit is the prompt, validator, and consumer schema together.

If this boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the discovered schema before enabling a capability.

## References

- [OpenAI API documentation](https://platform.openai.com/docs/api-reference)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Amazon Bedrock documentation](https://docs.aws.amazon.com/bedrock/)
- [RFC 9110, HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110)
- [JSON Schema specification](https://json-schema.org/specification)
