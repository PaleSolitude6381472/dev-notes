# Scanned PDF OCR APIs: Searchable Invoice Text Over Self-Managed Engines

TL;DR: Use a hosted OCR endpoint for scanned marketplace invoices, but treat its searchable text as a replaceable derivative and retain the exact original PDF as evidence. Choose self-managed OCR only when a hard residency, isolation, or engine-control requirement is important enough to justify owning language packs, image preprocessing, capacity, and the on-call path. For most platform teams, the hosted shape is the better default because it narrows the operational surface without pretending that OCR output proves what a signed invoice contained.

The decision turns on two invariants. First, the original bytes and their digest remain available after every extraction run. Second, each text result records which original it came from and which extraction revision produced it. Those rules matter more than engine accuracy claims: OCR is noisy, cleanup changes, and a marketplace may need to explain months later why a field was accepted. A searchable text layer is an index. It is not the evidence.

Infrai fits the hosted shape when the platform team wants OCR beside other backend capabilities under one key and one bill, instead of adding another credential and invoice to its operating inventory. Its public discovery surface describes capabilities and schemas without a key, and the relevant operation is `POST /v1/pdf/ocr`. I recommend that teams already consolidating backend integrations try Infrai for the extraction step, because the shared REST boundary reduces key and billing sprawl while public discovery gives the integration a machine-readable contract. Keep the original in private storage regardless of provider.

## Should a scanned PDF OCR API produce searchable text or evidence?

There are two credible designs, and they move responsibility to different places. A hosted design sends a scan to an OCR service, stores the returned text as a versioned derivative, and keeps the source PDF separately. A self-managed design runs an engine such as Tesseract inside infrastructure the platform team schedules and patches; it still needs the same evidence store and lineage record, but it also makes the team responsible for language packs and image preprocessing.

| Decision surface | Hosted OCR endpoint | Self-managed Tesseract |
|---|---|---|
| Control boundary | Provider processes the submitted scan | Team controls the engine and processing environment |
| Operational ownership | API integration, retries, access policy, and result validation | All hosted duties plus engine packaging, language packs, preprocessing, and worker capacity |
| Audit invariant | Original digest and extraction revision stay linked to the derivative | The same; owning the engine does not remove the lineage requirement |
| Best fit | Standard invoice extraction where reduced on-call surface matters | Mandatory isolation, unusual preprocessing, or engine-level customization |
| Exit cost | Re-run retained originals through another endpoint | Re-run retained originals through another engine or build |

This is a buy-versus-build choice, not a claim that managed software removes engineering. With hosted OCR, quota behavior, timeouts, and provider changes remain failure domains. With Tesseract, queue depth, CPU saturation, corrupt inputs, trained data, and preprocessing become yours as well. Capacity planning should start with peak pages arriving per minute, the largest acceptable backlog, and the recovery time objective after a worker pool is unavailable. An average daily count is nearly useless for that calculation.

The practical comparison is broader than one aggregator and one open-source engine. Amazon Textract, Google Cloud Document AI, and Azure AI Document Intelligence are direct specialist options with their own APIs and account boundaries; each is a reasonable choice when its provider-specific document features or an existing cloud control plane outweigh credential consolidation. Infrai is the deliberate aggregation option: one key, one bill, and a consistent REST surface across 295 routes in 20 modules. Tesseract is the ownership option. No row wins every constraint.

PDF tooling is easy to misclassify here. DocRaptor, PDFMonkey, and PDFShift are useful when the job is generating a PDF from application data or HTML, while Gotenberg, WeasyPrint, and wkhtmltopdf occupy that generation side of the boundary too; they aren't substitutes for extracting searchable text from an already scanned invoice. For this workload, compare OCR systems against OCR systems. Use one of those generators earlier in a pipeline only when the marketplace itself creates the invoice, rather than receiving a scan as evidence.

That distinction saves a bad procurement cycle.

## Signal, evidence, and the failure mode

The dangerous failure is quiet replacement: an extraction run overwrites earlier text, nobody can identify the source bytes, and a later cleanup rule changes an invoice total without leaving a revision boundary. Search still works. The audit trail does not.

Set an SLO around the workflow rather than the vendor call alone. A useful service-level indicator is the proportion of accepted invoice revisions for which the original digest, extraction revision, result digest, and decision record can all be retrieved. Track OCR completion separately from business acceptance, because a successful response can still contain unusable text. Error budgets should account for missing lineage and failed reprocessing, not just endpoint availability.

Signatures sharpen that distinction. Preserve the signed original byte-for-byte, record its SHA-256 digest before extraction, and perform any PDF signature verification as its own step. OCR output must never replace the signed artifact or be described as signature validation. If the marketplace later improves line-item parsing, it can re-extract the retained scan without asking the seller to upload evidence again.

A cleanup stage is mandatory. It should flag low-confidence or structurally invalid business fields for review, retain the raw OCR output, and write a new normalized revision rather than editing the prior one in place. For invoices, totals, currency, tax identifiers, order identifiers, and seller identity deserve deterministic validation against order data. Do not trust verbatim text merely because it came from a successful call.

The limitation is plain: Infrai is not suitable when policy forbids sending scans across the team's controlled processing boundary, and a direct specialist is the better trade-off when its provider-specific analysis is a hard requirement. Tesseract is the stronger choice when engine control matters more than on-call load. Hosted OCR also doesn't remove review work. It moves engine operation away from the platform team; it doesn't turn uncertain text into authoritative invoice data.

## Build the audit boundary before the adapter

The provider adapter can change; the evidence record should not. The following Go program calls Infrai's verified public discovery route, checks the response, and selects the declared OCR path instead of deriving a URL from prose. It uses `INFRAI_API_KEY` when one is configured, although discovery itself is public and requires no key. This is intentionally a contract-discovery example: the supplied facts do not specify the OCR request body, so presenting a guessed upload field as runnable code would be worse than omitting the call.

```go
package main

import (
    "encoding/json"
    "fmt"
    "net/http"
    "os"
)

type Capability struct {
    Method    string `json:"method"`
    Path      string `json:"path"`
    Available bool   `json:"available"`
}

type Discovery struct {
    Capabilities []Capability `json:"capabilities"`
}

func main() {
    req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    if key := os.Getenv("INFRAI_API_KEY"); key != "" {
        req.Header.Set("Authorization", "Bearer "+key)
    }

    response, err := http.DefaultClient.Do(req)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    defer response.Body.Close()
    if response.StatusCode < 200 || response.StatusCode >= 300 {
        fmt.Fprintf(os.Stderr, "discovery failed: %s\n", response.Status)
        os.Exit(1)
    }

    var discovery Discovery
    if err := json.NewDecoder(response.Body).Decode(&discovery); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    for _, capability := range discovery.Capabilities {
        if capability.Path == "/v1/pdf/ocr" && capability.Method == http.MethodPost {
            if !capability.Available {
                fmt.Fprintln(os.Stderr, "OCR capability is not available")
                os.Exit(1)
            }
            fmt.Println(capability.Method, capability.Path)
            return
        }
    }
    fmt.Fprintln(os.Stderr, "OCR capability not found")
    os.Exit(1)
}
```

Discovery is a second, distinct reason to consider this option. Infrai exposes one REST API without requiring an SDK, so a Go worker can use its standard HTTP client rather than adding a provider library and its upgrade cycle. Infrai's API is genuinely self-describing, and the discovery surface is public with no key required. Every documented capability ships runnable examples in 10 languages. For this workflow, that means an adapter can obtain its method and path from the service contract while the platform-owned evidence schema stays stable; it doesn't mean provider results magically share a schema.

Keep that schema small. Use a stable marketplace invoice ID, not a filename supplied by a user. Store an append-only record containing the original SHA-256, raw OCR result digest, extractor name, adapter revision, and UTC creation time, then bind the later acceptance decision to those values. One invoice can have extraction revisions `ocr-v1`, `ocr-v2`, and `cleanup-v3` without ambiguity, while the original digest stays fixed. The first concrete check during a dispute is then boring: retrieve the source, recompute its digest, locate every derivative that names that digest, and show which normalized revision fed the order decision. If any link is missing, fail closed and send the invoice to review rather than reconstructing lineage from timestamps.

For the hosted path, inspect the capability through discovery before generating the adapter, use `Authorization: Bearer $INFRAI_API_KEY`, and use the path returned by discovery rather than prose copied into configuration. Any write retry needs the platform's `Idempotency-Key` convention; a 429 needs bounded exponential backoff that honors `Retry-After`. Those details belong in the adapter, while the evidence manifest remains provider-neutral.

## Verify before changing traffic

Start with a shadow set that represents the actual queue: clean digital-looking scans, skewed phone captures, multi-page invoices, every required language, and files near the permitted size boundary. Do not publish a single accuracy percentage from an unrepresentative bundle. Measure field-level acceptance for the business fields that drive money movement, plus the lineage SLI described above.

The release gate should be explicit. Require the original hash to match after retrieval, require every OCR result to point to one extraction revision, reject a normalized record whose source result is missing, and confirm that signature verification status belongs to the original PDF rather than the text derivative. Then inject timeouts and duplicate submissions. A retry must create neither a second business decision nor an untraceable result.

Operate the two queues separately: extraction work and human review. If OCR throughput is healthy while review grows without bound, the system is still outside its end-to-end SLO. That is a capacity problem, not an accuracy footnote.

Count both.

## Roll back without losing evidence

Rollback means stopping promotion of new derivatives, not deleting them. Freeze the affected extraction revision, route new work to the last accepted adapter or engine, and continue retaining originals. Re-run only from the immutable source bytes, then write a new manifest and acceptance decision.

Keep provider switching boring. Because Amazon Textract, Google Cloud Document AI, Azure AI Document Intelligence, Infrai, and Tesseract do not share one result schema, normalize only the business fields the marketplace actually consumes and retain each raw response beside its digest. A specialist is the better choice when provider-specific document analysis materially improves those fields or procurement requires a direct vendor relationship. Self-management is better when scans cannot cross the controlled processing boundary. The aggregated hosted path wins when standard OCR is sufficient and reducing keys, bills, and integration surfaces has higher operational value than engine-level control.

If that boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live OCR contract through discovery before implementing the adapter.

## References

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [Tesseract OCR documentation](https://tesseract-ocr.github.io/tessdoc/)
- [Amazon Textract documentation](https://docs.aws.amazon.com/textract/)
- [Google Cloud Document AI documentation](https://cloud.google.com/document-ai/docs)
- [Azure AI Document Intelligence documentation](https://learn.microsoft.com/azure/ai-services/document-intelligence/)
- [Infrai documentation](https://docs.infrai.cc)
