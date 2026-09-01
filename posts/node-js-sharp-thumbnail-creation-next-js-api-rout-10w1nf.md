# Node.js Sharp Thumbnail Creation: Next.js API Route Boundaries for Private Object Storage

Short answer: let the Next.js API route authenticate and record the upload, then create the Sharp thumbnail in a bounded worker that writes a deterministic object key; keeping decode work out of the request path protects the upload SLO and makes retries safe.

The production failure I keep seeing is not a bad resize algorithm. It is an innocent-looking route that downloads a private original, decodes it, and calls Sharp before returning `200`. The first test image is small, the happy path is quick, and the design review moves on. A phone photo or a retry storm changes the shape of the workload. The route now owns native memory, CPU contention, signed-URL policy, and a queue it never admits exists. I don't want that much operational responsibility hidden behind one handler, because the blast radius is larger than the feature suggests: a burst of uploads can consume the same process that serves authentication, status, and signed reads, and the resulting latency breach looks like an application-wide incident rather than an image-processing saturation problem.

That incident pattern gives me one invariant: a thumbnail is a durable derivative with an identity, not a temporary response body. Once that is explicit, the architecture is easier to operate and easier to replace.

## What should a Next.js API route do after a Sharp upload?

Keep the request contract narrow. Validate the caller and content metadata, stream the original to a private bucket, persist an upload record, enqueue a derivative job, and return a state that says the derivative is pending. A browser can poll a status endpoint or receive an event; it should not make the upload request wait for image decoding.

The order matters. Read enough of the image header to enforce a pixel ceiling before accepting work. File size is a weak proxy: compressed formats can expand dramatically when decoded. A 4-byte-per-pixel planning estimate makes the memory budget visible, and a worker pool can reserve slots against that estimate instead of guessing from CPU count.

I write the invariant into the job data: original key, content validator, target width, and a schema version. The validator can be an object-store ETag when its semantics are known, or a digest calculated during ingestion. If the user replaces an image at the same logical path, the validator changes and the derivative key changes with it. Old objects become garbage that a separate retention job can enumerate and delete; they cannot silently masquerade as the new image.

The short version is boring. Good.

Measure first.

## The worker path: bounded decode, deterministic write

The following Go sketch shows the contract I want even when the actual consumer is Node.js with Sharp. It does not prescribe a queue or a storage SDK. Those are replaceable details; the memory ceiling, idempotency check, and write ordering are not.

```go
package derivative

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

type Store interface {
	Get(context.Context, string) (io.ReadCloser, error)
	Put(context.Context, string, string, io.Reader) error
	Exists(context.Context, string) (bool, error)
}

type Job struct {
	OriginalKey string
	Validator   string
	Width       int
	Pixels      int64
}

const maxPixels = 40_000_000

var ErrPixelBudget = errors.New("image exceeds decode budget")

type Worker struct {
	Originals Store
	Derived   Store
	Resize    func(context.Context, io.Reader, int) (io.Reader, error)
	Slots     chan struct{}
}

func derivativeKey(j Job) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|w=%d|v1", j.OriginalKey, j.Validator, j.Width)))
	return fmt.Sprintf("derivatives/%s/w%d.webp", hex.EncodeToString(h[:8]), j.Width)
}

func (w *Worker) Handle(ctx context.Context, j Job) error {
	if j.Pixels > maxPixels {
		return ErrPixelBudget
	}

	key := derivativeKey(j)
	if exists, err := w.Derived.Exists(ctx, key); err != nil {
		return err
	} else if exists {
		return nil
	}

	select {
	case w.Slots <- struct{}{}:
		defer func() { <-w.Slots }()
	case <-ctx.Done():
		return ctx.Err()
	}

	src, err := w.Originals.Get(ctx, j.OriginalKey)
	if err != nil {
		return err
	}
	defer src.Close()

	out, err := w.Resize(ctx, src, j.Width)
	if err != nil {
		return err
	}
	return w.Derived.Put(ctx, key, "image/webp", out)
}
```

The existence check is an optimization and a correctness aid for at-least-once delivery, but it is not a transaction. Two workers can pass it at the same time. The destination write therefore needs an overwrite policy that is harmless for identical bytes, or a conditional create primitive when the storage system provides one. A successful retry must leave the same key and content type, and a failed write must not update the database row to `ready`.

I also keep the original and derivative namespaces separate. That makes retention, encryption policy, and access logging legible. Private image delivery should mint short-lived signed URLs only after authorization; putting a bucket behind a public-read rule because a thumbnail is “safe enough” is an access-control decision disguised as a convenience.

## How do private images, object storage, and Node.js retries fit together?

Treat the queue message as a fact, not as a promise that work finished. The API writes `uploaded`, the worker writes `processing`, and only a successful object write followed by a durable metadata update writes `ready`. A dead-letter record needs the original job payload, attempt count, and a reason category such as invalid header, pixel budget, transient storage failure, or unsupported format. That classification is more useful at 03:00 than a generic “thumbnail failed.”

Retries should be bounded and jittered. A timeout on the source GET, a timeout on Sharp, and a timeout on the destination PUT are separate controls; one global request timeout hides which resource is exhausted. Alert on queue age and derivative readiness percentage, not just worker process health. An instance can be green while the oldest user-facing placeholder is several hours old.

For capacity planning, reserve memory for decoded pixels plus the runtime and several buffers, then set `Slots` below the arithmetic maximum. Measure p95 and p99 decode duration by source dimensions, not only by compressed megabytes. If a 12-megapixel image is the common case and a rare panoramic upload is much larger, the admission check should make that distinction explicit. Your mileage may vary on the exact headroom multiplier; the important part is that it is a documented budget with an alert when the distribution moves.

There is a small but important browser detail: upload completion and derivative completion are different user-visible states. Returning `201` for the original and exposing `thumbnailStatus: "pending"` is honest. Returning `200` with a URL that may not exist turns an ordinary queue delay into a confusing broken-image report.

## Buy, build, or defer the derivative system

I use this table to force the on-call question into the design review. Unit price is one input; ownership of failure is the larger one.

| Placement | Request-path cost | Primary failure mode | Fits when |
| --- | --- | --- | --- |
| Inline resize in the API route | CPU and decode memory per upload | Concurrent large images exhaust the handler | Uploads are rare and a strict pixel cap is enforced |
| Bounded asynchronous worker | Enqueue latency only | Queue age delays a visible derivative | Most product workloads with eventual consistency |
| Managed image transformation | Little application CPU | Provider URL and cache semantics become a dependency | A small team needs many variants and accepts that coupling |
| On-demand worker with write-back | Work only for requested sizes | First viewer pays the miss latency | Variant combinations are broad and demand is uneven |

The trade-off is not abstract. A self-hosted worker means owning queue operations, dead-letter replay, backfills, and deletion scans. A managed transformer reduces that code but can make cache keys, authorization, and migration harder to control. Inline work is simple to deploy and difficult to protect once traffic is bursty.

## When this design is the wrong answer

Do not add a queue for a tightly controlled internal tool that accepts tiny avatars and must return a processed file in the same response. A synchronous call to a dedicated resize process can be clearer there, provided the pixel ceiling and timeout are enforced before decode.

Do not precompute every size when the product has a wide, rarely used variant matrix. Generate on a cache miss, write back under the same deterministic key, and record which variants are actually requested. Conversely, if a compliance workflow requires a hash of the derivative before acknowledging the upload, eventual consistency is the wrong contract; make the synchronous boundary explicit and budget it separately from the web API.

The catch is deletion. Private originals can have retention or takedown obligations, and content-derived names make reverse lookup non-trivial. Store a small manifest of derivative keys with the upload record, or maintain an index that a deletion workflow can query. Build that path before the first customer asks for erasure.

## References

- AWS S3 documentation: Presigned URLs — https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- Google Cloud Storage documentation — https://cloud.google.com/storage/docs
- Sharp documentation — https://sharp.pixelplumbing.com/
- libvips API reference — https://www.libvips.org/API/current/
- MDN Web Docs: HTTP caching — https://developer.mozilla.org/en-US/docs/Web/HTTP/Caching
- Go image.DecodeConfig — https://pkg.go.dev/image#DecodeConfig
