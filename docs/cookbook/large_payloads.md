# Handling Large Payloads

Generally, message brokers are optimized for routing millions of tiny messages quickly.
They are not designed to store or transfer massive binary blobs.

If your application needs to process large datasets asynchronously - such as parsing a big CSV
upload or performing analytics on millions of records - sending that data directly through your
message queue causes memory spikes, high serialization overhead, and hurts performance.

This cookbook explains how to safely handle large payloads in Repid by passing data by reference
using S3-compatible storage and the [Polars](https://pola.rs/) DataFrame library.

## The Scenario: Bulk Audience Processing

Imagine a marketing platform where users upload massive CSV files containing millions of customer
records to generate a "Custom Audience Summary Report".

Uploading and parsing this file synchronously in a web server (like FastAPI) would tie up the worker
and time out the HTTP request. Instead, we use **Presigned URLs**:

1. The client requests a Presigned URL from the web server.
2. The client uploads the large CSV directly to an S3-compatible storage bucket.
3. The client notifies the web server that the upload is complete, providing the S3 key.
4. The web server enqueues a Repid task containing only a lightweight reference:
   `{"job_id": 123, "s3_key": "uploads/123.csv"}`.

This completely bypasses the web server for the heavy network I/O and keeps the message queue
blazing fast.

## Prerequisites

For this example, we will use `aioboto3` for asynchronous S3 interactions and `polars` for fast
data processing.

```bash
pip install repid aioboto3 polars
```

## The Publisher: Enqueuing the Reference

The publisher code (e.g., your FastAPI endpoint) doesn't handle the file at all. It simply receives
the confirmation from the client and enqueues the Repid task with the S3 reference.

```python
from repid import Repid

# Assuming `app` is your initialized and configured Repid instance

async def enqueue_audience_processing(job_id: int, s3_key: str) -> None:
    await app.send_message_json(
        channel="process_audience",
        payload={
            "job_id": job_id,
            "s3_key": s3_key,
        },
    )  # (1)!
```

1. We enqueue a tiny JSON payload containing just the reference (the `s3_key`).
The actual 50MB file safely resides in S3-compatible storage.

## The Worker: Downloading, Processing, and Uploading

The Repid actor receives the lightweight payload and performs the computation.

```python
import io
import asyncio
import polars as pl
import aioboto3
from repid import Router

router = Router()

def _heavy_computation(file_data: bytes) -> bytes:  # (1)!
    df = pl.read_csv(io.BytesIO(file_data))

    # Perform heavy lifting: e.g., aggregations and deduplication
    summary_df = (
        df.unique(subset=["email"])
        .group_by("country")
        .agg(pl.len().alias("user_count"))
    )

    buffer = io.BytesIO()
    summary_df.write_csv(buffer)
    return buffer.getvalue()


@router.actor
async def process_audience(job_id: int, s3_key: str) -> None:
    session = aioboto3.Session()

    # Step 1: Download the raw data
    async with session.client("s3") as s3:
        response = await s3.get_object(Bucket="my-marketing-bucket", Key=s3_key)
        file_data = await response["Body"].read()

    # Step 2: Offload CPU-bound work to a separate thread
    summary_bytes = await asyncio.to_thread(_heavy_computation, file_data)  # (2)!

    # Step 3: Upload the result and clean up
    summary_key = f"reports/summary_{job_id}.csv"

    async with session.client("s3") as s3:
        await s3.put_object(
            Bucket="my-marketing-bucket",
            Key=summary_key,
            Body=summary_bytes,
        )  # (3)!

        await s3.delete_object(Bucket="my-marketing-bucket", Key=s3_key)  # (4)!
```

1. This function runs synchronously in a separate thread.
It takes the raw bytes, parses them, performs heavy analytical aggregations,
and returns the serialized result bytes.
2. `asyncio.to_thread` offloads the work to avoid blocking the event loop.
3. We upload the result (summary report) back to S3-compatible storage so that
the client can download it later.
4. We delete the temporary raw upload to minimize storage costs now that the job is done.
