# Security Policy

## Supported Versions

Only the latest major version of Repid is currently supported for security updates.

| Version | Supported          |
| ------- | ------------------ |
| 2.x.x   | :white_check_mark: |
| < 2.x.x | :x:                |

## Reporting a Vulnerability

If you discover a potential security vulnerability, please report it immediately by emailing
<me@aleksul.space>. Please be as detailed as possible, providing step-by-step instructions
to reproduce the issue. Including a Minimal Reproducible Example (MRE) is highly appreciated.

The author (@aleksul) will review your report thoroughly and respond as soon as possible.

## Public Discussions

Please refrain from publicly discussing potential security vulnerabilities before they are resolved.

It is critically important to discuss such issues privately first. This ensures a patch can be
developed and distributed, mitigating potential impacts on users.

## Additional Considerations

Repid integrates with various message brokers. Some protocol implementations are native to
Repid, while others rely on external libraries.

If a security issue originates in an external dependency or the broker itself, please report
it directly to the respective maintainers.

| Integration | Where Maintained |
| --- | --- |
| AMQP 1.0 | In Repid |
| GCP Pub/Sub | In Repid / [grpcio](https://github.com/grpc/grpc) / [google-auth](https://github.com/googleapis/google-cloud-python/tree/main/packages/google-auth) |
| Amazon SQS | [aiobotocore](https://github.com/aio-libs/aiobotocore) |
| Redis | [redis-py](https://github.com/redis/redis-py) |
| NATS | [nats-py](https://github.com/nats-io/nats.py) |
| Kafka | [aiokafka](https://github.com/aio-libs/aiokafka) |
