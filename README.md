# Gromo Circuit Go

Circuit breaker SDK for HTTP, TCP, databases, Redis, Kafka, MongoDB, RabbitMQ, or any external operation.

Features: closed/open/half-open states, concurrency bulkhead, per-call timeout, fallback in a separate goroutine, and non-blocking telemetry to Gromo OpsCore. Outcomes are success, failure, busy, and rejected.

The OpsCore rolling error rate is failure events divided by total request events during the immediately preceding hour.
