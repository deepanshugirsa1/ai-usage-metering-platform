# Architecture and Design Decisions

## Objective

Create an explainable path from raw AI usage to recognized usage revenue while preserving the evidence required to investigate discrepancies, rerun a billing period, and support financial close.

## System boundaries

The platform owns:

- ingestion of versioned usage-event contracts;
- canonicalization, deduplication, and late-event classification;
- effective-dated pricing and event-level rating;
- invoice assembly and double-entry journal generation;
- data-quality controls and financial reconciliations.

The platform does not yet own customer entitlements, payment collection, tax calculation, revenue-recognition schedules, or upstream API instrumentation. Those items are included in the remaining 22% scope.

## Event contract

Each event requires:

- `event_id`: globally stable idempotency key;
- `customer_id`: account responsible for the charge;
- `model`: rated product identifier;
- `input_tokens` and `output_tokens`: non-negative metered quantities;
- `occurred_at`: business time used for billing and effective pricing;
- `received_at`: platform arrival time used to detect lateness;
- `source`: producing system.

The ingestion layer adds an immutable `ingestion_id`, a canonical payload hash, and an ingestion timestamp. Duplicate deliveries remain visible in raw data but contribute only once to billed usage.

## Canonical grains

- `raw_usage_events`: one row per ingestion attempt.
- `metered_usage`: one row per unique `event_id`.
- `rated_usage`: one row per rated event.
- `invoice_lines`: one row per invoice and model.
- `invoices`: one row per customer and billing month.
- `general_ledger`: one row per journal account.

Explicit grains prevent accidental fan-out and make every reconciliation boundary testable.

## Processing semantics

### Exactly-once business outcome

Distributed systems generally provide at-least-once delivery. This project obtains an exactly-once financial outcome by retaining all deliveries, selecting one deterministic record per stable event ID, and rebuilding downstream tables transactionally.

### Event time

Billing period and price selection use `occurred_at`, not arrival time. Events received more than 24 hours after occurrence are retained and flagged. This avoids silently losing revenue while preserving the operational signal needed for close.

### Pricing

Prices are effective-dated by model. A usage event must match one price whose validity interval contains its event timestamp. Input and output token charges remain separate through rating so invoices are explainable.

### Monetary precision

All charges and journal amounts use fixed-point decimal types. Floating-point values are not persisted in financial data products.

## Reconciliation controls

The pipeline persists two financial tie-outs:

1. Rated usage total equals finalized invoice total.
2. Finalized invoice total equals Usage Revenue credits.

Journal-level controls separately enforce that Accounts Receivable debits equal Usage Revenue credits. A non-zero difference fails the run rather than being rounded away.

## Quality controls

Current controls validate required fields, non-negative quantities, unique canonical event IDs, complete pricing coverage, and balanced journals. Results are persisted by run ID to create durable operational evidence.

## Failure and rerun behavior

Serving tables are rebuilt inside one transaction. Any transformation failure rolls back the entire rebuild, preventing mixed-version financial outputs. Raw events and prior quality evidence remain intact.

## Production mapping

- Replace local event input with Kafka topics governed by a schema registry.
- Land immutable payloads in object storage before warehouse ingestion.
- Implement canonical models as incremental dbt models on Snowflake.
- Use stream processing for low-latency metering and warehouse jobs for close-grade recomputation.
- Publish quality and reconciliation status to observability and incident-management systems.
- Enforce least-privilege access, PII classification, retention, and deletion policies.

This split keeps real-time product feedback fast while retaining a deterministic batch path for finance and audit.
