from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_usage_events (
    ingestion_id VARCHAR PRIMARY KEY,
    event_id VARCHAR NOT NULL,
    customer_id VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    source VARCHAR NOT NULL,
    payload_hash VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS pricing (
    model VARCHAR NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    input_rate_per_million DECIMAL(18, 6) NOT NULL,
    output_rate_per_million DECIMAL(18, 6) NOT NULL,
    PRIMARY KEY (model, effective_from)
);

CREATE TABLE IF NOT EXISTS metered_usage (
    event_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    billing_period DATE NOT NULL,
    is_late BOOLEAN NOT NULL,
    source_payload_hash VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS rated_usage (
    event_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    billing_period DATE NOT NULL,
    input_charge DECIMAL(18, 8) NOT NULL,
    output_charge DECIMAL(18, 8) NOT NULL,
    total_charge DECIMAL(18, 8) NOT NULL,
    pricing_effective_from TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS invoice_lines (
    invoice_id VARCHAR NOT NULL,
    customer_id VARCHAR NOT NULL,
    billing_period DATE NOT NULL,
    model VARCHAR NOT NULL,
    event_count BIGINT NOT NULL,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    line_amount DECIMAL(18, 8) NOT NULL,
    PRIMARY KEY (invoice_id, model)
);

CREATE TABLE IF NOT EXISTS invoices (
    invoice_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    billing_period DATE NOT NULL,
    invoice_amount DECIMAL(18, 8) NOT NULL,
    status VARCHAR NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS general_ledger (
    journal_id VARCHAR NOT NULL,
    invoice_id VARCHAR NOT NULL,
    billing_period DATE NOT NULL,
    account VARCHAR NOT NULL,
    debit DECIMAL(18, 8) NOT NULL,
    credit DECIMAL(18, 8) NOT NULL,
    posted_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (journal_id, account)
);

CREATE TABLE IF NOT EXISTS quality_results (
    run_id VARCHAR NOT NULL,
    check_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    observed_value VARCHAR NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS reconciliation_results (
    run_id VARCHAR NOT NULL,
    reconciliation_name VARCHAR NOT NULL,
    left_amount DECIMAL(18, 8) NOT NULL,
    right_amount DECIMAL(18, 8) NOT NULL,
    difference DECIMAL(18, 8) NOT NULL,
    status VARCHAR NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL
);
"""


DEFAULT_PRICES = [
    {
        "model": "gpt-demo-mini",
        "effective_from": "2026-01-01T00:00:00+00:00",
        "effective_to": None,
        "input_rate_per_million": "0.150000",
        "output_rate_per_million": "0.600000",
    },
    {
        "model": "gpt-demo",
        "effective_from": "2026-01-01T00:00:00+00:00",
        "effective_to": None,
        "input_rate_per_million": "2.500000",
        "output_rate_per_million": "10.000000",
    },
]


def _stable_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def demo_events() -> list[dict[str, Any]]:
    """Return deterministic events including a duplicate and a late arrival."""
    events = [
        {
            "event_id": "evt-001",
            "customer_id": "cus-alpha",
            "model": "gpt-demo",
            "input_tokens": 2_000_000,
            "output_tokens": 200_000,
            "occurred_at": "2026-02-02T12:00:00+00:00",
            "received_at": "2026-02-02T12:00:03+00:00",
            "source": "api_gateway",
        },
        {
            "event_id": "evt-002",
            "customer_id": "cus-alpha",
            "model": "gpt-demo-mini",
            "input_tokens": 4_000_000,
            "output_tokens": 500_000,
            "occurred_at": "2026-02-03T09:30:00+00:00",
            "received_at": "2026-02-03T09:30:02+00:00",
            "source": "api_gateway",
        },
        {
            "event_id": "evt-003",
            "customer_id": "cus-beta",
            "model": "gpt-demo",
            "input_tokens": 1_500_000,
            "output_tokens": 100_000,
            "occurred_at": "2026-02-01T08:00:00+00:00",
            "received_at": "2026-02-05T11:00:00+00:00",
            "source": "replay_service",
        },
    ]
    events.append(dict(events[0]))
    return events


class MonetizationPipeline:
    def __init__(self, database: str | Path = ":memory:") -> None:
        self.database = str(database)
        self.connection = duckdb.connect(self.database)

    def initialize(self) -> None:
        self.connection.execute(SCHEMA_SQL)

    def load_prices(self, prices: Iterable[dict[str, Any]] = DEFAULT_PRICES) -> int:
        loaded = 0
        for price in prices:
            self.connection.execute(
                """
                INSERT INTO pricing VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (model, effective_from) DO UPDATE SET
                    effective_to = excluded.effective_to,
                    input_rate_per_million = excluded.input_rate_per_million,
                    output_rate_per_million = excluded.output_rate_per_million
                """,
                [
                    price["model"],
                    price["effective_from"],
                    price.get("effective_to"),
                    price["input_rate_per_million"],
                    price["output_rate_per_million"],
                ],
            )
            loaded += 1
        return loaded

    def ingest(self, events: Iterable[dict[str, Any]]) -> int:
        ingested = 0
        for sequence, event in enumerate(events):
            payload_hash = _stable_hash(event)
            ingestion_id = hashlib.sha256(
                f"{event['event_id']}:{payload_hash}:{sequence}".encode()
            ).hexdigest()
            self.connection.execute(
                """
                INSERT INTO raw_usage_events (
                    ingestion_id, event_id, customer_id, model, input_tokens,
                    output_tokens, occurred_at, received_at, source, payload_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [
                    ingestion_id,
                    event["event_id"],
                    event["customer_id"],
                    event["model"],
                    event["input_tokens"],
                    event["output_tokens"],
                    event["occurred_at"],
                    event["received_at"],
                    event["source"],
                    payload_hash,
                ],
            )
            ingested += 1
        return ingested

    def transform(self) -> None:
        """Rebuild deterministic serving tables from immutable raw events."""
        self.connection.execute("BEGIN TRANSACTION")
        try:
            for table in (
                "general_ledger",
                "invoices",
                "invoice_lines",
                "rated_usage",
                "metered_usage",
            ):
                self.connection.execute(f"DELETE FROM {table}")

            self.connection.execute(
                """
                INSERT INTO metered_usage
                SELECT
                    event_id,
                    customer_id,
                    model,
                    input_tokens,
                    output_tokens,
                    occurred_at,
                    received_at,
                    date_trunc('month', occurred_at)::DATE AS billing_period,
                    received_at > occurred_at + INTERVAL '24 hours' AS is_late,
                    payload_hash
                FROM raw_usage_events
                QUALIFY row_number() OVER (
                    PARTITION BY event_id ORDER BY received_at, ingestion_id
                ) = 1
                """
            )

            self.connection.execute(
                """
                INSERT INTO rated_usage
                SELECT
                    m.event_id,
                    m.customer_id,
                    m.model,
                    m.billing_period,
                    (m.input_tokens / 1000000.0 * p.input_rate_per_million)::DECIMAL(18, 8),
                    (m.output_tokens / 1000000.0 * p.output_rate_per_million)::DECIMAL(18, 8),
                    (
                        m.input_tokens / 1000000.0 * p.input_rate_per_million
                        + m.output_tokens / 1000000.0 * p.output_rate_per_million
                    )::DECIMAL(18, 8),
                    p.effective_from
                FROM metered_usage m
                JOIN pricing p
                  ON m.model = p.model
                 AND m.occurred_at >= p.effective_from
                 AND (p.effective_to IS NULL OR m.occurred_at < p.effective_to)
                """
            )

            self.connection.execute(
                """
                INSERT INTO invoice_lines
                SELECT
                    md5(r.customer_id || ':' || r.billing_period::VARCHAR) AS invoice_id,
                    r.customer_id,
                    r.billing_period,
                    r.model,
                    count(*) AS event_count,
                    sum(m.input_tokens)::BIGINT AS input_tokens,
                    sum(m.output_tokens)::BIGINT AS output_tokens,
                    sum(r.total_charge)::DECIMAL(18, 8) AS line_amount
                FROM rated_usage r
                JOIN metered_usage m USING (event_id)
                GROUP BY r.customer_id, r.billing_period, r.model
                """
            )

            self.connection.execute(
                """
                INSERT INTO invoices
                SELECT
                    invoice_id,
                    customer_id,
                    billing_period,
                    sum(line_amount)::DECIMAL(18, 8),
                    'finalized',
                    current_timestamp
                FROM invoice_lines
                GROUP BY invoice_id, customer_id, billing_period
                """
            )

            self.connection.execute(
                """
                INSERT INTO general_ledger
                SELECT
                    'jrn-' || invoice_id,
                    invoice_id,
                    billing_period,
                    account,
                    debit,
                    credit,
                    current_timestamp
                FROM invoices
                CROSS JOIN LATERAL (
                    VALUES
                        ('Accounts Receivable', invoice_amount, 0::DECIMAL(18, 8)),
                        ('Usage Revenue', 0::DECIMAL(18, 8), invoice_amount)
                ) entries(account, debit, credit)
                """
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def validate(self, run_id: str | None = None) -> list[dict[str, Any]]:
        run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        checks = [
            (
                "raw_required_fields",
                (
                    "SELECT count(*) FROM raw_usage_events WHERE "
                    "event_id IS NULL OR customer_id IS NULL OR model IS NULL"
                ),
                lambda value: value == 0,
            ),
            (
                "non_negative_tokens",
                (
                    "SELECT count(*) FROM raw_usage_events "
                    "WHERE input_tokens < 0 OR output_tokens < 0"
                ),
                lambda value: value == 0,
            ),
            (
                "unique_metered_event_ids",
                "SELECT count(*) - count(DISTINCT event_id) FROM metered_usage",
                lambda value: value == 0,
            ),
            (
                "all_metered_events_rated",
                (
                    "SELECT (SELECT count(*) FROM metered_usage) "
                    "- (SELECT count(*) FROM rated_usage)"
                ),
                lambda value: value == 0,
            ),
            (
                "balanced_journals",
                (
                    "SELECT count(*) FROM (SELECT journal_id FROM general_ledger "
                    "GROUP BY journal_id HAVING sum(debit) <> sum(credit))"
                ),
                lambda value: value == 0,
            ),
        ]
        results: list[dict[str, Any]] = []
        for name, query, predicate in checks:
            observed = self.connection.execute(query).fetchone()[0]
            status = "PASS" if predicate(observed) else "FAIL"
            self.connection.execute(
                "INSERT INTO quality_results VALUES (?, ?, ?, ?, current_timestamp)",
                [run_id, name, status, str(observed)],
            )
            results.append({"check": name, "status": status, "observed": observed})
        return results

    def reconcile(self, run_id: str | None = None) -> list[dict[str, Any]]:
        run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        reconciliations = [
            (
                "rated_usage_to_invoices",
                "SELECT coalesce(sum(total_charge), 0) FROM rated_usage",
                "SELECT coalesce(sum(invoice_amount), 0) FROM invoices",
            ),
            (
                "invoices_to_revenue_ledger",
                "SELECT coalesce(sum(invoice_amount), 0) FROM invoices",
                (
                    "SELECT coalesce(sum(credit), 0) FROM general_ledger "
                    "WHERE account = 'Usage Revenue'"
                ),
            ),
        ]
        results: list[dict[str, Any]] = []
        for name, left_query, right_query in reconciliations:
            left = self.connection.execute(left_query).fetchone()[0]
            right = self.connection.execute(right_query).fetchone()[0]
            difference = left - right
            status = "PASS" if difference == 0 else "FAIL"
            self.connection.execute(
                """
                INSERT INTO reconciliation_results
                VALUES (?, ?, ?, ?, ?, ?, current_timestamp)
                """,
                [run_id, name, left, right, difference, status],
            )
            results.append(
                {
                    "reconciliation": name,
                    "left": str(left),
                    "right": str(right),
                    "difference": str(difference),
                    "status": status,
                }
            )
        return results

    def summary(self) -> dict[str, Any]:
        metrics = self.connection.execute(
            """
            SELECT
                (SELECT count(*) FROM raw_usage_events) AS raw_events,
                (SELECT count(*) FROM metered_usage) AS unique_metered_events,
                (SELECT count(*) FROM metered_usage WHERE is_late) AS late_events,
                (SELECT count(*) FROM invoices) AS invoices,
                (SELECT coalesce(sum(invoice_amount), 0) FROM invoices) AS revenue
            """
        ).fetchone()
        return {
            "raw_events": metrics[0],
            "unique_metered_events": metrics[1],
            "duplicate_events_removed": metrics[0] - metrics[1],
            "late_events": metrics[2],
            "invoices": metrics[3],
            "revenue": str(metrics[4]),
        }

    def run_demo(self) -> dict[str, Any]:
        self.initialize()
        self.load_prices()
        self.ingest(demo_events())
        self.transform()
        quality = self.validate()
        reconciliation = self.reconcile()
        return {
            "summary": self.summary(),
            "quality": quality,
            "reconciliation": reconciliation,
        }

    def close(self) -> None:
        self.connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the AI usage-to-revenue metering demo."
    )
    parser.add_argument("--database", default="data/monetization.duckdb")
    parser.add_argument("--output", default="data/output/run_summary.json")
    args = parser.parse_args()

    database = Path(args.database)
    output = Path(args.output)
    database.parent.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)

    pipeline = MonetizationPipeline(database)
    try:
        result = pipeline.run_demo()
        output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2))
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
