from decimal import Decimal

import pytest

from monetization_pipeline.pipeline import MonetizationPipeline, demo_events


@pytest.fixture()
def pipeline():
    instance = MonetizationPipeline()
    instance.initialize()
    instance.load_prices()
    yield instance
    instance.close()


def test_duplicate_events_are_metered_once(pipeline):
    pipeline.ingest(demo_events())
    pipeline.transform()

    summary = pipeline.summary()

    assert summary["raw_events"] == 4
    assert summary["unique_metered_events"] == 3
    assert summary["duplicate_events_removed"] == 1


def test_late_event_is_retained_and_flagged(pipeline):
    pipeline.ingest(demo_events())
    pipeline.transform()

    event = pipeline.connection.execute(
        "SELECT event_id, is_late FROM metered_usage WHERE event_id = 'evt-003'"
    ).fetchone()

    assert event == ("evt-003", True)


def test_effective_price_rating_is_exact(pipeline):
    pipeline.ingest(demo_events())
    pipeline.transform()

    charge = pipeline.connection.execute(
        "SELECT total_charge FROM rated_usage WHERE event_id = 'evt-001'"
    ).fetchone()[0]

    assert charge == Decimal("7.00000000")


def test_revenue_reconciles_penny_for_penny(pipeline):
    pipeline.ingest(demo_events())
    pipeline.transform()

    results = pipeline.reconcile("test-reconciliation")

    assert all(result["status"] == "PASS" for result in results)
    assert all(Decimal(result["difference"]) == Decimal(0) for result in results)


def test_general_ledger_is_balanced(pipeline):
    pipeline.ingest(demo_events())
    pipeline.transform()

    imbalance = pipeline.connection.execute(
        "SELECT sum(debit) - sum(credit) FROM general_ledger"
    ).fetchone()[0]

    assert imbalance == Decimal("0E-8")


def test_pipeline_is_idempotent(pipeline):
    pipeline.ingest(demo_events())
    pipeline.transform()
    first_summary = pipeline.summary()

    pipeline.transform()
    second_summary = pipeline.summary()

    assert first_summary == second_summary


def test_all_quality_controls_pass(pipeline):
    pipeline.ingest(demo_events())
    pipeline.transform()

    results = pipeline.validate("test-quality")

    assert len(results) == 5
    assert all(result["status"] == "PASS" for result in results)
