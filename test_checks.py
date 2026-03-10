"""
tests/test_checks.py
Unit tests for all four data quality checks.
Uses in-memory SQLite so tests are fast and isolated.
"""

import sqlite3
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from checks import DuplicateCheck, DataTypeCheck, RangeCheck, FreshnessCheck


# ── FIXTURES ────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_db():
    """In-memory DB with perfectly clean orders data."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE orders (
            order_id   TEXT,
            amount     REAL,
            updated_at TEXT
        );
        INSERT INTO orders VALUES ('ORD001', 100.0,  datetime('now','-1 hour'));
        INSERT INTO orders VALUES ('ORD002', 250.5,  datetime('now','-2 hours'));
        INSERT INTO orders VALUES ('ORD003', 49.99,  datetime('now','-30 minutes'));
        INSERT INTO orders VALUES ('ORD004', 3500.0, datetime('now','-5 hours'));
    """)
    yield conn
    conn.close()


@pytest.fixture
def dirty_db():
    """In-memory DB with all four types of quality issues injected."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE orders (
            order_id   TEXT,
            amount     TEXT,
            updated_at TEXT
        );
        -- Clean rows
        INSERT INTO orders VALUES ('ORD001', '100.0',  datetime('now','-1 hour'));
        INSERT INTO orders VALUES ('ORD002', '250.5',  datetime('now','-2 hours'));
        -- Duplicate
        INSERT INTO orders VALUES ('ORD001', '100.0',  datetime('now','-1 hour'));
        -- Bad data type
        INSERT INTO orders VALUES ('ORD003', 'N/A',    datetime('now','-1 hour'));
        -- Out of range (negative)
        INSERT INTO orders VALUES ('ORD004', '-50.0',  datetime('now','-1 hour'));
        -- Stale (72 hours old)
        INSERT INTO orders VALUES ('ORD005', '75.0',   datetime('now','-72 hours'));
    """)
    yield conn
    conn.close()


# ── DUPLICATE CHECK TESTS ────────────────────────────────────────────────────

class TestDuplicateCheck:

    def test_passes_when_no_duplicates(self, clean_db):
        check = DuplicateCheck(table="orders", key_columns=["order_id"])
        result = check.run(clean_db)
        assert result.status == "PASS"
        assert result.failing_rows == 0

    def test_fails_when_duplicates_exist(self, dirty_db):
        check = DuplicateCheck(table="orders", key_columns=["order_id"])
        result = check.run(dirty_db)
        assert result.status == "FAIL"
        assert result.failing_rows >= 1

    def test_metric_reflects_duplicate_groups(self, dirty_db):
        check = DuplicateCheck(table="orders", key_columns=["order_id"])
        result = check.run(dirty_db)
        assert result.metric >= 1   # at least 1 group of duplicates

    def test_sample_ids_populated_on_fail(self, dirty_db):
        check = DuplicateCheck(table="orders", key_columns=["order_id"])
        result = check.run(dirty_db)
        assert len(result.sample_ids) > 0


# ── DATA TYPE CHECK TESTS ────────────────────────────────────────────────────

class TestDataTypeCheck:

    def test_passes_on_clean_numeric_column(self, clean_db):
        check = DataTypeCheck(table="orders", column="amount", expected_type="numeric")
        result = check.run(clean_db)
        assert result.status == "PASS"
        assert result.failing_rows == 0

    def test_fails_when_non_numeric_values_present(self, dirty_db):
        check = DataTypeCheck(table="orders", column="amount", expected_type="numeric")
        result = check.run(dirty_db)
        assert result.status == "FAIL"
        assert result.failing_rows >= 1

    def test_failing_rows_count_is_accurate(self, dirty_db):
        check = DataTypeCheck(table="orders", column="amount", expected_type="numeric")
        result = check.run(dirty_db)
        # We injected exactly 1 "N/A" value
        assert result.failing_rows == 1


# ── RANGE CHECK TESTS ────────────────────────────────────────────────────────

class TestRangeCheck:

    def test_passes_when_all_values_in_range(self, clean_db):
        check = RangeCheck(table="orders", column="amount", min_val=0.01, max_val=10000)
        result = check.run(clean_db)
        assert result.status == "PASS"

    def test_fails_on_negative_values(self, dirty_db):
        check = RangeCheck(table="orders", column="amount", min_val=0.01, max_val=10000)
        result = check.run(dirty_db)
        assert result.status == "FAIL"
        assert result.failing_rows >= 1

    def test_warn_only_mode(self, dirty_db):
        check = RangeCheck(
            table="orders", column="amount",
            min_val=0.01, max_val=10000,
            warn_only=True
        )
        result = check.run(dirty_db)
        # Should warn, not fail
        assert result.status == "WARN"

    def test_threshold_string_in_result(self, clean_db):
        check = RangeCheck(table="orders", column="amount", min_val=1, max_val=5000)
        result = check.run(clean_db)
        assert "1" in result.threshold
        assert "5000" in result.threshold


# ── FRESHNESS CHECK TESTS ────────────────────────────────────────────────────

class TestFreshnessCheck:

    def test_passes_when_data_is_recent(self, clean_db):
        check = FreshnessCheck(table="orders", column="updated_at", max_age_hours=24)
        result = check.run(clean_db)
        assert result.status == "PASS"

    def test_fails_when_data_is_stale(self, dirty_db):
        # dirty_db has a row that's 72 hours old
        check = FreshnessCheck(table="orders", column="updated_at", max_age_hours=48)
        result = check.run(dirty_db)
        # The most recent record in dirty_db is only 1h old, so latest passes —
        # but stale_count should show rows older than threshold
        assert result.failing_rows >= 1

    def test_very_tight_threshold_fails(self, clean_db):
        # Threshold of 0 hours — everything should fail
        check = FreshnessCheck(table="orders", column="updated_at", max_age_hours=0)
        result = check.run(clean_db)
        assert result.status in ("FAIL", "WARN")

    def test_metric_contains_hours(self, clean_db):
        check = FreshnessCheck(table="orders", column="updated_at", max_age_hours=24)
        result = check.run(clean_db)
        assert "h" in str(result.metric)
