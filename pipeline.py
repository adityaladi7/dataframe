"""
pipeline.py
Orchestrates all quality checks and returns a structured report.
"""

import sqlite3
from checks import DuplicateCheck, DataTypeCheck, RangeCheck, FreshnessCheck


# ── CONFIGURE YOUR CHECKS HERE ──────────────────────────────────────────────
def build_checks():
    return [

        # ── orders table ────────────────────────────────────────────────────

        DuplicateCheck(
            table      = "orders",
            column     = "order_id",
            key_columns= ["order_id"],
        ),

        DataTypeCheck(
            table         = "orders",
            column        = "amount",
            expected_type = "numeric",
        ),

        RangeCheck(
            table     = "orders",
            column    = "amount",
            min_val   = 0.01,
            max_val   = 10000,
            id_column = "order_id",
        ),

        FreshnessCheck(
            table         = "orders",
            column        = "updated_at",
            max_age_hours = 48,
        ),

        # ── customers table ─────────────────────────────────────────────────

        DuplicateCheck(
            table      = "customers",
            column     = "customer_id",
            key_columns= ["customer_id"],
        ),

        DuplicateCheck(
            table      = "customers",
            column     = "email",
            key_columns= ["email"],
        ),

    ]


# ── RUNNER ───────────────────────────────────────────────────────────────────
def run_pipeline(db_path: str = "data/pipeline.db") -> dict:
    conn    = sqlite3.connect(db_path)
    checks  = build_checks()
    results = []

    print("\n🔍  Running Data Quality Checks...\n" + "─" * 50)

    for check in checks:
        result = check.run(conn)
        results.append(result)

        icon = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️ "}.get(result.status, "?")
        print(f"{icon}  [{result.status}]  {result.check_name} — {result.table}.{result.column or ''}")
        print(f"      {result.details}")
        if result.sample_ids:
            print(f"      Sample IDs: {', '.join(result.sample_ids[:3])}")
        print()

    conn.close()

    total  = len(results)
    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status == "FAIL")
    warned = sum(1 for r in results if r.status == "WARN")

    print("─" * 50)
    print(f"📊  Summary: {passed}/{total} passed  |  {failed} failed  |  {warned} warnings\n")

    return {
        "results":  results,
        "total":    total,
        "passed":   passed,
        "failed":   failed,
        "warned":   warned,
        "score":    round(passed / total * 100, 1),
    }


if __name__ == "__main__":
    run_pipeline()
