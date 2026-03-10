"""
main.py
Entry point — seeds DB, runs all checks, generates HTML report.
Run: python main.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from seed_database import seed
from pipeline import run_pipeline
from reports.report import generate_html_report


def main():
    print("=" * 50)
    print("  DATA PIPELINE QUALITY FRAMEWORK")
    print("  github.com/adityaladi7")
    print("=" * 50)

    # Step 1 — seed / refresh the database
    print("\n📦  Step 1: Seeding database with sample data...")
    seed()

    # Step 2 — run all quality checks
    print("\n🔍  Step 2: Running quality checks...")
    summary = run_pipeline()

    # Step 3 — generate HTML report
    print("📄  Step 3: Generating HTML report...")
    report_path = generate_html_report(summary)

    print("\n" + "=" * 50)
    print(f"✅  Done! Quality score: {summary['score']}%")
    print(f"    Open your report: {report_path}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()
