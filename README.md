# 🔍 Data Pipeline Quality Framework

A production-style data quality framework built with **Python + SQLite** that automatically validates pipeline data across four critical dimensions.

## What It Does

| Check | What It Catches |
|---|---|
| **DuplicateCheck** | Duplicate records on key columns |
| **DataTypeCheck** | Non-numeric values in numeric columns |
| **RangeCheck** | Values outside expected min/max bounds |
| **FreshnessCheck** | Stale data beyond a time threshold |

Outputs a clean **HTML report** with pass/fail status, metrics, and sample failing row IDs.

## Project Structure

```
data_quality_framework/
├── main.py               # Entry point — run this
├── seed_database.py      # Creates SQLite DB with injected issues
├── pipeline.py           # Orchestrates all checks
├── checks/
│   ├── base.py           # Abstract BaseCheck + CheckResult dataclass
│   └── checks.py         # All four check implementations
├── reports/
│   └── report.py         # HTML report generator
├── tests/
│   └── test_checks.py    # pytest unit tests (16 tests)
└── data/
    └── pipeline.db       # Auto-generated SQLite database
```

## Quick Start

```bash
# 1. Run the full pipeline
python main.py

# 2. Run tests
pytest tests/ -v

# 3. Open the HTML report
open reports/report.html
```

## How to Add a New Check

1. Create a class in `checks/checks.py` that inherits from `BaseCheck`
2. Implement the `run(self, conn) -> CheckResult` method
3. Add an instance to the `build_checks()` list in `pipeline.py`
4. Write tests in `tests/test_checks.py`

## Tech Stack

- **Python 3.10+**
- **SQLite** (swap for PostgreSQL by changing the connection string)
- **pytest** for unit testing
- Plain HTML/CSS for reports (no external dependencies)

## Skills Demonstrated

- ETL data validation patterns
- Object-oriented Python (abstract classes, dataclasses)
- SQL window functions, aggregations, type casting
- pytest fixtures, parametrization, test isolation
- HTML report generation
- Production engineering mindset (configurable thresholds, warn vs fail modes)

---
Built by **Aditya Gaur** | BI & Data Analyst | [github.com/adityaladi7](https://github.com/adityaladi7)
