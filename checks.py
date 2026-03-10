"""
checks/checks.py
Four production-grade data quality checks:
  1. DuplicateCheck      — detects duplicate rows on key columns
  2. DataTypeCheck       — validates a column contains only numeric values
  3. RangeCheck          — flags values outside an expected min/max band
  4. FreshnessCheck      — ensures a timestamp column is recent enough
"""

from .base import BaseCheck, CheckResult


# ── 1. DUPLICATE CHECK ──────────────────────────────────────────────────────
class DuplicateCheck(BaseCheck):
    """
    Detects duplicate records based on one or more key columns.

    Config:
        key_columns (list[str])  : columns that together should be unique
        max_duplicates (int)     : acceptable duplicate count (default 0)
    """

    def run(self, conn) -> CheckResult:
        key_cols   = self.config.get("key_columns", [self.column])
        max_dups   = self.config.get("max_duplicates", 0)
        col_list   = ", ".join(key_cols)

        sql = f"""
            SELECT {col_list}, COUNT(*) AS cnt
            FROM   {self.table}
            GROUP  BY {col_list}
            HAVING COUNT(*) > 1
        """
        rows = self._fetch(conn, sql)
        dup_groups  = len(rows)
        extra_rows  = sum(r[-1] - 1 for r in rows)   # total surplus rows

        # grab sample IDs for report
        sample = [str(r[0]) for r in rows[:5]]

        status = "PASS" if dup_groups <= max_dups else "FAIL"

        return CheckResult(
            check_name   = "DuplicateCheck",
            table        = self.table,
            column       = col_list,
            status       = status,
            metric       = dup_groups,
            threshold    = f"≤ {max_dups} duplicate groups",
            failing_rows = extra_rows,
            details      = (
                f"{dup_groups} duplicate groups found "
                f"({extra_rows} surplus rows)."
                if dup_groups else "No duplicates found."
            ),
            sample_ids   = sample,
        )


# ── 2. DATA TYPE CHECK ───────────────────────────────────────────────────────
class DataTypeCheck(BaseCheck):
    """
    Validates that all non-null values in a column are numeric.

    Config:
        expected_type (str) : "numeric" (more types can be added)
    """

    def run(self, conn) -> CheckResult:
        expected = self.config.get("expected_type", "numeric")

        if expected == "numeric":
            # CAST trick: non-numeric strings cast to NULL in SQLite
            sql = f"""
                SELECT {self.table}.rowid, {self.column}
                FROM   {self.table}
                WHERE  {self.column} IS NOT NULL
                  AND  CAST({self.column} AS REAL) IS NULL
            """
        else:
            raise ValueError(f"Unsupported expected_type: {expected}")

        rows        = self._fetch(conn, sql)
        failing     = len(rows)
        sample      = [str(r[0]) for r in rows[:5]]

        status = "PASS" if failing == 0 else "FAIL"

        return CheckResult(
            check_name   = "DataTypeCheck",
            table        = self.table,
            column       = self.column,
            status       = status,
            metric       = failing,
            threshold    = f"0 non-{expected} values",
            failing_rows = failing,
            details      = (
                f"{failing} rows have non-{expected} values in '{self.column}'."
                if failing else
                f"All values in '{self.column}' are {expected}."
            ),
            sample_ids   = sample,
        )


# ── 3. RANGE CHECK ───────────────────────────────────────────────────────────
class RangeCheck(BaseCheck):
    """
    Flags rows where a numeric column falls outside [min_val, max_val].

    Config:
        min_val (float)         : inclusive lower bound
        max_val (float)         : inclusive upper bound
        warn_only (bool)        : if True, status = WARN instead of FAIL
        id_column (str)         : column to use for sample IDs
    """

    def run(self, conn) -> CheckResult:
        min_val    = self.config.get("min_val", 0)
        max_val    = self.config.get("max_val", float("inf"))
        warn_only  = self.config.get("warn_only", False)
        id_col     = self.config.get("id_column", "rowid")

        # only check rows where value is actually numeric
        sql = f"""
            SELECT {id_col}, CAST({self.column} AS REAL) AS val
            FROM   {self.table}
            WHERE  CAST({self.column} AS REAL) IS NOT NULL
              AND  (
                     CAST({self.column} AS REAL) < {min_val}
                  OR CAST({self.column} AS REAL) > {max_val}
              )
        """
        rows    = self._fetch(conn, sql)
        failing = len(rows)
        sample  = [str(r[0]) for r in rows[:5]]

        if failing == 0:
            status = "PASS"
        elif warn_only:
            status = "WARN"
        else:
            status = "FAIL"

        return CheckResult(
            check_name   = "RangeCheck",
            table        = self.table,
            column       = self.column,
            status       = status,
            metric       = failing,
            threshold    = f"values in [{min_val}, {max_val}]",
            failing_rows = failing,
            details      = (
                f"{failing} rows have '{self.column}' outside [{min_val}, {max_val}]."
                if failing else
                f"All values in '{self.column}' are within range."
            ),
            sample_ids   = sample,
        )


# ── 4. FRESHNESS CHECK ───────────────────────────────────────────────────────
class FreshnessCheck(BaseCheck):
    """
    Ensures the most-recent value in a datetime column is within
    max_age_hours of NOW.

    Config:
        max_age_hours (int)     : e.g. 48 means data must be < 48 h old
        warn_only (bool)        : if True, status = WARN instead of FAIL
    """

    def run(self, conn) -> CheckResult:
        max_age   = self.config.get("max_age_hours", 24)
        warn_only = self.config.get("warn_only", False)

        sql = f"""
            SELECT
                MAX({self.column})                          AS latest,
                COUNT(*)                                    AS stale_rows,
                ROUND(
                  (JULIANDAY('now') - JULIANDAY(MAX({self.column}))) * 24,
                  1
                )                                           AS age_hours
            FROM {self.table}
        """
        row = self._fetch(conn, sql)[0]
        latest, stale_rows, age_hours = row

        # Also count individual stale rows
        stale_sql = f"""
            SELECT COUNT(*)
            FROM   {self.table}
            WHERE  (JULIANDAY('now') - JULIANDAY({self.column})) * 24 > {max_age}
        """
        stale_count = self._fetch(conn, stale_sql)[0][0]

        if age_hours is None:
            status  = "WARN"
            details = f"No data found in '{self.column}'."
        elif age_hours <= max_age:
            status  = "PASS"
            details = f"Latest record is {age_hours}h old — within {max_age}h threshold."
        else:
            status  = "WARN" if warn_only else "FAIL"
            details = (
                f"Latest record in '{self.column}' is {age_hours}h old "
                f"(threshold: {max_age}h). {stale_count} stale rows detected."
            )

        return CheckResult(
            check_name   = "FreshnessCheck",
            table        = self.table,
            column       = self.column,
            status       = status,
            metric       = f"{age_hours}h",
            threshold    = f"≤ {max_age}h",
            failing_rows = stale_count if age_hours and age_hours > max_age else 0,
            details      = details,
            sample_ids   = [],
        )
