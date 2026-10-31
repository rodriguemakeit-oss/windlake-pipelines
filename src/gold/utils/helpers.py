from pyspark.sql import functions as F


def reorder_columns(df, first_cols):
    cols = df.columns
    last_cols = [col for col in cols if col not in first_cols]
    return df.select(first_cols + last_cols)


def drop_months_after_report(df, date_col="date", report_col="reporting_date",
                             tolerance_months=1):
    """Drop GenAI rows dated after the report month (impossible values).

    A report cannot carry availability for a month after its own month. Such
    rows come from LLM year/month mislabelling of rolling 12-month tables (e.g.
    the Alisio Sept-2025 report extracted as 2025-10..2026-09). They then win
    the per-month dedup downstream for months with no other source.
    ``tolerance_months`` keeps reports filed one folder-month early
    (``reporting_date`` is derived from the folder). Rows without a
    ``reporting_date`` are kept. ``date_col`` is a 'yyyy-MM' string.
    """
    limit = F.date_format(F.add_months(F.col(report_col), tolerance_months), "yyyy-MM")
    return df.filter(F.col(report_col).isNull() | (F.col(date_col) <= limit))