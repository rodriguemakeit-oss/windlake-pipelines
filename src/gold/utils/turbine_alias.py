"""Report-label → Telemetry serial mapping used in Phase 1 of the GenAI match.

Reports name turbines with SCADA labels (WTG1_01, A01) instead of the Telemetry
serial. The business maintains the crosswalk in silver.dim_turbine_report_alias.
Join key is (country, report label) — farm is not available on the GenAI side.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

COUNTRY_TO_PIC_CODE = {
    "belgium": "BE",
    "spain": "ES",
    "portfolio green": "FR",
    "germany": "DE",
    "italy": "IT",
    "netherlands": "NL",
    "poland": "PL",
    "romania": "RO",
    "united kingdom": "GB",
    "portugal": "PT",
    "australia": "AU",
    "south africa": "ZA",
}


def pic_country_col(country_col):
    """Map a country name to its PIC code, UNKNOWN fallback."""
    pairs = []
    for name, code in COUNTRY_TO_PIC_CODE.items():
        pairs.extend([F.lit(name), F.lit(code)])
    mapping = F.create_map(*pairs)
    return F.coalesce(mapping[F.lower(F.trim(country_col))], F.lit("UNKNOWN"))


def label_key(col):
    """Canonical join key for a turbine label.

    Both the maintainer report (``turbine_id``) and the SGRE mapping
    (``report_turbine_name``) sometimes carry a trailing serial in parentheses
    or after a pipe (e.g. ``"AG01 (61028550)"``, ``"AE1|61004509"``), and that
    suffix may be present on only one side. Strip everything from the first
    ``(`` or ``|``, then trim and upper-case, so both sides collapse to the same
    key (``"AG01"``) regardless of which side carries the serial.
    """
    return F.upper(F.trim(F.regexp_replace(col, r"\s*[(|].*$", "")))


def serial_key(col):
    """Canonical join key for a report serial carried by a turbine label.

    Complements :func:`label_key`. The SGRE mapping stores the report serial
    inside parentheses (``"A01 (61010441)"``) while the maintainer extraction
    sometimes emits only that bare serial (``"61010441"``) with no label. This
    extracts the serial from either side — the parenthesised token, the token
    after a pipe, or the whole value — and keeps it only when it is a plausible
    numeric serial (>= 4 digits). Returns null for pure labels or model codes
    (e.g. ``"A12 | G87"`` → null), so the serial join never collides with a
    model/label token.
    """
    paren = F.regexp_extract(col, r"\(([^)]*)\)", 1)
    pipe = F.regexp_extract(col, r"\|\s*(.+)$", 1)
    candidate = (
        F.when(paren != "", paren)
        .when(pipe != "", pipe)
        .otherwise(F.trim(col))
    )
    candidate = F.regexp_replace(candidate, r"\s+", "")
    return F.when(
        candidate.rlike(r"^[0-9]{4,}$"), candidate
    ).otherwise(F.lit(None).cast("string"))


def site_file_key(file_col):
    """Normalise a report file path into a stable per-site key (option B).

    Keeps only the alphabetic tokens of the file basename after stripping the
    extension, dates and month names, so monthly variants of the same site's
    report collapse to the same key (e.g. 'PE de Fafe 2026-01' and
    'PE de Fafe 2026-02' → 'pe de fafe'). Used as the cache key for LLM
    disambiguation so a given site is resolved only once.
    """
    base = F.element_at(F.split(F.coalesce(file_col, F.lit("")), "/"), -1)
    s = F.lower(base)
    s = F.regexp_replace(s, r"\.[a-z0-9]+$", " ")                      # extension
    s = F.regexp_replace(s, r"[0-9]{4}[-_./ ][0-9]{1,2}([-_./ ][0-9]{1,2})?", " ")  # dates
    s = F.regexp_replace(s, r"(19|20)[0-9]{2}", " ")                  # bare years
    s = F.regexp_replace(
        s,
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|january|february|"
        r"march|april|june|july|august|september|october|november|december|"
        r"enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|"
        r"noviembre|diciembre)\b",
        " ",
    )
    s = F.regexp_replace(s, r"[^a-z]+", " ")                          # keep letters only
    s = F.regexp_replace(s, r"\s+", " ")
    return F.trim(s)


def attach_alias_serial(df_genai: DataFrame, df_alias: DataFrame,
                        df_resolution: DataFrame = None) -> DataFrame:
    """Add pdf_country and alias_serial (Telemetry serial, null when unmapped).

    Unambiguous ``(country, label)`` keys are joined directly. Keys that map to
    several serials (same label reused across farms) are resolved per report file
    via ``df_resolution`` (the LLM cache, keyed on country/label/file_key).
    """
    df_genai = (
        df_genai
        .withColumn("pdf_country", pic_country_col(F.col("country")))
        .withColumn("_file_key", site_file_key(F.col("file")))
        # Canonical label: strips any " (serial)" / "| serial" suffix so the
        # report label joins the alias regardless of which side carries the serial.
        .withColumn("_label", label_key(F.col("turbine_id")))
        # Canonical report serial: set when the extraction emitted the bare
        # Gamesa serial (e.g. "61010441") instead of the "A01" label.
        .withColumn("_serial", serial_key(F.col("turbine_id")))
    )

    alias_keyed = df_alias.select(
        pic_country_col(F.col("country")).alias("_a_cc"),
        label_key(F.col("report_turbine_name")).alias("_a_label"),
        serial_key(F.col("report_turbine_name")).alias("_a_skey"),
        F.col("serial_number").cast("string").alias("_a_serial"),
    )

    # Unambiguous keys = exactly one distinct serial for the (country, label)
    key_counts = alias_keyed.groupBy("_a_cc", "_a_label").agg(
        F.countDistinct("_a_serial").alias("_n")
    )
    unique_alias = (
        alias_keyed
        .join(key_counts, ["_a_cc", "_a_label"], "inner")
        .filter(F.col("_n") == 1)
        .select("_a_cc", "_a_label", "_a_serial")
        .dropDuplicates(["_a_cc", "_a_label"])
    )

    # Same idea keyed on the report serial (for extractions that emit the bare
    # serial with no usable label). Unambiguous (country, report_serial) only.
    skey_counts = (
        alias_keyed
        .filter(F.col("_a_skey").isNotNull())
        .groupBy("_a_cc", "_a_skey")
        .agg(F.countDistinct("_a_serial").alias("_ns"))
    )
    unique_alias_serial = (
        alias_keyed
        .filter(F.col("_a_skey").isNotNull())
        .join(skey_counts, ["_a_cc", "_a_skey"], "inner")
        .filter(F.col("_ns") == 1)
        .select("_a_cc", "_a_skey", F.col("_a_serial").alias("_a_serial_bys"))
        .dropDuplicates(["_a_cc", "_a_skey"])
    )

    df = (
        df_genai
        .join(
            F.broadcast(unique_alias),
            (F.col("pdf_country") == F.col("_a_cc"))
            & (F.col("_label") == F.col("_a_label")),
            "left",
        )
        .withColumnRenamed("_a_serial", "_unique_serial_bylabel")
        .drop("_a_cc", "_a_label")
        .join(
            F.broadcast(unique_alias_serial),
            (F.col("pdf_country") == F.col("_a_cc"))
            & (F.col("_serial") == F.col("_a_skey")),
            "left",
        )
        .drop("_a_cc", "_a_skey")
        # Prefer the label match; fall back to the report-serial match.
        .withColumn(
            "_unique_serial",
            F.coalesce(F.col("_unique_serial_bylabel"), F.col("_a_serial_bys")),
        )
        .drop("_unique_serial_bylabel", "_a_serial_bys")
    )

    # Ambiguous keys: resolved per file via the LLM cache (country, label, file_key)
    if df_resolution is not None:
        res = (
            df_resolution
            .select(
                F.col("country_pic").alias("_r_cc"),
                label_key(F.col("report_turbine_name")).alias("_r_label"),
                F.col("file_key").alias("_r_file_key"),
                F.col("resolved_serial").cast("string").alias("_r_serial"),
            )
            .filter(F.col("_r_serial").isNotNull())
        )
        df = (
            df
            .join(
                F.broadcast(res),
                (F.col("pdf_country") == F.col("_r_cc"))
                & (F.col("_label") == F.col("_r_label"))
                & (F.col("_file_key") == F.col("_r_file_key")),
                "left",
            )
            .withColumnRenamed("_r_serial", "_resolved_serial")
            .drop("_r_cc", "_r_label", "_r_file_key")
        )
    else:
        df = df.withColumn("_resolved_serial", F.lit(None).cast("string"))

    return (
        df
        .withColumn(
            "alias_serial",
            F.coalesce(F.col("_unique_serial"), F.col("_resolved_serial")),
        )
        .drop("_unique_serial", "_resolved_serial", "_file_key", "_label", "_serial")
    )


def clean_serial_col(col_expr):
    """Strip whitespace, everything up to 'WEA', then 'SEN' and 'NX'."""
    return (
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(col_expr, r"\s+", ""),
                    r".*WEA", "",
                ),
                "SEN", "",
            ),
            "NX", "",
        )
    )


def prepare_turbine_match_keys(df_genai: DataFrame, df_alias: DataFrame,
                              df_resolution: DataFrame = None) -> DataFrame:
    """Build the serial match keys for Phase 2, using the alias serial when mapped."""
    df = attach_alias_serial(df_genai, df_alias, df_resolution)

    df = (
        df
        .withColumnRenamed("value", "maintainer_availability")
        .withColumnRenamed("date", "maintainer_date")
        .withColumn("maintainer_year", F.substring("maintainer_date", 1, 4))
        .withColumn("maintainer_month", F.substring("maintainer_date", 6, 2))
        .withColumn("_sn_split", F.split(F.col("turbine_id"), r"\|"))
        .withColumn(
            "serial_number_1",
            clean_serial_col(
                F.coalesce(F.col("alias_serial"), F.trim(F.col("_sn_split").getItem(0)))
            ),
        )
        .withColumn(
            "serial_number_2",
            F.when(
                F.size(F.col("_sn_split")) > 1,
                clean_serial_col(F.trim(F.col("_sn_split").getItem(1))),
            ).otherwise(F.lit(None).cast("string")),
        )
        .drop("_sn_split")
        .withColumn(
            "serial_number_1_country",
            F.concat(F.col("serial_number_1"), F.lit("_WIF_"), F.col("pdf_country")),
        )
        .withColumn(
            "serial_number_2_country",
            F.when(
                F.col("serial_number_2").isNotNull(),
                F.concat(F.col("serial_number_2"), F.lit("_WIF_"), F.col("pdf_country")),
            ).otherwise(F.lit(None).cast("string")),
        )
    )
    return df
