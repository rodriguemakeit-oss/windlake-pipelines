# Databricks notebook source
# MAGIC %md
# MAGIC # Comparaison Dataiku → Databricks
# MAGIC
# MAGIC Ce notebook compare les datasets de l'ancien projet Dataiku (stockés dans des Volumes Unity Catalog) avec les datasets générés par le nouveau pipeline Databricks.
# MAGIC
# MAGIC Pour chaque dataset :
# MAGIC 1. **Comparaison de schéma** — colonnes manquantes / en plus
# MAGIC 2. **Comparaison de valeurs** — stats d'égalité par colonne (tolérance ±0.01 pour les numériques)
# MAGIC 3. **Détail des lignes différentes** — quelles colonnes diffèrent ligne par ligne
# MAGIC 4. **Score de proximité global** — résumé pondéré par nombre de lignes

# COMMAND ----------

# ─── WIDGETS ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog", "ltsa_dev", "Catalog (ltsa_dev / ltsa_prod)")
dbutils.widgets.text("gold_bucket", "demo-data-bucket", "Gold S3 bucket name")

catalog = dbutils.widgets.get("catalog")
gold_bucket = dbutils.widgets.get("gold_bucket")

print(f"catalog    : {catalog}")
print(f"gold_bucket: {gold_bucket}")

# COMMAND ----------

# ─── IMPORTS & CONSTANTES ───────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, DecimalType, MapType, DateType, TimestampType

DATAIKU_VOLUME_ROOT = "/Volumes/ltsa_dataiku_old_project/default"
NUMERIC_TOLERANCE   = 0.01
MAX_DIFF_ROWS       = 50     # nb max de lignes différentes affichées par dataset
ROLLING_MONTHS      = 13    # fenêtre glissante : seuls les N derniers mois sont comparés pour les datasets availability
EXCLUDE_COLS        = {"run_date"}                 # colonnes exclues de toute comparaison
JSON_DICT_COLS      = {"computed_availabilities"}  # colonnes JSON dict à comparer clé par clé
NORMALIZE_PATH_COLS = {"file"}                    # colonnes path : strip "/" initial avant comparaison


# COMMAND ----------

# ─── CONFIGURATION DES DATASETS ─────────────────────────────────────────────
#
# Chaque entrée décrit :
#   dataiku_path   : chemin du Volume Dataiku
#                    Dataiku stocke les fichiers parquet dans /managed
#   databricks_ref : chemin S3 relatif au bucket  (is_s3=True)
#                    ou table 3-part Unity Catalog (is_s3=False, pour les tables
#                    sans sortie S3 : turbine/substation/farm_production)
#   join_keys      : colonnes utilisées pour la jointure de comparaison
#   is_s3          : True -> lecture delta S3 | False -> spark.table() catalog
#   enrich_dbr     : join optionnel pour résoudre une FK surrogate

DATASETS = {
    "wind_turbine_ltsa_scope": {
        "dataiku_path": f"{DATAIKU_VOLUME_ROOT}/wind_turbine_ltsa_scope",
        "databricks_ref": "wind_turbine_ltsa_scope/data",
        "join_keys": ["wind_turbine_code"],
        "is_s3": True,
        "enrich_dbr": None,
        "col_tolerances": None,
    },
    "turbine_availability": {
        "dataiku_path": f"{DATAIKU_VOLUME_ROOT}/turbine_availability",
        "databricks_ref": "turbine_availability/data",
        "join_keys": ["wind_turbine_code", "year", "month"],
        "is_s3": True,
        "enrich_dbr": None,
        "col_tolerances": None,
    },
    "substation_availability": {
        "dataiku_path": f"{DATAIKU_VOLUME_ROOT}/substation_availability",
        "databricks_ref": "substation_availability/data",
        "join_keys": ["substation_code", "year", "month"],
        "is_s3": True,
        "enrich_dbr": None,
        "col_tolerances": None,
    },
    "farm_availability": {
        "dataiku_path": f"{DATAIKU_VOLUME_ROOT}/farm_availability",
        "databricks_ref": "farm_availability/data",
        "join_keys": ["farm_code", "year", "month"],
        "is_s3": True,
        "enrich_dbr": None,
        "col_tolerances": None,
    },
    "turbine_production": {
        "dataiku_path": f"{DATAIKU_VOLUME_ROOT}/turbine_production",
        "databricks_ref": f"{catalog}.gold.turbine_production",
        "join_keys": ["wind_turbine_code", "year", "month"],
        "is_s3": False,
        "enrich_dbr": None,
        # production_kwh = sum of actpow_10min/6 over many intervals → floating drift
        # Tolerance: abs=10 kWh ou 0.5% relatif (gère petits et grands volumes)
        "col_tolerances": {"production_kwh": {"abs": 10.0, "rel": 0.005}},
    },
    "substation_production": {
        "dataiku_path": f"{DATAIKU_VOLUME_ROOT}/substation_production",
        "databricks_ref": f"{catalog}.gold.substation_production",
        "join_keys": ["substation_code", "year", "month"],
        "is_s3": False,
        "enrich_dbr": None,
        "col_tolerances": {"production_kwh": {"abs": 50.0, "rel": 0.005}},
    },
    "farm_production": {
        "dataiku_path": f"{DATAIKU_VOLUME_ROOT}/farm_production",
        "databricks_ref": f"{catalog}.gold.farm_production",
        "join_keys": ["farm_code", "year", "month"],
        "is_s3": False,
        "enrich_dbr": None,
        "col_tolerances": {"production_kwh": {"abs": 100.0, "rel": 0.005}},
    },
    "slb_completeness_turbine": {
        "dataiku_path": f"{DATAIKU_VOLUME_ROOT}/slb_completeness_turbine",
        "databricks_ref": "slb_completeness_turbine/data",
        "join_keys": ["wind_turbine_code", "slb_year", "slb_month"],
        "is_s3": True,
        "enrich_dbr": None,
        # Les durées en minutes accumulent des erreurs flottantes.
        # Quelques écarts résiduels jusqu'à ~10 min observés sur total_duration_min.
        "col_tolerances": {
            "total_duration_min":       15.0,  # minutes
            "missing_iec_duration_min":  1.0,  # minutes
            "completeness_percentage":   0.5,  # points de %
        },
        # NULL dans Dataiku = 100 % (aucune donnée manquante → complétude totale)
        "col_null_defaults": {
            "completeness_percentage": 100.0,
        },
    },
    "referential_guarantee_farm": {
        "dataiku_path": f"{DATAIKU_VOLUME_ROOT}/referential_guarantee_farm",
        "databricks_ref": "referential_guarantee_farm/data",
        "join_keys": ["farm_code", "contract_technical_id"],
        "is_s3": True,
        "enrich_dbr": None,
        # Les durées en minutes accumulent des erreurs flottantes.
        # Quelques écarts résiduels jusqu'à ~10 min observés sur total_duration_min.
        "col_tolerances": {
            "total_duration_min":       15.0,  # minutes
            "missing_iec_duration_min":  1.0,  # minutes
            "completeness_percentage":   0.5,  # points de %
        },
        # NULL dans Dataiku = 100 % (aucune donnée manquante → complétude totale)
        "col_null_defaults": {
            "completeness_percentage": 100.0,
        },
    },
}


# COMMAND ----------

# ─── FONCTIONS UTILITAIRES ───────────────────────────────────────────────────

import json as _json


def _is_numeric_type(dtype):
    """Retourne True si le type Spark est numérique (Double, Float, Decimal)."""
    return isinstance(dtype, (DoubleType, FloatType, DecimalType))


def _is_date_type(dtype):
    """Retourne True si le type Spark est une date ou un timestamp."""
    return isinstance(dtype, (DateType, TimestampType))


def _normalized_str(col_expr):
    """
    Normalise une colonne string pour la comparaison :
    - cast string
    - trim espaces
    - chaîne vide -> NULL (Dataiku exporte fréquemment NULL en "")
    Ainsi NULL / "" / "  " sont équivalents.
    """
    trimmed = F.trim(col_expr.cast("string"))
    return F.when(trimmed == F.lit(""), F.lit(None).cast("string")).otherwise(trimmed)


def _dict_values_eq(d1, d2):
    """Compare deux dicts (déjà parsés) clé par clé avec tolérance numérique."""
    if set(d1.keys()) != set(d2.keys()):
        return False
    for k in d1:
        v1, v2 = d1[k], d2[k]
        try:
            if abs(float(v1) - float(v2)) > NUMERIC_TOLERANCE:
                return False
        except (TypeError, ValueError):
            if str(v1) != str(v2):
                return False
    return True


@F.udf("boolean")
def _map_eq_udf(m1, m2):
    """
    Compare deux colonnes MapType Spark (reçues comme dicts Python).
    Utilisé quand computed_availabilities est un MapType.
    """
    if m1 is None and m2 is None:
        return True
    if m1 is None or m2 is None:
        return False
    return _dict_values_eq(dict(m1), dict(m2))


@F.udf("boolean")
def _json_dict_eq_udf(s1, s2):
    """
    Compare deux colonnes StringType contenant un JSON dict.
    Utilisé si computed_availabilities est stocké comme string JSON.
    """
    if s1 is None and s2 is None:
        return True
    if s1 is None or s2 is None:
        return False
    try:
        d1 = _json.loads(s1)
        d2 = _json.loads(s2)
        return _dict_values_eq(d1, d2)
    except Exception:
        return str(s1) == str(s2)


def load_dataiku(path: str):
    """
    Charge un dataset Dataiku depuis un Volume Unity Catalog.
    Tente d'abord le chemin direct, puis avec le suffixe /managed
    (Dataiku stocke les fichiers parquet dans un sous-dossier /managed).
    """
    try:
        df = spark.read.format("parquet").load(path)
        if len(df.head(1)) == 0:
            raise Exception("empty")
        return df
    except Exception:
        return spark.read.format("parquet").load(f"{path}/managed")


def load_databricks(cfg: dict):
    """
    Charge le dataset Databricks selon la configuration.
    - is_s3=True  : lecture delta depuis S3
    - is_s3=False : spark.table() depuis Unity Catalog
    - enrich_dbr  : join optionnel pour résoudre une FK surrogate
    """
    if cfg["is_s3"]:
        path = f"s3://{gold_bucket}/{cfg['databricks_ref']}"
        df = spark.read.format("delta").load(path)
    else:
        df = spark.table(cfg["databricks_ref"])

    enrich = cfg.get("enrich_dbr")
    if enrich and enrich["type"] == "join_dim_turbine":
        dim = spark.table(enrich["dim_table"]).select(
            F.col(enrich["dim_key"]).alias(enrich["join_on"]),
            *enrich["add_cols"],
        )
        df = df.join(dim, on=enrich["join_on"], how="left").drop(enrich["join_on"])

    return df


def _filter_rolling_months(df):
    """
    Filtre df pour ne garder que les ROLLING_MONTHS derniers mois (fenêtre glissante).
    Fonctionne que year/month soient stockés en string ou en int.
    Exemple : aujourd'hui = 2026-05, ROLLING_MONTHS=13 → conserve >= 2025-05.
    """
    from datetime import date
    today = date.today()
    # cutoff_total = index numérique du mois le plus ancien à conserver
    # (year*12 + month) > cutoff_total  <=>  (year,month) >= (cutoff_year, cutoff_month+1)
    cutoff_total = today.year * 12 + today.month - ROLLING_MONTHS
    return df.filter(
        F.col("year").cast("int") * 12 + F.col("month").cast("int") > F.lit(cutoff_total)
    )


def _filter_common_periods(df_dku, df_dbr, year_col: str, month_col: str, label: str):
    """
    Aligne les deux datasets sur l'intersection des périodes (year, month).
    Permet d'éviter de pénaliser le score lorsqu'un côté contient des périodes hors périmètre.
    """
    def _pick_period_cols(df, y_col: str, m_col: str):
        cols = set(df.columns)
        if y_col in cols and m_col in cols:
            return y_col, m_col
        for fy, fm in (("year", "month"), ("slb_year", "slb_month")):
            if fy in cols and fm in cols:
                return fy, fm
        raise ValueError(f"No period columns found for {label}. Available columns: {sorted(cols)}")

    dku_y_col, dku_m_col = _pick_period_cols(df_dku, year_col, month_col)
    dbr_y_col, dbr_m_col = _pick_period_cols(df_dbr, year_col, month_col)

    dku_periods = df_dku.select(
        F.col(dku_y_col).cast("int").alias("_y"),
        F.col(dku_m_col).cast("int").alias("_m"),
    ).distinct()
    dbr_periods = df_dbr.select(
        F.col(dbr_y_col).cast("int").alias("_y"),
        F.col(dbr_m_col).cast("int").alias("_m"),
    ).distinct()
    common_periods = dku_periods.intersect(dbr_periods)

    dku_before = df_dku.count()
    dbr_before = df_dbr.count()

    df_dku_filtered = (
        df_dku
        .withColumn("_y", F.col(dku_y_col).cast("int"))
        .withColumn("_m", F.col(dku_m_col).cast("int"))
        .join(common_periods, on=["_y", "_m"], how="inner")
        .drop("_y", "_m")
    )
    df_dbr_filtered = (
        df_dbr
        .withColumn("_y", F.col(dbr_y_col).cast("int"))
        .withColumn("_m", F.col(dbr_m_col).cast("int"))
        .join(common_periods, on=["_y", "_m"], how="inner")
        .drop("_y", "_m")
    )

    dku_after = df_dku_filtered.count()
    dbr_after = df_dbr_filtered.count()
    common_count = common_periods.count()
    print(
        f"  🔎 Périodes communes ({label}) [{dku_y_col}/{dku_m_col} vs {dbr_y_col}/{dbr_m_col}] : {common_count} | "
        f"Dataiku {dku_before:,}->{dku_after:,} | Databricks {dbr_before:,}->{dbr_after:,}"
    )
    return df_dku_filtered, df_dbr_filtered


def compare_schema(df_dku, df_dbr, name: str) -> dict:
    """
    Compare les schémas (noms de colonnes) entre Dataiku et Databricks.
    Affiche les colonnes manquantes de chaque côté.
    Retourne un dict avec les colonnes communes.
    """
    cols_dku = set(df_dku.columns)
    cols_dbr = set(df_dbr.columns)

    only_dku = sorted(cols_dku - cols_dbr)
    only_dbr = sorted(cols_dbr - cols_dku)
    common   = sorted(cols_dku & cols_dbr)

    sep = "═" * 70
    print(f"\n{sep}")
    print(f"  DATASET : {name}")
    print(sep)
    print(f"  Dataiku    : {len(cols_dku):3d} colonnes")
    print(f"  Databricks : {len(cols_dbr):3d} colonnes")
    print(f"  Communes   : {len(common):3d} colonnes")

    if only_dku:
        print(f"\n  ⚠️  Présentes dans Dataiku UNIQUEMENT ({len(only_dku)}) :")
        for c in only_dku:
            print(f"       - {c}")
    else:
        print("\n  ✅  Aucune colonne absente côté Databricks")

    if only_dbr:
        print(f"\n  ℹ️  Présentes dans Databricks UNIQUEMENT ({len(only_dbr)}) :")
        for c in only_dbr:
            print(f"       - {c}")
    else:
        print("  ✅  Aucune colonne supplémentaire côté Databricks")

    return {"common": common, "only_dku": only_dku, "only_dbr": only_dbr}


def compare_rows(df_dku, df_dbr, join_keys: list, common_cols: list, name: str):
    """
    Compare les valeurs ligne par ligne sur les colonnes communes.

    Stratégie :
    - Full outer join sur join_keys
    - Colonnes JSON dict / MapType (computed_availabilities) : comparaison clé par clé ±0.01
    - Colonnes Date/Timestamp : comparaison sur la partie date uniquement (j/m/a), heure ignorée
    - Colonnes numériques (Double/Float/Decimal) : tolérance abs(a-b) <= NUMERIC_TOLERANCE
    - Autres colonnes : cast string + égalité null-safe
    - run_date et colonnes EXCLUDE_COLS : ignorées
    - Lignes orphelines (null d'un côté) comptées comme entièrement différentes

    Retourne (col_stats dict, pct_global float, total_rows int, diagnostics dict)
    """
    # Tolérance par colonne (surcharge NUMERIC_TOLERANCE si défini dans DATASETS)
    _col_tol          = (DATASETS.get(name) or {}).get("col_tolerances") or {}
    # Valeur de substitution pour NULL (ex: NULL → 100.0 pour completeness_percentage)
    _col_null_defaults = (DATASETS.get(name) or {}).get("col_null_defaults") or {}

    # Colonnes à comparer = communes - clés de jointure - colonnes exclues
    value_cols = [c for c in common_cols if c not in join_keys and c not in EXCLUDE_COLS]

    if not value_cols:
        print("  Aucune colonne de valeur à comparer (seulement des clés).")
        diagnostics = {
            "matched_rows": 0,
            "only_in_dataiku": 0,
            "only_in_databricks": 0,
            "duplicate_keys_dataiku": 0,
            "duplicate_keys_databricks": 0,
        }
        return {}, 100.0, 0, diagnostics

    # ── Détermine les types côté Databricks pour choisir la stratégie de comparaison
    dbr_types = {f.name: f.dataType for f in df_dbr.schema.fields}
    dku_types = {f.name: f.dataType for f in df_dku.schema.fields}

    # ── Vérifie les clés dupliquées avant join (signal fort de mismatch structurel)
    dku_dup_keys = df_dku.groupBy(*[F.col(k) for k in join_keys]).count().filter(F.col("count") > 1).count()
    dbr_dup_keys = df_dbr.groupBy(*[F.col(k) for k in join_keys]).count().filter(F.col("count") > 1).count()

    # ── Aliase les deux dataframes pour le join
    dku = df_dku.alias("dku")
    dbr = df_dbr.alias("dbr")

    # Construction de la condition de jointure avec normalisation des clés
    # pour éviter les faux mismatches (ex: "05" vs 5, espaces, "" vs NULL).
    numeric_key_names = {"year", "month", "slb_year", "slb_month"}

    def _join_key_expr(alias: str, key: str):
        dtype = (dku_types if alias == "dku" else dbr_types).get(key)
        col_expr = F.col(f"{alias}.{key}")
        if key in numeric_key_names or _is_numeric_type(dtype):
            return col_expr.cast("int")
        # Clés métier textuelles : normalisation + uppercase (codes/id souvent sensibles à la casse)
        norm = _normalized_str(col_expr)
        if key.endswith("_code") or key.endswith("_id") or key == "contract_technical_id":
            return F.upper(norm)
        return norm

    join_cond = [_join_key_expr("dku", k).eqNullSafe(_join_key_expr("dbr", k)) for k in join_keys]

    joined = dku.join(dbr, on=join_cond, how="full_outer")
    total_rows = joined.count()

    # ── Lignes orphelines (clé absente d'un côté)
    first_key = join_keys[0]
    only_in_dku = joined.filter(F.col(f"dbr.{first_key}").isNull() & F.col(f"dku.{first_key}").isNotNull()).count()
    only_in_dbr = joined.filter(F.col(f"dku.{first_key}").isNull() & F.col(f"dbr.{first_key}").isNotNull()).count()
    matched_rows = total_rows - only_in_dku - only_in_dbr

    # Echantillons de clés orphelines pour debug rapide
    sample_only_in_dku_rows = (
        joined
        .filter(F.col(f"dbr.{first_key}").isNull() & F.col(f"dku.{first_key}").isNotNull())
        .select(*[F.col(f"dku.{k}").alias(k) for k in join_keys])
        .limit(5)
        .collect()
    )
    sample_only_in_dbr_rows = (
        joined
        .filter(F.col(f"dku.{first_key}").isNull() & F.col(f"dbr.{first_key}").isNotNull())
        .select(*[F.col(f"dbr.{k}").alias(k) for k in join_keys])
        .limit(5)
        .collect()
    )

    sample_only_in_dku = [{k: r[k] for k in join_keys} for r in sample_only_in_dku_rows]
    sample_only_in_dbr = [{k: r[k] for k in join_keys} for r in sample_only_in_dbr_rows]

    # Couverture temporelle (si la clé contient year/month ou slb_year/slb_month)
    period_diagnostics = {}
    if {"year", "month"}.issubset(set(join_keys)):
        dku_periods_df = df_dku.select(
            F.col("year").cast("int").alias("year"),
            F.col("month").cast("int").alias("month"),
        ).distinct()
        dbr_periods_df = df_dbr.select(
            F.col("year").cast("int").alias("year"),
            F.col("month").cast("int").alias("month"),
        ).distinct()
        dku_periods = dku_periods_df.select(F.concat_ws("-", F.col("year"), F.lpad(F.col("month").cast("string"), 2, "0")).alias("p"))
        dbr_periods = dbr_periods_df.select(F.concat_ws("-", F.col("year"), F.lpad(F.col("month").cast("string"), 2, "0")).alias("p"))
        only_periods_dku = [r["p"] for r in dku_periods.subtract(dbr_periods).orderBy("p").limit(12).collect()]
        only_periods_dbr = [r["p"] for r in dbr_periods.subtract(dku_periods).orderBy("p").limit(12).collect()]
        period_diagnostics = {
            "period_key": "year-month",
            "distinct_periods_dataiku": dku_periods_df.count(),
            "distinct_periods_databricks": dbr_periods_df.count(),
            "sample_periods_only_in_dataiku": only_periods_dku,
            "sample_periods_only_in_databricks": only_periods_dbr,
        }
    elif {"slb_year", "slb_month"}.issubset(set(join_keys)):
        dku_periods_df = df_dku.select(
            F.col("slb_year").cast("int").alias("year"),
            F.col("slb_month").cast("int").alias("month"),
        ).distinct()
        dbr_periods_df = df_dbr.select(
            F.col("slb_year").cast("int").alias("year"),
            F.col("slb_month").cast("int").alias("month"),
        ).distinct()
        dku_periods = dku_periods_df.select(F.concat_ws("-", F.col("year"), F.lpad(F.col("month").cast("string"), 2, "0")).alias("p"))
        dbr_periods = dbr_periods_df.select(F.concat_ws("-", F.col("year"), F.lpad(F.col("month").cast("string"), 2, "0")).alias("p"))
        only_periods_dku = [r["p"] for r in dku_periods.subtract(dbr_periods).orderBy("p").limit(12).collect()]
        only_periods_dbr = [r["p"] for r in dbr_periods.subtract(dku_periods).orderBy("p").limit(12).collect()]
        period_diagnostics = {
            "period_key": "slb_year-slb_month",
            "distinct_periods_dataiku": dku_periods_df.count(),
            "distinct_periods_databricks": dbr_periods_df.count(),
            "sample_periods_only_in_dataiku": only_periods_dku,
            "sample_periods_only_in_databricks": only_periods_dbr,
        }

    print(f"\n  Lignes totales (full outer) : {total_rows:,}")
    print(f"  Lignes matchées (join)      : {matched_rows:,}")
    if only_in_dku > 0:
        print(f"  ⚠️  Lignes uniquement dans Dataiku    : {only_in_dku:,}")
    if only_in_dbr > 0:
        print(f"  ⚠️  Lignes uniquement dans Databricks : {only_in_dbr:,}")
    if dku_dup_keys > 0:
        print(f"  ⚠️  Clés dupliquées côté Dataiku      : {dku_dup_keys:,}")
    if dbr_dup_keys > 0:
        print(f"  ⚠️  Clés dupliquées côté Databricks   : {dbr_dup_keys:,}")

    # ── Calcul des colonnes d'égalité
    eq_exprs = {}
    for col in value_cols:
        dku_dtype = dku_types.get(col)
        dbr_dtype = dbr_types.get(col)
        dtype = dbr_dtype or dku_dtype

        if col in JSON_DICT_COLS:
            # MapType : passer le dict Python directement au UDF (pas de cast string)
            # StringType : passer la string JSON au UDF pour parsing
            if isinstance(dtype, MapType):
                eq_col = _map_eq_udf(
                    F.col(f"dku.{col}"),
                    F.col(f"dbr.{col}"),
                ).alias(f"_eq_{col}")
            else:
                eq_col = _json_dict_eq_udf(
                    F.col(f"dku.{col}").cast("string"),
                    F.col(f"dbr.{col}").cast("string"),
                ).alias(f"_eq_{col}")
        elif col in NORMALIZE_PATH_COLS:
            # Strip leading "/" pour normaliser les paths Dataiku avant comparaison
            # + trim + "" -> NULL via _normalized_str pour gérer les chaînes vides
            dku_norm = _normalized_str(F.regexp_replace(F.col(f"dku.{col}").cast("string"), r"^/", ""))
            dbr_norm = _normalized_str(F.regexp_replace(F.col(f"dbr.{col}").cast("string"), r"^/", ""))
            eq_col = dku_norm.eqNullSafe(dbr_norm).alias(f"_eq_{col}")
        elif _is_date_type(dku_dtype) or _is_date_type(dbr_dtype):
            # Date/Timestamp : comparer uniquement la partie date (j/m/a), heure ignorée
            eq_col = F.col(f"dku.{col}").cast("date").eqNullSafe(
                F.col(f"dbr.{col}").cast("date")
            ).alias(f"_eq_{col}")
        elif _is_numeric_type(dtype):
            # Tolérance par colonne : peut être un scalaire (abs seulement)
            # ou un dict {abs, rel} → abs(a-b) <= max(abs, rel * max(|a|,|b|))
            tol_spec = _col_tol.get(col, NUMERIC_TOLERANCE)
            if isinstance(tol_spec, dict):
                abs_tol = float(tol_spec.get("abs", NUMERIC_TOLERANCE))
                rel_tol = float(tol_spec.get("rel", 0.0))
            else:
                abs_tol = float(tol_spec)
                rel_tol = 0.0

            def _close(a_expr, b_expr, abs_tol=abs_tol, rel_tol=rel_tol):
                diff = F.abs(a_expr - b_expr)
                if rel_tol > 0.0:
                    rel_bound = F.lit(rel_tol) * F.greatest(F.abs(a_expr), F.abs(b_expr))
                    return diff <= F.greatest(F.lit(abs_tol), rel_bound)
                return diff <= F.lit(abs_tol)

            null_default = _col_null_defaults.get(col)
            if null_default is not None:
                # NULL sémantiquement équivalent à null_default (ex: NULL = 100.0 pour completeness)
                dku_expr = F.coalesce(F.col(f"dku.{col}").cast("double"), F.lit(float(null_default)))
                dbr_expr = F.coalesce(F.col(f"dbr.{col}").cast("double"), F.lit(float(null_default)))
                eq_col = _close(dku_expr, dbr_expr).alias(f"_eq_{col}")
            else:
                # Tolérance numérique — null == null → True, null != valeur → False
                eq_col = F.when(
                    F.col(f"dku.{col}").isNull() & F.col(f"dbr.{col}").isNull(), F.lit(True)
                ).when(
                    F.col(f"dku.{col}").isNull() | F.col(f"dbr.{col}").isNull(), F.lit(False)
                ).otherwise(
                    _close(F.col(f"dku.{col}").cast("double"), F.col(f"dbr.{col}").cast("double"))
                ).alias(f"_eq_{col}")
        else:
            # String : trim + "" -> NULL des deux côtés, puis eqNullSafe
            # Capture les cas "" (Dataiku) vs NULL (Databricks) et les espaces de bordure
            eq_col = _normalized_str(F.col(f"dku.{col}")).eqNullSafe(
                _normalized_str(F.col(f"dbr.{col}"))
            ).alias(f"_eq_{col}")
        eq_exprs[col] = eq_col

    # Ajoute toutes les colonnes d'égalité en une passe
    joined_eq = joined.select(
        # COALESCE pour afficher la vraie clé même pour les lignes orphelines (dku NULL ou dbr NULL)
        *[F.coalesce(F.col(f"dku.{k}"), F.col(f"dbr.{k}")).alias(k) for k in join_keys],
        *eq_exprs.values(),
        # Valeurs brutes pour l'affichage des diffs
        *[F.col(f"dku.{c}").alias(f"dku_{c}") for c in value_cols],
        *[F.col(f"dbr.{c}").alias(f"dbr_{c}") for c in value_cols],
    )

    # ── Stats d'égalité par colonne (sur joined complet, orphans inclus = penalisé)
    agg_exprs = [F.avg(F.col(f"_eq_{c}").cast("double")).alias(c) for c in value_cols]
    stats_row = joined_eq.agg(*agg_exprs).first()

    # ── Stats sur matched rows uniquement (orphans exclus) : visibilité column-level pure
    first_key = join_keys[0]
    matched_filter = F.col(f"dku.{first_key}").isNotNull() & F.col(f"dbr.{first_key}").isNotNull()
    joined_matched = joined.filter(matched_filter)
    if matched_rows > 0:
        # Reconstruire les eq_exprs sur la version matched (les exprs sont sur "joined", pas sur "joined_eq")
        matched_eq = joined.filter(matched_filter).select(*eq_exprs.values())
        matched_stats_row = matched_eq.agg(*[F.avg(F.col(f"_eq_{c}").cast("double")).alias(c) for c in value_cols]).first()
    else:
        matched_stats_row = None

    col_stats = {}
    print("\n  ── Stats d'égalité par colonne (% de lignes identiques) ──")
    for col in value_cols:
        pct = (stats_row[col] or 0.0) * 100.0
        col_stats[col] = pct
        if col in JSON_DICT_COLS:
            col_dtype = dbr_types.get(col) or dku_types.get(col)
            strategy = "map" if isinstance(col_dtype, MapType) else "json"
        elif col in NORMALIZE_PATH_COLS:
            strategy = "path"
        elif _is_date_type(dku_types.get(col)) or _is_date_type(dbr_types.get(col)):
            strategy = "date"
        elif _is_numeric_type(dbr_types.get(col) or dku_types.get(col)):
            strategy = "num"
        else:
            strategy = "str"
        print(f"    {col:<45} {pct:6.1f}%  [{strategy}]")

    # ── Score global (moyenne non pondérée sur les colonnes)
    pct_global = sum(col_stats.values()) / len(col_stats) if col_stats else 100.0
    print(f"\n  Score de proximité global (colonnes) : {pct_global:.1f}%")

    # ── Score sur matched rows uniquement (visibilité sur qualité column-level hors orphans)
    matched_col_stats = {}
    if matched_stats_row is not None:
        for col in value_cols:
            matched_col_stats[col] = (matched_stats_row[col] or 0.0) * 100.0
        pct_matched_only = sum(matched_col_stats.values()) / len(matched_col_stats) if matched_col_stats else 100.0
        print(f"  Score column-level sur matched rows  : {pct_matched_only:.1f}%  (matched={matched_rows}, orphans={only_in_dku + only_in_dbr})")
    else:
        pct_matched_only = None

    # ── Affichage des lignes différentes
    diff_filter = None
    for c in value_cols:
        cond = ~F.col(f"_eq_{c}")
        diff_filter = cond if diff_filter is None else (diff_filter | cond)

    diff_rows = joined_eq.filter(diff_filter).limit(MAX_DIFF_ROWS).collect()

    if diff_rows:
        print(f"\n  ── Lignes différentes (max {MAX_DIFF_ROWS}) ──")
        for row in diff_rows:
            key_str = ", ".join([f"{k}={row[k]}" for k in join_keys])
            differing = [c for c in value_cols if not row[f"_eq_{c}"]]
            print(f"  [{key_str}]")
            for c in differing:
                dku_val = row[f"dku_{c}"]
                dbr_val = row[f"dbr_{c}"]
                print(f"      {c}: Dataiku={dku_val!r}  →  Databricks={dbr_val!r}")
    else:
        print("\n  ✅  Aucune ligne différente détectée (sur colonnes communes)")

    diagnostics = {
        "matched_rows": int(matched_rows),
        "only_in_dataiku": int(only_in_dku),
        "only_in_databricks": int(only_in_dbr),
        "duplicate_keys_dataiku": int(dku_dup_keys),
        "duplicate_keys_databricks": int(dbr_dup_keys),
        "sample_keys_only_in_dataiku": sample_only_in_dku,
        "sample_keys_only_in_databricks": sample_only_in_dbr,
        "periods": period_diagnostics,
        "pct_matched_only": round(pct_matched_only, 4) if pct_matched_only is not None else None,
    }
    return col_stats, pct_global, total_rows, diagnostics


# Dictionnaire global pour le résumé final
global_results = {}

print("✅ Fonctions utilitaires chargées")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. `wind_turbine_ltsa_scope`

# COMMAND ----------

_name = "wind_turbine_ltsa_scope"
cfg = DATASETS[_name]

df_dku = load_dataiku(cfg["dataiku_path"])
df_dbr = load_databricks(cfg)

# Dataiku peut stocker le code turbine sous le nom ent_code ou wind_turbine_code
if "ent_code" in df_dku.columns and "wind_turbine_code" not in df_dku.columns:
    df_dku = df_dku.withColumnRenamed("ent_code", "wind_turbine_code")

schema_info = compare_schema(df_dku, df_dbr, _name)
col_stats, pct_global, total, diagnostics = compare_rows(
    df_dku, df_dbr, cfg["join_keys"], schema_info["common"], _name
)
global_results[_name] = {"pct": pct_global, "total": total, "col_stats": col_stats, "diagnostics": diagnostics}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. `turbine_availability`

# COMMAND ----------

_name = "turbine_availability"
cfg = DATASETS[_name]

df_dku = load_dataiku(cfg["dataiku_path"])
df_dbr = load_databricks(cfg)

if "ent_code" in df_dku.columns and "wind_turbine_code" not in df_dku.columns:
    df_dku = df_dku.withColumnRenamed("ent_code", "wind_turbine_code")

df_dku = _filter_rolling_months(df_dku)
df_dbr = _filter_rolling_months(df_dbr)
df_dku, df_dbr = _filter_common_periods(df_dku, df_dbr, "year", "month", _name)

schema_info = compare_schema(df_dku, df_dbr, _name)
col_stats, pct_global, total, diagnostics = compare_rows(
    df_dku, df_dbr, cfg["join_keys"], schema_info["common"], _name
)
global_results[_name] = {"pct": pct_global, "total": total, "col_stats": col_stats, "diagnostics": diagnostics}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. `substation_availability`

# COMMAND ----------

_name = "substation_availability"
cfg = DATASETS[_name]

df_dku = load_dataiku(cfg["dataiku_path"])
df_dbr = load_databricks(cfg)

df_dku = _filter_rolling_months(df_dku)
df_dbr = _filter_rolling_months(df_dbr)
df_dku, df_dbr = _filter_common_periods(df_dku, df_dbr, "year", "month", _name)

schema_info = compare_schema(df_dku, df_dbr, _name)
col_stats, pct_global, total, diagnostics = compare_rows(
    df_dku, df_dbr, cfg["join_keys"], schema_info["common"], _name
)
global_results[_name] = {"pct": pct_global, "total": total, "col_stats": col_stats, "diagnostics": diagnostics}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. `farm_availability`

# COMMAND ----------

_name = "farm_availability"
cfg = DATASETS[_name]

df_dku = load_dataiku(cfg["dataiku_path"])
df_dbr = load_databricks(cfg)

df_dku = _filter_rolling_months(df_dku)
df_dbr = _filter_rolling_months(df_dbr)
df_dku, df_dbr = _filter_common_periods(df_dku, df_dbr, "year", "month", _name)

schema_info = compare_schema(df_dku, df_dbr, _name)
col_stats, pct_global, total, diagnostics = compare_rows(
    df_dku, df_dbr, cfg["join_keys"], schema_info["common"], _name
)
global_results[_name] = {"pct": pct_global, "total": total, "col_stats": col_stats, "diagnostics": diagnostics}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. `turbine_production`

# COMMAND ----------

_name = "turbine_production"
cfg = DATASETS[_name]

df_dku = load_dataiku(cfg["dataiku_path"])
df_dbr = load_databricks(cfg)

if "ent_code" in df_dku.columns and "wind_turbine_code" not in df_dku.columns:
    df_dku = df_dku.withColumnRenamed("ent_code", "wind_turbine_code")

schema_info = compare_schema(df_dku, df_dbr, _name)
col_stats, pct_global, total, diagnostics = compare_rows(
    df_dku, df_dbr, cfg["join_keys"], schema_info["common"], _name
)
global_results[_name] = {"pct": pct_global, "total": total, "col_stats": col_stats, "diagnostics": diagnostics}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. `substation_production`

# COMMAND ----------

_name = "substation_production"
cfg = DATASETS[_name]

df_dku = load_dataiku(cfg["dataiku_path"])
df_dbr = load_databricks(cfg)

schema_info = compare_schema(df_dku, df_dbr, _name)
col_stats, pct_global, total, diagnostics = compare_rows(
    df_dku, df_dbr, cfg["join_keys"], schema_info["common"], _name
)
global_results[_name] = {"pct": pct_global, "total": total, "col_stats": col_stats, "diagnostics": diagnostics}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. `farm_production`

# COMMAND ----------

_name = "farm_production"
cfg = DATASETS[_name]

df_dku = load_dataiku(cfg["dataiku_path"])
df_dbr = load_databricks(cfg)

schema_info = compare_schema(df_dku, df_dbr, _name)
col_stats, pct_global, total, diagnostics = compare_rows(
    df_dku, df_dbr, cfg["join_keys"], schema_info["common"], _name
)
global_results[_name] = {"pct": pct_global, "total": total, "col_stats": col_stats, "diagnostics": diagnostics}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. `slb_completeness_turbine`

# COMMAND ----------

_name = "slb_completeness_turbine"
cfg = DATASETS[_name]

df_dku = load_dataiku(cfg["dataiku_path"])
df_dbr = load_databricks(cfg)  # enrichi via join dim_turbine (wind_turbine_code récupéré)

# Dataiku peut stocker le code turbine sous ent_code
if "ent_code" in df_dku.columns and "wind_turbine_code" not in df_dku.columns:
    df_dku = df_dku.withColumnRenamed("ent_code", "wind_turbine_code")

df_dku, df_dbr = _filter_common_periods(df_dku, df_dbr, "slb_year", "slb_month", _name)

schema_info = compare_schema(df_dku, df_dbr, _name)
col_stats, pct_global, total, diagnostics = compare_rows(
    df_dku, df_dbr, cfg["join_keys"], schema_info["common"], _name
)
global_results[_name] = {"pct": pct_global, "total": total, "col_stats": col_stats, "diagnostics": diagnostics}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. `referential_guarantee_farm`

# COMMAND ----------

_name = "referential_guarantee_farm"
cfg = DATASETS[_name]

df_dku = load_dataiku(cfg["dataiku_path"])
df_dbr = load_databricks(cfg)  # lecture delta depuis S3

schema_info = compare_schema(df_dku, df_dbr, _name)
col_stats, pct_global, total, diagnostics = compare_rows(
    df_dku, df_dbr, cfg["join_keys"], schema_info["common"], _name
)
global_results[_name] = {"pct": pct_global, "total": total, "col_stats": col_stats, "diagnostics": diagnostics}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Résumé global de proximité

# COMMAND ----------

# ─── RÉSUMÉ GLOBAL ──────────────────────────────────────────────────────────

sep     = "═" * 80
sep_mid = "─" * 80

print(f"\n{sep}")
print("  RÉSUMÉ GLOBAL — COMPARAISON DATAIKU → DATABRICKS")
print(sep)
print(f"  {'DATASET':<40} {'LIGNES':>10}  {'PROXIMITÉ':>10}")
print(sep_mid)

total_weighted_pct = 0.0
total_rows_all     = 0

for ds_name, res in global_results.items():
    pct  = res["pct"]
    rows = res["total"]
    bar_len = int(pct / 5)  # barre sur 20 caractères max
    bar  = "█" * bar_len + "░" * (20 - bar_len)

    # Indicateur couleur textuel
    if pct >= 95:
        indicator = "✅"
    elif pct >= 80:
        indicator = "⚠️ "
    else:
        indicator = "❌"

    print(f"  {indicator} {ds_name:<38} {rows:>10,}   {pct:6.1f}%  {bar}")

    # Pondération par nombre de lignes
    total_weighted_pct += pct * rows
    total_rows_all     += rows

print(sep_mid)

# Score pondéré global
if total_rows_all > 0:
    weighted_score = total_weighted_pct / total_rows_all
else:
    weighted_score = 0.0

# Score non pondéré (moyenne des datasets)
unweighted_score = sum(r["pct"] for r in global_results.values()) / len(global_results) if global_results else 0.0

print(f"  {'TOTAL':.<40} {total_rows_all:>10,}")
print(f"\n  Score moyen non pondéré (par dataset)  : {unweighted_score:6.1f}%")
print(f"  Score moyen pondéré    (par lignes)    : {weighted_score:6.1f}%")
print(sep)

# ── Détail par colonne pour les datasets avec score < 95%
low_score_datasets = {k: v for k, v in global_results.items() if v["pct"] < 95.0}

if low_score_datasets:
    print("\n  ── Colonnes les plus problématiques (datasets < 95%) ──")
    for ds_name, res in low_score_datasets.items():
        print(f"\n  [{ds_name}]")
        bad_cols = sorted(
            [(col, pct) for col, pct in res["col_stats"].items() if pct < 95.0],
            key=lambda x: x[1],
        )
        if bad_cols:
            for col, pct in bad_cols:
                print(f"    {col:<45} {pct:6.1f}%")
        else:
            print("    (problème de lignes manquantes, pas de colonnes dégradées)")
else:
    print("\n  ✅  Tous les datasets ont un score de proximité >= 95%")

# Sortie structurée pour les runs Databricks Jobs (capturée dans notebook_output.result)
result_payload = {
    "status": "ok",
    "catalog": catalog,
    "gold_bucket": gold_bucket,
    "dataset_count": len(global_results),
    "scores": {
        "unweighted": round(unweighted_score, 4),
        "weighted": round(weighted_score, 4),
        "total_rows": int(total_rows_all),
    },
    "datasets": {
        ds_name: {
            "pct": round(res["pct"], 4),
            "rows": int(res["total"]),
            "diagnostics": res.get("diagnostics") or {},
            "bad_columns_lt_99_5": [
                {"name": col, "pct": round(pct, 4)}
                for col, pct in sorted(res["col_stats"].items(), key=lambda x: x[1])
                if pct < 99.5
            ],
        }
        for ds_name, res in global_results.items()
    },
}

dbutils.notebook.exit(_json.dumps(result_payload, ensure_ascii=False))

