# Databricks notebook source
# MAGIC %md
# MAGIC # Diagnostic — pm_budget_tracker enrichi avec colonnes IEC / production / disponibilités
# MAGIC
# MAGIC Vue complète par turbine × mois pour comparaison avec la nomenclature Excel attendue.
# MAGIC
# MAGIC Sources :
# MAGIC - `gold.pm_budget_tracker`            — budget IANOSM (cap / cumsum / contrat)
# MAGIC - `gold.slb_and_production`           — toutes les catégories IEC (durée min + MWh)
# MAGIC - `gold.slb_completeness_turbine`     — ActProd_DataAv (% de données valides)
# MAGIC - `gold.turbine_availability`         — TB_M_C_Av / EB_M_C_Av (time/energy v1/v2/v3)
# MAGIC
# MAGIC Sortie : table `gold.diag_pm_budget_tracker_enriched` + affichage

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "ltsa_dev")
CATALOG = dbutils.widgets.get("catalog")

DUR = "stop_duration_min"   # suffix pour les durées dans slb_and_production
MWH = "loss_value_mwh"      # suffix pour les MWh    dans slb_and_production

# COMMAND ----------

# =============================================================================
# CHARGEMENT
# =============================================================================

pm = spark.table(f"{CATALOG}.gold.pm_budget_tracker")
slb = spark.table(f"{CATALOG}.gold.slb_and_production")
compl = spark.table(f"{CATALOG}.gold.slb_completeness_turbine")
ta = spark.table(f"{CATALOG}.gold.turbine_availability")
fa = spark.table(f"{CATALOG}.gold.fact_farm_availability")
dim_farm = spark.table(f"{CATALOG}.gold.dim_farm")

# COMMAND ----------

# COMMAND ----------

# =============================================================================
# AGGREGATION SLB AU NIVEAU TURBINE × MOIS
# (slb_and_production est déjà aggrégé à ce niveau, pas besoin de re-groupBy)
# =============================================================================

# Colonnes IEC nécessaires (noms complets dans slb_and_production)
IEC_CATS = ["iaogpp", "iaongts", "iaongen", "iaongrs", "iaongel",
            "ianosm", "ianopca", "ianofo", "ianos", "iafm", "iu"]

slb_cols = ["wind_turbine_code", "month", "year", "production_kwh"]
for cat in IEC_CATS:
    for resp in ["maintainer", "other"]:
        slb_cols += [f"{cat}_{resp}_{DUR}", f"{cat}_{resp}_{MWH}"]

# Sélection défensive : ne prendre que les colonnes qui existent
# Renommage year/month pour éviter ambiguïté dans la jointure
slb_existing = [c for c in slb_cols if c in slb.columns]
slb_sel = (
    slb.select(slb_existing)
    .withColumnRenamed("wind_turbine_code", "_slb_wt_code")
    .withColumnRenamed("year", "_slb_year")
    .withColumnRenamed("month", "_slb_month")
)

# COMMAND ----------

# =============================================================================
# DISPONIBILITÉS PAR TURBINE
# On joint via wind_turbine_code / year / month
# =============================================================================

# gold.turbine_availability (delivery table from wind_availability_merge) already
# carries wind_turbine_code directly — no need to join with dim_turbine.
ta_enriched = (
    ta
    .filter(F.col("wind_turbine_code").isNotNull())
    .select(
        F.col("wind_turbine_code").alias("_ta_wt_code"),
        F.col("year").cast("int").alias("_ta_year"),
        F.col("month").cast("int").alias("_ta_month"),
        F.col("availability_portfolio_time").alias("_TB_M_C_Av_v1"),
        F.col("availability_portfolio_energy").alias("_EB_M_C_Av_v1"),
        F.col("computed_availabilities")["time_v2"].cast("double").alias("_TB_M_C_Av_v2"),
        F.col("computed_availabilities")["energy_v2"].cast("double").alias("_EB_M_C_Av_v2"),
        F.col("availability_maintainer").alias("_TB_M_C_Av_v3"),
    )
)

# COMMAND ----------

# =============================================================================
# DISPONIBILITÉS AU NIVEAU FARM (fallback pour les lignes WTcode IS NULL)
# gold.fact_farm_availability est indexée par farm_id (FK vers dim_farm) et
# stocke computed_availabilities en JSON string (cf. build_computed_availabilities_json).
# =============================================================================

farm_avail_sel = (
    fa
    .join(
        dim_farm.select(F.col("id").alias("_dimf_id"), F.col("code").alias("_fa_farm_code")),
        F.col("farm_id") == F.col("_dimf_id"),
        how="left"
    )
    .select(
        F.col("_fa_farm_code"),
        F.col("year").cast("int").alias("_fa_year"),
        F.col("month").cast("int").alias("_fa_month"),
        F.col("time_availability").alias("_farm_TB_M_C_Av_v1"),
        F.col("energy_availability").alias("_farm_EB_M_C_Av_v1"),
        F.get_json_object(F.col("computed_availabilities"), "$.time_v2").cast("double").alias("_farm_TB_M_C_Av_v2"),
        F.get_json_object(F.col("computed_availabilities"), "$.energy_v2").cast("double").alias("_farm_EB_M_C_Av_v2"),
    )
)

# COMMAND ----------

# =============================================================================
# COMPLETENESS (ActProd_DataAv)
# =============================================================================

compl_sel = (
    compl
    .select(
        F.col("wind_turbine_code").alias("_cp_wt_code"),
        F.col("year").cast("int").alias("_cp_year"),
        F.col("month").cast("int").alias("_cp_month"),
        F.col("completeness_percentage").alias("_ActProd_DataAv"),
    )
)

# COMMAND ----------

# =============================================================================
# CONSTRUCTION DES COLONNES IEC (durée en heures, énergie en kWh)
# =============================================================================

def _dur(cat, resp="both"):
    """Durée totale (maintainer + other) ou juste maintainer, en heures."""
    if resp == "maintainer":
        col = f"{cat}_maintainer_{DUR}"
        return F.coalesce(F.col(col), F.lit(0.0)) / 60
    m = F.coalesce(F.col(f"{cat}_maintainer_{DUR}"), F.lit(0.0))
    o = F.coalesce(F.col(f"{cat}_other_{DUR}"), F.lit(0.0))
    return (m + o) / 60

def _kwh(cat, resp="both"):
    """Énergie totale (maintainer + other) ou juste maintainer, en kWh."""
    if resp == "maintainer":
        col = f"{cat}_maintainer_{MWH}"
        return F.coalesce(F.col(col), F.lit(0.0)) * 1000
    m = F.coalesce(F.col(f"{cat}_maintainer_{MWH}"), F.lit(0.0))
    o = F.coalesce(F.col(f"{cat}_other_{MWH}"), F.lit(0.0))
    return (m + o) * 1000

# COMMAND ----------

# =============================================================================
# JOINTURES
# =============================================================================

pm_y = pm.withColumn("_pm_year", F.col("year").cast("int")).withColumn("_pm_month", F.col("month").cast("int"))

base = (
    pm_y
    # SLB par turbine
    .join(
        slb_sel,
        (F.col("turbine") == F.col("_slb_wt_code"))
        & (F.col("_pm_year") == F.col("_slb_year"))
        & (F.col("_pm_month") == F.col("_slb_month")),
        how="left"
    )
    # Disponibilités
    .join(
        ta_enriched,
        (F.col("turbine") == F.col("_ta_wt_code"))
        & (F.col("_pm_year") == F.col("_ta_year"))
        & (F.col("_pm_month") == F.col("_ta_month")),
        how="left"
    )
    # Complétude
    .join(
        compl_sel,
        (F.col("turbine") == F.col("_cp_wt_code"))
        & (F.col("_pm_year") == F.col("_cp_year"))
        & (F.col("_pm_month") == F.col("_cp_month")),
        how="left"
    )
    # Disponibilités au niveau farm (fallback pour les lignes WTcode IS NULL)
    .join(
        farm_avail_sel,
        (F.col("farm") == F.col("_fa_farm_code"))
        & (F.col("_pm_year") == F.col("_fa_year"))
        & (F.col("_pm_month") == F.col("_fa_month")),
        how="left"
    )
    .drop("_slb_wt_code", "_slb_year", "_slb_month",
          "_ta_wt_code", "_ta_year", "_ta_month",
          "_cp_wt_code", "_cp_year", "_cp_month",
          "_fa_farm_code", "_fa_year", "_fa_month",
          "_pm_year", "_pm_month")
)

# COMMAND ----------

# =============================================================================
# SÉLECTION FINALE
# =============================================================================

result = base.select(
    # ── Identifiants temporels ─────────────────────────────────────────────
    F.make_date(F.col("year").cast("int"), F.col("month").cast("int"), F.lit(1))
        .alias("PeriodStart_incl"),
    F.add_months(
        F.make_date(F.col("year").cast("int"), F.col("month").cast("int"), F.lit(1)), 1
    ).alias("PeriodEnd_excl"),

    # ── Codes équipements ─────────────────────────────────────────────────
    F.col("farm").alias("WFcode"),
    F.col("substation").alias("WScode"),
    F.col("turbine").alias("WTcode"),

    # ── Colonnes pm_budget_tracker existantes ─────────────────────────────
    F.col("year"),
    F.col("month"),
    F.col("contract_id"),
    F.col("contract_level"),
    F.col("guarantee_start_date"),
    F.col("guarantee_end_date"),
    F.col("ianosm_budget_guarantee_period"),
    F.col("ianosm_budget_unit"),
    F.col("ianosm_consumed_hours_this_month"),
    F.col("ianosm_consumed_mwh_this_month"),
    F.col("ianosm_above_cap_hours_this_month"),
    F.col("ianosm_above_cap_mwh_this_month"),
    F.col("ianosm_consumed_hours_guarantee_period_so_far"),
    F.col("ianosm_consumed_mwh_guarantee_period_so_far"),
    F.col("ianosm_above_cap_hours_guarantee_period_so_far"),
    F.col("ianosm_above_cap_mwh_guarantee_period_so_far"),
    F.col("ianosm_below_cap_hours_guarantee_period_so_far"),
    F.col("ianosm_below_cap_mwh_guarantee_period_so_far"),
    F.col("is_above_cap"),

    # ── Production ────────────────────────────────────────────────────────
    F.col("_ActProd_DataAv").alias("ActProd_DataAv"),
    F.coalesce(F.col("production_kwh"), F.lit(0.0)).alias("ActProd_kWh"),

    # ── IEC time (heures) ─────────────────────────────────────────────────
    _dur("iaogpp").alias("IAOGPP_time"),
    _dur("iaongts").alias("IAONGTS_time"),
    _dur("iaongen").alias("IAONGEN_time"),
    _dur("iaongrs").alias("IAONGRS_time"),
    _dur("iaongel").alias("IAONGEL_time"),
    _dur("ianosm").alias("IANOSM_time"),
    _dur("ianopca").alias("IANOPCA_time"),
    _dur("ianofo").alias("IANOFO_time"),
    _dur("ianos").alias("IANOS_time"),
    _dur("iafm").alias("IAFM_time"),
    _dur("iu").alias("IU_time"),

    # ── IEC Maintainer time ───────────────────────────────────────────────
    _dur("iaogpp", "maintainer").alias("IAOGPP_Maintainer_time"),
    _dur("iaongts", "maintainer").alias("IAONGTS_Maintainer_time"),
    _dur("iaongen", "maintainer").alias("IAONGEN_Maintainer_time"),
    _dur("iaongrs", "maintainer").alias("IAONGRS_Maintainer_time"),
    _dur("iaongel", "maintainer").alias("IAONGEL_Maintainer_time"),
    _dur("ianosm", "maintainer").alias("IANOSM_Maintainer_time"),
    _dur("ianopca", "maintainer").alias("IANOPCA_Maintainer_time"),
    _dur("ianofo", "maintainer").alias("IANOFO_Maintainer_time"),
    _dur("ianos", "maintainer").alias("IANOS_Maintainer_time"),
    _dur("iafm", "maintainer").alias("IAFM_Maintainer_time"),

    # ── IANOSM cap split (heures) ─────────────────────────────────────────
    F.col("ianosm_above_cap_hours_this_month").alias("IANOSM_Maintainer_AboveCap_time"),
    (
        F.col("ianosm_consumed_hours_this_month")
        - F.col("ianosm_above_cap_hours_this_month")
    ).alias("IANOSM_Maintainer_BelowCap_time"),

    # ── IEC kWh ───────────────────────────────────────────────────────────
    _kwh("iaogpp").alias("IAOGPP_kWh"),
    _kwh("iaongts").alias("IAONGTS_kWh"),
    _kwh("iaongen").alias("IAONGEN_kWh"),
    _kwh("iaongrs").alias("IAONGRS_kWh"),
    _kwh("iaongel").alias("IAONGEL_kWh"),
    _kwh("ianosm").alias("IANOSM_kWh"),
    _kwh("ianopca").alias("IANOPCA_kWh"),
    _kwh("ianofo").alias("IANOFO_kWh"),
    _kwh("ianos").alias("IANOS_kWh"),
    _kwh("iafm").alias("IAFM_kWh"),
    _kwh("iu").alias("IU_kWh"),

    # ── IEC Maintainer kWh ────────────────────────────────────────────────
    _kwh("iaogpp", "maintainer").alias("IAOGPP_Maintainer_kWh"),
    _kwh("iaongts", "maintainer").alias("IAONGTS_Maintainer_kWh"),
    _kwh("iaongen", "maintainer").alias("IAONGEN_Maintainer_kWh"),
    _kwh("iaongrs", "maintainer").alias("IAONGRS_Maintainer_kWh"),
    _kwh("iaongel", "maintainer").alias("IAONGEL_Maintainer_kWh"),
    _kwh("ianosm", "maintainer").alias("IANOSM_Maintainer_kWh"),
    _kwh("ianopca", "maintainer").alias("IANOPCA_Maintainer_kWh"),
    _kwh("ianofo", "maintainer").alias("IANOFO_Maintainer_kWh"),
    _kwh("ianos", "maintainer").alias("IANOS_Maintainer_kWh"),
    _kwh("iafm", "maintainer").alias("IAFM_Maintainer_kWh"),

    # ── IANOSM cap split (kWh) ────────────────────────────────────────────
    F.col("ianosm_above_cap_mwh_this_month").alias("IANOSM_Maintainer_AboveCap_kWh"),
    (
        F.col("ianosm_consumed_mwh_this_month")
        - F.col("ianosm_above_cap_mwh_this_month")
    ).alias("IANOSM_Maintainer_BelowCap_kWh"),

    # ── Disponibilités (%) ────────────────────────────────────────────────
    # v1 = formule IEC standard (IANOPCA + IANOFO au numérateur)
    # v2 = formule maintenance-corrected (IANOSM above-cap au numérateur)
    # v3 = disponibilité déclarée par le mainteneur (GenAI)
    F.coalesce(F.col("_TB_M_C_Av_v1"), F.col("_farm_TB_M_C_Av_v1")).alias("TB_M_C_Av_v1"),
    F.coalesce(F.col("_TB_M_C_Av_v2"), F.col("_farm_TB_M_C_Av_v2")).alias("TB_M_C_Av_v2"),
    F.col("_TB_M_C_Av_v3").cast("double").alias("TB_M_C_Av_v3"),
    F.coalesce(F.col("_EB_M_C_Av_v1"), F.col("_farm_EB_M_C_Av_v1")).alias("EB_M_C_Av_v1"),
    F.coalesce(F.col("_EB_M_C_Av_v2"), F.col("_farm_EB_M_C_Av_v2")).alias("EB_M_C_Av_v2"),
)

# COMMAND ----------

print(f"Lignes: {result.count()}")
result.printSchema()

# COMMAND ----------

result.filter(F.col("WTcode").isNotNull()).show(20, truncate=False)

# COMMAND ----------

# Sauvegarde dans une table de debug (overwrite à chaque run)
result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.gold.diag_pm_budget_tracker_enriched"
)
print(f"Écrit : {CATALOG}.gold.diag_pm_budget_tracker_enriched")
