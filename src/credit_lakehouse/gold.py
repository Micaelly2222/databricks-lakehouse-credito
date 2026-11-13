from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from credit_lakehouse.config import SERIES, Tables

KEYS = [s["key"] for s in SERIES]


def build_monthly(spark: SparkSession, tables: Tables) -> DataFrame:
    # Séries diárias (dólar) viram média do mês; o valor de fim de mês também fica disponível
    return (
        spark.table(tables.silver).alias("i")
        .join(spark.table(tables.series).alias("s"), "series_code")
        .groupBy("series_code", "series_key", F.trunc("ref_date", "month").alias("month"))
        .agg(
            F.avg("value").alias("value"),
            F.max_by("value", "ref_date").alias("value_end_of_month"),
            F.count("*").alias("observations"),
        )
    )


def build_credit_kpis(monthly: DataFrame) -> DataFrame:
    wide = monthly.groupBy("month").pivot("series_key", KEYS).agg(F.first("value"))
    by_month = Window.orderBy("month")
    last_12 = by_month.rowsBetween(-11, 0)

    ipca_factor = F.exp(F.sum(F.log1p(F.col("ipca") / 100)).over(last_12))
    ipca_months = F.count("ipca").over(last_12)

    def yoy(col: str):
        return F.round((F.col(col) / F.lag(col, 12).over(by_month) - 1) * 100, 2)

    return (
        wide
        .withColumn("ipca_12m", F.when(ipca_months == 12, F.round((ipca_factor - 1) * 100, 2)))
        .withColumn(
            "juro_real",
            F.when(ipca_months == 12, F.round(((1 + F.col("selic") / 100) / ipca_factor - 1) * 100, 2)),
        )
        .withColumn("credito_total_yoy", yoy("credito_total"))
        .withColumn("credito_pf_yoy", yoy("credito_pf"))
        .withColumn("credito_pj_yoy", yoy("credito_pj"))
        .withColumn("participacao_pf", F.round(F.col("credito_pf") / F.col("credito_total") * 100, 2))
        .withColumn("inad_total_var_12m_pp", F.round(F.col("inad_total") - F.lag("inad_total", 12).over(by_month), 2))
        .withColumn("spread_bancario", F.round(F.col("juros_credito") - F.col("selic"), 2))
        .withColumn("ibc_br_yoy", yoy("ibc_br"))
        .withColumn("updated_at", F.current_timestamp())
    )


def publish(spark: SparkSession, tables: Tables, optimize: bool = False) -> None:
    monthly = build_monthly(spark, tables)
    monthly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tables.gold_monthly)
    kpis = build_credit_kpis(spark.table(tables.gold_monthly))
    kpis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tables.gold_credit)
    if optimize:
        spark.sql(f"OPTIMIZE {tables.silver} ZORDER BY (series_code, ref_date)")
        spark.sql(f"OPTIMIZE {tables.gold_monthly} ZORDER BY (series_key, month)")
