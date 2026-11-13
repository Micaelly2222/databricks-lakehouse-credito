from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from credit_lakehouse.config import SERIES, Tables


class QualityError(Exception):
    pass


@dataclass
class Check:
    name: str
    passed: bool
    detail: str
    critical: bool = True


def run_checks(spark: SparkSession, tables: Tables) -> list[Check]:
    silver = spark.table(tables.silver)
    gold = spark.table(tables.gold_credit)
    checks = []

    dupes = silver.groupBy("series_code", "ref_date").count().filter("count > 1").count()
    checks.append(Check("silver_sem_duplicidade", dupes == 0, f"{dupes} chaves duplicadas"))

    nulls = silver.filter(F.col("value").isNull() | F.col("ref_date").isNull()).count()
    checks.append(Check("silver_sem_nulos", nulls == 0, f"{nulls} linhas nulas"))

    expected = {s["code"] for s in SERIES}
    present = {r[0] for r in silver.select("series_code").distinct().collect()}
    missing = sorted(expected - present)
    checks.append(Check("todas_as_series_carregadas", not missing, f"faltando: {missing}"))

    inconsistent = gold.filter(
        F.col("credito_total").isNotNull()
        & (F.abs(F.col("credito_pf") + F.col("credito_pj") - F.col("credito_total")) > F.col("credito_total") * 0.01)
    ).count()
    checks.append(Check("credito_pf_mais_pj_igual_total", inconsistent == 0, f"{inconsistent} meses com diferença acima de 1%"))

    latest = gold.agg(F.max("month")).collect()[0][0]
    stale = spark.sql(f"SELECT months_between(current_date(), DATE'{latest}') AS m").collect()[0][0] if latest else None
    checks.append(Check("dados_recentes", stale is not None and stale <= 4, f"último mês {latest}", critical=False))
    return checks


def log_and_assert(spark: SparkSession, tables: Tables, run_id: str) -> list[Check]:
    checks = run_checks(spark, tables)
    rows = [(run_id, c.name, c.passed, c.detail, c.critical) for c in checks]
    (
        spark.createDataFrame(rows, "run_id string, check_name string, passed boolean, detail string, critical boolean")
        .withColumn("checked_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(tables.quality)
    )
    failed = [c for c in checks if c.critical and not c.passed]
    if failed:
        raise QualityError("; ".join(f"{c.name}: {c.detail}" for c in failed))
    return checks
