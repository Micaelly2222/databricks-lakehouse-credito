import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from credit_lakehouse.config import SERIES, Tables

logger = logging.getLogger(__name__)

RATE_SERIES = [21082, 21083, 21084]
BALANCE_SERIES = [20539, 20540, 20541]


def upsert_series(spark: SparkSession, tables: Tables) -> None:
    source = spark.createDataFrame(SERIES).withColumn("code", F.col("code").cast("int"))
    source = source.withColumnRenamed("code", "series_code").withColumnRenamed("key", "series_key")
    if not spark.catalog.tableExists(tables.series):
        source.write.format("delta").saveAsTable(tables.series)
        return
    (
        DeltaTable.forName(spark, tables.series).alias("t")
        .merge(source.alias("s"), "t.series_code = s.series_code")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def parse(bronze: DataFrame) -> DataFrame:
    return (
        bronze
        .withColumn("ref_date", F.expr("try_to_date(raw_date, 'dd/MM/yyyy')"))
        .withColumn("value", F.expr("try_cast(raw_value AS DOUBLE)"))
        .withColumn(
            "reject_reason",
            F.when(F.col("ref_date").isNull(), "data inválida")
            .when(F.col("value").isNull(), "valor inválido")
            .when(F.col("series_code").isin(RATE_SERIES) & ~F.col("value").between(0, 100), "taxa fora de 0-100%")
            .when(F.col("series_code").isin(BALANCE_SERIES) & (F.col("value") <= 0), "saldo não positivo"),
        )
    )


def latest_per_key(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("series_code", "ref_date").orderBy(F.col("ingested_at").desc(), F.col("raw_value").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def process_batch(spark: SparkSession, tables: Tables, batch_id: str) -> dict:
    upsert_series(spark, tables)
    parsed = parse(spark.table(tables.bronze).filter(F.col("batch_id") == batch_id))

    rejected = parsed.filter(F.col("reject_reason").isNotNull())
    n_rejected = rejected.count()
    if n_rejected:
        (
            rejected.select("series_code", "raw_date", "raw_value", "reject_reason", "batch_id", "ingested_at")
            .write.format("delta").mode("append").saveAsTable(tables.quarantine)
        )
        logger.warning("%d linhas enviadas para a quarentena", n_rejected)

    valid = latest_per_key(parsed.filter(F.col("reject_reason").isNull())).select(
        "series_code", "ref_date", "value", "batch_id", F.col("ingested_at").alias("updated_at")
    )

    if not spark.catalog.tableExists(tables.silver):
        (
            valid.withColumn("revision_count", F.lit(0))
            .write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(tables.silver)
        )
        return {"inserted": valid.count(), "updated": 0, "rejected": n_rejected}

    # Upsert idempotente: reprocessar o mesmo lote não duplica e só conta revisão se o valor mudou
    target = DeltaTable.forName(spark, tables.silver)
    (
        target.alias("t")
        .merge(valid.alias("s"), "t.series_code = s.series_code AND t.ref_date = s.ref_date")
        .whenMatchedUpdate(
            condition="t.value <> s.value",
            set={
                "value": "s.value",
                "batch_id": "s.batch_id",
                "updated_at": "s.updated_at",
                "revision_count": "t.revision_count + 1",
            },
        )
        .whenNotMatchedInsert(
            values={
                "series_code": "s.series_code",
                "ref_date": "s.ref_date",
                "value": "s.value",
                "batch_id": "s.batch_id",
                "updated_at": "s.updated_at",
                "revision_count": "0",
            }
        )
        .execute()
    )
    metrics = target.history(1).select("operationMetrics").collect()[0][0]
    return {
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
        "rejected": n_rejected,
    }
