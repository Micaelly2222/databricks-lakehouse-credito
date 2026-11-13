import logging
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Callable

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from credit_lakehouse.config import SERIES, Tables

logger = logging.getLogger(__name__)

URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
FIRST_DATE = date(2012, 1, 1)
# O Banco Central revisa os últimos meses; cada carga relê esse período para capturar revisões
REVISION_LOOKBACK_DAYS = 190

BRONZE_SCHEMA = StructType([
    StructField("series_code", IntegerType(), False),
    StructField("raw_date", StringType(), True),
    StructField("raw_value", StringType(), True),
])

Fetcher = Callable[[int, date, date], list[dict]]


def fetch_sgs(code: int, start: date, end: date, retries: int = 4) -> list[dict]:
    rows: list[dict] = []
    window_start = start
    while window_start <= end:
        window_end = min(date(window_start.year + 9, 12, 31), end)
        params = {
            "formato": "json",
            "dataInicial": window_start.strftime("%d/%m/%Y"),
            "dataFinal": window_end.strftime("%d/%m/%Y"),
        }
        for attempt in range(retries):
            try:
                response = requests.get(URL.format(code=code), params=params, timeout=60)
                if response.status_code == 404:
                    break
                response.raise_for_status()
                rows.extend(response.json())
                break
            except (requests.RequestException, ValueError):
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)
        window_start = date(window_end.year + 1, 1, 1)
    return rows


def watermarks(spark: SparkSession, tables: Tables) -> dict[int, date]:
    if not spark.catalog.tableExists(tables.silver):
        return {}
    rows = spark.table(tables.silver).groupBy("series_code").agg(F.max("ref_date").alias("last")).collect()
    return {r["series_code"]: r["last"] for r in rows}


def ingest(spark: SparkSession, tables: Tables, fetcher: Fetcher = fetch_sgs, today: date | None = None) -> str:
    today = today or date.today()
    batch_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
    marks = watermarks(spark, tables)

    records = []
    for series in SERIES:
        last = marks.get(series["code"])
        start = max(FIRST_DATE, last - timedelta(days=REVISION_LOOKBACK_DAYS)) if last else FIRST_DATE
        rows = fetcher(series["code"], start, today)
        records.extend((series["code"], r.get("data"), r.get("valor")) for r in rows)
        logger.info("%-14s desde %s: %d registros", series["key"], start, len(rows))

    df: DataFrame = (
        spark.createDataFrame(records, BRONZE_SCHEMA)
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source", F.lit("bcb_sgs_api"))
    )
    df.write.format("delta").mode("append").saveAsTable(tables.bronze)
    logger.info("Lote %s gravado na bronze com %d linhas", batch_id, len(records))
    return batch_id
