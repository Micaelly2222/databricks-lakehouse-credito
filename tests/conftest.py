import shutil
import tempfile
import uuid
from datetime import date

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from credit_lakehouse.config import SERIES, Tables


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip

    warehouse = tempfile.mkdtemp(prefix="lakehouse-")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def tables(spark):
    schema = "t_" + uuid.uuid4().hex[:8]
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Tables(schema)
    spark.sql(f"DROP SCHEMA {schema} CASCADE")


def monthly_values(code: int, i: int) -> float:
    return {
        433: 0.5, 4189: 10.0, 20539: 1000 + 10 * i, 20540: 400 + 4 * i, 20541: 600 + 6 * i,
        21082: 3.0, 21083: 2.0, 21084: 4.0, 20714: 30.0, 24364: 100 + i,
    }[code]


class FakeSGS:
    """Simula a API do Banco Central: 30 meses de dados e um dólar diário."""

    def __init__(self, overrides: dict | None = None, extra_rows: dict | None = None):
        self.overrides = overrides or {}
        self.extra_rows = extra_rows or {}
        self.calls = []

    def __call__(self, code: int, start: date, end: date) -> list[dict]:
        self.calls.append((code, start, end))
        if code == 1:
            days = pd.date_range("2022-01-01", periods=900, freq="D")
            rows = [{"data": d.strftime("%d/%m/%Y"), "valor": "5.0"} for d in days]
        else:
            months = pd.date_range("2022-01-01", periods=30, freq="MS")
            rows = [
                {"data": d.strftime("%d/%m/%Y"), "valor": str(self.overrides.get((code, i), monthly_values(code, i)))}
                for i, d in enumerate(months)
            ]
        rows = [r for r in rows if start <= pd.to_datetime(r["data"], format="%d/%m/%Y").date() <= end]
        return rows + self.extra_rows.get(code, [])


ALL_CODES = [s["code"] for s in SERIES]
