import argparse
import logging

from credit_lakehouse import bronze, gold, quality, silver
from credit_lakehouse.config import Tables
from credit_lakehouse.spark import get_spark

logger = logging.getLogger("credit_lakehouse")


def run(step: str, catalog: str | None, schema: str, batch_id: str | None = None, optimize: bool = False) -> str | None:
    spark = get_spark()
    namespace = f"{catalog}.{schema}" if catalog else schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
    tables = Tables(namespace)

    if step in ("bronze", "all"):
        batch_id = bronze.ingest(spark, tables)
    if step in ("silver", "all"):
        if not batch_id:
            batch_id = spark.table(tables.bronze).agg({"batch_id": "max"}).collect()[0][0]
        logger.info("Silver: %s", silver.process_batch(spark, tables, batch_id))
    if step in ("gold", "all"):
        gold.publish(spark, tables, optimize=optimize)
    if step in ("quality", "all"):
        for check in quality.log_and_assert(spark, tables, run_id=batch_id or "manual"):
            logger.info("%s %s (%s)", "OK  " if check.passed else "FAIL", check.name, check.detail)
    return batch_id


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Lakehouse de crédito com dados do Banco Central")
    parser.add_argument("--step", choices=["bronze", "silver", "gold", "quality", "all"], default="all")
    parser.add_argument("--catalog", default=None, help="catálogo do Unity Catalog (vazio para rodar local)")
    parser.add_argument("--schema", default="credito")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--optimize", action="store_true")
    args = parser.parse_args()
    run(args.step, args.catalog, args.schema, args.batch_id, args.optimize)


if __name__ == "__main__":
    main()
