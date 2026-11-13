from datetime import date

import pytest
from pyspark.sql import functions as F

from credit_lakehouse import bronze, gold, quality, silver
from credit_lakehouse.config import SERIES
from tests.conftest import FakeSGS

TODAY = date(2024, 6, 30)


def load_all(spark, tables, fetcher):
    batch = bronze.ingest(spark, tables, fetcher=fetcher, today=TODAY)
    stats = silver.process_batch(spark, tables, batch)
    gold.publish(spark, tables)
    return batch, stats


def test_bronze_guarda_dado_bruto_com_lote(spark, tables):
    batch = bronze.ingest(spark, tables, fetcher=FakeSGS(), today=TODAY)
    df = spark.table(tables.bronze)
    assert df.select("batch_id").distinct().collect()[0][0] == batch
    assert set(df.columns) >= {"series_code", "raw_date", "raw_value", "ingested_at", "source"}
    assert dict(df.dtypes)["raw_value"] == "string"


def test_silver_tipa_deduplica_e_isola_linhas_ruins(spark, tables):
    extra = {
        433: [{"data": "01/03/2022", "valor": "0.5"}, {"data": "data ruim", "valor": "1"}],
        21082: [{"data": "01/12/2030", "valor": "150"}],
    }
    batch = bronze.ingest(spark, tables, fetcher=FakeSGS(extra_rows=extra), today=TODAY)
    stats = silver.process_batch(spark, tables, batch)

    s = spark.table(tables.silver)
    assert s.groupBy("series_code", "ref_date").count().filter("count > 1").count() == 0
    reasons = {r[0] for r in spark.table(tables.quarantine).select("reject_reason").collect()}
    assert reasons == {"data inválida", "taxa fora de 0-100%"}
    assert stats["rejected"] == 2


def test_reprocessar_o_mesmo_lote_e_idempotente(spark, tables):
    batch = bronze.ingest(spark, tables, fetcher=FakeSGS(), today=TODAY)
    silver.process_batch(spark, tables, batch)
    before = spark.table(tables.silver).count()
    stats = silver.process_batch(spark, tables, batch)
    assert spark.table(tables.silver).count() == before
    assert stats["inserted"] == 0 and stats["updated"] == 0


def test_revisao_do_banco_central_atualiza_valor_e_conta_revisao(spark, tables):
    load_all(spark, tables, FakeSGS())
    revised = FakeSGS(overrides={(20539, 29): 9999.0})
    batch = bronze.ingest(spark, tables, fetcher=revised, today=TODAY)
    stats = silver.process_batch(spark, tables, batch)

    row = spark.table(tables.silver).filter((F.col("series_code") == 20539) & (F.col("ref_date") == "2024-06-01")).first()
    assert row["value"] == 9999.0
    assert row["revision_count"] == 1
    assert stats["updated"] == 1


def test_carga_incremental_usa_marca_d_agua_com_janela_de_revisao(spark, tables):
    load_all(spark, tables, FakeSGS())
    fetcher = FakeSGS()
    bronze.ingest(spark, tables, fetcher=fetcher, today=TODAY)
    starts = {code: start for code, start, _ in fetcher.calls}
    assert starts[433] > date(2012, 1, 1)
    assert starts[433] <= date(2024, 6, 1)


def test_gold_calcula_indicadores(spark, tables):
    load_all(spark, tables, FakeSGS())
    kpis = spark.table(tables.gold_credit).orderBy("month").toPandas()

    assert kpis["ipca_12m"].iloc[:11].isna().all()
    expected_ipca = round((1.005 ** 12 - 1) * 100, 2)
    assert kpis["ipca_12m"].iloc[11] == pytest.approx(expected_ipca)
    assert kpis["juro_real"].iloc[11] == pytest.approx((1.10 / 1.005 ** 12 - 1) * 100, abs=0.01)

    row = kpis.iloc[12]
    assert row["credito_total_yoy"] == pytest.approx(12.0)
    assert row["participacao_pf"] == pytest.approx(60.0)
    assert row["spread_bancario"] == pytest.approx(20.0)
    assert row["usd_brl"] == pytest.approx(5.0)


def test_dolar_diario_vira_mensal(spark, tables):
    load_all(spark, tables, FakeSGS())
    usd = spark.table(tables.gold_monthly).filter("series_key = 'usd_brl' AND month = '2022-01-01'").first()
    assert usd["observations"] == 31
    assert usd["value"] == pytest.approx(5.0)


def test_qualidade_passa_e_fica_registrada(spark, tables):
    load_all(spark, tables, FakeSGS())
    checks = quality.log_and_assert(spark, tables, run_id="teste")
    critical = [c for c in checks if c.critical]
    assert all(c.passed for c in critical)
    assert spark.table(tables.quality).count() == len(checks)


def test_qualidade_falha_quando_pf_mais_pj_nao_fecha(spark, tables):
    load_all(spark, tables, FakeSGS(overrides={(20540, 5): 10.0}))
    with pytest.raises(quality.QualityError, match="credito_pf_mais_pj_igual_total"):
        quality.log_and_assert(spark, tables, run_id="teste")


def test_dimensao_de_series_tem_todas(spark, tables):
    load_all(spark, tables, FakeSGS())
    assert spark.table(tables.series).count() == len(SERIES)
