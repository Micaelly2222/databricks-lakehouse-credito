from dataclasses import dataclass, field

# Séries do SGS (Banco Central). Para incluir uma série nova, basta acrescentar aqui.
SERIES = [
    {"code": 433, "key": "ipca", "name": "IPCA - variação mensal", "unit": "%", "frequency": "M", "topic": "inflacao"},
    {"code": 4189, "key": "selic", "name": "Taxa Selic anualizada", "unit": "% a.a.", "frequency": "M", "topic": "juros"},
    {"code": 1, "key": "usd_brl", "name": "Dólar comercial (venda)", "unit": "R$", "frequency": "D", "topic": "cambio"},
    {"code": 20539, "key": "credito_total", "name": "Saldo de crédito - total", "unit": "R$ milhões", "frequency": "M", "topic": "credito"},
    {"code": 20540, "key": "credito_pj", "name": "Saldo de crédito - pessoas jurídicas", "unit": "R$ milhões", "frequency": "M", "topic": "credito"},
    {"code": 20541, "key": "credito_pf", "name": "Saldo de crédito - pessoas físicas", "unit": "R$ milhões", "frequency": "M", "topic": "credito"},
    {"code": 21082, "key": "inad_total", "name": "Inadimplência - total", "unit": "%", "frequency": "M", "topic": "inadimplencia"},
    {"code": 21083, "key": "inad_pj", "name": "Inadimplência - pessoas jurídicas", "unit": "%", "frequency": "M", "topic": "inadimplencia"},
    {"code": 21084, "key": "inad_pf", "name": "Inadimplência - pessoas físicas", "unit": "%", "frequency": "M", "topic": "inadimplencia"},
    {"code": 20714, "key": "juros_credito", "name": "Taxa média de juros do crédito", "unit": "% a.a.", "frequency": "M", "topic": "juros"},
    {"code": 24364, "key": "ibc_br", "name": "IBC-Br dessazonalizado", "unit": "índice", "frequency": "M", "topic": "atividade"},
]


@dataclass(frozen=True)
class Tables:
    """Nomes das tabelas. No Databricks o namespace é catalog.schema; nos testes, só o schema."""

    namespace: str
    names: dict = field(default_factory=lambda: {
        "bronze": "bronze_sgs_raw",
        "series": "silver_series",
        "silver": "silver_indicator",
        "quarantine": "silver_quarantine",
        "gold_monthly": "gold_indicator_monthly",
        "gold_credit": "gold_credit_kpis",
        "quality": "ops_quality_log",
    })

    def __getattr__(self, item: str) -> str:
        names = object.__getattribute__(self, "names")
        if item in names:
            return f"{self.namespace}.{names[item]}"
        raise AttributeError(item)
