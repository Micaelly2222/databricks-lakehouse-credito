-- Governança no Unity Catalog. Rodar uma vez por ambiente, com um usuário administrador.
-- Grupos: data_engineers (escreve em tudo), data_analysts (lê silver e gold), bi_consumers (lê só a gold).

CREATE SCHEMA IF NOT EXISTS workspace.credito
COMMENT 'Lakehouse de crédito e indicadores econômicos do Banco Central (SGS)';

-- Engenharia: dona do schema
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA workspace.credito TO `data_engineers`;

-- Analistas: leitura de silver e gold, sem acesso à bronze nem à quarentena
GRANT USE SCHEMA ON SCHEMA workspace.credito TO `data_analysts`;
GRANT SELECT ON TABLE workspace.credito.silver_indicator TO `data_analysts`;
GRANT SELECT ON TABLE workspace.credito.silver_series TO `data_analysts`;
GRANT SELECT ON TABLE workspace.credito.gold_indicator_monthly TO `data_analysts`;
GRANT SELECT ON TABLE workspace.credito.gold_credit_kpis TO `data_analysts`;

-- Consumidores de BI: só a camada gold
GRANT USE SCHEMA ON SCHEMA workspace.credito TO `bi_consumers`;
GRANT SELECT ON TABLE workspace.credito.gold_credit_kpis TO `bi_consumers`;

-- Documentação das tabelas e colunas (aparece no Catalog Explorer)
COMMENT ON TABLE workspace.credito.bronze_sgs_raw IS 'Dados brutos da API do SGS, só acrescentados, com lote e horário de ingestão';
COMMENT ON TABLE workspace.credito.silver_indicator IS 'Uma linha por série e data, tipada e sem duplicidade. Revisões do Banco Central atualizam o valor via MERGE';
COMMENT ON TABLE workspace.credito.silver_quarantine IS 'Linhas rejeitadas pelas regras de qualidade, com o motivo';
COMMENT ON TABLE workspace.credito.gold_credit_kpis IS 'Indicadores mensais de crédito, inadimplência, juros e inflação para BI';
ALTER TABLE workspace.credito.silver_indicator ALTER COLUMN revision_count COMMENT 'Quantas vezes o Banco Central revisou o valor desta data';
ALTER TABLE workspace.credito.gold_credit_kpis ALTER COLUMN juro_real COMMENT 'Selic descontada do IPCA acumulado em 12 meses (% a.a.)';

-- Classificação por tags
ALTER TABLE workspace.credito.bronze_sgs_raw SET TAGS ('layer' = 'bronze', 'source' = 'bcb_sgs', 'pii' = 'none');
ALTER TABLE workspace.credito.silver_indicator SET TAGS ('layer' = 'silver', 'source' = 'bcb_sgs', 'pii' = 'none');
ALTER TABLE workspace.credito.gold_credit_kpis SET TAGS ('layer' = 'gold', 'domain' = 'credito', 'pii' = 'none');

-- Filtro de linha: consumidores de BI veem apenas dados a partir de 2015
CREATE OR REPLACE FUNCTION workspace.credito.rf_bi_desde_2015(month DATE)
RETURN IS_ACCOUNT_GROUP_MEMBER('data_engineers')
    OR IS_ACCOUNT_GROUP_MEMBER('data_analysts')
    OR month >= DATE'2015-01-01';

ALTER TABLE workspace.credito.gold_credit_kpis SET ROW FILTER workspace.credito.rf_bi_desde_2015 ON (month);

-- Auditoria: quem leu a gold nos últimos 7 dias
SELECT event_time, user_identity.email, action_name
FROM system.access.audit
WHERE request_params.full_name_arg = 'workspace.credito.gold_credit_kpis'
  AND event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- Linhagem: de onde vem a gold
SELECT source_table_full_name, target_table_full_name, event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'workspace.credito.gold_credit_kpis'
ORDER BY event_time DESC;
