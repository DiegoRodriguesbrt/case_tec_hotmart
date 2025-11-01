-- DDL de exemplo para a tabela Delta `silver.fct_gmv_diario`
-- Ajuste o local/path conforme a sua configuração de metastore e storage.
-- Caso use Unity Catalog ou outro catálogo, prefixe com <catalog>.silver.fct_gmv_diario

CREATE TABLE IF NOT EXISTS silver.fct_gmv_diario (
    gmv_date DATE COMMENT 'Data de negócio do GMV criada com base na release_date',
    subsidiary STRING COMMENT 'Empresa que embora controlada ou dirigida por outra possui grande parte ou o total de suas ações',
    gmv_total_day DOUBLE COMMENT 'Valor total de GMV calculado para o dia e subsidiária',
    calculation_timestamp TIMESTAMP COMMENT 'Momento em que o cálculo foi materializado',
    is_latest BOOLEAN COMMENT 'Indicador se esta linha é a versão mais recente do cálculo'
)
USING DELTA
PARTITIONED BY (gmv_date)
COMMENT 'Fato diária de GMV por subsidiária com versionamento imutável (SCD Type 2 via snapshots).'
LOCATION 'path_to_external_table';
 