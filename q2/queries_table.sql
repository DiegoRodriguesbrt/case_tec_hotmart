 -- Para obter a visão mais recente e correta do GMV, utilize o flag is_latest

 SELECT
  gmv_date,
  subsidiary,
  gmv_total_day
FROM silver.fct_gmv_diario
WHERE is_latest = true

-- Consultando o GMV em um Ponto no Tempo (Viagem no Tempo)

WITH ranked_history AS (
  SELECT
    gmv_date,
    subsidiary,
    gmv_total_day,
    ROW_NUMBER() OVER(PARTITION BY gmv_date, subsidiary ORDER BY calculation_timestamp DESC) as rn
  FROM silver.fct_gmv_diario
  WHERE calculation_timestamp <= '2023-03-31 23:59:59'
    AND gmv_date BETWEEN '2023-01-01' AND '2023-01-31'
)
SELECT
  gmv_date,
  subsidiary,
  gmv_total_day
FROM ranked_history
WHERE rn = 1;