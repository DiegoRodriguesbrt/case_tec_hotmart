-- Quais são os 50 maiores produtores em faturamento ($) de 2021? 

SELECT
    producer_id,
    SUM(purchase_total_value) AS total_revenue
FROM
    purchase
WHERE
    release_date >= '2021-01-01' AND release_date < '2022-01-01'
    AND purchase_status = 'APROVADA'
GROUP BY
    producer_id
ORDER BY
    total_revenue DESC
LIMIT 50; 
 