-- Quais são os 2 produtos que mais faturaram ($) de cada produtor? 

WITH producer_product_revenue AS (
    SELECT
        p.producer_id,
        pi.product_id,
        SUM(pi.purchase_value) AS total_revenue
    FROM
        purchase p
    JOIN
        product_item pi ON p.prod_item_id = pi.prod_item_id AND p.prod_item_partition = pi.prod_item_partition
    WHERE
        p.purchase_status = 'APROVADA'
    GROUP BY
        p.producer_id,
        pi.product_id
),
ranked_products AS (
    SELECT
        producer_id,
        product_id,
        total_revenue,
        ROW_NUMBER() OVER(PARTITION BY producer_id ORDER BY total_revenue DESC) AS product_rank
    FROM
        producer_product_revenue
)
SELECT
    producer_id,
    product_id,
    total_revenue
FROM
    ranked_products
WHERE
    product_rank <= 2
ORDER BY
    producer_id,
    product_rank;