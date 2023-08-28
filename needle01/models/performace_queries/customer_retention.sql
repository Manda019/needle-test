WITH df AS (
SELECT DATE(DATE_TRUNC(purchase_datetime, WEEK)) data_week,
customer_unique_id,
COUNT(DISTINCT order_id) total_order
FROM `needle.fact__order` 
GROUP BY 1, 2
),


acquired AS (
SELECT customer_unique_id, MIN(data_week) acquired_date
FROM df
GROUP BY 1
),

final AS (
SELECT a.customer_unique_id, a.acquired_date,
DATE_DIFF(data_week, acquired_date, WEEK) interval_week
FROM acquired a
LEFT JOIN df b
ON a.customer_unique_id = b.customer_unique_id
ORDER BY 1, 2, 3
)

SELECT acquired_date, interval_week, COUNT(DISTINCT customer_unique_id) total_user
FROM final
GROUP BY 1, 2
ORDER BY 1, 2