SELECT DATE(purchase_datetime) purchase_date, -- use DATE_TRUNC for week or month 
       COUNT(DISTINCT order_id) num_of_order
FROM `needle.fact__order_transaction`
GROUP BY 1
ORDER BY 1