WITH tx AS(
  SELECT DATE(purchase_datetime) purchase_date,-- use DATE_TRUNC for week or month 
         customer_unique_id,
         COUNT(DISTINCT order_id) num_of_order
  FROM `needle.fact__order_transaction`
  GROUP BY 1,2
)
SELECT purchase_date,
       customer_unique_id
FROM tx
WHERE num_of_order >=1