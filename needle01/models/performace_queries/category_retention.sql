WITH tx AS(
  SELECT
    purchase_datetime,order_id, customer_unique_id,
    product_category,COUNT(order_id) AS num_of_item
  FROM `needle.fact__order_transaction`
  GROUP BY 1,2,3,4
),
list AS(
  SELECT *,
       ROW_NUMBER() OVER(PARTITION BY customer_unique_id
                         ORDER BY purchase_datetime ASC) rn1,
       ROW_NUMBER() OVER(PARTITION BY customer_unique_id,product_category
                         ORDER BY purchase_datetime ASC) rn2
  FROM tx
),
new_list AS(
  SELECT * EXCEPT(rn1,rn2),
         ROW_NUMBER() OVER (PARTITION BY customer_unique_id,product_category,rn1 - rn2
                          ORDER BY purchase_datetime ASC) AS rn
  FROM list
)
SELECT *
FROM new_list
WHERE rn > 1
ORDER BY 1