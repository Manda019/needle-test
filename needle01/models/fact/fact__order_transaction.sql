{{ config(
    materialized='table')
    }}

WITH orders_dataset as(
    SELECT * FROM {{  source('raw','olist_orders_dataset')  }}
),
order_items_dataset as(
    SELECT * FROM {{  source('raw','olist_order_items_dataset')  }}
),
products_dataset as(
    SELECT * FROM {{  source('raw','olist_products_dataset')  }}
),
customers_dataset as(
    SELECT * FROM {{  source('raw','olist_customers_dataset')  }}
),
tx AS(
  SELECT a.order_id,
         order_purchase_timestamp AS purchase_datetime,
         order_approved_at AS approved_datetime,
         UPPER(order_status) AS status,
         customer_unique_id,
         product_category_name AS product_category

  FROM orders_dataset a
  LEFT JOIN order_items_dataset b
    ON a.order_id = b.order_id
  LEFT JOIN products_dataset c
    ON b.product_id = c.product_id
  LEFT JOIN customers_dataset d
    ON a.customer_id = d.customer_id
),
final AS(
  SELECT * FROM tx
)
SELECT * FROM final