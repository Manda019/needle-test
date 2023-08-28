WITH tx AS(
  SELECT a.order_id,
         order_purchase_timestamp AS purchase_datetime,
         order_approved_at AS approved_datetime,
         UPPER(order_status) AS status,
         customer_unique_id,
         product_category_name AS product_category

  FROM `needle.olist_orders_dataset` a
  LEFT JOIN `needle.olist_order_items_dataset` b
    ON a.order_id = b.order_id
  LEFT JOIN `needle.olist_products_dataset`c
    ON b.product_id = c.product_id
  LEFT JOIN `needle.olist_customers_dataset`d
    ON a.customer_id = d.customer_id
)
SELECT * FROM tx