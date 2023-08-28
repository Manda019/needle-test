{{ config(
    tags=["daily"],
    materialized='table',
    partition_by={
      "field": "last_updated_datetime",
      "data_type": "datetime",
      "granularity": "day"
    }
)}}


WITH
qris_transaction AS (
SELECT transaction_id, charge_vendor_code,
       
       CASE
       WHEN charge_vendor_code = 'shopee' THEN 'SHOPEEPAY'
       ELSE UPPER(charge_vendor_code) 
       END AS vendor,

       CASE 
       WHEN charge_vendor_code = 'SHOPEEPAY' THEN 
            CASE 
            WHEN REGEXP_CONTAINS(user_id_hash, '[A-z]+') = TRUE THEN 'ON_US'
            ELSE 'OFF_US'
            END
       WHEN charge_vendor_code = 'NOBU' THEN 
            CASE 
            WHEN JSON_VALUE(vendor_additional_data, '$.issuerId') = '93600503' THEN 'ON_US'
            ELSE 'OFF_US'  
            END
       WHEN charge_vendor_code ='mnc' THEN
            CASE
            WHEN UPPER(JSON_VALUE(vendor_additional_data, '$.fromInfo'))= 'MOTIONPAY' THEN 'ON_US'  --updated by Manda mnc vendor
            ELSE 'OFF_US'
            END 
       ELSE transaction_channel_type --updated by Fathur 28 July 2023 for Dana,Danamon an New Vendor in the Future
       END category,

       ROW_NUMBER() OVER(PARTITION BY id ORDER BY last_updated_datetime DESC) AS row_num
  FROM {{ ref('stg__qris_transaction') }}
),
b2x_checkout_transaction AS (
SELECT *,
       ROW_NUMBER() OVER(PARTITION BY id ORDER BY last_updated_datetime DESC) AS row_num
  FROM {{ ref('stg__b2x_checkout_transaction') }}
),
b2x_payment_checkout AS (
SELECT bct.created_datetime,
       bct.last_updated_datetime,
       bct.id,
       'B2X_PAYMENT_CHECKOUT' AS feature,
       bct.charge_amount,
       bct.transaction_status,
       bct.payment_method,
       bct.sender_bank_code AS bank_code,
       bct.sender_bank_code AS bank_name,
       UPPER(COALESCE(CONCAT(qt.vendor, '_', qt.category), bct.sender_bank_code, 'UNDEFINED')) AS vendor
  FROM {{ ref('fact__b2x_payment_checkout') }} AS bct
  LEFT JOIN qris_transaction AS qt 
    ON bct.transaction_id = qt.transaction_id
   AND UPPER(bct.payment_method) = 'QRIS'
   AND qt.row_num = 1
  WHERE bct.payment_method ='QRIS'
),
b2x_payment_routing_acceptance AS (
SELECT pr.created_datetime,
       pr.last_updated_datetime,
       pr.id,
       'B2X_PAYMENT_ROUTING_ACCEPTANCE' AS feature,
       pr.charge_amount,
       pr.transaction_status,
       pr.payment_method,
       pr.sender_bank_code AS bank_code,
       bct.sender_bank_code AS bank_name,
       UPPER(COALESCE(CONCAT(qt.vendor, '_', qt.category), bct.sender_bank_code, 'UNDEFINED')) AS vendor

  FROM {{ ref('fact__b2x_payment_routing_acceptance') }} AS pr
  LEFT JOIN b2x_checkout_transaction AS bct 
    ON pr.acceptance_transaction_id = bct.payment_link_id
   AND bct.row_num = 1
   AND bct.is_active = TRUE
  LEFT JOIN qris_transaction AS qt 
    ON bct.charge_transaction_id = qt.transaction_id
   AND UPPER(pr.payment_method) = 'QRIS'
   AND qt.row_num = 1
  WHERE UPPER(pr.payment_method) ='QRIS'
),

b2x_payment_invoice AS (
SELECT inv.created_datetime,
       inv.last_updated_datetime,
       inv.id,
       CONCAT('B2X_', inv.feature) AS feature,
       inv.charge_amount,
       inv.transaction_status,
       inv.payment_method,
       inv.sender_bank_code AS bank_code,
       UPPER(COALESCE(inv.sender_bank_code, 'UNDEFINED')) AS bank_name,
       UPPER(COALESCE(CONCAT(qt.vendor, '_', qt.category), inv.sender_bank_code, 'UNDEFINED'))AS vendor
  FROM {{ ref('fact__b2x_payment_invoice') }} AS inv
  LEFT JOIN qris_transaction AS qt 
    ON inv.transaction_id = qt.transaction_id
   AND UPPER(inv.payment_method) = 'QRIS'  
   AND qt.row_num = 1  
  WHERE inv.payment_method = 'QRIS'
),

deduction_transaction AS (
SELECT *
  FROM b2x_payment_checkout
 UNION ALL 
SELECT *
  FROM b2x_payment_routing_acceptance
 UNION ALL
SELECT *
  FROM b2x_payment_invoice
),

all_deduction AS (
SELECT *,
       CASE 
       WHEN transaction_status IN ('SUCCESS', 'REFUND') AND bank_code NOT IN ('SHOPEEPAY','MNC','NOBU')
            THEN SUM(charge_amount) OVER(PARTITION BY 
                                                DATE(created_datetime), bank_code, transaction_status IN('SUCCESS','REFUND')
                                   ORDER BY created_datetime,id)
       ELSE NULL
       END AS cum_sum_daily,
       CASE 
       WHEN transaction_status IN ('SUCCESS', 'REFUND') AND bank_code NOT IN ('SHOPEEPAY','MNC','NOBU')
            THEN SUM(charge_amount) OVER(PARTITION BY 
                                                DATE_TRUNC(created_datetime,MONTH), bank_code, transaction_status IN('SUCCESS','REFUND')
                                   ORDER BY created_datetime,id)
       ELSE NULL
       END AS cum_sum_monthly
  FROM deduction_transaction
),
monthly_sum AS(
  SELECT DATE_TRUNC(last_updated_datetime,MONTH) date_month, vendor,
  MAX(cum_sum_monthly) monthly_tier
  FROM all_deduction
  GROUP BY 1,2 
),
final_deduction AS(
  SELECT a.*,b.monthly_tier
  FROM all_deduction a
  LEFT JOIN monthly_sum b
    ON DATE_TRUNC(last_updated_datetime,MONTH) = b.date_month 
    AND a.vendor = b.vendor
),
join_config AS (
SELECT deduction.*, 
       COALESCE(fix_cost, 0) AS fix_cost,
       COALESCE(variable_cost, 0) AS variable_cost,
       COALESCE(fix_cost, 0) + (COALESCE(variable_cost, 0) * charge_amount) AS deduction_cost
  FROM final_deduction AS deduction
  LEFT JOIN {{ source('sheet', 'sheet_deduction_cost_configuration') }} AS config
    ON UPPER(config.payment_method) LIKE CONCAT('%', UPPER(deduction.payment_method), '%')
   AND CASE
          WHEN UPPER(config.bank_code) IN('SHOPEEPAY','MNC','NOBU')
            THEN UPPER(config.vendor) LIKE CONCAT('%', UPPER(deduction.vendor), '%')
          WHEN UPPER(config.bank_code) IN('DANA','DANAMON')
            THEN deduction.cum_sum_monthly BETWEEN COALESCE(config.low_amount, 0) AND COALESCE(config.high_amount, 1E15)
                 AND UPPER(config.vendor) LIKE CONCAT('%', UPPER(deduction.vendor), '%')
          WHEN UPPER(config.bank_code) = 'BAGI'
            THEN  deduction.cum_sum_daily BETWEEN COALESCE(config.low_amount, 0) AND COALESCE(config.high_amount, 1E15)
                 AND UPPER(config.vendor) LIKE CONCAT('%', UPPER(deduction.vendor), '%')
        END
   AND DATE(deduction.last_updated_datetime) BETWEEN DATE(config.start_date) AND DATE(config.end_date)
   AND deduction.transaction_status IN ('SUCCESS', 'REFUND') 
),



final as (
    SELECT * EXCEPT(cum_sum_daily,cum_sum_monthly,monthly_tier)
    FROM join_config
)

select * from final
