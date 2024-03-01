{{
  config(
    materialized='table'
  )
}}

SELECT
    bsk.order_id AS order_id,
    bsk.basket_item_id AS basket_item_id,
    ord.partner_id AS partner_id,
    req.request_id AS request_id,
    usr.user_id,
    --bsk.ordered_product_skus AS sku,
    --prd.product_name AS product_name,
    --prd.price_currency AS currency,
    bsk.item_price AS item_price,
    bsk.basket_total::decimal AS basket_total,
    ord.order_date AS order_date,
    ord.profit::decimal AS profit,
    (bsk.item_price::decimal * prt.partner_commission::decimal) AS item_profit,
    
    prt.partner_name AS partner_name,
    prt.partner_commission AS partner_commission,

    -- User information
    usr.email AS user_email,
    usr.created_date AS user_created_date,
    usr.browser AS user_browser,
   

    -- Support request information
    
    req.request_date AS request_date,
    req.reason AS request_reason,
    req.feedback_rating AS feedback_rating,

    -- Metrics
    COUNT(DISTINCT bsk.basket_item_id) AS basket_item_count,
    SUM(bsk.item_price::decimal) AS total_item_price,
    AVG(bsk.item_price::decimal) AS avg_item_price,
    MAX(bsk.item_price::decimal) AS max_item_price,
    MIN(bsk.item_price::decimal) AS min_item_price,
    SUM(bsk.basket_total::decimal) AS total_basket_amount,
    AVG(bsk.basket_total::decimal) AS avg_basket_amount,
    MAX(bsk.basket_total::decimal) AS max_basket_amount,
    MIN(bsk.basket_total::decimal) AS min_basket_amount

FROM
    {{ref('dbt_baskets')}} bsk
--LEFT JOIN
   -- products prd ON bsk.ordered_product_skus::VARCHAR = prd.sku::VARCHAR
LEFT JOIN
   {{ref('dbt_orders')}} ord ON bsk.order_id = ord.order_id
LEFT JOIN
    partners prt ON ord.partner_id = prt.partner_id
LEFT JOIN
   dbt_users usr ON ord.user_id = usr.user_id
LEFT JOIN
   dbt_support_requests req ON req.order_id = ord.order_id
GROUP BY
    bsk.order_id,
    bsk.basket_item_id,
    ord.partner_id ,
    usr.user_id,
    --bsk.ordered_product_skus,
   -- prd.product_name,
   -- prd.price_currency,
    bsk.item_price,
    bsk.basket_total,
    ord.order_date,
    ord.profit,
    prt.partner_name,
    prt.partner_commission,
    usr.email,
    usr.created_date,
    usr.browser,
   -- (PARSE_JSON(shipping_address):city,
   -- (PARSE_JSON(shipping_address):country,
    req.request_id,
    req.request_date,
    req.reason,
    req.feedback_rating

ORDER BY
    bsk.basket_item_id::int ASC