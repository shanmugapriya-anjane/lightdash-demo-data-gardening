{{
  config(
    tags = ['not_lightdash']
  )
}}

SELECT ord.order_id,
  ord.order_date,
  ord.partner_id,
  prt.partner_name,
  prt.partner_commission,
  ord.currency,
  ord.basket_total::decimal AS basket_total,
  ord.profit::decimal AS profit,
  ord.referrer,
  ord.user_id,
  usr.email,
  usr.created_date AS user_created_date,
  usr.browser,
   replace(PARSE_JSON(shipping_address):city,'"', '') AS shipping_city,
    replace(PARSE_JSON(shipping_address):country,'"', '') AS shipping_country
  --shipping_address::jsonb->>'city' AS shipping_city,
  --shipping_address::jsonb->>'country' AS shipping_country
FROM {{ref('orders')}} ord
  LEFT JOIN {{ref('users')}} usr ON ord.user_id = usr.user_id
  LEFT JOIN {{ref('partners')}} prt ON ord.partner_id = prt.partner_id
ORDER BY CAST(order_id AS int) ASC