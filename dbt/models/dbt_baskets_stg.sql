{{
  config(
    materialized='table'
  )
}}
select * from {{ref('baskets')}}