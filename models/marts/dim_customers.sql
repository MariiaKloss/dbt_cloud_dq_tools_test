{{
    config(
        materialized='table'
    )
}}

with customer as (
    select *
    from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select * from {{ ref('stg_jaffle_shop_orders') }}
),

custom_orders as (
    select 
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders
    group by customer_id
),

final as (
    select
        customer.customer_id,
        customer.first_name,
        customer.last_name,
        custom_orders.first_order_date,
        custom_orders.most_recent_order_date,
        coalesce(custom_orders.number_of_orders, 0) as number_of_orders

    from customer

    left join custom_orders using (customer_id)
)

select * from final