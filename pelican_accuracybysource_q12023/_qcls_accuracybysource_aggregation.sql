select 
-- global_entity_id, 
  date_trunc(date(timestamp_valid_from), month) as month_start
  , b.management_entity
  , a.global_entity_id
  , updated_by_client
  , basketsize_buckets
  -- , orders_perday_buckets
  , count(*) as count_updates
  , coalesce(sum(case when active is true then 1 end),0) as count_active_updates
  , coalesce(sum(case when product_status IN ('NOT_FOUND', 'REPLACED') and active is true then 1 end),0) as count_incorrect_updates
  , coalesce(count(distinct case when product_status IN ('NOT_FOUND', 'REPLACED') and active is true then order_id end),0) as count_incorrect_orders
  , coalesce(sum(case when product_status IN ('NOT_FOUND', 'REPLACED') and active is true then quantity end),0) as count_incorrect_quantity
  , coalesce(sum(case when product_status IN ('NOT_FOUND', 'REPLACED') and active is true then quantity * unit_price end),0) as sum_incorrect_gmv
  , coalesce(sum(case when product_status IN ('IN_CART') and active is true then 1 end),0) as count_correct_updates
from `dh-darkstores-stg.local_shops_analytics.pelican_oos_availability_sourceaccuracy_basetemp_sku_6months` as a 
left join `fulfillment-dwh-production.cl_dmart.sources` as b
on a.global_entity_id = b.global_entity_id
left join `dh-darkstores-stg.dev_dmart._qcls_pelican_orders_avgbasketsize` as c
on a.global_entity_id = c.global_entity_id
and a.vendor_id_corrected = c.platform_vendor_id_wi
inner join `dh-darkstores-stg.local_shops_analytics.pelican_oos_vendor_tiering_rawdata` as d
on a.global_entity_id = d.global_entity_id
and a.vendor_id_corrected = d.platform_vendor_id_wi
where true
and placed_at is not null
and (TIMESTAMP_DIFF(placed_at, timestamp_valid_from, HOUR) <= time_difference)
and active_previous is not null
and order_rank = 1
and length(updated_by_client) <=25
group by 1,2,3,4,5
