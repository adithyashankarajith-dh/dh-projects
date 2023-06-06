select 
  date_trunc(date(timestamp_valid_from), month) as month_start
  , b.management_entity
  , a.global_entity_id
  , active_previous
  , source_previous_availability
  , active
  , updated_by_client
  , count(distinct concat(a.global_entity_id, vendor_id_corrected, global_catalog_id, timestamp_valid_from)) as count_updates
  , count(distinct case when pricing_type != 'KG' then order_id end) AS count_orders
  , count(distinct case when product_status IN ('NOT_FOUND', 'REPLACED') and pricing_type != 'KG' then order_id end) as oos_orders
  , sum(case when pricing_type != 'KG' then quantity end) AS qty_ordered
  , sum(case when pricing_type != 'KG' then coalesce(pickup_quantity,0) end ) AS qty_pickedup
from `dh-darkstores-stg.local_shops_analytics.pelican_oos_availability_all_sourceaccuracy_basetemp_sku_mar22tomay2` as a 
left join `fulfillment-dwh-production.cl_dmart.sources` as b
on a.global_entity_id = b.global_entity_id
where true
and date(timestamp_valid_from) between '2022-03-01' and '2023-04-30'
and active_previous is not null
and (product_status NOT IN ('NOT_PROCESSED') or product_status IS NULL)
group by 1,2,3,4,5,6,7
