CREATE OR REPLACE TABLE `dh-darkstores-stg.local_shops_analytics.pelican_oos_availability_sourceaccuracy_basetemp_sku_6months`
AS


with 
ls_vendor_list AS 
(
  SELECT 
    global_entity_id
    , vendor_code
    -- , max(is_pelican_vendor) as is_pelican_vendor
  FROM `fulfillment-dwh-production.cl_dmart._qcpa_ml_qc_vendor_logs`
  WHERE TRUE
    AND is_local_shops_vendor IS TRUE
    AND is_pelican_vendor IS TRUE
  GROUP BY 1,2
),
catalog_vendor_ids AS (
  SELECT
    a.id AS vendor_id
    , a.country_code
    , b.remote_vendor_id
    , b.additional_remote_id
    , b.platform_id
    , c.global_entity_id
    , d.management_entity
  FROM `fulfillment-dwh-production.dl_dmart.catalog_vendor` AS a 
  LEFT JOIN `fulfillment-dwh-production.dl_dmart.catalog_platform_vendor` AS b
    ON a.id = b.vendor_id   
    AND a.country_code = b.country_code
  AND a.region = b.region
  LEFT JOIN `fulfillment-dwh-production.dl_dmart.catalog_platform` AS c
    ON b.platform_id = c.id
    AND a.country_code = c.country_code
    LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS d
    on c.global_entity_id = d.global_entity_id
    group by 1,2,3,4,5,6,7
),

corrected_vendor_id_updates as 
(
select 
  a.*
  , IF(LOWER(b.management_entity) IN ('talabat', 'hungerstation'),b.additional_remote_id, a.vendor_id) as vendor_id_corrected
from `dh-darkstores-stg.dev_dmart._qcls_product_stream_subset_availability_updates_6months` as a 
left join catalog_vendor_ids as b
on a.global_entity_id = b.global_entity_id
and a.vendor_id = b.remote_vendor_id
),

ls_updates as 
(
select a.* from 
corrected_vendor_id_updates as a 
inner join ls_vendor_list as b
on a.global_entity_id = b.global_entity_id
and a.vendor_id_corrected = b.vendor_code
)




SELECT 
  a.global_entity_id
  , a.global_catalog_id
  , a.vendor_id
  , a.vendor_id_corrected
  , a.active
  , a.updated_by_client
  , a.active_previous
  , a.source_previous
  , a.timestamp_valid_from
  , a.timestamp_valid_until
  , a.product_name
  , b.id as order_id
  , b.product_status
  , b.order_status
  , b.placed_at
  , b.unit_price
  , b.quantity
  , DENSE_RANK() OVER(PARTITION BY a.global_entity_id, a.global_catalog_id, a.vendor_id_corrected,a.timestamp_valid_from ORDER BY b.placed_at) as order_rank
FROM ls_updates AS a
LEFT JOIN `dh-darkstores-stg.dev_dmart._qcls_pelican_orders_productlevel_subset` AS b
ON a.global_entity_id = b.global_entity_id
AND a.product_id = b.product_id
AND a.vendor_id_corrected = b.platform_vendor_id_wi
AND b.placed_at > a.timestamp_valid_from
AND b.placed_at between a.timestamp_valid_from and a.timestamp_valid_until
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
