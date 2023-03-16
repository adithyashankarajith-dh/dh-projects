CREATE or replace table `dh-darkstores-stg.dev_dmart._qcls_product_stream_subset_availability_updates_6months`
partition by update_date
as


with ps_subset as 
(
select 
  global_entity_id
  , vendor_id
  , global_catalog_id
  , product_name
  , product_id
  , updated_at_timestamp
  , updated_value as updated_by_client
  , active
  , active_field_value
  , LAG(active) OVER (
        PARTITION BY global_entity_id, vendor_id, global_catalog_id
        ORDER BY updated_at_timestamp ASC
      ) AS active_previous
    , LAG(updated_value) OVER (
      PARTITION BY global_entity_id, vendor_id, global_catalog_id
      ORDER BY updated_at_timestamp ASC
    ) AS source_previous
    , LAG(updated_at_timestamp) OVER (
      PARTITION BY global_entity_id, vendor_id, global_catalog_id
      ORDER BY updated_at_timestamp ASC
    ) AS updated_at_previous
from `dh-darkstores-stg.dev_dmart._qcls_product_stream_subset_pelican_oos_6months`
where true
and updated_field = 'updatedByClient'
GROUP BY 1,2,3,4,5,6,7,8,9
) 

select 
  global_entity_id
  , vendor_id
  , global_catalog_id
  , product_name
  , product_id
  , updated_at_timestamp as timestamp_valid_from
  , COALESCE(
        LAG(updated_at_timestamp) OVER (PARTITION BY global_entity_id, vendor_id, global_catalog_id ORDER BY updated_at_timestamp DESC)
        , '2030-01-01 00:00:00.000 UTC'
      ) AS timestamp_valid_until
  , updated_by_client
  , active
  , active_previous 
  , source_previous           -- currently takes in source of last update and not of last availability update, needs to be changed
  , updated_at_previous       -- currently takes in timestamp of last update and not of last availability update, needs to be changed
  , date(updated_at_timestamp) as update_date
from ps_subset
where true
and active_previous is null
or active_previous <> active
