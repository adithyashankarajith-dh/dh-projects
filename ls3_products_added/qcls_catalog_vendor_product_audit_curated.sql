CREATE OR REPLACE TABLE `dh-darkstores-stg.dev_dmart._qcls_ls3_products_added_cvpa_afterfix`
AS 


select 
  a.id
  , a.vendor_id
  , a.chain_product_id
  , a.available
  , a.status
  , a.country_code
  , a.region
  , a.creation_date
  , b.created_by
  , p.global_entity_id
  , (CASE WHEN pv.additional_remote_id = "" THEN pv.remote_vendor_id ELSE pv.additional_remote_id END) AS platform_vendor_id
  , c.sku
from fulfillment-dwh-production.dl_dmart.catalog_vendor_product as a 
LEFT JOIN fulfillment-dwh-production.dl_dmart.catalog_vendor_product_audit as b
on a.id = b.vendor_product_id
and a.country_code = b.country_code
JOIN `fulfillment-dwh-production.dl_dmart.catalog_platform_vendor` pv 
ON pv.vendor_id = a.vendor_id 
AND pv.country_code = a.country_code
JOIN `fulfillment-dwh-production.dl_dmart.catalog_platform` p 
      ON p.id = pv.platform_id 
      AND p.country_code = pv.country_code
JOIN `fulfillment-dwh-production.dl_dmart.catalog_chain_product` c
      ON a.chain_product_id = c.id 
      AND a.country_code = c.country_code
where true
-- and b.created_by = 'vendor_portal'
-- and a.status = 'ACTIVE'
-- and sku = '07793147570286'
-- and (additional_remote_id = '40929' or remote_vendor_id = '40929')
-- and global_entity_id = 'PY_AR'
-- and date(a.creation_date) = '2023-03-24'
