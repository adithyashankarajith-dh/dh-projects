CREATE OR REPLACE TABLE `dh-darkstores-stg.dev_dmart._qcls_product_stream_subset_pelican_oos_6months`
AS 

SELECT
    metadata.source
    , content.global_entity_id
    , content.vendor.vendor_id AS vendor_id
    , content.global_catalog_id AS global_catalog_id
    , content.timestamp AS updated_at_timestamp
    , content.active AS active_field_value
    , content.name AS product_name
    , content.product_id
    , h.attribute_id AS updated_field
    , h.name AS updated_value
    , content.active 
  FROM `fulfillment-dwh-production.curated_data_shared_data_stream.product_stream`
  LEFT JOIN UNNEST(content.attributes) AS h
  WHERE TRUE
    AND metadata.source = 'CATALOG'
    AND h.attribute_id IN (
      'salesBuffer'
      , 'originalPrice'
      , 'vatRate'
      , 'updatedByClient'
      , 'sku'
    )
    and date(timestamp) between '2022-08-01' and  '2023-01-31'
