SELECT 
  content.global_entity_id
    , content.vendor.vendor_id AS vendor_id
    , content.global_catalog_id AS global_catalog_id
    , content.timestamp AS updated_at_timestamp
    , content.active AS active_field_value
    , content.name AS product_name
    , att.attribute_id AS updated_field
    , att.name AS updated_value
    , content.active 
    , content.active 
    , metadata.source as source
FROM `fulfillment-dwh-production.curated_data_shared_data_stream.product_stream`
LEFT JOIN UNNEST(content.attributes) as att
where true
and date(timestamp) = '2023-02-20'
limit 1000
