view: products {
  sql_table_name: thelook_web_analytics.products ;;

  dimension: id {primary_key:yes  type:number  sql: ${TABLE}.id ;;}
  dimension: brand {sql: ${TABLE}.brand ;;}
  dimension: category {sql: ${TABLE}.category ;;}
  dimension: cost {type:number  sql: ${TABLE}.cost ;;}
  dimension: department {sql: ${TABLE}.department ;;}
  dimension: distribution_center_id {type:number  sql: ${TABLE}.distribution_center_id ;;}
  dimension: name {sql: ${TABLE}.name ;;}
  dimension: retail_price {type:number  sql: ${TABLE}.retail_price ;;}
  dimension: sku {sql: ${TABLE}.sku ;;}

  measure: brand_list {type:list  list_field:brand}
  measure: category_list {type:list  list_field:category}
  measure: count {type:count
    drill_fields: [id, name, brand,category, inventory_items.count, order_items.count ]}
}
