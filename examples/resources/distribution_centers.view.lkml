view: distribution_centers {
  sql_table_name: thelook_web_analytics.distribution_centers ;;

  dimension: id {primary_key:yes  type:number}
  dimension: latitude {type:number}
  dimension: longitude {type:number}
  dimension: name {}
  measure: count {type:count  drill_fields:[id, name, products.count]}
}
