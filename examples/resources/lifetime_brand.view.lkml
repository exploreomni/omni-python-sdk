view: lifetime_brand {
  dimension: ever_purchased_brand {
    suggest_explore: products
    suggest_dimension: products.brand
    sql: ${TABLE} ;;
  }
}
