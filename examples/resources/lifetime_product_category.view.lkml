view: lifetime_product_category {
  dimension: ever_purchased_in_category {
    suggest_explore: products
    suggest_dimension: products.category
    sql: ${TABLE} ;;
  }
}
