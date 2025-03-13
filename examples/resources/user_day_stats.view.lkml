include: "users.view"
include: "user_joins.explore"

explore: user_day_stats {
  extends: [user_joins]
  view_name: events
  from: user_day_events
  join: orders {from:user_day_orders relationship:one_to_one
    sql_on: ${orders.user_id} = ${events.user_id}
        AND ${orders.created_date} = ${events.created_date};;}
  join: users {relationship:many_to_one
    sql_on:  ${users.id} = COALESCE(${events.user_id}, ${orders.user_id}) ;;}
}

include: "events.explore"
include: "order_items.explore"

view: user_day_events {
  derived_table: {
    explore_source: events {
      column: user_id {field:events.user_id}
      column: events {field:events.count}
      column: created_date {field:events.created_date}
      filters: {field:events.user_id  value:"NOT NULL"}
    }
  }
  dimension: user_id {}
  dimension: created_date {}
  measure: event_count {type:sum  sql: ${TABLE}.events ;;}
}


view: user_day_orders {
  derived_table: {
    explore_source: order_items {
      column: user_id { field: order_items.user_id }
      column: created_date { field: order_items.created_date }
      column: revenue { field: order_items.total_revenue }
      column: orders { field: order_items.order_count }
    }
  }
  dimension: created_date {}
  dimension: user_id {}
  measure: total_revenue {type:sum  sql: ${TABLE}.revenue ;;}
  measure: order_count {type:sum  sql: ${TABLE}.orders ;;}
}
