include: "products.view"
include: "users.view"
include: "events.explore"

view: event_sessions {
  derived_table: {
    #persist_for: "2 hours"
    explore_source: events {
      column: session_id { field: events.session_id }
      column: event_types { field: events.event_types }
      column: session_time { field: events.minimum_time }
      column: session_end_time {field: events.max_time}
      column: ip_addresses {field: events.ip_addresses }
      column: user_id {field: events.first_user_id }
      column: product_ids_visited {field: events.product_ids_visited}
      derived_column: session_sequence {
        sql: ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_time) ;;
      }
    }
  }
  dimension: session_id {primary_key:yes}
  dimension: event_types {}
  dimension_group: session {type:time  sql: ${TABLE}.session_time ;;}
  dimension: session_end_time {hidden:yes}
  dimension: ip_addresses {}
  dimension: user_id {}
  dimension: product_ids_visited {}
  dimension: session_sequence {type:number}
  dimension: session_length {type:number
    sql: TIMESTAMP_DIFF(${session_end_time},${session_raw}, SECOND) ;;}
  dimension: session_length_tiered {type:tier  tiers: [0,60,120]  sql: ${session_length} ;;}

  measure: count_sessions {type:count  drill_fields:[session*]}
  measure: count_sessions_with_cart {type:count  drill_fields:[session*]
    filters: {field:event_types  value:"%Cart%"}}
  measure: count_sessions_with_purchases {type:count  drill_fields: [session*]
    filters: {field:event_types value:"%Purchase%"}}

  set: session{ fields:[session_time, session_id, user_id, event_types]}
}

view: id {
  dimension: id {sql: ${TABLE} ;;}
}
