include: "events.explore"

explore: user_events1 {hidden: yes}
view: user_events1 {
  derived_table: {
    explore_source: events {
      column: user_id { field: events.user_id}
      column: created_time {field: events.created_time}
      column: traffic_source {field: events.traffic_source}
      derived_column: event_sequence {
        sql: ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_time);;
      }
    }
  }
  dimension: user_id {}
  dimension: traffic_source {}
  dimension: event_sequence {type: number}
}

explore: user_event_attribution {hidden: no}
view: user_event_attribution {
  derived_table: {
    persist_for: "2 hours"
    explore_source: user_events1 {
      column: user_id {field: user_events1.user_id}
      column: traffic_source {field: user_events1.traffic_source}
      filters: {
        field: user_events1.event_sequence
        value: "1"
      }
    }
  }
  dimension: user_id {hidden: yes}
  dimension: traffic_source {}
}
