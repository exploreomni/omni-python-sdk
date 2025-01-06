include: "users.explore.lkml"

explore: filtered_lookml_dt {}

view: filtered_lookml_dt {
  derived_table: {
    explore_source: users {
      column: age {field: users.age}
      column: people {field: users.count}
      bind_filters: {
        to_field: users.created_date
        from_field: filtered_lookml_dt.filter_date
      }
    }
  }

  filter: filter_date {
    type: date
  }
  dimension: age {type: number}
  dimension: people {type: number}
}
