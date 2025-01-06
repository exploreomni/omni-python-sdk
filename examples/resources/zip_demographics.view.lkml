include: "/acs/geo.explore"

explore: zip_demographics {}
view: zip_demographics {
  derived_table: {
    persist_for: "24 hours"
    explore_source: geo {
      column: zip { field: tract_zcta_map.ZCTA5 }
      column: median_home_value { field: median_value.weighted_median_value }
      column: probability_hispanic { field: hispanic.percentage_selected }
      column: total_population { field: population.total_base}
    }
  }
  dimension: zip {
    map_layer_name: us_zipcode_tabulation_areas
    #hidden: yes
    primary_key: yes
  }
  dimension: median_home_value {
    type: number
    #hidden: yes
  }
  dimension: probability_hispanic {
    type: number
    #hidden: yes
    value_format_name: percent_2
    sql: ${TABLE}.probability_hispanic ;;
  }
  measure: total_population {
    type: sum
    sql: ${TABLE}.total_population ;;
  }
}
