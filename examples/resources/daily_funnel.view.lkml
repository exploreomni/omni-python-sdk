include: "order_items.explore"
include: "users.explore"

explore: daily_funnel {
  join: funnel_orders {relationship:one_to_one
    sql_on: ${funnel_orders.order_date} = ${daily_funnel.date} ;;}
  join: funnel_signups {relationship:one_to_one
    sql_on: ${funnel_signups.signup_date} = ${daily_funnel.date} ;;}
}


# Start with a Date table
view: daily_funnel {
  derived_table: {
    sql:
     SELECT * FROM (
       SELECT
           DATE_ADD('2010-01-01', INTERVAL num DAY) as d
       FROM UNNEST(GENERATE_ARRAY(1,10000)) num
     )
     WHERE {% condition daily_funnel.filter_date %}
        TIMESTAMP(d)
        {% endcondition %}
    ;;
  }
  filter: filter_date {type:date datatype:date}
  dimension: date {can_filter:no  sql: ${TABLE}.d ;;}
}

# Join orders by day
view: funnel_orders {
  derived_table: {
    explore_source: order_items {
      column: order_date {field:order_items.created_date}
      column: order_count {field:order_items.order_count}
      bind_filters: {from_field: daily_funnel.filter_date
        to_field: order_items.created_date }
    }
  }
  dimension: order_date {}
  dimension: order_count {type: number}
}

# Join user signups.
view: funnel_signups {
  derived_table: {
    explore_source: users {
      column: signup_date {field:users.created_date}
      column: signup_count {field:users.count}
      bind_filters: {from_field:daily_funnel.filter_date
        to_field:users.created_date}
    }
  }
  dimension: signup_date {}
  dimension: signup_count {type: number}
}
