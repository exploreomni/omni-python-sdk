# All the facts we've built around the users
explore: user_joins {
  extension: required
  join: user_order_facts {relationship: many_to_one
    sql_on: ${user_order_facts.user_id} = ${users.id} ;;}
  join: user_event_attribution {view_label:"Users"  relationship:many_to_one
    sql_on: ${user_event_attribution.user_id} = ${users.id} ;;}
  join: lifetime_brand {view_label:"Users" relationship:one_to_many
    sql: LEFT JOIN UNNEST(SPLIT(${user_order_facts.lifetime_brands},'|RECORD|')) as lifetime_brand ;;}
  join: lifetime_product_category { view_label:"Users"  relationship:one_to_many
    sql: LEFT JOIN UNNEST(SPLIT(${user_order_facts.lifetime_product_categories},'|RECORD|')) as lifetime_product_category ;;}
  join: zip_demographics {relationship:many_to_one
    sql_on: ${users.zip} =  ${zip_demographics.zip};;}
  join: probability_male {relationship:many_to_one
    sql_on: ${users.first_name} = UPPER(${probability_male.name}) ;;}
}
