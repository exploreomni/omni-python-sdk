include: "users.view"
include: "events.view"
include: "user_joins.explore"

explore: events {
  extends: [user_joins]
  join: users {relationship:many_to_one  sql_on: ${events.user_id} = ${users.id} ;;}
}
