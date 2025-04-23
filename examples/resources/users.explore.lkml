include: "user_joins.explore"
include: "users.view"

explore: users {
  view_name: users
  extends: [user_joins]
}
