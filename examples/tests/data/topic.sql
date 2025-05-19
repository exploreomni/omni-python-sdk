CREATE SEMANTIC VIEW order_items

TABLES (
  order_items AS public.order_items
    PRIMARY KEY (order_items.id),
  users AS public.users
    PRIMARY KEY (users.id)
)

RELATIONSHIPS (
  order_items_users AS
    order_items (order_items.user_id) REFERENCES users
)

DIMENSIONS (
  created_at AS order_items.created_at,
  delivered_at AS order_items.delivered_at,
  id AS order_items.id,
  inventory_item_id AS order_items.inventory_item_id,
  margin AS order_items.sale_price - products.cost,
  order_id AS order_items.order_id,
  returned_at AS order_items.returned_at,
  sale_price AS order_items.sale_price,
  shipped_at AS order_items.shipped_at,
  status AS status,
  user_id AS order_items.user_id,
  age AS users.age,
  city AS users.city,
  country AS country,
  created_at AS users.created_at,
  email AS users.email,
  first_name AS users.first_name,
  gender AS users.gender,
  id AS users.id,
  last_name AS users.last_name,
  latitude AS users.latitude,
  longitude AS users.longitude,
  state AS users.state,
  traffic_source AS users.traffic_source,
  zip AS users.zip
)

METRICS (
  calculation AS order_items.sale_price_sum / order_items.order_id_count_distinct,
  created_at_min AS MIN(order_items.created_at),
  margin_sum AS OMNI_SUM(order_items.margin),
  count AS COUNT(*),
  order_id_count_distinct AS COUNT(DISTINCT order_items.order_id),
  sale_price_sum AS OMNI_SUM(order_items.sale_price),
  count AS COUNT(*)
)