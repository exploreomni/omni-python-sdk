CREATE SEMANTIC VIEW order_items

TABLES (
  distribution_centers AS public.distribution_centers
    PRIMARY KEY (distribution_centers.id),
  first_order_facts AS public.first_order_facts
    PRIMARY KEY (first_order_facts.user_id),
  inventory_items AS public.inventory_items
    PRIMARY KEY (inventory_items.id),
  order_items AS public.order_items
    PRIMARY KEY (order_items.id),
  products AS public.products
    PRIMARY KEY (products.id),
  users AS public.users
    PRIMARY KEY (users.id)
)

RELATIONSHIPS (
  order_items_users AS
    order_items (order_items.user_id) REFERENCES users,
  order_items_inventory_items AS
    order_items (order_items.inventory_item_id) REFERENCES inventory_items,
  inventory_items_products AS
    inventory_items (inventory_items.product_id) REFERENCES products,
  products_distribution_centers AS
    products (products.distribution_center_id) REFERENCES distribution_centers,
  order_items_first_order_facts AS
    order_items (order_items.user_id) REFERENCES first_order_facts
)

DIMENSIONS (
  distribution_centers.id AS distribution_centers.id,
  distribution_centers.latitude AS distribution_centers.latitude,
  distribution_centers.longitude AS distribution_centers.longitude,
  distribution_centers.name AS distribution_centers.name,
  first_order_facts.created_at_min AS first_order_facts.created_at_min,
  first_order_facts.user_id AS first_order_facts.user_id,
  inventory_items.cost AS inventory_items.cost,
  inventory_items.created_at AS inventory_items.created_at,
  inventory_items.id AS inventory_items.id,
  inventory_items.product_brand AS inventory_items.product_brand,
  inventory_items.product_category AS inventory_items.product_category,
  inventory_items.product_department AS inventory_items.product_department,
  inventory_items.product_distribution_center_id AS inventory_items.product_distribution_center_id,
  inventory_items.product_id AS inventory_items.product_id,
  inventory_items.product_name AS inventory_items.product_name,
  inventory_items.product_retail_price AS inventory_items.product_retail_price,
  inventory_items.product_sku AS inventory_items.product_sku,
  inventory_items.sold_at AS inventory_items.sold_at,
  order_items.created_at AS order_items.created_at,
  order_items.delivered_at AS order_items.delivered_at,
  order_items.id AS order_items.id,
  order_items.inventory_item_id AS order_items.inventory_item_id,
  order_items.margin AS order_items.sale_price - products.cost,
  order_items.order_id AS order_items.order_id,
  order_items.returned_at AS order_items.returned_at,
  order_items.sale_price AS order_items.sale_price,
  order_items.shipped_at AS order_items.shipped_at,
  order_items.status AS status,
  order_items.user_id AS order_items.user_id,
  products.brand AS products.brand,
  products.category AS products.category,
  products.cost AS products.cost,
  products.department AS products.department,
  products.distribution_center_id AS products.distribution_center_id,
  products.id AS products.id,
  products.name AS products.name,
  products.retail_price AS products.retail_price,
  products.sku AS products.sku,
  users.age AS users.age,
  users.city AS users.city,
  users.country AS country,
  users.created_at AS users.created_at,
  users.email AS users.email,
  users.first_name AS users.first_name,
  users.gender AS users.gender,
  users.id AS users.id,
  users.last_name AS users.last_name,
  users.latitude AS users.latitude,
  users.longitude AS users.longitude,
  users.state AS users.state,
  users.traffic_source AS users.traffic_source,
  users.zip AS users.zip
)

METRICS (
  distribution_centers.count AS COUNT(*),
  first_order_facts.count AS COUNT(*),
  inventory_items.count AS COUNT(*),
  order_items.calculation AS order_items.sale_price_sum / order_items.order_id_count_distinct,
  order_items.created_at_min AS MIN(order_items.created_at),
  order_items.margin_sum AS OMNI_SUM(order_items.margin),
  order_items.count AS COUNT(*),
  order_items.order_id_count_distinct AS COUNT(DISTINCT order_items.order_id),
  order_items.sale_price_sum AS OMNI_SUM(order_items.sale_price),
  products.cost_sum AS OMNI_SUM(products.cost),
  products.count AS COUNT(*),
  users.count AS COUNT(*)
)