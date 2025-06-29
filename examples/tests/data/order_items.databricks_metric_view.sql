CREATE VIEW `order_items`
(`distribution_centers.id`, `distribution_centers.latitude`, `distribution_centers.longitude`, `distribution_centers.name`, `first_order_facts.created_at_min`, `first_order_facts.user_id`, `inventory_items.cost`, `inventory_items.created_at`, `inventory_items.id`, `inventory_items.product_brand`, `inventory_items.product_category`, `inventory_items.product_department`, `inventory_items.product_distribution_center_id`, `inventory_items.product_id`, `inventory_items.product_name`, `inventory_items.product_retail_price`, `inventory_items.product_sku`, `inventory_items.sold_at`, `order_items.created_at`, `order_items.delivered_at`, `order_items.id`, `order_items.inventory_item_id`, `order_items.margin`, `order_items.order_id`, `order_items.returned_at`, `order_items.sale_price`, `order_items.shipped_at`, `order_items.status`, `order_items.user_id`, `products.brand`, `products.category`, `products.cost`, `products.department`, `products.distribution_center_id`, `products.id`, `products.name`, `products.retail_price`, `products.sku`, `users.age`, `users.city`, `users.country`, `users.created_at`, `users.email`, `users.first_name`, `users.gender`, `users.id`, `users.last_name`, `users.latitude`, `users.longitude`, `users.state`, `users.traffic_source`, `users.zip`, `distribution_centers.count`, `first_order_facts.count`, `inventory_items.count`, `order_items.calculation`, `order_items.created_at_min`, `order_items.margin_sum`, `order_items.count`, `order_items.order_id_count_distinct`, `order_items.sale_price_sum`, `products.cost_sum`, `products.count`, `users.count`)
WITH METRICS
LANGUAGE YAML

AS $$
dimensions:
- expr: distribution_centers.id
  name: distribution_centers.id
- expr: distribution_centers.latitude
  name: distribution_centers.latitude
- expr: distribution_centers.longitude
  name: distribution_centers.longitude
- expr: distribution_centers.name
  name: distribution_centers.name
- expr: first_order_facts.created_at_min
  name: first_order_facts.created_at_min
- expr: first_order_facts.user_id
  name: first_order_facts.user_id
- expr: inventory_items.cost
  name: inventory_items.cost
- expr: inventory_items.created_at
  name: inventory_items.created_at
- expr: inventory_items.id
  name: inventory_items.id
- expr: inventory_items.product_brand
  name: inventory_items.product_brand
- expr: inventory_items.product_category
  name: inventory_items.product_category
- expr: inventory_items.product_department
  name: inventory_items.product_department
- expr: inventory_items.product_distribution_center_id
  name: inventory_items.product_distribution_center_id
- expr: inventory_items.product_id
  name: inventory_items.product_id
- expr: inventory_items.product_name
  name: inventory_items.product_name
- expr: inventory_items.product_retail_price
  name: inventory_items.product_retail_price
- expr: inventory_items.product_sku
  name: inventory_items.product_sku
- expr: inventory_items.sold_at
  name: inventory_items.sold_at
- expr: order_items.created_at
  name: order_items.created_at
- expr: order_items.delivered_at
  name: order_items.delivered_at
- expr: order_items.id
  name: order_items.id
- expr: order_items.inventory_item_id
  name: order_items.inventory_item_id
- expr: order_items.sale_price - products.cost
  name: order_items.margin
- expr: order_items.order_id
  name: order_items.order_id
- expr: order_items.returned_at
  name: order_items.returned_at
- expr: order_items.sale_price
  name: order_items.sale_price
- expr: order_items.shipped_at
  name: order_items.shipped_at
- expr: status
  name: order_items.status
- expr: order_items.user_id
  name: order_items.user_id
- expr: products.brand
  name: products.brand
- expr: products.category
  name: products.category
- expr: products.cost
  name: products.cost
- expr: products.department
  name: products.department
- expr: products.distribution_center_id
  name: products.distribution_center_id
- expr: products.id
  name: products.id
- expr: products.name
  name: products.name
- expr: products.retail_price
  name: products.retail_price
- expr: products.sku
  name: products.sku
- expr: users.age
  name: users.age
- expr: users.city
  name: users.city
- expr: country
  name: users.country
- expr: users.created_at
  name: users.created_at
- expr: users.email
  name: users.email
- expr: users.first_name
  name: users.first_name
- expr: users.gender
  name: users.gender
- expr: users.id
  name: users.id
- expr: users.last_name
  name: users.last_name
- expr: users.latitude
  name: users.latitude
- expr: users.longitude
  name: users.longitude
- expr: users.state
  name: users.state
- expr: users.traffic_source
  name: users.traffic_source
- expr: users.zip
  name: users.zip
joins:
- name: users
  'on': order_items.user_id = users.id
  source: public.users
- name: inventory_items
  'on': order_items.inventory_item_id = inventory_items.id
  source: public.inventory_items
- name: products
  'on': inventory_items.product_id = products.id
  source: public.products
- name: distribution_centers
  'on': products.distribution_center_id = distribution_centers.id
  source: public.distribution_centers
- name: first_order_facts
  'on': order_items.user_id = first_order_facts.user_id
  source: public.first_order_facts
measures:
- expr: COUNT(*)
  name: distribution_centers.count
- expr: COUNT(*)
  name: first_order_facts.count
- expr: COUNT(*)
  name: inventory_items.count
- expr: Measure(order_items.sale_price_sum) / Measure(order_items.order_id_count_distinct)
  name: order_items.calculation
- expr: MIN(order_items.created_at)
  name: order_items.created_at_min
- expr: SUM(order_items.margin)
  name: order_items.margin_sum
- expr: COUNT(*)
  name: order_items.count
- expr: COUNT(DISTINCT order_items.order_id)
  name: order_items.order_id_count_distinct
- expr: SUM(order_items.sale_price)
  name: order_items.sale_price_sum
- expr: SUM(products.cost)
  name: products.cost_sum
- expr: COUNT(*)
  name: products.count
- expr: COUNT(*)
  name: users.count
source: public.order_items
version: '0.1'

$$