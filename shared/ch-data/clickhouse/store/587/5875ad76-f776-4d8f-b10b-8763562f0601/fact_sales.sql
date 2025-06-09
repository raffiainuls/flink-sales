ATTACH TABLE _ UUID '0a225722-557d-4883-bd04-619344e2ea71'
(
    `id` Int32,
    `product_id` Int32,
    `customer_id` Int32,
    `branch_id` Int32,
    `quantity` Int32,
    `payment_method` Int32,
    `order_date` DateTime64(3),
    `order_status` Int32,
    `payment_status` Float32,
    `shipping_status` Float32,
    `is_online_transactions` UInt8,
    `delivery_fee` Int32,
    `is_free_delivery_fee` String,
    `created_at` DateTime64(3),
    `modified_at` DateTime64(3),
    `product_name` String,
    `product_category` String,
    `sub_category_product` String,
    `price` Int64,
    `disc` Int32,
    `disc_name` String,
    `amount` Int64
)
ENGINE = ReplacingMergeTree(modified_at)
ORDER BY id
SETTINGS index_granularity = 8192
