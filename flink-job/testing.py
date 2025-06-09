import logging
import sys
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

try:
    logger.info("🚀 Starting PyFlink environment setup...")

    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)
    logger.info("✅ TableEnvironment created.")

    # 1. Create region source table
    logger.info("📦 Creating Kafka source table: tbl_product...")
    table_env.execute_sql("""
    CREATE TABLE tbl_product (
    payload ROW<
        id INT,
        product_name STRING,
        category STRING,
        sub_category STRING,
        price INT,
        profit INT,
        stock INT,
        created_time STRING,
        modified_time FLOAT
    >
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'table.public.tbl_product',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        );
    """)
    logger.info("✅ Kafka source table 'tbl_product' created.")


    # 2. Create region source table
    logger.info("📦 Creating Kafka source table: tbl_promotions...")
    table_env.execute_sql("""
    CREATE TABLE tbl_promotions (
        payload ROW<
            id INT,
            event_name STRING,
            disc INT,
            `time` STRING,
            created_at STRING
        >
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'table.public.tbl_promotions',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    );
    """)
    logger.info("✅ Kafka source table 'tbl_promotions' created.")

    # 2. Create region source table
    logger.info("📦 Creating Kafka source table: tbl_sales...")
    table_env.execute_sql("""
    CREATE TABLE tbl_sales (
        payload ROW<
            id INT,
            product_id INT,
            customer_id INT,
            branch_id INT,
            quantity INT,
            payment_method INT,
            order_date STRING,
            order_status INT,
            payment_status FLOAT,
            shipping_status FLOAT,
            is_online_transaction BOOLEAN,
            delivery_fee INT,
            is_free_delivery_fee STRING,
            created_at STRING,
            modified_at STRING
        >
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'table.public.tbl_sales',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    );
    """)
    logger.info("✅ Kafka source table 'tbl_sales' created.")

    # 3. Create print sink table
    logger.info("🖨️ Creating print sink table: joined_output...")
    table_env.execute_sql("""
    CREATE TABLE fact_sales (
        id INT,
        product_id INT, 
        customer_id INT, 
        branch_id INT, 
        quantity INT, 
        payment_method INT, 
        order_date timestamp,
        order_status int,
        payment_status FLOAT,
        shipping_status FLOAT,
        is_online_transactions BOOLEAN, 
        delivery_fee int,
        is_free_delivery_fee string,
        created_at timestamp,
        modified_at timestamp,
        product_name string,
        product_category string,
        sub_category_product string,
        price bigint,
        disc int,
        disc_name string, 
        amount bigint
    ) WITH (
        'connector' = 'print'
    )
    """)
    logger.info("✅ Print sink table 'fact_sales' created.")

    # 4. Join region and territory, and insert into print sink
    logger.info("🔗 Joining and submitting job...")
    table_env.execute_sql("""
    INSERT INTO fact_sales
        select
        ts.payload.id as id,
        ts.payload.product_id as product_id,
        ts.payload.customer_id,
        ts.payload.branch_id,
        ts.payload.quantity,
        ts.payload.payment_method,
        cast(ts.payload.order_date as timestamp),
        ts.payload.order_status,
        ts.payload.payment_status,
        ts.payload.shipping_status,
        ts.payload.is_online_transaction,
        ts.payload.delivery_fee,
        ts.payload.is_free_delivery_fee,
        cast(ts.payload.created_at as timestamp),
        cast(ts.payload.modified_at as timestamp),
        tp.payload.product_name,
        tp.payload.category AS product_category,
        tp.payload.sub_category AS sub_category_product,
        tp.payload.price,
        tps.payload.disc,
        tps.payload.event_name AS disc_name,
        CAST(
            CASE
                WHEN tps.payload.disc IS NOT NULL
                THEN (tp.payload.price * ts.payload.quantity) - (tp.payload.price * ts.payload.quantity) * COALESCE(tps.payload.disc, 0) / 100
                ELSE tp.payload.price * ts.payload.quantity
            END AS BIGINT
        ) AS amount
    FROM tbl_sales ts
    LEFT JOIN tbl_product tp
        ON tp.payload.id = ts.payload.product_id
    LEFT JOIN tbl_promotions tps
        ON to_date(ts.payload.order_date) = to_date(tps.payload.`time`)
    WHERE ts.payload.order_status = 2
        AND ts.payload.payment_status = 2
        AND (ts.payload.shipping_status = 2 OR ts.payload.shipping_status IS NULL) 
    """)
    logger.info("✅ Job submitted. Watch the output for joined results!")

except Exception as e:
    logger.error("❌ An error occurred!")
    traceback.print_exc(file=sys.stdout)
