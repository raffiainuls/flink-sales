import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_fact_sales(with_clause: str, table: str):
    return f"""
    CREATE TABLE {table} (
        id INT,
        product_id INT, 
        customer_id INT, 
        branch_id INT, 
        quantity INT, 
        payment_method INT, 
        order_date TIMESTAMP(3),
        order_status INT,
        payment_status FLOAT,
        shipping_status FLOAT,
        is_online_transaction BOOLEAN, 
        delivery_fee INT,
        is_free_delivery_fee STRING,
        created_at TIMESTAMP(3),
        modified_at TIMESTAMP(3),
        product_name STRING,
        product_category STRING,
        sub_category_product STRING,
        price BIGINT,
        disc INT,
        disc_name STRING, 
        amount BIGINT,
        PRIMARY KEY (id) NOT ENFORCED
    )  {with_clause.strip()}
    """

def kafka_sink_config():
    config = """
        WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'fact_sales',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'key.format' = 'json',
            'value.format' = 'json'
        )
        """
    return config

def postgres_sink_config():
    config = """
        WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://host.docker.internal:5432/yourdb',
        'table-name' = 'fact_sales',
        'username' = 'postgres',
        'password' = 'postgres'
    )
        """
    return config

def clickhouse_sink_config():
    config = """
    WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://host.docker.internal:8123/default',
    'table-name' = 'fact_sales',
    'driver' = 'ru.yandex.clickhouse.ClickHouseDriver',
    'username' = 'default',
    'password' = ''
    )
        """
    return config


def insert_into_fact_sales(table_env, table:str):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql(f"""
    INSERT INTO {table}
    SELECT
        ts.payload.id AS id,
        ts.payload.product_id AS product_id,
        ts.payload.customer_id,
        ts.payload.branch_id,
        ts.payload.quantity,
        ts.payload.payment_method,
        CAST(ts.payload.order_date AS TIMESTAMP(3)) AS order_date,
        ts.payload.order_status,
        ts.payload.payment_status,
        ts.payload.shipping_status,
        ts.payload.is_online_transaction,
        ts.payload.delivery_fee,
        ts.payload.is_free_delivery_fee,
        CAST(ts.payload.created_at AS TIMESTAMP(3)) AS created_at,
        CAST(ts.payload.modified_at AS TIMESTAMP(3)) AS modified_at,
        tp.payload.product_name,
        tp.payload.category AS product_category,
        tp.payload.sub_category AS sub_category_product,
        CAST(tp.payload.price AS BIGINT) AS price,
        tps.payload.disc,
        tps.payload.event_name AS disc_name,
        CAST(
            CASE
                WHEN tps.payload.disc IS NOT NULL THEN
                    (tp.payload.price * ts.payload.quantity) - (tp.payload.price * ts.payload.quantity) * COALESCE(tps.payload.disc, 0) / 100
                ELSE tp.payload.price * ts.payload.quantity
            END AS BIGINT
        ) AS amount
    FROM tbl_sales ts
    LEFT JOIN tbl_product tp ON tp.payload.id = ts.payload.product_id
    LEFT JOIN tbl_promotions tps ON TO_DATE(ts.payload.order_date) = TO_DATE(tps.payload.`time`)
    WHERE ts.payload.order_status = 2
        AND ts.payload.payment_status = 2
        AND (ts.payload.shipping_status = 2 OR ts.payload.shipping_status IS NULL)
    """)
    logger.info("✅ Data inserted into sink table.")