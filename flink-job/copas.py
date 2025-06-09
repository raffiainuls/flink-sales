import logging
import sys
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_table_env():
    logger.info("🚀 Starting PyFlink environment setup...")
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)
    logger.info("✅ TableEnvironment created.")
    return table_env


def create_tables(table_env, kafka_config):
    logger.info("📦 Creating Kafka source table: fact_sales...")
    table_env.execute_sql(f"""
    CREATE TABLE fact_sales (
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
        is_online_transactions BOOLEAN, 
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
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = '{kafka_config["source_topic"]}',
        'properties.bootstrap.servers' = '{kafka_config["bootstrap_servers"]}',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """)
    logger.info("✅ Kafka source table created.")

    logger.info("🖨️ Creating Kafka sink table: sum_transactions...")
    table_env.execute_sql(f"""
    CREATE TABLE sum_transactions (
        type STRING,
        sales_id INT,
        branch_id INT,
        employee_id INT,
        description STRING,
        `date` TIMESTAMP,
        amount BIGINT,
        PRIMARY KEY (type, sales_id, branch_id, employee_id) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = '{kafka_config["sink_topic"]}',
        'properties.bootstrap.servers' = '{kafka_config["bootstrap_servers"]}',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """)
    logger.info("✅ Kafka sink table created.")


def insert_into_sink(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO sum_transactions
    WITH 
    iofs AS (
        SELECT 
            'income' AS type,
            id AS sales_id,
            branch_id,
            COALESCE(CAST(NULL AS INT), 0) AS employee_id,
            CONCAT('Penjualan ', product_name, ' sejumlah ', CAST(quantity AS STRING)) AS description,
            order_date AS `date`,
            amount
        FROM fact_sales
        WHERE is_online_transactions = false
    ), 
    ions AS (
        SELECT 
            'income' AS type,
            id AS sales_id,
            branch_id,
            COALESCE(CAST(NULL AS INT), 0) AS employee_id,
            CONCAT('Penjualan ', product_name, ' sejumlah ', CAST(quantity AS STRING)) AS description,
            order_date AS `date`,
            price * quantity AS amount
        FROM fact_sales
        WHERE is_online_transactions = true
    ),
    oos AS (
        SELECT 
            'outcome' AS type,
            id AS sales_id,
            branch_id,
            COALESCE(CAST(NULL AS INT), 0) AS employee_id,
            'Pengeluaran untuk biaya ongkir' AS description,
            order_date AS `date`,
            delivery_fee AS amount
        FROM fact_sales
        WHERE is_free_delivery_fee = 'true'
    ),
    ods AS (
        SELECT 
            'outcome' AS type,
            id AS sales_id,
            branch_id,
            COALESCE(CAST(NULL AS INT), 0) AS employee_id,
            CONCAT('Pengeluaran diskon ', CAST(disc_name AS STRING)) AS description,
            order_date AS `date`,
            price * quantity * disc / 100 AS amount
        FROM fact_sales
        WHERE disc IS NOT NULL
    )
    SELECT * FROM iofs
    UNION ALL
    SELECT * FROM ions
    UNION ALL
    SELECT * FROM oos
    UNION ALL
    SELECT * FROM ods
    """)
    logger.info("✅ Data inserted into sink table.")


def main():
    try:
        kafka_config = {
            "bootstrap_servers": "host.docker.internal:9093",
            "source_topic": "fact_sales",
            "sink_topic": "sum_transactions"
        }

        table_env = create_table_env()
        create_tables(table_env, kafka_config)
        insert_into_sink(table_env)
        logger.info("✅ All steps completed successfully.")
    except Exception as e:
        logger.error("❌ An error occurred!")
        traceback.print_exc(file=sys.stdout)


if __name__ == "__main__":
    main()
