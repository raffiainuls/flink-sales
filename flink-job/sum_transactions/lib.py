import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_sum_transactions():
    create_sql_sum_transactions = """
    CREATE TABLE sum_transactions (
        type string,
        sales_id int,
        branch_id int,
        employee_id int,
        description string,
        `date` timestamp,
        amount bigint,
        PRIMARY KEY (type,sales_id, branch_id, employee_id) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'sum_transactions',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_sum_transactions

def insert_into_sum_transactions(table_env):
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
    WHERE is_online_transaction = false
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
    WHERE is_online_transaction = true
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