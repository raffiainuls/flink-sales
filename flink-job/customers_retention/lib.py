import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_customers_retention():
    create_sql_customers_retention = """
    CREATE TABLE customers_retention (
        bulan date,
        total_customers int, 
        retained_customers INT, 
        retention_rate_percent float, 
        PRIMARY KEY (bulan) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'customers_retention',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_customers_retention

def insert_into_customers_retention(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO customers_retention
      -- monthly custome retention 
        WITH monthly_customers AS (
            SELECT 
                customer_id,
                DATE_TRUNC('month', order_date) AS bulan
            FROM fact_sales
            GROUP BY customer_id, bulan
        ),
        retention AS (
            SELECT 
                mc1.bulan,
                COUNT(DISTINCT mc1.customer_id) AS total_customers,
                COUNT(DISTINCT mc2.customer_id) AS retained_customers
            FROM monthly_customers mc1
            LEFT JOIN monthly_customers mc2
                ON mc1.customer_id = mc2.customer_id  
                AND mc2.bulan = mc1.bulan + INTERVAL '1 month'
            GROUP BY mc1.bulan
        ),
        retention_rate AS (
            SELECT 
                bulan,
                total_customers,
                retained_customers, 
                (retained_customers * 100.0 / NULLIF(total_customers, 0)) AS retention_rate_percent
            FROM retention
        )
        SELECT * FROM retention_rate
    """)
    logger.info("✅ Data inserted into sink table.")