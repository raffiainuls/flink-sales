import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_product_performance():
    create_sql_product_performance = """
    CREATE TABLE product_performance (
        product_id INT,
        product_name string, 
        jumlah_terjual INT, 
        PRIMARY KEY (product_id) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'product_performance',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_product_performance

def insert_into_product_performance(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO product_performance
       select 
        fs2.product_id,
        tp.product_name,
        sum(quantity) as jumlah_terjual 
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        group by fs2.product_id,  tp.product_name
    """)
    logger.info("✅ Data inserted into sink table.")