import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



def create_monthly_branch_performance(with_clause: str):
    return f"""
    CREATE TABLE monthly_branch_performance (
        branch_id INT,
        branch_name STRING, 
        bulan DATE,
        total_sales INT,
        amount BIGINT,
        PRIMARY KEY (branch_id, bulan) NOT ENFORCED
    )
    {with_clause}
    """
def kafka_sink_config():
    config = """
        WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'monthly_branch_performance',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'key.format' = 'json',
            'value.format' = 'json'
        )
        """
    return config



    


def create_monthly_branch_performance():
    create_sql_monthly_branch_performance = """
    CREATE TABLE monthly_branch_performance (
        branch_id INT,
        branch_name string, 
        bulan date,
        total_sales int,
        amount bigint,
        PRIMARY KEY (branch_id, bulan) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'monthly_branch_performance',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_monthly_branch_performance

def insert_into_branch_performance(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO monthly_branch_performance
       select 
        fs2.branch_id,
        tb.name  as branch_name,
        CAST(DATE_FORMAT(order_date, 'yyyy-MM-01') AS DATE)  as bulan,
        sum(quantity) as total_sales,
        sum(amount) as amount
        from fact_sales fs2 
        left join tbl_branch tb 
        on tb.id = fs2.branch_id 
        group by fs2.branch_id, tb.name,  CAST(DATE_FORMAT(order_date, 'yyyy-MM-01') AS DATE)
    """)
    logger.info("✅ Data inserted into sink table.")