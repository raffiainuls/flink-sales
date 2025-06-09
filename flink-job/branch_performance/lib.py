import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_branch_performance():
    create_sql_branch_performance = """
    CREATE TABLE branch_performance (
        branch_id INT,
        branch_name string, 
        total_sales INT, 
        amount bigint, 
        PRIMARY KEY (branch_id) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'branch_performance',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_branch_performance

def insert_into_branch_performance(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO branch_performance
      select 
        fs2.branch_id,
        tb.name as branch_name,
        sum(quantity) as total_sales,
        sum(amount) amount
        from fact_sales fs2 
        left join tbl_branch tb 
        on tb.id = fs2.branch_id 
        group by fs2.branch_id, tb.name
    """)
    logger.info("✅ Data inserted into sink table.")