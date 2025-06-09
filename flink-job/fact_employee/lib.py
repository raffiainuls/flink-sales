import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_fact_employee():
    create_sql_fact_employee = """
    CREATE TABLE fact_employee (
        id INT,
        branch_id int, 
        name string, 
        salary bigint, 
        active boolean,
        address string, 
        phone string, 
        email string,
        created_at timestamp,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'fact_employee',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_fact_employee

def insert_into_fact_employee(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO fact_employee
      select 
        id,
        branch_id,
        name, 
        salary,
        active,
        address,
        phone,
        email,
        cast(created_at as timestamp)
        from tbl_employee 
        where active = true
    """)
    logger.info("✅ Data inserted into sink table.")