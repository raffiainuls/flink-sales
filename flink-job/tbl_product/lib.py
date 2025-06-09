import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_tbl_product():
    create_sql_tbl_product = """
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
    )
    """
    return create_sql_tbl_product