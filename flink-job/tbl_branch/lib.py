import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_tbl_branch():
    create_sql_tbl_branch = """
    CREATE TABLE tbl_branch (
        payload ROW<
            id INT,
            name string,
            location string,
            address string,
            email string,
            phone string,
            created_time STRING,
            modified_time string
        >
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'table.public.tbl_branch',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    return create_sql_tbl_branch