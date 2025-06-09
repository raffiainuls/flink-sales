import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_tbl_employee():
    create_sql_tbl_employee = """
    CREATE TABLE tbl_employee (
    payload ROW<
        id INT,
        branch_id int, 
        name string, 
        salary bigint, 
        active boolean,
        address string, 
        phone string, 
        email string,
        created_at timestamp
        >
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'table.public.tbl_employee',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    return create_sql_tbl_employee
