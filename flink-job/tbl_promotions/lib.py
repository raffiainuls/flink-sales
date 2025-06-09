import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_tbl_promotions():
    create_sql_tbl_promotions = """
        CREATE TABLE tbl_promotions (
        payload ROW<
            id INT,
            event_name STRING,
            disc INT,
            `time` STRING,
            created_at STRING
        >
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'table.public.tbl_promotions',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    return create_sql_tbl_promotions