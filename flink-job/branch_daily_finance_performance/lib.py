import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_branch_daily_finance_performance():
    create_sql_branch_daily_finance_performance = """
    CREATE TABLE branch_daily_finance_performance (
        name string,
        `date` date, 
        unit_sold INT, 
        gmv bigint, 
        outcome bigint,
        revenue bigint,
        gross_profit bigint,
        nett_profit bigint,
        PRIMARY KEY (name, `date`) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'branch_daily_finance_performance',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_branch_daily_finance_performance

def insert_into_branch_daily_finance_performance(table_env):
    logger.info("🔁 Inserting data into sink table...")

    table_env.execute_sql("""
    INSERT INTO branch_daily_finance_performance
    WITH uss AS (
        SELECT 
            tb.name, 
            cast(fs2.order_date as date) AS `date`,
            SUM(quantity) AS unit_sold
        FROM fact_sales fs2 
        LEFT JOIN tbl_branch tb ON tb.id = fs2.branch_id 
        GROUP BY tb.name, cast(fs2.order_date as date)
    ),
    gmv AS (
        SELECT 
            tb.name, 
            cast(fs2.order_date as date) AS `date`,
            SUM(quantity * price) AS amount
        FROM fact_sales fs2 
        LEFT JOIN tbl_branch tb ON tb.id = fs2.branch_id 
        GROUP BY tb.name, cast(fs2.order_date as date)
    ),
    outcome AS (
        SELECT 
            tb.name, 
            cast(st.`date` as date) AS `date`,
            SUM(amount) AS amount
        FROM sum_transactions st 
        LEFT JOIN tbl_branch tb ON tb.id = st.branch_id 
        WHERE type = 'outcome'
        GROUP BY tb.name, cast(st.`date` as date)
    ),
    revenue AS (
        SELECT
            tb.name,
            cast(st.`date` as date) AS `date`,
            SUM(amount) AS amount
        FROM sum_transactions st 
        LEFT JOIN tbl_branch tb ON tb.id = st.branch_id 
        WHERE type = 'income' AND sales_id IS NOT NULL 
        GROUP BY tb.name, cast(st.`date` as date)
    ),
    gross_profit AS (
        SELECT 
            tb.name, 
            cast(fs2.order_date as date) AS `date`,
            SUM((fs2.price * tp.profit / 100) * fs2.quantity) AS amount
        FROM fact_sales fs2 
        LEFT JOIN tbl_product tp ON tp.id = fs2.product_id 
        LEFT JOIN tbl_branch tb ON tb.id = fs2.branch_id 
        GROUP BY tb.name, cast(fs2.order_date as date)
    ),
    net_profit AS (
        SELECT 
            gp.name,
            gp.`date`,
            (gp.amount - COALESCE(o.amount, 0)) AS amount
        FROM gross_profit gp
        LEFT JOIN outcome o ON o.`date` = gp.`date` AND o.name = gp.name 
    )
    SELECT 
        u.name,
        u.`date`,
        u.unit_sold,
        g.amount AS gmv,
        o.amount AS outcome,
        r.amount AS revenue,
        gp.amount AS gross_profit,
        np.amount AS nett_profit
    FROM uss u
    LEFT JOIN gmv g ON g.`date` = u.`date` AND g.name = u.name
    LEFT JOIN outcome o ON o.`date` = u.`date` AND o.name = u.name 
    LEFT JOIN revenue r ON r.`date` = u.`date` AND r.name = u.name 
    LEFT JOIN gross_profit gp ON gp.`date` = u.`date` AND gp.name = u.name 
    LEFT JOIN net_profit np ON np.`date` = u.`date` AND np.name = u.name 
    """)
    logger.info("✅ Data inserted into sink table.")