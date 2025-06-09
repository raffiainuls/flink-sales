import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_finance_performance():
    create_sql_finance_performance = """
    CREATE TABLE finance_performance (
        unit_sold INT,
        outcome bigint, 
        gmv bigint, 
        revenue bigint, 
        gross_profit bigint,
        net_profit bigint,
        PRIMARY KEY (unit_sold, outcome, gmv, revenue, gross_profit, net_profit) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'finance_performance',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_finance_performance

def insert_into_finance_performance(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO finance_performance
              with uss as (
        select 
        sum(quantity) unit_sold
        from fact_sales fs2 
        ),
        -- gmv 
        gmv as (
        select 
        sum(quantity * price) amount
        from fact_sales fs2 
        ),
        -- total outcome
        outcome as (
        select 
        sum(amount) amount
        from sum_transactions st 
        where type = 'outcome'
        ),
        -- revenue 
        revenue as (
        select
        sum(amount) amount
        from sum_transactions st 
        where type = 'income' and sales_id is not null 
        ),
        -- gross profit
        gross_profit as (
        select 
        sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        ),
        -- net_profit
        net_profit as (
        select (select amount from gross_profit) - (select amount from outcome) as amount
        )
        SELECT 
            (SELECT unit_sold FROM uss) AS unit_sold,
            (SELECT amount FROM outcome) AS outcome,
            (SELECT amount FROM gmv) AS gmv, 
            (SELECT amount FROM revenue) AS revenue,
            (SELECT amount FROM gross_profit) AS gross_profit,
            (SELECT amount FROM net_profit) AS net_profit
    """)
    logger.info("✅ Data inserted into sink table.")