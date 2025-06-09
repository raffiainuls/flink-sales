import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_weeakly_finance_performance():
    create_weeakly_finance_performance = """
    CREATE TABLE weeakly_finance_performance (
        week date,
        unit_sold int, 
        gmv bigint, 
        outcome bigint, 
        revenue bigint,
        gross_profit bigint,
        nett_profit bigint,
        PRIMARY KEY (week) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'weeakly_finance_performance',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_weeakly_finance_performance

def insert_into_weeakly_finance_performance(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO weeakly_finance_performance
       -- dims_weakly_finance_performance 
    --  unit_sold 
    with uss as (
    select 
    TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7, DATE '2024-12-29') AS week,
    sum(quantity) unit_sold
    from fact_sales fs2 
    group by TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7, DATE '2024-12-29')
    ),
    -- gmv 
    gmv as (
    select 
    TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7, DATE '2024-12-29') AS week,
    sum(quantity * price) amount
    from fact_sales fs2 
    group by TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7, DATE '2024-12-29')
    ),
    -- total outcome
    outcome as (
    select 
    TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(st.`date` AS DATE)) / 7) * 7,DATE '2024-12-29') AS week,
    sum(amount) amount
    from sum_transactions st 
    where type = 'outcome'
    group by TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(st.`date` AS DATE)) / 7) * 7,DATE '2024-12-29')
    ),
    -- revenue 
    revenue as (
    select
    TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(st.`date` AS DATE)) / 7) * 7,DATE '2024-12-29') AS week,
    sum(amount) amount
    from sum_transactions st 
    where type = 'income' and sales_id is not null 
    group by TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(st.`date` AS DATE)) / 7) * 7,DATE '2024-12-29')
    ),
    -- gross profit
    gross_profit as (
    select 
    TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7, DATE '2024-12-29') AS week,
    sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
    from fact_sales fs2 
    left join tbl_product tp 
    on tp.id  = fs2.product_id 
    group by TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7, DATE '2024-12-29')
    ),
    -- net_profit
    net_profit as (
    select 
    gp.week,
    (gp.amount - o.amount) as amount
    from gross_profit gp
    left join outcome o
    on  o.week = gp.week
    )
    select 
    u.week,
    u.unit_sold,
    g.amount as gmv,
    o.amount as outcome,
    r.amount as revenue,
    gp.amount as gross_profit,
    np.amount as nett_profit
    from uss u
    left join gmv g
    on g.week = u.week
    left join outcome o
    on o.week = u.week
    left join revenue r
    on r.week = u.week
    left join gross_profit gp 
    on gp.week = u.week 
    left join net_profit np 
    on np.week = u.week
    """)
    logger.info("✅ Data inserted into sink table.")