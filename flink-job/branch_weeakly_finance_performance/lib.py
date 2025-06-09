import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_branch_weeakly_finance_performance():
    create_sql_branch_weeakly_finance_performance = """
    CREATE TABLE branch_weeakly_finance_performance (
        name string,
        week date, 
        unit_sold INT, 
        gmv bigint, 
        outcome bigint, 
        revenue bigint,
        gross_profit bigint,
        nett_profit bigint,
        PRIMARY KEY (name,week) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'branch_weeakly_finance_performance',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_branch_weeakly_finance_performance

def insert_into_branch_weeakly_finance_performance(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO branch_weeakly_finance_performance
      -- dim_branch_weakly_finance_performance 
        --  unit_sold 
        with uss as (
        select 
        tb.name, 
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7, DATE '2024-12-29') AS week,
        sum(quantity) unit_sold
        from fact_sales fs2 
        left join tbl_branch tb
        on tb.id = fs2.branch_id 
        group by tb.name, 
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7,DATE '2024-12-29')
        ),
        -- gmv 
        gmv as (
        select 
        tb.name, 
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7,DATE '2024-12-29') AS week,
        sum(quantity * price) amount
        from fact_sales fs2 
        left join tbl_branch tb 
        on tb.id = fs2.branch_id 
        group by tb.name, 
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7,DATE '2024-12-29')
        ),
        -- total outcome
        outcome as (
        select 
        tb.name, 
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(st.`date` AS DATE)) / 7) * 7,DATE '2024-12-29') as week,
        sum(amount) amount
        from sum_transactions st 
        left join tbl_branch tb
        on tb.id = st.branch_id
        where type = 'outcome'
        group by tb.name,
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(st.`date` AS DATE)) / 7) * 7,DATE '2024-12-29')
        ),
        -- revenue 
        revenue as (
        select
        tb.name, 
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(st.`date` AS DATE)) / 7) * 7,DATE '2024-12-29')  AS week,
        sum(amount) amount
        from sum_transactions st 
        left join tbl_branch tb
        on tb.id = st.branch_id 
        where type = 'income' and sales_id is not null 
        group by  tb.name, 
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(st.`date` AS DATE)) / 7) * 7,DATE '2024-12-29')
        ),
        -- gross profit
        gross_profit as (
        select 
        tb.name, 
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7,DATE '2024-12-29') AS week,
        sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        left join tbl_branch tb
        on tb.id = fs2.branch_id 
        group by tb.name, 
        TIMESTAMPADD(DAY,FLOOR(TIMESTAMPDIFF(DAY, DATE '2024-12-29', CAST(fs2.order_date AS DATE)) / 7) * 7,DATE '2024-12-29')
        ),
        -- net_profit
        net_profit as (
        select 
        gp.name, 
        gp.week,
        (gp.amount - o.amount) as amount
        from gross_profit gp
        left join outcome o
        on  o.week = gp.week and o.name = gp.name
        )
        select 
        u.name, 
        u.week,
        u.unit_sold,
        g.amount as gmv,
        o.amount as outcome,
        r.amount as revenue,
        gp.amount as gross_profit,
        np.amount as nett_profit
        from uss u
        left join gmv g
        on g.week = u.week and g.name = u.name 
        left join outcome o
        on o.week = u.week and o.name = u.name
        left join revenue r
        on r.week = u.week and r.name = u.name
        left join gross_profit gp 
        on gp.week = u.week  and gp.name = u.name
        left join net_profit np 
        on np.week = u.week and np.name = u.name
    """)
    logger.info("✅ Data inserted into sink table.")