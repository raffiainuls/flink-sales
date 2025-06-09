import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_branch_monthly_finance_performance():
    create_sql_branch_monthly_finance_performance = """
    CREATE TABLE branch_monthly_finance_performance (
        name string,
        `month` date, 
        unit_sold int, 
        gmv bigint, 
        outcome bigint,
        revenue bigint,
        gross_profit bigint,
        nett_profit bigint,
        PRIMARY KEY (name, `month`) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'branch_monthly_finance_performance',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_branch_monthly_finance_performance

def insert_into_branch_monthly_finance_performance(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO branch_monthly_finance_performance
       -- dim_branch_montlhy_finance_performance 
        --  unit_sold 
        with uss as (
        select 
        tb.name,
        CAST(TIMESTAMPADD( MONTH, TIMESTAMPDIFF(MONTH,TIMESTAMP '1970-01-01 00:00:00',
        CAST(fs2.order_date AS TIMESTAMP)),TIMESTAMP '1970-01-01 00:00:00') AS DATE) AS `month`,
        sum(quantity) unit_sold
        from fact_sales fs2 
        left join tbl_branch tb
        on tb.id = fs2.branch_id
        group by tb.name, CAST(TIMESTAMPADD( MONTH, TIMESTAMPDIFF(MONTH,TIMESTAMP '1970-01-01 00:00:00',
        CAST(fs2.order_date AS TIMESTAMP)),TIMESTAMP '1970-01-01 00:00:00') AS DATE) 
        ),
        -- gmv 
        gmv as (
        select 
        tb.name,  
        CAST(TIMESTAMPADD( MONTH, TIMESTAMPDIFF(MONTH,TIMESTAMP '1970-01-01 00:00:00',
        CAST(fs2.order_date AS TIMESTAMP)),TIMESTAMP '1970-01-01 00:00:00') AS DATE) as `month`,
        sum(quantity * price) amount
        from fact_sales fs2 
        left join tbl_branch tb
        on tb.id = fs2.branch_id
        group by tb.name,  
        CAST(TIMESTAMPADD( MONTH, TIMESTAMPDIFF(MONTH,TIMESTAMP '1970-01-01 00:00:00',
        CAST(fs2.order_date AS TIMESTAMP)),TIMESTAMP '1970-01-01 00:00:00') AS DATE)
        ),
        -- total outcome
        outcome as (
        select 
        tb.name,
        CAST(TIMESTAMPADD(MONTH,TIMESTAMPDIFF(MONTH, TIMESTAMP '1970-01-01 00:00:00', CAST(st.`date` AS TIMESTAMP)),
        TIMESTAMP '1970-01-01 00:00:00') AS DATE) AS `month`,
        sum(amount) amount
        from sum_transactions st 
        left join tbl_branch tb 
        on tb.id = st.branch_id
        where type = 'outcome'
        group by  tb.name,
        CAST(TIMESTAMPADD(MONTH,TIMESTAMPDIFF(MONTH, TIMESTAMP '1970-01-01 00:00:00', CAST(st.`date` AS TIMESTAMP)),
        TIMESTAMP '1970-01-01 00:00:00') AS DATE) 
        ),
        -- revenue 
        revenue as (
        select
        tb.name,
        CAST(TIMESTAMPADD(MONTH,TIMESTAMPDIFF(MONTH, TIMESTAMP '1970-01-01 00:00:00', CAST(st.`date` AS TIMESTAMP)),
        TIMESTAMP '1970-01-01 00:00:00') AS DATE)  as `month`,
        sum(amount) amount
        from sum_transactions st 
        left join tbl_branch tb 
        on tb.id = st.branch_id 
        where type = 'income' and sales_id is not null 
        group by tb.name,
        CAST(TIMESTAMPADD(MONTH,TIMESTAMPDIFF(MONTH, TIMESTAMP '1970-01-01 00:00:00', CAST(st.`date` AS TIMESTAMP)),
        TIMESTAMP '1970-01-01 00:00:00') AS DATE) 
        ),
        -- gross profit
        gross_profit as (
        select
        tb.name, 
        CAST(TIMESTAMPADD( MONTH, TIMESTAMPDIFF(MONTH,TIMESTAMP '1970-01-01 00:00:00',
        CAST(fs2.order_date AS TIMESTAMP)),TIMESTAMP '1970-01-01 00:00:00') AS DATE) as `month`,
        sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        left join tbl_branch tb 
        on tb.id = fs2.branch_id
        group by tb.name, 
        CAST(TIMESTAMPADD( MONTH, TIMESTAMPDIFF(MONTH,TIMESTAMP '1970-01-01 00:00:00',
        CAST(fs2.order_date AS TIMESTAMP)),TIMESTAMP '1970-01-01 00:00:00') AS DATE)
        ),
        -- net_profit
        net_profit as (
        select 
        gp.name,
        gp.`month`,
        (gp.amount - o.amount) as amount
        from gross_profit gp
        left join outcome o
        on  gp.name = o.name and  o.`month` = gp.`month`
        )
        select 
        u.name,
        u.`month`,
        u.unit_sold,
        g.amount as gmv,
        o.amount as outcome,
        r.amount as revenue,
        gp.amount as gross_profit,
        np.amount as nett_profit
        from uss u
        left join gmv g
        on g.`month` = u.`month` and g.name = u.name
        left join outcome o
        on o.`month` = u.`month` and o.name = u.name
        left join revenue r
        on r.`month` = u.`month` and r.name = u.name
        left join gross_profit gp 
        on gp.`month` = u.`month` and gp.name = u.name
        left join net_profit np 
        on np.`month` = u.`month` and np.name = u.name
    """)
    logger.info("✅ Data inserted into sink table.")