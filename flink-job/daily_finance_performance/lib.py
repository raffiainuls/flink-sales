import logging
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_daily_finance_performance():
    create_sql_daily_finance_performance = """
    CREATE TABLE daily_finance_performance (
        `date` date,
        unit_sold int, 
        gmv bigint, 
        outcome bigint, 
        revenue bigint,
        gross_profit bigint,
        nett_profit bigint,
        PRIMARY KEY (`date`) NOT ENFORCED
    ) WITH (
       'connector' = 'upsert-kafka',
        'topic' = 'daily_finance_performance',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_daily_finance_performance

def insert_into_daily_finance_performance(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO daily_finance_performance
       --dims_daily_finance_performance
        --  unit_sold 
        with uss as (
        select 
        cast(fs2.order_date as date) as `date`,
        sum(quantity) unit_sold
        from fact_sales fs2 
        group by  cast(fs2.order_date as date)
        ),
        -- gmv 
        gmv as (
        select 
        cast(fs2.order_date as date) as `date`,
        sum(quantity * price) amount
        from fact_sales fs2 
        group by  cast(fs2.order_date as date)
        ),
        -- total outcome
        outcome as (
        select 
        cast(st.`date` as date) as `date`,
        sum(amount) amount
        from sum_transactions st 
        where type = 'outcome'
        group by cast(st.`date` as date)
        ),
        -- revenue 
        revenue as (
        select
        cast(st.`date` as date) as `date`,
        sum(amount) amount
        from sum_transactions st 
        where type = 'income' and sales_id is not null 
        group by cast(st.`date` as date)
        ),
        -- gross profit
        gross_profit as (
        select 
        cast(fs2.order_date as date) as `date`,
        sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        group by  cast(fs2.order_date as date)
        ),
        -- net_profit
        net_profit as (
        select 
        gp.`date`,
        (gp.amount - o.amount) as amount
        from gross_profit gp
        left join outcome o
        on  o.`date` = gp.`date`
        )
        select 
        u.`date`,
        u.unit_sold,
        g.amount as gmv,
        o.amount as outcome,
        r.amount as revenue,
        gp.amount as gross_profit,
        np.amount as nett_profit
        from uss u
        left join gmv g
        on g.`date` = u.`date`
        left join outcome o
        on o.`date` = u.`date`
        left join revenue r
        on r.`date` = u.`date`
        left join gross_profit gp 
        on gp.`date` = u.`date` 
        left join net_profit np 
        on np.`date` = u.`date`
    """)
    logger.info("✅ Data inserted into sink table.")