import logging
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from helper.function import create_env, create_table_if_not_exists
from tbl_product.lib import create_tbl_product
from tbl_promotions.lib import create_tbl_promotions
from tbl_sales.lib import create_tbl_sales
from fact_sales.lib import create_fact_sales, insert_into_fact_sales, kafka_sink_config


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    try:
        table_env = create_env()
        tbl_product = create_tbl_product()
        create_table_if_not_exists(table_env, "tbl_product", tbl_product)
        tbl_promotions = create_tbl_promotions()
        create_table_if_not_exists(table_env, "table_promotions",tbl_promotions)
        tbl_sales = create_tbl_sales()
        create_table_if_not_exists(table_env, "tbl_sales", tbl_sales)

        logger.info("🧾 Creating Kafka sink: fact_sales...")
        fact_sales = create_fact_sales(kafka_sink_config(), "fact_sales")
        create_table_if_not_exists(table_env, "fact_sales",fact_sales)

        # logger.info("🧾 Creating Postgres sink: fact_sales...")
        # fact_sales_postgres = create_fact_sales(postgres_sink_config(), "fact_sales_postgres")
        # create_table_if_not_exists(table_env, "fact_sales_postgres",fact_sales_postgres)

        # logger.info("🧾 Creating clickhouse sink: fact_sales...")
        # fact_sales_clickhouse = create_fact_sales(clickhouse_sink_config(), "fact_sales_clickhouse")
        # create_table_if_not_exists(table_env, "fact_sales_clickhouse",fact_sales_clickhouse)


        insert_into_fact_sales(table_env, "fact_sales")
        # insert_into_fact_sales(table_env, "fact_sales_postgres")
        logger.info("✅ All steps completed successfully.")
    except Exception as e:
        logger.error("❌ An error occurred!")
        traceback.print_exc(file=sys.stdout)


if __name__ == "__main__":
    main()