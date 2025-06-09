import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from helper.function import create_env, create_table_if_not_exists
from fact_sales.lib import create_fact_sales, kafka_sink_config
from tbl_product.lib import create_tbl_product
from sum_transactions.lib import create_sum_transactions 
from weeakly_finance_performance.lib import create_weeakly_finance_performance, insert_into_weeakly_finance_performance 

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    try:
        table_env = create_env()

        tbl_product= create_tbl_product()
        create_table_if_not_exists(table_env, "tbl_product", tbl_product)

        fact_sales = create_fact_sales(kafka_sink_config(), "fact_sales")
        create_table_if_not_exists(table_env, "fact_sales",fact_sales)

        sum_transactions = create_sum_transactions()
        create_table_if_not_exists(table_env, "sum_transactions", sum_transactions)

        weeakly_finance_performance = create_weeakly_finance_performance()
        create_table_if_not_exists(table_env, "weeakly_finance_performance", weeakly_finance_performance)

        insert_into_weeakly_finance_performance(table_env)
        logger.info("✅ All steps completed successfully.")
    except Exception as e:
        logger.error("❌ An error occurred!")
        traceback.print_exc(file=sys.stdout)

if __name__ == "__main__":
    main()