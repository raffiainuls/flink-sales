import logging
import sys
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from helper.function import create_env, create_table_if_not_exists
from fact_sales.lib import create_fact_sales, kafka_sink_config
from tbl_branch.lib import create_tbl_branch
from branch_performance.lib import create_branch_performance, insert_into_branch_performance

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    try:
        table_env = create_env()

        fact_sales = create_fact_sales(kafka_sink_config(), "fact_sales")
        create_table_if_not_exists(table_env, "fact_sales",fact_sales)

        tbl_branch = create_tbl_branch()
        create_table_if_not_exists(table_env, "tbl_branch", tbl_branch)

        branch_performance = create_branch_performance()
        create_table_if_not_exists(table_env, "branch_performance", branch_performance)
        
        insert_into_branch_performance(table_env)
        logger.info("✅ All steps completed successfully.")
    except Exception as e:
        logger.error("❌ An error occurred!")
        traceback.print_exc(file=sys.stdout)

if __name__ == "__main__":
    main()