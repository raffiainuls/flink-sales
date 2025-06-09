import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from helper.function import create_env, create_table_if_not_exists
from tbl_employee.lib import create_tbl_employee
from fact_employee.lib import create_fact_employee, insert_into_fact_employee

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    try:
        table_env = create_env()

        tbl_employee = create_tbl_employee()
        create_table_if_not_exists(table_env, "tbl_employee", tbl_employee)

        fact_employee = create_fact_employee()
        create_table_if_not_exists(table_env, "fact_employee", fact_employee)

        insert_into_fact_employee(table_env)
        logger.info("✅ All steps completed successfully.")
    except Exception as e:
        logger.error("❌ An error occurred!")
        traceback.print_exc(file=sys.stdout)

if __name__ == "__main__":
    main()