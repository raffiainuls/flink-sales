import logging
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_env():
    logger.info("🚀 Starting PyFlink environment setup...")
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)
    logger.info("✅ TableEnvironment created.")
    return table_env

def create_table_if_not_exists(table_env, table_name: str, create_sql: str):
    catalog = table_env.get_current_catalog()
    database = table_env.get_current_database()
    obj_path = ObjectPath(database, table_name)

    # get instance catalog 
    catalog_inst = table_env.get_catalog(catalog)


    # checking there is table?
    if not catalog_inst.table_exists(obj_path):
        logger.info(f"📦 Creating table '{table_name}'...")
        table_env.execute_sql(create_sql)
        logger.info(f"✅ Table '{table_name}' created.")
    else:
        logger.info(f"ℹ️ Table '{table_name}' already exists. Skipping creation.")

    
