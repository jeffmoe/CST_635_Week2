from data_add import PostgresExamples
import logging
import sys
import time
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_db(retry=5, timeout=10):
    for attempt in range(retry):
        try:
            logger.info("Waiting for DB connection")
            examples = PostgresExamples()
            examples.get_envs()
            conn = examples.connect_to_db()
            if conn:
                examples.close_connection()
                logger.info("DB connection established")
            return True
        except Exception as e:
            logger.warning(f"Db is not ready to connect (attempt ({attempt+1})/{retry}): {e}")
            time.sleep(timeout)
    logger.warning("Db connection failed")
    return False

if __name__ == "__main__":
    if not wait_for_db():
        sys.exit(1)
    examples = PostgresExamples()
    try:
        examples.get_envs()
        examples.conn = examples.connect_to_db()
        examples.df_data = examples.fake_data()
        examples.create_table()
        logger.info("All operations completed successfully!")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        sys.exit(1)
    finally:
        if examples.conn:
            examples.close_connection()