from data_add import PostgresExamples
import os
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    examples = PostgresExamples()
    examples.get_envs()
    examples.connect_to_db()
    examples.fake_data()
    examples.create_tables()