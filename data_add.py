"""
Allows for inserting data from a csv file to a Postgres database.
"""
from __future__ import annotations
import pandas as pd
import os
from faker import Faker
import psycopg as pg
import logging
import json
from dataclasses import dataclass, field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PostgresExamples:
    def __init__(self):
        pass
    username: str = "postgres"
    password: str | None = None
    db_host: str = "localhost"
    db_name: str = "testdb"
    port: str = "5432"
    conn: None = None
    df_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    read_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    def get_envs(self) -> tuple[str, str]:
        """Get database credentials from environment variables"""
        self.password = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
        self.username = os.getenv("DB_USER") or os.getenv("POSTGRES_USER") or self.username
        self.db_host = os.getenv("DB_HOST") or self.db_host
        self.db_name = os.getenv("DB_NAME") or self.db_name
        self.port = os.getenv("DB_PORT") or self.port
        if self.password is None:
            raise RuntimeError(
                "Database password not found in environment variables. Set DB_PASSWORD or POSTGRES_PASSWORD.")
        logger.info(f"Connecting to database: {self.db_name} at {self.db_host}:{self.port} as user {self.username}")
        return self.username, self.password
    def connect_to_db(self):
        """Connect to PostgreSQL database"""
        self.username, self.password = self.get_envs()
        try:
            self.conn = pg.connect(
                host=self.db_host,
                port=self.port,
                dbname=self.db_name,
                user=self.username,
                password=self.password
            )
            cur = self.conn.cursor()
            cur.execute('SELECT version()')
            db_version = cur.fetchone()
            logger.info("Connected to Postgres database version: %s", db_version)
            cur.close()
        except (Exception, pg.DatabaseError) as e:
            logger.error(f"Error connecting to the database: {e}")
            self.conn = None
            raise
        finally:
            if self.conn is not None:
                logger.info("Database connection established.")
        return self.conn
    @staticmethod
    def generate_data():
        """Generate fake data for the DB."""
        fake = Faker()
        num_entries = 50
        data = {
            "last_name": [],
            "first_name": [],
            "gender": [],
            "address": []
        }
        genders = ['m', 'f', 'x']
        for _ in range(num_entries):
            data['last_name'].append(fake.last_name())
            data['first_name'].append(fake.first_name())
            data['gender'].append(fake.random_element(genders))
            address_json = {
                "address": fake.street_address(),
                "city": fake.city(),
                "state": fake.state()
            }
            data['address'].append(address_json)
        return data
    def fake_data(self, filepath=None, filename='fake_data.csv'):
        """Saves the fake data to csv and returns that csv back as a DF for testing."""
        data = self.generate_data()
        if filepath is None:
            filepath = 'data'
            os.makedirs(filepath, exist_ok=True)
        result = os.path.join(filepath, filename)
        self.df_data = pd.DataFrame(data)
        self.df_data.to_csv(result, index=False)
        self.read_data = pd.read_csv(result)
        return self.read_data
    def create_table(self, table_name="testtable"):
        """Creates table and inserts data into it."""
        if self.conn is None:
            logger.error("No database connection. Call connect_to_db() first.")
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_name} "
                    f"(last_name varchar(80), first_name varchar(80), gender varchar(80), address JSON);")
                if not self.df_data.empty:
                    data_tuples = [
                    (row['last_name'], row['first_name'], row['gender'], json.dumps(row['address']))
                    for _, row in self.df_data.iterrows()
                ]
                    cur.executemany(
                    f"INSERT INTO {table_name} (last_name, first_name, gender, address) VALUES (%s, %s, %s, %s)",
                    data_tuples
                    )
                    self.conn.commit()
                    logging.info("Data successfully inserted into testtable.")
                else:
                    logging.warning("No data to insert into testtable.")
        except (Exception, pg.DatabaseError) as e:
            logging.error(f"Error inserting data: {e}")
            if self.conn:
                self.conn.rollback()
            raise
    def close_connection(self):
        if self.conn:
            self.conn.close()
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
