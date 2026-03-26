from future import annotations
import numpy as np
import pandas as pd
import os
import Faker
import psycopg2 as pg
import logging
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
    conn: None = None
    df_data: pd.DataFrame = field(default_factory=pd.DataFrame)

    def get_envs(self) -> str:
        self.password = os.getenv("POSTGRES_PASSWORD")
        if self.password is None:
            raise RuntimeError
        return self.username, self.password

    def connect_to_db(self):
        self.username, self.password = self.get_envs()
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.username,
                password=self.password
            )
            cur = self.conn.cursor()
            cur.execute('SELECT version()')
            db_version = cur.fetchone()
            logging.info("Connected to Postgres database version: %s", db_version)
            cur.close()
        except (Exception, psycopg2.DatabaseError) as e:
            logging.error(f"Error connecting to the database: {e}")
            self.conn = None
        finally:
            if self.conn is not None:
                logging.info("Database connection established.")
        return self.conn

    @staticmethod
    def generate_data():
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

    def fake_data(self):
        data = self.generate_data()
        filepath = '/mnt/c/Users/jefft/OneDrive - Concordia University, St. Paul/CST-635'
        filename = 'fake_data.csv'
        result = os.path.join(filepath, filename)
        self.df_data = pd.DataFrame(data)
        self.df_data.to_csv(result, index=False)
        return self.df_data

    def create_table(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS testtable (last_name varchar(80), first_name varchar(80), gender varchar(80), address JSON);")
                data_tuples = [
                    (row['last_name'], row['first_name'], row['gender'], json.dumps(row['address']))
                    for _, row in self.df_data.iterrows()
                ]
                cur.executemany(
                    "INSERT INTO testtable (last_name, first_name, gender, address) VALUES (%s, %s, %s, %s)",
                    data_tuples
                )
                self.conn.commit()
                logging.info("Data successfully inserted into testtable.")
        except (Exception, psycopg2.DatabaseError) as e:
            logging.error(f"Error inserting data: {e}")

    def pull_data(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM testtable WHERE gender = 'm';")
                rows = cur.fetchall()
                logging.info(f"Retrieved {len(rows)} rows where gender was male.")
                rows_df = pd.DataFrame(rows, columns=['last_name', 'first_name', 'gender', 'address'])
                print(rows_df.info())
                print(rows_df.describe())
        except (Exception, psycopg2.DatabaseError) as e:
            logging.error(f"Error retrieving data: {e}")

    def change_data_test(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM testtable WHERE address->>'state' = 'New York';")
                rows = cur.fetchall()
                logging.info(f"Retrieved {len(rows)} rows where state was New York.")
                rows_df = pd.DataFrame(rows, columns=['last_name', 'first_name', 'gender', 'address'])
                print(rows_df.info())
                print(rows_df.describe())

                cur.execute("DELETE FROM testtable WHERE address->>'state' = 'New York';")
                self.conn.commit()
                logging.info("Deleted rows where state was New York.")

                cur.execute("SELECT address ->> 'state' FROM testtable;")
                states = cur.fetchall()
                logging.info(f"Retrieved states from address column: {states}")
                states_df = pd.DataFrame(states, columns=['state'])
                print(states_df)

        except (Exception, psycopg2.DatabaseError) as e:
            logging.error(f"Error retrieving data: {e}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
