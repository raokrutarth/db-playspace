import logging
import os
import sys
import psycopg2
from random import randint, random
from argparse import ArgumentParser
from multiprocessing import Process
from threading import Thread
from time import time, sleep
from datetime import datetime


'''
    Notes

    - To copy from large text file, place file in DB container and run command below:
      - COPY weather FROM '/home/user/weather.txt';

'''
# configure logging with filename, function name and line numbers
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "DEBUG"),
    datefmt='%I:%M:%S %p %Z',
    format='%(levelname)s [%(asctime)s - %(filename)s:%(lineno)s::%(funcName)s]\t%(message)s',
)
log = logging.getLogger()


class PGPlayspace:

    def __init__(self, host="postgres", port=5432, dbname="myPGDB", user="myPGUser", password="myPGPassword"):
        self.db_client = self._get_client(host, port, dbname, user, password)
        self.db_cursor = self.db_client.cursor()

    def terminate(self):
        self.db_client.close()

    def _get_client(self, host, port, dbname, user, password):
        try:
            client = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
            )
            return client
        except Exception:
            log.exception("Unable to connect to database with exception", exc_info=True)
            sys.exit(1)

    def create_table(self):

        self.db_cursor.execute("""
            CREATE TABLE weather (
                city            varchar(80),
                temp_lo         int,           -- low temperature
                temp_hi         int,           -- high temperature
                prcp            real,          -- precipitation
                date            date
            );
        """)
        self.db_cursor.execute("""
            CREATE TABLE cities (
                name            varchar(80),
                location        point
            );
        """)
        self.db_client.commit()

    def delete_table(self):
        try:
            self.db_cursor.execute("""
                DROP TABLE cities;
                DROP TABLE weather;
            """)
            self.db_client.commit()
        except Exception:
            self.db_client.rollback()
            log.warning("unable to delete tables")

    def add_data(self):
        '''
            basic inster into multiple tables
        '''
        try:
            for i in range(50):
                r = randint(1000, 50000000)
                self.db_cursor.execute(f"""
                    INSERT INTO weather (city, temp_lo, temp_hi, prcp, date)
                    VALUES ('City-{r}', {r*2}, {i**3}, {random()}, '{datetime.now()}');
                """)

                self.db_cursor.execute(f"""
                    INSERT INTO cities VALUES ('City-{r}', '({random()}, {random()})');
                """)

            self.db_cursor.execute("""
                select * from weather;
            """)

            self.db_client.commit()
            records = self.db_cursor.fetchall()  # TODO stability
            log.info("Added %d records", len(records))
            log.info("Head: %s", records[:2])
            return records if records else []
        except Exception:
            self.db_client.rollback()
            log.error("unable to add dummy data with exception", exc_info=True)

    def basic_select_queries(self):
        self.db_cursor.execute("""
            SELECT * FROM weather;
        """)
        # log.debug(self.db_cursor.fetchall())

        # numeric aggregation
        self.db_cursor.execute("""
            SELECT city, (temp_hi+temp_lo)/2 AS temp_avg, date FROM weather;
        """)
        # log.debug(self.db_cursor.fetchall())

        # substring matching and filtering
        self.db_cursor.execute("""
            SELECT * FROM weather
            WHERE city LIKE 'City-4%' AND prcp > 0.0;
        """)
        # log.debug(self.db_cursor.fetchall())

        # numeric calculation, substring matching, sorting and filtering
        self.db_cursor.execute("""
            SELECT city, POW(temp_hi+temp_lo, 2)/POW(temp_hi, 2) AS temp_alpha FROM weather
            WHERE city LIKE 'City-4%' AND prcp > 0.0
            ORDER BY temp_alpha;
        """)
        # log.debug(self.db_cursor.fetchall())

        # rows with unique values only and sort
        self.db_cursor.execute("""
            SELECT DISTINCT city
            FROM weather
            ORDER BY city;
        """)
        # log.debug(self.db_cursor.fetchall())

    def basic_join_queries(self):
        self.db_cursor.execute("""
            SELECT * FROM weather;
        """)
        # log.debug(self.db_cursor.fetchall())


def main(args):
    log.info("Running with args: %s", args)

    p = PGPlayspace()

    p.delete_table()
    p.create_table()
    p.add_data()

    p.basic_select_queries()

    p.terminate()


if __name__ == "__main__":
    parser = ArgumentParser(prog="posrgres", usage="TODO")
    parser.add_argument("--my_arg", required=False)

    args = parser.parse_args()
    main(args)
