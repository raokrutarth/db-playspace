import logging
import os
import sys
import psycopg2
from random import randint, random
from multiprocessing import Process
from threading import Thread
from time import time, sleep
from datetime import datetime

# configure logging with filename, function name and line numbers
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "DEBUG"),
    datefmt='%I:%M:%S %p %Z',
    format='%(levelname)s [%(asctime)s - %(filename)s:%(lineno)s::%(funcName)s]\t%(message)s',
)
log = logging.getLogger()


class PGPlayspace:

    def __init__(self, host="postgres", port=5432, dbname="myPGDB", user="myPGUser", password="myPGPassword"):
        self.db_client = self.get_client(host, port, dbname, user, password)
        self.db_cursor = self.db_client.cursor()

    def get_client(self, host, port, dbname, user, password):
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
        self.db_client.commit()  # <--- makes sure the change is shown in the database
        # log.info(self.db_cursor.fetchall())

    def delete_table(self):
        try:
            self.db_cursor.execute("""
                    DROP TABLE cities;
                    DROP TABLE weather;
                """)
            self.db_client.commit()
        except:
            pass

    def add_data(self):
        try:
            for i in range(500):
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

                self.db_cursor.execute("""
                select * from cities;
                """)
                self.db_client.commit()  # <--- makes sure the change is shown in the database
            records = self.db_cursor.fetchall()
            return records if records else []
        except:
            pass
            return None


def main(args):
    log.info("Running with args: %s", args)

    def proc():
        def thr():
            x = 1
            pg_play = PGPlayspace()
            while 1:
                records = pg_play.add_data()
                x += 1
                if not x % (randint(5, 25) * 1000):
                    log.info("len of records: %d", len(records))
                    log.info(f"random record: {records[randint(0, len(records))]}")
        thr()

    p = PGPlayspace()
    p.delete_table()
    sleep(2)
    p.create_table()

    sleep(2)
    for _ in range(24):
        Process(target=proc, daemon=True).start()

    log.info("Started all processes")
    while True:
        sleep(30)


if __name__ == "__main__":
    main(sys.argv)
