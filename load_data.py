import csv
import codecs
from datetime import date
import logging
import os
import sys

import psycopg2
from psycopg2 import sql
import requests


# The log level makes sure we can see logger.info messages, by default we only see errors and debug messages
# The format gives us an informative message with a timestamp, namespace and log level
# The handlers allow us to log to both a specified file and STDOUT
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    handlers=[
                        logging.FileHandler("data_to_pg.log"),
                        logging.StreamHandler(sys.stdout)
                    ])
logger = logging.getLogger(__name__)



filename = f'covid_deaths_{date.today()}.csv' # easy to debug
url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/excess-deaths/deaths.csv'
table = 'covid_deaths'

def download_data(filename, url):
    logger.info("Starting to download data into %s", filename)

    r = requests.get(url, stream = True)
    try:
        r.raise_for_status() # check if an error has occurred
    except requests.exceptions.HTTPError as e:
        logger.exception('Unable to connect to url %s', url)
        raise

    counter = 0

    with open (filename, 'w') as f:
        # note that requests.response.iter_lines somtetimes returns bytestrings
        # codecs.iterdecode to decode the bytestrings
        response_iter = codecs.iterdecode(r.iter_lines(), 'utf-8')
        for row in response_iter:
            f.write(row + '\n') # new line needed to be manually added
            counter += 1

    logger.info('Successfully downloaded data of %i rows into %s', counter, filename)



def load_into_pg(filename, table):
    logger.info('Starting to load data from %s into PostgreSQL table %s', filename, table)

    try:
        with psycopg2.connect(user = os.environ["PGUSER"], password = os.environ["PGPASSWORD"], host = os.environ["PGHOST"],\
                            port = os.environ["PGPORT"], database = os.environ["PGDBNAME"]) as conn,\
                            conn.cursor() as cursor, \
                                open(filename) as f:
                            # ignore first line (header)
                            f.readline()
                            # only load latest data
                            cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
                            cursor.copy_from(f, table, sep = ',', null = '')
    except psycopg2.Error as e:
        logger.exception('Something wrong with the loading %s into postgres', filename)
        raise

    logger.info('Successfully loaded file %s into table %s', filename, table)


def my_handler(type, value, tb):
    logger.exception("Uncaught exception: %s", value)

# Install exception handler
sys.excepthook = my_handler


def main():
    download_data(filename, url)
    load_into_pg(filename, table)


if __name__ == '__main__':
    main()
