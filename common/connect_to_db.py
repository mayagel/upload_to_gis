import sys
import os

imports_dirname = os.path.dirname(__file__)
main_dirname = os.path.dirname(imports_dirname)
sys.path.append(main_dirname)

from contextlib import contextmanager
import pyodbc
import oracledb
import json
import logging
import logging.config
import psycopg2
from common.config import *
from typing import Union
GenericCursor = pyodbc.Cursor | oracledb.Cursor
ORACLE_CLIENT_INITALIZED = False
ConnectionType = Union[pyodbc.Connection, oracledb.Connection]


with open('logging_config.json', 'r') as file:
    config_dict = json.load(file)

logging.config.dictConfig(config_dict)
logger = logging.getLogger(__name__)

def connect_to_sql_server(config: DBConfig):

    connstr = f"Driver={config.driver};Server={config.host};Database={config.db_name};"

    if os.name == 'nt' and not config.user and not config.password:
        connstr += 'Trusted_Connection=yes'
    else:
        connstr += f"UID={config.user};PWD={config.password}"

    if os.name != 'nt':
        connstr += ';TrustServerCertificate=Yes'

    try:
        conn = pyodbc.connect(connstr)
        logger.debug(f"Connected to sql {config.host}.{config.db_name} successfully")
        return conn
    except Exception as e:
        logger.exception(f"Error while connecting to sql db {e}")
        raise

def connect_to_oracle(config) -> oracledb.Connection:
    global ORACLE_CLIENT_INITALIZED
    
    if not ORACLE_CLIENT_INITALIZED:
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LOCATION)
        ORACLE_CLIENT_INITALIZED = True

    try:
        conn = oracledb.connect(
            user=config.user,
            password=config.password,
            dsn=config.dsn
        )
        logger.debug(f"Connected to oracle {config.dsn} successfully")
        return conn
    except Exception as e:
        logger.exception(f"Error while connecting to oracle db {e}")
        raise
    
def connect_to_gis(config: DBConfig) -> psycopg2.extensions.connection:
    try:
        conn = psycopg2.connect(
            host=config.host,
            user=config.user,
            dbname=config.db_name,
            password=config.password,
            port=config.port
        )
        logger.debug(f"Connected to postgresql {config.host}.{config.db_name} successfully")
        return conn
    except Exception as e:
        logger.exception(f"Error while connecting to GIS db {e}")
        raise
 
def connect_to_postgres(config: DBConfig) -> psycopg2.extensions.connection:

    try:
        conn = psycopg2.connect(
            host=config.host,
            user=config.user,
            dbname=config.db_name,
            password=config.password,
            port=config.port
        )
        logger.debug(f"Connected to postgresql {config.host}.{config.db_name} successfully")
        return conn
    except Exception as e:
        logger.exception(f"Error while connecting to postgresql db {e}")
        raise

@contextmanager
def transactional_cursor(conn: pyodbc.Connection | psycopg2.extensions.connection):
    # A transactional cursor that rolls back on exception. Use as follows:
    # with transactional_cusror(conn) as cursor:
    #    cursor.execute(......)
    #    cursor.execute(......)
    #
    # Everything will be wrapped in a transaction which will be committed if no exception is raised, and rolled back otherwise.
    with conn.cursor() as cursor:
        try:
            cursor.execute("BEGIN TRANSACTION")
            yield cursor
            cursor.execute("COMMIT TRANSACTION")
        except Exception:
            cursor.execute("ROLLBACK TRANSACTION")
            raise