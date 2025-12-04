import sys
import os

imports_dirname = os.path.dirname(__file__)
main_dirname = os.path.dirname(imports_dirname)
sys.path.append(main_dirname)

from contextlib import contextmanager
import pyodbc
import datetime
try:
    import oracledb
except ImportError:
    print("Some required libraries are not installed. Installing them...")
    try:
        import subprocess

        # Path to the target Python executable
        python_exe = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"

        # Ensure the path exists
        if not os.path.exists(python_exe):
            print(f"Error: Python executable not found at '{python_exe}'")
            sys.exit(1)

        # Install oracledb using pip
        try:
            subprocess.check_call([python_exe, "-m", "pip", "install", "oracledb"])
            print("oracledb installed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to install oracledb. Error: {e}")
            sys.exit()
        print("Required libraries installed successfully.")
    except Exception as e:
        print(f"Failed to install required libraries: {e}")
        print(f"Process finish time: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        sys.exit(1)

import json
import logging
import logging.config
import psycopg2
from common.config import *
from typing import Union
# GenericCursor = pyodbc.Cursor | oracledb.Cursor
ORACLE_CLIENT_INITALIZED = False
# ConnectionType = Union[pyodbc.Connection, oracledb.Connection]

import arcpy


# with open('logging_config.json', 'r') as file:
#     config_dict = json.load(file)

# logging.config.dictConfig(config_dict)
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
    
def connect_to_gis(config: DBConfig) -> arcpy.da.UpdateCursor:
    edit = arcpy.da.Editor(postgres_SDE_path)
    try:
        arcpy.env.workspace = f"Database Connections/{DBConfig.db_name}.sde"
        arcpy.env.overwriteOutput = True
        edit.startEditing(with_undo=True, multiuser_mode=False)
        edit.startOperation()
        conn = arcpy.da.UpdateCursor(postgres_SDE_path, ['*'])
        logger.debug(f"Connected to postgresql {config.host}.{config.db_name} successfully")
        return conn
    except Exception as e:
        edit.stopOperation()
        edit.stopEditing(save_changes=False)
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