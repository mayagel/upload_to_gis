"""
Database Operations Handler

This module provides common database operations for managing tables, data, and schema operations.
All database operations are centralized here for better maintainability and reusability.
"""

import logging
from typing import List, Dict, Optional
from .connect_to_db import connect_to_postgres, connect_to_gis, connect_to_sql_server, transactional_cursor
from .config import CENTRAL_CATALOG_PG_CONFIG, GIS_CONFIG, IAA_SQLPROD03_CONFIG

logger = logging.getLogger(__name__)


class DatabaseOperations:
    """Handles common database operations for table management and data manipulation."""
    
    def __init__(self, config: dict = None, connection_type: str = "postgres"):
        """
        Initialize DatabaseOperations with database configuration.
        
        Args:
            config (dict, optional): Database configuration. Defaults to CENTRAL_CATALOG_PG_CONFIG.
            connection_type (str): Type of connection - "postgres", "gis", or "sql_server"
        """
        self.connection_type = connection_type
        if connection_type == "postgres":
            self.config = config or CENTRAL_CATALOG_PG_CONFIG
        elif connection_type == "gis":
            self.config = config or GIS_CONFIG
        elif connection_type == "sql_server":
            self.config = config or IAA_SQLPROD03_CONFIG
        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")
        
        # Establish connection once during initialization
        self._connection = None
        self._establish_connection()
    
    def _establish_connection(self):
        """Establish the database connection based on connection type."""
        try:
            if self.connection_type == "postgres":
                self._connection = connect_to_postgres(self.config)
            elif self.connection_type == "gis":
                self._connection = connect_to_gis(self.config)
            elif self.connection_type == "sql_server":
                self._connection = connect_to_sql_server(self.config)
            
            logger.info(f"Successfully established {self.connection_type} connection")
        except Exception as e:
            logger.exception(f"Error establishing {self.connection_type} connection: {e}")
            raise
    
    def _get_connection(self):
        """Get the established database connection, reconnect if needed."""
        try:
            # Check if connection is still alive
            if self._connection and hasattr(self._connection, 'closed') and not self._connection.closed:
                return self._connection
            else:
                # Reconnect if connection was closed
                logger.info(f"Reconnecting to {self.connection_type} database")
                self._establish_connection()
                return self._connection
        except Exception as e:
            logger.exception(f"Error getting connection: {e}")
            # Try to reconnect
            self._establish_connection()
            return self._connection
    
    def close_connection(self):
        """Close the database connection."""
        if self._connection:
            try:
                if self.connection_type == "gis":
                    self._connection.stopOperation()
                    self._connection.stopEditing(save_changes=True)
                self._connection.close()
                logger.info(f"Closed {self.connection_type} connection")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close_connection()
    
    def delete_table(self, scheme_name: str, table_name: str) -> None:
        """
        Delete a table from the database.
        
        Args:
            scheme_name (str): The schema name where the table exists
            table_name (str): The name of the table to delete
        """
        logger.info(f"start delete_table for {scheme_name}.{table_name}")
        
        delete_table_sql = f"""
        DROP TABLE IF EXISTS {scheme_name}.{table_name} CASCADE;
        """
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(delete_table_sql)
                conn.commit()
                logger.info(f"Successfully deleted table {scheme_name}.{table_name}")
        except Exception as e:
            logger.exception(f"Error deleting table {scheme_name}.{table_name}: {e}")
            raise
    
    def create_table(self, scheme_name: str, table_name: str, create_sql: str) -> None:
        """
        Create a table in the database.
        
        Args:
            scheme_name (str): The schema name where to create the table
            table_name (str): The name of the table to create
            create_sql (str): The CREATE TABLE SQL statement
        """
        logger.info(f"start create_table for {scheme_name}.{table_name}")
        
        # If create_sql is empty, table already exists and is valid
        if not create_sql.strip():
            logger.info(f"Table {scheme_name}.{table_name} already exists with correct schema")
            return
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(create_sql)
                conn.commit()
                logger.info(f"Successfully created table {scheme_name}.{table_name}")
        except Exception as e:
            logger.exception(f"Error creating table {scheme_name}.{table_name}: {e}")
            raise
    
    def copy_table_data(self, source_scheme: str, source_table: str, target_scheme: str, target_table: str, 
                       columns: Optional[List[str]] = None, where_clause: Optional[str] = None) -> None:
        """
        Copy data from one table to another.
        
        Args:
            source_scheme (str): Source table schema name
            source_table (str): Source table name
            target_scheme (str): Target table schema name
            target_table (str): Target table name
            columns (List[str], optional): List of columns to copy. If None, copies all columns
            where_clause (str, optional): WHERE clause to filter data. If None, copies all data
        """
        logger.info(f"start copy_table_data from {source_scheme}.{source_table} to {target_scheme}.{target_table}")
        
        # Build column list
        if columns:
            columns_str = ", ".join(columns)
            select_columns = columns_str
            insert_columns = columns_str
        else:
            select_columns = "*"
            insert_columns = ""  # Will be handled by INSERT INTO table_name (no columns specified)
        
        # Build the SQL query
        if columns:
            copy_sql = f"""
            INSERT INTO {target_scheme}.{target_table} ({insert_columns})
            SELECT {select_columns}
            FROM {source_scheme}.{source_table}
            """
        else:
            copy_sql = f"""
            INSERT INTO {target_scheme}.{target_table}
            SELECT {select_columns}
            FROM {source_scheme}.{source_table}
            """
        
        # Add WHERE clause if specified
        if where_clause:
            copy_sql += f" WHERE {where_clause}"
        
        copy_sql += ";"
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(copy_sql)
                conn.commit()
                logger.info(f"Successfully copied data from {source_scheme}.{source_table} to {target_scheme}.{target_table}")
        except Exception as e:
            logger.exception(f"Error copying data from {source_scheme}.{source_table} to {target_scheme}.{target_table}: {e}")
            raise
    
    def copy_table_data_with_timestamp(self, source_scheme: str, source_table: str, target_scheme: str, target_table: str,
                                     timestamp_columns: Optional[Dict[str, str]] = None,
                                     columns: Optional[List[str]] = None,
                                     where_clause: Optional[str] = None) -> None:
        """
        Copy data from one table to another with timestamp columns.
        
        Args:
            source_scheme (str): Source table schema name
            source_table (str): Source table name
            target_scheme (str): Target table schema name
            target_table (str): Target table name
            timestamp_columns (Dict[str, str], optional): Mapping of column names to timestamp values
            columns (List[str], optional): List of columns to copy. If None, copies all columns
            where_clause (str, optional): WHERE clause to filter data. If None, copies all data
        """
        logger.info(f"start copy_table_data_with_timestamp from {source_scheme}.{source_table} to {target_scheme}.{target_table}")
        
        # Build column list
        if columns:
            columns_str = ", ".join(columns)
            select_columns = columns_str
            insert_columns = columns_str
        else:
            select_columns = "*"
            insert_columns = ""
        
        # Build the SQL query
        if columns:
            copy_sql = f"""
            INSERT INTO {target_scheme}.{target_table} ({insert_columns})
            SELECT {select_columns}
            FROM {source_scheme}.{source_table}
            """
        else:
            copy_sql = f"""
            INSERT INTO {target_scheme}.{target_table}
            SELECT {select_columns}
            FROM {source_scheme}.{source_table}
            """
        
        # Add WHERE clause if specified
        if where_clause:
            copy_sql += f" WHERE {where_clause}"
        
        copy_sql += ";"
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(copy_sql)
                conn.commit()
                logger.info(f"Successfully copied data with timestamp from {source_scheme}.{source_table} to {target_scheme}.{target_table}")
        except Exception as e:
            logger.exception(f"Error copying data with timestamp from {source_scheme}.{source_table} to {target_scheme}.{target_table}: {e}")
            raise
    
    def clear_table_data(self, scheme_name: str, table_name: str) -> None:
        """
        Clear all data from a table.
        
        Args:
            scheme_name (str): The schema name where the table exists
            table_name (str): The name of the table to clear
        """
        logger.info(f"start clear_table_data for {scheme_name}.{table_name}")
        
        clear_table_sql = f"""
        DELETE FROM {scheme_name}.{table_name};
        """
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(clear_table_sql)
                conn.commit()
                logger.info(f"Successfully cleared data from {scheme_name}.{table_name}")
        except Exception as e:
            logger.exception(f"Error clearing data from {scheme_name}.{table_name}: {e}")
            raise
    
    def execute_sql(self, sql: str, params: Optional[Dict] = None) -> None:
        """
        Execute a custom SQL statement.
        
        Args:
            sql (str): The SQL statement to execute
            params (Dict, optional): Parameters for the SQL statement
        """
        # logger.info(f"start execute_sql: {sql[:100]}...")
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                conn.commit()
                # logger.info("Successfully executed SQL statement")
        except Exception as e:
            logger.exception(f"Error executing SQL statement: {e}")
            raise
    
    def execute_query(self, sql: str, params: Optional[Dict] = None):
        """
        Execute a query and return results.
        
        Args:
            sql (str): The SQL query to execute
            params (Dict, optional): Parameters for the SQL query
            
        Returns:
            List: Query results
        """
        # logger.info(f"start execute_query: {sql[:100]}...")
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                results = cursor.fetchall()
                # logger.info(f"Successfully executed query, returned {len(results)} rows")
                return results
        except Exception as e:
            logger.exception(f"Error executing query: {e}")
            raise
    
    def execute_gis_query(self, sql: str, params: Optional[Dict] = None):
        """
        Execute a query on GIS database and return results.
        
        Args:
            sql (str): The SQL query to execute
            params (Dict, optional): Parameters for the SQL query
            
        Returns:
            List: Query results
        """
        # logger.info(f"start execute_gis_query: {sql[:100]}...")
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                results = cursor.fetchall()
                # logger.info(f"Successfully executed GIS query, returned {len(results)} rows")
                return results
        except Exception as e:
            logger.exception(f"Error executing GIS query: {e}")
            raise
    
    def execute_sql_server_query(self, sql: str, params: Optional[Dict] = None):
        """
        Execute a query on SQL Server database and return results.
        
        Args:
            sql (str): The SQL query to execute
            params (Dict, optional): Parameters for the SQL query
            
        Returns:
            List: Query results
        """
        # logger.info(f"start execute_sql_server_query: {sql[:100]}...")
        
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                results = cursor.fetchall()
                # logger.info(f"Successfully executed SQL Server query, returned {len(results)} rows")
                return results
        except Exception as e:
            logger.exception(f"Error executing SQL Server query: {e}")
            raise
    
    def get_sql_from_columns_names(self, table_name: str, field_types: dict, scheme_name='org_structure') -> str:
        """
        Dynamically create table schema based on the fields detected in the data.
        
        Args:
            table_name (str): Name of the table to create
            sample_data (list): List of objects to analyze for fields
            table_type (str): Type of table - "main" or "archive"
            
        Returns:
            str: CREATE TABLE SQL statement
        """
        if not field_types:
            raise ValueError("No columns names provided to create table schema")
        
        # Check if table exists and validate schema
        try:
            schema_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s
            AND table_name = %s 
            ORDER BY ordinal_position
            """
            
            existing_columns = [row[0] for row in self.execute_query(schema_query, (table_name, scheme_name))]
            
            if existing_columns:
                # Table exists, validate schema
                if [s.lower() for s in existing_columns] != [s.lower() for s in field_types.keys()]:
                    raise ValueError(f"Table {table_name} schema mismatch! Expected: {field_types.keys()}, Found: {existing_columns}")
                logger.info(f"Table {table_name} already exists with correct schema")
                return ""  # Return empty string since table exists and is valid
        except Exception as e:
            if "schema mismatch" in str(e):
                raise
            # If other error (like table doesn't exist), continue to create table
        
        # Build CREATE TABLE SQL
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {scheme_name}.{table_name} (
        """
        
        field_definitions = []
        for field in field_types.keys():
            field_definitions.append(f"    {field} {field_types[field]}")
        
        create_sql += ",\n".join(field_definitions)
        create_sql += "\n);"
        
        logger.info(f"Generated dynamic table schema for {table_name} with fields: {field_types.keys()}")
        return create_sql

    def copy_table_data_cross_db(self, source_config: dict, source_scheme: str, source_table: str, 
                                target_config: dict, target_scheme: str, target_table: str,
                                columns: Optional[List[str]] = None, where_clause: Optional[str] = None) -> None:
        """
        Copy data from one database to another database.
        
        Args:
            source_config (dict): Source database configuration
            source_scheme (str): Source table schema name
            source_table (str): Source table name
            target_config (dict): Target database configuration
            target_scheme (str): Target table schema name
            target_table (str): Target table name
            columns (List[str], optional): List of columns to copy. If None, copies all columns
            where_clause (str, optional): WHERE clause to filter data. If None, copies all data
        """
        logger.info(f"start copy_table_data_cross_db from {source_config.get('database', 'unknown')}.{source_scheme}.{source_table} to {target_config.get('database', 'unknown')}.{target_scheme}.{target_table}")
        
        try:
            # Connect to source database
            source_conn = connect_to_postgres(source_config)
            target_conn = connect_to_postgres(target_config)
            
            try:
                # Build column list
                if columns:
                    columns_str = ", ".join(columns)
                    select_columns = columns_str
                    insert_columns = columns_str
                else:
                    select_columns = "*"
                    insert_columns = ""
                
                # Build the SELECT query
                select_sql = f"SELECT {select_columns} FROM {source_scheme}.{source_table}"
                if where_clause:
                    select_sql += f" WHERE {where_clause}"
                select_sql += ";"
                
                # Execute SELECT on source database
                with source_conn.cursor() as source_cursor:
                    source_cursor.execute(select_sql)
                    rows = source_cursor.fetchall()
                    column_names = [desc[0] for desc in source_cursor.description] if not columns else columns
                
                if not rows:
                    logger.info("No data to copy")
                    return
                
                # Build INSERT query for target database
                if columns:
                    insert_sql = f"INSERT INTO {target_scheme}.{target_table} ({insert_columns}) VALUES ({', '.join(['%s'] * len(columns))})"
                else:
                    insert_sql = f"INSERT INTO {target_scheme}.{target_table} VALUES ({', '.join(['%s'] * len(column_names))})"
                
                # Execute INSERT on target database
                with target_conn.cursor() as target_cursor:
                    target_cursor.executemany(insert_sql, rows)
                    target_conn.commit()
                
                logger.info(f"Successfully copied {len(rows)} rows from {source_scheme}.{source_table} to {target_scheme}.{target_table}")
                
            finally:
                source_conn.close()
                target_conn.close()
                
        except Exception as e:
            logger.exception(f"Error copying data across databases: {e}")
            raise
