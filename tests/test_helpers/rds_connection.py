from abc import ABC, abstractmethod
import pyodbc
import logging
from contextlib import contextmanager


class DatabaseConnection(ABC):
    @abstractmethod
    def create_connection(self):
        pass

    @abstractmethod
    def close_connection(self):
        pass
    
    @abstractmethod
    @contextmanager
    def get_cursor(self):
        """Context manager to get cursor and handle open/close."""
        pass

class ConnectToRDS(DatabaseConnection):
    """Connect read and execute queries on RDS.

    Args:
        DatabaseConnection (_type_): _description_
        user (str): User to connect to the RDS Instance
        password (str): Password for the RDS instance
        host (str): tunnel host name
        port (str): port opened to communicate to the RDS instance.
    """
    def __init__(self, user:str, password: str, host_name: str, port: str):
        self.user = user
        self.password = password
        self.host_name = host_name
        self.port = port
        self.conn_string = ("DRIVER={ODBC DRIVER 18 for SQL Server};"
                            f"SERVER={self.host_name,self.port};"
                            f"UID={self.user};"
                            f"PWD={self.password}")
        self.connection = None
        self.cursor = None

    def create_connection(self):
        self.connection = pyodbc.connect(self.conn_string)
        self.cursor = self.connection.cursor()

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logging.info("RDS connection and cursor closed.")

    
    @contextmanager
    def get_cursor(self):
        try:
            self.create_connection()
            cursor = self.connection.cursor()
            yield cursor
            self.connection.commit()
            cursor.close()
        finally:
            self.close_connection()

    def read_query(self, query):
        with self.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def write_query(self, query, params=None):
        with self.get_cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
