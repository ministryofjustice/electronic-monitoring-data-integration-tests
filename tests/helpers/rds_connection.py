from abc import ABC, abstractmethod
import pyodbc
import logging
from contextlib import contextmanager
import pytest


class DatabaseConnection(ABC):
	"""Abstract base class defining the interface for a database connection.

	This class serves as a contract, ensuring that any concrete database
	connection class implements a standard set of methods for creating,
	closing, and managing connections and cursors.
	"""

	@abstractmethod
	def create_connection(self):
		"""Establishes and opens a connection to the database."""
		pass

	@abstractmethod
	def close_connection(self):
		"""Closes the connection to the database."""
		pass

	@abstractmethod
	@contextmanager
	def get_cursor(self):
		"""A context manager to safely acquire and release a database cursor.

		Yields:
		    A database cursor object for executing queries.
		"""
		pass


class ConnectToRDS(DatabaseConnection):
	"""Manages a connection to an RDS SQL Server instance using pyodbc.

	This class provides methods to connect, execute read/write queries, and
	manage the connection lifecycle for a Microsoft SQL Server database,
	hosted on AWS RDS.

	Attributes:
	    user (str): The username for the database connection.
	    password (str): The password for the database user.
	    server_endpoint (str): The server address for the RDS instance.
	    connection (pyodbc.Connection | None): The active pyodbc connection
	        object, or None if not connected.
	    cursor (pyodbc.Cursor | None): The active pyodbc cursor, or None if
	        not connected.
	"""

	def __init__(self, user: str, password: str, server_endpoint: str):
		"""Initializes the ConnectToRDS object with connection credentials.

		Args:
		    user (str): The username to connect to the RDS Instance.
		    password (str): The password for the specified user.
		    server_endpoint (str): The endpoint hostname of the RDS instance.
		"""
		self.user = user
		self.password = password
		self.server_endpoint = server_endpoint
		self.connection = None
		self.cursor = None

	def create_connection(self):
		"""Creates a connection to the RDS SQL Server using pyodbc."""
		self.connection = pyodbc.connect(
			driver='ODBC DRIVER 18 for SQL Server',
			server=self.server_endpoint,
			uid=self.user,
			pwd=self.password,
			TrustServerCertificate='yes',
		)
		self.cursor = self.connection.cursor()

	def close_connection(self):
		"""Safely closes the active cursor and connection."""
		if self.cursor:
			self.cursor.close()
		if self.connection:
			self.connection.close()
		logging.info('RDS connection and cursor closed.')

	@contextmanager
	def get_cursor(self):
		"""Context manager for acquiring a cursor and managing the connection.

		It creates a connection, yields a cursor for operations, commits the
		transaction upon successful exit, and ensures the connection is
		closed in all cases.

		Yields:
		    pyodbc.Cursor: A cursor object to execute queries.
		"""
		try:
			self.create_connection()
			cursor = self.connection.cursor()
			yield cursor
			self.connection.commit()
		finally:
			self.close_connection()

	def read_query(self, query: str) -> list:
		"""Executes a read query and fetches all results.

		Args:
		    query (str): The SQL SELECT statement to execute.

		Returns:
		    list: A list of rows, where each row is represented as a pyodbc.Row object.
		"""
		with self.get_cursor() as cursor:
			cursor.execute(query)
			return cursor.fetchall()

	def write_query(self, query: str):
		"""Executes a write query (e.g., INSERT, UPDATE, DELETE).

		Args:
		    query (str): The SQL statement to execute.
		"""
		with self.get_cursor() as cursor:
			cursor.execute(query)

	@pytest.fixture
	def remove_test_data(self):
		"""Pytest fixture to clean up data after a test completes.

		This fixture does nothing during setup but executes a cleanup
		SQL command during the teardown phase after 'yield'.
		"""
		yield
		with self.get_cursor() as cursor:
			cursor.execute('TBD based on test data upload')
