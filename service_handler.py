"""
Deals with the webserver and the service modules
"""
import json
import warnings
import MySQLdb

import utils
import mysqldb_ops
import config

logger = utils.getLogger(__name__)


class ServiceManager():
	"""
	Manages service modules
	"""

	servicethreads = []
	servicemodules = []

	def __init__(self):
		logger.info("Starting Service Manager")
		self.setup_database()

	def setup_database(self):
		"""
		Ensures that the required DB and tables are ready
		"""
		warnings.filterwarnings('ignore', category=MySQLdb.Warning)

		# INGESTOR_DB
		sql = 'CREATE DATABASE IF NOT EXISTS ' + mysqldb_ops.INGESTOR_DB
		mysqldb_ops.execute_sql(sql=sql)
		logger.info('INGESTOR_DB is ready')

		sql_arr = []
		# Table: tasks 
		sql = "CREATE TABLE IF NOT EXISTS tasks ( "
		sql += "id int NOT NULL UNIQUE AUTO_INCREMENT, service_name varchar(255), type varchar(255), meta varchar(255), started int DEFAULT 0, completed int DEFAULT 0, created datetime NULL, started_date datetime NULL, completed_date datetime NULL, PRIMARY KEY (id)"
		sql += ");"
		sql_arr.append(sql)

		# Table: config
		sql = "CREATE TABLE IF NOT EXISTS config ( "
		sql += "id int NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY, param_name varchar(255), param_value varchar(1027), UNIQUE KEY name(param_name)"
		sql += ");"
		sql_arr.append(sql)

		# Records for the config table
		insert_arr = self.insert_config_table()
		sql_arr = sql_arr + insert_arr

		mysqldb_ops.execute_sql(sql_arr=sql_arr)
		logger.info('Table tasks & config is ready')

		warnings.filterwarnings('always', category=MySQLdb.Warning)

	def insert_config_table(self):
		insert_arr = []
		sql = "INSERT INTO config (param_name, param_value) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE param_value='%s'" % ('IGNORE_SERVICES', json.dumps(config.IGNORE_SERVICES), json.dumps(config.IGNORE_SERVICES))
		insert_arr.append(sql)
		return insert_arr





class Servicehandler():
	"""
	Service handler deals with the threaded nature of the application
	"""
	manager = None

	# Creates a singleton
	_instance = None

	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(Servicehandler, cls).__new__(cls, *args, **kwargs)
		return cls._instance

	def __init__(self):
		self.manager = ServiceManager()


