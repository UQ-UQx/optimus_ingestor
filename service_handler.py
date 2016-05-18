"""
Deals with the webserver and the service modules
"""
import json
import warnings
import importlib
import MySQLdb
import os
import threading
import time

import utils
import mysqldb_ops
import config

logger = utils.getLogger(__name__)


class ServiceManager():
	"""
	Manages service modules
	"""

	servicethreads = []
	#servicemodules = []

	def __init__(self):
		logger.info("Starting Service Manager")
		self.setup_database()

	def load_services(self):
		"""
		Loads each module
		"""
		servicespath = os.path.join(utils.basepath, 'services')
		for servicename in os.listdir(servicespath):
			if servicename not in config.IGNORE_SERVICES:
				servicepath = os.path.join(servicespath, servicename, 'service.py')
				if os.path.exists(servicepath):
					logger.debug("Loading module: " + servicename)
					service_module = importlib.import_module('services.' + servicename + '.service')
					service_thread = threading.Thread(target=service_module.service, args=[self.servicethreads])
					service_thread.daemon = True
					service_thread.start()

		time.sleep(60)
		print 'aha?'
		print self.servicethreads


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


		# Table: courses
		sql = "CREATE TABLE IF NOT EXISTS courses ( "
		sql += "id int NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY, org varchar(255), course varchar(255), run varchar(255), site varchar(255), course_id varchar(255), ingest boolean default 1, api boolean default 1, dashboard boolean default 1"
		sql += ");"
		sql_arr.append(sql)


		# Records for the config table
		insert_arr = self.insert_config_table()
		sql_arr = sql_arr + insert_arr

		mysqldb_ops.execute_sql(sql_arr=sql_arr)
		logger.info('Table tasks, config, courses is ready')

		warnings.filterwarnings('always', category=MySQLdb.Warning)

	def insert_config_table(self):
		insert_arr = []

		# Ignored services
		sql = "INSERT INTO config (param_name, param_value) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE param_value='%s'" % ('IGNORE_SERVICES', json.dumps(config.IGNORE_SERVICES), json.dumps(config.IGNORE_SERVICES))
		insert_arr.append(sql)

		# Ignored courses
		sql = "INSERT INTO config (param_name, param_value) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE param_value='%s'" % ('IGNORE_COURSES_INGEST', json.dumps(config.IGNORE_COURSES_INGEST), json.dumps(config.IGNORE_COURSES_INGEST))
		insert_arr.append(sql)
		sql = "INSERT INTO config (param_name, param_value) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE param_value='%s'" % ('IGNORE_COURSES_API', json.dumps(config.IGNORE_COURSES_API), json.dumps(config.IGNORE_COURSES_API))
		insert_arr.append(sql)
		sql = "INSERT INTO config (param_name, param_value) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE param_value='%s'" % ('IGNORE_COURSES_DASHBOARD', json.dumps(config.IGNORE_COURSES_DASHBOARD), json.dumps(config.IGNORE_COURSES_DASHBOARD))
		insert_arr.append(sql)

		return insert_arr

class Servicehandler():
	"""
	Service handler deals with the threaded nature of the application
	"""
	manager = None

	_instance = None

	# Make sure singleton
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(Servicehandler, cls).__new__(cls, *args, **kwargs)
		return cls._instance

	def __init__(self):
		self.manager = ServiceManager()
		self.manager.load_services()


