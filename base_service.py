
import json
import time
import os
import signal

import utils
import mysqldb_ops
import config


logger = utils.getLogger(__name__)
ALIVE = True


def exit_handler(signal, frame):
	"""
	Sets the variable ALIVE to false, killing threads
	:param signal: the signal
	:param frame: the frame
	"""
	global ALIVE
	ALIVE = False

signal.signal(signal.SIGINT, exit_handler)


class BaseService(object):
	"""
	The base service which all services extend
	"""

	def __init__(self, servicethreads):
		servicethreads.append(self)

		#Auto variables
		#The lowercase name of the service
		self.servicename = ""
		#The current state of the service
		self.status = 'stopped'
		#The last time the service was running
		self.last_awake = ''

		#Overriden variables
		#Whether the service is enabled
		self.enabled = False
		#Whether to run more than once
		self.loop = False
		#The amount of time to sleep in seconds
		self.sleep_time = 60


	# Starting service
	def initialize(self):
		"""
		Sets default status and begins the run loop
		"""
		if self.enabled:
			self.servicename = self.get_name()
			logger.info("Starting service " + self.servicename)
			self.status = 'loading'
			self.setup()
			while self.loop and ALIVE:
				self.status = 'running'
				self.run()
				self.status = 'sleeping'
				self.last_awake = time.strftime('%Y-%m-%d %H:%M:%S')
				time.sleep(self.sleep_time)

	
	@classmethod
	def get_name(cls):
		return cls.__name__

	def setup(self):
		"""
		Setup function, this should be overridden by the service
		"""
		logger.error("BAD METHOD, SETUP SHOULD BE SUBCLASSED IN "+self.servicename)

	def run(self):
		"""
		Run function, this should be overridden by the service
		"""
		logger.error("BAD METHOD, SETUP SHOULD BE SUBCLASSED IN "+self.servicename)

	def add_to_tasks(self, param_dict):
		"""
		Inserts a row into the {tasks} table
		:param param_dict: a dictionary of parameters
		"""
		key_list = param_dict.keys()
		if not ({'service_name', 'type', 'meta'} <= set(key_list)):
			logger.error("Lack of necessary fields in 'add_to_tasks'")

		if 'dependent' in key_list:
			param_dict['dependent'] = json.dumps(param_dict['dependent'])
		else:
			param_dict['dependent'] = 'NULL'
		if 'prepare_id' not in key_list:
			param_dict['prepare_id'] = 0
		if 'dependent' not in key_list:
			param_dict['dependent'] = ''
		if 'started' not in key_list:
			param_dict['started'] = 0
		if 'completed' not in key_list:
			param_dict['completed'] = 0
		if 'started_date' not in key_list:
			param_dict['started_date'] = 'NULL'
		else:
			param_dict['started_date'] = "'" + param_dict['started_date'] + "'"
		if 'completed_date' not in key_list:
			param_dict['completed_date'] = 'NULL'
		else:
			param_dict['completed_date'] = "'" + param_dict['completed_date'] + "'"

		query = "INSERT INTO tasks (service_name, type, meta, prepare_id, dependent, started, completed, started_date, completed_date) " \
		"VALUES ('%(service_name)s', '%(type)s', '%(meta)s', %(prepare_id)d, '%(dependent)s', %(started)d, %(completed)d, %(started_date)s, %(completed_date)s)" \
		% param_dict

		#mysqldb_ops.execute_sql(sql=query)
		return query


	@staticmethod
	def get_data_path():
		return os.path.realpath(config.DATA_PATH)

	@staticmethod
	def get_collection_name(logfile_name):
	    return logfile_name[:-4]		


	def get_existing_tasks(self, service_name):
		"""
		Return all existing tasks with service_name or the self class name 
		:param service_name: The name of the service to check. If not provided, the name of self class will be used
		:return: The records of tasks satisfying conditions
		"""
		if service_name == None:
			service_name = self.get_name()

		query = "SELECT * FROM tasks WHERE service_name = '%s' " % service_name
		cur = mysqldb_ops.execute_query(sql=query)
		rows = cur.fetchall()
		mysqldb_ops.close_cur(cur)
		return rows



	def get_not_start_tasks(self, service_name=None):
		"""
		Retrieves the tasks not started for the service_name
		""" 
		if service_name == None:
			service_name = str(self.__class__.__name__)

		query = "SELECT * FROM %s WHERE service_name = '%s' AND started = 0 ORDER BY created ASC" % ('tasks', service_name)
		cur = mysqldb_ops.execute_query(sql=query)
		tasks = []
		for row in cur.fetchall():
			task = {}
			task['id'] = row[0]
			task['type'] = row[2]
			task['meta'] = row[3]
			tasks.append(task)
		mysqldb_ops.close_cur(cur)
		return tasks


	def is_service_finished(self, service_name):
		"""
		Checks whether the service is running or not
		:param service_name: The name of the service to check
		:return: True or False depending on whether the ingestion is finished
		"""
		query = "SELECT * FROM %s WHERE service_name = '%s' AND completed = 0" % ('tasks', service_name) 
		cur = mysqldb_ops.execute_query(sql=query)
		finished = True
		if cur and cur.rowcount:
			finished = False	
		mysqldb_ops.close_cur(cur)
		return finished

	def find_last_run(self, service_name):
		"""
		Finds the date of the last time the service ran
		:param service_name: The name of the service to find
		:return: The date of the last run
		"""
		query = "SELECT created FROM %s WHERE service_name = '%s' AND started = 1 AND completed = 1  ORDER BY created DESC limit 1" % ('tasks', service_name)
		cur = mysqldb_ops.execute_query(sql=query)
		row = cur.fetchone()
		date = row[0]
		mysqldb_ops.close_cur(cur)
		return date



	def get_start_not_complete_tasks(self, service_name=None):
		"""
		Retrieves the tasks started but not completed for this service_name
		"""
		if service_name == None:
			service_name = str(self.__class__.__name__)

		query = "SELECT * FROM %s WHERE service_name = '%s' AND started = 1 AND completed = 0 ORDER BY created ASC" % ('tasks', service_name)
		cur = mysqldb_ops.execute_query(sql=query)
		tasks = []
		for row in cur.fetchall():
			task = {}
			task['id'] = row[0]
			task['type'] = row[2]
			task['meta'] = row[3]
			tasks.append(task)
		mysqldb_ops.close_cur(cur)
		return tasks

	def get_completed_tasks(self, service_name=None):
		"""
		Retrieves the tasks already completed for this service_name
		"""
		if service_name == None:
			service_name = str(self.__class__.__name__)

		query = "SELECT * FROM %s WHERE service_name = '%s' AND completed = 1 ORDER BY created ASC" % ('tasks', service_name)
		cur = mysqldb_ops.execute_query(sql=query)
		tasks = []
		for row in cur.fetchall():
			task = {}
			task['id'] = row[0]
			task['type'] = row[2]
			task['meta'] = row[3]
			tasks.append(task)
		mysqldb_ops.close_cur(cur)
		return tasks
	

	@staticmethod
	def start_task(task_id):
		"""
		Starts an task entry
		:param task_id: the ID of the task entry
		"""
		current_date = time.strftime('%Y-%m-%d %H:%M:%S')
		query = "UPDATE %s SET started=1, started_date='%s' WHERE id=%d" % ('tasks', current_date, task_id)
		mysqldb_ops.execute_sql(sql=query)


	@staticmethod
	def finish_task(task_id):
		"""
		finish an task entry
		:param task_id: the ID of the task entry
		"""
		current_date = time.strftime('%Y-%m-%d %H:%M:%S')
		query = "UPDATE %s SET completed=1, completed='%s' WHERE id=%d" % ('tasks', current_date, task_id)
		mysqldb_ops.execute_sql(sql=query)

	@staticmethod
	def delete_task(task_id):
		"""
		delete an task entry
		:param task_id: the ID of the task entry
		"""
		query = "DELETE FROM %s WHERE id=%d" % ('tasks', task_id)
		mysqldb_ops.execute_sql(sql=query)









