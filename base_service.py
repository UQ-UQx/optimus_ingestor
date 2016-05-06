
import utils
import mysqldb_ops

ALIVE = True


logger = utils.getLogger(__name__)


class BaseService(object):
	"""
	The base service which all services extend
	"""

	def __init__(self):
		#Auto variables
		#The lowercase name of the service
		self.servicename = ""
		#The current state of the service
		self.status = 'stopped'
		#The last time the service was running
		self.last_awake = ''
		#The database for doing ingestion calls (service specific)
		self.sql_db = None
		#The current task
		self.task = ""
		#The current progress
		self.task_progress = 0
		#The amount of total progress for the current task
		self.task_progress_total = 0

		#Overriden variables
		#The pretty name of the service
		self.pretty_name = "Unknown Service"
		#Whether the service is enabled
		self.enabled = False
		#Whether to run more than once
		self.loop = True
		#The amount of time to sleep in seconds
		self.sleep_time = 60

	# Starting service
	def initialize(self):
		"""
		Sets default status and begins the run loop
		"""
		if self.enabled:
			self.servicename = str(self.__class__.__name__).lower()
			logger.info("Starting service " + self.servicename)
			self.status = 'loading'
			self.setup()
			while self.loop and ALIVE
				self.status = 'running'
				self.run()
				self.status = 'sleeping'
				self.last_awake = time.strftime('%Y-%m-%d %H:%M:%S')
				time.sleep(self.sleep_time)

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









