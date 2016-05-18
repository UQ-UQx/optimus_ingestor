"""
"""
import os
import shutil


import base_service
import utils
import mysqldb_ops

logger = utils.getLogger(__name__)

class CourseList(base_service.BaseService):

	def __init__(self, servicethreads):
		super(CourseList, self).__init__(servicethreads)

		self.enabled = True
		self.outputdir = 'www/course_structure'

		self.initialize()

	def run(self):
		print 'run...'
		# Get all course structure files 
		cs_info = self.get_course_structure_files()
		
		# Get all courses form DB
		query = "SELECT * FROM courses"
		cur = mysqldb_ops.execute_query(sql=query)
		db_cs = []
		db_string = []
		for row in cur.fetchall():
			db_cs.append(row)
			db_string.append(row['org'] + '-' + row['course'] + '-' + row['run'] + '-course_structure-' + row['site'] + '-analytics.json')
		
		# Find courses not in db
		cs_not_in_db = list(set(cs_info['cs_files']) - set(db_string))
		if len(cs_not_in_db) > 0:
			logger.warning("New courses are finded")

			# Write into the table {courses}
			sql_arr = []
			for course in cs_not_in_db:
				logger.warning(course + " will be added into the table {courses}")
				meta = course.split("-")
				org = meta[0]
				course = meta[1]
				run = meta[2]
				site = meta[4]
				if len(meta) == 7:
					site = meta[4] + '-' + meta[5]
				query = "INSERT INTO courses (org, course, run, site) VALUES ('%s', '%s', '%s', '%s')" % (org, course, run, site)
				sql_arr.append(query)
			mysqldb_ops.execute_sql(sql_arr=sql_arr)

		"""
		# Find courses not in 'cs_files'
		cs_not_in_files = list(set(db_string) - set(cs_info['cs_files']))
		# Update the table {courses} set the field 'ingest' to 0
		update_arr = []
		for course in cs_not_in_files:
			meta = course.split("-")
			org = meta[0]
			course = meta[1]
			run = meta[2]
			site = meta[4]
			if len(meta) == 7:
				site = meta[4] + '-' + meta[5]
			query = "UPDATE courses SET ingest = 0 WHERE org = '%s' AND course = '%s' AND run = '%s' AND site = '%s'" % (org, course, run, site)
			update_arr.append(query)
		mysqldb_ops.execute_sql(sql_arr=update_arr)
		"""
		pass

	def setup(self):
		pass

	def get_course_structure_files(self):
		required_files = []

		datapath = self.get_data_path()
		mainpath = os.path.realpath(os.path.join(datapath, 'database_state', 'latest'))
		for file in os.listdir(mainpath):
			if os.path.isfile(os.path.join(mainpath, file)) and file.endswith("-analytics.json"):
				required_files.append(file)

		return {'mainpath': mainpath, 'cs_files': required_files}


def service(servicethreads):
	"""
	Returns an instance of the service
	"""
	return CourseList(servicethreads)

