"""
"""
import os
import shutil
import warnings
import time
import fnmatch


import base_service
import utils
import mysqldb_ops
import config

logger = utils.getLogger(__name__)

class PrepareIngest(base_service.BaseService):

    def __init__(self, servicethreads):
        super(PrepareIngest, self).__init__(servicethreads)

        self.enabled = True
        self.loop = True
        self.sleep_time = 5
        #self.sleep_time = 60*60
        self.initialize()


    def setup(self):
        self.cs_dir = 'www/course_structure'
        self.data_path = self.get_data_path()
        self.db_path = ''
        self.event_path = os.path.realpath(os.path.join(self.data_path, 'clickstream_logs', 'latest'))
        pass


    def run(self):
        print 'PrepareIngest run...'        

        new_db_path = os.path.realpath(os.path.join(self.data_path, 'database_state', 'latest'))

        # only run when there are new data downloaded
        if self.db_path != new_db_path:
            logger.warning('New data has been downloaded.')

            self.db_path = new_db_path
            # Get course stucture json files
            self.cs_files = self.get_course_structure_files()

            # Add a record to the table {tasks}
            param_dict = {}
            param_dict['service_name'] = self.get_name()
            param_dict['type'] = 'mysql path'
            param_dict['meta'] = self.db_path
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            param_dict['started_date'] = current_time
            param_dict['started'] = True
            query = self.add_to_tasks(param_dict)
            mysqldb_ops.execute_sql(sql=query)

            # Get prepare_id
            self.prepare_id = self.get_last_prepare_id()
            #print prepare_id

            # Update the {courses} table based on self.cs_files
            self.update_courses_table1()

            # Update the {courses} table based on the config file
            self.update_courses_table2()

            # Get the courses need to be ingested
            self.get_ingest_courses()

            #@todo: process previous not processed/finished tasks

            # Process the files of 'Database Data'
            self.preprocess_db_files()

            # Process the files of 'Event Data'
            self.preprocess_event_files()

        pass

    def preprocess_event_files(self):
        print self.event_path
        ignore_files = []
        existings = self.get_existing_tasks('Clickstream')
        for existing_task in existings:
            if existing_task['type'] == 'file':
                ignore_files.append(existing_task['meta'])

        print ignore_files

        new_files = []
        for file in os.listdir(self.event_path):
            file_path = os.path.realpath(os.path.join(self.event_path, file))
            if os.path.isfile(file_path):
                if fnmatch.fnmatch(file, config.CLICKSTREAM_PREFIX + '????-??-??.log'):
                    if file_path not in ignore_files:
                        new_files.append(file)
                else:
                    logger.warning('Inconsistency (event file): ' + file_path)

        new_files.sort()
        # Remove the last one as it possibly does not have full data of that day
        del new_files[-1]

        query_arr = []
        for file in new_files:
            collection = self.get_collection_name(file)

            param_dict = {}
            param_dict['service_name'] = 'Clickstream'
            param_dict['type'] = 'file'
            param_dict['meta'] = os.path.realpath(os.path.join(self.event_path, file))
            param_dict['prepare_id'] = self.prepare_id
            query = self.add_to_tasks(param_dict)
            query_arr.append(query)

            param_dict = {}
            param_dict['service_name'] = 'TimeFinder'
            param_dict['type'] = 'mongodb collection'
            param_dict['meta'] = collection
            param_dict['prepare_id'] = self.prepare_id
            param_dict['dependent'] = ['Clickstream']
            query = self.add_to_tasks(param_dict)
            query_arr.append(query)

            param_dict = {}
            param_dict['service_name'] = 'IpToCountry'
            param_dict['type'] = 'mongodb collection'
            param_dict['meta'] = collection
            param_dict['prepare_id'] = self.prepare_id
            param_dict['dependent'] = ['Clickstream', 'TimeFinder']
            query = self.add_to_tasks(param_dict)
            query_arr.append(query)

            param_dict = {}
            param_dict['service_name'] = 'PersonCourse'
            param_dict['type'] = 'mongodb collection'
            param_dict['meta'] = collection
            param_dict['prepare_id'] = self.prepare_id
            param_dict['dependent'] = ['Clickstream', 'TimeFinder', 'IpToCountry', 'DatabaseState']
            query = self.add_to_tasks(param_dict)
            query_arr.append(query)

            param_dict = {}
            param_dict['service_name'] = 'EventCount'
            param_dict['type'] = 'mongodb collection'
            param_dict['meta'] = collection
            param_dict['prepare_id'] = self.prepare_id
            param_dict['dependent'] = ['Clickstream', 'TimeFinder', 'IpToCountry']
            query = self.add_to_tasks(param_dict)
            query_arr.append(query)

        #print query_arr
        mysqldb_ops.execute_sql(sql_arr=query_arr)


        
    def preprocess_db_files(self):
        #print self.ingest_courses
        #print self.ingest_courses_prefix

        query_arr = []

        # preprocess files in db_path
        for file in os.listdir(self.db_path):
            file_path = os.path.realpath(os.path.join(self.db_path, file))
            if os.path.isfile(file_path):
                #print file
                validate = self.file_validate(file)
                if validate == 'Email':
                    param_dict = {}                     
                    # @Todo change later
                    param_dict['service_name'] = 'Email'
                    param_dict['type'] = 'file'
                    param_dict['meta'] = file_path
                    param_dict['prepare_id'] = self.prepare_id
                    query = self.add_to_tasks(param_dict)
                    query_arr.append(query)                 

                if validate == 'CS not Ingest':
                    logger.info('Not ingest: ' + file_path)

                if validate == 'Inconsistency':
                    logger.warning('Inconsistency (file validate): ' + file_path)

                if validate == 'Ingest':
                    if file.endswith('.sql'):
                        param_dict = {}
                        # @Todo change later
                        param_dict['service_name'] = 'DatabaseState'
                        param_dict['type'] = 'file'
                        param_dict['meta'] = file_path
                        param_dict['prepare_id'] = self.prepare_id
                        query = self.add_to_tasks(param_dict)
                        query_arr.append(query)
                    elif file.endswith('.mongo'):
                        param_dict = {}                     
                        # @Todo change later
                        param_dict['service_name'] = 'DiscussionForum'
                        param_dict['type'] = 'file'
                        param_dict['meta'] = file_path
                        param_dict['prepare_id'] = self.prepare_id
                        query = self.add_to_tasks(param_dict)
                        query_arr.append(query)
                    # Copy couse structure json files to the cs_dir
                    elif file.endswith('-analytics.json'):
                        shutil.copy2(file_path, self.cs_dir)
                    else:
                        logger.warning('No service for: ' + file_path)



        #print query_arr
        mysqldb_ops.execute_sql(sql_arr=query_arr)



    def file_validate(self, file_name):
        """
        Validate file_name
        :param file_name: the name of file to be validated
        :return: 'Inconsistency': No corresponding name for the file_name (data inconsistency)
                 'Ingest': the file should be ingested (in self.ingest_courses)
                 'CS not Ingest': the file should not be ingested but find corresponding course structure file
                 'Email': the file is the csv file for email addresses
        """
        if fnmatch.fnmatch(file_name, '*email*analytics.csv'):
            return 'Email'
        for course_prefix in self.ingest_courses_prefix:
            if file_name.startswith(course_prefix):
                return 'Ingest'
        for course_prefix in self.cs_files_prefix:
            if file_name.startswith(course_prefix):
                return  'CS not Ingest'
        return 'Inconsistency'

    def update_courses_table2(self):

        # Update the field of 'ingest' in the {cuorses} table based on config.IGNORE_COURSES_INGEST
        ignore_ingest = []
        for conditions in config.IGNORE_COURSES_INGEST:
            sql = "UPDATE courses SET ingest = 0 WHERE "
            query_conditions = []
            for key, value in conditions.iteritems():
                condition = "%s = '%s'" % (key, value)
                query_conditions.append(condition)
            conditions_str = ' AND '.join(query_conditions)
            sql = sql + conditions_str
            ignore_ingest.append(sql)
        mysqldb_ops.execute_sql(sql_arr=ignore_ingest)

        # Update the field of 'api' in the {cuorses} table based on config.IGNORE_COURSES_API
        ignore_api = []
        for conditions in config.IGNORE_COURSES_API:
            sql = "UPDATE courses SET api = 0 WHERE "
            query_conditions = []
            for key, value in conditions.iteritems():
                condition = "%s = '%s'" % (key, value)
                query_conditions.append(condition)
            conditions_str = ' AND '.join(query_conditions)
            sql = sql + conditions_str
            ignore_api.append(sql)
        mysqldb_ops.execute_sql(sql_arr=ignore_api)

        # Update the field of 'dashboard' in the {cuorses} table based on config.IGNORE_COURSES_DASHBOARD
        ignore_dashboard = []
        for conditions in config.IGNORE_COURSES_DASHBOARD:
            sql = "UPDATE courses SET dashboard = 0 WHERE "
            query_conditions = []
            for key, value in conditions.iteritems():
                condition = "%s = '%s'" % (key, value)
                query_conditions.append(condition)
            conditions_str = ' AND '.join(query_conditions)
            sql = sql + conditions_str
            ignore_dashboard.append(sql)
        mysqldb_ops.execute_sql(sql_arr=ignore_dashboard)

    def get_last_prepare_id(self):
        query = "SELECT id FROM tasks WHERE service_name = '%s' ORDER BY id DESC limit 1" % self.get_name()
        cur = mysqldb_ops.execute_query(sql=query)
        row = cur.fetchone()
        prepare_id = row['id']
        mysqldb_ops.close_cur(cur)
        return prepare_id

    def get_ingest_courses(self):
        sql = "SELECT * FROM courses WHERE has_data = 1 AND ingest = 1"
        cur = mysqldb_ops.execute_query(sql=sql)
        ingest_courses = []
        ingest_courses_prefix = []
        for row in cur.fetchall():
            ingest_courses.append(row)
            ingest_courses_prefix.append(row['org'] + '-' + row['course'] + '-' + row['run'])

        self.ingest_courses = ingest_courses
        self.ingest_courses_prefix = ingest_courses_prefix

    def update_courses_table1(self):
        # Get all courses from DB
        sql = "SELECT * FROM courses"
        cur = mysqldb_ops.execute_query(sql=sql)
        courses = []
        courses_strings = []
        cs_files_prefix = []
        for row in cur.fetchall():
            courses.append(row)
            courses_strings.append(row['org'] + '-' + row['course'] + '-' + row['run'] + '-course_structure-' + row['site'] + '-analytics.json')
            cs_files_prefix.append(row['org'] + '-' + row['course'] + '-' + row['run'])
        self.cs_files_prefix = cs_files_prefix
        mysqldb_ops.close_cur(cur)

        # For courses not in db
        courses_not_in_db = list(set(self.cs_files) - set(courses_strings))
        if len(courses_not_in_db) > 0:
            logger.warning("New courses are finded")

            # Write into the table {courses}
            sql_arr = []
            for course in courses_not_in_db:
                meta = course.split("-")
                org = meta[0]
                course = meta[1]
                run = meta[2]
                site = meta[4]
                if len(meta) == 7:
                    site = meta[4] + '-' + meta[5]
                sql = "INSERT INTO courses (org, course, run, site, has_data) VALUES ('%s', '%s', '%s', '%s', 1)" % (org, course, run, site)
                sql_arr.append(sql)
                logger.warning('-'.join([org, course, run]) + " will be added into the table {courses}.")
            mysqldb_ops.execute_sql(sql_arr=sql_arr)

        # For courses in db but not in the 'cs_files' array
        courses_not_has_data = list(set(courses_strings) - set(self.cs_files))
        # Update the table {courses} set the field 'has_data' to 0
        update_arr = []
        for course in courses_not_has_data:
            meta = course.split("-")
            org = meta[0]
            course = meta[1]
            run = meta[2]
            site = meta[4]
            if len(meta) == 7:
                site = meta[4] + '-' + meta[5]
            sql = "UPDATE courses SET has_data = 0 WHERE org = '%s' AND course = '%s' AND run = '%s' AND site = '%s'" % (org, course, run, site)
            update_arr.append(sql)
            logger.warning('-'.join([org, course, run]) + " does not has data. The field has_data is set to be 0.")
        mysqldb_ops.execute_sql(sql_arr=update_arr)

        # For courses in both sets
        courses_in_both = set(courses_strings).intersection(set(self.cs_files))
        # Update the table {courses} at the field 'has_data' to 1
        update_arr = []
        for course in courses_in_both:
            meta = course.split("-")
            org = meta[0]
            course = meta[1]
            run = meta[2]
            site = meta[4]
            if len(meta) == 7:
                site = meta[4] + '-' + meta[5]
            sql = "UPDATE courses SET has_data = 1 WHERE org = '%s' AND course = '%s' AND run = '%s' AND site = '%s'" % (org, course, run, site)
            update_arr.append(sql)
        mysqldb_ops.execute_sql(sql_arr=update_arr)


    def get_course_structure_files(self):
        required_files = []

        for file in os.listdir(self.db_path):
            if os.path.isfile(os.path.join(self.db_path, file)) and file.endswith("-analytics.json"):
                required_files.append(file)

        return required_files


def service(servicethreads):
    """
    Returns an instance of the service
    """
    return PrepareIngest(servicethreads)

