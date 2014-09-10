"""
The base service which all services extend
"""
from utils import *
import time
import MySQLdb
import courses as config_courses


class BaseService(object):
    """
    The base class which all services extend
    """

    def __init__(self):

        #Auto variables
        #The lowercase name of the service
        self.servicename = ""
        #The current state of the service
        self.status = 'stopped'
        #The last time the service was running
        self.last_awake = ''
        #The database for doing ingestor system calls
        self.api_db = None
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

    # Starting the base service
    def initialize(self):
        """
        The initialisation method, sets default status and begins the run loop
        """
        if self.enabled:
            self.servicename = str(self.__class__.__name__).lower()
            log("Starting service "+self.servicename)
            self.status = 'loading'
            self.setup_ingest_api()
            self.setup()
            while self.loop:
                self.status = 'running'
                self.run()
                self.status = 'sleeping'
                self.last_awake = time.strftime('%Y-%m-%d %H:%M:%S')
                time.sleep(self.sleep_time)

    def setup(self):
        """
        Setup function, this should be overridden by the service
        """
        log("BAD METHOD, SETUP SHOULD BE SUBCLASSED IN "+self.servicename)

    def run(self):
        """
        Run function, this should be overridden by the service
        """

    def setup_ingest_api(self):
        """
        Sets up the API DB for getting service information
        """
        self.api_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db='api')

    def finished_ingestion(self, service_name):
        """
        Checks whether the service is running or not
        :param service_name: The name of the service to check
        :return: True or False depending on whether the ingestion is finished
        """
        self.setup_ingest_api()
        cur = self.api_db.cursor()
        query = "SELECT * FROM ingestor WHERE service_name = '" + service_name + "' AND started = 0 AND completed = 0 ORDER BY created ASC;"
        cur.execute(query)
        finished = True
        for row in cur.fetchall():
            finished = False
        return finished

    def find_last_run_ingest(self, service_name):
        """
        Finds the date of the last time the service ran
        :param service_name: The name of the service to find
        :return: The date of the last run
        """
        self.setup_ingest_api()
        cur = self.api_db.cursor()
        query = "SELECT * FROM ingestor WHERE service_name = '" + service_name + "' AND started = 1 AND completed = 1 ORDER BY created DESC limit 1;"
        cur.execute(query)
        date = datetime.datetime.fromtimestamp(0)
        for row in cur.fetchall():
            date = row[6]
        cur.close()
        return date

    def save_run_ingest(self):
        """
        Saves an entry to the ingest of the last time the service was run
        """
        self.setup_ingest_api()
        cur = self.api_db.cursor()
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        service_name = str(self.__class__.__name__)
        query = "INSERT INTO ingestor "
        query += "(service_name, type, meta, started, completed, created, started_date, completed_date) "
        query += "VALUES "
        query += '("' + service_name + '", "' + "save_run" + '", "", 1, 1, "' + current_time + '", "' + current_time + '", "' + current_time + '")'
        print query
        cur.execute(query)
        self.api_db.commit()

    def get_ingests(self):
        """
        Retrieves the relevant ingests for the service
        """
        self.setup_ingest_api()
        cur = self.api_db.cursor()
        query = "SELECT * FROM ingestor WHERE service_name = '" + str(self.__class__.__name__) + "' AND started = 0 AND completed = 0 ORDER BY created ASC;"
        cur.execute(query)
        ingests = []
        for row in cur.fetchall():
            ingest = {
                'id': row[0],
                'type': row[2],
                'meta': row[3]
            }
            ingests.append(ingest)
        cur.close()
        return ingests

    def start_ingest(self, ingest_id):
        """
        Starts an ingestion entry
        :param ingest_id: the ID of the ingestion entry
        """
        cur = self.api_db.cursor()
        current_date = time.strftime('%Y-%m-%d %H:%M:%S')
        query = "UPDATE ingestor SET started=1, started_date='" + current_date + "' WHERE id=" + str(ingest_id) + ";"
        cur.execute(query)
        self.api_db.commit()
        pass

    def finish_ingest(self, ingest_id):
        """
        Finishes an ingestion entry
        :param ingest_id: the ID of the ingestion entry
        """
        cur = self.api_db.cursor()
        current_date = time.strftime('%Y-%m-%d %H:%M:%S')
        query = "UPDATE ingestor SET completed=1, completed_date='" + current_date + "' WHERE id=" + str(ingest_id) + ";"
        cur.execute(query)
        self.api_db.commit()
        pass

    def use_sql_database(self, database_name):
        """
        Changes the SQL database to the one requested
        :param database_name: The name of the database
        """
        try:
            self.sql_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db=database_name)
            return True
        except MySQLdb.OperationalError:
            self.sql_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db='mysql')
            cur = self.sql_db.cursor()
            cur.execute("CREATE DATABASE "+database_name)
            try:
                self.sql_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db=database_name)
                return True
            except MySQLdb.OperationalError:
                log("Could not connect to MySQL Database: %s" % database_name)
                return False

    def sql_query(self, query, commit=False):
        """
        Performs a Query on the SQL database
        :param query: The query to perform
        :param commit: Whether to commit the save
        Returns the results of the query
        """
        results = []
        if self.sql_db:
            cur = self.sql_db.cursor()
            cur.execute(query)
            for row in cur.fetchall():
                results.append(row)
            if commit:
                self.sql_db.commit()
        return results

    @staticmethod
    def number_of_lines(the_file):
        """
        Returns the number of lines in a file
        :param the_file: The file to check
        :return: The number of lines
        """
        i = 0
        for i, l in enumerate(the_file):
            pass
        the_file.seek(0)
        return i + 1

    @staticmethod
    def get_all_courses():
        """
        Returns all the courses
        :return: All the courses
        """
        course_dict = config_courses.EDX_DATABASES
        del(course_dict['default'])
        del(course_dict['personcourse'])
        return course_dict