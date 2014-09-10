import base_service
import os
import utils
import pymongo
import time
import config

class ExtractSample(base_service.BaseService):

    inst = None

    def __init__(self):
        ExtractSample.inst = self
        super(ExtractSample, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Extract Student Sample"
        #Whether the service is enabled
        self.enabled = False
        #Whether to run more than once
        self.loop = False
        #The amount of time to sleep in seconds
        self.sleep_time = 60

        self.sql_db = None

        self.mongo_db = None
        self.mongo_dbname = ""

        self.sample_size = 100
        self.pass_ratio = 0.5

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        last_run = self.find_last_run_ingest("ExtractSample")
        last_timefinder = self.find_last_run_ingest("TimeFinder")
        last_iptocountry = self.find_last_run_ingest("IpToCountry")
        if self.finished_ingestion("TimeFinder") and last_run < last_timefinder and self.finished_ingestion("IpToCountry") and last_run < last_iptocountry:

            ### add a step to remove existing sample tables ###
            #self.remove_exist_tables()

            ### add a step to remove existing sample collections ###
            #self.remove_exist_collections()

            self.save_run_ingest()
            utils.log("Subset completed")

            pass

    # def subset_backup(self):
    #     """
    #     Creates a backup of the subset database
    #     """
    #     backup_path = config.EXPORT_PATH
    #     current_time = time.strftime('%m%d%Y-%H%M%S')
    #     backup_prefix = self.course_id + current_time
    #
    #     # Backup MySql
    #     tables = ["certificates_generatedcertificate_sample", "auth_user_sample", "auth_userprofile_sample", "courseware_studentmodule_sample", "student_courseenrollment_sample", "user_api_usercoursetag_sample", "user_id_map_sample", "wiki_article_sample", "wiki_articlerevision_sample"]
    #     backup_file = os.path.join(backup_path, backup_prefix+".sql")
    #     cmd = "mysqldump -u %s -p%s -h %s %s " + " ".join(["%s"] * len(tables)) + " > %s"
    #     arguments = [config.SQL_USERNAME, config.SQL_PASSWORD, config.SQL_HOST, self.sql_dbname] + tables + [backup_file]
    #     cmd = cmd % tuple(arguments)
    #     os.popen(cmd)
    #     sedcmd = "sed -i 's/_sample//g' " + backup_file
    #     os.system(sedcmd)
    #
    #     # Backup Mongo
    #     # Discussion forum
    #     backup_file = os.path.join(backup_path, backup_prefix)
    #     cmd_str = "mongodump --db %s --collection %s --out %s"
    #     arguments = ["discussion_forum", self.course['discussiontable'] + "_sample", backup_file]
    #     cmd = cmd_str % tuple(arguments)
    #     os.popen(cmd)
    #     # Clickstream
    #     arguments = ["logs", "clickstream_" + self.course_id + "_sample", backup_file]
    #     cmd = cmd_str % tuple(arguments)
    #     os.popen(cmd)
    #
    # def sql_user_id(self, table):
    #     """
    #     Returns the user IDs for a specific table
    #     :param table: the table to find the user IDs
    #     :return: an array of user IDs
    #     """
    #     user_ids = []
    #     query = "SELECT user_id FROM " + table
    #     self.cursor.execute(query)
    #     result = self.cursor.fetchall()
    #     for record in result:
    #         user_ids.append(record[0])
    #     return user_ids
    #
    # def sql_select_students(self, tablename=None, columns = []):
    #     if tablename is None:
    #         tablename = 'certificates_generatedcertificate'
    #
    #     # Counting the rows of the table
    #     query0 = "SELECT COUNT(*) FROM " + tablename
    #     table_size = self.sql_query_size(query0)
    #     utils.log("Row count of %s: %s" % (tablename,  table_size))
    #
    #     # Counting how many students passed
    #     query0 = "SELECT COUNT(*) FROM %s WHERE status = '%s'" % (tablename, "downloadable")
    #     pass_size = self.sql_query_size(query0)
    #     utils.log("  Count of %s: %s" % ("downloadedable", pass_size))
    #
    #     # Counting how many students only registered
    #     query0 = "SELECT COUNT(*) FROM %s WHERE grade = '%s'" % (tablename, "0.0")
    #     only_registered = self.sql_query_size(query0)
    #     utils.log("  Count of %s: %s" % ("only registered", only_registered))
    #
    #     # Counting how many students viewed but not passed
    #     query0 = "SELECT COUNT(*) FROM %s WHERE grade != '%s' AND status = '%s'" % (tablename, "0.0", "notpassing")
    #     view_notpass = self.sql_query_size(query0)
    #     utils.log("  Count of %s: %s" % ("Viewed but Notpassing", view_notpass))
    #
    #     pass_samplesize = int(float(self.sample_size) * float(self.pass_ratio))
    #     view_notpass_samplesize = int(self.sample_size) - pass_samplesize
    #
    #     if table_size <= int(self.sample_size):
    #         utils.log("Row count of %s is equal or less than sample size. No more work to be done." % tablename)
    #         return False
    #
    #     # Create subset table if not exist
    #     new_tablename = tablename + "_sample"
    #     self.sql_create_subset_table(new_tablename, tablename)
    #
    #     # Do nothing if the table already has records
    #     query0 = "SELECT COUNT(*) FROM " + new_tablename
    #     newtable_size = self.sql_query_size(query0)
    #     if newtable_size > 0:
    #         utils.log("%s already has %d records. No more work to be done." % (new_tablename, newtable_size))
    #         return False
    #
    #     # Insert records of students passed
    #     query1 = "INSERT INTO %s SELECT * FROM %s WHERE status = '%s' ORDER BY RAND() LIMIT %d" % (new_tablename, tablename, "downloadable", pass_samplesize)
    #     # Insert records of students viewed but not passed
    #     query2 = "INSERT INTO %s SELECT * FROM %s WHERE status = '%s' AND grade <> '%s' ORDER BY RAND() LIMIT %d" % (new_tablename, tablename, "notpassing", "0.0", view_notpass_samplesize)
    #     self.cursor.execute(query1)
    #     self.cursor.execute(query2)
    #     return new_tablename
    #
    # def sql_create_subset_table(self, new_tablename, tablename):
    #     query = "CREATE TABLE IF NOT EXISTS " + new_tablename
    #     query += " AS SELECT * FROM " + tablename + " WHERE 0"
    #     utils.log("%s exists now." % new_tablename)
    #     self.cursor.execute(query)
    #
    # def sql_create_table(self, tablename, columns):
    #     query = "CREATE TABLE IF NOT EXISTS " + tablename + "_sample"
    #     query += " ("
    #     for column in columns:
    #         coltype = "varchar(255)"
    #         if column == "id":
    #             coltype = "int NOT NULL UNIQUE"
    #         if column == "key":
    #             column = "_key"
    #         if column == "state" or column == "content" or column == "meta":
    #             coltype = "longtext"
    #         if column == "goals" or column == "mailing_address":
    #             coltype = "text"
    #         query += column.replace("\n", "") + " " + coltype + ", "
    #     query += " xhash varchar(200) "
    #     query += ", UNIQUE (xhash)"
    #     query += " );"
    #     self.cursor.execute(query)
    #
    # def sql_query_size(self, query):
    #     """
    #     Returns the first result for a query
    #     :param query: The query
    #     :return: The MySQL result
    #     """
    #     try:
    #         self.cursor.execute(query)
    #         result = self.cursor.fetchone()
    #         return result[0]
    #     except self.sql_db.ProgrammingError:
    #         print "Error: unable to fetch data"
    #
    # def remove_exist_tables(self):
    #     """
    #     Remove all sample tables from mysql database if exist
    #     """
    #     table_names = []
    #
    #     query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '%s' AND table_name like '%%_sample'" % self.sql_dbname
    #     self.cursor.execute(query)
    #     result = self.cursor.fetchall()
    #     if len(result) > 0:
    #         for record in result:
    #             table_names.append(record[0])
    #         query = "DROP TABLE IF EXISTS " + ",".join(["%s"] * len(table_names))
    #         query = query % tuple(table_names)
    #         utils.log(query)
    #         self.cursor.execute(query)
    #         self.sql_db.commit()
    #
    # def remove_exist_collections(self):
    #     """
    #     Remove all sample collections from mongodb if exist
    #     """
    #     # logs
    #     self.mongo_dbname = "logs"
    #     self.connect_to_mongo(self.mongo_dbname, "")
    #     for col in self.mongo_db.collection_names():
    #         if col.endswith("_sample"):
    #             self.mongo_db.drop_collection(col)
    #             utils.log('Collection %s is deleted.' % col)
    #
    #     # Discussion forum
    #     self.mongo_dbname = "discussion_forum"
    #     self.connect_to_mongo(self.mongo_dbname, "")
    #     for col in self.mongo_db.collection_names():
    #         if col.endswith("_sample"):
    #             self.mongo_db.drop_collection(col)
    #             utils.log('Collection %s is deleted.' % col)


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    print path
    required_files = []
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "ExtractSample"


def service():
    """
    Returns an instance of the service
    """
    return ExtractSample()