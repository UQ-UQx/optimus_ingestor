"""
Service for importing the edX database
"""
import base_service
from utils import *
import warnings
import MySQLdb
import hashlib


class DatabaseState(base_service.BaseService):
    """
    Collects the database state from the SQL files
    """
    inst = None

    def __init__(self):
        DatabaseState.inst = self
        super(DatabaseState, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Database State"
        #Whether the service is enabled
        self.enabled = True
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

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
        ingests = self.get_ingests()
        for ingest in ingests:
            if ingest['type'] == 'file':

                self.start_ingest(ingest['id'])

                path = ingest['meta']
                file_name = os.path.basename(path)

                #find the tablename
                table_name = file_name[file_name.find('UQx-'):]
                table_name = table_name[:table_name.find('-prod-analytics.sql')]
                database_name = table_name.split("-")
                table_name = database_name[len(database_name)-1]
                database_name = '_'.join(database_name)
                database_name = database_name.replace("_"+table_name, "").replace(".","")

                ingest_file = open(path)

                #split the headers
                columns = []
                for line in ingest_file:
                    columns = line.replace("\n", "").split("\t")
                    break

                if self.create_table_and_validate(database_name, table_name, columns):
                    self.task = file_name
                    self.task_progress = 0
                    self.task_progress_total = self.number_of_lines(ingest_file)

                    for line in ingest_file:
                        if line[:2] == 'id' or line[:4] == 'hash':
                            continue
                        datahash = hashlib.sha256(line).hexdigest()
                        line = line.replace("\n", "")
                        line = line.replace('"', "''")
                        data = line.split("\t")
                        data.append(datahash)
                        insertdata = '"' + '","'.join(data) + '"'
                        self.sql_query("REPLACE INTO " + table_name + " VALUES ( " + insertdata + " );", True)
                        self.task_progress += 1

                    self.finish_ingest(ingest['id'])
        pass

    def create_table_and_validate(self, database_name, table_name, columns=None):
        """
        Checks that the table exists, and if not creates it
        :param database_name: The database name to use
        :param table_name: The table name to use
        :param columns: The columns for the table
        """
        if not columns:
            columns = []
        isvalid = False

        self.use_sql_database(database_name)

        if self.sql_db:
            #Create the ingestor table if necessary
            query = "CREATE TABLE IF NOT EXISTS "
            query += table_name
            query += " ( "
            for column in columns:
                coltype = "varchar(255)"
                if column == "id":
                    coltype = "int NOT NULL UNIQUE"
                if column == "key":
                    column = "_key"
                if column == "state" or column == "content" or column == "meta":
                    coltype = "longtext"
                if column == "created" or column == "modified" or column == "created_date" or column == "modified_date" or column == "last_login" or column == "date_joined":
                    coltype = "datetime"
                if column == "goals" or column == "mailing_address":
                    coltype = "text"
                query += column.replace("\n", "")+" "+coltype+", "
            query += " xhash varchar(200) "
            query += ", UNIQUE (xhash)"
            query += " );"
            warnings.filterwarnings('ignore', category=MySQLdb.Warning)
            self.sql_query(query)
            warnings.filterwarnings('always', category=MySQLdb.Warning)
            isvalid = True

        return isvalid


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    required_files = []
    main_path = os.path.realpath(os.path.join(path, 'database_state', 'latest'))
    for filename in os.listdir(main_path):
        extension = os.path.splitext(filename)[1]
        if extension == '.sql':
            required_files.append(os.path.join(main_path, filename))
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "DatabaseState"


def service():
    """
    Returns an instance of the service
    """
    return DatabaseState()