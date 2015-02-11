"""
Service for importing the edX clickstream
"""
import base_service
import os
import utils
import time
import config
import urllib2
import json
from pymongo import MongoClient
from datetime import datetime






class Eventcount(base_service.BaseService):
    """
    Collects the clickstream logs from the edX data package using mongoimport system command
    """
    inst = None

    def __init__(self):
        Eventcount.inst = self
        super(Eventcount, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Eventcount"
        #Whether the service is enabled
        self.enabled = True
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

        self.ec_table = "courseevent"
        self.ec_db = "Course_Event"

        self.sql_ec_conn = None
        # self.sql_course_conn = None

        self.mongo_dbname = "logs"
        self.mongo_collectionname = "clickstream"

        self.mongo_client = None
        self.mongo_db = None
        self.mongo_collection = None

        self.courses = {}

        self.initialize()

        pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        ensure_mongo_indexes()
        self.courses = self.get_all_courses()
        self.sql_ec_conn = self.connect_to_sql(self.sql_ec_conn, self.ec_db, True)
        self.connect_to_mongo(self.mongo_dbname, self.mongo_collectionname)
        # self.sql_course_conn = self.connect_to_sql(self.sql_course_conn, "", True)

        print "SETTING UP EVENT COUNT"
        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        print "RUNNING THE EVENT COUNT"
        utils.log("The Event Count starts at: " + str(datetime.now()))
        self.clean_ec_db()

        last_run = self.find_last_run_ingest("EventCount")
        last_timefinder = self.find_last_run_ingest("TimeFinder")

        #if self.finished_ingestion("TimeFinder") and last_run < last_timefinder:
            # self.create_ec_table()
        # need to be indented in the furture
        #print self.courses
        for course_id, course in self.courses.items():
            print course_id

            # Get events from course info
            json_file = course['dbname'].replace("_", "-") + ".json"
            courseinfo = self.loadcourseinfo(json_file)
            if courseinfo is None:
                utils.log("Can not find course info for ." + str(course_id))
                continue

            #print courseinfo

            # Get events
            events = self.get_events(courseinfo)

            # Create courseevent table
            self.create_ec_table(course_id, events)

            events_date_counts = {}

            for event_id in events:
                self.group_event_by_date(course['mongoname'], event_id, events_date_counts)

            #print events_date_counts

            # Insert records into database
            self.insert_ec_table(course_id, events_date_counts)

            self.sql_ec_conn.commit()

        self.loop = False
        utils.log("The Event Count ends at: " + str(datetime.now()))


    def group_event_by_date(self, course_mongo_name, event_id, events_date_counts):

        results = self.mongo_collection.find({"event_type": {"$regex": event_id}, "context.course_id": course_mongo_name})
        for item in results:
            if 'time_date' in item:
                item_date = item['time_date'].date()
            elif 'time' in item:
                item_date = datetime.strptime(item['time'][:19], '%Y-%m-%dT%H:%M:%S').date()
            else:
                continue

            if item_date not in events_date_counts:
                events_date_counts[item_date] = {}
                events_date_counts[item_date][event_id] = 0
            elif event_id not in events_date_counts[item_date]:
                events_date_counts[item_date][event_id] = 0

            events_date_counts[item_date][event_id] += 1

    def get_chapters(self, obj, found=None):
        """
        Gets the chapter for the object (recursive)
        :param obj: the object being added
        :param found: an array of previously found elements
        :return the found object
        """
        if not found:
            found = []
        if obj['tag'] == 'chapter':
            found.append(obj)
        else:
            for child in obj['children']:
                found = self.get_chapters(child, found)
        return found

    def get_events(self, courseinfo):
        """
        Analyses chapters within a course
        :param chapters: An array of chapter dictionaries
        :return: A dict containing the analysis
        """

        chapters = []
        chapters = self.get_chapters(courseinfo, chapters)

        events = []
        for chapter in chapters:
            for sequential in chapter['children']:
                if sequential['tag'] == 'sequential' and 'children' in sequential:
                    for vertical in sequential['children']:
                        if vertical['tag'] == 'vertical' and 'children' in vertical:
                            for child in vertical['children']:
                                if 'url_name' in child:
                                    events.append(child['url_name'])

        return events


    def create_ec_table(self, course_id, events):
        """
        Creates the event course tables
        """
        cursor = self.sql_ec_conn.cursor()
        columns = [
            {"col_name": "id", "col_type": "int NOT NULL AUTO_INCREMENT PRIMARY KEY"},
            {"col_name": "course_id", "col_type": "varchar(255)"},
            {"col_name": "event_date", "col_type": "date"},
        ]

        # Add events to columns, add "u_" in front of event id to avoid possible column name syntax problems.
        for event_id in events:
            columns.append({"col_name": "u_" + event_id, "col_type": "int DEFAULT 0"})

        ec_tablename = self.ec_table + "_" + course_id
        query = "CREATE TABLE IF NOT EXISTS " + ec_tablename
        query += " ("
        for column in columns:
            query += column["col_name"] + " " + column["col_type"] + ", "
        query = query[:-2]
        query += " );"
        cursor.execute(query)
        self.sql_ec_conn.commit()


    def insert_ec_table(self, course_id, events_date_counts):

        cursor = self.sql_ec_conn.cursor()

        ec_tablename = self.ec_table + "_" + course_id

        for event_date, events_counts in events_date_counts.iteritems():
            if len(events_counts) == 0:
                continue

            columns_name = ""
            columns_value = ""
            for event_id, count in events_counts.iteritems():
                columns_name += "u_" + event_id + ", "
                columns_value += str(count) + ", "

            columns_name = columns_name[:-2]
            columns_value = columns_value[:-2]

            query = "INSERT INTO " + ec_tablename + " (course_id, event_date, " + columns_name + ") VALUES ('" + course_id + "', '" + event_date.strftime("%Y-%m-%d") + "', " + columns_value + ");"

            #print query

            cursor.execute(query)
        pass


    def loadcourseinfo(self, json_file):
        """
        Loads the course information from JSON course structure file
        :param json_file: the name of the course structure file
        :return the course information
        """
        print self
        courseurl = config.SERVER_URL + '/datasources/course_structure/' + json_file
        courseinfofile = urllib2.urlopen(courseurl)
        if courseinfofile:
            courseinfo = json.load(courseinfofile)
            return courseinfo
        return None

    def clean_ec_db(self):
        cursor = self.sql_ec_conn.cursor()

        for course_id, course in self.courses.items():
            ec_tablename = self.ec_table + "_" + course_id
            query = "DROP TABLE IF EXISTS %s" % ec_tablename
            cursor.execute(query)

        self.sql_ec_conn.commit()
        utils.log(self.ec_db + " has been cleaned.")

    def connect_to_mongo(self, db_name="", collection_name=""):
        """
        Connects to a mongo database
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            if self.mongo_client is None:
                self.mongo_client = MongoClient('localhost', 27017)
            if db_name != "":
                self.mongo_db = self.mongo_client[db_name]
                if self.mongo_db:
                    self.mongo_dbname = db_name
                    if collection_name != "":
                        self.mongo_collection = self.mongo_db[collection_name]
                        if self.mongo_collection:
                            self.mongo_collectionname = collection_name
            return True
        except Exception, e:
            utils.log("Could not connect to MongoDB: %s" % e)
        return False


def ensure_mongo_indexes():
    """
    Runs commands on the mongo indexes to ensure that they are set
    :return: None
    """
    utils.log("Setting index for countries")
    cmd = "mongo  --quiet " + config.MONGO_HOST + "/logs --eval \"db.clickstream.ensureIndex({country:1})\""
    #os.system(cmd)


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    ignore_dates = []
    existings = base_service.BaseService.get_existing_ingests("Clickstream")
    for existing in existings:
        if existing[2] == 'file':
            pathvars = existing[3].split('/')
            ignore_dates.append(pathvars[len(pathvars)-2] + "/" + pathvars[len(pathvars)-1])

    required_files = []
    main_path = os.path.realpath(os.path.join(path, 'clickstream_logs', 'latest'))

    # Changed for new clickstream format
    #for subdir in os.listdir(main_path):
    #    if os.path.isdir(os.path.join(main_path, subdir)):
    for filename in os.listdir(main_path):
        extension = os.path.splitext(filename)[1]
        if extension == '.log':
            pathvars = os.path.join(main_path, filename).split('/')
            ignore_check = pathvars[len(pathvars)-2] + "/" + pathvars[len(pathvars)-1]
            if ignore_check not in ignore_dates:
                required_files.append(os.path.join(main_path, filename))
            else:
                pass
                #print "IGNORING "+ignore_check
    maxdates = {}
    for required_file in required_files:
        dirname = os.path.dirname(required_file)
        filename = os.path.basename(required_file)
        filetime = filenametodate(filename)
        if dirname not in maxdates:
            maxdates[dirname] = filetime
        if filetime > maxdates[dirname]:
            maxdates[dirname] = filetime
    for i in reversed(xrange(len(required_files))):
        dirname = os.path.dirname(required_files[i])
        filename = os.path.basename(required_files[i])
        filetime = filenametodate(filename)
        if maxdates[dirname] == filetime:
            del required_files[i]
        pass
    return required_files


def filenametodate(filename):
    """
    Extracts the date from a clickstream filename
    :param filename: The filename to extract
    :return: The date
    """
    date = filename.replace(".log", "").replace(config.CLICKSTREAM_PREFIX, "")
    date = time.strptime(date, "%Y-%m-%d")
    return date


def name():
    """
    Returns the name of the service class
    """
    return "EventCount"


def service():
    """
    Returns an instance of the service
    """
    return Eventcount()

