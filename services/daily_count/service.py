"""
A service for generating counts for each day
"""
import base_service
import os
import utils
import MySQLdb
import config
import warnings
import dateutil.parser
import math
from models import PCModel
from models import CFModel
import urllib2
import json
import time
import datetime
import csv
from pymongo import MongoClient


class DailyCount(base_service.BaseService):
    """
    Generates derived data for each day
    """

    inst = None

    def __init__(self):
        DailyCount.inst = self
        super(DailyCount, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Daily Count"
        #Whether the service is enabled
        self.enabled = True
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

        #self.pc_table = 'personcourse'
        #self.cf_table = 'courseprofile'
        #self.pc_db = 'Person_Course'

        #self.sql_pc_conn = None
        #self.sql_course_conn = None

        #Vars
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_dbname = ""
        self.mongo_collection = None
        self.mongo_collectionname = ""
        self.courses = {}

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        #self.courses = self.get_all_courses()
        print "STARTING DAILY COUNT"
        self.courses = self.get_all_courses()
        self.connect_to_mongo("logs", "clickstream")
        #self.sql_pc_conn = self.connect_to_sql(self.sql_pc_conn, "Person_Course", True)
        #self.sql_course_conn = self.connect_to_sql(self.sql_course_conn, "", True)
        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        last_run = self.find_last_run_ingest("DailyCount")
        last_timefinder = self.find_last_run_ingest("TimeFinder")
        last_iptocountry = self.find_last_run_ingest("IpToCountry")
        if self.finished_ingestion("TimeFinder") and last_run < last_timefinder and self.finished_ingestion("IpToCountry") and last_run < last_iptocountry:

            for course_id, course in self.courses.items():


                print "RUNNNNING"
                print course

                user_events = self.mongo_collection.aggregate([
                    {"$match": {"context.course_id": course['mongoname']}},
                    {"$sort": {"time": 1}},
                    {"$group": {"_id": "$context.user_id", "countrySet": {"$addToSet": "$country"}, "eventSum": {"$sum": 1}, "last_event": {"$last": "$time"}}}
                ], allowDiskUse=True)['result']
                print "WEEE"
                print user_events
                print "XXXX"


                day = {
                    "course": "",
                    "time": "",
                    "modes": {
                        "audit": 0,
                        "honor": 0,
                        "verified": 0
                    },
                    "categories": {
                        "registered": 0,
                        "viewed": 0,
                        "explored": 0,
                        "certified": 0
                    },
                    "age": {
                        "Less than 12": 0,
                        "12-15": 0,
                        "16-18": 0,
                        "19-22": 0,
                        "23-25": 0,
                        "26-30": 0,
                        "31-40": 0,
                        "41-50": 0,
                        "Over 50": 0,
                        "Unknown": 0
                    },
                    "country": {

                    },
                    "gender": {
                        "Male": 0,
                        "Female": 0,
                        "Other": 0,
                        "Unspecified": 0
                    },
                    "modes_unique": {
                        "audit": 0,
                        "honor": 0,
                        "verified": 0
                    },
                    "categories_unique": {
                        "registered": 0,
                        "viewed": 0,
                        "explored": 0,
                        "certified": 0
                    },
                    "age_unique": {
                        "Less than 12": 0,
                        "12-15": 0,
                        "16-18": 0,
                        "19-22": 0,
                        "23-25": 0,
                        "26-30": 0,
                        "31-40": 0,
                        "41-50": 0,
                        "Over 50": 0,
                        "Unknown": 0
                    },
                    "country_unique": {

                    },
                    "gender_unique": {
                        "Male": 0,
                        "Female": 0,
                        "Other": 0,
                        "Unspecified": 0
                    }
                }

            #self.save_run_ingest()
            utils.log("Daily count completed")


    # Connects to a Mongo Database
    def connect_to_mongo(self, db_name="", collection_name=""):
        """
        Connects to a mongo database
        :param db_name:
        :param collection_name:
        :return:
        """
        db_name = safe_name(db_name)
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

    def connect_to_sql(self, sql_connect, db_name="", force_reconnect=False, create_db=True):
        """
        Connect to SQL database or create the database and connect
        :param sql_connect: the variable to set
        :param db_name: the name of the database
        :param force_reconnect: force the database connection
        :param create_db: create the database
        :return the created SQL connection
        """
        print self
        if sql_connect is None or force_reconnect:
            try:
                sql_connect = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db=db_name)
                return sql_connect
            except Exception, e:
                # Create the database
                if e[0] and create_db and db_name != "":
                    if sql_connect is None:
                        sql_connect = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD)
                    utils.log("Creating database " + db_name)

                    cur = sql_connect.cursor()
                    cur.execute("CREATE DATABASE " + db_name)
                    sql_connect.commit()
                    sql_connect.select_db(db_name)
                    return sql_connect
                else:
                    utils.log("Could not connect to MySQL: %s" % e)
                    return None
        return None


def safe_name(filename):
    """
    Protection against bad database names
    :param filename: the filename
    :return: a safe fileneame
    """
    return str(filename).replace('.', '_')


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
    return "DailyCount"


def service():
    """
    Returns an instance of the service
    """
    return DailyCount()