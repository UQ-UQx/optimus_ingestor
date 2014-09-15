"""
Service for attaching time objects to mongo entries with timestamps
"""
import base_service
import utils
from bson import ObjectId
from pymongo import *
import dateutil.parser


class TimeFinder(base_service.BaseService):
    """
    Finds entries in Mongo which have a time stamp and adds a mongo time object to them
    """

    inst = None

    def __init__(self):
        TimeFinder.inst = self
        super(TimeFinder, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Time Finder"
        #Whether the service is enabled
        self.enabled = True
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

        self.mongo_client = None
        self.mongo_db = None
        self.mongo_dbname = 'logs'
        self.timefield = 'time'

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
        last_run = self.find_last_run_ingest("TimeFinder")
        last_clickstream = self.find_last_run_ingest("Clickstream")
        if self.finished_ingestion("Clickstream") and last_run < last_clickstream:

            if self.mongo_client is None:
                self.mongo_client = MongoClient('localhost', 27017)
                if self.mongo_db is None:
                    self.mongo_db = self.mongo_client[self.mongo_dbname]

            if self.mongo_db:
                for collection in self.mongo_db.collection_names():
                    mongo_collection = self.mongo_db[collection]
                    if mongo_collection:
                        utils.log("CHECKING TIME")
                        toupdates = mongo_collection.find({self.timefield: {'$exists': True}, 'time_date': {'$exists': False}})
                        utils.log("FOUND TIME")
                        for toupdate in toupdates:
                            #utils.log("*** Adding time to " + str(toupdate))
                            mongo_collection.update({"_id": ObjectId(toupdate['_id'])}, {"$set": {"time_date": dateutil.parser.parse(toupdate['time'])}})
                            print "*** ADDING TIME"
                utils.log("FINISHED TIME")
                self.save_run_ingest()

        pass


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
    return "TimeFinder"


def service():
    """
    Returns an instance of the service
    """
    return TimeFinder()