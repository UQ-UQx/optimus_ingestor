"""
Service for attaching countries to mongo entries with IP addresses
"""
import base_service
import utils
import os
import geoip2
from geoip2 import database
from geoip2.errors import *
from bson import ObjectId
from pymongo import *

basepath = os.path.dirname(__file__)


class IPToCountry(base_service.BaseService):
    """
    Finds entries in Mongo which have an IP and adds a country to them
    """

    inst = None

    def __init__(self):
        IPToCountry.inst = self
        super(IPToCountry, self).__init__()

        #The pretty name of the service
        self.pretty_name = "IP To Country"
        #Whether the service is enabled
        self.enabled = True
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

        self.mongo_client = None
        self.mongo_db = None
        self.mongo_dbname = 'logs'
        self.ipfield = 'ip'

        self.geo_reader = None

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        self.geo_reader = geoip2.database.Reader(basepath+'/lib/GeoIP2-Country.mmdb')
        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        last_run = self.find_last_run_ingest("IpToCountry")
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
                        toupdates = mongo_collection.find({self.ipfield: {'$exists': True}, 'country': {'$exists': False}})
                        for toupdate in toupdates:
                            if toupdate[self.ipfield] != '::1':
                                try:
                                    country = self.geo_reader.country(toupdate[self.ipfield])
                                    isocountry = country.country.iso_code
                                    mongo_collection.update({"_id": toupdate['_id']}, {"$set": {"country": isocountry}})
                                except AddressNotFoundError:
                                    utils.log("Could not find address for " + str(toupdate))
                                    pass
                            else:
                                mongo_collection.update({"_id": ObjectId(toupdate['_id'])}, {"$set": {"country": ""}})

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
    return "IPToCountry"


def service():
    """
    Returns an instance of the service
    """
    return IPToCountry()