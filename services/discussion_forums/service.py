"""
Service for importing discussion forum data
"""
import base_service
import os
import utils
from pymongo import MongoClient
import json
from bson.objectid import ObjectId
import datetime
from pymongo.errors import OperationFailure
from dateutil import parser

class DiscussionForums(base_service.BaseService):
    """
    Imports the edX discussion forum data into Mongo
    """
    inst = None

    def __init__(self):
        DiscussionForums.inst = self
        super(DiscussionForums, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Discussion Forums"
        #Whether the service is enabled
        self.enabled = True
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

        self.mongo_dbname = "discussion_forum"
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_collection = None
        self.mongo_collectionname = None

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
            if not base_service.ALIVE:
                print "BREAKING"
                break
            if ingest['type'] == 'file':

                self.start_ingest(ingest['id'])

                path = ingest['meta']

                coursename = os.path.basename(os.path.normpath(path)).replace(".mongo", "")

                self.connect_to_mongo(self.mongo_dbname, coursename)

                #ensure index on author_id which is used for queries
                create_index_status = self.mongo_collection.create_index([("author_id")])
                utils.log("Mongo Db Discussion Forum Index Created Status " + create_index_status)

                ingest_file = open(path)

                for line in ingest_file:
                    document = json.loads(line)
                    if '_id' in document:
                        try:
                            self.insert_with_id(document)
                        except OperationFailure as err:
                            utils.log("ERROR: BAD ID FOR DOCUMENT" + str(document))
			    utils.log("code is " + str(err.code))
                    else:
                        utils.log("ERROR: BAD ID FOR DOCUMENT" + str(document))

                self.finish_ingest(ingest['id'])
        pass

    def insert_with_id(self, document):
        """
        Inserts the JSON into the mongo database
        :param document: The Document
        """
        document = self.format_document(document)
        doc_id = document.pop('_id')
        done = self.mongo_collection.update({"_id": doc_id}, {"$set": document}, upsert=True)
        if done['updatedExisting']:
            pass
        else:
            pass

    #@staticmethod
    #def format_document2(document):
    #    """
    #    Formats the document for ingestion
    #    :param document: The Document
    #    :return The formatted Document
    #    """
    #    for key, item in document.items():
    #        #replace $ in key to _ and . in key to -
    #        new_key = key.replace("$", "_").replace(".", '"-')
    #        document[new_key] = document.pop(key)
    #        #process "$oid" and "$date" in item
    #        if isinstance(item, type({})):
    #            if "$oid" in item:
    #                document[key] = ObjectId(str(item["$oid"]))
    #            if "$date" in item:
    #                document[key] =  parser.parse(item["$date"])   #datetime.datetime.utcfromtimestamp(item["$date"]/1e3)
    #        # Process "parent_ids"
    #        if key == "parent_ids" and item:
    #            parent_ids = []
    #            for sub_item in item:
    #                if "$oid" in sub_item:
    #                    parent_ids.append(ObjectId(str(sub_item["$oid"])))
    #            document[key] = parent_ids
    #    return document

    @staticmethod
    def format_document(doc,outerdoc=None, k=None):
        for key, item in doc.items():
                if isinstance(item, type({})):
                        DiscussionForums.format_document(item, doc, key)
                if key=="$oid":
                        outerdoc[k]=ObjectId(str(item))
                elif key=="$date":
                        outerdoc[k]=parser.parse(item)
                elif key == "parent_ids": #and item:
                        parent_ids = []
                        for sub_item in item:
                                if "$oid" in sub_item:
                                        parent_ids.append(ObjectId(str(sub_item["$oid"])))
                        doc[key] = parent_ids
                else:
                        new_key = key.replace("$", "_").replace(".", '"-')
                        doc[new_key] = doc.pop(key)
        return doc





    def connect_to_mongo(self, db_name="", collection_name=""):
        """
        Connects to the specified mongo database
        :param db_name: The name of the database
        :param collection_name: The name of the collection
        :return True or False on success
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
    required_files = []
    main_path = os.path.realpath(os.path.join(path, 'database_state', 'latest'))
    for filename in os.listdir(main_path):
        extension = os.path.splitext(filename)[1]
        if extension == '.mongo':
            required_files.append(os.path.join(main_path, filename))
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "DiscussionForums"


def service():
    """
    Returns an instance of the service
    """
    return DiscussionForums()
