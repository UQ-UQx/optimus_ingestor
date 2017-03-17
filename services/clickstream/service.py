"""
Service for importing the edX clickstream
"""
import base_service
import os
import utils
import time
import config


class Clickstream(base_service.BaseService):
    """
    Collects the clickstream logs from the edX data package using mongoimport system command
    """
    inst = None

    def __init__(self):
        Clickstream.inst = self
        super(Clickstream, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Clickstream"
        #Whether the service is enabled
        self.enabled = True
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

        self.mongo_dbname = "logs"
        self.mongo_collectionname = "clickstream_delta"

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        #ensure_mongo_indexes()
        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        ingests = self.get_ingests()
        for ingest in ingests:
            if ingest['type'] == 'file':

                self.start_ingest(ingest['id'])
                utils.log("Importing from ingestor " + str(ingest['id']))
                cmd = "mongoimport --quiet --host " + config.MONGO_HOST + " --db "+self.mongo_dbname+" --collection "+self.mongo_collectionname+" < "+ingest['meta']
                os.system(cmd)

                self.finish_ingest(ingest['id'])
        pass


def ensure_mongo_indexes():
    """
    Runs commands on the mongo indexes to ensure that they are set
    :return: None
    """
    utils.log("Setting index for countries")
    cmd = "mongo  --quiet " + config.MONGO_HOST + "/logs --eval \"db.clickstream_delta.ensureIndex({country:1})\""
    os.system(cmd)
    utils.log("Setting index for event-course")
    cmd = "mongo  --quiet " + config.MONGO_HOST + "/logs --eval \"db.clickstream_delta.ensureIndex( {event_type: 1,'context.course_id': 1} )\""
    os.system(cmd)
    utils.log("Setting index for course")
    cmd = "mongo  --quiet " + config.MONGO_HOST + "/logs --eval \"db.clickstream_delta.ensureIndex( {'context.course_id': 1} )\""
    os.system(cmd)


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
        if (extension == '.log') and ("edge" not in filename):
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
    return "Clickstream"


def service():
    """
    Returns an instance of the service
    """
    return Clickstream()
