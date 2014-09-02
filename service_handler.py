"""
Deals with the webserver and the service modules
"""
import importlib
import BaseHTTPServer
import SimpleHTTPServer
from SocketServer import ThreadingMixIn
import threading
from time import sleep
from utils import *
import json
import MySQLdb
import warnings

#Web server
Protocol = "HTTP/1.0"
ServerPort = 8850


class ServiceManager():
    """
    Manages service modules
    """

    servicethreads = []
    servicemodules = []
    sql_db = None

    def __init__(self):
        log("Starting Service Manager")
        self.setup_ingest_database()
        self.autoload()

    def autoload(self):
        """
        Loads each module
        """
        servicespath = os.path.join(basepath, 'services')
        for servicename in os.listdir(servicespath):
            if servicename not in config.IGNORE_SERVICES:
                servicepath = os.path.join(servicespath, servicename, 'service.py')
                if os.path.exists(servicepath):
                    log("Starting module: "+servicename)
                    service_module = importlib.import_module('services.' + servicename + '.service')
                    self.servicemodules.append(service_module)
                    #Start Thread
                    #servicethread = threading.Thread(target=servicemodule.runservice)
                    #servicethread.start()
                    #self.servicethreads.append(servicethread)

    def setup_ingest_database(self):
        """
        Ensures that the required DB and tables exist
        """
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        #Create and connect to the API database
        try:
            self.sql_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db='api')
        except MySQLdb.OperationalError:
            self.sql_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db='mysql')
            cur = self.sql_db.cursor()
            cur.execute("CREATE DATABASE API")
            self.sql_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db='api')
        if self.sql_db:
            #Create the ingestor table if necessary
            cur = self.sql_db.cursor()
            query = "CREATE TABLE IF NOT EXISTS ingestor ( "
            query += "id int NOT NULL UNIQUE AUTO_INCREMENT, service_name varchar(255), type varchar(255), meta varchar(255), started int DEFAULT 0, completed int DEFAULT 0, created datetime NULL, started_date datetime NULL, completed_date datetime NULL, PRIMARY KEY (id)"
            query += ");"
            cur.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)

    def add_to_ingestion(self, service_name, ingest_type, meta):
        """
        Inserts a line into the ingestion table
        :param meta: any information the service needs
        :param ingest_type: the type of the ingestion
        :param service_name: the name of the service
        """
        insert = True
        cur = self.sql_db.cursor()
        #Check if entry already exists
        query = 'SELECT count(*) FROM ingestor WHERE service_name="' + service_name + '" AND type="' + ingest_type + '" AND meta="' + meta + '";'
        cur.execute(query)
        for row in cur.fetchall():
            if int(row[0]) > 0:
                insert = False
        if insert:
            #Insert the new entry
            created = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            query = 'INSERT INTO ingestor (service_name,type,meta,created) VALUES ('
            query += '"'+service_name+'","'+ingest_type+'","'+meta+'","'+created+'");'
            cur.execute(query)
            self.sql_db.commit()

    def get_next_entry(self, service_name):
        """
        Returns the next task which the service needs to run
        :param service_name: The name of the service
        :return: A dictionary of the next task
        """
        cur = self.sql_db.cursor()
        pass

    def start_ingest(self, entry_id):
        """
        Starts an ingestion entry
        :param entry_id: the ID of the ingestion entry
        """
        cur = self.sql_db.cursor()
        pass

    def finish_ingest(self, entry_id):
        """
        Finishes an ingestion entry
        :param entry_id: the ID of the ingestion entry
        """
        cur = self.sql_db.cursor()
        pass


def queue_data(servicehandler):
    """
    Asks each service for which files it needs, and adds them to the ingestor
    :param servicehandler: The service handler
    :return: Returns True when completed
    """
    for path in config.DATA_PATHS:
        for service_module in ServiceManager.servicemodules:
            required_files = service_module.get_files(path)
            for required_file in required_files:
                #Add file to the ingestion table
                pass
                servicehandler.manager.add_to_ingestion(service_module.name(), 'file', required_file)
    return True


class RequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    """
    Responds to HTTP Requests
    """

    response = 0
    servicehandler = None

    def runmodule(self, modulename, meta):
        """
        Executes a module to run (usually a module which does not loop)
        """
        servicespath = os.path.join(basepath, 'services')
        servicepath = os.path.join(servicespath, modulename, 'service.py')
        if os.path.exists(servicepath):
            pass
            #log("Starting once-off module "+servicename)
            #servicemodule = baseservice.load_module(servicename)
            #print meta
            #servicethread = threading.Thread(target=servicemodule.runservice, args=meta)
            #servicethread.start()

    def do_GET(self):
        """
        Response to GET requests
        """
        response = {}
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if self.path == "/status":
            status = {}
            #for sv in ServiceLoader.servicemodules:
            #    status[sv.name()] = sv.status()
            response['response'] = status
        elif "newdata":
            response['response'] = 'Could not queue data'
            response['statuscode'] = 500
            if queue_data(self.servicehandler):
                response['response'] = 'Data successfully queued'
                response['statuscode'] = 200
        elif "/":
            response['response'] = 'Ingestion running'
            response['statuscode'] = 200
        else:
            response['response'] = "error"
            response['statuscode'] = 404
        self.wfile.write(json.dumps(response))


class ThreadedHTTPServer(ThreadingMixIn, BaseHTTPServer.HTTPServer):
    """
    Threaded HTTP Server
    """
    allow_reuse_address = True

    def shutdown(self):
        """
        Shuts down the HTTP Server
        """
        self.socket.close()
        BaseHTTPServer.HTTPServer.shutdown(self)


class Servicehandler():
    """
    Service handler deals with the threaded nature of the application
    """
    server = None
    server_thread = None
    manager = None

    # Instance
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Servicehandler, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self.manager = ServiceManager()
        self.setup_webserver()

    def setup_webserver(self):
        """
        Creates and starts the web server
        """
        RequestHandler.servicehandler = self
        server_address = ('0.0.0.0', ServerPort)
        RequestHandler.protocol_version = Protocol
        self.server = ThreadedHTTPServer(server_address, RequestHandler)
        log("Starting Web Server")
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        #@todo remove this
        queue_data(self)
        self.sleepmainthread()

    def sleepmainthread(self):
        """
        Sleeps the main thread
        """
        sleep(2)
        self.sleepmainthread()