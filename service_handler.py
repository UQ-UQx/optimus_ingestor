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

from services.database_state.service import load_file


class ServiceManager():

    servicethreads = []
    servicemodules = []
    sql_db = None

    def __init__(self):
        log("Starting Service Manager")
        self.setup_ingest_database()
        self.autoload()

    def autoload(self):
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
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        #Create and connect to the API database
        try:
            self.sql_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db='api')
        except MySQLdb.OperationalError, e:
            self.sql_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db='mysql')
            cur = self.sql_db.cursor()
            cur.execute("CREATE DATABASE API")
            self.sql_db = MySQLdb.connect(host=config.SQL_HOST, user=config.SQL_USERNAME, passwd=config.SQL_PASSWORD, db='api')
        if self.sql_db:
            print "ALL GOOD"
        #Create the ingestor table if necessary
        cur = self.sql_db.cursor()
        query = "CREATE TABLE IF NOT EXISTS ingestor ( "
        query += "id int NOT NULL UNIQUE AUTO_INCREMENT, service_name varchar(255), type varchar(255), meta varchar(255), started int DEFAULT 0, completed int DEFAULT 0, created datetime NULL, started_date datetime NULL, completed_date datetime NULL, PRIMARY KEY (id)"
        query += ");"
        cur.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)

    def add_to_ingestion(self, service_name, type, meta):
        cur = self.sql_db.cursor()
        created = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = "INSERT INTO ingestor "
        query += "(service_name,type,meta,created) VALUES ("
        query += '"'+service_name+'","'+type+'","'+meta+'","'+created+'"'
        query += ");"
        print query
        cur.execute(query)
        self.sql_db.commit()
        pass

    def start_ingest(self, id):
        pass

    def finish_ingest(self, id):
        pass


def queue_data(servicehandler):
    for path in config.DATA_PATHS:
        for service_module in ServiceManager.servicemodules:
            required_files = service_module.get_files(path)
            for required_file in required_files:
                #Add file to the ingestion table
                pass
                servicehandler.manager.add_to_ingestion(service_module.name, 'file', os.path.realpath(required_file))
    return True


class RequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    response = 0
    servicehandler = None

    def runmodule(self, modulename, meta):
        servicespath = os.path.join(basepath, 'services')
        servicename = modulename
        servicepath = os.path.join(servicespath, modulename, 'service.py')
        if os.path.exists(servicepath):
            pass
            #log("Starting once-off module "+servicename)
            #servicemodule = baseservice.load_module(servicename)
            #print meta
            #servicethread = threading.Thread(target=servicemodule.runservice, args=meta)
            #servicethread.start()

    def do_GET(self):
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
            if queue_data(servicehandler):
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

    allow_reuse_address = True

    def shutdown(self):
        self.socket.close()
        BaseHTTPServer.HTTPServer.shutdown(self)


class Servicehandler():
    """
    Service handler deals with the web server
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
        RequestHandler.servicehandler = self
        queue_data(self)
        server_address = ('0.0.0.0', ServerPort)
        RequestHandler.protocol_version = Protocol
        self.server = ThreadedHTTPServer(server_address, RequestHandler)
        log("Starting Web Server")
        self.start_webserver()
        log("Sleeping Main Thread")
        self.sleepmainthread()

    def sleepmainthread(self):
        sleep(2)
        self.sleepmainthread()

    def start_webserver(self):
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()