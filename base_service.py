from utils import *
import time

class BaseService(object):
    """
    The base class which all services extend
    """

    def __init__(self):

        #Auto variables
        #The lowercase name of the service
        self.servicename = ""
        #The current state of the service
        self.status = 'stopped'
        #The last time the service was running
        self.last_awake = ''

        #Overriden variables
        #The pretty name of the service
        self.pretty_name = "Unknown Service"
        #Whether the service is enabled
        self.enabled = False
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

    # Starting the base service
    def initialize(self):
        """
        The initialisation method, sets default status and begins the run loop
        """
        if self.enabled:
            self.servicename = str(self.__class__.__name__).lower()
            log("Starting service "+self.servicename)
            self.status = 'loading'

            self.setup()
            while self.loop:
                self.status = 'running'
                self.run()
                self.status = 'sleeping'
                self.last_awake = time.strftime('%Y-%m-%d %H:%M:%S')
                time.sleep(self.sleep_time)

    def setup(self):
        """
        Setup function, this should be overridden by the service
        """
        log("BAD METHOD, SETUP SHOULD BE SUBCLASSED IN "+self.servicename)

    def run(self):
        """
        Run function, this should be overridden by the service
        """