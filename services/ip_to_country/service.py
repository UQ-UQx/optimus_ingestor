import base_service
import os
import utils

class IPToCountry(base_service.BaseService):

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
        pass


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
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