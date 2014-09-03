import base_service
import os
from utils import *

class DatabaseState(base_service.BaseService):

    inst = None

    def __init__(self):
        DatabaseState.inst = self
        super(DatabaseState, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Database State"
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
    main_path = os.path.join(path, 'database_state', 'latest')
    for filename in os.listdir(main_path):
        extension = os.path.splitext(filename)[1]
        if extension == '.sql':
            required_files.append(os.path.join(main_path, filename))
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "DatabaseState"


def service():
    """
    Returns an instance of the service
    """
    return DatabaseState()