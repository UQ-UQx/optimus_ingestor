import base_service
import os
import utils

class CourseStructure(base_service.BaseService):
    pass


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    required_files = []
    main_path = os.path.join(path, 'database_state', 'latest')
    for subdir in os.listdir(main_path):
        if os.path.isdir(os.path.join(main_path, subdir)):
            required_files.append(os.path.join(main_path, subdir))
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "CourseStructure"


def service():
    """
    Returns an instance of the service
    """
    return CourseStructure()