import base_service
import os
import utils

class DiscussionForums(base_service.BaseService):
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