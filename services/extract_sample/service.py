import base_service
import os
import utils

class ExtractSample(base_service.BaseService):
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
    return "ExtractSample"


def service():
    """
    Returns an instance of the service
    """
    return ExtractSample()