import base_service
import os
import utils

class Clickstream(base_service.BaseService):
    pass


def get_files(path):
    required_files = []
    main_path = os.path.join(path, 'clickstream_logs', 'latest')
    for subdir in os.listdir(main_path):
        if os.path.isdir(os.path.join(main_path, subdir)):
            for filename in os.listdir(os.path.join(main_path, subdir)):
                extension = os.path.splitext(filename)[1]
                if extension == '.log':
                    required_files.append(os.path.join(main_path, subdir, filename))
    return required_files


def name():
    return "Clickstream"


def service():
    return Clickstream()