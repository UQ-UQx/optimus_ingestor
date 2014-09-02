import base_service
import os
import utils

class Database_State(base_service.BaseService):
    pass


def get_files(path):
    required_files = []
    main_path = os.path.join(path, 'database_state', 'latest')
    for filename in os.listdir(main_path):
        extension = os.path.splitext(filename)[1]
        if extension == '.sql':
            required_files.append(os.path.join(main_path,filename))
    return required_files

name = "Database_State"