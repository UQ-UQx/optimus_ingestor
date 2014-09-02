import inspect
import os
import logging
import datetime
import config

basepath = os.path.dirname(__file__)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
loggers = {}


def log(message):
    """
    Logs a message to screen and to the file
    :param message:
    """
    class_name = os.path.splitext(os.path.basename(inspect.stack()[1][1]))[0]
    if class_name not in loggers:
        log_file = basepath + '/logs/' + class_name + '.log'
        new_logger = logging.getLogger(class_name)
        new_log_handler = logging.FileHandler(log_file)
        new_logger.addHandler(new_log_handler)
        loggers[class_name] = new_logger
    the_log = loggers[class_name]
    the_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = the_time + ":" + " " + message
    the_log.info(log_message)