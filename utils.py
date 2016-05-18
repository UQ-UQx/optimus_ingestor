import logging
import logging.handlers
import os
import threading

basepath = os.path.dirname(__file__)
logpath = os.path.join(basepath, 'log')
if not os.path.exists(logpath):
    os.makedirs(logpath)


def getLogger(name):
	logger = logging.getLogger(name)	
	if not len(logger.handlers):
	    logger.setLevel(logging.DEBUG)
	    logger.addHandler(createFileHandler(name))
	    logger.addHandler(createConsoleHandler(name))	    
	return logger
	
# All messages will be writen in to log files
# File paths based on the module which creates messages	
def createFileHandler(name):
	fh = logging.FileHandler(os.path.join(logpath, name + '.log'))
	fh.setLevel(logging.DEBUG)
	fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', '%d/%m/%Y %H:%M:%S'))
	return fh
	
# Msg Levels based on the thread which creates messages
def createConsoleHandler(name):
	ch = logging.StreamHandler()
	if threading.current_thread().name == 'MainThread':
	    #ch.setLevel(logging.INFO)
	    ch.setLevel(logging.DEBUG)
	else:
	    #ch.setLevel(logging.ERROR)
	    ch.setLevel(logging.DEBUG)
	ch.setFormatter(logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s', '%d/%m/%Y %H:%M:%S'))
	return ch