#!/usr/bin/env python

import os
import logging

basepath = os.path.dirname(__file__)

#Logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(filename=basepath+'/logs/debug.log', level=logging.DEBUG, formatter=formatter)
logger = logging.getLogger(__name__)