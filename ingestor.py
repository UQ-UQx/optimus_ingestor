#!/usr/bin/env python
"""
Initiates the Ingestor
"""

import utils
from service_handler import Servicehandler

from tendo import singleton
me = singleton.SingleInstance()

utils.log("Starting Ingestor")

ingestor = Servicehandler()