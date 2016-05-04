#!/usr/bin/env python
"""
Initiates the Ingestor
"""
from tendo import singleton
import utils
from service_handler import Servicehandler


me = singleton.SingleInstance()

logger = utils.getLogger(__name__)
logger.info("Starting Ingestor")

ingestor = Servicehandler()


