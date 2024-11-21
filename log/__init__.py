"""
log library
"""
import logging
import sys

from config import log_format
from log.log_handler import get_log_handler

log_file_name = "log_collector_v2_pub.log"


logging.basicConfig(stream=sys.stdout, filemode="a", format=log_format, level=logging.INFO)
logger = logging.getLogger()

