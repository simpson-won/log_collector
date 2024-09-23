import logging
from logging import Logger
import sys

from config import log_format

logging.basicConfig(stream=sys.stdout, filemode="a", format=log_format, level=logging.INFO)
logger = logging.getLogger()
