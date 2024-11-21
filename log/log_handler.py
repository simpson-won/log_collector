import gzip
import os
import shutil
import logging
import logging.handlers
from config import log_format

log_handler = None
log_dir = 'logs'
max_bytes = 200 * 1024 * 1024  # (200MB)
backup_count = 7

if not os.path.exists(log_dir):
    os.makedirs(log_dir)


def namer(name):
    return name + ".gz"


def rotator(source, dest):
    with open(source, 'rb') as f_in:
        with gzip.open(dest, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(source)


def get_log_handler(file_name):
    rh = logging.handlers.RotatingFileHandler(file_name=f'{log_dir}/{file_name}', maxBytes=max_bytes, backupCount=backup_count)
    rh.rotator = rotator
    rh.namer = namer
    
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(rh)
    # f = logging.Formatter('%(asctime)s %(message)s')
    f = logging.Formatter(log_format)
    rh.setFormatter(f)
