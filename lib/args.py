import os
import argparse


def get_args():

    parser = argparse.ArgumentParser(description='Mongodb Log Monitoring.')
    parser.add_argument('--filepath', dest='filepath', action='store', default="/data/log/mongodb.log")
    parser.add_argument('--run-mode', dest='run_mode', action='store', default="publisher")
    args = parser.parse_args()
    
    mongodb_log_path = args.filepath
    run_mode = args.run_mode

    try:
        mongodb_log_path_env = os.environ['MONGO_LOG']
    except Exception as e:
        mongodb_log_path_env = None
    
    try:
        run_mode_env = os.environ['RUN_MODE']
    except Exception as e:
        run_mode_env = None
    
    if run_mode_env is not None:
        run_mode = run_mode_env
    
    if mongodb_log_path_env is not None:
        mongodb_log_path = mongodb_log_path_env
        
    return mongodb_log_path, run_mode
