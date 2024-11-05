from service.user_access_svc import insert_value as insert_ua_value
from service.user_cmds_svc import insert_value as insert_uc_value
from service.user_cmds_svc import insert_value_1 as insert_uc_value_1
import logging


ignore_lines = ["No drop-pending idents have expired",
                "Removing historical entries older than",
                "Slow query",
                "Received interrupt request for unknown op",
                "Scanning sessions",
                "running TTL job for index",
                "Completed unstable checkpoint.",
                "Finished checkpoint, updated iteration counter",
                "WiredTiger message",
                "Checkpoint thread sleeping",
                "Deleted expired documents using index",
                "cleaning up unused lock buckets of the global lock manager",
                "Collection does not exist. Using EOF plan",
                "WiredTigerSizeStorer::store",
                "Using idhack",
                "Connection ended",
                "Received first command on ingress connection since session start or auth handshake",
                "Checking authorization failed",
                "User assertion",
                "Terminating session due to error",
                "Ending session",
                "Session from remote encountered a network error during SourceMessage",
                "Assertion while executing command",
                "Command not found in registry",
                "Interrupted operation as its client disconnected",
                "client metadata",
                "Auth metrics report",
                "Only one plan is available",
                ]

exclude_cmds = ['hello',
                'ping',
                'ismaster',
                'getLog',
                'isMaster',
                'getParameter',
                'isClusterMember',
                'aggregate',
                'buildInfo',
                'saslStart',
                'saslContinue',
                'endSessions',
                'connectionStatus',
                'listCollections',
                'listDatabases',
                'hostInfo',
                'top',
                'replSetUpdatePosition',
                'replSetHeartbeat',
                'getMore',
                'serverStatus',
                'dbStats',
                'getCmdLineOpts',
                ]

exclude_dbs = ['config', 'local', 'admin']
exclude_users = ['__system', 'admin']


def check_accept_state(log_dict, real_write=True):
    pass


def check_authenticated(log_dict, args=[], real_write=True):
    if "user" in log_dict["attr"] and log_dict["attr"]["user"] not in exclude_users:
        insert_ua_value((log_dict["attr"]["client"],
                         log_dict["ctx"],
                         log_dict["t"]["$date"],
                         log_dict["attr"]["db"],
                         log_dict["attr"]["user"],
                         args[0],
                         args[1]),
                        real_write=real_write)


def check_authenticated2(log_dict, args=[], real_write=True):
    if "user" in log_dict["attr"] and log_dict["attr"]["user"] not in exclude_users:
        insert_ua_value((log_dict["attr"]["remote"],
                         log_dict["ctx"],
                         log_dict["t"]["$date"],
                         log_dict["attr"]["authenticationDatabase"],
                         log_dict["attr"]["principalName"],
                         args[0],
                         args[1]),
                        real_write=real_write)


def check_connection_ended(log_dict, args=[], real_write=True):
    pass


def check_returning_user_from_cache(log_dict, args=[], real_write=True):
    pass


def cmd_process_4_run(log_dict, args=[], real_write=True):
    cmd_args = log_dict["attr"]["commandArgs"]
    cmd_keys = list(cmd_args.keys())
    if cmd_keys[0] not in exclude_cmds:
        if log_dict["attr"]["db"] not in exclude_dbs:
            if cmd_keys[0] == "find":
                if "limit" in cmd_args:
                    filter_str = "filters = " + str(cmd_args.get("filter")) + ", limit = " + str(cmd_args.get("limit"))
                else:
                    filter_str = "filters = " + str(cmd_args.get("filter"))
            elif cmd_keys[0] == "update":
                filter_str = "filters = " + str(cmd_args.get("filter"))
            else:
                filter_str = ""
            insert_uc_value(values=[log_dict["attr"]["client"],
                                    cmd_keys[0],
                                    log_dict["ctx"],
                                    log_dict["t"]["$date"],
                                    log_dict["attr"]["db"],
                                    cmd_args.get(cmd_keys[0])],
                            filter_str=filter_str,
                            database_name=args[0],
                            host=args[1], real_write=real_write)


def cmd_process_on_find(log_dict, args=[], real_write=True):
    client = log_dict["attr"]["remote"]
    cmd_key = "find"
    ctx = log_dict["ctx"]
    date = log_dict["t"]["$date"]
    db = log_dict["attr"]["ns"].split(".")[0]
    table_name = log_dict["attr"]["command"].get(cmd_key)
    filter_str = str(log_dict["attr"]["command"]["filter"])[0:1000]
    
    insert_uc_value(values=[client, cmd_key, ctx, date, db, table_name],
                    filter_str=filter_str,
                    database_name=args[0],
                    host=args[1],
                    real_write=real_write)


def cmd_process_on_update(log_dict, args=[], real_write=True):
    client = log_dict["attr"]["remote"]
    cmd_key = "update"
    ctx = log_dict["ctx"]
    date = log_dict["t"]["$date"]
    db = log_dict["attr"]["ns"].split(".")[0]
    table_name = log_dict["attr"]["command"].get(cmd_key)
    
    if ctx.startswith('ReplWriterWorker'):
        return

    if db not in exclude_dbs:
        insert_uc_value(values=[client, cmd_key, ctx, date, db, table_name],
                        filter_str="",
                        database_name=args[0],
                        host=args[1],
                        real_write=real_write)


def index_process_on_create_index(log_dict, args=[], real_write=True):
    cmd_key = "createIndexes"
    ctx = log_dict['ctx']
    date = log_dict["t"]["$date"]
    db = log_dict["attr"]["namespace"].split(".")[0]
    table_name = log_dict["attr"]["command"].get(cmd_key)
    index_args = str(log_dict['attr']['command']['indexes'])
    
    if ctx.startswith('ReplWriterWorker'):
        return

    if db not in exclude_dbs:
        insert_uc_value_1(values=[cmd_key, ctx, date, db, table_name],
                          filter_str=index_args,
                          database_name=args[0],
                          host=args[1],
                          real_write=real_write)


def cmd_process_on_drop_index(log_dict, args=[], real_write=True):
    cmd_key = "dropIndexes"
    ctx = log_dict['ctx']
    date = log_dict["t"]["$date"]
    db, table_name = log_dict["attr"]["namespace"].split(".")
    index_args = log_dict['attr']['indexes']
    
    if ctx.startswith('ReplWriterWorker'):
        return

    if db not in exclude_dbs:
        insert_uc_value_1(values=[cmd_key, ctx, date, db, table_name],
                          filter_str=index_args,
                          database_name=args[0],
                          host=args[1],
                          real_write=real_write)


def storage_process_on_create_collection(log_dict, args=[], real_write=True):
    cmd_key = "createCollection"
    ctx = log_dict['ctx']
    date = log_dict["t"]["$date"]
    db, table_name = log_dict["attr"]["namespace"].split(".")
    if "options" in log_dict["attr"]:
        args_str = log_dict["attr"]["options"]
    else:
        args_str = ""
    
    if ctx.startswith('ReplWriterWorker'):
        return
    
    if db not in exclude_dbs:
        insert_uc_value_1(values=[cmd_key, ctx, date, db, table_name],
                          filter_str=args_str,
                          database_name=args[0],
                          host=args[1],
                          real_write=real_write)


def cmd_procees_on_drop(log_dict, args=[], real_write=True):
    cmd_key = "drop"
    ctx = log_dict['ctx']
    date = log_dict["t"]["$date"]
    db, table_name = log_dict["attr"]["namespace"].split(".")
    index_args = ""
    
    if ctx.startswith('ReplWriterWorker'):
        return

    if db not in exclude_dbs:
        insert_uc_value_1(values=[cmd_key, ctx, date, db, table_name],
                          filter_str=index_args,
                          database_name=args[0],
                          host=args[1],
                          real_write=real_write)


detailed_cmds = {"find": cmd_process_on_find,
                 "update": cmd_process_on_update}


def cmd_process_4_slowquery(log_dict, args=[], real_write=True):
    command_dict = log_dict['attr']['command']
    command = [*command_dict.keys()][0]
    if command in detailed_cmds:
        detailed_cmds[command](log_dict, args, real_write)


monitoring_lines = {"Connection accepted": check_accept_state,
                    "Returning user from cache": check_returning_user_from_cache,
                    "Connection ended": check_connection_ended,
                    }


processes_of_access = {
    "Successfully authenticated": check_authenticated,
    "Authentication succeeded": check_authenticated2,
}


processes_of_command = {
    "About to run the command": cmd_process_4_run,
    "Slow query": cmd_process_4_slowquery,
    "CMD: dropIndexes": cmd_process_on_drop_index,
    "CMD: drop": cmd_procees_on_drop
}


processes_of_storage = {
    "createCollection": storage_process_on_create_collection,
}


processes_of_index = {
    "Index build: registering": index_process_on_create_index,
}


def check_command(log_dict, args=[], real_write=True):
    if log_dict["msg"] in processes_of_command.keys():
        return processes_of_command[log_dict['msg']](log_dict, args, real_write=real_write)


def check_access(log_dict, args=[], real_write=True):
    if log_dict["msg"] in processes_of_access.keys():
        return processes_of_access[log_dict["msg"]](log_dict, args, real_write=real_write)


def check_storage(log_dict, args=[], real_write=True):
    if log_dict["msg"] in processes_of_storage.keys():
        return processes_of_storage[log_dict["msg"]](log_dict, args, real_write=real_write)


def check_index(log_dict, args=[], real_write=True):
    if log_dict["msg"] in processes_of_index.keys():
        return processes_of_index[log_dict["msg"]](log_dict, args, real_write=real_write)


log_c_process = {
    "ACCESS": check_access,
    "COMMAND": check_command,
    "INDEX": check_index,
    "STORAGE": check_storage
}


if __name__ == "__main__":
    import sys
    import signal
    import json
    from config import host, db_name
    
    def sig_exit(sig, arg):
        exit()
    
    signal.signal(signal.SIGTERM, sig_exit)
    
    if len(sys.argv) == 2:
        file_name = sys.argv[1]
    else:
        file_name = "test.log"
    
    with open(file_name, "rt") as log_fd:
        line = log_fd.readline()
        while line:
            if len(line) > 5:
                try:
                    log_json = json.loads(line)
                    if log_json['c'] in log_c_process.keys():
                        log_c_process[log_json['c']](log_json, [db_name, host], False)
                except json.JSONDecodeError as ex:
                    print(f'except : {ex}')
                    print(f'line = ##{line}##, len={len(line)}')
            line = log_fd.readline()
