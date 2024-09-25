from log import logger
import json
from service.user_access_svc import insert_value as insert_ua_value
from service.user_cmds_svc import insert_value as insert_uc_value


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

exclude_dbs = ['config']
exclude_users = ['__system']


def check_accept_state(log_dict):
    pass


def check_authenticated(log_dict):
    # logger.info(f'check_authenticated: {log_dict["attr"]["client"]}, {log_dict["ctx"]}, {log_dict["t"]["$date"]}, {log_dict["attr"]["db"]}, {log_dict["attr"]["user"]}')
    insert_ua_value(log_dict["attr"]["client"],
                    log_dict["ctx"],
                    log_dict["t"]["$date"],
                    log_dict["attr"]["db"],
                    log_dict["attr"]["user"])


def check_connection_ended(log_dict):
    pass


def check_returning_user_from_cache(log_dict):
    pass


def check_command(log_dict):
    # logger.info(f'check_command: {log_dict}')
    cmd_info = log_dict["attr"]["commandArgs"]
    cmd_keys = list(cmd_info.keys())
    if cmd_keys[0] not in exclude_cmds:
        if log_dict["attr"]["db"] not in exclude_dbs:
            # logger.info(f'check_command: date={date}, ctx={ctx}, cmd={cmd}, client={client}, table_name={table}, db={db}')
            insert_uc_value([log_dict["attr"]["client"],
                             cmd_keys[0],
                             log_dict["ctx"],
                             log_dict["t"]["$date"],
                             log_dict["attr"]["db"],
                             cmd_info.get(cmd_keys[0])])
        return


monitoring_lines = {"Connection accepted": check_accept_state,
                    "Returning user from cache": check_returning_user_from_cache,
                    "About to run the command": check_command,
                    "Successfully authenticated": check_authenticated,
                    "Connection ended": check_connection_ended,
                    }


def data_parse_process_internal(data):
    # logger.info(f'data_parse_process_internal: data={data}')
    log_dict = json.loads(str(data))
    if log_dict["msg"] in monitoring_lines.keys():
        # logger.info(f'data_parse_process_internal: log_dict={log_dict}')
        monitoring_lines[log_dict["msg"]](log_dict)


def data_parse_process(data):
    try:
        if type(data) in [str, bytes]:
            if type(data) == bytes:
                data = data.decode('utf-8')
            if data.startswith('{'):
                log_dict = json.loads(str(data))
                if log_dict["msg"] in monitoring_lines.keys():
                    monitoring_lines[log_dict["msg"]](log_dict)
    except Exception as e:
        logger.error(f'data_parse_process: Exception\n\t\t\{e}')

