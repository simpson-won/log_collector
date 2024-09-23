from log import logger
from service.backend_api import send_user_access, send_user_command


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
    date = log_dict["t"]["$date"]
    client = log_dict["attr"]["client"]
    user = log_dict["attr"]["user"]
    db = log_dict["attr"]["db"]
    ctx = log_dict["ctx"]
    logger.info(f'check_authenticated: {date} ctx = {ctx}, cmd = AUTH, client = {client}, user = {user}, db = {db}')
    send_user_access(date=date, ctx=ctx, cmd="AUTH", client=client, user=user, db=db)


def check_connection_ended(log_dict):
    date = log_dict["t"]["$date"]
    client = log_dict["attr"]["remote"]
    ctx = log_dict["ctx"]
    logger.info(f'check_connection_ended: {date} ctx = {ctx}, cmd = DISCON, client = {client}')


def check_returning_user_from_cache(log_dict):
    pass


def check_command(log_dict):
    cmd_info = log_dict["attr"]["commandArgs"]
    cmd_keys = list(cmd_info.keys())
    cmd = cmd_keys[0]
    client = log_dict["attr"]["client"]
    if cmd not in exclude_cmds and len(client) > 0:
        table = cmd_info.get(cmd)
        db = log_dict["attr"]["db"]
        date = log_dict["t"]["$date"]
        ctx = log_dict["ctx"]
        logger.info(f'=== {date} ctx = {ctx}, cmd = {cmd}, client = {client}, table = {table}, db = {db}')
        send_user_command(date=date, ctx=ctx, cmd=cmd, client=client, table_name=table, db=db)
        return
    if "speculativeAuthenticate" in cmd_info.keys:
        auth_info = cmd_info['speculativeAuthenticate']
        db = auth_info['db']
        user = "userdb.won@aimmo.co.kr".replace(db + ".", "")
        ctx = log_dict["ctx"]
        date = log_dict["t"]["$date"]
        send_user_access(date=date, ctx=ctx, cmd=cmd, client=client, user=user, db=db)
