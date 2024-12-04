"""service library for user_cmds"""
from model.user_command import UserCommand, table_name
import logging


def select_value(cursor=None, value: UserCommand = None):
    """select value from user_cmds"""
    from lib.mysql import db_read_handle, select_datas
    value_list = []
    values = select_datas(db_read_handle,
                          table=table_name,
                          where=value.where_all())
    for val in values:
        value_list.append(val)
    return value_list


def insert_value(values: list, cursor=None,
                 auto_commit=True, filter_str="",
                 database_name="", host="",
                 real_write=True):
    """insert single value to user_cmds"""
    from lib.mysql import db_write_handle, insert_data
    if len(values) != 6:
        return
    if cursor is None:
        t_cursor = db_write_handle.cursor()
    else:
        t_cursor = cursor
    user = "Unknown"
    values.append(user)
    values.append(filter_str)
    values.append(database_name)
    values.append(host)
    user_cmd = UserCommand.create(*values)
    if real_write:
        insert_data(conn=db_write_handle,
                    cursor=t_cursor,
                    table=table_name,
                    value=user_cmd,
                    auto_commit=auto_commit)
    else:
        logging.info("table = %s data = %s", table_name, user_cmd)

    if cursor is None:
        t_cursor.close()


def insert_value_1(values: list, cursor=None,
                   auto_commit=True, filter_str="",
                   database_name="", host="",
                   real_write=True):
    """insert value"""
    from lib.mysql import db_write_handle, insert_data
    if len(values) != 5:
        return
    if cursor is None:
        t_cursor = db_write_handle.cursor()
    else:
        t_cursor = cursor
    user = "Unknown"
    client = "Unknown"
    values.insert(0, client)
    values.append(user)
    values.append(filter_str)
    values.append(database_name)
    values.append(host)
    user_cmd = UserCommand.create(*values)
    if real_write:
        insert_data(conn=db_write_handle,
                    cursor=t_cursor,
                    table=table_name,
                    value=user_cmd,
                    auto_commit=auto_commit)
    else:
        logging.info("table = %s data = %s", table_name, user_cmd)
    if cursor is None:
        t_cursor.close()
