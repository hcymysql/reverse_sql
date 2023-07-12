#!/usr/bin/env python3
import argparse
import time
import datetime
import pytz
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from queue import Queue
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)
from tqdm import tqdm

timezone = pytz.timezone('Asia/Shanghai')

result_queue = Queue()
result_queue_replace = Queue()
combined_array = []
combined_array_replace = []

# 创建一个锁对象
file_lock = threading.Lock()


def check_binlog_settings(mysql_host=None, mysql_port=None, mysql_user=None,
                          mysql_passwd=None, mysql_database=None, mysql_charset=None):
    # 连接 MySQL 数据库
    source_mysql_settings = {
        "host": mysql_host,
        "port": mysql_port,
        "user": mysql_user,
        "passwd": mysql_passwd,
        "database": mysql_database,
        "charset": mysql_charset
    }

    conn = pymysql.connect(**source_mysql_settings)
    cursor = conn.cursor()

    try:
        # 查询 binlog_format 的值
        cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
        row = cursor.fetchone()
        binlog_format = row[1]

        # 查询 binlog_row_image 的值
        cursor.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
        row = cursor.fetchone()
        binlog_row_image = row[1]

        # 检查参数值是否满足条件
        if binlog_format != 'ROW' and binlog_row_image != 'FULL':
            exit("\nMySQL 的变量参数 binlog_format 的值应为 ROW，参数 binlog_row_image 的值应为 FULL\n")

    finally:
        # 关闭数据库连接
        cursor.close()
        conn.close()


def process_binlogevent(binlogevent, start_time, end_time):
    database_name = binlogevent.schema

    if start_time <= binlogevent.timestamp <= end_time:
        for row in binlogevent.rows:
            event_time = binlogevent.timestamp

            if isinstance(binlogevent, WriteRowsEvent):
                if only_operation and only_operation != 'insert':
                    continue
                else:
                    sql = "INSERT INTO {}({}) VALUES ({});".format(
                        f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                        ','.join(["`{}`".format(k) for k in row["values"].keys()]),
                        ','.join(["'{}'".format(v) if isinstance(v, (
                            str, datetime.datetime)) else 'NULL' if v is None else str(v)
                                  for v in row["values"].values()])
                    )

                    rollback_sql = "DELETE FROM {} WHERE {};".format(f"`{database_name}`.`{binlogevent.table}`"
                                                                     if database_name else binlogevent.table,
                                                                     ' AND '.join(["`{}`={}".format(k, "'{}'".format(v)
                                                                     if isinstance(v, (str,
                                                                                       datetime.datetime)) else 'NULL' if v is None else str(
                                                                         v))
                                                                                   for k, v in row["values"].items()]))

                    result_queue.put({"event_time": event_time, "sql": sql, "rollback_sql": rollback_sql})

            elif isinstance(binlogevent, UpdateRowsEvent):
                if only_operation and only_operation != 'update':
                    continue
                else:
                    set_values = []
                    for k, v in row["after_values"].items():
                        if isinstance(v, str):
                            set_values.append(f"`{k}`='{v}'")
                        elif isinstance(v, datetime.datetime):
                            set_values.append(f"`{k}`='{v}'")  # 将时间字段转换为字符串形式
                        else:
                            set_values.append(f"`{k}`={v}" if v is not None else f"`{k}`= NULL")
                    set_clause = ','.join(set_values)

                    where_values = []
                    for k, v in row["before_values"].items():
                        if isinstance(v, str):
                            where_values.append(f"`{k}`='{v}'")
                        elif isinstance(v, datetime.datetime):
                            where_values.append(f"`{k}`='{v}'")  # 添加对时间类型的处理
                        else:
                            where_values.append(f"`{k}`={v}" if v is not None else f"`{k}` IS NULL")
                    where_clause = ' AND '.join(where_values)

                    sql = f"UPDATE `{database_name}`.`{binlogevent.table}` SET {set_clause} WHERE {where_clause};"

                    rollback_set_values = []
                    for k, v in row["before_values"].items():
                        if isinstance(v, str):
                            rollback_set_values.append(f"`{k}`='{v}'")
                        elif isinstance(v, datetime.datetime):
                            rollback_set_values.append(f"`{k}`='{v}'")  # 添加对时间类型的处理
                        else:
                            rollback_set_values.append(f"`{k}`={v}" if v is not None else f"`{k}`=NULL")
                    rollback_set_clause = ','.join(rollback_set_values)

                    rollback_where_values = []
                    for k, v in row["after_values"].items():
                        if isinstance(v, str):
                            rollback_where_values.append(f"`{k}`='{v}'")
                        elif isinstance(v, datetime.datetime):
                            rollback_where_values.append(f"`{k}`='{v}'")  # 添加对时间类型的处理
                        else:
                            rollback_where_values.append(f"`{k}`={v}" if v is not None else f"`{k}` IS NULL")
                    rollback_where_clause = ' AND '.join(rollback_where_values)

                    rollback_sql = f"UPDATE `{database_name}`.`{binlogevent.table}` SET {rollback_set_clause} WHERE {rollback_where_clause};"

                    try:
                        rollback_replace_set_values = []
                        for v in row["before_values"].values():
                            if v is None:
                                rollback_replace_set_values.append("NULL")
                            elif isinstance(v, (str, datetime.datetime)):
                                rollback_replace_set_values.append(f"'{v}'")
                            else:
                                rollback_replace_set_values.append(str(v))
                        rollback_replace_set_clause = ','.join(rollback_replace_set_values)
                        fields_clause = ','.join([f"`{k}`" for k in row["after_values"].keys()])
                        rollback_replace_sql = f"REPLACE INTO `{database_name}`.`{binlogevent.table}` ({fields_clause}) VALUES ({rollback_replace_set_clause});"
                    except Exception as e:
                        print("出现异常错误：", e)
                    # print(rollback_replace_sql)

                    result_queue.put({"event_time": event_time, "sql": sql, "rollback_sql": rollback_sql})
                    result_queue_replace.put(
                        {"event_time": event_time, "sql": sql, "rollback_sql": rollback_replace_sql})

            elif isinstance(binlogevent, DeleteRowsEvent):
                if only_operation and only_operation != 'delete':
                    continue
                else:
                    sql = "DELETE FROM {} WHERE {};".format(
                        f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                        ' AND '.join(["`{}`={}".format(k, "'{}'".format(v) if isinstance(v, (str, datetime.datetime))
                        else 'NULL' if v is None else str(v))
                                      for k, v in row["values"].items()])
                    )

                    rollback_sql = "INSERT INTO {}({}) VALUES ({});".format(
                        f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                        '`' + '`,`'.join(list(row["values"].keys())) + '`',
                        ','.join(["'%s'" % str(i) if isinstance(i, (
                        str, datetime.datetime)) else 'NULL' if i is None else str(i)
                                  for i in list(row["values"].values())])
                    )

                    result_queue.put({"event_time": event_time, "sql": sql, "rollback_sql": rollback_sql})


def main(only_tables=None, only_operation=None, mysql_host=None, mysql_port=None, mysql_user=None, mysql_passwd=None,
         mysql_database=None, mysql_charset=None, binlog_file=None, binlog_pos=None, st=None, et=None, max_workers=None,
         print_output=False, replace_output=False):
    valid_operations = ['insert', 'delete', 'update']

    if only_operation:
        only_operation = only_operation.lower()
        if only_operation not in valid_operations:
            print('请提供有效的操作类型进行过滤！')
            sys.exit(1)

    source_mysql_settings = {
        "host": mysql_host,
        "port": mysql_port,
        "user": mysql_user,
        "passwd": mysql_passwd,
        "database": mysql_database,
        "charset": mysql_charset
    }

    start_time = int(time.mktime(time.strptime(st, '%Y-%m-%d %H:%M:%S')))
    end_time = int(time.mktime(time.strptime(et, '%Y-%m-%d %H:%M:%S')))

    interval = (end_time - start_time) // max_workers  # 将时间范围划分为 10 等份
    executor = ThreadPoolExecutor(max_workers=max_workers)

    stream = BinLogStreamReader(
        connection_settings=source_mysql_settings,
        server_id=1234567890,
        blocking=False,
        resume_stream=True,
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        log_file=binlog_file,
        log_pos=int(binlog_pos),
        only_tables=only_tables
    )

    next_binlog_file = binlog_file
    next_binlog_pos = binlog_pos

    next_binlog_file_lock = threading.Lock()
    next_binlog_pos_lock = threading.Lock()

    for i in range(max_workers):
        task_start_time = start_time + i * interval
        task_end_time = task_start_time + interval
        if i == (max_workers - 1):
            # task_end_time = end_time - (max_workers-1) * interval
            task_end_time = end_time

        tasks = []

        # 创建进度条对象
        progress_bar = tqdm(desc='Processing binlogevents', unit='event')

        event_count = 0  # 初始化事件计数器

        for binlogevent in stream:
            event_count += 1  # 每迭代一次，计数器加一
            # 更新进度条
            progress_bar.update(1)
        #for binlogevent in tqdm(stream, desc='Processing binlogevents', unit='event'):
            if binlogevent.timestamp < task_start_time:  # 如果事件的时间小于任务的起始时间，则继续迭代下一个事件
                continue
            elif binlogevent.timestamp > task_end_time:  # 如果事件的时间大于任务的结束时间，则结束该任务的迭代
                break
            task = executor.submit(process_binlogevent, binlogevent, task_start_time, task_end_time)

            with next_binlog_file_lock:
                if stream.log_file > next_binlog_file:
                    next_binlog_file = stream.log_file

            with next_binlog_pos_lock:
                if stream.log_file == next_binlog_file and stream.log_pos > next_binlog_pos:
                    next_binlog_pos = stream.log_pos
            """
            with next_binlog_file_lock:
                next_binlog_file = stream.log_file

            with next_binlog_pos_lock:
                next_binlog_pos = stream.log_pos
            """
            tasks.append(task)

            # 刷新进度条显示
            progress_bar.refresh()

        wait(tasks)

        stream.close()

        stream = BinLogStreamReader(
            connection_settings=source_mysql_settings,
            server_id=1234567890,
            blocking=False,
            resume_stream=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            log_file=next_binlog_file,
            log_pos=int(next_binlog_pos),
            only_tables=only_tables
        )

        # 设置进度条的总长度为事件计数器的值
        progress_bar.total = event_count

        # 完成后关闭进度条
        progress_bar.close()

    while not result_queue.empty():
        combined_array.append(result_queue.get())

    while not result_queue_replace.empty():
        combined_array_replace.append(result_queue_replace.get())

    sorted_array = sorted(combined_array, key=lambda x: x["event_time"])
    sorted_array_replace = sorted(combined_array_replace, key=lambda x: x["event_time"])

    c_time = datetime.datetime.now()
    formatted_time = c_time.strftime("%Y-%m-%d_%H:%M:%S")

    for item in sorted_array:
        event_time = item["event_time"]
        dt = datetime.datetime.fromtimestamp(event_time, tz=timezone)
        current_time = dt.strftime('%Y-%m-%d %H:%M:%S')

        sql = item["sql"]
        rollback_sql = item["rollback_sql"]

        if print_output:
            print(
                f"-- SQL执行时间:{current_time} \n-- 原生sql:\n \t-- {sql} \n-- 回滚sql:\n \t{rollback_sql}\n-- ----------------------------------------------------------\n")

        # 写入文件
        filename = f"{binlogevent.schema}_{binlogevent.table}_recover_{formatted_time}.sql"
        # filename = f"{binlogevent.schema}_{binlogevent.table}_recover.sql"
        with file_lock:  # 获取文件锁
            with open(filename, "a", encoding="utf-8") as file:
                file.write(f"-- SQL执行时间:{current_time}\n")
                file.write(f"-- 原生sql:\n \t-- {sql}\n")
                file.write(f"-- 回滚sql:\n \t{rollback_sql}\n")
                file.write("-- ----------------------------------------------------------\n")

    if replace_output:
        # update 转换为 replace
        for item in sorted_array_replace:
            event_time = item["event_time"]
            dt = datetime.datetime.fromtimestamp(event_time, tz=timezone)
            current_time = dt.strftime('%Y-%m-%d %H:%M:%S')

            sql = item["sql"]
            rollback_sql = item["rollback_sql"]

            if print_output:
                print(
                    f"-- SQL执行时间:{current_time} \n-- 原生sql:\n \t-- {sql} \n-- 回滚sql:\n \t{rollback_sql}\n-- ----------------------------------------------------------\n")

            # 写入文件
            filename = f"{binlogevent.schema}_{binlogevent.table}_recover_{formatted_time}_replace.sql"
            # filename = f"{binlogevent.schema}_{binlogevent.table}_recover.sql"
            with file_lock:  # 获取文件锁
                with open(filename, "a", encoding="utf-8") as file:
                    file.write(f"-- SQL执行时间:{current_time}\n")
                    file.write(f"-- 原生sql:\n \t-- {sql}\n")
                    file.write(f"-- 回滚sql:\n \t{rollback_sql}\n")
                    file.write("-- ----------------------------------------------------------\n")

    stream.close()
    executor.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binlog数据恢复，生成反向SQL语句。", epilog=r"""
Example usage:
    shell> ./reverse_sql -ot table1 -op delete -H 192.168.198.239 -P 3336 -u admin -p hechunyang -d hcy \
            --binlog-file mysql-bin.000124 --start-time "2023-07-06 10:00:00" --end-time "2023-07-06 22:00:00" """,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-ot", "--only-tables", dest="only_tables", nargs="+", type=str, help="设置要恢复的表，多张表用,逗号分隔")
    parser.add_argument("-op", "--only-operation", dest="only_operation", type=str,
                        help="设置误操作时的命令（insert/update/delete）")
    parser.add_argument("-H", "--mysql-host", dest="mysql_host", type=str, help="MySQL主机名", required=True)
    parser.add_argument("-P", "--mysql-port", dest="mysql_port", type=int, help="MySQL端口号", required=True)
    parser.add_argument("-u", "--mysql-user", dest="mysql_user", type=str, help="MySQL用户名", required=True)
    parser.add_argument("-p", "--mysql-passwd", dest="mysql_passwd", type=str, help="MySQL密码", required=True)
    parser.add_argument("-d", "--mysql-database", dest="mysql_database", type=str, help="MySQL数据库名", required=True)
    parser.add_argument("-c", "--mysql-charset", dest="mysql_charset", type=str, default="utf8", help="MySQL字符集，默认utf8")
    parser.add_argument("--binlog-file", dest="binlog_file", type=str, help="Binlog文件", required=True)
    parser.add_argument("--binlog-pos", dest="binlog_pos", type=int, default=4, help="Binlog位置，默认4")
    parser.add_argument("--start-time", dest="st", type=str, help="起始时间", required=True)
    parser.add_argument("--end-time", dest="et", type=str, help="结束时间", required=True)
    parser.add_argument("--max-workers", dest="max_workers", type=int, default=4, help="线程数，默认4（并发越高，锁的开销就越大，适当调整并发数）")
    parser.add_argument("--print", dest="print_output", action="store_true", help="将解析后的SQL输出到终端")
    parser.add_argument("--replace", dest="replace_output", action="store_true", help="将update转换为replace操作")
    args = parser.parse_args()

    if args.only_tables:
        only_tables = args.only_tables[0].split(',') if args.only_tables else None
    else:
        only_tables = None

    if args.only_operation:
        only_operation = args.only_operation.lower()
    else:
        only_operation = None

    # 环境检查
    check_binlog_settings(
        mysql_host=args.mysql_host,
        mysql_port=args.mysql_port,
        mysql_user=args.mysql_user,
        mysql_passwd=args.mysql_passwd,
        mysql_database=args.mysql_database,
        mysql_charset=args.mysql_charset
    )

    main(
        only_tables=only_tables,
        only_operation=only_operation,
        mysql_host=args.mysql_host,
        mysql_port=args.mysql_port,
        mysql_user=args.mysql_user,
        mysql_passwd=args.mysql_passwd,
        mysql_database=args.mysql_database,
        mysql_charset=args.mysql_charset,
        binlog_file=args.binlog_file,
        binlog_pos=args.binlog_pos,
        st=args.st,
        et=args.et,
        max_workers=args.max_workers,
        print_output=args.print_output,
        replace_output=args.replace_output
    )

