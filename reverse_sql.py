#!/usr/bin/env python3
import argparse
import time
import datetime
import pytz
import sys
from concurrent.futures import ThreadPoolExecutor, wait
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)

timezone = pytz.timezone('Asia/Shanghai')

combined_array = []

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
    event_time = binlogevent.timestamp

    if start_time <= binlogevent.timestamp <= end_time:
        for row in binlogevent.rows:
            if isinstance(binlogevent, WriteRowsEvent):
                if only_operation and only_operation != 'insert':
                    continue
                else:
                    sql = "INSERT INTO {}({}) VALUES ({})".format(
                        f"{database_name}.{binlogevent.table}" if database_name else binlogevent.table,
                        ','.join(["`{}`".format(k) for k in row["values"].keys()]),
                        ','.join(["'{}'".format(v) if isinstance(v, (str, datetime.datetime)) else str(v) for v in
                                  row["values"].values()])
                    )

                    rollback_sql = "DELETE FROM {} WHERE {}".format(f"{database_name}.{binlogevent.table}"
                                if database_name else binlogevent.table, ' AND '.join(["`{}`={}".format(k, "'{}'".format(v)
                                if isinstance(v, (str, datetime.datetime)) else v) for k, v in row["values"].items()]))

                    combined_array.append({"event_time": event_time, "sql": sql, "rollback_sql": rollback_sql})

            elif isinstance(binlogevent, UpdateRowsEvent):
                if only_operation and only_operation != 'update':
                    continue
                else:
                    set_values = []
                    for k, v in row["after_values"].items():
                        if isinstance(v, str):
                            set_values.append(f"`{k}`='{v}'")
                        else:
                            set_values.append(f"`{k}`={v}")
                    set_clause = ','.join(set_values)

                    where_values = []
                    for k, v in row["before_values"].items():
                        if isinstance(v, str):
                            where_values.append(f"`{k}`='{v}'")
                        else:
                            where_values.append(f"`{k}`={v}")
                    where_clause = ' AND '.join(where_values)

                    sql = f"UPDATE {database_name}.{binlogevent.table} SET {set_clause} WHERE {where_clause}"

                    rollback_set_values = []
                    for k, v in row["before_values"].items():
                        if isinstance(v, str):
                            rollback_set_values.append(f"`{k}`='{v}'")
                        else:
                            rollback_set_values.append(f"`{k}`={v}")
                    rollback_set_clause = ','.join(rollback_set_values)

                    rollback_where_values = []
                    for k, v in row["after_values"].items():
                        if isinstance(v, str):
                            rollback_where_values.append(f"`{k}`='{v}'")
                        else:
                            rollback_where_values.append(f"`{k}`={v}")
                    rollback_where_clause = ' AND '.join(rollback_where_values)

                    rollback_sql = f"UPDATE {database_name}.{binlogevent.table} SET {rollback_set_clause} WHERE {rollback_where_clause}"

                    combined_array.append({"event_time": event_time, "sql": sql, "rollback_sql": rollback_sql})

            elif isinstance(binlogevent, DeleteRowsEvent):
                if only_operation and only_operation != 'delete':
                    continue
                else:
                    sql = "DELETE FROM {} WHERE {}".format(
                        f"{database_name}.{binlogevent.table}" if database_name else binlogevent.table,
                        ' AND '.join(
                            ["`{}`={}".format(k, "'{}'".format(v) if isinstance(v, (str, datetime.datetime)) else v) for
                             k, v in row["values"].items()])
                    )

                    rollback_sql = "INSERT INTO {}({}) VALUES ({})".format(
                        f"{database_name}.{binlogevent.table}" if database_name else binlogevent.table,
                        '`' + '`,`'.join(list(row["values"].keys())) + '`',
                        ','.join(["'%s'" % str(i) if isinstance(i, (str, datetime.datetime)) else str(i) for i in
                                  list(row["values"].values())])
                    )

                    combined_array.append({"event_time": event_time, "sql": sql, "rollback_sql": rollback_sql})


def main(only_tables=None, only_operation=None, mysql_host=None, mysql_port=None, mysql_user=None, mysql_passwd=None,
         mysql_database=None, mysql_charset=None, binlog_file=None, binlog_pos=None, st=None, et=None, max_workers=None, print_output=False):
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

    for i in range(max_workers):
        task_start_time = start_time + i * interval
        task_end_time = task_start_time + interval
        if i == (max_workers-1):
            task_end_time = end_time - (max_workers-1) * interval

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

        tasks = []
        for binlogevent in stream:
            if binlogevent.timestamp < task_start_time:  # 如果事件的时间小于任务的起始时间，则继续迭代下一个事件
                continue
            elif binlogevent.timestamp > task_end_time:  # 如果事件的时间大于任务的结束时间，则结束该任务的迭代
                break
            task = executor.submit(process_binlogevent, binlogevent, task_start_time, task_end_time)
            tasks.append(task)

        wait(tasks)
    # print(combined_array)

    sorted_array = sorted(combined_array, key=lambda x: x["event_time"])
    for item in sorted_array:
        event_time = item["event_time"]
        dt = datetime.datetime.fromtimestamp(event_time, tz=timezone)
        current_time = dt.strftime('%Y-%m-%d %H:%M:%S')

        sql = item["sql"]
        rollback_sql = item["rollback_sql"]

        # 将字符串类型的时间转换为datetime对象
        #current_time_obj = datetime.datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")

        # 格式化日期时间，生成一个新的字符串，例如：20230707_100000
        #formatted_time = current_time_obj.strftime("%Y%m%d_%H%M%S")

        if print_output:
            print(f"-- SQL执行时间:{current_time} \n-- 原生sql:\n \t-- {sql} \n-- 回滚sql:\n \t{rollback_sql}\n-- ----------------------------------------------------------\n")

        # 写入文件
        #filename = f"{binlogevent.schema}_{binlogevent.table}_recover_{formatted_time}.sql"
        filename = f"{binlogevent.schema}_{binlogevent.table}_recover.sql"
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
    parser.add_argument("-op", "--only-operation", dest="only_operation", type=str, help="设置误操作时的命令（insert/update/delete）")
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
    parser.add_argument("--max-workers", dest="max_workers", type=int, default=4, help="线程数，默认10")
    parser.add_argument("--print", dest="print_output", action="store_true", help="将解析后的SQL输出到终端")
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
        print_output=args.print_output
    )

