# reverse_sql 工具介绍
reverse_sql 是一个用于解析和转换 MySQL 二进制日志（binlog）的工具。它可以将二进制日志文件中记录的数据库更改操作（如插入、更新、删除）转换为反向的 SQL 语句，以便进行数据恢复。其运行模式需二进制日志设置为 ROW 格式。

该工具的主要功能和特点包括：

1、解析二进制日志：reverse_sql 能够解析 MySQL 的二进制日志文件，并还原出其中的 SQL 语句。

2、生成可读的 SQL：生成原始 SQL 和反向 SQL。

3、支持过滤和筛选：可以根据时间范围、表、DML操作等条件来过滤出具体的误操作 SQL 语句。

4、支持多线程并发解析binlog事件。

#### 请注意！reverse_sql 只是将二进制日志还原为 SQL 语句，而不会执行这些 SQL 语句来修改数据库。

### 原理：

调用官方 https://python-mysql-replication.readthedocs.io/ 库来实现，通过指定的时间范围，转换为timestamp时间戳，将整个时间范围平均分配给每个线程。

假设开始时间戳 start_timestamp 是 1625558400，线程数量 num_threads 是 4，整个时间范围被平均分配给每个线程。那么，通过计算可以得到以下结果：

    对于第一个线程（i=0），start_time 是 1625558400。
    
    对于第二个线程（i=1），start_time 是 1625558400 + time_range。
    
    对于第三个线程（i=2），start_time 是 1625558400 + 2 * time_range。
    
    对于最后一个线程（i=3），start_time 是 1625558400 + 3 * time_range。
    
这样，每个线程的开始时间都会有所偏移，确保处理的时间范围没有重叠，并且覆盖了整个时间范围。最后，将结果保存在一个列表里，并对列表做升序排序，取得最终结果。

### 使用
```
shell> ./reverse_sql --help
usage: reverse_sql [-h] [-ot ONLY_TABLES [ONLY_TABLES ...]] [-op ONLY_OPERATION] -H MYSQL_HOST
                   -P MYSQL_PORT -u MYSQL_USER -p MYSQL_PASSWD -d MYSQL_DATABASE
                   [-c MYSQL_CHARSET] --binlog-file BINLOG_FILE [--binlog-pos BINLOG_POS]
                   --start-time ST --end-time ET [--max-workers MAX_WORKERS] [--print]

Binlog数据恢复，生成反向SQL语句。

options:
  -h, --help            show this help message and exit
  -ot ONLY_TABLES [ONLY_TABLES ...], --only-tables ONLY_TABLES [ONLY_TABLES ...]
                        设置要恢复的表，多张表用,逗号分隔
  -op ONLY_OPERATION, --only-operation ONLY_OPERATION
                        设置误操作时的命令（insert/update/delete）
  -H MYSQL_HOST, --mysql-host MYSQL_HOST
                        MySQL主机名
  -P MYSQL_PORT, --mysql-port MYSQL_PORT
                        MySQL端口号
  -u MYSQL_USER, --mysql-user MYSQL_USER
                        MySQL用户名
  -p MYSQL_PASSWD, --mysql-passwd MYSQL_PASSWD
                        MySQL密码
  -d MYSQL_DATABASE, --mysql-database MYSQL_DATABASE
                        MySQL数据库名
  -c MYSQL_CHARSET, --mysql-charset MYSQL_CHARSET
                        MySQL字符集，默认utf8
  --binlog-file BINLOG_FILE
                        Binlog文件
  --binlog-pos BINLOG_POS
                        Binlog位置，默认4
  --start-time ST       起始时间
  --end-time ET         结束时间
  --max-workers MAX_WORKERS
                        线程数，默认10
  --print               将解析后的SQL输出到终端

Example usage:
    shell> ./reverse_sql -ot table1 -op delete -H 192.168.198.239 -P 3336 -u admin -p hechunyang -d hcy \
            --binlog-file mysql-bin.000124 --start-time "2023-07-06 10:00:00" --end-time "2023-07-06 22:00:00" 
```

工具运行后，会在当前目录下生成一个{db}_{table}_recover.sql文件，保存着原生SQL（原生SQL会加注释） 和 反向SQL，如果想将结果输出到前台终端，可以指定--print选项。

![图片](https://github.com/hcymysql/reverse_sql/assets/19261879/b06528a6-fbff-4e00-8adf-0cba19737d66)

注：reverse_sql 支持MySQL 5.7/8.0 和 MariaDB

