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
    
这样，每个线程的开始时间都会有所偏移，确保处理的时间范围没有重叠，并且覆盖了整个时间范围。
