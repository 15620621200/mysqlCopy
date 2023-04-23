#!/usr/bin/python3

import pymysql
import logging

fmt = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format=fmt,
                    # filename='logs.txt',
                    filemode='w',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )



f = open("datax_info.txt", "a", encoding="utf-8")
# 打开数据库连接
# 老mysql
db1 = pymysql.connect(host='192.168.138.133',
                      user='root',
                      password='root',
                      database='hive')
# 新mysql
db2 = pymysql.connect(host='192.168.138.133',
                      user='root',
                      password='root',
                      database='hive')

# 老mysql库的游标
cursor1 = db1.cursor()
# 新mysql库的游标
cursor2 = db2.cursor()
cursor1.execute("show databases")
all_databases = cursor1.fetchall()
for database in all_databases:
    if database[0] != "hive":
        continue
    # todo: current_database
    current_database = database[0]
    logging.info("当前扫描到的数据库是:%s" % current_database)
    # 查询当前数据库的建库语句
    cursor1.execute("show create database %s" % current_database)
    create_current_database_sql = cursor1.fetchall()[0][1]
    logging.info("当前扫描到的数据库的建库语句是:%s" % create_current_database_sql)

    # todo:在新mysql上创建database
    cursor2.execute(create_current_database_sql)
    create_database_result = cursor2.fetchone()
    logging.info("创建%s数据库结果：%s" % (current_database , create_database_result))

    # 获取当前数据库下有哪些表
    cursor1.execute("use %s" % current_database)
    cursor1.execute("show tables")
    tables = cursor1.fetchall()
    logging.info("当前数据库%s下已创建的数据表有：%s" % (current_database, tables))
    for table in tables:
        # 获取当前表的建表语句
        # todo: current_table
        current_table = table[0]
        logging.info("当前数据库：%s,当前数据表为：%s" % (current_database, current_table))
        cursor1.execute("show create table %s" % current_table)
        create_current_table_sql = cursor1.fetchall()[0][1]
        logging.info("当前数据库的建表语句：%s" % create_current_table_sql)

        # todo:在新mysql上创建database
        cursor2.execute("use %s" % current_database )
        cursor2.execute(create_current_table_sql)
        create_table_result = cursor2.fetchone()
        logging.info("在%s数据库下创建%s表结果：%s" % (current_database , current_table, create_database_result))
        # break
        # 获取当前表的字段信息
        fields = list()
        cursor1.execute("desc %s" % current_table)
        desc_table = cursor1.fetchall()
        for field in desc_table:
            # 将字段数据存储起来
            # print("当前数据库：%s,当前数据表为：%s,当前字段为：%s" % (current_database, current_table, field[0]))
            fields.append('"%s"' % field[0])
        # todo: columns
        columns = "[%s]" % ",".join(fields)
        logging.info("datax的columns信息：[%s]" % columns)
        f.writelines("^".join([current_database, current_table, columns]) + "\n")

# 关闭数据库连接
db1.close()
# 关闭文件
f.close()
