#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : clear_reject_domain_run.py
# @Author: Cedar
# @Date  : 2020/10/30
# @Desc  : 清理垃圾域名信源，reject_domain.txt可导入垃圾域名，config.ini可以编辑需要清理的表。
# 主要是数据源库上面的column_link，和中心库上面的cloud_listpage_url


import pymysql
import os
import configparser
import logging


logger = logging.getLogger()
logger.setLevel('DEBUG')
BASIC_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)
chlr = logging.StreamHandler()  # 输出到控制台的handler
chlr.setFormatter(formatter)
chlr.setLevel('INFO')  # 也可以不设置，不设置就默认用logger的level
fhlr = logging.FileHandler('./log/reject_domain_cleaner.log')  # 输出到文件的handler
fhlr.setFormatter(formatter)
logger.addHandler(chlr)
logger.addHandler(fhlr)

conf = configparser.RawConfigParser()
conf.read('config.ini', encoding="utf-8")


def get_reject_domain_list(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        _rd_list = f.readlines()
        rd_list = []
        for i in _rd_list:
            if i.startswith('.'):
                i = i[1:]
            rd_list.append(i.replace('\n', ''))
    return rd_list


def query_mysql(config_params, query_sql):
    """
    执行SQL
    :param config_params:
    :param query_sql:
    :return:
    """
    # 连接mysql
    config = {
        'host': config_params["host"],
        'port': config_params["port"],
        'user': config_params["user"],
        'passwd': config_params["passwd"],
        'db': config_params["db"],
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    results = None
    try:
        conn = pymysql.connect(**config)
        conn.autocommit(1)
        # 使用cursor()方法获取操作游标
        cur = conn.cursor()
        cur.execute(query_sql)  # 执行sql语句
        results = cur.fetchall()  # 获取查询的所有记录
        if 'delete' in query_sql:
            results = cur.rowcount
        conn.close()  # 关闭连接
    except Exception as e:
        # print(e)
        logging.ERROR(str(e))

    return results


def main():
    # 导入参数文件
    host = conf.get("database", "host")
    port = conf.get("database", "port")
    user = conf.get("database", "user")
    passwd = conf.get("database", "passwd")
    db = conf.get("database", "db")
    table = conf.get("database", "table")
    reject_domain_file = conf.get("reject_domain", "file_name")

    # 数据库配置
    # database_config = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}
    database_config = {'host': host, 'port': int(port), 'user': user, 'passwd': passwd, 'db': db}

    # 读取reject_domain
    reject_domain_list = get_reject_domain_list(reject_domain_file)

    # 执行删除操作
    for domain in reject_domain_list:
        sql = f'''delete from {table} where domain_code='{domain}';'''
        result = query_mysql(database_config, sql)
        if result > 0:
            logging.info(sql)
            logging.info('delete count: ' + str(result))
        # sql = f'''delete from {table} where host_code='{domain}';'''
        # result = query_mysql(database_config, sql)
        # if result > 0:
        #     logging.info(sql)
        #     logging.info('delete count: ' + str(result))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.ERROR(str(e))
