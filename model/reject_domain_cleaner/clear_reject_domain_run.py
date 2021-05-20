#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : clear_reject_domain_run.py
# @Author: Cedar
# @Date  : 2020/10/30
# @Desc  : 清理垃圾域名信源，reject_domain.txt可导入垃圾域名，config.ini可以编辑需要清理的表。
# 主要是数据源库上面的column_link，和中心库上面的cloud_listpage_url


import pymysql
import configparser
import logging
from logging.handlers import RotatingFileHandler
import sys
sys.path.append('../new_listpage_finder')
from model.lib import common


# 日志记录
logger = logging.getLogger()
logger.setLevel('DEBUG')

BASIC_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)

logFile = './log/clear_reject_domain_run.log'
fl_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5 * 1024 * 1024, backupCount=2, encoding='utf-8', delay=0)
fl_handler.setFormatter(formatter)

stream = logging.StreamHandler()  # 输出到控制台的handler
stream.setFormatter(formatter)
stream.setLevel('INFO')

logger.addHandler(fl_handler)
logger.addHandler(stream)

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
    for str_domain in reject_domain_list:
        logger.info(str_domain)
        domain = common.get_domain_code(str_domain)
        sql = f'''delete from {table} where domain_code='{domain}';'''
        result = common.query_mysql(database_config, sql)
        if result > 0:
            logger.info(sql)
            logger.info('delete count: ' + str(result))
        # sql = f'''delete from {table} where host_code='{domain}';'''
        # result = query_mysql(database_config, sql)
        # if result > 0:
        #     logger.info(sql)
        #     logger.info('delete count: ' + str(result))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(str(e))
