#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : clear_url_with_large_article_per_day.py
# @Author: Cedar
# @Date  : 2020/10/30
# @Desc  : 清理每天采集数据量大于1000的url，清理时对host做判断，host的PR值大于3就不删除，其他的删除


import pymysql
import configparser
import logging
from logging.handlers import RotatingFileHandler
import sys
sys.path.append('../new_listpage_finder')
from model.lib import common

# 日志记录
logger = logging.getLogger()
logger.setLevel('INFO')

BASIC_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)

logFile = './log/clear_url_with_large_article_per_day.log'
fl_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024, backupCount=2, encoding='utf-8', delay=0)
fl_handler.setFormatter(formatter)

stream = logging.StreamHandler()  # 输出到控制台的handler
stream.setFormatter(formatter)
stream.setLevel('ERROR')

logger.addHandler(fl_handler)
logger.addHandler(stream)

conf = configparser.RawConfigParser()
conf.read('config.ini', encoding="utf-8")


def get_reject_domain_list(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        _rd_list = f.readlines()
        rd_list = []
        for i in _rd_list:
            url = i.split(' ')[2]
            url = url.replace('\n', '')
            rd_list.append(url)
        # print(rd_list)
    return rd_list


def main():
    # 导入参数文件
    host = conf.get("database", "host")
    port = conf.get("database", "port")
    user = conf.get("database", "user")
    passwd = conf.get("database", "passwd")
    db = conf.get("database", "db")
    table = conf.get("database", "table")
    reject_url_file = 'reject_url.txt'

    # 数据库配置
    database_config_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    database_config = {'host': host, 'port': int(port), 'user': user, 'passwd': passwd, 'db': db}

    # 取出116数据库domain表中，PR值大于3的domain_code(实际上是host)
    host_with_pr_value = []
    domain_sql = 'select Domain_Code from domain where PR_Value is not null and PR_Value>3'
    domain_pr_value = common.query_mysql(database_config_116, domain_sql)
    for item in domain_pr_value:
        host_with_pr_value.append(item['Domain_Code'])
    # print(host_with_pr_value)

    # 读取 reject_url
    reject_url_list = get_reject_domain_list(reject_url_file)
    # 执行删除操作
    for url in reject_url_list:
        # logger.info(url)
        host = common.get_host_code(url)
        # print(host)
        if host in host_with_pr_value:
            print('-------------------------url didnt deleted:', host, url)
            logger.info('-------------------------url didnt deleted: ' + url)
        else:
            url_md5 = common.get_token(common.get_url_remove_http(url))
            # print(url_md5)
            sql = f'''delete from {table} where Record_MD5_ID='{url_md5}';'''
            result = common.query_mysql(database_config, sql)
            print(url, 'was deleted', result)


if __name__ == '__main__':
    main()

