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


def get_reject_domain_list(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        _rd_list = f.readlines()
    return _rd_list


def main():

    # 数据库配置
    database_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    database_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    # 读取reject_domain
    reject_domain_list = get_reject_domain_list("reject_url.txt")

    # 执行删除操作
    for str_domain in reject_domain_list:
        domain = common.get_domain_code(str_domain)
        # sql = f'''delete from column_link where domain_code='{domain}';'''
        # print(sql)
        # result = common.query_mysql(database_118, sql)
        sql = f'''delete from cloud_listpage_url where domain_code='{domain}';'''
        print(sql)
        result = common.query_mysql(database_116, sql)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(str(e))
