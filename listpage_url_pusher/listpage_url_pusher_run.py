#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : find_new_listpage_run.py
# @Author: Cedar
# @Date  : 2020/10/30
# @Desc  :


import pymysql
import os
import configparser
import logging
import redis
import json
import hashlib
import traceback
import random


# 日志记录
logger = logging.getLogger()
logger.setLevel('DEBUG')
BASIC_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)
chlr = logging.StreamHandler()  # 输出到控制台的handler
chlr.setFormatter(formatter)
chlr.setLevel('INFO')  # 也可以不设置，不设置就默认用logger的level
fhlr = logging.FileHandler('./log/listpage_url_pusher.log')  # 输出到文件的handler
fhlr.setFormatter(formatter)
logger.addHandler(chlr)
logger.addHandler(fhlr)

# 读取config
conf = configparser.RawConfigParser()
conf.read('config.ini', encoding="utf-8")

# redis 配置
redis_connect_params = dict(
    host=conf.get("redis", "host"),
    port=conf.get("redis", "port"),
    db=conf.get("redis", "db"),
    password=conf.get("redis", "password"),
    decode_responses=True  # 返回解码结果
)
pool = redis.ConnectionPool(**redis_connect_params)
redis_help = redis.Redis(connection_pool=pool)


def push_task_to_redis(website_no, url_list):
    # 提交任务至redis
    url_list_key_name = f'listpage_url:{website_no}'
    task_list = []
    for url in url_list:
        url = json.dumps(url)
        task_list.append(url)
    redis_help.lpush(url_list_key_name, *task_list)


def get_website_no_list(file_name):
    """
    从文件读取website_no 列表
    :param file_name:
    :return:
    """
    with open(file_name, 'r', encoding='utf-8') as f:
        _f_list = f.readlines()
        f_list = []
        for i in _f_list:
            f_list.append(i.replace('\n', ''))
    return f_list


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
    website_no_file = conf.get("website_no", "file_name")

    # 数据库配置
    # database_config = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    database_config = {'host': host, 'port': int(port), 'user': user, 'passwd': passwd, 'db': db}

    # 读取 website_no
    website_no_list = get_website_no_list(website_no_file)

    # 执行push redis操作
    for website_no in website_no_list:
        sql = f'''
            select ListPage_URL_ID,ListPage_URL,Website_No,ListPage_Title,ListPage_Save_Rule,
            LinkURL_Include_Keywords_CommaText,LinkURL_Exclude_Keywords_CommaText,LinkURL_Min_Length,
            LinkText_Include_Keywords_CommaText,LinkText_Exclude_Keywords_CommaText,LinkText_Min_Length,
            Media_Type_Code,Website_Language_Code 
            from View_Listpage_URL /*[Listpage_URL]*/ 
            where Is_Enabled=1 and Website_No='{website_no}';
            '''
        url_list = query_mysql(database_config, sql)
        # 打乱顺序，随机排序
        random.shuffle(url_list)
        # print(url_list)
        push_task_to_redis(website_no, url_list)

        logging.info(website_no)
        logging.info('select count: ' + str(len(url_list)))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        # print(e)
        logging.ERROR(traceback.format_exc())
