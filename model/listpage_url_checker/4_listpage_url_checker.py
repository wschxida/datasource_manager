#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 4_listpage_url_checker.py
# @Author: Cedar
# @Date  : 2020/11/5
# @Desc  : 检查listpage_url是否有效。方法是取页面前10个文本长度大于10的a节点计算MD5，对比7天前的MD5，假如一致就说明页面大概率是停更状态


import os
import configparser
import logging
from logging.handlers import RotatingFileHandler
import redis
import json
import hashlib
import traceback
import random
import time
import asyncio
import aiohttp
from lxml import etree
from requests.compat import urljoin
import warnings
from functools import wraps
import pymysql


# 日志记录
logger = logging.getLogger()
logger.setLevel('INFO')

BASIC_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)

logFile = './log/4_listpage_url_checker_run.log'
fl_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024, backupCount=2, encoding='utf-8', delay=0)
fl_handler.setFormatter(formatter)

stream = logging.StreamHandler()  # 输出到控制台的handler
stream.setFormatter(formatter)
stream.setLevel('INFO')

logger.addHandler(fl_handler)
logger.addHandler(stream)

# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')
# 读取config
conf = configparser.RawConfigParser()
conf.read('config.ini', encoding="utf-8")


def execute_time(fn):
    """
    修饰器，用于记录函数执行时长
    用法 @stat_time
    :param fn:
    :return:
    """
    @wraps(fn)
    def wrap(*args, **kw):
        start_time = time.time()
        ret = fn(*args, **kw)
        ended_time = time.time()
        print("call {}() cost: {} seconds".format(fn.__name__, ended_time - start_time))
        return ret

    return wrap


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
        conn.close()  # 关闭连接
    except Exception as e:
        print(e)

    return results


def get_token(md5str):
    # md5str = "abc"
    # 生成一个md5对象
    m1 = hashlib.md5()
    # 使用md5对象里的update方法md5转换
    m1.update(md5str.encode("utf-16LE"))
    token = m1.hexdigest()
    return token


def parse_html_to_database(text_input):
    """
    解析html
    :param text_input:
    :return:
    """
    try:
        root = etree.HTML(text_input, parser=etree.HTMLParser(encoding='utf-8'))
        items = root.xpath('//a')
        node_md5_source = []
        i = 0
        for item in items:
            title = "".join(item.xpath('.//text()'))
            # 取前10个文本长度大于10的a节点，计算md5
            if len(title) > 10:
                i += 1
                if i <= 10:
                    node_md5_source.append(title)
        # print(node_md5_source)
        node_md5 = get_token(''.join(node_md5_source))
        return node_md5, len(node_md5_source)

    except Exception as e:
        logger.error(traceback.format_exc())
        return '', 0


async def get_response(semaphore, url, id, last_check_node_md5):
    """
    异步请求html
    :param semaphore:
    :param url:
    :param id:
    :param last_check_node_md5:
    :return:
    """
    try:
        async with semaphore:
            timeout = aiohttp.ClientTimeout(total=60)
            connector = aiohttp.TCPConnector(limit=60, verify_ssl=False, enable_cleanup_closed=True)  # 60小于64。也可以改成其他数
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                headers = {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
                }
                async with session.get(url, headers=headers) as response:
                    # errors='ignore', 解决'gb2312' codec can't decode
                    text = await response.text(errors='ignore')
                    # print(response.get_encoding(), url)
                    logger.info(response.get_encoding() + url)
                    # 有些网站识别编码错误，如https://www.bannedbook.org/，识别是Windows-1254
                    if 'Windows' in response.get_encoding():
                        text = await response.text(errors='ignore', encoding='utf-8')
                    node_md5, last_check_node_count = parse_html_to_database(text)
                    return id, last_check_node_md5, node_md5, last_check_node_count

    except Exception as e:
        if len(str(e)) > 0:
            logger.error(str(e)+'  '+str(id))
        return id, last_check_node_md5, '', 0


@execute_time
def create_task(loop, database_config):
    semaphore = asyncio.Semaphore(100)  # 限制并发量为500
    try:
        tasks = []
        # 查询待采集目标
        select_column = f"""
            select ListPage_URL_ID,ListPage_URL,Last_Check_Node_MD5,Node_MD5 from listpage_url_check 
            where Extracted_Data_Count is null 
            and (Last_Check_Time<DATE_SUB(CURRENT_DATE(),INTERVAL 7 day) or Last_Check_Time is null)
            and Domain_Code not in ('facebook.com','twitter.com','instagram.com','quoka.de')
            order by RAND()
            limit 1000;
            """
        print('=====query new tasks=====')
        target_items = query_mysql(database_config, select_column)
        print('=====start tasks=====')
        for item in target_items:
            ListPage_URL_ID = item["ListPage_URL_ID"]
            ListPage_URL = item["ListPage_URL"]
            Node_MD5 = item["Node_MD5"]
            task = asyncio.ensure_future(get_response(semaphore, ListPage_URL, ListPage_URL_ID, Node_MD5))
            tasks.append(task)

        results = loop.run_until_complete(asyncio.gather(*tasks))
        print('=====finish tasks=====')
        # 计算Node_MD5 并更新
        update_sql_pattern = """
                            UPDATE listpage_url_check SET Last_Check_Time=CURRENT_TIME(),
                            Last_Check_Node_MD5 = CASE listpage_url_id 
                            {}
                            END,
                            Node_MD5 = CASE listpage_url_id 
                            {}
                            END,
                            Last_Check_Node_Count = CASE listpage_url_id 
                            {}
                            END,
                            Last_Check_Status = CASE listpage_url_id 
                            {}
                            END
                            WHERE listpage_url_id IN {};
                            """
        when_then_pattern = " WHEN {} THEN '{}' "
        Last_Check_Node_MD5_when_then = ''
        Node_MD5_when_then = ''
        Last_Check_Node_Count_when_then = ''
        Last_Check_Status_when_then = ''
        id_list = [0]
        for result in results:
            print(result)
            ListPage_URL_ID = result[0]
            id_list.append(ListPage_URL_ID)
            Last_Check_Node_MD5 = result[1]
            Node_MD5 = result[2]
            Last_Check_Node_Count = result[3]
            if Last_Check_Node_MD5 == Node_MD5 or Last_Check_Node_Count == 0:
                Last_Check_Status = 'F'
            else:
                Last_Check_Status = 'S'
            Last_Check_Node_MD5_when_then = Last_Check_Node_MD5_when_then + when_then_pattern.format(ListPage_URL_ID, Last_Check_Node_MD5)
            Node_MD5_when_then = Node_MD5_when_then + when_then_pattern.format(ListPage_URL_ID, Node_MD5)
            Last_Check_Node_Count_when_then = Last_Check_Node_Count_when_then + ' WHEN {} THEN {} '.format(ListPage_URL_ID, Last_Check_Node_Count)
            Last_Check_Status_when_then = Last_Check_Status_when_then + when_then_pattern.format(ListPage_URL_ID, Last_Check_Status)

        id_tuple = tuple(id_list)
        sql = update_sql_pattern.format(Last_Check_Node_MD5_when_then, Node_MD5_when_then, Last_Check_Node_Count_when_then, Last_Check_Status_when_then, id_tuple)
        # print(sql)
        print('=====update flag=====')
        query_mysql(database_config, sql)

        return len(results)

    except Exception as e:
        if len(str(e)) > 0:
            logger.error(str(e))
        return 0


def main():
    # 数据库配置
    database_config = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    # 异步请求
    event_loop = asyncio.get_event_loop()
    # event_loop = asyncio.ProactorEventLoop()
    # asyncio.set_event_loop(event_loop)

    try:
        target_count = create_task(event_loop, database_config)
        print('target_count: ', target_count)
    except Exception as e:
        logger.error(str(e))

    event_loop.close()


if __name__ == '__main__':
    main()

