#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : find_new_listpage_run.py
# @Author: Cedar
# @Date  : 2020/10/30
# @Desc  :


import os
import configparser
import logging
import redis
import json
import hashlib
import traceback
import random
import lib.common as common
import time
import asyncio
import aiohttp
from lxml import etree
from requests.compat import urljoin
import warnings
from functools import wraps


# 日志记录
logger = logging.getLogger()
logger.setLevel('DEBUG')
BASIC_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)
chlr = logging.StreamHandler()  # 输出到控制台的handler
chlr.setFormatter(formatter)
chlr.setLevel('ERROR')  # 也可以不设置，不设置就默认用logger的level, ERROR
fhlr = logging.FileHandler('./log/listpage_url_pusher.log')  # 输出到文件的handler
fhlr.setFormatter(formatter)
logger.addHandler(chlr)
logger.addHandler(fhlr)

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


def initialization_new_listpage_url_cache(database_config):
    # 初始化 new_listpage_url_cache
    # 读取 website, 删掉 new_listpage_url_cache 中目标website_no的记录，然后再重新插入主页url
    website_sql = 'select * from website;'
    website_result = common.query_mysql(database_config, website_sql)
    for website in website_result:
        Website_No = website['Website_No']
        Listpage_Title = website['Website_Name']
        Listpage_URL = website['Website_Main_Page_URL']
        # 计算url MD5 先去掉http和末尾斜杆
        md5_source = Listpage_URL
        md5_source = md5_source.replace('http://', '')
        md5_source = md5_source.replace('https://', '')
        md5_source = md5_source.rstrip('/')
        Listpage_URL_MD5 = common.get_token(md5_source)
        Domain_Code = common.get_domain_code(Listpage_URL)
        Host_Code = common.get_host_code(Listpage_URL)
        Score_Detail = '{"status": True, "message": "root page"}'
        # 清空 new_listpage_url_cache 已采的数据
        delete_cache_sql = f"delete from new_listpage_url_cache where website_no='{Website_No}';"
        common.query_mysql(database_config, delete_cache_sql)
        # 插入主页到 new_listpage_url_cache
        insert_url_to_cache = f"""
                insert ignore into new_listpage_url_cache(Column_Extraction_Deep, Listpage_URL, 
                Listpage_Title, Domain_Code, Host_Code, Listpage_URL_MD5, Level_Score, Score_Detail, Website_No) 
                value(1, '{Listpage_URL}', '{Listpage_Title}', '{Domain_Code}', '{Host_Code}', '{Listpage_URL_MD5}', 
                100, '{Score_Detail}', '{Website_No}')
                """
        common.query_mysql(database_config, insert_url_to_cache)


def parse_html_to_database(database_config, url, column_extraction_deep, domain_code_source, website_no, text):
    try:
        root = etree.HTML(text, parser=etree.HTMLParser(encoding='utf-8'))
        column_extraction_deep = int(column_extraction_deep) + 1

        items = root.xpath('//a')
        column_list = []
        for item in items:
            title = "".join(item.xpath('.//text()'))
            listpage_url = "".join(item.xpath('./@href'))
            listpage_url = urljoin(url, listpage_url)
            # 去掉标点符号
            title = common.filter_punctuation(title)
            listpage_url = common.match_url(listpage_url)
            # 计算url MD5 先去掉http和末尾斜杆
            md5_source = listpage_url
            md5_source = md5_source.replace('http://', '')
            md5_source = md5_source.replace('https://', '')
            md5_source = md5_source.rstrip('/')
            record_md5_id = common.get_token(md5_source)
            domain_code = common.get_domain_code(listpage_url)
            host_code = common.get_host_code(listpage_url)
            # domain 要与源域名一致
            if domain_code_source != domain_code:
                continue

            # 垃圾词、垃圾域名过滤
            level_score, score_detail = common.is_need_filter(title, listpage_url, False)
            # print(level_score, score_detail)
            logging.info(str(level_score) + '=' + score_detail)

            if level_score > 20:
                column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}', {column_extraction_deep}, '{domain_code}', '{host_code}', '{level_score}', '{score_detail}')"
                column_list.append(column)

        # 批量插入
        values = ",".join(column_list)
        insert_column = f"insert ignore into new_listpage_url_cache(Listpage_Title, Listpage_URL, Listpage_URL_MD5, Website_No, Column_Extraction_Deep, Domain_Code, Host_Code, Level_Score, Score_Detail) values{values};"
        # print(insert_column)
        common.query_mysql(database_config, insert_column)
        return True
    except Exception as e:
        # print(e)
        logging.ERROR(traceback.format_exc())


async def get_response(database_config, semaphore, url, column_extraction_deep=1, domain_code_source=None, website_no=None):
    try:
        async with semaphore:
            timeout = aiohttp.ClientTimeout(total=20)
            # ValueError: too many file descriptoersin select()报错问题
            # 一般是并发请求数太大导致的，通常通过减少并发数解决。
            #
            # 我遇到的情况：并发量设置的不高，运行一段时间后报该错误。通过搜索、调试，最后看aiohttp文档时发现是因为请求的https站点的服务器没有正确完成ssl连接，需要指定一个叫enable_cleanup_closed的参数为True：
            #
            # session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(enable_cleanup_closed=True)
            # 官方对enable_cleanup_closed参数的解释：
            #
            # Some ssl servers do not properly complete SSL shutdown process, in that case asyncio leaks SSL connections.
            # If this parameter is set to True, aiohttp additionally aborts underlining transport after 2 seconds. It is off by default.
            #
            # 作者：Ovie
            # 链接：https://www.jianshu.com/p/f7af4466f346
            # 来源：简书
            # 著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
            connector = aiohttp.TCPConnector(limit=60, verify_ssl=False, enable_cleanup_closed=True)  # 60小于64。也可以改成其他数
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                headers = {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
                }
                async with session.get(url, headers=headers) as response:
                    text = await response.text()
                    parse_html_to_database(database_config, url, column_extraction_deep, domain_code_source, website_no, text)
                    return len(text)

    except Exception as e:
        if len(str(e)) > 0:
            logging.error(str(e))
        return None


@execute_time
def create_task(loop, database_config):
    semaphore = asyncio.Semaphore(100)  # 限制并发量为500
    try:
        tasks = []
        # 查询待采集目标
        select_column = f"""
            select New_Listpage_URL_ID, Column_Extraction_Deep, Listpage_URL, Domain_Code, Website_No, Extracted_flag 
            from new_listpage_url_cache
            where Extracted_flag is null 
            ORDER BY Column_Extraction_Deep limit 100;
            """
        target_items = common.query_mysql(database_config, select_column)
        id_list = [0]
        for item in target_items:
            id_list.append(item["New_Listpage_URL_ID"])
            url = item["Listpage_URL"]
            domain_code = item["Domain_Code"]
            website_no = item["Website_No"]
            column_extraction_deep = item["Column_Extraction_Deep"]
            # 最多采集3层
            if column_extraction_deep <= 2:
                task = asyncio.ensure_future(get_response(database_config, semaphore, url, column_extraction_deep, domain_code, website_no))
                tasks.append(task)

        results = loop.run_until_complete(asyncio.gather(*tasks))
        print(results)
        # 更新Extracted_flag
        id_list = tuple(id_list)
        update_flag = f"update new_listpage_url_cache set Extracted_flag='S' where New_Listpage_URL_ID in {id_list};"
        common.query_mysql(database_config, update_flag)
        return len(target_items)

    except Exception as e:
        if len(str(e)) > 0:
            logging.error(str(e))
        return None


def main():
    # 导入参数文件
    host = conf.get("database", "host")
    port = conf.get("database", "port")
    user = conf.get("database", "user")
    passwd = conf.get("database", "passwd")
    db = conf.get("database", "db")

    # 数据库配置
    # database_config = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}
    database_config = {'host': host, 'port': int(port), 'user': user, 'passwd': passwd, 'db': db}

    # 初始化new_listpage_url_cache
    initialization_new_listpage_url_cache(database_config)

    # 异步请求
    event_loop = asyncio.get_event_loop()
    try:
        for i in range(10000000):
            target_count = create_task(event_loop, database_config)
            print(target_count)
            if target_count < 1:
                break
    finally:
        event_loop.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        # print(e)
        logging.ERROR(traceback.format_exc())
