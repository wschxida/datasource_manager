#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : website_from_grchina.com.py
# @Author: Cedar
# @Date  : 2020/11/17
# @Desc  :

import requests
import pymysql
from lxml import etree
import sys

sys.path.append('..')
import lib.common as common


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


def parse_html_to_database(database_config, url, column_extraction_deep, domain_code_source, website_no, Is_Need_VPN,
                           text):
    try:
        root = etree.HTML(text, parser=etree.HTMLParser(encoding='utf-8'))
        column_extraction_deep = 0
        items = root.xpath('//*[@id="wrap"]//tr')
        column_list = []
        for num, item in enumerate(items):
            title = "".join(item.xpath('./td[2]//a//text()'))
            listpage_url = "".join(item.xpath('./td[2]//a/@href'))
            if (len(title) > 0) and 'http' in listpage_url:
                listpage_url = listpage_url.replace('hit.php?u=', '')
                print(num, title, listpage_url)
                # 去掉标点符号
                # 计算url MD5 先去掉http和末尾斜杆
                md5_source = listpage_url
                record_md5_id = common.get_token(md5_source)

                # 垃圾词、垃圾域名过滤
                level_score = 100
                score_detail = '{"status": True, "message": "root page"}'

                if level_score > 20:
                    column = f"('{website_no}', '{title}', '{listpage_url}', '{record_md5_id}', 1)"
                    column_list.append(column)

        # 批量插入
        values = ",".join(column_list)
        insert_column = f"insert ignore into website(website_no, website_name, website_main_page_url, url_md5, is_need_vpn) values{values};"
        print(insert_column)
        query_mysql(database_config, insert_column)
        return True
    except Exception as e:
        pass


database_config = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}
url = 'http://www.cwrank.com/main/rank.php?geo=overseas&page=9'
proxies = {
    'http': '127.0.0.1:7777',
    'https': '127.0.0.1:7777'
}
response = requests.get(url, proxies=proxies)
response.encoding = 'UTF-8'
text = response.text
parse_html_to_database(database_config, url, 0, '', 'S18605', 1, text)
