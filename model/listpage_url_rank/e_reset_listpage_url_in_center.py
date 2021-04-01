#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Cedar
# @Date  : 2021/3/31
# @Desc  :


import pymysql
import sys


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
        pass

    return results


def main(website_no_input):
    config_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    config_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    _sql = f"""select rank_website_no from listpage_url_rank 
    where website_no in {website_no_input}
    and rank_website_no is not null GROUP BY rank_website_no;"""
    rank_website_no_list = query_mysql(config_118, _sql)
    for item in rank_website_no_list:
        rank_website_no = item["rank_website_no"]
        _ids_sql = f"select listpage_url_id from listpage_url_rank where rank_website_no='{rank_website_no}';"
        _ids = tuple([i["listpage_url_id"] for i in query_mysql(config_118, _ids_sql)]+[0])
        print(rank_website_no, "count:", len(_ids))
        _update_sql = f"update listpage_url set website_no='{rank_website_no}' where listpage_url_id in {_ids};"
        # print(_update_sql)
        # query_mysql(config_116, _update_sql)


if __name__ == '__main__':
    # 搜狐号
    website_no_input = ('S15347', 'S18604', 'S18603', 'S17969', 'S18309', 'S17968', 'S15326', 'S17967')
    # website_no_output = ('S17967', 'S15347', 'S15326', 'S17968', 'S17969')
    main(website_no_input)
