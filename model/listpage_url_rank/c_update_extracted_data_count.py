#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 3_update_extracted_data_count.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  : 计算每个url最近7天采集量并更新回listpage_url


import pymysql
from functools import wraps
import time


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
        pass

    return results


@execute_time
def main():
    config_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    # 初始化，把 extracted_data_count 全部置空
    print('开始初始化 extracted_data_count')
    initialization_extracted_data_count = """update listpage_url_rank set extracted_data_count=null;"""
    query_mysql(config_118, initialization_extracted_data_count)
    print('已置空 extracted_data_count')

    print('开始计算 extracted_data_count')
    select_extracted_data_count = """
                    select RefPage_URL_ID,count(1) as count from listpage_url_rank_by_detail GROUP BY RefPage_URL_ID;
                    """
    results = query_mysql(config_118, select_extracted_data_count)
    print('得到统计记录数: ', str(len(results)))
    print('已完成计算 extracted_data_count')

    print('开始更新 extracted_data_count')
    update_sql_pattern = """
                UPDATE listpage_url_rank SET extracted_data_count = CASE listpage_url_id 
                {}
                END 
                WHERE listpage_url_id IN {};
                """
    when_then_pattern = " WHEN {} THEN {} "
    id_list = []
    when_then = ''
    i = 0
    for item in results:
        i += 1
        RefPage_URL_ID = item['RefPage_URL_ID']
        count = item['count']
        when_then = when_then + when_then_pattern.format(RefPage_URL_ID, count)
        id_list.append(RefPage_URL_ID)

        # 每1000条更新一次
        if i % 1000 == 0 or i >= len(results):
            id_tuple = tuple(id_list)
            sql = update_sql_pattern.format(when_then, id_tuple)
            print(i)
            # print(sql)
            query_mysql(config_118, sql)
            when_then = ''
            id_list = []


if __name__ == '__main__':
    main()
