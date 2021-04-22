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


def main():
    config_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    config_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    # 查询最大ID
    maxid_sql = f"""select Cloud_Listpage_URL_ID from cloud_listpage_url_rank 
                ORDER BY Cloud_Listpage_URL_ID desc limit 1;"""
    maxid = query_mysql(config_118, maxid_sql)[0]["Cloud_Listpage_URL_ID"]
    run_count = int(maxid)//10000 + 1

    for i in range(run_count):
        print(i*10000, (i+1)*10000)
        _sql = f"""select Record_MD5_ID from cloud_listpage_url_rank where Extracted_Data_Count is null 
                and Cloud_Listpage_URL_ID BETWEEN {i*10000} and {(i+1)*10000};"""
        Record_MD5_ID_result = query_mysql(config_118, _sql)
        # print(Record_MD5_ID_result)
        Record_MD5_ID_list = []
        for item in Record_MD5_ID_result:
            Record_MD5_ID = item["Record_MD5_ID"]
            Record_MD5_ID_list.append(Record_MD5_ID)

        Record_MD5_ID_tuple = tuple(Record_MD5_ID_list)
        # print(Record_MD5_ID_tuple)
        _delete_center = f"delete from cloud_listpage_url where Record_MD5_ID in {Record_MD5_ID_tuple};"
        # print(_delete_center)
        _delete_datasource = f"delete from column_link where Record_MD5_ID in {Record_MD5_ID_tuple};"
        # print(_delete_datasource)

        query_mysql(config_116, _delete_center)
        # query_mysql(config_118, _delete_datasource)


if __name__ == '__main__':
    main()
