#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 2_output_listpage_to_datasource.py
# @Author: Cedar
# @Date  : 2021/4/1
# @Desc  : 把中心库的listpage_url导到数据源库

import pymysql


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


extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}


def transfer_data(start, end):

    # 查询时，去掉搜索、微博、自增等网站，去掉调度停止的网站
    select_column = f"""select * from listpage_url where 1=1
                        and Website_No not in 
                        (select Website_No from website where Website_Search_Type is not null and Website_Search_Type !='') 
                        and ListPage_URL_ID BETWEEN {start} AND {end};"""

    try:
        results = query_mysql(extractor_116, select_column)
        print('116 select count: ', len(results))
        column_list = []
        for item in results:
            ListPage_URL_ID = item['ListPage_URL_ID']
            Website_No = item['Website_No']
            ListPage_URL = item['ListPage_URL']

            column = f"""
            (
                {ListPage_URL_ID},
                '{Website_No}',
                '{ListPage_URL}'
            )
            """
            column_list.append(column)

        # 批量插入
        values = ",".join(column_list)
        values = values.replace("'None'", "''")
        values = values.replace("None", "null")
        insert_column = f"""replace into listpage_url_rank(
                ListPage_URL_ID,
                Website_No,
                ListPage_URL
        ) 
        values{values};"""
        # print(insert_column)
        print('118 insert count: ', len(column_list))
        if len(column_list) > 0:
            query_mysql(extractor_118, insert_column)

    except Exception as e:
        print(e)


def main():
    # 初始化，把 listpage_url_rank 全部置空
    print('开始初始化 listpage_url_rank')
    _initialization = """truncate table listpage_url_rank;"""
    query_mysql(extractor_118, _initialization)
    print('已置空 listpage_url_rank')

    # 最大ID
    max_id = \
    query_mysql(extractor_116, "select listpage_url_id from listpage_url order by listpage_url_id desc limit 1")[0][
        "listpage_url_id"]
    print(max_id)
    print(max_id // 10000 + 1)
    for i in range(0, max_id // 10000 + 1):
        print(i * 10000, (i + 1) * 10000 - 1)
        transfer_data(i * 10000, (i + 1) * 10000 - 1)


if __name__ == '__main__':
    main()

