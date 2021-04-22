#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 1_output_detail_to_datasource.py
# @Author: Cedar
# @Date  : 2021/4/1
# @Desc  : 把中心库的article_detail数据导到数据源库datasource， 目的是取里面的字段RefPage_URL_ID


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
        pass

    return results


def insert_data_to_datasource(config_center, config_datasource, start_id, end_id):
    """
    插入数据到数据源库datasource，表detail_refpage_url
    :param config_center:
    :param config_datasource:
    :param start_id:
    :param end_id:
    """

    select_column = f"""
                        select Article_Detail_ID,RefPage_URL_ID,Extracted_Time from article_detail 
                        where Article_Detail_ID BETWEEN {start_id} AND {end_id};
                        """

    results = query_mysql(config_center, select_column)

    column_list = []
    for i in results:
        Article_Detail_ID = i['Article_Detail_ID']
        RefPage_URL_ID = i['RefPage_URL_ID']
        Extracted_Time = i['Extracted_Time']
        if RefPage_URL_ID:
            column = f"({Article_Detail_ID}, {RefPage_URL_ID}, '{Extracted_Time}')"
            column_list.append(column)

    # 批量插入
    values = ','.join(column_list)
    insert_column = f"insert ignore into listpage_url_rank_by_detail(Article_Detail_ID,RefPage_URL_ID,Extracted_Time) values{values};"
    # print(insert_column)
    query_mysql(config_datasource, insert_column)


def main():
    config_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    config_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    # 初始化，把 listpage_url_rank_by_detail 全部置空
    print('开始初始化 listpage_url_rank_by_detail')
    _initialization = """truncate table listpage_url_rank_by_detail;"""
    query_mysql(config_118, _initialization)
    print('已置空 listpage_url_rank_by_detail')

    # 取最近7天article_detail最小ID
    select_min_id = """
                    select Article_Detail_ID from article_detail 
                    where Extracted_Time>DATE_SUB(CURRENT_DATE(),INTERVAL 30 day) 
                    ORDER BY Article_Detail_ID limit 1;
                    """
    min_id = query_mysql(config_116, select_min_id)[0]['Article_Detail_ID']
    print(min_id)

    # 取最近7天article_detail最大ID
    select_max_id = """
                    select Article_Detail_ID from article_detail 
                    where Extracted_Time>DATE_SUB(CURRENT_DATE(),INTERVAL 1 day) 
                    ORDER BY Article_Detail_ID desc limit 1;
                    """
    max_id = query_mysql(config_116, select_max_id)[0]['Article_Detail_ID']
    print(max_id)

    # 从最小ID开始，每次增长10万，超过最大ID后停止；可以最大限度降低mysql性能消耗
    # min_id = 7237300243
    # max_id = 7237300343
    i = min_id
    while True:
        print(i, i+100000-1)
        insert_data_to_datasource(config_116, config_118, i, i+100000-1)
        i += 100000
        if i > max_id:
            break


if __name__ == '__main__':
    main()
