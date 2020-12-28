#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 2_output_listpage_to_datasource.py
# @Author: Cedar
# @Date  : 2020/4/21
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


def main(start, end):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    # 查询时，去掉搜索、微博、自增等网站，去掉调度停止的网站
    select_column = f"""select * from listpage_url where 1=1
                        and Website_No not in 
                        (select Website_No from website where Website_Search_Type is not null and Website_Search_Type !='') 
                        and Website_No not in 
                        (select Website_No from website where Website_Name like'%微博%' or website_name like'%自增%') 
                        and Website_No not in 
                        (select Website_No from task_schedule where Schedule_Is_Enabled=0) 
                        and ListPage_URL_ID BETWEEN {start} AND {end};"""

    try:
        results = query_mysql(extractor_116, select_column)
        print('116 select count: ', len(results))
        column_list = []
        for item in results:
            ListPage_URL_ID = item['ListPage_URL_ID']
            Website_No = item['Website_No']
            ListPage_URL = item['ListPage_URL']
            ListPage_URL_MD5 = item['ListPage_URL_MD5']
            Record_GUID = item['Record_GUID']
            ListPage_Title = item['ListPage_Title'].replace("'", '"').replace("\\", '')   # 注意这里变换，容易出错
            Extracted_Flag = item['Extracted_Flag']
            Last_Valid_Link_Count = item['Last_Valid_Link_Count']
            Last_Check_HttpCode = item['Last_Check_HttpCode']
            Last_Check_Score_DOM = item['Last_Check_Score_DOM']
            Last_Check_Score_Text = item['Last_Check_Score_Text']
            Domain_Code = item['Domain_Code']
            Host_Code = item['Host_Code']
            column = f"""
            (
                {ListPage_URL_ID},
                '{Website_No}',
                '{ListPage_URL}',
                '{ListPage_URL_MD5}',
                '{Record_GUID}',
                '{ListPage_Title}',
                '{Extracted_Flag}',
                {Last_Valid_Link_Count},
                {Last_Check_HttpCode},
                {Last_Check_Score_DOM},
                {Last_Check_Score_Text},
                '{Domain_Code}',
                '{Host_Code}'
            )
            """
            column_list.append(column)

        # 批量插入
        values = ",".join(column_list)
        values = values.replace("'None'", "''")
        values = values.replace("None", "null")
        insert_column = f"""replace into listpage_url_check(
                ListPage_URL_ID,
                Website_No,
                ListPage_URL,
                ListPage_URL_MD5,
                Record_GUID,
                ListPage_Title,
                Extracted_Flag,
                Last_Valid_Link_Count,
                Last_Check_HttpCode,
                Last_Check_Score_DOM,
                Last_Check_Score_Text,
                Domain_Code,
                Host_Code
        ) 
        values{values};"""
        # print(insert_column)
        print('118 insert count: ', len(column_list))
        if len(column_list) > 0:
            query_mysql(extractor_118, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    for i in range(0, 11580):
    # for i in range(5765, 5766):
        print(i*10000, (i+1)*10000-1)
        main(i*10000, (i+1)*10000-1)
