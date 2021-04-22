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
        query_mysql(config_116, _update_sql)


if __name__ == '__main__':
    # 搜狐号
    website_no_input = (
    'S15324', 'S16848', 'S16849', 'S16850', 'S16851', 'S16852', 'S16853', 'S16854', 'S16855', 'S16856', 'S16857',
    'S16880', 'S16881', 'S16882', 'S16883', 'S16884', 'S16885', 'S16886', 'S16887', 'S16888', 'S16889', 'S16897',
    'S16898', 'S16899', 'S16900', 'S16901', 'S16902', 'S16903', 'S16904', 'S16905', 'S16906', 'S16907', 'S16908',
    'S16909', 'S16910', 'S16911', 'S16912', 'S17041', 'S17370', 'S17471', 'S17472', 'S17473', 'S18548', 'S18549',
    'S18550', 'S18551', 'S18552', 'S18553', 'S18570', 'S18572', 'S18573', 'S18574', 'S18575', 'S18576', 'S18577',
    'S18578', 'S18579', 'S18580', 'S18581', 'S18582', 'S18583', 'S18584', 'S18585', 'S18586', 'S18587', 'S18588',
    'S18589', 'S18590', 'S18591', 'S18592', 'S18593', 'S18594', 'S18595', 'S18596', 'S18597', 'S18598', 'S18599',
    'S18600', 'S18601', 'S18602')
    website_no_output = ('S16897', 'S16898', 'S16899', 'S16900', 'S16901')
    main(website_no_input)
