#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Cedar
# @Date  : 2021/5/19
# @Desc  : 更新datasource表column_link字段Is_Unknown_Domain，即domain表里没有domain_name的都标记为未知域名


import pymysql


config_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
config_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}


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
        if 'delete' in query_sql or 'update' in query_sql or 'insert' in query_sql:
            results = cur.rowcount
        conn.close()  # 关闭连接
    except Exception as e:
        print(str(e))

    return results


def get_unknown_domain():
    """
    获取 unknown domain
    :return:
    """
    sql = """
            select domain_code from domain where Domain_Name is null or Domain_Name='';
        """
    query_list = query_mysql(config_116, sql)
    return query_list


def main():
    # 获取116的domain表里面，unknown domain
    unknown_domain_dict = get_unknown_domain()
    # print(unknown_domain_dict)
    # 转成list
    unknown_domain_list = list(map(lambda x: x["domain_code"], unknown_domain_dict))
    # print(unknown_domain_list)

    # 开始更新Is_unknown_domain，或者删除cloud_listpage_url
    _temp_update_domain = []
    i = 0
    for index, domain in enumerate(unknown_domain_list):
        print(index, domain)
        i += 1
        _temp_update_domain.append(domain)
        # 每10个域名一个sql
        if i >= 10 or index == len(unknown_domain_list)-1:
            _temp_update_domain = tuple(_temp_update_domain)
            # 更新118，datasource
            update_sql = "update column_link set is_domain_unknown=1 where domain_code in {};".format(_temp_update_domain)
            # print(update_sql)
            # query_mysql(config_118, update_sql)

            # 116，mymonitor，cloud_listpage_url删除这些域名
            delete_sql = "delete from cloud_listpage_url where domain_code in {};".format(
                _temp_update_domain)
            print(delete_sql)
            query_mysql(config_116, delete_sql)

            i = 0
            _temp_update_domain = []


if __name__ == '__main__':
    main()
