#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 3_output_cache_to_new_listpage.py
# @Author: Cedar
# @Date  : 2020/11/12
# @Desc  :


import configparser
import model.lib.common as common
import warnings


# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')
# 读取config
conf = configparser.RawConfigParser()
conf.read('config.ini', encoding="utf-8")


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

    # 插入新采集的栏目到new_listpage_url
    insert_url_to_new = f"""
                insert ignore into new_listpage_url
                select * from new_listpage_url_cache;
                """
    common.query_mysql(database_config, insert_url_to_new)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)

