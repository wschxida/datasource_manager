#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 1_initialization_cache.py
# @Author: Cedar
# @Date  : 2020/11/12
# @Desc  :


import configparser
import lib.common as common
import warnings


# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')
# 读取config
conf = configparser.RawConfigParser()
conf.read('config.ini', encoding="utf-8")


def initialization_new_listpage_url_cache(database_config):
    # 初始化 new_listpage_url_cache
    # 读取 website, 删掉 new_listpage_url_cache 中目标website_no的记录，然后再重新插入主页url

    # 清空 new_listpage_url_cache 已采的数据
    delete_cache_sql = f"delete from new_listpage_url_cache where website_no in (select website_no from website);"
    # common.query_mysql(database_config, delete_cache_sql)
    website_sql = 'select * from website;'
    website_result = common.query_mysql(database_config, website_sql)
    for website in website_result:
        Website_No = website['Website_No']
        Listpage_Title = website['Website_Name']
        Listpage_URL = website['Website_Main_Page_URL']
        Is_Need_VPN = website['Is_Need_VPN']
        # 计算url MD5 先去掉http和末尾斜杆
        md5_source = Listpage_URL
        md5_source = md5_source.replace('http://', '')
        md5_source = md5_source.replace('https://', '')
        md5_source = md5_source.rstrip('/')
        Listpage_URL_MD5 = common.get_token(md5_source)
        Domain_Code = common.get_domain_code(Listpage_URL)
        Host_Code = common.get_host_code(Listpage_URL)
        Score_Detail = '{"status": True, "message": "root page"}'
        # 插入主页到 new_listpage_url_cache
        insert_url_to_cache = f"""
                insert ignore into new_listpage_url_cache(Column_Extraction_Deep, Listpage_URL, 
                Listpage_Title, Domain_Code, Host_Code, Listpage_URL_MD5, Level_Score, Score_Detail, Website_No, Is_Need_VPN) 
                value(1, '{Listpage_URL}', '{Listpage_Title}', '{Domain_Code}', '{Host_Code}', '{Listpage_URL_MD5}', 
                100, '{Score_Detail}', '{Website_No}', {Is_Need_VPN});
                """
        print(insert_url_to_cache)
        common.query_mysql(database_config, insert_url_to_cache)


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

    # 初始化new_listpage_url_cache
    initialization_new_listpage_url_cache(database_config)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)

