#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Cedar
# @Date  : 2021/2/4
# @Desc  : 找出域名所有的栏目url，如果全都不包含news关键字，大概率是非资讯类网站，删除

import re
from model.lib.common import query_mysql


config_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
config_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}


def get_url_list():
    """
    获取需要检测的域名和url
    :return:
    """
    sql = """
            select Domain_Code,listpage_url from listpage_url where website_no in
            (select website_no from website_tags where Website_Tag_ID=105)
            and domain_code is not null
        """
    query_list = query_mysql(config_116, sql)
    return query_list


def main():
    url_list = get_url_list()
    print(len(url_list))
    domain_urls_dict = {}
    # 先把url_list转换为字典形式，key为domain，value为该域名所有url之和
    for item in url_list:
        if item['Domain_Code'] in domain_urls_dict.keys():
            domain_urls_dict[item['Domain_Code']] += ',' + item['listpage_url']
        else:
            domain_urls_dict[item['Domain_Code']] = item['listpage_url']

    print(len(domain_urls_dict))

    for domain in domain_urls_dict.keys():
        # url数量小于20个的暂不删除
        if domain_urls_dict[domain].count(',') > 20:
            # 找出value中不包含news和forum的domain
            if re.search('news|forum|.gov', domain_urls_dict[domain]) is None:
                delete_sql = f"delete from listpage_url where website_no in (select website_no from website_tags where Website_Tag_ID=105) and domain_code='{domain}';"
                print(delete_sql)


if __name__ == '__main__':
    main()
