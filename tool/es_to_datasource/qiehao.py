#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : qiehao.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


from elasticsearch import Elasticsearch
import model.lib.common as common
import warnings

# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')
tasks = []

es = Elasticsearch(hosts="http://192.168.2.56:9200/")
extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}


body = {
    "query": {
        "match_all": {}
    }
}
query = es.search(index="qiehao_all", doc_type="qiehao",
                  body=body, size=100, scroll='5m')

results = query['hits']['hits']  # es查询出的结果第一页
total = query['hits']['total']  # es查询出的结果总量
scroll_id = query['_scroll_id']  # 游标用于输出es查询出的所有结果


column_list = []
for item in results:
    author_name = item['_source']['title']
    author_url = item['_source']['listpage_url']
    author_id = author_url.split('chlid=')[1]
    author_type = 'no_data'
    column = f"('{author_name}', '{author_url}', '{author_id}', '{author_type}')"
    column_list.append(column)

values = ",".join(column_list)
insert_column = f"insert ignore into author_qiehao(author_name, author_url, author_id, author_type) values{values};"
print(insert_column)
common.query_mysql(extractor_118, insert_column)


for i in range(0, int(total/100)+1):
    column_list = []
    # scroll参数必须指定否则会报错
    query_scroll = es.scroll(scroll_id=scroll_id, scroll='5m')['hits']['hits']
    for item in query_scroll:
        author_name = item['_source']['title']
        author_url = item['_source']['listpage_url']
        author_id = author_url.split('chlid=')[1]
        author_type = 'no_data'
        column = f"('{author_name}', '{author_url}', '{author_id}', '{author_type}')"
        column_list.append(column)

    values = ",".join(column_list)
    insert_column = f"insert ignore into author_qiehao(author_name, author_url, author_id, author_type) values{values};"
    print(insert_column)
    common.query_mysql(extractor_118, insert_column)




