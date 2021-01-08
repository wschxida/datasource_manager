#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : qiehao.py
# @Author: Cedar
# @Date  : 2020/4/17
# @Desc  :

import time
import asyncio, aiohttp
from aiohttp import ClientSession
import json
from datetime import datetime
from elasticsearch import Elasticsearch
import hashlib
import model.lib.common as common


tasks = []
# url_template = "https://view.inews.qq.com/getSubQQNewsIndex?chlid=5149480"
url_template = "https://view.inews.qq.com/getSubQQNewsIndex?chlid={}"
database_config = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}


def get_token(md5str):
    # md5str = "abc"
    # 生成一个md5对象
    m1 = hashlib.md5()
    # 使用md5对象里的update方法md5转换
    m1.update(md5str.encode("utf-16LE"))
    token = m1.hexdigest()
    return token


async def get_response(i, semaphore):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            try:
                headers = {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
                }
                url = url_template.format(i)
                async with session.get(url, headers=headers) as response:
                    text = await response.text()
                    text_json = json.loads(text)
                    newslist = text_json["newslist"]
                    # print(text_json)
                    if len(newslist) > 0:
                        try:
                            try:
                                source_name = text_json["newslist"][0]["source"]
                            except:
                                source_name = text_json["newslist"][0]["media_id"]

                            last_article_time = text_json["newslist"][0]["timestamp"]
                            print(url, source_name)

                            insert_sql = f'insert ignore into author_qiehao(author_name,author_url,author_id) VALUES("{source_name}","{url}","{i}");'
                            # print(insert_sql)
                            common.query_mysql(database_config, insert_sql)

                        except Exception as e:
                            print(e)

            except Exception as e:
                pass


async def create_task(start, end):
    semaphore = asyncio.Semaphore(10)  # 限制并发量为500
    for i in range(start, end):
        task = asyncio.ensure_future(get_response(i, semaphore))
        tasks.append(task)


def run():
    loop = asyncio.get_event_loop()
    for j in range(0, 100000):
        print(1000*j, 1000*j+1000)
        global tasks
        tasks = []
        loop.run_until_complete(create_task(1000*j, 1000*j+1000))
        loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


if __name__ == '__main__':
    run()




