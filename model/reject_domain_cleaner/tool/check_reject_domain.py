#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : check_reject_domain.py
# @Author: Cedar
# @Date  : 2020/11/18
# @Desc  : 利用用selenium的DOM页面渲染，来打开某些网站主页，大部分垃圾网站都会跳转到同一个域名或者包含垃圾词，
# 最后可以对结果进行分析归纳，清理出垃圾域名


import sys
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
import tldextract
import re
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


def get_domain_code(url):
    domain_code = ''
    domain_info = tldextract.extract(url)
    # print(domain_info)
    if domain_info.domain:
        if is_ip(domain_info.domain):
            domain_code = domain_info.domain
        elif domain_info.suffix:
            domain_code = f"{domain_info.domain}.{domain_info.suffix}"
            if domain_code.find('%') > -1:
                domain_code = ''
    return domain_code.strip('.')


def is_ip(_str):
    p = re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
    if p.match(_str):
        return True
    else:
        return False


def start_selenium():
    try:
        options = Options()

        #  Code to disable notifications pop up of Chrome Browser
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-infobars")
        options.add_argument("--mute-audio")
        # options.add_argument('--headless')  # 浏览器不提供可视化页面
        # twitter下面这个参数会导致登录退出
        options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
        options.add_argument('blink-settings=imagesEnabled=false')  # 不加载图片, 提升速度
        options.add_argument('--audio-output-channels=0')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-translate')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument('--disable-sync')
        # options.add_argument("--disable-javascript")    # 禁用JavaScript
        options.add_argument('--hide-scrollbars')  # 隐藏滚动条, 应对一些特殊页面
        options.add_argument('--no-sandbox')  # 以最高权限运行,解决DevToolsActivePort文件不存在的报错
        options.add_experimental_option('excludeSwitches', ['enable-automation'])  # 隐藏window.navigator.webdriver

        # options.add_argument('--window-size=1920,1080')
        global browser
        browser = webdriver.Chrome(options=options)
        browser.implicitly_wait(10)  # 隐性等待，最长等30秒
        # 下面的设置会导致超时就抛出异常，浏览器退出
        browser.set_page_load_timeout(60)  # 设置页面加载超时
        browser.set_script_timeout(60)  # 设置页面异步js执行超时

    except Exception as e:
        print(e)


def run():
    database_config = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}
    with open('url.txt', 'r', encoding='utf-8') as f:
        url_list = f.readlines()
        url_list = ['http://www.' + i.replace('\n', '') for i in url_list]
    print(url_list)

    start_selenium()
    for url in url_list:
        # url = 'http://www.xyferlan.cn'
        try:
            browser.get(url)
            cur_url = browser.current_url
            title = browser.title

        except Exception as e:
            print(e)
            cur_url = ''
            title = ''
            browser.quit()
            start_selenium()

        domain_code_req = get_domain_code(url)
        domain_code = get_domain_code(cur_url)
        print(url, cur_url, title)

        insert_sql = f"""insert into reject_domain_check(req_url, cur_url, domain_name, domain_code_req, domain_code) 
                        values('{url}', '{cur_url}', '{title}', '{domain_code_req}', '{domain_code}');"""
        query_mysql(database_config, insert_sql)

    browser.quit()


if __name__ == '__main__':
    run()



