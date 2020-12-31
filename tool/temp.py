#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : facebook_extractor.py
# @Author: Cedar
# @Date  : 2020/12/28
# @Desc  :


from requests.compat import urljoin


url = 'https://www.jianshu.com/p/20065f9b39bb'
listpage_url = '/nb/8438801'
result = urljoin(url, listpage_url)

print(result)