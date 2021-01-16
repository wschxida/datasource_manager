#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : run.py
# @Author: Cedar
# @Date  : 2020/10/30
# @Desc  :


import os
import time


def main():
    cmd = f'''python3 4_listpage_url_checker.py'''
    for i in range(123456789):
        print('round: ', i)

        try:
            os.system(cmd)
            status = '1'
            error = ''
        except Exception as e:
            status = '0'
            error = str(e)

        print('status: ', status)
        print('status: ', error)
        time.sleep(3)


if __name__ == '__main__':
    main()
