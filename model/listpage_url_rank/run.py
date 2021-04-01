#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : run.py
# @Author: Cedar
# @Date  : 2020/10/30
# @Desc  :


import os
import time
from a_output_detail_to_datasource import main as a_output_detail_to_datasource
from b_output_listpage_to_datasource import main as b_output_listpage_to_datasource
from c_update_extracted_data_count import main as c_update_extracted_data_count
from d_rank_listpage_url import main as d_rank_listpage_url
from e_reset_listpage_url_in_center import main as e_reset_listpage_url_in_center


# 需要初始化，如果是1，就重新导detail和listpage_url到118，比较耗时间，一般一周一次即可，手工运行
need_init = 0
website_no_input = ('S15347', 'S18604', 'S18603', 'S17969', 'S18309', 'S17968', 'S15326', 'S17967')
website_no_output = ('S17967', 'S15347', 'S15326', 'S17968', 'S17969')


def main():

    # 前三步一般一周运行一次即可
    if need_init:
        a_output_detail_to_datasource()
        b_output_listpage_to_datasource()
        c_update_extracted_data_count()

    d_rank_listpage_url(website_no_input, website_no_output)
    e_reset_listpage_url_in_center(website_no_input)


if __name__ == '__main__':
    main()
