# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
test_db_sql_manage.py
derived from test_wds_manage.py 
last 19  | since 191201
Function:
1,实现将原始地csv数据转换换成复权后的csv格式和sql数据库表，
    sql目前选择地是PostgreSQL。
2，按不同时间频率和指标等复权。

MENU :
self：现实需求
1，需要把现有wds原始表格数据里1~N的column名匹配成表格列，目前原始表格内
应该是按顺序排列的
2，按需存表格
    2.1，按照具体abm、3bl等具体策略的需要，把相关的表格都归集到一起去

3，Qs：之前弄的pgsql的程序在哪里呢？


TODO
1，从A股日交易数据做起。
1.1，test data
    file_path=G:\db_wind\AShareEODPrices
    file_name=WDS_TRADE_DT_20050104_ALL_20191127

2，基础数据分析：
2.1，对表格数据做一个基础统计，比如看看每列有多少数据是缺失的；
    方便用户决定用哪些数据。



'''
#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )

from get_wind_wds import wind_wds
wind_wds1 = wind_wds()
### Print all modules 
wind_wds1.print_info()

import pandas as pd 
import numpy as np 

#################################################################################
### 






















#################################################################################
### 























#################################################################################
### 


