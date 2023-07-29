# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
transform_wind_wds.py
todo:  
last 220922 | since 220913

功能：将wds数据【如A股日涨跌幅等，20060105~20201214区间】转存至excel文件，再存入db_quote.sqlite3
path= D:\db_wind\data_adj\ashare_ana
file= ADJ_timing_TRADE_DT_20201214_ALL.csv


derived from transform_wind_wds.py
===============================================
'''
import sys,os
# from tracemalloc import stop # 占用内存统计
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )
import pandas as pd
import numpy as np 
import json 
sys.path.append("..") 
import datetime as dt
import os


class transform_wds_sql():
    # 类的初始化操作
    def __init__(self):
        import datetime as dt  
        self.time_now = dt.datetime.now()
        self.time_pre =  self.time_now - dt.timedelta(days=1) 
        self.time_pre10 =  self.time_now - dt.timedelta(days=10) 
        ### 获取近1w、1m、3m、6m、1year日期
        self.time_now_str = dt.datetime.strftime(self.time_now , "%Y%m%d")
        self.time_pre_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=1) , "%Y%m%d")
        self.time_pre10_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=10) , "%Y%m%d")

    def print_info(self):   
        ##########################################
        ### 提取数据
        print("trans_ashare_ana_sql | 转换ashare_ana文件夹内数据到sql")  
        print("get_port_perf | 获取组合收益率和区间绩效指标")     
        print("get_port_unit | 获取组合和给定基准的历史净值 ")
    
    def trans_ashare_ana_sql( self, obj_trans ) :
        ###  转换ashare_ana文件夹内数据到sql
        
        




        return obj_trans