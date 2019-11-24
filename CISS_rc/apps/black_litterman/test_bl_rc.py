# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
需求：
实现black litterman模型

last  || since 191113
derived from test_bl.py

Function:
功能：

todo:

Notes:
===============================================
'''
import json
import pandas as pd 
import numpy as np 
import math
# import sys
# sys.path.append("..") 
##############################################################
### Import Data 
'''
func 给定代码、日期，获取过去2年月度收益率
数据来源：excel-windapi提取
formula_windapi= =WSD(C2:C801,"pct_chg","2012-02-28","2014-02-28","Per=M","TradingCalendar=SSE","PriceAdj=F","rptType=1","ShowParams=Y","cols=800;rows=25")
columns = date_trade	date_ann	code	name	ind
日期	最新调整日期	Wind代码	证券名称	行业
file_name = "basic_info" | 
file_path = "D:\\CISS_db\\db_bl\\data\\"
======
股票月收益率
file_name ="in_stock_ret_m.csv"
columns: Date，各个股票代码
index：0,1,2，......
======
GICS一级行业指数月收益率，代码模式，需要和行业匹配
file_name ="in_stockbm_ind_ret_m.csv
======
市场组合月收益率
file_name ="in_stockbm_ret_m.csv"

'''
ind_list=["能源","材料","工业","可选消费","日常消费","医疗保健","金融","信息技术","电信服务","公用事业","房地产"]
record = 0 
from bl_rc import bl_rc 
bl_rc0 = bl_rc("")
file_path2 = "D:\\CISS_db\\db_bl\\data\\output\\"

code_index="000906.SH"
###################################################
### 计算BL模型第一层的股票权重，频率=季度
### Choice 1：循环计算多个日期
# notes:20140301还没按照这个模式改造，里边缺了3列
for temp_y in ["2014","2015","2016","2017","2018","2019"]:
    for temp_m in ["0301","0601","0901","1201"] :
        temp_date = temp_y +temp_m
        print( temp_date  ) 
        if temp_date not in ["20140301","20191201"] :

            for temp_i in range( len(ind_list)) :
                code_ind = ind_list[temp_i]
                #  code_ind = "材料"
                # temp_date ="20140301"
                
                output= bl_rc0.optimize_bl_stock(temp_i, code_ind,temp_date ,code_index  )
                if record == 0 :
                    df_date = output
                    
                else :
                    df_date = df_date.append(output)
                
                record=record+1

asd

### Choice 2：只计算几个日期
# for temp_date in ["20190301","20190601","20190901"] :
#     for code_ind in ind_list :
#         #  code_ind = "材料"
#         # temp_date ="20140301"
        
#         output= bl_rc0.optimize_bl_stock( code_ind,temp_date ,code_index  )
#         if record == 0 :
#             df_date = output
            
#         else :
#             df_date = df_date.append(output)
        
#         output.to_csv(file_path2+"w_"+temp_date+"_"+code_index+".csv",encoding="gbk")
#         record=record+1


### todo，获取20140601 ~ 20190901的原始数据 

###################################################
### 计算BL模型第2层的股票：公募基金，频率=季度
### todo：获取公募基金行业配置or持仓数据，并进行匹配












###################################################
### 计算BL模型第3层的股票：保险
### todo：获取保险资金大类资产配置，频率=半年










