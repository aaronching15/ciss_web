# -*- coding: utf-8 -*-
__author__ = " ruoyu.Cheng"
"""
todo
1，

功能：0，构建历史数据和因子变量
1，按照不同的


last  | since 20201206
derived from  
"""

# from keras.models import load_model
# from custom_layer import *
# from util import *
import numpy as np
from scipy import stats
import pandas as pd
import scipy.io as sio
import h5py

import matplotlib.pyplot as plt

#####################################################################
### time
import datetime as dt 
# 20201129_161059
temp_date = dt.datetime.strftime( dt.datetime.now(),"%Y%m%d_%H%M%S")
print("Curent time is ", temp_date)

input_ID = input("type ID name for this training，example=20201206_102305_fac1.csv...")
if len( input_ID) <12:
    input_ID = "20201201_132913"

#####################################################################
### 设置模型位置path
# model_path = 'model10d/regress10/1/' 'model20d/regress6/' 'model5d/regress1/'
# 'model10d/v2/' ; 'model10d/v3/'

path0 = "D:\\CISS_db\\outside_sellside\\AI_alphanet_huatai\\"
path_zz500 = path0+ "\\ZZ500\\"
path_data = path0+ "\\data2\\"
#
# path_model_10d_v3 = path_zz500 + "model10d\\v3\\"
path_v3 = path_zz500 + "model10d\\v3\\"

### 模型的保存目录
path_model_output_ID = path_v3+"model_" + input_ID  + "\\"
### 输出数据、统计指标的保存目录
path_data_output_ID = path_v3+"output_" + input_ID  + "\\" 

### 配置wind_wds数据位置;D:\db_wind\data_wds\AShareEODPrices
path_wind_wds = "D:\\db_wind\\data_wds\\"
path_ashare_price = path_wind_wds + "AShareEODPrices\\" 
# file_name ,WDS_TRADE_DT_20201120_ALL.csv
file_prefix = "WDS_TRADE_DT_"
file_portfix = "_ALL.csv"

#####################################################################
### 1，导入日期序列
### Case 1 ：约400个周频率日期 ？
file_date = "date_match.csv"
df_date =pd.read_csv(path_zz500 + file_date )
### columns ="date","value" 
### 由于数据文件中日期菜用整数顺序，也就是0，1，2...，需要用index来替代
df_date["date_int"] = df_date.index
print("df_date  \n" )

### notes:计算来说日期整数从3161开始，大约对应2010~2011
# [3161, 3162, 3163] [5454, 5455, 5456]
list_date_int = list( df_date.index )
list_date_int = [ x for x in list_date_int if x>= 3161 ]
print("Date list ...", len(list_date_int)  ) 

#####################################################################
### 2，导入股票代码
file_code = "code.csv"
df_code = pd.read_csv(path_zz500 + file_code)

#####################################################################
### 根据日期和日期整数的匹配，获取股票区间收益率

### 新建区间收益率对象 df_ret_period
df_ret_period = pd.DataFrame(index= df_code.code , columns= list_date_int   )
df_ret_period["wind_code"] = df_ret_period.index 

count_date = 0 
### 最后一个交易日不取收益率
for temp_date_int in df_ret_period.columns[ 0:-1] :
    #############################################################
    ### 获取下一个日期和匹配日期  

    temp_date_int_next = df_ret_period.columns[ count_date+1 ]
    # temp_date_int,temp_date_int_next 3161 3162
    ### 根据date_int 获取date
    df_date_sub = df_date[ df_date["date_int"]==temp_date_int]
    temp_date = df_date_sub["date"].values[0]
    ### 
    df_date_sub = df_date[ df_date["date_int"]==temp_date_int_next ]
    temp_date_next  = df_date_sub["date"].values[0]

    print("count_date",count_date,"temp_date,temp_date_next",temp_date,temp_date_next)
    
    #############################################################
    ### 对所有股票获取当期复权收盘价
    # path_ashare_price = path_wind_wds + "AShareEODPrices\\" 
    # file_name ,WDS_TRADE_DT_20201120_ALL.csv
    ### date start 
    file_name = file_prefix  + str(temp_date) + file_portfix  
    try :
        df_ashare_price = pd.read_csv(path_ashare_price + file_name   )
    except :
        df_ashare_price = pd.read_csv(path_ashare_price + file_name ,encoding="gbk"  )
    df_ashare_price = df_ashare_price.loc[:, ["S_INFO_WINDCODE","S_DQ_ADJCLOSE"] ]
    ### date end 
    file_name2 = file_prefix  + str(temp_date_next) + file_portfix  
    try :
        df_ashare_price_next  = pd.read_csv(path_ashare_price + file_name2   )
    except :
        df_ashare_price_next  = pd.read_csv(path_ashare_price + file_name2  ,encoding="gbk"  )
    
    df_ashare_price_next = df_ashare_price_next.loc[:, ["S_INFO_WINDCODE","S_DQ_ADJCLOSE"] ]
    df_ashare_price_next.rename(columns={'S_DQ_ADJCLOSE':'S_DQ_ADJCLOSE_next'}, inplace=True)
    ###############################
    ### 用 "S_INFO_WINDCODE" 合并df_ashare_price 和 df_ashare_price_next
    '''DataFrame.merge（df2，how ='inner'，on = None，left_on = None，right_on = None，left_index = False，right_index = False，sort = False，suffixes =（'_ x'，' _ y '），copy = True，指示符= False，validate = None ）[来源]
    参数： df2： DataFrame
    如何：{'左'，'右'，'外'，'内'，默认'内'
    left：仅使用左框架中的键，类似于SQL左外连接; 保留关键顺序
    right：仅使用右框架中的键，类似于SQL右外连接; 保留关键顺序
    outer：使用来自两个帧的键的并集，类似于SQL全外连接; 按字典顺序排序键
    inner：使用两个帧的键交集，类似于SQL内连接; 保留左键的顺序
    on：标签或列表
    要加入的列或索引级别名称。这些必须在两个DataFrame中找到。如果on为None且未合并索引，则默认为两个DataFrame中列的交集。
    left_on：标签或列表，或类似数组
    要在左侧DataFrame中连接的列级或索引级别名称。也可以是左数据帧长度的数组或数组列表。这些数组被视为列。
    right_on：标签或列表，或类似数组
    '''
    df_ashare_price_next = df_ashare_price_next.merge(df_ashare_price,how ='left',left_on ="S_INFO_WINDCODE", right_on ="S_INFO_WINDCODE"   )
    df_ashare_price_next["ret_period"] = df_ashare_price_next["S_DQ_ADJCLOSE_next"] /df_ashare_price_next["S_DQ_ADJCLOSE"]  -1 
    '''     S_INFO_WINDCODE  S_DQ_ADJCLOSE_next  S_DQ_ADJCLOSE  ret_period
    2043       600106.SH               20.29          20.36   -0.003438
    2044       600105.SH               52.72          51.70    0.019729'''
    ###############################
    ### 用 "S_INFO_WINDCODE" 合并 和 df_ashare_price_next
    df_ret_period = df_ret_period.merge(df_ashare_price_next,how ='left',left_on ="wind_code", right_on ="S_INFO_WINDCODE"   )  
    
    ###############################
    ### 赋值和删除无用的columns
    df_ret_period[ temp_date_int  ]  = df_ret_period[ "ret_period"  ]

    df_ret_period = df_ret_period.drop(["S_INFO_WINDCODE","S_DQ_ADJCLOSE_next","S_DQ_ADJCLOSE","ret_period"],axis=1   )
    ### notes:df_ret_period里 "wind_code"还没删除，且index变成了0,1,2,3,
    
    # ### 
    # for temp_code in df_ret_period.index :
    #     print("temp_code ",temp_code )
    #     ### find code return in df_ashare_price_next
    #     df_sub = df_ashare_price_next[ df_ashare_price_next["S_INFO_WINDCODE"] == temp_code ]
    #     ### 先默认 0.0%收益
    #     df_ret_period.loc[temp_code, temp_date_int ] = 0.0 

    #     if len( df_sub.index ) > 0 : 
    #         df_ret_period.loc[temp_code, temp_date_int ] = df_sub["ret_period"].values[0]
    #     ###

    count_date =count_date +1 

    if count_date % 20 == 0 :
        ### save to csv 
        file_output = input_ID +"_ashare_ret.csv"
        df_ret_period.to_csv( path_data_output_ID + file_output  )


### ??? 为了统计方便，还需要获取每一年内的股票区间收益率






















