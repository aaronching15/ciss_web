# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
需求：
在test_data_wind.py下载完wind原始数据后，将多个资产数据合并成相同时间区间的表格。
这里主要对应A股权益类资产。

last 190616 || since 190529

Function:
功能：

todo:

Notes:
##############################################
变量： 
##############################################
Steps：
1，引入benchmark指数，指定时间区间，计算bench的周收益率 
2，对于每个资产，按照bench的周时间序列，计算周收益率数据

benchmark：000300.SH
asset：["601318.SH","600519.SH","600036.SH","000651.SZ","000333.SZ","601166.SH","600030.SH","600276.SH","601398.SH","000002.SZ","000001.SZ","000725.SZ","600104.SH","600900.SH","600009.SH"  ]


===============================================
'''
import json
import pandas as pd 
import numpy as np 
import math
# import sys
# sys.path.append("..") 
#####################################################################
### Configuration parameters
path_out ="D:\\"
path0 = "D:\\data_Input_Wind\\"
### Create dictionary object for symbols
dict_symbol = {}
dict_symbol["path_csv"] = path_out


#####################################################################
### Import week date list 
path_date = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\db\\db_times\\"
# 
file_date = "times_CN_week_20120101_20181102.csv"
# file_date = "times_CN_week_20090101_20190612.csv"

list_week = pd.read_csv(path_date+file_date)
list_week = list_week.dropna( axis=0 )
list_week_SSE = list( list_week["SSE"]  )
print("list_week ")
print( list_week_SSE[:3] )
print( list_week_SSE[-3:] )

#####################################################################
### Working on benchmark
benchmark_name = "CSI300"
benchmark_code = "000300.SH"


file_name = "Wind_" + benchmark_code + "_updated.csv"

temp_df = pd.read_csv(path0+ file_name )

temp_df = temp_df.dropna( axis=0 )
print(temp_df.head() )
# temp_df["date"] = temp_df["Unnamed: 0"]
# temp_df= temp_df.drop(["Unnamed: 0"],axis=1)
temp_df["date"] = temp_df["DATE"]


print("Quotes for benchmark ", benchmark_name, "\n ")
print( temp_df.info()   )
print( temp_df.head()   )

# temp_df2 = temp_df[ temp_df["date"].isin(list_week_SSE)]
temp_df2 = temp_df[ temp_df["date"].isin( list_week_SSE ) ]
print("Weekly close")
print( temp_df2.head()  )
print( temp_df2.tail()  )

### 计算周涨跌幅数据。
### notics 第一个值是 NaN, type= <class 'pandas.core.series.Series'>
temp_df2["chg_week"] = temp_df2.CLOSE.diff()/temp_df2.CLOSE
print( temp_df2.head()  )
print( temp_df2.tail()  )

### Set date as index 
bench_chg_w = pd.DataFrame(temp_df2["chg_week"].values , index=temp_df2["date"],columns=["chg_week"]  )
### Send values into dict object 
dict_symbol["benchmark_name"] = benchmark_name
dict_symbol["benchmark_code"] = benchmark_code
### Save dataframe to dict obj
# dict_symbol["benchmark_chg_week_df"] = bench_chg_w

#####################################################################
### Get weekly return for benchmark and assets
code_list = ["601318.SH","600519.SH","600036.SH","000651.SZ","000333.SZ","601166.SH","600030.SH","600276.SH","601398.SH","000002.SZ","000001.SZ","000725.SZ","600104.SH","600900.SH","600009.SH"  ]

### Initialize dataframe for all asset returns 
asset_chg_w = pd.DataFrame(columns=code_list, index=bench_chg_w.index  )

count1 = 0 
for temp_code in code_list :
    print("Working on code ", temp_code)

    file_name = "Wind_" + temp_code + "_updated.csv"
    temp_df = pd.read_csv(path0+ file_name )

    ### Quotes for asset
    temp_df = temp_df.dropna( axis=0 )
    
    # temp_df["date"] = temp_df["Unnamed: 0"]
    # temp_df= temp_df.drop(["Unnamed: 0"],axis=1)
    temp_df["date"] = temp_df["DATE"]

    ### get Weekly close
    # temp_df2 = temp_df[ temp_df["date"].isin(list_week_SSE)]
    temp_df2 = temp_df[ temp_df["date"].isin( list_week_SSE ) ]
    
    ### 计算周涨跌幅数据。
    ### notics 第一个值是 NaN, type= <class 'pandas.core.series.Series'>
    temp_df2["chg_week"] = temp_df2.CLOSE.diff()/temp_df2.CLOSE
    
    ### Import date list from benchmark weekly data
    # note，以benchmark的周日期为准，如果某asset的日期有而benchmark没有，需要删除
    temp_chg_w = pd.DataFrame(temp_df2["chg_week"].values , index=temp_df2["date"],columns=[temp_code]  )
    print("temp_chg_w head\n", temp_chg_w.head() )

    temp_chg_w = temp_chg_w.loc[ list(bench_chg_w.index),: ]
    ### Assign temp_chg_w to asset_chg_w
    asset_chg_w[temp_code] = temp_chg_w[temp_code]

print( asset_chg_w.head() )

### Save dataframe to dict obj
# dict_symbol["asset_chg_week_df"] = asset_chg_w

#####################################################################
### save json to jsgon file 
dict_symbol["asset_name"] = "stock"
dict_symbol["asset_code_list"] = code_list

print("dict_symbol \n ", dict_symbol )
with open( path_out+ 'data_bl.json', 'w') as output_file:
    json.dump( dict_symbol, output_file)

bench_chg_w.to_csv( path_out+ 'bench_chg_w.csv' )
asset_chg_w.to_csv( path_out+ 'asset_chg_w.csv' )





#####################################################################








































