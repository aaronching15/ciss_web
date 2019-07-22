# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
Function：

derived from test_Wind.py
last  | since 190718 
'''
import sys
# 添加祖父目录
sys.path.append("../..")
sys.path.append("C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\db\\")

from db.db_assets.get_wind import wind_api
wind_api_1 = wind_api()

import time
temp_time1 = time.strftime('%y%m%d%H%M%S',time.localtime(time.time()))
print( temp_time1 )
#########################################################################
### Part 1,下载当日所有指数、ETF，股票{A,H}收盘数据
#########################################################################
### Set parameters 

path_data = 'D:\\data_Input_Wind\\'

SP_path = 'D:\\CISS_db\\data_csi\\'
print("Path for symbol list:",SP_path)
### A_Index&ETF, A_stocks, csi_HK300
### A股指数和ETF，A股全部股票，中证港股300指数成分。
file_list = ['All_Index_ETF.csv',"cicslevel2_1907.csv" ,'H11164cons.csv']
list_names =["Index-ETF","CN-stocks","HK-stocks"]
print("File name for symbols :",file_list)
print("List name for symbols :",list_names)
# Excel format for code2wind_code, 
# =REPT("0",(5-len(code) ) )&E2&".HK"
# =if(left(code,1)="6",code&".SH",'..SZ') || 删除2和9开头的股票

temp_date =  input('Please type in Date,e.g.190718 : ')
temp_predate = input('Please type in Pre Day,e.g.190717 : ')

#########################################################################
### 根据 股票，指数+ETF，下载wind-WSQ数据 


# j=0
# for temp_f in  file_list :
#     print('Working on Symbol List :', temp_f )
#     # Get Wind-WSQ single day data
#     # step 1 get SymbolList from : SL_path : path of SymbolList
#     path_list = SP_path  + temp_f

#     list_name = list_names[j]
#     quote_list = wind_api_1.Get_wsq(path_list,temp_date,path_data,list_name,temp_f ,  '')

#     j=j+1

# ASD

#########################################################################
### Part 2 更新个股历史前复权行情和不复权行情。

'''
steps:
0，T-1，T日，股票s：T-1,T转为datetime格式
1,读取股票s的前复权数据、不复权数据，读取数据中的最新日期T_end(默认读取的时间序列数据是升序)。
T_end转为datetime格式，做日期匹配。
    若:
    case1： T_end=T-1,将T日数据写入不复权列表，同时判断当日是否发生"分红送配",调整后计算复权因子，写入前复权列表。
    case2： T_end<T-1, 存在缺失日期【T_end+1,T】,需要补全这部分数据
    case3： T_end>T-1, 报错：数据已经更新或输入日期有误。

2，case1：
    2.1,获取股票前复权数据，获得CLOSE(T_end),计算收盘价的百分比差额
     CLOSE(T_end) / RT_PRE_CLOSE(T) -1, 是否小于0.1%
    逻辑：分红的股息率一般不会低于千分之一，若低于千分之一则可以忽略。
    
    if diff < 0.1% :
        # 说明可以将T日直接贴到T_end之后
    else :
        # 检查是否存在分红送配。
        # todo，一次性引用分红送配数据(对ETF很重要，但是对于策略回测来说不着急)

        # 计算复权因子: T日前复权价格/T日真实收盘价
        adj_factor =  RT_PRE_CLOSE(T)/CLOSE(T_end) 
        # 将 df_stock_f 前复权日期数据中所有历史"open,high,low,close"等价格数据乘 adj_factor,并取4位小数。
        # 同时 vol除以adj_factor并取整

        # adj_factor 保存至 df_stock_noadj
3,case2:
    3.1，wind抓取【T_end+1，T】期间的不复权行情数据，和前复权的收盘价数据。
    3.2，识别出存在“分红送配”的日期，可能有 1~n次的分红送配情况，并依次计算复权因子。
    
reference:rC_Data_Initial.py\\Update_WSQ_Get_errorCodes
'''

#########################################################################
### 2.1,step0，股票s：T-1,T转为datetime格式
# 1,读取股票s的前复权数据、不复权数据，读取数据中的最新日期T_end(默认读取的时间序列数据是升序)。
# T_end转为datetime格式，做日期匹配。

import pandas as pd 
import datetime as dt

file_list = ['All_Index_ETF.csv',"cicslevel2_1907.csv" ,'H11164cons.csv']
list_names =["Index-ETF","CN-stocks","HK-stocks"]

for j in [1,2] :
    list_name = list_names[j]
    file_name = file_list[j]

    ### read code list from 
    # Assign wind_code to indx 
    df_quote_list = pd.read_csv(path_data + 'Wind_' + list_name + '_' + temp_date+ '_updated' + '.csv',index_col='Unnamed: 0')
    for temp_code in df_quote_list.index :
        ###################################################################
        ### 拼接历史行情数据中未更新部分
        df_stock = wind_api_1.quote_concat_csv(temp_code,temp_date,list_name,path_data)


    j=j+1 
asd

###################################################################
### 下载当日分红送配数据，更新至本地文件，识别当日分红送配数据并提示。



# Wind_all_A_Stocks_wind_190717_updated.csv
# temp_predate = "190717"
# temp_date = "190718"



ASD

close_pre_hist = 1



# for temp_code in df_T.index :



















# MA_x = [3, 8, 16, 40, 100]
# P_MA = [0 , 0, 0, 0, 0]  # 1 if Price>MA(x)
# MA_up = [1, 1, 1, 1, 1]  # # of days we want such MA(x) to be Up