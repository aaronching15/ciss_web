# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 181122 | since  181122
Menu : 
1, Get and save all unit and mmdd of ind-X at the end of account sum
2, get unit and mmdd at every end of month
3, Import related index and make comparision, Get every month end unit and mdd

4, 



Notes:  
===============================================
'''
import numpy as np
import json
import pandas as pd 
import sys
sys.path.append("..") 
import os 
import datetime as dt
import time
# D:\CISS_db\port_rc181121_value_10\accounts 
##############################################################################################
### Load setting 
# path_raw1 = "D:\\CISS_db\\port_rc181121_"+"value"+"_"+ "10" + "\\accounts"
ind1_list_2 = ["10","40","50","60","20","15","25","45","55","35","30"]
# ind1_list_2 =["15"]
# path_raw2 = "D:\\CISS_db\\port_rc181122_"+"value"+"_"+ "10" + "\\accounts"
# ind1_list_2 = ["25","15"]


##############################################################################################
### Get and save all unit and mmdd of ind-X at the end of account sum
temp_df0= pd.DataFrame() 
temp_df1= pd.DataFrame() 
date_lastUpdate1 = "2018-11-05"
date_lastUpdate = "20181105"
# for temp_ind1 in ind1_list_1 :
#     for sty_v_g in ["value","growth"] :
#         path_raw1 = "D:\\CISS_db\\port_rc181121_"+sty_v_g+"_"+ temp_ind1 + "\\accounts\\"
#         # id_account_1542811496_port_rc181121_value_10_Asum_20181105.csv
#         ## Get all file name in current directory
#         temp_file_list = os.listdir( path_raw1 )
#         ## all file list from earliest modified date to latest at the end 
#         dir_list = sorted(temp_file_list , key=lambda x: os.path.getmtime(os.path.join(path_raw1,x)))
#         dir_list_s = dir_list[-5:] 
#         for file_name in dir_list_s :
#             if date_lastUpdate in file_name and "Asum" in file_name :
#                 temp_df= pd.read_csv( path_raw1 + file_name  )
#                 temp_index = temp_df[ temp_df['date']== date_lastUpdate1  ].index[0]
                
#                 temp_df0.loc[temp_ind1+"_"+sty_v_g,"ind1"] = temp_ind1
#                 temp_df0.loc[temp_ind1+"_"+sty_v_g,"date"] = date_lastUpdate
#                 temp_df0.loc[temp_ind1+"_"+sty_v_g,"unit"] =  temp_df.loc[temp_index,"unit" ]
#                 temp_df0.loc[temp_ind1+"_"+sty_v_g,"mdd"] =temp_df.loc[temp_index,"mdd" ]
#                 temp_df0.loc[temp_ind1+"_"+sty_v_g,"market_value"] = temp_df.loc[temp_index,"market_value" ]
#                 temp_df0.loc[temp_ind1+"_"+sty_v_g,"total"] = temp_df.loc[temp_index,"total" ] 

# temp_df0.to_csv("D:\\181122.csv")






####################################################################################
### Get weight list of indX for level1 from temp directory 
# temp_df_value_999_2016-11-30
path_temp = "D:\\CISS_db\\temp\\"

### Get date list 
from db.times import times
times0 = times('CN','SSE')
method4time='stock_index_csi'
temp_date = "2014-05-31" # "2014-05-31"
temp_date2 = "2018-11-15" # "2014-11-30"

temp_date = "2007-05-31" # "2014-05-31"
temp_date2 = "2014-05-31" # "2014-11-30"

temp_date_now = times0.get_time_format('%Y%m%d','str')
# get date list with all pivot time point that we need to update stockpool,strategy and portfolio
# DatetimeIndex(['2014-05-31', '2014-11-30', '2015-05-31' ......
date_periods = times0.get_port_rebalance_dates(temp_date,temp_date2 ,method4time )
print("date_periods","   periods_reference_change ")
# [Timestamp('2014-05-31 00:00:00'), Timestamp('2014-11-30 00:00:00')]
# print(date_periods.periods_reference_change )
# print( date_periods.periods_start ) # timestamp 140531 to 180531
# print( date_periods.periods_end )

### Get ind and style list 
list_ind1 = ["10","40","50","60","20","25","45","55","35","30","15"] # 15 is not yet ok 
list_style = ["growth" , "value" ] # "growth"

########################################################################################
## 没确定号怎么用，全列出来 3w多行
# df_out = pd.DataFrame( columns=["code"])

# temp_i = 0 
# for temp_ind1 in list_ind1 :    
#     for sty_v_g in list_style :
#         for temp_dt in date_periods.periods_start :
#             time_str = dt.datetime.strftime(temp_dt,"%Y-%m-%d")
#             print(temp_ind1, sty_v_g, time_str) 
#             # print("file_name ",file_name)
#             file_name = "temp_df_"+sty_v_g+"_"+temp_ind1+ "_"+time_str +".csv"            

#             temp_df=pd.read_csv(path_temp +file_name )
#             print(temp_df.info() )
#             print( temp_df.head() )
#             # create a new DataFrame 
#             # "para_value",  "w_allo_value_ind1"
#             for temp_i2 in temp_df.index :
#                 ### Part 1 
#                 df_out.loc[temp_i,"ind1"] = temp_ind1
#                 df_out.loc[temp_i,"sty_v_g"] = temp_ind1
#                 df_out.loc[temp_i,"date"] = time_str
#                 df_out.loc[temp_i,"file_name"] = file_name 

#                 ### Part 2 
#                 value_1 = temp_df.loc[temp_i2, "code"]
#                 df_out.loc[temp_i,"code"] = value_1 
#                 value_1 = temp_df.loc[temp_i2, "para_"+sty_v_g]
#                 df_out.loc[temp_i,"para_"+sty_v_g] = value_1 
#                 value_1 = temp_df.loc[temp_i2,  "w_allo_"+sty_v_g+"_ind1"]
#                 df_out.loc[temp_i, "w_allo_"+sty_v_g+"_ind1"] = value_1 

#                 value_1 = temp_df.loc[temp_i2,  "profit_q4_es" ]
#                 df_out.loc[temp_i, "profit_q4_es"] = value_1 
#                 value_1 = temp_df.loc[temp_i2,  "profit_q4_es_dif" ]
#                 df_out.loc[temp_i, "profit_q4_es_dif"] = value_1 

#                 ### part 3 
 
#                 ############################
#                 temp_i = temp_i + 1


# df_out.to_csv("D:\\df_out.csv")

####################################################################################
### Stockpool
### table 1: single industry, index: ; column: .
### 把每6个月调整的分行业，分成长和价值的股票清单和相对系数都存在一个表格里。

# temp_ind1 = "0"
# sty_v_g ="growth"
# def output_df(temp_ind1, sty_v_g, date_periods,path_temp ):
#     df_out_1 = pd.DataFrame( columns=["code"])
#     temp_i = 0 
#     for temp_dt in date_periods.periods_start :
#         time_str = dt.datetime.strftime(temp_dt,"%Y-%m-%d")
#         print(temp_ind1, sty_v_g, time_str) 
#         # print("file_name ",file_name)
#         file_name = "temp_df_"+sty_v_g+"_"+temp_ind1+ "_"+time_str +".csv"            

#         temp_df=pd.read_csv(path_temp +file_name )
#         # ascending=True
#         if temp_ind1 == "999" :
#             temp_df = temp_df.sort_values(by="w_allo_"+sty_v_g+"_ind1" , ascending=False)
#         else :
#         	temp_df = temp_df.sort_values(by="para_"+sty_v_g , ascending=False )
#         print( temp_df.info() )
#         print( temp_df.head() )
        
#         # create a new DataFrame 
#         # "para_value",  "w_allo_value_ind1"
#         ### initialize new column
#         df_out_1["code_"+ time_str] = ""
#         df_out_1[time_str] = 0.0 
#         index_out_1 = 0 
#         for temp_i2 in temp_df.index :
#             df_out_1.loc[index_out_1,"code_"+ time_str] = temp_df.loc[temp_i2, "code"]
#             if temp_ind1 == "999" :
#                 df_out_1.loc[index_out_1, time_str] = temp_df.loc[temp_i2, "w_allo_"+sty_v_g+"_ind1"]
#             else :
#                 df_out_1.loc[index_out_1, time_str] = temp_df.loc[temp_i2, "para_"+sty_v_g]

#             ############################
#             index_out_1 = index_out_1 + 1

#     df_out_1.to_csv("D:\\CISS_db\\df_out\\df_out_1904_"+temp_ind1+"_"+sty_v_g +".csv")

#     return 1


# ### Get ind and style list 
# list_ind1 = ["10","40","50","60","20","25","45","55","35","30","15"] # 15 is not yet ok 
# list_ind1 = ["999"]
# list_style = ["growth" , "value" ] # "growth"
# list_style = [ "value" ] 

# for temp_ind1 in list_ind1 :
#     for sty_v_g in list_style :
#         ##############################################
#         result = output_df(temp_ind1, sty_v_g, date_periods,path_temp )
#         # elapsed = (time.clock() - start)
#         # print(temp_ind1, sty_v_g," Total time used:",elapsed)

# asd
####################################################################################
path0 = "D:\\CISS_db\\portable\\"
temp_file_list = os.listdir( path0 )
### 获取所有文件夹下的文件名称
dir_list = sorted(temp_file_list , key=lambda x: os.path.getmtime(os.path.join(path0,x)))
print("dir_list")

####################################################################################
### table 2: single industry, index: ; column: .
### todo: 作为 trade statistics 融入模块。
### 大类portfolio rebalance| tradeplan : Buy and Sell, summarize 净值比例,分买入和卖出的金额
    # 每6个月，净买入和净卖出的净值比例，因为一个部分买入市值是净值的6%，期末是净值9%《那么如果新的目标是7%，
    # 那么实际上应该卖出2%，这些细节需要描述清楚呀。(做的细致使我们的优势)；进一步来说，卖出对应的实现盈亏也应该有所体现。
    # 
### 文件位置：trades_id_1543136329_port_rc181123_w_allo_value_45_TP_20181105.csv
### steps：对于dir下每个文件名，split by "_", 


# # filename = dir_list[0]
# # ['trades', 'id', '1543131047', 'port', 'rc181123', 'w', 'allo', 'value', '10', 'TP', '20181105.csv']
# # ['trades', 'id', '1543132113', 'port', 'rc181123', 'w', 'allo', 'growth', '10', 'TP', '20181105.csv']

# def get_monthly_sum(path0 ,file_name) :
#     # df_TP
#     df_TP = pd.read_csv(path0 + file_name)
    
#     # ind1="15", 有145个"weight_dif"是空值，有122个"signal_pure"是空值，需要去掉
#     # print( df_TP["weight_dif"].isnull().value_counts() )
#     # print( df_TP["signal_pure"].isnull().value_counts() )
#  	# axis：0-行操作（默认），1-列操作 
# 	# how：any-只要有空值就删除（默认），all-全部为空值才删除 
# 	# inplace：False-返回新的数据集（默认），True-在愿数据集上操作
#     # df_TP = df_TP.dropna(axis=0, how='any', inplace=False)

#     # notes: "weight_dif" 有正有负，"weight_dif2"都是正数
#     df_TP["weight_dif2"] =df_TP["weight_dif"]*df_TP["signal_pure"]
    
#     # print(df_TP.describe() )
#     # "signal_pure" "weight_dif" 
#     ##############################################################################
#     ### 设想，table columns=[ ind1, style, date_list{ "2014-05-31",...} ]
#     ### 1，新建2列，分别是买入金额和卖出金额
#     ### 2，"date_trade_1st" 变成 datetime, 然后按月份汇总，得到每年5,11月的交易数据
#     ### 3，存到df，index是总的B，S weight，columns是组合名称
#     df_TP["weight_dif_add"] =  df_TP["weight_dif"].apply(lambda x: max(0.0, x) )
#     df_TP["weight_dif_minus"] =df_TP["weight_dif"].apply(lambda x: min(0.0, x) )
#     df_TP["date"]= pd.to_datetime( df_TP["date_trade_1st"],format="%Y-%m-%d" )
    
#     ### df summarize by month or year 
#     ### method 1 using datetime as index and then df["2014-06"]
#     # df_TP = df_TP.set_index('date')
#     # print( df_TP["2014-06"].describe() )
#     # print( df_TP["2014-06"].head() )

#     ### show by month 
#     df_TP = df_TP.set_index('date')
#     # print("df_summary__________")
    
#     df_summ = df_TP.resample("M").sum()
#     df_summ = df_summ[ df_summ["total_amount"]>1 ]
#     # print( df_summ )

#     return df_summ
# #########################################################################
# import datetime as dt 

# temp_i = 0 
# for file_name in dir_list :
#     str_list = file_name.split("_")
#     if str_list[-2] == "TP" :
#         print(str_list  )
#         ### Get summary of df in monthly summary
#         df_summ = get_monthly_sum(path0 ,file_name)

#         # print("df_summ" )
#         # print( type( df_summ.index) )
#         # print( df_summ.index )

#         ### asve to the big pd
#         if temp_i == 0 :
#             list_index0 = []
#             for temp_date in df_summ.index :
#                 list_index0 = list_index0 +[ dt.datetime.strftime(temp_date,"%Y%m%d")  ]
#             list_index = list_index0 +  ["port_name","indX","style","diff_direction"] 
#             df_out = pd.DataFrame( index =  list_index)
#             df_out = df_out.T
#             # df_out["port_name"] = ""
#             # df_out["indX"]  = ""
#             # df_out["style"] = ""
#             # df_out["diff_direction"] = ""
#         # print("df_out_______")
#         # print( df_out )
#         position_level = 0.0 
#         ### first line of 3 lines in total 
#         # index 是日期
#         df_out.loc[temp_i,"port_name"] = str_list[4]
#         df_out.loc[temp_i,"indX"] = str_list[8]
#         df_out.loc[temp_i,"style"] = str_list[7]
#         df_out.loc[temp_i,"diff_direction"] = "add"
#         df_out.loc[temp_i,list_index0] = df_summ.loc[df_summ.index,"weight_dif_add"]
#         temp_i =temp_i + 1  
#         ### Second line 
#         df_out.loc[temp_i,"port_name"] = str_list[4]
#         df_out.loc[temp_i,"indX"] = str_list[8]
#         df_out.loc[temp_i,"style"] = str_list[7]
#         df_out.loc[temp_i,"diff_direction"] = "minus"
#         df_out.loc[temp_i,list_index0] = df_summ.loc[df_summ.index,"weight_dif_minus"]
#         temp_i =temp_i + 1  
#         ### Second line 
#         df_out.loc[temp_i,"port_name"] = str_list[4]
#         df_out.loc[temp_i,"indX"] = str_list[8]
#         df_out.loc[temp_i,"style"] = str_list[7]
#         df_out.loc[temp_i,"diff_direction"] = "net"
#         temp_df = df_summ.loc[df_summ.index,"weight_dif_add"] + df_summ.loc[df_summ.index,"weight_dif_minus"]
        
#         # print( temp_df.cumsum()  )
#         df_out.loc[temp_i,list_index0] = temp_df
#         temp_i =temp_i + 1  




# print("df_out______2")
# print(df_out)
# df_out.to_csv("D:\\CISS_db\\p1_stat\\df_TP_1206.csv")
# asd

############################################################################################
### 大类portfolio rebalance| tradebook:
### Start of part 5
#     # 对每笔交易进行分析，这个会涉及到对市场的影响，因为是多日平均执行，可以分析占市场流动性的比例，已经流动性更差情况下是否需要
#     # 用更多日的均价，而不是5D。估计在市场占比20%的情况下，可以容纳多少组合规模~~
# # 交易计划偏交易净值比例，总金额，买卖方向，交易书TB偏重交易明细，具体品种，金额，比例。

# def get_trade_stat(path0 ,file_name) :
#     # df_TP
#     df_TB = pd.read_csv(path0 + file_name)

#     ### 时间角度： 买卖笔数统计，平均成交金额，平均实现收益，交易费用。
#     # df_tb_time 
#     df_TB["date2"] = pd.to_datetime( df_TB["date"] ) 
#     df_TB = df_TB.set_index('date2')
#     # 
#     print("df_TB")
#     print(df_TB.info() )
#     amt_sum = df_TB["amount"].sum() 
#     if amt_sum > 0 :
#         df_TB["amt_pct"] = df_TB["amount"]/amt_sum
#     else :
#         df_TB["amt_pct"] = df_TB["amount"]*-1/amt_sum
#     df_TB["amt_buy"] = df_TB["amount"]* df_TB["BSH"].apply(lambda x : max(x,0.0)  )
#     df_TB["amt_sell"] = df_TB["amount"]* df_TB["BSH"].apply(lambda x : min(x,0.0)*-1  )
#     df_TB["num_buy"] =  df_TB["BSH"].apply(lambda x : max(x,0.0)  )
#     df_TB["num_sell"] = df_TB["BSH"].apply(lambda x : min(x,0.0)*-1  )
#     # 有成交的每个月的 买入笔数，卖出笔数，买入总金额，卖出总金额，总盈亏，总费用
#     #   买入平均金额，卖出平均金额，平均盈亏，平均费用
#     df_mean = df_TB.resample("M").mean()
#     df_mean =df_mean[df_mean["amount"]>0  ]
#     df_sum = df_TB.resample("M").sum()
#     df_sum =df_sum[df_sum["amount"]>0  ]
#     # print("df_mean________________________")
#     # # df_mean.describe().loc["count","amt_sell"]
#     # print( df_mean.info()  )
#     # print("df_sum________________________")
#     # print( df_mean.sum()  )

#     df_stat = df_sum
#     df_stat["pct_fees_profit"] = df_stat["fees"]/df_stat["profit_real"] 
#     df_stat["ave_amt_buy"] = df_mean["amt_buy"]
#     df_stat["ave_amt_sell"] = df_mean["amt_sell"]
#     df_stat["ave_profit"] = df_mean["profit_real"]
#     df_stat["ave_fees"] = df_mean["fees"]

#     ########################################################################
#     ### 股票角度，总交易金额占比，总利润占比
#     # df_tb_stock
#     # 对于每个股票，统计：有交易的月份，每个月交易金额，总盈亏，总费用，取top5~20
#     # vipvip 我们想要观察anchor stock的表现，多大程度上能代表主动基准组合的收益和风险
#     df_stat_s = df_TB.groupby("symbol").sum()
#     # 47 stocks 
#     df_stat_s["amt_pct"] =df_stat_s["amount"]/df_stat_s["amount"].sum()
#     profit_sum = df_stat_s["profit_real"].sum()
#     if profit_sum >0 :
#         df_stat_s["profit_pct"] =df_stat_s["profit_real"]/profit_sum
#     else :
#         df_stat_s["profit_pct"] =df_stat_s["profit_real"]*-1/profit_sum
#     df_stat_s = df_stat_s.sort_values(["amt_pct"] ,ascending= False)

#     # df_stat_s.to_csv("D:\\df_stats.csv")

#     # ERROR: ValueError: level name symbol is not the name of the index
#     # df_stat = df_mean
#     return df_stat,df_stat_s

# # 假设最多500个股票，未来也许会更多。
# df_out_by_stock = pd.DataFrame(index=np.arange(2000) )

# ### 190415 only market value and market growth are partially available
# path0 = "C:\\zd_zxjtzq\\RC_trashes\\temp\\18p1_data\\"
# temp_file_list = os.listdir( path0 )
# ### 获取所有文件夹下的文件名称
# dir_list = sorted(temp_file_list , key=lambda x: os.path.getmtime(os.path.join(path0,x)))
# print("dir_list")

# temp_i = 0 
# for file_name in dir_list :
#     str_list = file_name.split("_")
#     # only use tradebook files
#     if str_list[-2] == "TB" :

#         ##########################################################################
#         ### initialization 
#         print("str_list ") 
#         print(str_list  )
#         if str_list[7] == "999" :
#             indX = str_list[7] # "10"
#             style = str_list[6] # value or growth
#             time_end =  str_list[9][:-4]
#         else :
#             indX = str_list[8] # "10"
#             style = str_list[7] # value or growth
#             time_end =  str_list[10][:-4]
#         ##########################################################################
#         ### get data df by time and by stock 
        
#         (df_by_time,df_by_stock) = get_trade_stat(path0 ,file_name)
#         # todo 要保存到 df里，分行业
#         print("trade statistics by_time")
#         print( df_by_time.info() )
#         print( df_by_time.head() )
#         print( "Percentage of fees on profit realized during 6m" )
#         print( round(df_by_time["fees"].sum()/df_by_time["profit_real"].sum()*100,2)  )

#         print("trade statistics by_stock")
#         print( df_by_stock.info() )
#         print( df_by_stock.head() )
        

#         ##########################################################################
#         ### 1，by_stock,重点是股票代码和总盈亏占比(分母需要是正数的和)；总交易金额没必要，因为在配置比例里就决定了。
#         # 1,一列是股票代码，一列是对应的利润总数，一列是交易总金额
#         ### notes 注意个股的排序是按照总交易金额排的！！
#         # index = top 20利润贡献的股票，columns 每个行业2列，一列代码，一列利润占比。

#         ### save to the big pd
#         len_1 = len( df_by_stock.index )
#         df_out_by_stock.loc[0:len_1,indX+"_"+style ] = " "
#         df_out_by_stock.loc["indX",indX+"_"+style ] = indX
#         df_out_by_stock.loc["style",indX+"_"+style ] =style
#         df_out_by_stock.loc["time_end",indX+"_"+style ] = time_end

#         df_out_by_stock.loc[0:len_1,indX+"_"+style ] = df_by_stock.index

#         df_out_by_stock.loc["indX",indX+"_"+style+"_2" ] = indX
#         df_out_by_stock.loc["style",indX+"_"+style+"_2" ] =style
#         df_out_by_stock.loc["time_end",indX+"_"+style+"_2" ] = time_end
#         df_out_by_stock.loc[0:len_1,indX+"_"+style+"_2"] = df_by_stock['profit_pct'].values
        

#         ##########################################################################
#         ### 1，by_time,  df_out_by_time
#         ### 定了，每个组合一张图，columns 是items，index是 时间
#         # define 
#         cols= ["profit_real","amount","fees","amt_pct","amt_buy","amt_sell","num_buy","num_sell","pct_fees_profit","ave_amt_buy","ave_amt_sell","ave_profit","ave_fees"]
#         cols_CN=["总实现收益","总交易金额","交易费用","交易额占比","买入金额","卖出金额","买入次数","卖出次数","交易成本占利润比例","单笔平均买入金额","平均卖出金额","平均实现收益","平均交易成本"]
#         df_out_by_time = df_by_time.loc[:,cols]
#         df_out_by_time.columns = cols_CN
#         df_out_by_time["indX"] = indX 
#         df_out_by_time["style"] = style

#         print("df_out_by_time " )
#         print( df_out_by_time.head()  )
#         # todo 最好能放在一个df，csv里，方便
#         # df_out_by_time["index"] =df_out_by_time.index
#         # df_out_by_time.index = np.linspace(1,1,len(df_out_by_time.index ) )
#         if temp_i == 0 :
#             df_out_by_time_all = df_out_by_time
#         else :
#             df_out_by_time_all = df_out_by_time_all.append(df_out_by_time, ignore_index = False)

#         # filename_by_time = "df_by_time_"+ indX + "_"+ style + "_"+ time_end
#         # df_out_by_time.to_csv(path_out +filename_by_time  )

#         temp_i = temp_i +1

# path_out = "D:\\CISS_db\\p1_stat\\"

# filename_by_time = "df_by_time_"+ "indX_style" +"_1904.csv"
# df_out_by_time_all.to_csv(path_out +filename_by_time ,encoding="gbk" )
# filename_by_stock = "df_by_stock_"+ "indX_style" +"_1904.csv"
# df_out_by_stock.to_csv( path_out +filename_by_stock  )

# asd
#################################################################################
# # 181215
# # 分组合，画出实现收益或亏损的分布
# # ind1_list_2 = ["10","40","50","60","20","15","25","45","55","35","30"]
# i = 0 
# list1= []

# for style1 in ["value","growth"]:
#     for indX in ind1_list_2 :
#         df_temp = df_out_by_time_all[ df_out_by_time_all['indX']==indX ]

#         df_temp = df_temp[ df_temp['style']== style1  ]

#         df_temp2 = df_temp["总实现收益"]/10000
        
#         # print("df_temp2")
#         # print(df_temp2)
#         # asd
#         if i == 0 :
#             df_profit_real = df_temp2 
#             df_profit_real.columns = [indX+"_"+style1 ]
#             list1= list1 + [indX+"_"+style1 ]
#         else :             
#             df_profit_real = pd.concat( [df_profit_real,df_temp2 ], axis=1)
#             list1= list1 + [indX+"_"+style1 ]

#         i=i+1 
#         print("df_profit_real ",i)
#         print( df_profit_real  )
#         print( list1)


# filename_by_time_profit = "df_by_time_profit"+ "indX_style" +".csv"
# df_profit_real.to_csv( path_out +filename_by_time_profit  )
# print(path_out +filename_by_time_profit )


#################################################################################
# # 181215 # 交易成本占利润比例
# # 分组合，画出实现收益或亏损的分布
# # ind1_list_2 = ["10","40","50","60","20","15","25","45","55","35","30"]
# i = 0 
# list1= []

# for style1 in ["value","growth"]:
#     for indX in ind1_list_2 :
#         df_temp = df_out_by_time_all[ df_out_by_time_all['indX']==indX ]

#         df_temp = df_temp[ df_temp['style']== style1  ]

#         df_temp2 = df_temp["交易费用"] 
        
#         # print("df_temp2")
#         # print(df_temp2)
#         # asd
#         if i == 0 :
#             df_profit_real = df_temp2 
#             df_profit_real.columns = [indX+"_"+style1 ]
#             list1= list1 + [indX+"_"+style1 ]
#         else :             
#             df_profit_real = pd.concat( [df_profit_real,df_temp2 ], axis=1)
#             list1= list1 + [indX+"_"+style1 ]

#         i=i+1 
#         print("df_profit_real ",i)
#         print( df_profit_real  )
#         print( list1)


# filename_by_time_profit = "df_by_time_fees_profit"+ "indX_style" +".csv"
# df_profit_real.to_csv( path_out +filename_by_time_profit  )
# print(path_out +filename_by_time_profit )


# asd
### End of part 5
###############################################################################################
### portfolio performance 
### Start of part 6
# # todo list 
# ###  AS，比较value和growth持仓结构，交易是实现收益的分析比较，AS是账面浮动收益的分析比较。
# # 市场组合为例，value组合平均盈利+18%，很多浮盈超100%| growth组合平均 -14.39%。
# # 抓取各个组合最新持仓浮盈亏情况。
# temp_df0= pd.DataFrame() 
# temp_df1= pd.DataFrame()
# date2 ="1904" 
# filename_as_pnl_weighted = "df_AS_pnl_weighted" +'_'+ date2 +".csv"
# filename_as_pnl = "df_AS_pnl" +'_'+ date2 +".csv"
# path_out = "D:\\CISS_db\\stats\\"

# path2 = "D:\\CISS_db\\port_rc181123_w_allo_"
# path2 = "D:\\CISS_db\\port_rc1904_market_"
# ind1_list_2 = ["10","40","50","60","20","15","25","45","55","35","30"]
# ind1_list_2 =["999"]
# ### 
# i=0
# for sty_v_g in ["value","growth"] :
#     for temp_ind2 in ind1_list_2 :
#         print(temp_ind2,"   ", sty_v_g)
#         path_raw2 = path2 +sty_v_g+"_"+ temp_ind2 +"\\accounts\\"
#         # id_account_1542811496_port_rc181121_value_10_Asum_20181105.csv
#         ## Get all file name in current directory
#         # try:
#         temp_file_list = os.listdir( path_raw2 )
#         ## all file list from earliest modified date to latest at the end 
#         dir_list = sorted(temp_file_list , key=lambda x: os.path.getmtime(os.path.join(path_raw2,x)))
#         dir_list_s = dir_list[-10:] 
#         print("=====================")
#         j= 0 
#         for file_name in dir_list_s :
#             print( file_name )
#             if "AS" in file_name and j == 0 :
#                 # if date_lastUpdate in file_name and "Asum" in file_name :
#                 # print( file_name.split("_") )
#                 # "num","ave_cost","last_quote","total_cost  market_value","pnl"," 
#                 # pnl_pct w_real  w_optimal   date_update date_in code    currency    market"
#                 temp_df= pd.read_csv( path_raw2 + file_name )
#                 print( temp_df.columns )

#                 temp_df = temp_df.sort_values("pnl",ascending=False )
#                 temp_df =temp_df.reset_index(drop=True)
#                 temp_df =temp_df.reset_index(drop=True)
#                 # sort to decending order 
#                 # type series to DataFrame
#                 temp_df2 = pd.DataFrame( temp_df["pnl_pct"]*temp_df["w_real"],columns = [temp_ind2+ "_"+ sty_v_g]) 
#                 temp_df2 = temp_df2.sort_values(temp_ind2+ "_"+ sty_v_g ,ascending=False )
#                 temp_df2 =temp_df2.reset_index(drop=True)
#                 print("temp_df 2")
#                 print(temp_df2 )
#                 # axis=1,横向表拼接（行对齐） 
#                 # ,ignore_index=True
#                 temp_df0 = pd.concat( [temp_df0, temp_df2 ], axis=1)
#                 print("temp_df0" )
#                 print(temp_df0 )


#                 ######################################################################################
#                 temp_df_pnl = pd.DataFrame( temp_df["pnl"] ,columns = [temp_ind2+ "_"+ sty_v_g])   
#                 temp_df_pnl =temp_df_pnl.reset_index(drop=True)
#                 print("temp_df_pnl ")
#                 print( temp_df_pnl )
#                 temp_df1 = pd.concat( [temp_df0, temp_df_pnl ],axis=1, ignore_index=True )
#                 # print(temp_df1 )

#                 temp_df0.to_csv( path_out +filename_as_pnl_weighted  )
#                 temp_df1.to_csv( path_out +filename_as_pnl  )
#                 j = j+ 1 

#         # except:
#         #     print( path_raw2 ) 
#         i=i+1



# temp_df0.to_csv( path_out +filename_as_pnl_weighted  )
# temp_df1.to_csv( path_out +filename_as_pnl  )


# asd
#################################################################
### 提取组合最新净值  | 181215

# path_out = "D:\\CISS_db\\p1_stat\\"
# i=0
# ind1_list_2 = ["10","40","50","60","20","15","25","45","55","35","30"]
# for sty_v_g in ["value","growth"] :
#     for temp_ind2 in ind1_list_2 :
#         print(temp_ind2," | ", sty_v_g)
#         path_raw2 = "D:\\CISS_db\\port_rc181123_w_allo_"+sty_v_g+"_"+ temp_ind2 +"\\accounts\\"
#         # id_account_1542811496_port_rc181121_value_10_Asum_20181105.csv
#         ## Get all file name in current directory
#         # try:
#         temp_file_list = os.listdir( path_raw2 )
#         ## all file list from earliest modified date to latest at the end 
#         dir_list = sorted(temp_file_list , key=lambda x: os.path.getmtime(os.path.join(path_raw2,x)))
#         dir_list_s = dir_list[-20:] 
#         print("=====================")
#         print( path_raw2  )
        
#         for file_name in dir_list_s :
#             if "Asum.csv" in file_name  :
#             # if date_lastUpdate in file_name and "Asum" in file_name :
#                 print( file_name ) 
#                 print( file_name.split("_") )
#                 temp_df= pd.read_csv( path_raw2 + file_name )
#                 temp_index = temp_df[ temp_df['date']== date_lastUpdate1  ].index[0]
                
#                 temp_df0.loc[temp_ind2+"_"+sty_v_g,"ind1"] = temp_ind2
#                 temp_df0.loc[temp_ind2+"_"+sty_v_g,"date"] = date_lastUpdate
#                 temp_df0.loc[temp_ind2+"_"+sty_v_g,"unit"] =  temp_df.loc[temp_index,"unit" ]
#                 temp_df0.loc[temp_ind2+"_"+sty_v_g,"mdd"] =temp_df.loc[temp_index,"mdd" ]
#                 temp_df0.loc[temp_ind2+"_"+sty_v_g,"market_value"] = temp_df.loc[temp_index,"market_value" ]
#                 temp_df0.loc[temp_ind2+"_"+sty_v_g,"total"] = temp_df.loc[temp_index,"total" ] 
#                 temp_df0.loc[temp_ind2+"_"+sty_v_g,"position_pct"] = temp_df.loc[temp_index,"market_value" ]/temp_df.loc[temp_index,"total" ] 
#                 print(temp_df0.loc[temp_ind2+"_"+sty_v_g,:])
#                 # temp_df.to_csv("D:\\"+"181126_"+temp_ind2+"_"+sty_v_g+'.csv' )
#                 temp_df0.to_csv(path_out +"unit_mdd.csv")

#         # except:
#         #   print( path_raw2 )
#         #   print(   os.listdir( path_raw2 ) )
#             # pass
#         i=i+1 


# temp_df0.to_csv(path_out +"unit_mdd.csv")

# asd

##############################################################################################
### Get all month dates || For ind-X list, get unit and mmdd at every end of month

## get all unit and mddd at  end of months
## path_stat = "C:\\zd_zxjtzq\\RC_trashes\\temp\\sys_stra_24h\\CISS_rc\\db\\db_times\\"
# path_stat = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\db\\db_times\\"
# file_time = "times_CN_day_20070101_20190412.csv" 

# time_raw = pd.read_csv(path_stat + file_time)
# time_raw =time_raw.dropna(axis=0)
# print( "time_raw")
# # print(time_raw )

# time_raw['date'] = pd.to_datetime( time_raw['SSE'] )

# years = [2014,2015,2016,2017,2018]
# years = [2007,2008,2009,2010,2011,2012,2013,2014]
# # float to string 
# months = np.linspace(1,12,12).astype(np.int16)
# import datetime as dt 
# end_mon_dates = []
# for i_year in years:
#     for i_month in months:
#         # notes: i_month might be single digit 
#         temp_date = str(i_year)+'-'+str(i_month)+'-'+'01'
#         temp_date = dt.datetime.strptime(temp_date,"%Y-%m-%d")
#         time_end_mon = time_raw[ time_raw['date']< temp_date ]
#         print(temp_date)
#         print("time_end_mon")
#         print( len(time_end_mon.index)  )
#         if len(time_end_mon.index) > 0 :
#             end_mon_dates = end_mon_dates + [ time_end_mon.loc[time_end_mon.index[-1],'SSE' ] ] 

# # print(end_mon_dates)

# df_end = pd.DataFrame(end_mon_dates,columns=["end_dates_mon"]  )
# print("end_dates_mon" ) 

# file_time2 = "month_end_dates.csv"
# # df_end.to_csv(path_stat+file_time2 )
# print( path_stat+file_time2 )


# #############################################################

# ind1_list_2 = ["10","40","50","60","20","25","45","55","35","30","15"] # 15 is not yet ok 
# path_out = "D:\\CISS_db\\stats\\df_mon\\"
# #############################################################
# ### 按月计算组合收益和回撤
# ### import unit file and get month return, monthly mdd|| For ind-X list, get unit and mmdd at every end of month

# sty_v_g = "value" 
# #  "D:\\CISS_db\\port_rc181122_"+sty_v_g+"_"+ temp_ind2 + "\\accounts\\"
# # # id_account_1542811496_port_rc181121_value_10_Asum_20181105.csv
# path_raw2 = "D:\\CISS_db\\stats\\" # 

# path_out = "D:\\CISS_db\\p1_stat\\"

# df_mon_unit = pd.DataFrame(  )
# df_mon_mdd = pd.DataFrame(  )
# df_mon_ret = pd.DataFrame(  )
# i=0
# ind1_list_2 = ["10","40","50","60","20","15","25","45","55","35","30"]
# ind1_list_2 = ["999"]
# for sty_v_g in ["value","growth"] :
#     for temp_ind2 in ind1_list_2 :
#         print(temp_ind2," | ", sty_v_g)
#         # For industry
#         # path_raw2 = "D:\\CISS_db\\port_rc181123_w_allo_"+sty_v_g+"_"+ temp_ind2 +"\\accounts\\"
#         ### For market portfolio
#         # port_rc181205_market_growth_999
#         # path_raw2 = "D:\\CISS_db\\port_rc181205_market_"+sty_v_g+"_999"+"\\accounts\\"
#         path_raw2 = "D:\\CISS_db\\port_rc1904_market_"+sty_v_g+"_999"+"\\accounts\\"
#         # id_account_1542811496_port_rc181121_value_10_Asum_20181105.csv
#         ## Get all file name in current directory
#         # try:
#         temp_file_list = os.listdir( path_raw2 )
#         ## all file list from earliest modified date to latest at the end 
#         dir_list = sorted(temp_file_list , key=lambda x: os.path.getmtime(os.path.join(path_raw2,x)))
#         dir_list_s = dir_list[-20:] 
#         print("=====================")
#         print( path_raw2  )
        
#         for file_name in dir_list_s :
#             if "Asum.csv" in file_name  :
#             # if date_lastUpdate in file_name and "Asum" in file_name :
#                 print( file_name ) 
#                 print( file_name.split("_") )
#                 temp_df= pd.read_csv( path_raw2 + file_name )
#                 temp_df=temp_df.loc[:,['date','SSE','cash','market_value','total','unit','mdd'] ]
                
#                 ### Get every month end unit and mdd 
#                 temp_df2 = temp_df[ temp_df["date"].isin(end_mon_dates ) ]
#                 # temp_df2.reset_index(drop=True) 
#                 temp_df2.index = temp_df2["date"]
#                 print( "temp_df2" )
#                 # print( temp_df2 )

#                 temp_df2['mdd_mon'] =0.0
#                 temp_df2['ret'] =0.0
#                 i=0 
#                 mdd_mon = 0
#                 for temp_i in temp_df2.index :
#                     # skip the first month
#                     if temp_i != temp_df2.index[0] :
#                         # we want to find monthly return and monthly mdd 
#                         unit_mon_pre = temp_df2.loc[index_pre ,'unit']
                        
#                         temp_mdd = temp_df2.loc[temp_i,'unit']/temp_df2.loc[:index_pre,'unit'].max()-1
#                         temp_df2.loc[temp_i,'mdd_mon'] = min(temp_df2.loc[index_pre,'mdd_mon'] , temp_mdd )
#                         temp_df2.loc[temp_i,'ret'] = temp_df2.loc[temp_i,'unit']/temp_df2.loc[index_pre,'unit']-1
#                         i = i+1
#                     ###
#                     index_pre = temp_i
                
#                 # print( temp_df2["ret"].head() )
#                 ### Assign to df 
#                 # temp_df2.to_csv(path_out+temp_ind2 +"_"+ sty_v_g + '.csv' )
#                 df_ret = pd.DataFrame( temp_df2["ret"] )
#                 df_ret.columns = [temp_ind2+ "_"+ sty_v_g] 
#                 df_mon_ret = pd.concat([df_mon_ret,df_ret ], axis= 1    )

#                 df_mdd = pd.DataFrame( temp_df2["mdd_mon"] ) 
#                 df_mdd.columns = [temp_ind2+ "_"+ sty_v_g]
#                 df_mon_mdd = pd.concat([df_mon_mdd,df_mdd ], axis= 1    )
                
#                 print( df_ret.head() )                
#                 # input1 = input("Type anything")
#                 df_mon_ret.to_csv(path_out+"df_mon_ret_1904.csv"   )
#                 df_mon_mdd.to_csv(path_out+"df_mon_mdd_1904.csv"   ) 

# print("df_mon_ret DESCRIBE")
# print( df_mon_ret.describe() ) 
# print("df_mon_mdd DESCRIBE")
# print( df_mon_mdd.describe() ) 

# ### 1, df with only monthly return and mdd for value group 
# df_mon_ret.to_csv(path_out+"df_mon_ret_1904.csv"   )
# df_mon_ret.describe().to_csv(path_out+"df_mon_ret_des.csv"   )
# ### 2 ,df with only monthly return and mdd for growth group 
# df_mon_mdd.to_csv(path_out+"df_mon_mdd_1904.csv"   ) 
# df_mon_mdd.describe().to_csv(path_out+"df_mon_mdd_des.csv"   )

# asd

#####################################################################################
### 需要引入每个行业指数的月度收益，进行比较
# # ["10","40","50","60","20","15","25","45","55","35","30"]
# path_out = "D:\\CISS_db\\p1_stat\\"
# path_data_wind = "D:\\data_Input_Wind\\"
# index_list_ind = ["882001.WI","882007.WI","882009.WI","882011.WI","882003.WI","882002.WI","882004.WI","882008.WI","882010.WI","882006.WI","882005.WI" ]
# ind1_list_2 = ["10","40","50","60","20","15","25","45","55","35","30"]

# index_list_ind = ["000300.SH" ]
# ind1_list_2 = ["999"]

# df_mon_mdd_index = pd.DataFrame(  )
# df_mon_ret_index = pd.DataFrame(  )
# j=0

# for temp_index in index_list_ind :
#     print(temp_index)
#     # temp_index = "882001.WI"
#     file_path = path_data_wind + "Wind_"+temp_index +"_updated.csv"
#     temp_df = pd.read_csv(file_path )

#     ## Get every month end unit and mdd 
#     temp_df["date"] = temp_df["DATE"]
#     temp_df2 = temp_df[ temp_df["DATE"].isin(end_mon_dates ) ]
#     # temp_df2.reset_index(drop=True) 
#     temp_df2["unit"] = temp_df2["CLOSE"]/temp_df2["CLOSE"].values[0]
#     temp_df2.index = temp_df2["date"]
#     print( "temp_df2" )
#     # print( temp_df2.head() )

#     temp_df2['mdd_mon'] =0.0
#     temp_df2['ret'] =0.0
#     i=0 
#     mdd_mon = 0
#     for temp_i in temp_df2.index :
#         # skip the first month
#         if temp_i != temp_df2.index[0] :
#             # we want to find monthly return and monthly mdd 
#             unit_mon_pre = temp_df2.loc[index_pre ,'unit']
            
#             temp_mdd = temp_df2.loc[temp_i,'unit']/temp_df2.loc[:index_pre,'unit'].max()-1
#             temp_df2.loc[temp_i,'mdd_mon'] = min(temp_df2.loc[index_pre,'mdd_mon'] , temp_mdd )
#             temp_df2.loc[temp_i,'ret'] = temp_df2.loc[temp_i,'unit']/temp_df2.loc[index_pre,'unit']-1
#             i = i+1
#         ###
#         index_pre = temp_i

#     print("Working on industry ", ind1_list_2[j]  )
#     print( temp_df2.head() )
#     ### Assign to df 
#     # temp_df2.to_csv(path_out+temp_ind2 +"_"+ sty_v_g + '.csv' )
#     df_ret = pd.DataFrame( temp_df2["ret"] )
#     df_ret.columns = [ ind1_list_2[j] ] 
#     df_mon_ret_index = pd.concat([df_mon_ret_index,df_ret ], axis= 1    )

#     df_mdd = pd.DataFrame( temp_df2["mdd_mon"] ) 
#     df_mdd.columns = [  ind1_list_2[j] ]
#     df_mon_mdd_index = pd.concat([df_mon_mdd_index,df_mdd ], axis= 1    )
    
#     print( df_ret.head() )                
#     # input1 = input("Type anything")
#     df_mon_ret_index.to_csv(path_out+"df_mon_ret_index.csv"   )
#     df_mon_mdd_index.to_csv(path_out+"df_mon_mdd_index.csv"   )
#     ####
#     j = j +1 


# print("df_mon_ret_index DESCRIBE")
# print( df_mon_ret_index.describe() ) 
# print("df_mon_mdd_index DESCRIBE")
# print( df_mon_mdd_index.describe() ) 

# ### 1, df with only monthly return and mdd for value group 
# df_mon_ret_index.to_csv(path_out+"df_mon_ret_index.csv"   )
# df_mon_ret_index.describe().to_csv(path_out+"df_mon_ret_des_index.csv"   )
# ### 2 ,df with only monthly return and mdd for growth group 
# df_mon_mdd_index.to_csv(path_out+"df_mon_mdd_index.csv"   ) 
# df_mon_mdd_index.describe().to_csv(path_out+"df_mon_mdd_des_index.csv"   )


# asd

#############################################################
### Import related index and make comparision
# codelist_ind4.csv | 
# wind_code sec_name    ind4_index_code ind3_index_code ind2_index_code ind1_index_code ind4_code   ind3_code   ind2_code   ind1_code
path_indX = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\db\\db_assets\\"
file_name = "codelist_ind4.csv"
print( path_indX + file_name  )
df_indX = pd.read_csv(path_indX + file_name ,encoding="gbk")
# # df1.to_csv("D:\\df1.csv")
# ind1_index_list = list( df_indX["ind1_index_code"].drop_duplicates() )
# print( ind1_index_list )
# items = ["open","high","low","close","volume","amt","chg","pct_chg"]
# from db.data_io import data_wind
# data_wind_1 = data_wind( '','D:\\db_wind\\')
# date_start = "2010-01-01"
# date_end = ""
file_path0 = 'D:\\db_wind\\'

# temp_ind2 ="10"
for temp_ind2 in ind1_list_2 :   
    # related indX_index with indX_code 

    temp_df_indX = df_indX[df_indX["ind1_code"]==int(temp_ind2) ]
    # print( temp_df_indX.head(4) )
    # print( temp_df_indX.loc[temp_df_indX.index[0]  ,"ind1_index_code"] )
    # ind1_index_code = 882007.WI
    ind1_index_code = temp_df_indX.loc[temp_df_indX.index[0]  ,"ind1_index_code"]
    temp_index = ind1_index_code
    ##############################################################################
    ### download index code 
    # print( temp_index )
    # wind_obj_0 = data_wind_1.data_wind_wsd(temp_index,date_start,date_end,'day')    
    # temp_path = file_path0 + "Wind_"+temp_index+"_updated.csv" 
    # print( temp_path )
    # wind_obj_0.wind_df.to_csv(temp_path )
    ###############################################################################
    temp_path = file_path0 + "Wind_"+temp_index+"_updated.csv" 
    wind_df = pd.read_csv( temp_path  )    
    # D:\db_wind\Wind_882009.WI_updated.csv
    print( "wind_df" )
    # print(  wind_df.tail() )
    
    ###############################################################################
    ### Get every month end unit and mdd 
    wind_df2 = wind_df[ wind_df["date"].isin(end_mon_dates ) ]
    print( wind_df2.head()  )
    index_pre = wind_df2.index[0]
    unit_mon_pre = 1
    ret_mon_pre = 0 
    mdd_mon_pre = 0
    df_mon_index = pd.DataFrame( columns=['date','unit','ret','mdd'] )
    i=0 
    close_init = wind_df2.loc[ wind_df2.index[0] ,'close']  
    unit_mon_pre =1
    for temp_i in wind_df2.index :
        # skip the first month
        if temp_i != wind_df2.index[0] :
            # we want to find monthly return and monthly mdd 
            close_mon = wind_df2.loc[temp_i,'close'] 
            unit_mon = wind_df2.loc[temp_i,'close']/close_init
            ret_mon = unit_mon/unit_mon_pre-1
            mdd_mon = 0
            for temp_i2 in range(index_pre,temp_i):
                mdd_mon = min(mdd_mon , wind_df2.loc[temp_i,'close']/wind_df2.loc[index_pre:temp_i,'close'].max()-1 )
            df_mon_index.loc[i,:] = [wind_df2.loc[temp_i,'date']  ,unit_mon,ret_mon,mdd_mon ]
            
            # todo 当前的mdd是月度mdd，但我们还希望看到月内的日频率数据的mdd
            
            # update data for pre period
            unit_mon_pre =unit_mon
            ret_mon_pre = ret_mon
            mdd_mon_pre = mdd_mon
            i = i+1


    print("df_mon_index")
    print( df_mon_index )

    df_mon_index.to_csv(path_out+temp_ind2 +"_"+ temp_index+ '.csv' )

##############################################################################
print(  )
date_lastUpdate = "20171229"

temp_file_list = os.listdir( path_raw2 )
## all file list from earliest modified date to latest at the end 
dir_list = sorted(temp_file_list , key=lambda x: os.path.getmtime(os.path.join(path_raw2,x)))
dir_list_s = dir_list[-5:] 
for file_name in dir_list_s :
    if date_lastUpdate in file_name and "Asum" in file_name :
        temp_df= pd.read_csv( path_raw2 + file_name  )

        temp_index = temp_df[ temp_df['date']== date_lastUpdate  ].index[0]
        
        temp_df0.loc[temp_ind2+"_"+sty_v_g,"ind1"] = temp_ind2
        temp_df0.loc[temp_ind2+"_"+sty_v_g,"date"] = date_lastUpdate
        temp_df0.loc[temp_ind2+"_"+sty_v_g,"unit"] =  temp_df.loc[temp_index,"unit" ]
        temp_df0.loc[temp_ind2+"_"+sty_v_g,"mdd"] =temp_df.loc[temp_index,"mdd" ]
        temp_df0.loc[temp_ind2+"_"+sty_v_g,"market_value"] = temp_df.loc[temp_index,"market_value" ]
        temp_df0.loc[temp_ind2+"_"+sty_v_g,"total"] = temp_df.loc[temp_index,"total" ] 
        print(temp_df0.loc[temp_ind2+"_"+sty_v_g,:])
        print("DEBUG======================")
        print( temp_df.head() )
        asd

# ###############################################################################
# import time
# start = time.clock()
# import datetime as dt 


### Asum 
### part1：1，画净值图；2，月度收益统计，回撤统计；3所有value和growth组合相邻比较，因为我们认为不同行业不太有可比性
###。part2：1，用anchor stock 分别和主动基准，行业指数做回归； 2，用11个value，growth主动基准和市场组合做回归，
### part3：1，组合统计描述；2,相关系数。

# 看看在多少个月份可以打败市场？？？ | 还有行业对行业。

































'''
https://baike.baidu.com/item/%E6%8A%80%E6%9C%AF%E7%BB%8F%E6%B5%8E%E5%AD%A6/81356?fr=aladdin

技术经济分析、论证、评价的方法很多，最常见的有决定型分析评价法、经济型分析评价法、不确定型分析评价法、比较型分析评价法、系统分析法价值分析法、可行性分析法等。其中最为基础的是层次分析法
薄弱环节
技术经济学科，没有形成自己的特色，一些领域的研究急需加强。存在的主要问题是：
1、学科理论有待创新与完善。技术经济学科尚没有形成自己完整的理论体系，技术经济学界对它的理论构架、学科体系、研究对象和研究内容存在较多争论。当今技术经济学已经开创了许多新的研究领域，例如，技术创新、生产率分析、资源经济、环境经济、知识经济、循环经济等，但理论较零散，缺乏技术经济学学科理论上的系统归纳。原有的学科体系和理论架构需要完善和再创新。
'''







































































elapsed = (time.clock() - start)
print("Total time used:",elapsed)



###############################################################################
### Portfolio rebalance： trade frequency，number, potential scale
# # 组合调整：交易频率，交易笔数，可支撑的组合规模
# # D:\CISS_db\port_rc181130_w_allo_growth_999\trades
# # trades_id_1543564755_port_rc181130_w_allo_growth_999_TB_20180531
# path0 = "D:\\CISS_db\\port_rc181130_w_allo_growth_999\\"
# path_TB = path0 + "trades\\"
# sty_v_g =  "growth" # "value" # "growth"
# file_TB = "trades_id_1543564755_port_rc181130_w_allo_"+sty_v_g+"_999_TB_20180531.csv"
# path_accounts = path0 + "accounts\\"
# file_asum = "id_account_1543564755_port_rc181130_w_allo_"+sty_v_g+"_999_Asum_20180531.csv"
# df_tb = pd.read_csv(path_TB + file_TB )
# print("df_tb")
# print( df_tb.info() )
# # length of years 
# time_start = dt.datetime.strptime( df_tb['datetime'].values[0],"%Y-%m-%d" )
# time_end =  dt.datetime.strptime(  df_tb['datetime'].values[-1] ,"%Y-%m-%d")
# # 2014-06-03 00:00:00 2017-12-06 00:00:00
# print( time_start,time_end )
# print( type( time_end-time_start ) )
# # 1282 days, 0:00:00
# # Get number of years

# num_years = (time_end-time_start)/dt.timedelta(days=365)
# ave_num_annual = len( df_tb['datetime'] )/ num_years
# print("Number of years ", round(num_years ,3 ) )
# # average number of trades per year 
# ave_num_annual = len( df_tb['datetime'] )/ num_years
# print("Average number of trades per year ", round(ave_num_annual ,0) )

# # average amount of trades 
# ave_amount_trade = df_tb['amount'].mean()
# print("Average amount of trades ", round(ave_amount_trade,2) )

# # average profit of trades 
# ave_profit_trade = df_tb['profit_real'].mean()
# print("Average profit of trades ", round(ave_profit_trade,2) )
# print("Average profit pct of trades ", round(ave_profit_trade*100/ave_amount_trade,2),"% " )

# # turnover rate per year || should be total amount minus first period 100% and then devide by 2 
# # todo:应该基于净值，而不是初始initial capital，会不合理。
# df_asum = pd.read_csv( path_accounts+ file_asum )
# print("df_asum")
# # print( df_asum.info() )
# print( df_asum.head() )
# df_tb['turnover'] = 0.0
# date_list = list( df_tb["datetime"].drop_duplicates(  ) )
# for temp_date in date_list :
#     index_list = df_tb[df_tb["datetime"]==temp_date ].index
#     indx_asum = df_asum[df_asum['date']==temp_date ].index[0]
#     temp_total = df_asum.loc[indx_asum,'total']
#     # print("temp_total ",temp_total)
#     # print("Index_list",index_list   ) 
#     df_tb.loc[index_list,'turnover']= df_tb.loc[index_list,'amount']/ temp_total

# # devide by 2 is for buy and sell 
# turnover_year = (df_tb["turnover"].sum()-1 )/2/num_years
# print("Turnover rate per year ", round(turnover_year*100 ,2 ),"%" )
# print("Turnover rate per 6 months ", round(turnover_year*100/2 ,2 ),"%" )

# df_tb.to_csv("D:\\df_tb.csv")
# # todo:how about effect of rebalancing on portfolio performanc on adjusting week or month?
# # 组合调整当月对于绩效的影响？
# # step2: denote T for every adjusting/trading  date 5-31 and 11-30, 
# # step 1,T+1 to T+5,观察5天内相对市场是否有abnormal return or risk, 
# # step 2,in next month 1,2,3, observe whether  there is a decaying effect on excess return 
# # just working on excel file : only obeserve monthly return of 6,7,8 and 12,1,2

# # Trading cost per year 
# cost_trade_year = (df_tb["fees"].sum()-300000000*0.0025 )/num_years
# print("Trading cost per year is ",round(cost_trade_year,2  )  )
# print("Trading cost pct per year ", round(turnover_year*100*2*0.0025 ,2 ),"%" )
# # for growth portfolio , trade cost per year is 0.72% of NAV,
# # we estimate value portfolio has a benefit on portfolio rebalancing cost. 


































