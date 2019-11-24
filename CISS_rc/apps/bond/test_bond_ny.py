# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：对中债指数进行分位数策略计算
last update 181226 | since 190103
Menu : 

todo:


Notes:

===============================================
'''
import json
import pandas as pd 
import sys
sys.path.append("..") 
from db.times import times
times0 = times('CN','cb')
method4time='stock_index_sp'
method4time='bond_index_cb'


###########################################################################
###########################################################################
### 1,  
### 2,  
### 3, 

###########################################################################
### 

###########################################################################
### step 1 下周债券资产数据，指数
temp_date = "2007-01-01" # "2011-01-01" "2014-05-31" 
# temp_date = "2018-09-31" # "2014-05-31" 
temp_date2 = "2019-01-03" # "2014-11-30" 

temp_date_now = times0.get_time_format('%Y%m%d','str')

import WindPy as WP
# w.wsd("CBA00203.CS", "open,high,low,close,volume,amt,yield_cnbd", "2018-12-04", "2019-01-02", "credibility=1;TradingCalendar=NIB")
WP.w.start()

#############################################################################
### single symbol case  
# symbol = "CBA00203.CS"
# WindData= WP.w.wsd(symbol , "close",temp_date, temp_date2 , "returnType=1;TradingCalendar=NIB")
# df1 = pd.DataFrame(WindData.Data,columns=WindData.Times)
# df1=df1.T
# df1.columns = [symbol]
# path_out = "D:\\db_wind\\index\\"
# df1.to_csv(path_out + symbol + ".csv")

#############################################################################
### symbol list case 
path_out = "D:\\db_wind\\index\\"
# Choice 1 
# symbol_list = ["CBA00203.CS","CBA05203.CS","CBA04233.CS","CBA02203.CS"]
### Choice 2
# # 下载超限额，注意，上次最多下载到 Working on symbol  CBD00113.CS
# # [['CWSDService: quota exceeded.']]
# symbol_list0 = pd.read_csv(path_out+"inde_list_chinabond.csv",encoding="gbk")
# print( symbol_list0.info() )
# symbol_list = symbol_list0["code"]

# i=0 
# for symbol in symbol_list : 
#     print("Working on symbol ", symbol )
#     if i > 164:
#         # try :
#         # w.wsd("CBA05203.CS", "ytm_b,close", "2012-12-16", "2013-01-14", "returnType=1;TradingCalendar=NIB")
#         WindData= WP.w.wsd(symbol , ["close", "ytm_b"],temp_date, temp_date2 , "returnType=1;TradingCalendar=NIB")
#         print(WindData.Data)
#         df1 = pd.DataFrame(WindData.Data,columns=WindData.Times)
#         df1=df1.T
#         df1.columns = [symbol+"_close",symbol+"_ytm"]
        
#         df1.to_csv(path_out + symbol + ".csv")
#         # except:
#         #     pass

#     i=i+1

# Choice 3 | since 190120
# 下载超限额，注意，上次最多下载到 Working on symbol  CBD00113.CS
# [['CWSDService: quota exceeded.']]
temp_date = "2013-01-01" # "2011-01-01" "2014-05-31" 
temp_date2 = "2019-01-18" # "2014-11-30" 

path_0 = "D:\\CISS_db\\bond_pct\\"
path_0 = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\static\\"
file_name = "chinabond_index_1to1_s.csv"
symbol_list0 = pd.read_csv(path_0+ file_name ,encoding="gbk")
print( symbol_list0.info() )
# symbol_full_price symbol_net_price 

i=0 
for temp_i in symbol_list0.index : 
    symbol_full_price = symbol_list0.loc[temp_i, "symbol_full_price"]
    symbol_net_price  = symbol_list0.loc[temp_i, "symbol_net_price"]

    print("Working on symbol ", symbol_full_price ,symbol_net_price  )
    for symbol in [symbol_full_price, symbol_net_price  ] :
        # 只下载收盘价
        # w.wsd("CBA05203.CS", "ytm_b,close", "2012-12-16", "2013-01-14", "returnType=1;TradingCalendar=NIB")
        WindData= WP.w.wsd(symbol , ["close" ],temp_date, temp_date2 , "returnType=1;TradingCalendar=NIB")
        print(WindData.Data)
        df1 = pd.DataFrame(WindData.Data,columns=WindData.Times)
        df1=df1.T
        df1.columns = [symbol+"_close"]
        
        df1.to_csv(path_out + symbol + ".csv")
    
    i=i+1

asd
#############################################################################

symbol = "CBA00203.CS"
df= pd.read_csv(path_out + symbol + ".csv"  )
df.index = df["Unnamed: 0"]
df= df.drop(["Unnamed: 0"] ,axis=1 )
df["close"] =df[symbol] 

#############################################################################
### Strategy ： MA
df["close_quan_010"] =df["close"].rolling(2000,min_periods=1).quantile(0.10)
df["close_quan_025"] =df["close"].rolling(2000,min_periods=1).quantile(0.25)
df["close_quan_075"] =df["close"].rolling(2000,min_periods=1).quantile(0.75)
df["close_quan_090"] =df["close"].rolling(2000,min_periods=1).quantile(0.90)



print( df.tail()  )
df.to_csv("D:\\"+ symbol + "_ana.csv"  )
asd
#############################################################################
### Strategy ： MA
# MA = [20,40,60]
# ma_str = "204060"

# df["ma_s"] = df["close"].rolling(MA[0], min_periods=1).mean()
# df["ma_m"] = df["close"].rolling(MA[1], min_periods=1).mean()
# df["ma_l"] = df["close"].rolling(MA[2], min_periods=1).mean()
# df["ana_s"] = df["ma_s"] > df["ma_m"] 
# df["ana_m"] = df["ma_m"]>= df["ma_l"] 
# df["sig"] = df["ana_s"]*df["ana_m"]




#############################################################################
### Portfolio test

df_stra= pd.DataFrame( index =df.index )
df_stra["sig"] = df["sig"]
df_stra["cash"] = 0.0 
df_stra["total_cost"] = 0.0 
df_stra["market_value"] = 0.0 
df_stra["total"] = 0.0 
df_stra["unit"] = 0.0 
df_stra["mdd"] = 0.0 
df_stra["number"] = 0 

### para setting 
fees = 0.0001
init_capital = 1000000000
cash_level = 0.05 
ret_gc = 0.025



i= 0 
for temp_i in df_stra.index :

    if i >= 30 :
        ### check strategy signal 
        print("==============  ", i,temp_i_pre1,temp_i_pre2 )
        if df_stra.loc[temp_i_pre1,"sig" ] and (not df_stra.loc[temp_i_pre2,"sig" ]) :
            ### buy action if buy signal from pre trading day 
            cash_budget = df_stra.loc[temp_i_pre1,"cash" ] - df_stra.loc[temp_i_pre1,"total"]*cash_level
            num = round(cash_budget/df.loc[temp_i,"close"]/100 )*100
            df_stra.loc[temp_i,"num" ] = num 

            df_stra.loc[temp_i,"total_cost" ] = df_stra.loc[temp_i,"num" ]*df.loc[temp_i,"close"]
            df_stra.loc[temp_i,"market_value" ] = df_stra.loc[temp_i,"num" ]*df.loc[temp_i,"close"]
            df_stra.loc[temp_i,"cash" ] = df_stra.loc[temp_i_pre1,"cash" ] - df_stra.loc[temp_i,"num" ]*df.loc[temp_i,"close"]*(1+fees)
            df_stra.loc[temp_i,"cash" ] = df_stra.loc[temp_i,"cash" ]*(1+ret_gc/365)
            df_stra.loc[temp_i,"total" ] = df_stra.loc[temp_i,"cash" ]+df_stra.loc[temp_i,"market_value" ]
            df_stra.loc[temp_i,"unit" ] = df_stra.loc[temp_i,"total" ]/ init_capital


        elif  df_stra.loc[temp_i_pre1,"sig" ] and  df_stra.loc[temp_i_pre2,"sig" ] :
            ### hold and update holding 
            df_stra.loc[temp_i,"total_cost" ] = df_stra.loc[temp_i_pre1,"total_cost" ] 
            df_stra.loc[temp_i,"num" ] =df_stra.loc[temp_i_pre1,"num" ]
            df_stra.loc[temp_i,"market_value" ] = df_stra.loc[temp_i,"num" ]*df.loc[temp_i,"close"]
            df_stra.loc[temp_i,"cash" ] = df_stra.loc[temp_i_pre1,"cash" ]*(1+ret_gc/365)
            df_stra.loc[temp_i,"total" ] = df_stra.loc[temp_i,"cash" ]+df_stra.loc[temp_i,"market_value" ]
            df_stra.loc[temp_i,"unit" ] = df_stra.loc[temp_i,"total" ]/ init_capital

        elif  (not df_stra.loc[temp_i_pre1,"sig" ] ) and  df_stra.loc[temp_i_pre2,"sig" ] :
            ### sell action
            df_stra.loc[temp_i,"num" ] = 0
            df_stra.loc[temp_i,"total_cost" ] = 0
            df_stra.loc[temp_i,"market_value" ] = 0
            df_stra.loc[temp_i,"cash" ] = df_stra.loc[temp_i_pre1,"cash" ] + df_stra.loc[temp_i_pre1,"num" ]*df.loc[temp_i,"close"]*(1-fees)
            df_stra.loc[temp_i,"cash" ] = df_stra.loc[temp_i,"cash" ]*(1+ret_gc/365)
            df_stra.loc[temp_i,"total" ] = df_stra.loc[temp_i,"cash" ]+df_stra.loc[temp_i,"market_value" ]
            df_stra.loc[temp_i,"unit" ] = df_stra.loc[temp_i,"total" ]/ init_capital

        else : 
            ### not holding 
            df_stra.loc[temp_i,"cash" ] = df_stra.loc[temp_i_pre1,"cash" ]*(1+ret_gc/365)
            df_stra.loc[temp_i,"total" ] = df_stra.loc[temp_i,"cash" ]+df_stra.loc[temp_i,"market_value" ]
            df_stra.loc[temp_i,"unit" ] = df_stra.loc[temp_i,"total" ]/ init_capital
        ### get mdd 
        
        temp_mdd = df_stra.loc[temp_i,"unit"]/ df_stra.loc[:,"unit"].max()-1 
        df_stra.loc[temp_i,"mdd" ] = min(df_stra.loc[temp_i_pre1,"mdd" ], temp_mdd)

    elif  i == 0 :
        ### account initialization 
        df_stra.loc[temp_i,"cash" ] = init_capital 
        df_stra.loc[temp_i,"total" ] = init_capital 
        df_stra.loc[temp_i,"unit" ] = 1.00 

        temp_i_pre2 = temp_i
        temp_i_pre1 = temp_i 

    else : 
        df_stra.loc[temp_i,"cash" ] = df_stra.loc[temp_i_pre1,"cash" ]*(1+ret_gc/365)
        df_stra.loc[temp_i,"total" ] = df_stra.loc[temp_i,"cash" ]+df_stra.loc[temp_i,"market_value" ]
        df_stra.loc[temp_i,"unit" ] = df_stra.loc[temp_i,"total" ]/ init_capital

    i  = i+1 
    temp_i_pre2 = temp_i_pre1
    temp_i_pre1 = temp_i 

print( df_stra.tail() )
df_stra.to_csv("D:\\df_stra"+ ma_str +".csv")


### results
## 20110104 to 20190103
## fees = 0.0025
## MA=[15,30,60] unit at 1.0944,-2.90
## MA=[20,40,60] unit at 1.071 ,-3.62
## MA=[40,100,100] unit at 1.127 ,-2.540
## MA=[40,100,160] unit at 1.108 ,-2.50

## 20110104 to 20190103
## fees = 0.0001
## MA=[15,30,60] 2019-01-03  1.201847 -0.018379
## MA=[20,40,60] 2019-01-03  1.201847 -0.018379
## MA=[40,100,100]  2019-01-03  1.171330 -0.020483
## MA=[40,100,160] 2019-01-03  1.152310 -0.020483


######################################################################################
### todo : 实现债券分位数策略和 均线策略。

######################################################################################
### 分位数策略逻辑：
# CBA 7-10年国开，3-5企业AAA，货币市场基金可投资债券 | a,b,c
### 计算历史收益率所属分位数 quan_cdb_7y,quan_corp_3a_3y,quan_money
# todo，需要下载收益率指数，而不是全价。
### quan_cdb_7y,quan_corp_3a_3y,quan_money
# w_cdb_7y = quan_cdb_7y - 0.1
# w_corp_3a_3y = quan_corp_3a_3y - 0.45 
# w_money = 1 - w_cdb_7y - w_corp_3a_3y
### periodic adjustment ？



# todo，需要下载收益率指数，而不是全价。

















