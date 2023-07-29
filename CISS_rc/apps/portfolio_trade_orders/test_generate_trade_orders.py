# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
todo
1,
last 200725 | since 20200630

功能：用于将策略和模型的组合配置文件转换成恒生O32系统的交易指令
1，导入下一交易日指数公司文件和O32系统最新持仓文件

基金经理需求：调仓计算改进
    1，禁投股票剔除问题
    2，新股被锁定问题
    3，当日买入算入冻结股
    4，未来转融券锁定影响可用问题
    5，vba根据文件名格式直接自动化复制粘贴成分股和目前实际持仓

CGS-Excel表格功能梳理：
1，股票池：实际持仓股票分成成分股池子和非成分股池子。

2，个股维度：数量、前收盘价、前收市值、前收权重、最新价、最新市值、最新市值权重

''' 
import os 
import pandas as pd 
import math
import datetime as dt
#####################################################################
### 导入配置文件 
### Input: date from typing or date from WindPy;只需要最近的2个交易日

path_py = os.getcwd()
df_config = pd.read_csv(path_py+ "\\"+ "config.csv",index_col=0,encoding="gbk" )
fund_name_o32 = df_config.loc["fund_name_o32","value"]  
index_name_csi = df_config.loc["index_name_csi","value"]  

path_o32 = df_config.loc["path_o32","value"]  
path_o32_output = df_config.loc["path_o32_output","value"]  
path_index_csi = df_config.loc["path_index_csi","value"]  
temp_date = df_config.loc["date","value"] 
temp_date_pre = df_config.loc["date_pre","value"]  
para_stock_mv_nav_max = float(df_config.loc["para_stock_mv_nav_max","value"]  )

# if 906 
if len( str(index_name_csi) ) < 6 :
    len1 = 6 - len( str(index_name_csi) )
    index_name_csi =  "0"*len1 + str(index_name_csi)

# temp_date = "20200629" # input("type in current trading date，such as 20200629:") 

if len( temp_date ) <8 :
    date_latest = dt.datetime.strftime( dt.datetime.now(), "%Y%m%d" ) 
    date_pre_30d =dt.datetime.strftime( dt.datetime.now() - dt.timedelta(days=14), "%Y%m%d" ) 
    # 用WindPy获取区间交易日
    import WindPy as wp 
    wp.w.start()
    # data=wp.w.tdays("2020-06-01", "2020-07-01", "")
    data_obj = wp.w.tdays(date_pre_30d, date_latest, "")
    ### data_obj.Data[0] 是升序排列，取最后2个值
    temp_date =  dt.datetime.strftime( data_obj.Data[0][-1] ,"%Y%m%d" )
    temp_date_pre =  dt.datetime.strftime( data_obj.Data[0][-2] ,"%Y%m%d" )

# else :
#     temp_date_pre ="20200628" # input("type in current trading date，such as 20200628:") 

#####################################################################
### Import

### 配置文件名称：
# path_index_csi = "D:\\db_wind\\data_index_constitutes\\"
# file_index_csi = "000906weightnextday"+ temp_date +".xls"
file_index_csi = index_name_csi + "weightnextday"+ temp_date +".xls"
sheet_index_csi = "Index Constituents Data"

# pre trading day 
file_index_csi_pre = index_name_csi +"weightnextday"+ temp_date_pre +".xls"

'''columns:
"生效日期Effective Date"	2020-06-30
"指数代码Index Code"	000906
"指数名称Index Name"	中证800
"指数英文名称Index Name(Eng.)"	CSI 800
"成分券代码Constituent Code"	000001
"成分券名称Constituent Name"	平安银行
"成分券英文名称Constituent Name(Eng.)"	Ping An Bank Co., Ltd.
"交易所Exchange"	Shenzhen
"总股本(股)\nTotal A Shares(share)"	19405918198
"自由流通比例(%)(归档后)Categorized  Inclusion Factor(%)"	50
"计算用股本(股)Shares in Index(share)"	9702959099
"权重因子Cap Factor"	1.000000
"收盘Close "	12.800
"调整后开盘参考价Reference Open Price for Next Trading Day"	12.800
"总市值Total Market Capitalization"	248395752934
"计算用市值Market Cap in Index"	124197876467
"权重(%) Weight(%)"	0.640
"交易货币 Trading Currency"	CNY
"汇率 Exchange Rate"	1.000000
'''
# path_o32 = "D:\\CISS_db\\o32_port_trade\\"
# file_o32 = "o32_tongqin800_"+ temp_date +".xls"
file_o32 = "o32_"+ fund_name_o32 +"_"+ temp_date +".xls"
sheet_o32 = "Current Holdings"
'''columns:
序号	1
日期	2020-06-22
基金名称	公募长盛同庆中证800指数
组合名称	同庆股票组
证券代码	601318
证券名称	中国平安
市值	14,087,325.99
持仓	193,587.00
T日指令可用数量	193,587.00
网上新股待上市数量	0.00
网下新股待上市数量	0.00
冻结数量	0.00
解冻数量	0.00
当日临时冻结数量	0.00
当日临时解冻数量	0.00
T+1临时冻结数量	0.00
T+1冻结数量	0.00
市值比例(%)	4.3747

notes:1,"冻结数量"为正数时会包括网上和网下新股待上市数量，负值时一般对应网下新股上市收入；
    2，"市值比例(%)"是股票市值比股票市值之和，不考虑非股票部分
'''
### import df_index_csi
df_index_csi = pd.read_excel(path_index_csi+file_index_csi ,sheet_name=sheet_index_csi ,encoding="gbk" ) 
### change column name 
df_index_csi = df_index_csi.rename( columns={"成分券代码\nConstituent Code":"code_csi"} )
df_index_csi = df_index_csi.rename( columns={"收盘\nClose ":"close"} )
df_index_csi = df_index_csi.rename( columns={"调整后开盘参考价\nReference Open Price for Next Trading Day":"open_adj"} )
df_index_csi = df_index_csi.rename( columns={"总市值\nTotal Market Capitalization":"mv_total"} )
df_index_csi = df_index_csi.rename( columns={"计算用市值\nMarket Cap in Index":"mv_cal"} )
df_index_csi = df_index_csi.rename( columns={"权重(%)\nWeight(%)":"weight"} )
df_index_csi = df_index_csi.rename( columns={"总股本(股)\nTotal A Shares(share)":"total_share"} )
print("df_index_csi")# , df_index_csi.head().T )

### import df_index_csi_pre
# df_index_csi_pre 主要用到前一日总股本="总股本(股)\nTotal A Shares(share)":"total_share"
df_index_csi_pre = pd.read_excel(path_index_csi+file_index_csi ,sheet_name=sheet_index_csi ,encoding="gbk" ) 
### change column name 
df_index_csi_pre = df_index_csi_pre.rename( columns={"成分券代码\nConstituent Code":"code_csi"} )
# df_index_csi_pre = df_index_csi_pre.rename( columns={"收盘\nClose ":"close"} )
# df_index_csi_pre = df_index_csi_pre.rename( columns={"调整后开盘参考价\nReference Open Price for Next Trading Day":"open_adj"} )
# df_index_csi_pre = df_index_csi_pre.rename( columns={"总市值\nTotal Market Capitalization":"mv_total"} )
# df_index_csi_pre = df_index_csi_pre.rename( columns={"计算用市值\nMarket Cap in Index":"mv_cal"} )
# df_index_csi_pre = df_index_csi_pre.rename( columns={"权重(%)\nWeight(%)":"weight"} )
df_index_csi_pre = df_index_csi_pre.rename( columns={"总股本(股)\nTotal A Shares(share)":"total_share"} )

### import df_o32
df_o32 = pd.read_excel(path_o32 +file_o32 ,sheet_name=sheet_o32,encoding="gbk"  ) 
df_o32 = df_o32.rename( columns={"证券代码":"code_o32"} )
df_o32 = df_o32.rename( columns={"市值":"mv"} )
df_o32 = df_o32.rename( columns={"持仓":"num_all"} )
df_o32 = df_o32.rename( columns={"T日指令可用数量":"num_available"} )
# df_o32 = df_o32.rename( columns={"网上新股待上市数量":"num_ipo_online"} )
# df_o32 = df_o32.rename( columns={"网下新股待上市数量":"num_ipo_down"} )
df_o32 = df_o32.rename( columns={"冻结数量":"num_frozen"} )
df_o32 = df_o32.rename( columns={"解冻数量":"num_unfreeze"} )
df_o32 = df_o32.rename( columns={"市值比例(%)":"weight_stock"} )
# from 净值比例(%) to  市值比净值
df_o32 = df_o32.rename( columns={"市值比净值":"weight_nav"} )
df_o32 = df_o32.rename( columns={"当日买量":"num_buy"} )
# 持仓多空标志: 证券融出 or 多仓
df_o32 = df_o32.rename( columns={"持仓多空标志":"borrowed"} )
print("df_o32")# , df_o32.head().T )

#####################################################################
### 将融券部分股票的【"市值","持仓","T日指令可用数量","市值比净值"】加到对应股票列
df_o32_borrow = df_o32[ df_o32["borrowed"] == "证券融出"]
### 只保留多仓
df_o32_longonly = df_o32[ df_o32["borrowed"] == "多仓"]

for temp_i in df_o32_borrow.index :
    temp_code = df_o32_borrow.loc[temp_i, "code_o32"]
    # find code in df_o32_longonly
    df_o32_sub = df_o32_longonly[df_o32_longonly["code_o32"]== temp_code ]
    if len(df_o32_sub.index) == 1 :
        temp_j = df_o32_sub.index[0]
        df_o32_longonly.loc[temp_j ,"mv" ]=df_o32_longonly.loc[temp_j ,"mv" ] + df_o32_borrow.loc[temp_i, "mv"]
        df_o32_longonly.loc[temp_j ,"num_all" ]=df_o32_longonly.loc[temp_j ,"num_all" ] + df_o32_borrow.loc[temp_i, "num_all"]
        df_o32_longonly.loc[temp_j ,"num_available"]=df_o32_longonly.loc[temp_j ,"num_available"] + df_o32_borrow.loc[temp_i, "num_available"]
        df_o32_longonly.loc[temp_j ,"weight_stock" ]=df_o32_longonly.loc[temp_j ,"weight_stock"] + df_o32_borrow.loc[temp_i,"weight_stock"]
        df_o32_longonly.loc[temp_j ,"weight_nav"]=df_o32_longonly.loc[temp_j ,"weight_nav"] + df_o32_borrow.loc[temp_i,"weight_nav"]

### 用新的df替代就df
df_o32 = df_o32_longonly

#####################################################################
### 用df_index_csi中的权重和df_o32中权重比较，计算差额
'''1，按前一日收盘情况，计算次日个股调整计划、组合调整系数
2,要区分是按占股票市值比例，还是占基金净资产比例

'''
### 股票市值、基金净值、股票仓位比例
stock_mv = df_o32["mv"].sum()
fund_nav = df_o32["mv"].max()/df_o32["weight_nav"].max()
### 如果df_config.index 里有 "nav_latest",则更新基金净值数据 ||有可能有临时的申购赎回数据变动
if "nav_latest" in df_config.index :
    # str to float 
    fund_nav = float( df_config.loc["nav_latest", "value"] ) 

# fund_nav = 342002777.28
stock_per = stock_mv/fund_nav

print("stock_mv|万,fund_nav|万,stock_per",round(stock_mv/10000,2), round(fund_nav/10000,2),round(stock_per*100,2)," %")

### 锁定股票市值
stock_mv_locked = 0.0 

### 计算股票交易数量：昨日收盘价算、次日开盘价算
''' 观察df_index_csi中"调整后开盘参考价"和"收盘"是否相同，不相同的股票需要识别是否有股权变动
例：200622，伟明环保603568.SH,"10派3.1转增3",收盘价和次日开盘价分别为28.83和21.94，变动幅度23.9%。
如果次日开票按21,94买入，对应的是更多地总股本，前日股票数量N会变成N*1.3
一般分红金额在股价的 0.1%~6%，如果转增股份一般10转增1
'''

df_o32["num_all_new"] = df_o32["num_all"]
df_o32["num_frozen_new"] = df_o32["num_frozen"]

for temp_i in df_o32.index :
    temp_code = df_o32.loc[temp_i, "code_o32"]
    # type( temp_code)=string , type( df_index_csi["code_csi"].values[0] = int 
    # 代码第一个数字X表示全部冻结状态股票    
    if type(temp_code) ==str and temp_code[0] !="X" :
        # find code in df_index_csi
        df_index_csi_sub = df_index_csi[ df_index_csi["code_csi"]== int(temp_code) ]
        if len(df_index_csi_sub.index ) > 0 :
            # TODO # 计算权重差异| 理论上应该用 市值占净值比例weight_nav 而不是 "weight_stock"
            df_o32.loc[temp_i, "weight_diff"] =df_index_csi_sub["weight"].values[0] - df_o32.loc[temp_i, "weight_stock"]  
            ### 比较公司总股本是否变动：在 df_index_csi,df_index_csi_pre中获取前一交易日总股本
            df_o32.loc[temp_i, "total_share"] = df_index_csi_sub["total_share"].values[0]
            df_index_csi_pre_sub = df_index_csi_pre[ df_index_csi_pre["code_csi"]== int(temp_code) ]
            if len( df_index_csi_pre_sub.index ) > 0 :
                df_o32.loc[temp_i, "total_share_pre"] = df_index_csi_pre_sub["total_share"].values[0]
                df_o32.loc[temp_i, "total_share_para"] = df_o32.loc[temp_i, "total_share"]/df_o32.loc[temp_i, "total_share_pre"] 
            else :
                # 小概率事件：有可能是新纳入的股票，其他方式判断当日是否有股权变动
                print("小概率事件：有可能是新纳入的股票，其他方式判断当日是否有股权变动")
                print("Check if new stock in index consti ", temp_code)
                print( df_o32.loc[temp_i, : ]  )
            ### 赋值次日开盘价
            df_o32.loc[temp_i, "open_adj"] = df_index_csi_sub["open_adj"].values[0]
        else :
            # 存在持仓数量和市值都是0.0的情况，例如新股
            if df_o32.loc[temp_i, "num_all"] > 0.0 :
                ### 说明该成分股无持仓,都要卖出
                df_o32.loc[temp_i, "weight_diff"] = -1* df_o32.loc[temp_i, "weight_stock"]  
                # 假设明日全部卖出前一日持仓数量，不考虑股权变动
                df_o32.loc[temp_i, "total_share_para"] = 1.0
                ### 开盘价定为昨日收盘价：
                df_o32.loc[temp_i, "open_adj"] = df_o32.loc[temp_i, "mv"]/df_o32.loc[temp_i, "num_all"]
        
        # 考虑总股本变动超过1%的情况，低于1%认为无显著影响，例如股权激励或债转股等
        if abs( df_o32.loc[temp_i, "total_share_para"]-1 ) > 0.01 : 
            df_o32.loc[temp_i, "num_all_new"] = df_o32.loc[temp_i, "num_all"]*df_o32.loc[temp_i, "total_share_para"]
            ### 同时也要调整"冻结数量",但是可用数量一般还是不变，因为转增的股票次日通常无法交易。
            df_o32.loc[temp_i, "num_frozen_new"] = df_o32.loc[temp_i, "num_frozen"]*df_o32.loc[temp_i, "total_share_para"]
    elif temp_code[0] =="X" :
        ### 计算冻结股票的市值之和
        stock_mv_locked = stock_mv_locked + df_o32.loc[temp_i, "mv"]
        

### 根据权重的差额计算需要买卖的股票，按占股票市值比例，还是占基金净资产比例
# notes:！！！ weight_diff是百分位，需要除以100
df_o32[ "amt_diff"] = df_o32[ "weight_diff"]*0.01 * (fund_nav* para_stock_mv_nav_max - stock_mv_locked) 
# 剔除正负绝对值金额小于100 rmb
df_o32[ "amt_diff"]= df_o32[ "amt_diff"].apply(lambda x : x if abs(x) > 100 else 0.0 )

### 用次日开盘价计算需要交易的数量
df_o32[ "num_diff"] = df_o32[ "amt_diff"]/df_o32["open_adj"]
# 直接取消非整百尾数 ；342//100=3,342/100=3.42,342%100= 42

df_o32[ "num_diff100"] = df_o32[ "num_diff"].apply(lambda x : (x//100)*100 if abs(x//100)>=1 else 0   )


### 匹配卖出股票的可用部分
# 如果次日有分红送配，需要对数量做调整。
# 例：伟明环保在csi200622文件中总股本(股)是1256558346,前一交易日200619中总股本(股)是966583343。
for temp_i in df_o32.index :
    temp_code = df_o32.loc[temp_i, "code_o32"]
    # 代码第一个数字X表示全部冻结状态股票    
    if type(temp_code) ==str and temp_code[0] !="X" :
        if df_o32.loc[temp_i, "num_diff100"] < 0 :
        ### 判断是否有足够的卖出数量：部分冻结的情况一般是当日有买入的，这些数量次日可以解冻 。
            temp_num_avail = df_o32.loc[temp_i, "num_available"] + df_o32.loc[temp_i, "num_buy"]
            if temp_num_avail + df_o32.loc[temp_i, "num_diff100"] < 100 :
                ### 持仓不够卖的，或尾数小于100股
                df_o32.loc[temp_i, "num_diff100"] = temp_num_avail*-1

       
### Save
df_o32.to_csv(path_o32_output+ "output_df_o32_adj_"+ temp_date +".csv",encoding="gbk")

#####################################################################
### 统计汇总：持仓股票属于成分股的数量、合计权重，
'''指标 || 还需要的数据：基金净资产——多导出1列占净资产比例
1，组合内指数成分股总市值，成分股总市值占基金净资产比例，成分股股票数量，股票数量占指数成分股比例
2，组合全部股票数量，非成分股数量。
'''

#####################################################################
### 单笔交易即可满足需要，生成交易指令
def code_stock_to_market(stock_code) :
    #若深圳股票返回2，上海股票返回1;例子：1,232,600587
    code_market = 2
    code_str = str(int(stock_code))
    if len( code_str ) ==6 :
        if code_str[0] == "6" :
            code_market = 1

    return code_market
def gen_trade_order(obj_order):
    ### 
    N = obj_order["N"] 
    ### Sell orders 
    if "if_sell_orders" in obj_order.keys():
        if obj_order["if_sell_orders"] == 0 :
            # just 1 order 
            df_order = obj_order["df_o32_sell"].loc[:,["code_o32" ,"num_diff100"] ]
            df_order.columns=["证券代码","指令数量"]
            df_order["指令数量"] = df_order["指令数量"].apply(lambda x : abs(x) )
            # 	委托方向,指令价格	价格模式	市场代码
            df_order["委托方向"] =2 
            df_order["指令价格"] = 0
            df_order["价格模式"] =4 
            df_order["市场代码"] = df_order["证券代码"].apply(lambda code : code_stock_to_market(code)  ) 
            ### save sell orders 
            file_name = "output_sell_order_"+ temp_date + "_" + fund_name_o32+ ".xls"
            df_order.to_excel( path_o32_output+ file_name,index=False,encoding="gbk" )

        elif obj_order["if_sell_orders"] == 1  :
            df_o32_sell = obj_order["df_o32_sell"]
            ### several orders
            for i in range(N) :
                ### 剔除股票数量为0的
                df_o32_sell_sub = df_o32_sell[ df_o32_sell["num_diff100_"+str(i)]>=100 ]
                
                df_order = obj_order["df_o32_sell"].loc[:,["code_o32" ,"num_diff100_"+str(i) ] ]
                df_order.columns=["证券代码","指令数量"]
                df_order["指令数量"] = df_order["指令数量"].apply(lambda x : abs(x) )
                # 	委托方向,指令价格	价格模式	市场代码
                df_order["委托方向"] =2 
                df_order["指令价格"] = 0
                df_order["价格模式"] =4 
                df_order["市场代码"] = df_order["证券代码"].apply(lambda code : code_stock_to_market(code)  ) 
                ### save sell orders 
                file_name = "output_sell_order_"+ temp_date +"_"+ str(i)+ "_" + fund_name_o32+ ".xls"
                df_order.to_excel( path_o32_output+ file_name,index=False,encoding="gbk" )
    ### Buy orders 
    if "if_buy_orders" in obj_order.keys():
        if obj_order["if_buy_orders"] == 0 :
            # just 1 order 
            df_order = obj_order["df_o32_buy"].loc[:,["code_o32" ,"num_diff100"] ]
            df_order.columns=["证券代码","指令数量"]
            df_order["指令数量"] = df_order["指令数量"].apply(lambda x : abs(x) )
            # 	委托方向,指令价格	价格模式	市场代码
            df_order["委托方向"] =1
            df_order["指令价格"] = 0
            df_order["价格模式"] =4 
            df_order["市场代码"] = df_order["证券代码"].apply(lambda code : code_stock_to_market(code)  ) 
            ### save buy orders 
            file_name = "output_buy_order_"+ temp_date + "_" + fund_name_o32+ ".xls"
            df_order.to_excel( path_o32_output+ file_name,index=False,encoding="gbk" )

        elif obj_order["if_buy_orders"] == 1  :
            df_o32_buy = obj_order["df_o32_buy"]
            ### several orders
            for i in range(N) :
                ### 剔除股票数量为0的
                df_o32_buy_sub = df_o32_buy[ df_o32_buy["num_diff100_"+str(i)]>=100 ]
                
                df_order = obj_order["df_o32_buy"].loc[:,["code_o32" ,"num_diff100_"+str(i) ] ]
                df_order.columns=["证券代码","指令数量"]
                df_order["指令数量"] = df_order["指令数量"].apply(lambda x : abs(x) )
                # 	委托方向,指令价格	价格模式	市场代码
                df_order["委托方向"] = 1
                df_order["指令价格"] = 0
                df_order["价格模式"] =4 
                df_order["市场代码"] = df_order["证券代码"].apply(lambda code : code_stock_to_market(code)  ) 
                ### save buy orders 
                file_name = "output_buy_order_"+ temp_date +"_"+ str(i)+ "_" + fund_name_o32+ ".xls"
                df_order.to_excel( path_o32_output+ file_name,index=False,encoding="gbk" )

    return obj_order 

#####################################################################
### 交易计划：
'''
1,限制条件：股票持仓比例下限90%，非冻结部分持仓比例，冻结部分持仓比例
2，成分股持仓比例下限 80%，实际成分股持仓比例
3，单次卖出比例上限，4.5%，单次卖出金额上限=比例*总市值
4，买卖批次：买入和卖出各分几次交易；设定为分开交易。

notes:新股上市部分不考虑，jjjl主动判断
'''

### 计算卖出指令：单次不超过股票总市值 4.5%,相对于5%留出了10%股价变动的余地
df_o32_sell = df_o32[ df_o32["num_diff100"] <0 ]
amt_sell = df_o32_sell[ "amt_diff"].sum()*-1 
if_sell_orders = 0 
if amt_sell > stock_mv*0.045 :
    if_sell_orders = 1 
    # 向上取整，分几批卖出，组合较小时可能会出现拆分后单只股票无法取到100股，这种情况按个股拆分到不同指令中
    N = math.ceil( amt_sell / (stock_mv*0.045) )

    ### Notes:这部分计算效率较低
    for temp_i in df_o32_sell.index :
        # notes：df_o32_sell.loc[temp_i, "num_diff100" ] 是负数
        num_begin = abs( df_o32_sell.loc[temp_i, "num_diff100" ] )
        num_rest = num_begin
        ### 计算单次交易数量num_1order
        if num_begin //(N*100) >1 :
            num_1order = 100*(num_rest//(N*100))
        else :
            # 例如贵州茅台单价1000多元的情况，通常只交易100股。
            num_1order = 100
        print("Working on ", df_o32_sell.loc[temp_i, "code_o32"] )
        for i in range(N) :
            # 0,1，N-1;剩余交易数量 num_rest            
            if num_rest >0   :
                df_o32_sell.loc[temp_i, "num_diff100_"+str(i) ] = num_1order
                num_rest = num_rest - num_1order
            else :
                df_o32_sell.loc[temp_i, "num_diff100_"+str(i) ] = 0 

obj_order ={}
obj_order["N"] = N
obj_order["if_sell_orders"] = if_sell_orders
obj_order["df_o32_sell"] = df_o32_sell

obj_order = gen_trade_order(obj_order)

### 计算买入指令：单次不超过股票总市值 4.5%
df_o32_buy = df_o32[ df_o32["num_diff100"] >0 ]
amt_buy = df_o32_buy[ "amt_diff"].sum()
if_buy_orders = 0 
if amt_buy > stock_mv*0.045 :
    if_buy_orders = 1 
    # 向上取整，分几批卖出，组合较小时可能会出现拆分后单只股票无法取到100股，这种情况按个股拆分到不同指令中
    N = math.ceil( amt_buy / (stock_mv*0.045) )
    
    for temp_i in df_o32_buy.index :
        num_rest = df_o32_buy.loc[temp_i, "num_diff100" ]
        ### 计算单次交易数量num_1order
        if df_o32_buy.loc[temp_i, "num_diff100" ]//(N*100)>1 :
            num_1order = 100*num_rest//(N*100)
        else :
            num_1order = 100
            
        for i in range(N) :
            # 0,1，N-1;剩余交易数量 num_rest            
            if num_rest >0   :
                df_o32_buy.loc[temp_i, "num_diff100_"+str(i) ] = num_1order
                num_rest = num_rest - num_1order
            else :
                df_o32_buy.loc[temp_i, "num_diff100_"+str(i) ] = 0 

obj_order ={}
obj_order["N"] = N
obj_order["if_buy_orders"] = if_buy_orders
obj_order["df_o32_buy"] = df_o32_buy
obj_order = gen_trade_order(obj_order)


### Save
print(df_o32.head().T )
df_o32.to_csv(path_o32_output+ "output_df_o32_adj_"+ temp_date +".csv",encoding="gbk")
df_o32_sell.to_csv(path_o32_output+ "output_df_o32_sell_"+ temp_date +".csv",encoding="gbk")
df_o32_buy.to_csv(path_o32_output+ "output_df_o32_buy_"+ temp_date +".csv",encoding="gbk")



#####################################################################
### 日内在交易机上输入最新净值(申购赎回等因素)，计算组合调整计划 || 交易机里没有python，只能事先算好数据
# Idea:提前计算好现有组合的比例，若基金有资金流入流出，则按照最新价计算的比例调整股票交易数量。
'''1,计算预调仓之后的参数
2，按日内最新价，计算日个股调整计划、组合调整系数
3，按照最近基金资产（净流入流出资金），计算个股调整计划。
notes:1,要注意日内因为股价变动导致的股票市值权重变动
'''



#####################################################################
### 指数增强：提高看好股票权重，降低看空股票权重或数量，例如偏离10~20%组合权重并计算对应的组合指令。






























#####################################################################
### 
