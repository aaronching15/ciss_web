# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
todo：
0，22-2,把这个脚本的所有功能逐步转移到网页，file=views_quick.py ;
url=http://127.0.0.1:8000/ciss_exhi/quick.html
2，根据细分策略的配置去更新不同组合；每个组合由1~N个策略构成。
4，1-5，确定要调整的指数基金时，所有包含指数策略的组合可以同步调仓。

功能：
1.管理Wind-PMS中组合
2，结合abcd3d个股数据和基金研究数据，更新股票和基金的指标
1，管理PMS组合列表；划分不同资产
2，单组合建仓；
3，单组合调仓，按目标配置比例更新组合
4，定期跟踪组合净值

步骤：
1，确定各个组合权益配置比例；sheet=组合仓位记录
2，根据配置计划、股票、基金策略，确定需要调整的组合；sheet=log 


last 220105| since 200704
步骤：
1，导入组合管理列表：识别需要定期更新的组合；
2，组合监控：导入组合持仓信息，识别持仓有大幅波动的证券，

notes：
'''
#################################################################################
### Initialization 
import sys,os
from unicodedata import name
path0 = "C:\\rc_HUARONG\\rc_HUARONG\\"
sys.path.append( path0 ) 
sys.path.append( path0+ "ciss_web\\CISS_rc\\db\\db_assets\\" ) 
sys.path.append( path0+ "ciss_web\\CISS_rc\\db\\analysis_indicators\\" ) 

import pandas as pd 
import numpy as np 
###
from get_wind_api import wind_api
class_wind_api = wind_api()
### 
from analysis_data_statistics import data_stat
class_data_stat = data_stat()
### date 
import datetime as dt  
time_now = dt.datetime.now()
time_now_str = dt.datetime.strftime(time_now   , "%Y%m%d")
time_pre_str = dt.datetime.strftime(time_now - dt.timedelta(days=1) , "%Y%m%d")
time_pre10_str = dt.datetime.strftime(time_now - dt.timedelta(days=10) , "%Y%m%d")

print( "当前时间：10天前",time_now_str , time_pre10_str )


##########################################################################################
### Part 1 管理PMS组合列表；划分不同资产
file= "pms_manage.xlsx"
path0 = "C:\\rc_HUARONG\\rc_HUARONG\\"
path = "C:\\rc_HUARONG\\rc_HUARONG\\data_pms\\"

path_wpf = path + "wpf\\" 
path_data = path + "wind_terminal\\" 
path_data_fund = path_data + "fund\\" 
path_fund = path + "fund\\" 
path_adj = path +  "data_adj\\"
#########################################################################
### INPUT
print("PMS：1，下载PMS持仓; 2，导入最新PMS持仓数据;8，多策略组合：资产配置和多策略;6，组合调整，自动上传到PMS组合")
print("PMS：9，PMS组合部分仓位调整; ")
print("Data and cal:3，下载AH股的价量数据；4，计算AH股的动量趋势和行业统计：")
print("股票策略：5,量化股票策略；")
print("基金策略：7,FOF持仓选基； ")   
data_type= input("输入数据类型：") 

################################################################################################
### S1 给定组合列表，下载最新PMS持仓数据
### 导入组合列表 | 不能用组合ID，无法识别。

if data_type in [1,"1" ]:
    obj_data = class_data_stat.get_list_pms_port( )
    df_pms_manage = obj_data["df_pms_manage"] 
    df_data_all = obj_data["df_data_all"] 
    asd

################################################################################################
### S1 给定组合列表，导入最新PMS持仓数据
# columns是 AssetClass   Windcode AssetName BeginHoldingValue  ...    TotalCost BeginPosition     Position EUnrealizedPL

if data_type in [2,"2" ]:
    for temp_i in df_pms_manage.index :
        ### 判断是否是有效组合
        print( df_pms_manage.loc[temp_i, "port_name"  ] , df_pms_manage.loc[temp_i, "if_active" ] )
        
        if df_pms_manage.loc[temp_i, "if_active" ] in [1,"1"] :
            pms_name = df_pms_manage.loc[temp_i, "port_name"  ]
            ### 
            file_name = "wpf_" + pms_name +  ".xlsx"
            df_data= pd.read_excel(path_wpf + file_name  )
            ### 去除无市值的持仓
            df_data= df_data[ df_data["NetHoldingValue"] > 0.0 ]
            print( df_data )
            asd 
    asd

################################################################################################
### PMS：9，PMS组合部分仓位调整
# 对单个或所有组合持有的某个股票进行调整
if data_type in [9,"9" ]:
    ##########################################
    ### Part 1，导入策略文件
    input_date = input("输入 input date for data file such as 220107 for file:")
    port_name = input("输入 input 所有组合all-ports=1=0,or port_name组合名称，such as 量化成长选股:")
    ### 清仓或替换 ：220208，医药类012238换成 大银行基金 011972.OF
    code_sell = input("输入 卖出证券code_sell，such as 012238.OF:")
    code_buy = input("输入 卖出证券code_sell，such as 011972.OF:")
    ### notes：买入价只能选最新价
    ##########################################
    ### 默认有卖有买
    trade_type = "sell_buy" 
    if len(input_date) == 6 :
        # 220107 to 20220107
        date_latest = "20" +input_date
    if len(code_buy) < 1 :
        ### 只卖不买
        trade_type = "sell_only"
    else :
        ### 有买入
        if len(code_sell) < 1 :
            trade_type = "buy_only"
            weight_buy = input("输入 买入权重，such as 0.003 or 卖出金额sell amount:")
        else :
            trade_type = "sell_buy" 

    ##########################################
    ### 上传组合到PMS
    obj_port = {}    
    obj_port["trade_type"] = trade_type 
    obj_port["code_sell"] = code_sell 
    obj_port["code_buy"] = code_buy 
    obj_port["date_latest"] = date_latest
    obj_port["port_name"] = port_name  
    if len(code_sell ) < 1 :
        ### 只买不卖
        trade_type = "buy_only"        
        obj_port["weight_buy"] = float(weight_buy)
    else :
        obj_port["weight_buy"] = ""
    from ports import manage_pms
    class_manage_pms = manage_pms() 
    ### 只卖不买；不需要获取买入证券的最新净值 
    obj_port = class_manage_pms.pms_upload_trade( obj_port )
    

        
    



    asd

################################################################################################
### S2 结合投资策略(基于abcd3d个股数据和基金研究等)
### 更新股票和基金的指标
# 用Wind导出的A股和港股数据，用Wind-API提取价量指标
if data_type in [3,"3" ]:

    ################################################
    ### 1,提取Wind data
    input_date = input("输入 input date for data file such as 220107 for file:")
    if len(input_date) == 6 :
        # 220107 to 20220107
        date_latest = "20" +input_date
    df_shares = class_wind_api.get_wss_ma_amt_mv( date_latest )
    asd
    ### Input:日期；output：保存的数据文件


################################################################################################
### 2，计算AH股策略指标:中短期趋势abcd3d
### INPUT：date_latest,path_adj,path .OUTPUT:
if data_type in [4,"4" ]:
    input_date = input("输入 input date for data file such as 220107 for file:")
    if len(input_date) == 6 :
        # 220107 to 20220107
        date_latest = "20" +input_date
    obj_data = class_data_stat.cal_AH_momentum_abcd3d(date_latest)
    # obj_data["df_shares_ah"] 
    # obj_data["df_shares_"+ temp_key] = df_shares
    # obj_data["df_ind1_"+ temp_key] = df_ind1
    # obj_data["df_ind3_"+ temp_key ] = df_ind3
    asd


################################################################################################
###################################################
### 量化股票策略：躺赢股基策略，做一个量化股票的回测：动量、成长、价值
### todo: 统计变化情况：主题板块	成长或价值	是否有投资机会	中期趋势变动	成交金额变动
### 1，行业配置:一级行业已经不够用了，甚至会出现
# 筛选条件：1，总市值：行业内前50%，越高越好; 成交金额市场前20%
# 基金持股比例；大于1%，且总金额（乘以流通市值）大于5亿元；越高越好？
# 市盈率(TTM)；无
# 净资产收益率(TTM) ；大于 10%
# 归母净利润同比增长率：三级行业内前50%，或前5名
# 动量：中期趋势大于0 ;近120天涨跌幅
# 主动股票池：参考权重

if data_type in [5,"5" ]:
    ###################################################
    ### Part 1 导入股票数据，初步筛选股票池;保存股票池文件。
    input_date = input("输入 input date for data file such as 220107 for file:")
    if len(input_date) == 6 :
        # 220107 to 20220107
        date_latest = "20" +input_date
    ##########################################
    ### 判断量化指标选股还是主观股票池
    stockpool_type = input("输入 input 量化指标选股|1| indi,还是主观股票池|2|ac|active:")
    
    if stockpool_type in [1,"1","indi"  ] :
        from stockpools import stockpool
        class_stockpool = stockpool()
        obj_shares = {}
        obj_shares["date_latest"] = date_latest
        obj_shares = class_stockpool.cal_stockpool_indi( obj_shares )
        
        ##########################################
        ### Part 2 生成策略配置比例，不超过50只股票
        # col_list = ["m_ave_amt","m_ave_mv","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","trend_mid","trend_short"   ]
        # obj_shares["col_list"] = col_list
        from func_stra import stra_allocation
        class_stra_allocation = stra_allocation()             
        obj_port = class_stra_allocation.stock_weights_by_indi( obj_shares ) 

    elif stockpool_type in [2,"2","ac","active"  ] :
        ### 给定股票池，计算主观股票
        obj_shares = {}
        obj_shares["date_latest"] = date_latest
        from func_stra import stra_allocation
        class_stra_allocation = stra_allocation()             
        obj_port = class_stra_allocation.stock_weights_by_active( obj_shares ) 
    

    
    asd 

############################################################################
### Part 3 组合调整，自动上传到PMS组合
if data_type in [6,"6" ]:
    ##########################################
    ### Part 3 组合调整，自动上传到PMS组合
    ### 1，导入策略文件
    input_date = input("输入 input date for data file such as 220107 for file:")
    port_name = input("输入 input port_name组合名称，such as 量化成长选股:")
    if len(input_date) == 6 :
        # 220107 to 20220107
        date_latest = "20" +input_date
    
    ################################################################################################
    ### INPUT data：判断组合类型：量化指标indi还是主观选股active
    ### notes:如果是不同的策略，应该是导入不同的策略文件
    if port_name == "量化成长选股" :
        asset_type = "stock"
        ### 初期低仓位运行
        weight_equity = 0.2
        ### 量化指标indi
        file_name = "stra_port_indi_" + date_latest +".xlsx" 
        df_port =pd.read_excel( path_adj + file_name ) 

    elif port_name == "行业成长价值精选" :
        asset_type = "stock"
        ### 初期低仓位运行
        weight_equity = 0.2
        ### 主观选股active
        file_name = "stra_port_active_" + date_latest +".xlsx" 
        df_port =pd.read_excel( path_adj + file_name ) 

    elif port_name == "躺赢FOF股基" :
        asset_type = "fund"
        ### 初期低仓位运行
        weight_equity = 0.95
        ### stra_port_fof_stock_20220128.xlsx
        file_name = "stra_port_fof_stock_" + date_latest +".xlsx" 
        df_port =pd.read_excel( path_adj + file_name ) 
    elif port_name == "躺赢FOF债基" :
        asset_type = "fund"
        ### 初期低仓位运行 
        ### stra_port_fof_stock_20220128.xlsx
        file_name = "stra_port_fof_bond_" + date_latest +".xlsx" 
        df_port =pd.read_excel( path_adj + file_name ) 
    elif port_name == "固收加FOF20" :
        asset_type = "multi" 
        ### stra_port_fof_stock_20220128.xlsx
        file_name = "固收加FOF20_" + date_latest +".xlsx" 
        df_port =pd.read_excel( path_adj + file_name ) 
    else :
        asd
    
    ##########################################
    ### 上传组合到PMS
    obj_port = {}    
    obj_port["asset_type"] = asset_type
    obj_port["df_port"] =df_port
    obj_port["date_latest"] = date_latest
    obj_port["port_name"] = port_name 
    if asset_type == "stock" :
        obj_port["weight_equity"] = weight_equity
    
    from ports import manage_pms
    class_manage_pms = manage_pms() 
    # "量化成长选股"
    obj_port["port_name"] = port_name 
    obj_port = class_manage_pms.pms_upload( obj_port )

    asd 


####################################################################################
### Part 4 基金策略：FOF持仓选基
if data_type in [7, "7", "FOF持仓选基" ]:
    input_date = input("输入 input date for data file such as 220128 for file:") 
    if len(input_date) == 6 :
        # 220107 to 20220107
        date_latest = "20" +input_date
    ####################################################################################
    ### Step：1，FOF业绩排名前15或前10%的FOF基金
    ### 从Wind终端-基金索引导出股票+混合FOF、债券+混合FOF两类文件
    # file=公募基金-股票FOF-20220128.xlsx;公募基金-债券FOF-20220128.xlsx
    # py-file ="test_fof_fund_pool.py"
    ##############################################################
    ### 股票FOF
    list_fund_type_stock = [ "灵活配置型基金","偏股混合型基金","偏债混合型基金","平衡混合型基金","普通股票型基金","国际(QDII)股票型基金","被动指数型基金","股票多空","增强指数型基金"]
    # file_name = "基金池_股票FOF_220128.xlsx"
    file_name = "基金池_FOF_" + date_latest +".xlsx"
    
    df_fof = pd.read_excel( path_fund + file_name,sheet_name="raw_data" )
    df_fof_top = df_fof.iloc[:20, :]
    # FOF基金持仓：   "基金代码"   
    ##########################################
    ### Step：2，获取不同类别基金持仓：股票、债券、混合 
    # file = "重仓基金(明细)-海通分类FOF-20211231.xlsx"; 用 "投资类型" 划分
    file_name2 = "重仓基金(明细)-海通分类FOF-20211231.xlsx"
    ##########################################
    ### 只保留股票类基金
    df_holding_fof = pd.read_excel( path_data_fund + file_name2 )
    df_holding_fof = df_holding_fof[ df_holding_fof["投资类型"].isin( list_fund_type_stock )  ]
    ###
    count_code = 0 
    for temp_code in  df_fof_top["基金代码"] : 
        ### find holding funds of temp_code 
        df_sub = df_holding_fof[ df_holding_fof["代码"]==temp_code ]
        if len( df_sub.index ) >0 :
            if count_code == 0  :
                df_holding_funds = df_sub
                count_code = 1
            else :
                df_holding_funds = df_holding_funds.append(df_sub, ignore_index= True)
    df_holding_funds.to_excel( "D:\\df_holding_fund_s.xlsx" )
    ##########################################
    ### 去除重复项
    df_holding_funds["change_pct"] = df_holding_funds["季度持仓变动(万份)"] / df_holding_funds["基金总规模(亿元)"]  
    # "持仓份额(万份)","季度持仓变动(万份)","持仓市值(万元)","占基金净值比(%)","占基金市值比(%)","基金总规模(亿元)","占基金总规模比(%)"
    df_funds = df_holding_funds.groupby( "基金代码" )["持仓份额(万份)","change_pct","持仓市值(万元)","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"].sum()
    # "change_pct","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"
    for temp_col in ["change_pct","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"] :
        df_funds["s_"+ temp_col ] = df_funds[temp_col] / df_funds[temp_col].sum()
    ### 分配权重
    df_funds["s_sum" ] = df_funds["s_"+ "change_pct"] *0.7
    for temp_col in ["占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"] :
        df_funds["s_sum" ] = df_funds["s_sum" ] + df_funds["s_"+ temp_col ] *0.1
    
    ### 取前20名基金
    df_funds = df_funds.sort_values(by="s_sum",ascending=False  )  
    df_funds = df_funds.iloc[:20,: ]

    ### 剔除负值
    df_funds = df_funds[ df_funds["s_sum"] > 0  ] 
    
    df_funds["code"] = df_funds.index
    ##########################################
    ### 确保单券权重位于 0.3~10%之间;save to str file 
    obj_fund = {} 
    obj_fund["date_latest"] =date_latest
    obj_fund["df_funds"] =df_funds
    obj_fund["file_name"] = "stra_port_fof_stock_" + date_latest + ".xlsx"
    from func_stra import stra_allocation
    class_stra_allocation = stra_allocation()     
    obj_fund= class_stra_allocation.fund_weights_by_score(obj_fund)

    ####################################################################################
    ### 债券FOF
    list_fund_type_bond = [ "被动指数型债券基金","短期纯债型基金","国际(QDII)债券型基金","混合债券型二级基金","混合债券型一级基金","中长期纯债型基金"]
    file_name = "基金池_债券FOF_220128.xlsx"
    df_fof = pd.read_excel( path_adj + file_name,sheet_name="raw_data" )
    df_fof_top = df_fof.iloc[:20, :]
    # FOF基金持仓：   "基金代码"   
    ##########################################
    ### Step：2，获取不同类别基金持仓：股票、债券、混合 
    # file = "重仓基金(明细)-海通分类FOF-20211231.xlsx"; 用 "投资类型" 划分
    file_name2 = "重仓基金(明细)-海通分类FOF-20211231.xlsx"
    ##########################################
    ### 只保留债券类基金
    df_holding_fof = pd.read_excel( path_data_fund + file_name2  )
    df_holding_fof = df_holding_fof[ df_holding_fof["投资类型"].isin( list_fund_type_bond )  ]
    ###
    count_code = 0 
    for temp_code in  df_fof_top["基金代码"] : 
        ### find holding funds of temp_code 
        df_sub = df_holding_fof[ df_holding_fof["代码"]==temp_code ]
        if len( df_sub.index ) >0 :
            if count_code == 0  :
                df_holding_funds = df_sub
                count_code = 1
            else :
                df_holding_funds = df_holding_funds.append(df_sub, ignore_index= True)
    df_holding_funds.to_excel( "D:\\df_holding_fund_b.xlsx" )
    ##########################################
    ### 去除重复项
    df_holding_funds["change_pct"] = df_holding_funds["季度持仓变动(万份)"] / df_holding_funds["基金总规模(亿元)"]  
    # "持仓份额(万份)","季度持仓变动(万份)","持仓市值(万元)","占基金净值比(%)","占基金市值比(%)","基金总规模(亿元)","占基金总规模比(%)"
    df_funds = df_holding_funds.groupby( "基金代码" )["持仓份额(万份)","change_pct","持仓市值(万元)","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"].sum()
    # "change_pct","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"
    for temp_col in ["change_pct","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"] :
        df_funds["s_"+ temp_col ] = df_funds[temp_col] / df_funds[temp_col].sum()
    ### 分配权重
    df_funds["s_sum" ] = df_funds["s_"+ "change_pct"] *0.7
    for temp_col in ["占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"] :
        df_funds["s_sum" ] = df_funds["s_sum" ] + df_funds["s_"+ temp_col ] *0.1
    
    ### 取前20名基金
    df_funds = df_funds.sort_values(by="s_sum",ascending=False  )  
    df_funds = df_funds.iloc[:20,: ]

    ### 剔除负值
    df_funds = df_funds[ df_funds["s_sum"] > 0  ]
    # df_funds["weight"] = df_funds["s_sum"] /df_funds["s_sum"].sum()
    df_funds["code"] = df_funds.index
    ##########################################
    ### 确保单券权重位于 0.3~10%之间
    obj_fund = {} 
    obj_fund["date_latest"] =date_latest
    obj_fund["df_funds"] =df_funds
    obj_fund["file_name"] = "stra_port_fof_bond_" + date_latest + ".xlsx"
    from func_stra import stra_allocation
    class_stra_allocation = stra_allocation()     
    obj_fund= class_stra_allocation.fund_weights_by_score(obj_fund)
    
    asd

############################################################################
### TODO Part 5 策略组合：大类资产配置
if data_type in [8, "8", "多策略组合" ]:

    input_date = input("输入 input date for data file such as 220107 for file:")
    port_name = input("输入 port_name组合名称，such as 固收加FOF20、指数股债配置、FOF行业景气配置:")
    if len(input_date) == 6 :
        # 220107 to 20220107
        date_latest = "20" +input_date
    
    ################################################################################################
    ### Step：1，判断组合类型  
    if port_name == "固收加FOF20" :
        ####################################################################################
        ### 导入对应的策略配置比例
        ##########################################
        ### 配置比例-股票基金：躺赢股基 10%； 
        file_name = "基金池rc_主动股票_220120.xlsx"
        path_sp = "C:\\rc_HUARONG\\rc_HUARONG\\0基金池和模拟组合\\"
        df_top_perf_stock = pd.read_excel( path_sp + file_name,sheet_name="raw_data" )
        df_top_perf_stock = df_top_perf_stock.iloc[:20, : ]
        df_top_perf_stock["weight"] = df_top_perf_stock["score"]/df_top_perf_stock["score"].sum()
        df_top_perf_stock = df_top_perf_stock.loc[:,["基金代码", "weight"] ]
        df_top_perf_stock.rename(columns={'基金代码':'code'}, inplace = True)
        
        ##########################################
        ### 配置比例-股票基金：躺赢FOF股，6%
        file_name = "stra_port_fof_stock_"+ date_latest +".xlsx"
        df_fof_stock = pd.read_excel( path_adj + file_name  ) 
        df_fof_stock = df_fof_stock.loc[:, ["code","weight" ]]        

        ##########################################
        ### 配置比例-债券基金：躺赢债基 50%
        file_name = "基金池rc_债券_20220121.xlsx"
        path_sp = "C:\\rc_HUARONG\\rc_HUARONG\\0基金池和模拟组合\\"
        df_top_perf_bond = pd.read_excel( path_sp + file_name,sheet_name="raw_data" )
        df_top_perf_bond = df_top_perf_bond.iloc[:20, : ]
        df_top_perf_bond["weight"] = df_top_perf_bond["score"]/df_top_perf_bond["score"].sum()
        df_top_perf_bond = df_top_perf_bond.loc[:,["基金代码", "weight"] ]
        df_top_perf_bond.rename(columns={'基金代码':'code'}, inplace = True)

        ##########################################
        ### 债券基金：躺赢FOF债基 30%
        file_name = "stra_port_fof_bond_"+ date_latest +".xlsx"
        df_fof_bond = pd.read_excel( path_adj + file_name  ) 
        df_fof_bond = df_fof_bond.loc[:, ["code","weight"] ]

        ##########################################
        ### 配置比例-股票：行业研究 2% 
        file_name = "stra_port_active_20220121.xlsx"
        df_stock_active = pd.read_excel( path_adj + file_name  ) 
        df_stock_active = df_stock_active.loc[:, ["代码","weight"] ]
        df_stock_active.rename(columns={'代码':'code'}, inplace = True)
        
        ##########################################
        ### 配置比例-股票： 量化2%    
        file_name = "stra_port_indi_20220121.xlsx"
        df_stock_indi = pd.read_excel( path_adj + file_name  ) 
        df_stock_indi = df_stock_indi.loc[:, ["代码","weight"] ]
        df_stock_indi.rename(columns={'代码':'code'}, inplace = True)

        ####################################################################################
        ### 分配权重
        ### 配置比例-股票基金：躺赢股基 10%；躺赢FOF股，6%
        df_top_perf_stock["weight"] = df_top_perf_stock["weight"] * 0.1 
        df_port = df_top_perf_stock    

        df_fof_stock["weight"] =df_fof_stock["weight"] *0.06        
        df_port = df_port.append( df_fof_stock,ignore_index=True ) 
        ### 配置比例-债券基金：躺赢债基 50%	躺赢FOF债基 30%
        df_top_perf_bond["weight"] = df_top_perf_bond["weight"] * 0.5
        df_port = df_port.append( df_top_perf_bond ,ignore_index=True )
        df_fof_bond["weight"] =df_fof_bond["weight"] *0.3
        df_port = df_port.append( df_fof_bond  ,ignore_index=True )
        ### 配置比例-股票：行业研究 2%；量化2%
        df_stock_active["weight"] =df_stock_active["weight"] *0.02
        df_port = df_port.append( df_stock_active ,ignore_index=True )
        df_stock_indi["weight"] =df_stock_indi["weight"] *0.02
        df_port = df_port.append( df_stock_indi ,ignore_index=True )
        
        ##########################################
        ### 去除重复项 :168 to 161
        df_sum = df_port.groupby("code")["weight"].sum()
        # 输入 df_sum is Series
        df_port = pd.DataFrame( df_sum )
        print( df_port.columns )
        df_port["code"] = df_port.index
        print( df_port.columns )

        ##########################################
        ### Save to excel 
        file_name = port_name + "_"+ date_latest +".xlsx"
        df_port.to_excel(path_adj + file_name  ,index=False ) 






###################################################
### TODO：21Q4基金持仓分析------
# 1，HS下载三季度、四季度持仓；2，筛选四季度增持个股趋势，以及12月、1月份相对强势股票匹配的基金。
 
################################################################################################
### 4， 股票池管理：核心池和基础池的行业分布、市值分布、短中长期超额收益情况；和不同市场指数比较。

# 诉求：从特定文件读取指标数据
#Qs:如何调整权重？参考：主动、动量、财务。|如果主动要偏离，需要有理由
# 监控功能：近20日涨跌幅	60日涨跌幅	120日涨跌幅 |多少算异常？ --和沪深300、创业板等主要指数的比较
# todo：对主要指数的每日监控。
# 入选股票池后，设置12个月目标市值上限和下限


#############################################
### Part1:价量：中期趋势	短期趋势；20日涨跌幅，60日涨跌幅，120日涨跌幅 

#############################################
### TODO,组合--策略对应: excel文件里导入每个组合的策略配置
# 例如：组合A:stra1,20%,stra2,60%,cash20% .....



asd  















################################################
### todo S 监控模板：
### s1,汇总所有组合持仓：股票、基金、债券；按需提取前收盘价或最新收盘价：w.wsq("0700.HK", "rt_latest") = 473.4
### s2,根据每个组合的权重，分别计算持仓浮动盈亏和本周盈亏。
### s3，识别大幅波动的标的，进行提示；例如近20日累计涨幅或者跌幅超过 10% 





################################################
### S 


##########################################################################################
### Part 2 PMS组合日监控






##########################################################################################
### Part 3 PMS组合批量调整


































