# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo:
topic：基金股票组合仿真和预测
功能：用市场基础组合和基金重仓股组合对基金净值进行拟合
###
last 20211129 | since 211124 
derived from ..\\apps\\fund_ana\\test_fund_ana_stock.py
################################################
################################################
功能：基金股票持仓的数据分析指标和算法
    0，过去2个季度的股票持仓分析，不同维度：
    1，个股行业维度：不同一级或3级行业占比比例；Qs:如何进行比较？

    2，动量指标：abcd3d：-6 ~6；indi_short，短期择时指标；indi_mid，中期择时指标

    3，个股权重：2个挡位，占比是否超过5%和9%；

    4，个股权重变动:和前2个季度、前1年、前2年比较：
        例如：STOCK_PER_pre	9.02	股票市值占总股票市值-前一季度；
    
    5，股票对应的基金的排名分档：长期、中期、短期

    6,个股市值、财务指标变动，后缀"_change":主要是前后2个披露季度之间的变动。
        对应风格特征。
    
    7，加权算法,预测指标,仿真组合建模和跟踪；尝试机器学习算法；
        7.1，组合构建：基于相关性、收益率等指标构建模拟组合，跟踪未来收益。
        7.2，组合评估：从超额收益、回撤、稳定性、不同市场环境时期的匹配度等维度评估仿真的效果
    8，绩效评估skill_stock 
    
################################################
todo：0，股票基金数据分析，从单个日期维度和单一基金等维度。
    Qs:现有的基金分析数据结果太死板，难以满足灵活的分析需要；理论上我们需要的是给定单个基金，生成分析的各项内容。
##################################################################################
### 仿真算法：
### STRA 1：从市场分组收益率的相近程度，构建仿真组合
### STRA 2：根据个股选择和加权方法构建组合，通过基金排名、股票特征、股票行业分布等维度
################################################
数据分析步骤：
    1，时间：对于更新时间t，确定对应季末时间T和上一季度末时间T-1
        1.1，时间：确定每年基金数据披露日t_report in [1,2,...,T]，给定t日，定位最近的t_report日;
        基金数据的发布时间分析：
        对于每一年，对于0131、0331、0430、0731、0830、1030六个基金数据披露截止时间，要根据披露的基金持仓
        信息补全。企业股东数据的披露截至时间是0430、0830、1030，基本上可以和上述6个对应起来。
        区间[0101,0131],[0101,0331],[0331,0430],[0630,0731],[0630,0830],[0930,1031],
            [1231,0430],[0331,0430],[0630,0830],[0930,1030],
        1.2，导入基金基础信息：导入该期披露的所有基金基础信息：代码，基金公司、基金经理、类型；
        1.3，调仓频率：基于数据披露日：季度；基于股票价格变动：月度；

    2，数据获取——基金,个股：...见“指标分析”，file=0基金持仓仿真.xlsx
    2.1，个股类数据：
        2.1.2，20050830可以得到2季度所有持仓，与之前的top10重合。
    2.2，基金类数据：
    2.3，行业类数据：28个中信一级行业组合，中信二级行业成长和价值龙头锚109*2=218个

    3，数据分析：例如：Brinson模型：行业和个股收益率的拆分；个股特征：如龙头股、权重变动；基金统计：如基金持仓抱团
        notes:：剔除新股。
    3.1，持仓个股指标：
    3.2，基金业绩和排名：例如，按照一定规则选出来的“绩优”基金；
    3.3，行业组合构建：按照市值、净利润、growth/PE构建行业组合；每个月末/季度末，计算按上月末行业分类下的当月行业分布，并统计上述组合的收益情况；

    4，指标：
    4.1，持仓指标：
        4.1.1，Brinson模型：行业和个股收益率的拆分：对于基金F1、全部基金持仓的股票
    4.2，基金收益率指标：
    4.3，行业类指标：财务、个股收益率、风格等
    4.4，市场类指标：根据市场变动，测算当前季度基金的仓位变动比例；

    5，指标和模型最优化：
    5.1，预测最新仓位，目标方程为最小化收益率误差、或股票组合的加权收益率。
    5.2，限制条件：组合调仓频率、行业配置偏离、个股配置偏离{成长、价值锚}。例如，组合调仓变动季度不超过40%，月度不超过15%；
    5.3，拟合：
        5.3.1，个股拟合
        5.3.2，行业拟合：用持仓占比较高的10个行业组合(或细分成长、价值)对基金组合进行拟合。

    6，统计分析：
    6.1，个股偏离率：估计的仓位和实际仓位的偏离情况；
    6.2，基金业绩和排名偏离率：
    6.3，交易行为：历史组合变动行为，是否低买高卖等。
    
notes:
1,让脚本更简洁：除了基本的路径和时间，其他脚本导入尽量都放在 config_XXX 里，保存再 obj 对象
2，像白云山这样市值、净利润等财务指标都较好，但长期缺乏重要卖方覆盖和公募基金持有。股票长期走势温和上涨甚至小幅优于
指数，但大部分实际投资者体验不佳特别是市场好的时候会被忽视。这种股票应该在市场震荡下行时配置，可以获得超额收益。
Wind行业分类里看起来是医药股，但实际上却是饮料股。这个问题可以通过主观修订的方式来调。另外，中信曾经将其改为饮料但后来
又改回了医药股。
################################################
################################################
Notes: 
1,200612首次实际应用该模块，为了汇总、统计和分析市场不同基金的排名和持仓数据等。
2，研究过程中我发现，定义标准，例如什么是业绩好的基金，对于分析和策略组合结果有非常重要的影响，可能类似于工艺而不是技术。
3，pd.read_csv时，可以用use_cols读取部分columns
===============================================
'''

from calendar import c
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )
sys.path.append(path_ciss_rc + "db\\fund_analysis\\" )

import pandas as pd
import numpy as np
import math
import time 
#######################################################################
### 导入配置文件对象，例如path_db_wind等
from config_data import config_data_fund_ana
config_data_fund_ana_1 = config_data_fund_ana()
from data_io import data_io 
data_io_1 = data_io()
from times import times
times_1 = times()
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

from data_io_fund_ana import data_io_fund_ana
data_io_fund_ana_1 =  data_io_fund_ana()

from data_io_fund_ana import data_io_fund_simulation
data_io_fund_simu_1 = data_io_fund_simulation()

################################################### 
###################################################
class fund_simulation():
    def __init__(self ):
        self.obj_config = config_data_fund_ana_1.obj_config
        self.data_io_1 = data_io()
        #######################################################################
        ### 转换后的基金wds数据表
        self.path_wds_fund = self.obj_config["dict"]["path_wds_fund"]
        
        ### 基金分析数据输出目录
        self.path_ciss_db_fund = self.obj_config["dict"]["path_ciss_db_fund"]
        ### simulation 数据输出目录
        self.path_fund_simu = "D:\\CISS_db\\fund_simulation\\output\\"

    def print_info(self):        
        print(" ")
        ### 股票基金 
        print("get_fund_data  |导入用于计算的基金和个股数据。  ")        
        print("cal_basic_port_mkt_ind |计算市场基础组合，如市值、行业、风格、重要个股等 等   ")  
        print("cal_basic_port_stock |给定股票列表作为单股票组合，计算收益率等指标 ")        
        print("cal_fund_port_top10stock |导入部分基金重仓股票，分别构建组合    ")   
        print("cal_fund_port_top10stock_given| 基于给定的基金代码（在下一个季度），构建top10组合，比 cal_fund_port_top10stock 简单")
        print("cal_fund_nav_indi |导入基金净值和计算区间业绩指标 ")   
        print("cal_fund_stockpct_simu |计算股票配置比例和主成分组合拟合 ")   
        print("cal_fund_skill | 预测能力评估：skill_set，计算三类skill 指标")

        

    def get_fund_data(self,obj_in):
        ### 导入用于计算的基金和个股数据
        ### output：obj_fund_ana["df_ashare_ana"]  股票信息 

        ################################################################################
        ### Step 0, 导入基金配置文件，初始化基金对象
        ### 1，时间：对于更新时间t，确定对应季末时间T和上一季度末时间T-1
        from config_fund import config_fund
        config_fund_1 = config_fund()
        from config_data import config_data
        config_data_1 = config_data()
        ################################################################################
        ### Step 1，数据和分析指标的计算、IO;数据获取——基金,个股：...见“指标分析”，file=0基金持仓仿真.xlsx
        ### 根据日期，导入基金obj、基金数据df和基金持仓df ——不一定需要哦
        
        obj_fund,obj_fund_ana = config_fund_1.load_obj_fund_ana( obj_in ) 

        # obj_fund["dict"] 主要是字典和日期文件
        ### date_list_after_ann;基金数据披露后的第一个交易日 
        # [20060801, 20060831, 20061101, 20070201, 20070402]
        date_list_after_ann = obj_fund_ana["dict"]["date_list_after_ann"] 
        ### "date_list_past"是1999年开始升序排列的之前的日期
        ### "date_list_post"是1999年开始升序排列的之后的日期
        # date_list = obj_fund_ana["dict"]["date_list_post"][:120]

        ### date_list_period;六个基金数据披露截止时间之后的第一个交易日
        ### 0131、0331、0430、0731、0830、1030 [20060731, 20060830, 20061031, 20070131, 20070331]
        # date_list_report  = obj_fund_ana["dict"]["date_list_report"]

        ### 每一个交易日
        # date_list_period = obj_fund_ana["dict"]["date_list_period"] 
        # print("date_list_after_ann", date_list_after_ann[:5])
        # print("date_list_period", date_list_report[:5]) 

        ### 导入df_fund_20070402.csv，df_fund_stock_port_20060801.csv，df_stockpool_fund_20060801.csv，df_fund_company_20180402.csv
        # obj_fund_ana["df_fund"] ；obj_fund_ana["df_stockpool_fund"] ；obj_fund_ana["df_fund_stock_port"] 
        # obj_fund_ana["df_fund_company"]   

        #########################################
        ### Step 2,导入市场个股数据：对于“市场、行业、成长价值”几个维度构建的分组，回溯建立模拟组合净值，用于和基金历史净值进行匹配
        ### 1，给定T日，导入当日的市场分组文件abcd3d，并回溯计算过去2个季度的各组模拟组合走势，与基金净值比较。

        path_temp = "D:\\db_wind\\data_adj\\ashare_ana\\"
        file_name = "ADJ_timing_TRADE_DT_" + obj_in["temp_date"] + "_ALL.csv"
        df_ashare_ana = pd.read_csv( path_temp + file_name  )
        # print("Check ...... \n", df_ashare_ana.head().T )
        ### 去除 Unnamed
        for temp_col in df_ashare_ana.columns:
            if "Unnamed" in temp_col :
                df_ashare_ana = df_ashare_ana.drop( temp_col, axis=1  )

        ### abcd3d_market_ana_trade_dt_20060112.csv
        # path_temp = "D:\\CISS_db\\timing_abcd3d\\market_status_group\\"
        # file_name = "abcd3d_market_ana_trade_dt_" + obj_in["temp_date"] + ".csv"
        # df_market_ana = pd.read_csv( path_temp + file_name ,encoding="gbk" )
        # print("Check ...... \n", df_market_ana.head().T )
        #########################################
        ### Step 3，生成数据输出目录
        obj_fund_ana["dict"]["path_out"] = self.path_fund_simu

        ### 赋值给 obj_fund_ana
        obj_fund_ana["df_ashare_ana"] = df_ashare_ana

        return obj_fund_ana

    def cal_basic_port_mkt_ind(self,obj_fund_ana ):
        ### 计算市场基础组合，如市值、行业、风格、重要个股等 
        ### output：
        # obj_port["dict"]["date_list"] ; 
        # obj_port["dict"]["col_list_port"] 已有的组合列表
        # obj_port["df_ashare_ana"] 股票信息和组合权重
        # obj_port["df_port_unit"] 同时保存净值和业绩指标
        # obj_port["df_perf_eval"] 只有业绩指标没有净值
        df_ashare_ana = obj_fund_ana["df_ashare_ana"]
        #  "20190801"
        temp_date = obj_fund_ana["dict"]["temp_date"] 
        #   "20190630" 
        quarter_end = obj_fund_ana["dict"]["quarter_end"]
        # date_list = obj_fund_ana["dict"]["date_list_post"][:120]

        #################################################################################
        ### Part 1 股票投资组合的量化分析 
        ##################################################################################
        ### Step 1, 股票指标和选股能力,参考 df_market_ana中的每个分组，计算模拟组合区间收益率
        # 参考  单个交易日abcd3d市场状态计算脚本在 ashares_timing_abcd3d.py
        #################################################################################
        ### S 1.1 三类标准化组合、因子组合：市场、行业、成长或价值 | 用df新增列标记的方式
        # 市值组合：总市值、流通市值、股票成交金额”按5%、15%、40%划分大市值、中市值、小市值、小微市值”四个组合。
        # 行业组合：30个一级行业；3个组合：总市值*成交金额前30%、价值指标（市值前40%，PE,ROE）排名前30%、成长指标（市值前40%、净利润增长率、roe）排名前30%；行业动量组合：12个月收益率和ma~ 
        # 成长或价值组合：价值指标（ROE或PE）排名前30%、成长指标（成交金额前30%、净利润增长率、PEG）排名前30%
        # 新建组合名称的列表：col_list_port 
        col_list_port = []
        #################################################################################
        ### S 1.2 市值组合 | S_DQ_MV、S_VAL_MV、S_DQ_AMOUNT
        ### 构建加权市值变量 mv_merge = 0.4*S_DQ_MV + 0.4*S_VAL_MV + 0.2*S_DQ_AMOUNT
        ### Notes:基础股票日数据表可能出现市值等指标缺失的情况，需要重新计算ADJ_timing_TRADE_DT_20200203_ALL.csv
        print("Debug= df_ashare_ana = ", df_ashare_ana.columns  )
        df_ashare_ana.to_excel("D:\\debug-df_ashare_ana.xlsx")

        df_ashare_ana["mv_adj_pct"] = 0.4* df_ashare_ana["S_DQ_MV"]/df_ashare_ana["S_DQ_MV"].sum() +0.4* df_ashare_ana["S_VAL_MV"]/df_ashare_ana["S_VAL_MV"].sum() +0.2* df_ashare_ana["S_DQ_AMOUNT"]/df_ashare_ana["S_DQ_AMOUNT"].sum()

        # 按总市值前5%、15%、40%划分大、中、小、小微市值；notes：quantile需要反过来前5%对应 quantile(0.95)
        mv_total_large = df_ashare_ana["mv_adj_pct"].quantile(0.95)
        mv_total_mid = df_ashare_ana["mv_adj_pct"].quantile(0.85)
        mv_total_small = df_ashare_ana["mv_adj_pct"].quantile(0.6)
        ### 计算各个组合权重 
        df_ashare_ana["port_w_mv_large"] =  df_ashare_ana["mv_adj_pct"].apply(lambda x : x if  x >= mv_total_large else 0   )
        df_ashare_ana["port_w_mv_large"] = df_ashare_ana["port_w_mv_large"] /df_ashare_ana["port_w_mv_large"].sum()
        df_ashare_ana["port_w_mv_mid"] =  df_ashare_ana["mv_adj_pct"].apply(lambda x : x if ( x < mv_total_large and  x >= mv_total_mid  ) else 0   )
        df_ashare_ana["port_w_mv_mid"] = df_ashare_ana["port_w_mv_mid"] /df_ashare_ana["port_w_mv_mid"].sum()
        df_ashare_ana["port_w_mv_small"] =  df_ashare_ana["mv_adj_pct"].apply(lambda x : x if ( x < mv_total_mid and  x >= mv_total_small  ) else 0   )
        df_ashare_ana["port_w_mv_small"] = df_ashare_ana["port_w_mv_small"] /df_ashare_ana["port_w_mv_small"].sum()
        df_ashare_ana["port_w_mv_xs"] =  df_ashare_ana["mv_adj_pct"].apply(lambda x : x if ( x < mv_total_small  ) else 0   )
        df_ashare_ana["port_w_mv_xs"] = df_ashare_ana["port_w_mv_xs"] /df_ashare_ana["port_w_mv_xs"].sum()

        col_list_port = col_list_port + ["port_w_mv_large","port_w_mv_mid","port_w_mv_small","port_w_mv_xs"   ]

        #################################################################################
        ### S 1.3 行业组合 | ind_code、S_VAL_MV、S_DQ_AMOUNT
        ind_list = list( df_ashare_ana["ind_code"].drop_duplicates() )
        ### 删除 0 的值，对应无行业
        ind_list = [ ind1 for ind1 in ind_list if ind1 >0  ]

        for temp_ind in ind_list : 
            ### 行业内股票
            df_ind =  df_ashare_ana[df_ashare_ana["ind_code"] ==temp_ind ]
            ##########################################
            ### 行业成长、价值、动量组合只在市值位于前40%的股票中按指标加权 || 40%对应 .quantile(0.60)
            mv_ind_quantile = df_ind["mv_adj_pct"].quantile(0.60)
            df_ind =  df_ind[df_ind["mv_adj_pct"] >= mv_ind_quantile ]

            # notes: 只有股票市值大于100亿，才有跟踪价值，比如电信行业、石油行业就没必要。单位是万
            if len( df_ind.index ) > 2 and df_ind["S_VAL_MV"].sum() > 100*10000 :
                ##########################################
                ### 行业整体组合
                temp_port = "port_w_"+ str( int(temp_ind ) )
                df_ashare_ana[temp_port ] = df_ashare_ana["ind_code"].apply(lambda x : 1 if  x == temp_ind else 0   )
                df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ] * df_ashare_ana["S_VAL_MV" ]  
                df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ]/ df_ashare_ana[temp_port ].sum()
                # 
                col_list_port = col_list_port + [ temp_port ]
                ##########################################
                ### 行业价值组合;价值指标（ROE或PE）排名前30% |notes:S_VAL_PE_TTM 和 EST_PE_FY1,EST_ROE_FY0这些指标数值都是不全的，特别是EST_前缀的可以看作是否有卖方覆盖 
                temp_port = "port_w_"+ str( int(temp_ind ) ) + "_value"
                df_ashare_ana[temp_port ] = df_ashare_ana["ind_code"].apply(lambda x : 1 if  x == temp_ind else 0   )
                df_ashare_ana["temp" ] = df_ashare_ana["S_VAL_PE_TTM"].apply(lambda x : 1/x if  x > 0  else 0   ) 
                df_ashare_ana["temp2" ] = df_ashare_ana["EST_PE_FY1"].apply(lambda x : 1/x if  x > 0  else 0   ) 
                ### PE市盈率倒数越大越好，df.max()默认对每一列取最大值，如果要对某2列取最大值需要先转置再计算，df["max"]=rr.loc[:,["PE_ttm","PE_FY1"]].T.max()
                df_ashare_ana[temp_port ] =df_ashare_ana.loc[:,["temp","temp2"] ].T.max()
                ### 取市值位于前40%的股票
                df_ashare_ana[temp_port ] =df_ashare_ana[temp_port ] * df_ashare_ana["mv_adj_pct"].apply(lambda x : 1 if  x >= mv_ind_quantile else 0  )
                ### 加权配置权重 
                df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ]/ df_ashare_ana[temp_port ].sum()
                # 
                col_list_port = col_list_port + [ temp_port ]

                ##########################################
                ### 行业成长组合 | 市值前40%、净利润增长率、roe）排名前30%；
                temp_port = "port_w_"+ str( int(temp_ind ) ) + "_growth"
                df_ashare_ana[temp_port ] = df_ashare_ana["ind_code"].apply(lambda x : 1 if  x == temp_ind else 0   )
                ### 同比增长率.净利润(%) ;NET_PROFIT_YOY | notes：这个指标有可能比较夸张，需要去除极端值,取值区间[0,200]
                df_ashare_ana["temp" ] = df_ashare_ana["NET_PROFIT_YOY"].apply(lambda x : 200 if  x > 200  else ( 0 if x<0  else x ))  
                # 除以中位数
                df_ashare_ana["temp" ] = df_ashare_ana["temp" ]/df_ashare_ana["temp" ].median()
                # EST_ROE_FY0 
                df_ashare_ana["temp2" ] = df_ashare_ana["EST_ROE_FY0"].apply(lambda x : x if  x > 0  else 0   ) 
                df_ashare_ana["temp2" ] = df_ashare_ana["temp2" ] / df_ashare_ana["temp2" ].median()
                
                ### 通过除以中位数，使得净利润增长率和ROE两个指标可比，并且选择数值更大的那个。
                df_ashare_ana[temp_port ] =df_ashare_ana.loc[:,["temp","temp2"] ].T.max()
                ### 取市值位于前40%的股票
                df_ashare_ana[temp_port ] =df_ashare_ana[temp_port ] * df_ashare_ana["mv_adj_pct"].apply(lambda x : 1 if  x >= mv_ind_quantile else 0  )
                ### 加权配置权重 
                df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ]/ df_ashare_ana[temp_port ].sum()
                # 
                col_list_port = col_list_port + [ temp_port ]
                
                ##########################################
                ### 行业动量组合 ；行业动量组合：12个月收益率和ma
                # 备选方案 2, indi_short,indi_mid ; 2，看短期均线相对于长期均线的偏离幅度： ma_s_16 > ma_s_100
                # temp_port = "port_w_"+ str( int(temp_ind ) ) + "_mom2"
                # df_ashare_ana[temp_port ] = df_ashare_ana["ind_code"].apply(lambda x : 1 if  x == temp_ind else 0   )
                # ### indi_short,indi_mid | 在大于0的基础上，数值越小越好，因此取倒数
                # df_ashare_ana["temp" ] = df_ashare_ana["indi_short"].apply(lambda x : 1/x if  x > 0 else 0 )  
                # df_ashare_ana["temp" ] = df_ashare_ana["temp" ]  * df_ashare_ana["indi_mid"].apply(lambda x : 1/x if  x > 0 else 0 )  
                # # 除以中位数
                # df_ashare_ana["temp" ] = df_ashare_ana["temp" ]/df_ashare_ana["temp" ].median() 
                # ### 取市值位于前40%的股票
                # df_ashare_ana[temp_port ] =df_ashare_ana[temp_port ] * df_ashare_ana["mv_adj_pct"].apply(lambda x : 1 if  x >= mv_ind_quantile else 0  )
                # ### 加权配置权重 
                # df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ]/ df_ashare_ana[temp_port ].sum()
                # # 
                # col_list_port = col_list_port + [ temp_port ]

                ##########################################
                ### 行业动量组合 ；
                # 备选方案 1，看短期均线相对于长期均线的偏离幅度： ma_s_16 > ma_s_100
                temp_port = "port_w_"+ str( int(temp_ind ) ) + "_mom"
                df_ashare_ana[temp_port ] = df_ashare_ana["ind_code"].apply(lambda x : 1 if  x == temp_ind else 0   )
                ### ma_s_16 > ma_s_100
                df_ashare_ana["temp" ] = df_ashare_ana["ma_s_16"] - df_ashare_ana["ma_s_100"] 
                df_ashare_ana["temp" ] = df_ashare_ana["temp" ].apply(lambda x : x if  x > 0 else 0 )  
                # 除以中位数
                df_ashare_ana["temp" ] = df_ashare_ana["temp" ]/df_ashare_ana["temp" ].median() 
                ### 取市值位于前40%的股票
                df_ashare_ana[temp_port ] =df_ashare_ana[temp_port ] * df_ashare_ana["mv_adj_pct"].apply(lambda x : 1 if  x >= mv_ind_quantile else 0  )
                ### 加权配置权重 
                df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ]/ df_ashare_ana[temp_port ].sum()
                # 
                col_list_port = col_list_port + [ temp_port ]

        #################################################################################
        ### S 1.4 风格组合 | 总市值S_VAL_MV：成交金额S_DQ_AMOUNT前20%；成长、价值
        mv_market_quantile = df_ashare_ana["mv_adj_pct"].quantile(0.80)
        
        ##########################################
        ### 全市场价值组合;价值指标（ROE或PE）排名前30% |notes:S_VAL_PE_TTM 和 EST_PE_FY1,EST_ROE_FY0这些指标数值都是不全的，特别是EST_前缀的可以看作是否有卖方覆盖 
        temp_port = "port_w_"+  "value"
        df_ashare_ana[temp_port ] = 0.0
        df_ashare_ana["temp" ] = df_ashare_ana["S_VAL_PE_TTM"].apply(lambda x : 1/x if  x > 0  else 0   ) 
        df_ashare_ana["temp2" ] = df_ashare_ana["EST_PE_FY1"].apply(lambda x : 1/x if  x > 0  else 0   ) 
        ### PE市盈率倒数越大越好，df.max()默认对每一列取最大值，如果要对某2列取最大值需要先转置再计算，df["max"]=rr.loc[:,["PE_ttm","PE_FY1"]].T.max()
        df_ashare_ana[temp_port ] =df_ashare_ana.loc[:,["temp","temp2"] ].T.max()
        ### 取市值位于前40%的股票
        df_ashare_ana[temp_port ] =df_ashare_ana[temp_port ] * df_ashare_ana["mv_adj_pct"].apply(lambda x : 1 if  x >= mv_market_quantile else 0  )
        ### 加权配置权重 
        df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ]/ df_ashare_ana[temp_port ].sum()
        # 
        col_list_port = col_list_port + [ temp_port ]

        ##########################################
        ### 行业成长组合 | 市值前40%、净利润增长率、roe）排名前30%；
        temp_port =  "port_w_"+  "growth" 
        ### 同比增长率.净利润(%) ;NET_PROFIT_YOY | notes：这个指标有可能比较夸张，需要去除极端值,取值区间[0,200]
        df_ashare_ana["temp" ] = df_ashare_ana["NET_PROFIT_YOY"].apply(lambda x : 200 if  x > 200  else ( 0 if x<0  else x ))  
        # 除以中位数
        df_ashare_ana["temp" ] = df_ashare_ana["temp" ]/df_ashare_ana["temp" ].median()
        # EST_ROE_FY0   
        df_ashare_ana["temp2" ] = df_ashare_ana["EST_ROE_FY0"].apply(lambda x : x if  x > 0  else 0   ) 
        df_ashare_ana["temp2" ] = df_ashare_ana["temp2" ] / df_ashare_ana["temp2" ].median()

        ### 通过除以中位数，使得净利润增长率和ROE两个指标可比，并且选择数值更大的那个。
        df_ashare_ana[temp_port ] =df_ashare_ana.loc[:,["temp","temp2"] ].T.max()
        ### 取市值位于前40%的股票
        df_ashare_ana[temp_port ] =df_ashare_ana[temp_port ] * df_ashare_ana["mv_adj_pct"].apply(lambda x : 1 if  x >= mv_market_quantile else 0  )
        ### 加权配置权重 
        df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ]/ df_ashare_ana[temp_port ].sum()
        # 
        col_list_port = col_list_port + [ temp_port ]
                
        ##########################################
        ### 行业动量组合 ；行业动量组合：12个月收益率和ma
        # 备选方案 1，看短期均线相对于长期均线的偏离幅度： ma_s_16 > ma_s_100
        temp_port = "port_w_"+"mom" 
        ### ma_s_16 > ma_s_100
        df_ashare_ana["temp" ] = df_ashare_ana["ma_s_16"] - df_ashare_ana["ma_s_100"] 
        df_ashare_ana["temp" ] = df_ashare_ana["temp" ].apply(lambda x : x if  x > 0 else 0 )  
        # 除以中位数
        df_ashare_ana[temp_port] = df_ashare_ana["temp" ]/df_ashare_ana["temp" ].median() 
        ### 取市值位于前40%的股票
        df_ashare_ana[temp_port ] =df_ashare_ana[temp_port ] * df_ashare_ana["mv_adj_pct"].apply(lambda x : 1 if  x >= mv_market_quantile else 0  )
        ### 加权配置权重 
        df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ]/ df_ashare_ana[temp_port ].sum()
        # 
        col_list_port = col_list_port + [ temp_port ]

        # 备选方案 2, indi_short,indi_mid ; 2，看短期均线相对于长期均线的偏离幅度： ma_s_16 > ma_s_100
        # ntoes:数据有点问题，先不要了
        # temp_port = "port_w_"+"mom2"
        # ### indi_short,indi_mid | 在大于0的基础上，数值越小越好，因此取倒数
        # df_ashare_ana["temp" ] = df_ashare_ana["indi_short"].apply(lambda x : 1/x if  x > 0 else 0 )  
        # df_ashare_ana["temp" ] = df_ashare_ana["temp" ]  * df_ashare_ana["indi_mid"].apply(lambda x : 1/x if  x > 0 else 0 )  
        # # 除以中位数
        # df_ashare_ana[temp_port ] = df_ashare_ana["temp" ]/df_ashare_ana["temp" ].median() 

        # ### 取市值位于前40%的股票
        # df_ashare_ana[temp_port ] =df_ashare_ana[temp_port ] * df_ashare_ana["mv_adj_pct"].apply(lambda x : 1 if  x >= mv_market_quantile else 0  )
        # ### 加权配置权重 
        # df_ashare_ana[temp_port ] = df_ashare_ana[temp_port ]/ df_ashare_ana[temp_port ].sum()
        # # 
        # col_list_port = col_list_port + [ temp_port ]

        #################################################################################
        ### S 1.5 个股组合 | "ind_code","S_DQ_MV","S_VAL_MV"
        ## 个股组合的原因是一级行业难以描述顶级基金的收益； 以10个组合为例，日净值相关系数大多在 73%~ 97%，也有 45%、-43% 说明仓位有较大变动或者完全无关的
        ## 每个一级行业市值前3名的公司，取流通市值前50名
        ind_list = list( df_ashare_ana["ind_code"].drop_duplicates()  )
        ind_list = [ i for i in ind_list if int(i) > 0  ]
        stock_list = []
        for temp_ind in ind_list : 
            df_sub = df_ashare_ana[ df_ashare_ana["ind_code"]== temp_ind ]
            ### 市值降序排列 "mv_adj_pct" ||  before:"S_VAL_MV"
            df_sub = df_sub.sort_values( by="mv_adj_pct",ascending=False ) 
            temp_code_list = list(df_sub["S_INFO_WINDCODE"].values )[:3]
            if len( temp_code_list ) > 0 : 
                for temp_code in temp_code_list :
                    stock_list = stock_list +  [temp_code ]
        df_sub = df_ashare_ana[ df_ashare_ana["S_INFO_WINDCODE"].isin( stock_list ) ]
        ### 市值降序排列"mv_adj_pct" ||  before:"S_VAL_MV"
        df_sub = df_sub.sort_values( by="mv_adj_pct",ascending=False )
        ###################################
        ### 取前50个股票
        df_sub =df_sub.iloc[:50,: ]

        for temp_i in df_sub.index :
            ### 对每一个股票赋权重
            temp_port = "port_s_" + df_sub.loc[ temp_i, "S_INFO_WINDCODE" ]
            df_ashare_ana.loc[ temp_i, temp_port] = 1 

            col_list_port = col_list_port + [ temp_port ]
        

        ###################################
        ### Notes:df_ashare_ana可能又很多NaN值
        df_ashare_ana = df_ashare_ana.fillna( 0.0 )

        ##################################################################################
        ### Step 2，对所有股票组合，计算未来100个交易日的日收益率和区间指标：ret、mdd、vol、

        ################################################################################## 
        ### S 2.1, 数据导入：所有股票过去20天、60天、90or120天的日收益率
        # 参考脚本： obj_ana = analysis_factor_1.market_status_abcd3d_ana( obj_ana) 

        from data_io_pricevol_financial import data_pricevol_financial
        data_pricevol_financial_1 = data_pricevol_financial()
        # 目标脚本：
        obj_data={}
        obj_data["dict"] ={}
        obj_data["dict"]["code_list"] = df_ashare_ana["S_INFO_WINDCODE"].to_list()
        # 定义日期相关参数 || notes: 
        obj_data["dict"]["if_date_list"] = 1
        obj_data["dict"]["date_list"] = obj_fund_ana["dict"]["date_list"]

        obj_data["dict"]["latest_date"] = obj_fund_ana["dict"]["temp_date"]
        ### ["if_date_list"] = 0时，需要填写 date_len,"date_pre_post"
        # obj_data["dict"]["date_len"] = 120 
        # 默认是向后取N日，"post,"date_len=100天;如果向前取交易日则是 "pre"
        # obj_data["dict"]["date_pre_post"] = "post" 

        ### 导入历史行情和abcd3d数据 
        obj_data = data_pricevol_financial_1.import_data_ashare_period_change( obj_data)
        df_ashare_pctchg = obj_data["df_ashare_pctchg"]
        df_ashare_adjclose = obj_data["df_ashare_adjclose"] 
        # print( df_ashare_pctchg.head() ) 
        # output : 调整后的收盘价：obj_data["df_ashare_adjclose"],百分比涨跌幅： 
        # obj_data["df_ashare_pctchg"]:index是股票代码，columns是[ "S_INFO_WINDCODE",20060801,...,20060203 ] 
        
        ################################################################################## 
        ### S 2.2, 对所有基础股票组合，计算区间组合日涨跌幅、区间收益率、最大回撤、超额收益、波动率等指标。ret、mdd、vol
        ###Input: 1，所有组合权重：df_ashare_ana ； 2， 股票复权收盘价，df_ashare_pctchg； 3， 股票涨跌幅，df_ashare_adjclose

        from ports import portfolio_return
        portfolio_return_1 = portfolio_return()
        #########################################
        ### 设置输入对象 obj_port 
        obj_port ={}
        obj_port["dict"] ={}
        obj_port["dict"]["date_list"] = obj_data["date_list"]
        obj_port["df_ashare_ana"] = df_ashare_ana
        obj_port["df_ashare_pctchg"] = df_ashare_pctchg
        obj_port["df_ashare_adjclose"] = df_ashare_adjclose
        obj_port["dict"]["col_list_port"] = col_list_port

        #########################################
        ### 给定组合初始权重和区间日股票收益率，计算区间组合日收益率，期间无调仓
        obj_port = portfolio_return_1.cal_port_ret_notrade( obj_port )

        #######################################################################################
        ### 主要有用的是 df_port_unit
        # df_port_unit: index是组合名称，columns是日期  
        # obj_port["df_port_all"].to_excel(self.path_fund_simu + "df_basic_port_all_"+ temp_date +".xlsx") 
        # obj_port["df_port_cash"].to_excel(self.path_fund_simu + "df_basic_port_cash_"+ temp_date +".xlsx")
        # obj_port["df_port_stockvalue"].to_excel(self.path_fund_simu + "df_basic_port_stockvalue_"+ temp_date +".xlsx") 
        # obj_port["df_port_bondvalue"].to_excel(self.path_fund_simu + "df_basic_port_bondvalue_"+ temp_date +".xlsx")
        # obj_port["df_port_stock_num"].to_excel(self.path_fund_simu + "df_basic_port_stock_num_"+ temp_date +".xlsx")
        # obj_port["df_port_stock_cost"].to_excel(self.path_fund_simu + "df_basic_port_stock_cost_"+ temp_date +".xlsx")
        ### 这个净值最重要
        # obj_port["df_port_unit"].to_excel(self.path_fund_simu + "df_basic_port_unit_"+ temp_date +".xlsx") 
        
        
        #########################################
        ### 区间收益指标：区间收益率、最大回撤、超额收益、波动率等指标。ret、mdd、vol
        from performance_eval import perf_eval_ashare_port
        perf_eval_ashare_port_1 = perf_eval_ashare_port()
        ### 设置计算组合收益率的对象 
        ### 需要确定市场基础组合用于计算相对收益和相对回撤:一般用大市值组合
        obj_port["dict"]["port_benchmark"] = "port_w_mv_large"
        obj_port = perf_eval_ashare_port_1.perf_eval_port_ret_N(obj_port ) 
        ### output:obj_port["df_port_unit"]同时保存净值和业绩指标 ; obj_port["col_list"]业绩指标的list ;
        # obj_port["df_perf_eval"]  只有业绩指标没有净值

        # notes:导入日收益率后，不但要计算累计收益率和回撤，还要计算超额收益、夏普比率等！！！ 
        obj_port["df_basic_port_unit"] =obj_port["df_port_unit"]
        obj_port["df_basic_perf_eval"] =obj_port["df_perf_eval"]
        
        return obj_port

    def cal_basic_port_stock(self,obj_port):
        ################################################################################## 
        ### 给定股票列表作为单股票组合，计算收益率等指标
        # derived from def cal_basic_port_mkt_ind,因为下一个季度拟合组合收益率计算需要引入上一个季度的top50股票
        ### obj_port通常指的是上个季度的所有信息
        df_port_unit = obj_port["df_port_unit"] ###  同时保存净值和业绩指标
        df_perf_eval = obj_port["df_perf_eval"] ### 只有业绩指标没有净值
        df_ashare_ana = obj_port["df_ashare_ana"]
        
        ### 当季度股票组合列表
        stock_port_list = list( obj_port["df_port_unit"].index ) 
        stock_port_list = [ i for i in stock_port_list if "port_s_" in i  ]
        ### 股票列表
        stock_list = obj_port["dict"]["stock_list_diff"]
        print("stock_list-", stock_list) 

        ################################################################################## 
        ### S 2.1, 数据导入：所有股票过去20天、40、60天|| 90or120天的日收益率
        # 参考脚本： obj_ana = analysis_factor_1.market_status_abcd3d_ana( obj_ana) 

        from data_io_pricevol_financial import data_pricevol_financial
        data_pricevol_financial_1 = data_pricevol_financial()
        # 目标脚本：
        obj_data={}
        obj_data["dict"] ={}
        obj_data["dict"]["code_list"] = stock_list
        # 定义日期相关参数 || notes: ["if_date_list"] = 0时，需要填写 date_len,"date_pre_post"
        obj_data["dict"]["if_date_list"] = 1
        obj_data["dict"]["date_list"] = obj_port["dict"]["date_list_next"]
        obj_data["dict"]["latest_date"] = obj_port["dict"]["temp_date_next"] 
        # obj_data["dict"]["date_len"] = 120 
        # 默认是向后取N日，"post,"date_len=100天;如果向前取交易日则是 "pre"
        # obj_data["dict"]["date_pre_post"] = "post" 

        ### 导入历史行情和abcd3d数据 
        obj_data = data_pricevol_financial_1.import_data_ashare_period_change( obj_data)
        df_ashare_pctchg = obj_data["df_ashare_pctchg"]
        df_ashare_adjclose = obj_data["df_ashare_adjclose"] 
        
        ################################################################################## 
        ### S 2.2, 对所有单一股票组合，计算区间组合日涨跌幅、区间收益率、最大回撤、超额收益、波动率等指标。ret、mdd、vol
        ###Input: 1，所有组合权重：df_ashare_ana ； 2， 股票复权收盘价，df_ashare_pctchg； 3， 股票涨跌幅，df_ashare_adjclose
        from ports import portfolio_return
        portfolio_return_1 = portfolio_return()
        #########################################
        ### 设置输入对象 obj_port_sub:这里的部分组合不能覆盖整个组合！！
        obj_port_sub ={}
        obj_port_sub["dict"] ={}
        obj_port_sub["dict"]["date_list"] = obj_port["dict"]["date_list_next"]
        obj_port_sub["df_ashare_ana"] = df_ashare_ana
        obj_port_sub["df_ashare_pctchg"] = df_ashare_pctchg
        obj_port_sub["df_ashare_adjclose"] = df_ashare_adjclose
        obj_port_sub["dict"]["col_list_port"] = stock_port_list 

        #########################################
        ### 给定组合初始权重和区间日股票收益率，计算区间组合日收益率，期间无调仓
        obj_port_sub = portfolio_return_1.cal_port_ret_notrade( obj_port_sub )

        #########################################
        ### obj_port_sub 赋值给 obj_port  
        obj_port["df_stock_unit_next"] =obj_port_sub["df_port_unit"] 
        
        # obj_port["df_stock_unit_next"].to_excel(self.path_fund_simu + "df_stock_unit_next_"+str(obj_port["dict"]["temp_date_next"] ) +".xlsx") 

        return obj_port

    def cal_fund_port_top10stock(self,obj_fund_ana,obj_port ):
        ### 导入基金重仓股票数据，选取近一年业绩前10基金的重仓股，分别构建组合 
        if "num_fund_simu" in obj_port["dict"].keys() : 
            # 10个不够，下一个季度容易出现0~2个有数据的情况
            num_fund_simu = int(obj_port["dict"]["num_fund_simu"])
        else :
            num_fund_simu = 20

        ###
        df_ashare_ana = obj_fund_ana["df_ashare_ana"]
        #  "20190801"
        temp_date = obj_fund_ana["dict"]["temp_date"] 
        #   "20190630" 
        quarter_end = obj_fund_ana["dict"]["quarter_end"]
        date_list =  obj_port["dict"]["date_list"]
        ### 已有的基础组合列表
        col_list_port = obj_port["dict"]["col_list_port"]

        ################################################################################# 
        ################################################################################# 
        ### S 2.3，导入基金重仓股票，分别构建组合 
        fund_list = list( obj_fund_ana["df_fund"]["F_INFO_WINDCODE"] )
        # obj_in["quarter_end"] = 20190630
        path_holding = "D:\\db_wind\\data_adj\\fund_ana\\" +  quarter_end  +"\\"
        ### notes:大部分文件名：ALL_funds_20110331_20110420.csv
        ### 获取目录内文件，不包括子目录
        file_list = os.listdir( path_holding )
        ### 1,匹配字符“ALL_funds_20110331_" || 
        str_prefix =  "ALL_funds_" + quarter_end
        file_list2= [ i for i in file_list if str_prefix in i   ]
        ### 2，获取符合条件的文件的修改时间
        file_list3= [ os.stat( path_holding + i  ).st_mtime for i in file_list2   ]
        index_1 = file_list3.index( max(file_list3) )
        ### file_holding =  "ALL_funds_" + quarter_end +".csv";文件名：ALL_funds_20110331_20110420.csv
        file_holding = file_list2[ index_1 ]
                
        df_holding = pd.read_csv(path_holding + file_holding ,encoding="gbk" )

        #########################################
        ### 基金持仓数据处理
        ### 去除比例过于小的打新品种 | 阈值按 0.1% 
        df_holding = df_holding[df_holding["STOCK_PER"] > 0.1  ]
        ### 只需要保留部分列 || STOCK_PER 数值比 F_PRT_STKVALUETONAV 大
        df_holding = df_holding.loc[:, ["S_INFO_STOCKWINDCODE","S_INFO_WINDCODE", "STOCK_PER" ] ]
        ### 仅保留基金代码对应的股票组合 | 以19Q2为例，49000条记录降为20000左右。
        df_holding = df_holding[ df_holding["S_INFO_WINDCODE"].isin(fund_list )  ]

        ### 选取股票列表
        list_stock = list( df_holding["S_INFO_STOCKWINDCODE"].drop_duplicates())
        list_stock.sort()
        ### 构建股票组合df
        df_fund_port = pd.DataFrame( index=list_stock, columns= fund_list    )
        df_fund_port[ "S_INFO_WINDCODE"] = df_fund_port.index 

        ######################################### 
        ### 2.3.2，选取近一年业绩前10基金的重仓股 | F_AVGRETURN_YEAR
        df_temp = obj_fund_ana["df_fund"]
        df_temp = df_temp [ df_temp["F_AVGRETURN_YEAR"] > df_temp["F_AVGRETURN_YEAR"].quantile(0.05) ]
        ### 筛选有持仓数据的基金 || notes:有可能出现选出的业绩前列的基金，但是在十大重仓股文件里没有净值
        fund_list_holding =  list( df_holding["S_INFO_WINDCODE"].drop_duplicates() )
        df_temp= df_temp[ df_temp["F_INFO_WINDCODE" ].isin( fund_list_holding ) ]
        df_temp=df_temp.sort_values(by="F_AVGRETURN_YEAR",ascending=False  )
        ### 选中地基金列表 | 10个不够，下一个季度容易出现0~2个有数据的情况
        fund_list_short = list( df_temp["fund_code"].values[:num_fund_simu ] )
        
        ######################################### 
        ###  构建top10组合=fund_list_short，将股票权重存入 df_ashare_ana
        col_list_fund=[]
        count_fund_port = 0 
        for temp_fund in fund_list_short : 
            if count_fund_port < num_fund_simu : 
                ### 基金对应的股票
                df_temp = df_holding[df_holding["S_INFO_WINDCODE"] == temp_fund   ]
                df_temp["w"] = df_temp["STOCK_PER"] / df_temp["STOCK_PER"].sum()  
                
                for temp_i in df_temp.index : 
                    temp_code = df_temp.loc[temp_i,"S_INFO_STOCKWINDCODE" ]
                    ### find temp_code in df_ashare_ana;只会有1个
                    temp_df = df_ashare_ana[ df_ashare_ana["S_INFO_WINDCODE"] == temp_code  ]
                    ### 将权重赋值给 df_ashare_ana
                    df_ashare_ana.loc[ temp_df.index, "top10_"+temp_fund ] = df_temp.loc[temp_i,"w" ]
                    
                count_fund_port =count_fund_port +1 
                print("count_fund_port = ",count_fund_port,temp_fund ) 

                col_list_fund= col_list_fund + [ "top10_"+ temp_fund ]
        
        ######################################### 
        ### col_list_fund 是所有要计算的基金top10持仓组合 
        obj_fund_ana["dict"]["fund_list_short"] = fund_list_short 

        ######################################### 
        ###  
        from ports import portfolio_return
        portfolio_return_1 = portfolio_return()
        #########################################
        ### 设置输入对象 obj_port 
        obj_port_fund ={}
        obj_port_fund["dict"] ={}
        obj_port_fund["dict"]["date_list"] = date_list # date_list_period[:120]
        obj_port_fund["df_ashare_ana"] = df_ashare_ana
        obj_port_fund["df_ashare_pctchg"] = obj_port["df_ashare_pctchg"]
        obj_port_fund["df_ashare_adjclose"] = obj_port["df_ashare_adjclose"]

        ### 与基础组合不同，需要合并基础组合和基金组合 
        col_list_port = col_list_port + col_list_fund
        obj_port_fund["dict"]["col_list_port"] = col_list_port 

        #########################################
        ### 2.3.2，给定组合初始权重和区间日股票收益率，计算区间组合日收益率，期间无调仓
        ### Notes:为了下一步指标计算，这里需要有基础组合和基金组合
        obj_port_fund = portfolio_return_1.cal_port_ret_notrade( obj_port_fund )
        ### 同时包括市场基础组合和基金top10组合的净值
        # obj_port_fund["df_port_unit"]

        #########################################
        ### 2.3.3，区间收益指标：区间收益率、最大回撤、超额收益、波动率等指标。ret、mdd、vol 
        ### 需要确定市场基础组合用于计算相对收益和相对回撤:一般用大市值组合
        ### notes：obj_port_fund["df_port_unit"]同时包括了基础组合和基金前十大组合。  
        from performance_eval import perf_eval_ashare_port
        perf_eval_ashare_port_1 = perf_eval_ashare_port()
        obj_port["df_port_unit"] = obj_port_fund["df_port_unit"]
        obj_port["dict"]["port_benchmark"] = "port_w_mv_large"
        obj_port = perf_eval_ashare_port_1.perf_eval_port_ret_N(obj_port ) 

        ### output:obj_port["df_port_unit"] ; obj_port["col_list"] ;
        #  obj_port["df_perf_eval"]          # obj_port["df_port_unit"] 
        return obj_fund_ana,obj_port


    def cal_fund_port_top10stock_given(self,obj_fund_ana,obj_port ):
        ### 基于给定的基金代码（在下一个季度），构建top10组合，比 cal_fund_port_top10stock 简单
        ### Notes:有可能出现部分股票下一个季度无法导入，例如2007-2的600205，在20170109退市,或者港股缺乏数据。
        ### 参考cal_fund_port_top10stock | 再次导入下一个季度的基金十大重仓股，存到 df_ashare_ana_next里用于比较
        # notes：对于导入数据， df_ashare_ana在obj_port， 不在 obj_fund_ana里
        df_ashare_ana = obj_port["df_ashare_ana"]
        #  "20190801"
        temp_date = obj_fund_ana["dict"]["temp_date"] 
        #   "20190630" 
        quarter_end = obj_fund_ana["dict"]["quarter_end"]
        date_list =  obj_port["dict"]["date_list"]
        ### 已有的基础组合列表
        col_list_port = obj_port["dict"]["col_list_port"]
        ###
        fund_list_given = obj_fund_ana["dict"]["fund_list_given"] 

        ################################################################################# 
        ################################################################################# 
        ### S 2.3，导入基金重仓股票，分别构建组合 
        
        # obj_in["quarter_end"] = 20190630
        path_holding = "D:\\db_wind\\data_adj\\fund_ana\\" +  quarter_end  +"\\"
        ### notes:大部分文件名：ALL_funds_20110331_20110420.csv
        ### 获取目录内文件，不包括子目录
        file_list = os.listdir( path_holding )
        ### 1,匹配字符“ALL_funds_20110331_" || 
        str_prefix =  "ALL_funds_" + quarter_end
        file_list2= [ i for i in file_list if str_prefix in i   ]
        ### 2，获取符合条件的文件的修改时间
        file_list3= [ os.stat( path_holding + i  ).st_mtime for i in file_list2   ]
        index_1 = file_list3.index( max(file_list3) )
        ### file_holding =  "ALL_funds_" + quarter_end +".csv";文件名：ALL_funds_20110331_20110420.csv
        file_holding = file_list2[ index_1 ]
                
        df_holding = pd.read_csv(path_holding + file_holding ,encoding="gbk" )

        #########################################
        ### 基金持仓数据处理
        ### 去除比例过于小的打新品种 | 阈值按 0.1% 
        df_holding = df_holding[df_holding["STOCK_PER"] > 0.1  ]
        ### 只需要保留部分列 || STOCK_PER 数值比 F_PRT_STKVALUETONAV 大
        df_holding = df_holding.loc[:, ["S_INFO_STOCKWINDCODE","S_INFO_WINDCODE", "STOCK_PER" ] ]
        ### 仅保留基金代码对应的股票组合 | 以19Q2为例，49000条记录降为20000左右。
        df_holding = df_holding[ df_holding["S_INFO_WINDCODE"].isin(  fund_list_given )  ]

        ### 选取股票列表
        list_stock = list( df_holding["S_INFO_STOCKWINDCODE"].drop_duplicates())
        list_stock.sort()
        ### 构建股票组合df
        # df_fund_port = pd.DataFrame( index=list_stock, columns= fund_list_given  )
        # df_fund_port[ "S_INFO_WINDCODE"] = df_fund_port.index 

        #########################################  
        ######################################### 
        ###  构建top10组合=fund_list_given，将股票权重存入 df_ashare_ana
        col_list_fund=[]
        count_fund_port = 0 
        for temp_fund in fund_list_given : 
            if count_fund_port < 10: 
                ### 基金对应的股票
                df_temp = df_holding[df_holding["S_INFO_WINDCODE"] == temp_fund   ]
                df_temp["w"] = df_temp["STOCK_PER"] / df_temp["STOCK_PER"].sum()  
                
                for temp_i in df_temp.index : 
                    temp_code = df_temp.loc[temp_i,"S_INFO_STOCKWINDCODE" ]
                    ### find temp_code in df_ashare_ana;只会有1个
                    temp_df = df_ashare_ana[ df_ashare_ana["S_INFO_WINDCODE"] == temp_code  ]
                    ### 将权重赋值给 df_ashare_ana
                    df_ashare_ana.loc[ temp_df.index, "top10_"+temp_fund ] = df_temp.loc[temp_i,"w" ]
                    
                count_fund_port =count_fund_port +1 
                print("count_fund_port = ",count_fund_port,temp_fund ) 

                col_list_fund= col_list_fund + [ "top10_"+ temp_fund ]
        
        ######################################### 
        ### save to output 
        obj_port["df_ashare_ana"] = df_ashare_ana 

        return obj_fund_ana,obj_port
    
    
    def cal_fund_nav_indi(self,obj_fund_ana,obj_port ):
        ################################################################################# 
        ### 导入基金净值和计算区间业绩指标 
        ### Notes：提取下一个季度基金净值时，可能出现历史上有净值，但数据文件里没有，比如184688.SZ在20070801没有净值数据
        temp_date = obj_fund_ana["dict"]["temp_date"] 
        ### 筛选出的基金列表： notes:fund_list_short 需要保证基金代码没有前缀后缀
        fund_list_short = obj_fund_ana["dict"]["fund_list_short"]  
        ### 所有交易日，用于匹配历史文件，以防部分净值文件不全 || obj_port_next["dict"]["date_list"]
        date_list = obj_port["dict"]["date_list"]
                
        
        #################################################################################
        ### S 2.4，导入基金未来90/120天净值和收益率;计算基金区间波动率
        ### file=WDS_ANN_DATE_20210106_ALL.csv,WDS_full_table_full_table_ALL.csv;path=D:\\db_wind\\data_wds\\ChinaMutualFundNAV\\
        ### 获取基金列表:剔除指数基金"IS_INDEXFUND"都是0、
        # df_temp = obj_fund_ana["df_fund"]
        ### 股票市值占净值比例，F_PRT_STOCKTONAV ；obj_fund_ana["df_fund"]里地已经筛选过了。
        # df_temp = df_temp [ df_temp["F_PRT_STOCKTONAV"] >=50 ]

        #########################################
        ### 2.4.1,导入基金净值
        from data_io_fund_ana import data_io_fund_ana
        data_io_fund_ana_1 = data_io_fund_ana()
        ### input需要2个维度的数据：基金代码列表和日期列表
        obj_fund={}
        obj_fund["if_1fund"] = 100  
        ### Wind方式或tushare方式# "tushare" # or "wind" 
        obj_fund["data_source"] ="wind" 
        obj_fund["date_list"] = obj_port["dict"]["date_list"]
        ### 对于给定日期没有对应的数据文件，需要用历史之前的日期替代: 
        ### Method 1：获取全部基金净值
        # obj_fund["fund_list"] = list( df_temp["F_INFO_WINDCODE"] )
        ### Method 2：获取给定基金净值 | notes:fund_list_short 需要保证基金代码没有前缀后缀
        obj_fund["fund_list"] = fund_list_short

        ##########################################
        ### 导入将基金的日收益率，并保存至 df_port_ret
        obj_fund = data_io_fund_ana_1.import_data_fund_nav_period(obj_fund )
                
        ### 部分组合可能由于各种各样的原因存在净值缺失
        print("fund_list_error:", obj_fund["fund_list_error"] ) 

        ##########################################
        ### S 2.4，计算基金区间波动率
        from performance_eval import perf_eval_ashare_port
        perf_eval_ashare_port_1 = perf_eval_ashare_port()
        ### 需要把基金的净值和市场基础组合合并起来：obj_fund["df_port_unit"] ； obj_fund["df_fund_ret"]
        ### 找出不在列表里的基金
        # Notes:在使用导入数据计算跨期基金净值时，有可能出现重复的组合名称
        list_append = list( obj_fund["df_fund_ret"].index ) 
        port_list_append = [ i for i in list_append if i not in obj_port["df_port_unit"].index ]
        # index是组合名称、columns是代码和日期
        if len( list_append  ) > 0 :
            obj_port["df_port_unit"] =  obj_port["df_port_unit"].append( obj_fund["df_fund_ret"].loc[list_append, :] )
            obj_port["dict"]["port_benchmark"] = "port_w_mv_large" 
            
            ##########################################
            ### 设置不要保存到文件，否则下一季度数据会覆盖上一个季度  
            
            ##########################################
            ### 计算绩效指标等
            obj_port = perf_eval_ashare_port_1.perf_eval_port_ret_N(obj_port ) 
        
            ################################################################################ 
            obj_port["dict"]["col_list_port"] = obj_port["dict"]["col_list_port"] + port_list_append
            ### port_list_append 是需要增加的组合列表
            obj_port["dict"]["port_list_append"] = port_list_append

        return obj_fund_ana,obj_port


    def cal_fund_stockpct_simu(self,obj_fund_ana,obj_port ) :
        ### 计算股票配置比例和主成分组合拟合    
        ### 新建拟合相关的columns list
        col_list_simu = []     
        #################################################################################
        ### S 2.5，根据基金收益率和重仓股、市场组合日收益率做回归，估计基金股票配置比例！ 
        ''' 分析文件 C:\rc_202X\rc_202X\2020IAMAC课题\temp.xlsx
        ### 基金前海开源稀缺资产001679.OF，全部基础组合都无法解释，是否引入重要个股？
        # 重要个股标准：市值1000亿以上，每个行业选前3名（很多小行业就没有股票了）
        # ——会有很多同行业滞后的股票，比如民生银行 
        '''
        fund_list_short = obj_fund_ana["dict"]["fund_list_short"] 
        df_perf_eval = obj_port["df_perf_eval"]
        ### 仅有基础组合业绩指标 ，
        df_basic_perf_eval =  obj_port["df_basic_perf_eval"] 
        
        str_list = ["short","mid","long"]
        ### 拟合组合的名字
        port_list_simu = [] 
        ### 只保存有基金净值的list
        fund_list_short_nav = []
        for temp_fund in fund_list_short :
            #################################################################################
            ### 1，基于vol波动率计算股票配置比率，stock_pct= vol_last_fund/vol_last_top10；取值【0~95】
            temp_fund_top10 = "top10_" + temp_fund
            ########################################
            ### 判断是否有净值文件：有时候只有1个基金没有数据，有时候全部10个基金都没有数据
            
            if temp_fund in df_perf_eval.index and temp_fund_top10 in df_perf_eval.index :
                fund_list_short_nav = fund_list_short_nav + [ temp_fund ]
                ### notes:部分季度如20100201、20100802没有数据，因为基金净值缺失，现实中由于基金清算或转型等的变动很正常。
                print("Debug=df_basic_perf_eval",temp_fund, temp_fund_top10)
                
                ### 长、中、短期指标加权
                temp_stock_pct = 0.5*df_perf_eval.loc[temp_fund,"vol_last_long"]/df_perf_eval.loc[temp_fund_top10,"vol_last_long"]
                temp_stock_pct = temp_stock_pct + 0.3*df_perf_eval.loc[temp_fund,"vol_last_mid"]/df_perf_eval.loc[temp_fund_top10,"vol_last_mid"]
                temp_stock_pct = temp_stock_pct + 0.2*df_perf_eval.loc[temp_fund,"vol_last_short"]/df_perf_eval.loc[temp_fund_top10,"vol_last_short"]
                ### 如果 stock_pct >= 0.2 就用这个;notes有可能基金组合波动率比基金净值小很多
                if temp_stock_pct >= 0.2 and temp_stock_pct< 1.1 :
                    ### 如果数值大于1，就要找更合适的替代：
                    ### 如果基金净值波动率显著大于前十大重仓组合，则说明存在非前十大股票可能是波动更大的小盘股或者股票仓位有较大变动。
                    df_perf_eval.loc[temp_fund,"stock_pct"] = min(0.99,temp_stock_pct)
                    print( "股票配置比例 stock_pct,1",temp_stock_pct )
                ### 如果 stock_pct < 0.2 ,基金x和所有基础组合里，收益率最接近的5个组合，取波动率平均值，计算股票配置比例
                if temp_stock_pct < 0.2 or temp_stock_pct >= 1.1 : 
                    df_basic_perf_eval["temp_diff"] = df_basic_perf_eval["ret_last_long"].apply(lambda x : abs( x -df_perf_eval.loc[temp_fund,"ret_last_long"])  ) 
                    ### 升序排列，取前5绝对值最小的
                    df_basic_perf_eval = df_basic_perf_eval.sort_values(by="temp_diff",ascending=True ) 
                    temp_stock_pct2 = 0.5* df_perf_eval.loc[temp_fund,"vol_last_long"] / df_basic_perf_eval["vol_last_long"].iloc[:5].mean()
                    temp_stock_pct2 = temp_stock_pct2 + 0.3*df_perf_eval.loc[temp_fund,"vol_last_mid"]   /df_basic_perf_eval["vol_last_mid"].iloc[:5].mean()
                    temp_stock_pct2 = temp_stock_pct2 + 0.2*df_perf_eval.loc[temp_fund,"vol_last_short"] /df_basic_perf_eval["vol_last_short"].iloc[:5].mean()
                    df_perf_eval.loc[temp_fund,"stock_pct"] = min(0.99,temp_stock_pct2)
                    print( "股票配置比例 stock_pct,2",temp_stock_pct,temp_stock_pct2 ) 
                #################################################################################
                ### 方法一：1，单指标拟合
                ### 分别对每个指标进行计算：alpha_last_short ，sharp_annual_last_short，calmar_annual_last_short | sortino数值太夸张不要。
                for temp_str in [ "ret_last_" , "mdd_last_", "vol_last_","vol_relative_last_","alpha_last_","sharp_annual_last_","calmar_annual_last_" ] :
                    #######################################
                    ### 2，基于单个指标拟合   收益率，ret_last_ ；ret_last_short	ret_last_mid,ret_last_long
                    ### 长、中、短期指标加权
                    # temp_str = "ret_last_"
                    temp_indicator = 0.5*df_perf_eval.loc[temp_fund, temp_str+"long"]
                    temp_indicator = temp_indicator + 0.3*df_perf_eval.loc[temp_fund, temp_str+"mid"]
                    temp_indicator = temp_indicator + 0.2*df_perf_eval.loc[temp_fund, temp_str+"short"]
                    ### 基金收益率temp_indicator 要转化成基金的股票组合收益率
                    temp_indicator_stock = temp_indicator/ df_perf_eval.loc[temp_fund,"stock_pct"]
                    ### 
                    df_basic_perf_eval["temp_diff"] = 0.5*df_perf_eval[temp_str+"long"]+0.3*df_perf_eval[temp_str+"mid"]+ 0.2*df_perf_eval[ temp_str+"short"]
                    ### 计算绝对偏离值 Qs:是否只选择正偏离？  ||  todo
                    df_basic_perf_eval["temp_diff"] = df_basic_perf_eval["temp_diff"].apply(lambda x : abs( x- temp_indicator_stock  )  )
                    df_basic_perf_eval = df_basic_perf_eval.sort_values(by="temp_diff",ascending=True )  
                    ### 如果所有基础组合收益率都小于基金，则取负值里最小的 || 理论上有可能
                    #######################################
                    ### 取得拟合组合的权重【以基础组合而不是个股】保存到 df_port_simu
                    ### 取前三名组合，按50：30：20或等权重配置
                    temp_simu_port = "simu_" + temp_str + temp_fund
                    # 
                    df_perf_eval.loc[ temp_simu_port,"simu_port_1"] =  df_basic_perf_eval.index[0]
                    df_perf_eval.loc[ temp_simu_port,"simu_port_1_w"] = 0.5 
                    df_perf_eval.loc[ temp_simu_port,"simu_port_2"] =  df_basic_perf_eval.index[1]
                    df_perf_eval.loc[ temp_simu_port,"simu_port_2_w"] = 0.3 
                    df_perf_eval.loc[ temp_simu_port,"simu_port_3"] =  df_basic_perf_eval.index[2]
                    df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"] = 0.2
                    port_list_simu = port_list_simu + [ temp_simu_port ]

                #################################################################################
                ### 方法二：分长、中、短期拟合
                for temp_str_date in [ "short" , "mid", "long" ] :
                    df_basic_perf_eval["score_"+temp_str_date ] = 0.0 
                    ### 
                    col_list_indi = ["ret_last_" + temp_str_date ,"mdd_last_" + temp_str_date,"vol_last_" + temp_str_date,"vol_relative_last_" + temp_str_date]
                    col_list_indi = col_list_indi + [ "alpha_last_" + temp_str_date, "alpha_annual_last_" + temp_str_date,"sharp_annual_last_" + temp_str_date,"calmar_annual_last_" + temp_str_date]
                    num_col = len( col_list_indi )
                    #######################################
                    ### 将指标标准化后相加 | 取绝对值
                    for temp_col in col_list_indi :  
                        ### 计算基础组合里该指标和基金组合该指标的偏离程度 
                        temp_value=  df_perf_eval.loc[temp_fund, temp_col ]
                        df_basic_perf_eval["temp_diff"] = df_basic_perf_eval[temp_col ].apply( lambda x : abs(x -temp_value ) )
                        ### 去除极端值 :如果有100~200个基础组合，前3%对应约3~6个组合的数值
                        temp_min = df_basic_perf_eval["temp_diff"].quantile(0.02)
                        temp_max = df_basic_perf_eval["temp_diff"].quantile(0.98)
                        df_basic_perf_eval["temp_diff"] = df_basic_perf_eval["temp_diff"].apply(lambda x: min(temp_max, max(x, temp_min ) ) )
                        temp_min = df_basic_perf_eval["temp_diff"].min() 
                        temp_max = df_basic_perf_eval["temp_diff"].max() 
                        df_basic_perf_eval["score_temp"] = (df_basic_perf_eval["temp_diff"] - temp_min)/ (temp_max -temp_min  )
                        df_basic_perf_eval["score_"+temp_str_date ] =df_basic_perf_eval["score_"+temp_str_date ] +df_basic_perf_eval["score_temp"] 
                    
                    ### 最后除以指标个数
                    df_basic_perf_eval["score_"+temp_str_date ] =df_basic_perf_eval["score_"+temp_str_date ] / num_col 
                    #######################################
                    ### 将指标标准化后相加，总偏离值越小越好
                    df_basic_perf_eval = df_basic_perf_eval.sort_values(by="score_"+temp_str_date ,ascending=True )   
                    #######################################
                    ### 取得拟合组合的权重【以基础组合而不是个股】保存到 df_port_simu
                    ### 取前三名组合，按50：30：20或等权重配置
                    temp_simu_port = "simu_" + temp_str_date + "_" + temp_fund
                    # 
                    df_perf_eval.loc[ temp_simu_port,"simu_port_1"] =  df_basic_perf_eval.index[0]
                    df_perf_eval.loc[ temp_simu_port,"simu_port_1_w"] = 0.5 
                    df_perf_eval.loc[ temp_simu_port,"simu_port_2"] =  df_basic_perf_eval.index[1]
                    df_perf_eval.loc[ temp_simu_port,"simu_port_2_w"] = 0.3 
                    df_perf_eval.loc[ temp_simu_port,"simu_port_3"] =  df_basic_perf_eval.index[2]
                    df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"] = 0.2
                    # 
                    port_list_simu = port_list_simu + [ temp_simu_port ]

        ######################
        ### save to output
        obj_port["df_perf_eval"] = df_perf_eval
        ### ??? 不知道为什么 obj_port["df_port_simu"] 在最后边会变没
        obj_port["df_port_simu"]= df_perf_eval
        obj_port["df_basic_perf_eval"]= df_basic_perf_eval

        if len( fund_list_short_nav ) > 0 :
            obj_port["dict"]["col_list_indi" ] = col_list_indi
            ### 拟合组合的名字
            obj_port["dict"]["port_list_simu"] = port_list_simu
        
        ### 拟合组合对应的列
        col_list_simu = [ "stock_pct","simu_port_1","simu_port_1_w","simu_port_2","simu_port_2_w","simu_port_3","simu_port_3_w" ] 
        obj_port["dict"]["col_list_simu"] = col_list_simu
        ###
        obj_fund_ana["dict"]["fund_list_short"] = fund_list_short_nav
        obj_port["dict"]["fund_list_short"] = fund_list_short_nav
        ### for debug use  
        

        return obj_fund_ana,obj_port


    def cal_simu_next_period(self, obj_fund_ana,obj_port ):
        #################################################################################
        ### S 2.6，根据"simu_"拟合组合的配置比例，在下一个季度区间（90天）统计组合收益率
        # df_port_simu : index是基础组合，columns是权重 ———— 用于下一个周期的skill评估 
        temp_date_next = obj_port["dict"]["temp_date_next"] 
        quarter_end_next = obj_port["dict"]["quarter_end_next"] 

        fund_list_short = obj_fund_ana["dict"]["fund_list_short"] 
        ### 构成基金simu拟合组合配置的信息在  df_perf_eval
        df_perf_eval = obj_port["df_perf_eval"] 
        ### 每个基金约对应10个拟合simu的组合，组合名称保持在 temp_str,temp_str_date
        # temp_simu_port = "simu_" + temp_str + temp_fund
        list_temp_str = [ "ret_last" , "mdd_last", "vol_last","vol_relative_last","alpha_last","sharp_annual_last","calmar_annual_last" ] 
        # temp_simu_port = "simu_" + temp_str_date + "_" + temp_fund
        list_temp_str_date = [ "short" , "mid", "long" ]
        ### 
        df_port_unit = obj_port["df_port_unit"]

        ############################################################################## 
        ### 1, 导入下一个周期的基础组合和相关数据
        ### 1.1，导入下一期的数据：
        obj_port_next,obj_fund_ana_next = data_io_fund_simu_1.import_fund_simulation(  temp_date_next) 

        ### 获取基础组合日净值和业绩指标
        df_basic_port_unit_next = obj_port_next["df_basic_port_unit"]
        ### 所有组合净值和业绩指标 | 下一季度的基金没用，需要用之前10个基金匹配下一个季度数据
        # df_port_unit = obj_port["df_port_unit"]
        date_list_next = obj_port_next["dict"]["date_list"]
        
        
        ############################################################################## 
        ### 1.2，导入当季度单一股票组合，在下一个季度的收益率
        ### 下一季度已经有的股票就不用再导入了
        stock_port_list_next = list( obj_port_next["df_port_unit"].index ) 
        stock_port_list_next = [ i for i in stock_port_list_next if "port_s_" in i  ]
        obj_port["dict"]["stock_list_next"] = [ i.split("port_s_")[-1] for i in stock_port_list_next ]
        ### 当季度股票列表
        stock_port_list = list( obj_port["df_port_unit"].index ) 
        stock_port_list = [ i for i in stock_port_list if "port_s_" in i  ]
        obj_port["dict"]["stock_list"] = [ i.split("port_s_")[-1] for i in stock_port_list ] 
        ### 不重合的股票列表：
        obj_port["dict"]["stock_list_diff"] = [ i for i in obj_port["dict"]["stock_list"] if i not in obj_port["dict"]["stock_list_next"] ]  

        # 必须的input
        obj_port["dict"]["date_list_next"] = date_list_next
        obj_port["df_ashare_ana_next"] = obj_port_next["df_ashare_ana"]
        ##########################################################################
        ### 避免数据混乱，使用临时对象 obj_port_temp
        obj_port_temp = self.cal_basic_port_stock(obj_port )
        ### output:只有股票组合下一季度净值的df： obj_port["df_stock_unit_next"]  
        ##########################################################################
        ### Notes:VIP,还需要将差额的股票转为组合，方便后续skill_stock中拟合组合的权重合并计算
        df_ashare_ana_next = obj_port_next["df_ashare_ana"]
        if len( obj_port_temp["dict"]["stock_list_diff"]  ) > 0 :
            for temp_stock in obj_port_temp["dict"]["stock_list_diff"]  :
                ### find stock in 
                df_sub = df_ashare_ana_next[ df_ashare_ana_next["S_INFO_WINDCODE"]==temp_stock ]
                if len(df_sub.index) > 0 :
                    df_ashare_ana_next["port_s_"+temp_stock  ] = 0.0 
                    df_ashare_ana_next.loc[df_sub.index[0] , "port_s_"+temp_stock  ] = 1 

        obj_port_next["df_ashare_ana"] = df_ashare_ana_next 

        ##################################################################################
        ### 1.3，单一股票组合收益率与基础组合的合并后，计算绩效指标：  ret、mdd、vol
        from performance_eval import perf_eval_ashare_port
        perf_eval_ashare_port_1 = perf_eval_ashare_port()
        ### 设置计算组合收益率的对象 
        ### 需要确定市场基础组合用于计算相对收益和相对回撤:一般用大市值组合
        obj_port_next["dict"]["port_benchmark"] = "port_w_mv_large"
        #########################################
        ### 为了减少计算量，只把市场组合添加到股票组合里
        df_stock_unit_next = obj_port_temp["df_stock_unit_next"] 
        # 保存只有股票组合的list
        stock_port_list = df_stock_unit_next.index
        df_stock_unit_next = df_stock_unit_next.append( obj_port_next["df_port_unit"].loc[ "port_w_mv_large" , : ] )
        ### 避免数据混乱，使用临时对象 obj_port_temp
        ### Notes: 如果 obj_port_temp = obj_port_next  会导致 obj_port_next 数值跟着前者变动
        obj_port_temp = {}
        obj_port_temp["dict"]={}
        obj_port_temp["dict"]["date_list"] = obj_port_next["dict"]["date_list"]
        obj_port_temp["df_port_unit"] = df_stock_unit_next
        obj_port_temp["dict"]["port_benchmark"] = obj_port_next["dict"]["port_benchmark"]

        obj_port_temp = perf_eval_ashare_port_1.perf_eval_port_ret_N(obj_port_temp )
        ### output:obj_port["df_port_unit"]同时保存净值和业绩指标 ; obj_port["col_list"]业绩指标的list ;
        # obj_port["df_perf_eval"]  只有业绩指标没有净值 
        #########################################
        ### 将_temp个股组合添加到_next对象： obj_port_temp["df_port_unit"]；obj_port_temp["df_perf_eval"]
        # obj_port["df_basic_perf_eval"] ; obj_port["df_basic_port_unit"]
        ### notes:部分stock_port_list的股票已经在  obj_port_next["df_port_unit"].index里了 
        ### 判断stock_port_list里的股票是否在obj_port_next["df_port_unit"]里边
        # 以20091102为例，stock_port_list之前50个股票，去重复项和剩余7个。
        stock_port_list = [code for code in stock_port_list if code not in obj_port_next["df_port_unit"].index  ]
        obj_port_next["df_port_unit"] = obj_port_next["df_port_unit"].append( obj_port_temp["df_port_unit"].loc[stock_port_list,: ] )

        obj_port_next["df_basic_port_unit"] = obj_port_next["df_basic_port_unit"].append( obj_port_temp["df_port_unit"].loc[stock_port_list,: ] )
        obj_port_next["df_perf_eval"] = obj_port_next["df_perf_eval"].append( obj_port_temp["df_perf_eval"].loc[stock_port_list,: ] )
        obj_port_next["df_basic_perf_eval"] = obj_port_next["df_basic_perf_eval"].append( obj_port_temp["df_perf_eval"].loc[stock_port_list,: ] )

        
        ####################################### 
        ### 2,获取基金的净值| 
        obj_fund_ana_next["dict"]["fund_list_short"] = fund_list_short
        ### 设置不要保存到文件，否则下一季度数据会覆盖上一个季度
        obj_fund_ana_next,obj_port_next = self.cal_fund_nav_indi( obj_fund_ana_next,obj_port_next)
        ### output:obj_port["dict"]["col_list_port"] = obj_port["dict"]["col_list_port"] + port_list_append
        ### 是需要增加的组合列表 :obj_port["dict"]["port_list_append"] 
        ## 净值数据在：obj_port["df_port_unit"] =  obj_port["df_port_unit"].append( obj_fund["df_fund_ret"] )
        ## 基金列表：fund_list_short = obj_fund_ana["dict"]["fund_list_short"] 
        df_port_unit_next = obj_port_next["df_port_unit"]

        # print("df_perf_eval. \n" ,df_perf_eval.head()  )
        ##############################################################################
        ### 按照之前计算好的基础组合配置比例，计算simu拟合组合在下一个季度的净值！
        ### 基础组合信息
        # df_basic_perf_eval_next =  obj_port_next["df_basic_perf_eval"] 
        
        for temp_fund in fund_list_short :
            #######################################
            ### 构成基金simu拟合组合配置的信息在  df_perf_eval
            ### 基于单一指标的多个时期
            for temp_str in list_temp_str:
                temp_simu_port = "simu_" + temp_str +"_"+ temp_fund
                ### 上一季度预测的股票配置比例
                temp_stock_pct = df_perf_eval.loc[temp_fund,"stock_pct"]
                ### 基础组合及权重
                temp_port1 = df_perf_eval.loc[ temp_simu_port,"simu_port_1"] 
                temp_port1_w = df_perf_eval.loc[ temp_simu_port,"simu_port_1_w"]
                temp_port2 = df_perf_eval.loc[ temp_simu_port,"simu_port_2"]
                temp_port2_w = df_perf_eval.loc[ temp_simu_port,"simu_port_2_w"] 
                temp_port3 = df_perf_eval.loc[ temp_simu_port,"simu_port_3"] 
                temp_port3_W = df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"]  
                #######################################
                ### 拟合基础组合的净值：以及业绩指标
                ### Notes:df_port_unit_next.index中，部分基础组合可能存在重复：例如20091102，"port_s_601766.SH"
                df_port_unit_next.loc[temp_simu_port , :]=temp_port1_w* df_port_unit_next.loc[temp_port1, :] 
                df_port_unit_next.loc[temp_simu_port , :]=df_port_unit_next.loc[temp_simu_port ,:] +temp_port2_w* df_port_unit_next.loc[temp_port2, :]
                df_port_unit_next.loc[temp_simu_port , :]=df_port_unit_next.loc[temp_simu_port ,:] +df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"] * df_port_unit_next.loc[temp_port3, :]
                

            #######################################
            ### 基于日期的指标组合
            for temp_str_date in list_temp_str_date :
                temp_simu_port =  "simu_" + temp_str_date + "_" + temp_fund
                ### 获取数据
                temp_port1 = df_perf_eval.loc[ temp_simu_port,"simu_port_1"] 
                temp_port1_w = df_perf_eval.loc[ temp_simu_port,"simu_port_1_w"]
                temp_port2 = df_perf_eval.loc[ temp_simu_port,"simu_port_2"]
                temp_port2_w = df_perf_eval.loc[ temp_simu_port,"simu_port_2_w"] 
                temp_port3 = df_perf_eval.loc[ temp_simu_port,"simu_port_3"] 
                temp_port3_W = df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"] 
                #######################################
                ### 拟合基础组合的净值：
                df_port_unit_next.loc[temp_simu_port , :]=temp_port1_w* df_port_unit_next.loc[temp_port1, :]
                df_port_unit_next.loc[temp_simu_port , :]=df_port_unit_next.loc[temp_simu_port , :] +temp_port2_w* df_port_unit_next.loc[temp_port2, :]
                df_port_unit_next.loc[temp_simu_port , :]=df_port_unit_next.loc[temp_simu_port , :] + df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"]  * df_port_unit_next.loc[temp_port3, :]

        ##############################################################################
        ### 按照之前计算好的基础组合配置比例，计算simu拟合组合在开始季度的净值，用于skill_stable的计算
        ### 基础组合信息
        # df_basic_perf_eval =  obj_port["df_basic_perf_eval"] 
        
        for temp_fund in fund_list_short :
            #######################################
            ### 构成基金simu拟合组合配置的信息在  df_perf_eval
            ### 基于单一指标的多个时期
            for temp_str in list_temp_str:
                temp_simu_port = "simu_" + temp_str +"_"+ temp_fund
                ### 上一季度预测的股票配置比例
                temp_stock_pct = df_perf_eval.loc[temp_fund,"stock_pct"]
                ### 基础组合及权重
                temp_port1 = df_perf_eval.loc[ temp_simu_port,"simu_port_1"] 
                temp_port1_w = df_perf_eval.loc[ temp_simu_port,"simu_port_1_w"]
                temp_port2 = df_perf_eval.loc[ temp_simu_port,"simu_port_2"]
                temp_port2_w = df_perf_eval.loc[ temp_simu_port,"simu_port_2_w"] 
                temp_port3 = df_perf_eval.loc[ temp_simu_port,"simu_port_3"] 
                temp_port3_W = df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"]  
                #######################################
                ### 拟合基础组合的净值：以及业绩指标
                df_port_unit.loc[temp_simu_port , :]=temp_port1_w* df_port_unit.loc[temp_port1, :] 
                df_port_unit.loc[temp_simu_port , :]=df_port_unit.loc[temp_simu_port ,:] +temp_port2_w* df_port_unit.loc[temp_port2, :]
                df_port_unit.loc[temp_simu_port , :]=df_port_unit.loc[temp_simu_port ,:] +df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"] * df_port_unit.loc[temp_port3, :]
                

            df_port_unit.to_excel("D:\\debug-df_port_unit.xlsx")
            #######################################
            ### 基于日期的指标组合
            for temp_str_date in list_temp_str_date :
                temp_simu_port =  "simu_" + temp_str_date + "_" + temp_fund
                ### 获取数据
                temp_port1 = df_perf_eval.loc[ temp_simu_port,"simu_port_1"] 
                temp_port1_w = df_perf_eval.loc[ temp_simu_port,"simu_port_1_w"]
                temp_port2 = df_perf_eval.loc[ temp_simu_port,"simu_port_2"]
                temp_port2_w = df_perf_eval.loc[ temp_simu_port,"simu_port_2_w"] 
                temp_port3 = df_perf_eval.loc[ temp_simu_port,"simu_port_3"] 
                temp_port3_W = df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"] 
                #######################################
                ### 拟合基础组合的净值：
                df_port_unit.loc[temp_simu_port , :]=temp_port1_w* df_port_unit.loc[temp_port1, :]
                df_port_unit.loc[temp_simu_port , :]=df_port_unit.loc[temp_simu_port , :] +temp_port2_w* df_port_unit.loc[temp_port2, :]
                df_port_unit.loc[temp_simu_port , :]=df_port_unit.loc[temp_simu_port , :] + df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"]  * df_port_unit.loc[temp_port3, :]

        #######################################
        ### 保存到输出文件 | obj_fund_ana_next也有变动
        obj_port_next["df_port_unit"] = df_port_unit_next 
        obj_port["df_port_unit"] = df_port_unit

        return obj_port,obj_fund_ana_next,obj_port_next
        









 