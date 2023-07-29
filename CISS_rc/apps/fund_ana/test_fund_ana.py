# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
################################################
last 201229 | since 200201
derived from rc_data\test_wds_data_transform_fund.py
目录：path=C:\rc_reports_cs\rc_2020论文_课题\0paper_基金持仓研究和基金分类
path=C:\ciss_web\CISS_rc\apps\fund_ana;file=0基金持仓仿真.xlsx
################################################
功能： 1.股票基金数据分析：梳理A股基金权益部分的收益和持仓变动
    2,应用：应用机器学习算法构建基金持仓仿真的策略，实现收益率和波动率的仿真
    3，仿真组合建模和跟踪
################################################
todo：0，股票基金数据分析，从单个日期维度和单一基金等维度。

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
3.1，像白云山这样市值、净利润等财务指标都较好，但长期缺乏重要卖方覆盖和公募基金持有。股票长期走势温和上涨甚至小幅优于
指数，但大部分实际投资者体验不佳特别是市场好的时候会被忽视。这种股票应该在市场震荡下行时配置，可以获得超额收益。
Wind行业分类里看起来是医药股，但实际上却是饮料股。这个问题可以通过主观修订的方式来调。另外，中信曾经将其改为饮料但后来
又改回了医药股。
################################################
'''
#################################################################################
### Initialization，load configuration
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )


import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

### 导入数据目录配置文件  
from config_data import config_data_fund_ana
config_data_fund_ana_1 =  config_data_fund_ana()

### 导入数据IO模块
from data_io import data_io
data_io_1 = data_io()
### 导入基金分析IO模块
from data_io_fund_ana import data_io_fund_ana
data_io_fund_ana_1 = data_io_fund_ana()
data_io_fund_ana_1.print_info()

# ### data_timing_abcd3d,data_factor_model
# data_timing_abcd3d_1 = data_timing_abcd3d()
# data_timing_abcd3d_1.print_info()
# data_factor_model_1 = data_factor_model()
# data_factor_model_1.print_info()

### 导入wds数据转换模块
from transform_wind_wds import transform_wds
transform_wds1 = transform_wds()
### Print all modules 
transform_wds1.print_info()
### 导入日期序列
obj_dates = transform_wds1.import_df_dates()
print("date_start",obj_dates["date_start"],"date_end",obj_dates["date_end"]  )

### 导入基金数据分析模块
from funds import fund_ana
fund_ana_1 = fund_ana()
fund_ana_1.print_info()


#####################################################################
### 初始化基金管理对象 df || 
'''
1，时间：对于更新的交易日t，确定最近的4个披露期T_pre4，T_pre3,T_pre2，T_pre1
    从20050104开始对于每一年，对于0131、0331、0430、0731、0830、1030六个基金数据披露截止时间，要根据披露的基金持仓
    信息补全。企业股东数据的披露截至时间是0430、0830、1030，基本上可以和上述6个对应起来。
    区间[0101,0131],[0101,0331],[0331,0430],[0630,0731],[0630,0830],[0930,1031],
        [1231,0430],[0331,0430],[0630,0830],[0930,1030],
    
2，t日所有有净值的基金，剔除不满足标准的基金：
    2.1，基金成立日期超过6个月，最近2期持仓有股票；
   
3，导入该期披露的所有基金基础信息：代码，基金公司、基金经理、类型；
    3.1，table=
4，设置基金不同数据的对应目录：
    4.1，table=
5, 更新频率设置：
    1，按季度根据持仓变动分析边际信息；
    2，按周、月或市场重大数据变动分析

'''
########################################################################
### 设置组合id，是否单一基金"single_fund"，开始和结束日期
obj_fund_ana = {}
obj_fund_ana["dict"] ={}
# "rc_2005"
# str_year = input("Type in year such as 2012,2016,2019:")

obj_fund_ana["dict"]["id_output"] = "rc_2021" 
# input("Type in id for portfolio,such as rc_2005:")  
obj_fund_ana["dict"]["single_fund"] = 1
### 若单个一级行业，则取行业内前50%;默认前30%；若indi_quantile_tail=1, 则取尾部指标值，默认值0取指标最大的。
obj_fund_ana["dict"]["indi_quantile_tail"] = 0 # 1

###################################
### VIP：设置开始和结束日期区间
# 201805和201808、201811、201905没有 || # "20200501"  # "20080401"  # "20060401" 
# obj_fund_ana["dict"]["date_start"] = str_year + "0701" 
obj_fund_ana["dict"]["date_start"] = "20200501" 
obj_fund_ana["dict"]["date_end"] ="20201231" 
#"20211231" # input("Type in date start such as 20190506: ")

########################################################################
### 导入区间日期数据,0131、0331、0430、0731、0830、1030六个基金数据披露截止时间之后的第一个交易日
### todo: 要生成匹配的季度末日期 WDS_F_PRT_ENDDATE_20120630_ALL.csv
### "if_only_quarter"] = 1 表示不考虑半年度的日期：0131、0430、0830、1030
obj_fund_ana["dict"]["if_only_quarter"] = 1
obj_fund_ana = data_io_1.get_after_ann_days_fund( obj_fund_ana )
# date_list_past时开始日期之前的所有历史交易日
# date_list_past = obj_fund_ana["dict"]["date_list_past"]  
# date_list_post = obj_fund_ana["dict"]["date_list_post"]  

# date_list_period 指的是全部区间交易日
date_list_period = obj_fund_ana["dict"]["date_list_period"] 
date_list_before_ann = obj_fund_ana["dict"]["date_list_before_ann"]

date_list_after_ann = obj_fund_ana["dict"]["date_list_after_ann"]
date_list_report = obj_fund_ana["dict"]["date_list_report"]

date_list_after_ann = date_list_after_ann +[ obj_fund_ana["dict"]["date_end"] ]

### 新建df，保存未来一期组合收益：
obj_fund_ana["df_ret_next"]= pd.DataFrame( columns=["date_start","date_end","ret_port"] )

### 获取所有交易日
obj_date={}
obj_date["date"] = "20060101"
obj_date = data_io_1.get_trading_days( obj_date )
date_list_all = obj_date["date_list_post"]
date_list_all.sort()


### 建立PMS权重文件
obj_fund_ana["df_pms"] = pd.DataFrame( columns=["证券代码","持仓权重","成本价格","调整日期","证券类型"] )
obj_fund_ana["count_pms"] =0
### Loop 
obj_fund = {} 
count_date_period = 0

########################################################################
### 数据和分析指标的计算、IO
for temp_date_start in date_list_after_ann:
    ########################################################################
    ### Initialization:特别注意几个不同的日期设置
    ''' ### 注意：在调仓日T有3套时间：
    1，T和之前1次披露时间；[date_ann_pre, date_ann ]
    2，T之前的2个季末财务日期；[date_q_pre,date_q]
    3，T日至下一个财务披露日期:[date_ann, date_ann_next ]。
    '''
    obj_fund = {} 
    obj_fund["dict"] = {} 
    obj_fund["dict"]["id_output"] = obj_fund_ana["dict"]["id_output"] 
    ### 财务报告披露日and组合调整日期：obj_fund["dict"]["date_adj_port"] = str(temp_date_start)
    obj_fund["dict"]["date_adj_port"] = str(temp_date_start)
    obj_fund["dict"]["date_adj_port_next"] = str( date_list_after_ann[ date_list_after_ann.index(temp_date_start)+1 ] )
    ## 保存所有历史区间交易日
    obj_fund["date_list_period"] = date_list_period

    obj_fund["dict"]["date_ann"] = str( date_list_report[ date_list_after_ann.index(temp_date_start) ] )
    # obj_fund["dict"]["date_ann_next"] =str( date_list_report[ date_list_after_ann.index(temp_date_start)+1 ] )
    if count_date_period > 0 :
        obj_fund["dict"]["date_adj_port_pre"] = str( date_list_after_ann[ date_list_after_ann.index(temp_date_start)-1 ] )
        obj_fund["dict"]["date_ann_pre"] = str( date_list_after_ann[ date_list_after_ann.index(temp_date_start)-1 ] )
    else :
        temp_list = [x for x in date_list_all if x< temp_date_start ]
        ### 获取前一个季度末日期
        obj_date_temp ={}
        obj_date_temp["date"] = str( max(temp_list) )
        obj_date_temp = data_io_1.get_report_date_fund(obj_date_temp ) 
        ### notes:obj_date["date_q_pre"] 对应的是前一个季度日期，obj_date["date_q_pre2"] 对应的是前2个季度日期 
        obj_fund["dict"]["date_adj_port_pre"] = max(temp_list)
        obj_fund["dict"]["date_ann_pre"] = obj_date_temp["date_q_pre"]

    # obj_fund["dict"]["date"] = str(temp_date_start) #  "20191105"  
    ### 获取 之前的1个交易日用于导入最近的预期数据；date_list_all
    temp_list = [date for date in date_list_all if date< temp_date_start]
    obj_fund["dict"]["date_adj_port_pre"] = max(temp_list) 
    
    #####################################################################
    ### 导入基金基础数据，基金公司、代码、成立日F_INFO_SETUPDATE 等
    # notes:这部分时间比较久，考虑未来直接读取缩减后的小表
    print("导入基金基础数据")
    obj_fund = data_io_fund_ana_1.import_data_fund_ashare_des(obj_fund)
    # print("Debug==== date_q,date_q_pre" ,obj_fund["dict"]["date_report"],obj_fund["dict"]["date_report_pre"] )
    ''' output:    
    obj_fund["df_fund"] ; obj_fund["col_list_fund_rank"]  
    # 最近2个季度末日期
    date_q = obj_fund["dict"]["date_report"] 
    date_q_pre = obj_fund["dict"]["date_report_pre"] 
    # 最近1、2年前季度末日期
    obj_fund["dict"]["date_report_pre_1y"] = date_q_pre_1y
    obj_fund["dict"]["date_report_pre_2y"] = date_q_pre_2y
    fund_list = obj_fund["fund_list"] 
    df_fund = obj_fund["df_fund"] 
    col_list = obj_fund["col_list_fund_des"] 
    '''

    #####################################################################
    ### 基金净值、持仓、交易数据分析
    '''
    基金数据分析： 
    1，净值和排名数据：
    2，持仓数据
        2.1，重仓个股
        2.2，个股仓位变动
        2.3，中信一级、二级行业变动 
        2.4，剔除新股部分，计算新股收益。
    3，规模、交易换手率数据：历史换手率高低；
    5，基金分组：全市场、基金公司、基金风格、基金业绩、股票持仓、股票持仓分行业-中信1、2级。
    6，Brinson建模分析，基于披露的持仓股票：
        6.1，基准组合股票权重：沪深300、中证500、创业板；
        6.2，基金组合股票权重，和基准组合的差异

    数据分析：对2019q3剔除季度内涨跌幅变动后的市值变动top100的股票做回归分析，分析扣除上涨后的
    市值变动和区间涨跌幅相关系数是 -0.04，取top10和tail10也每发现有显著性。这说明基金整体选股
    在季度区间内没有显著的超额收益能力
    '''
    ### 导入基金净值和排名数据 
    print("导入基金净值和排名数据")
    obj_fund = data_io_fund_ana_1.import_data_fund_nav(obj_fund)
    
    # output:obj_fund["df_fund"] 
    ### 导入基金前十大或全部持股
    print("导入基金前十大或全部持股")
    obj_fund = data_io_fund_ana_1.import_data_fund_holdings(obj_fund)
    # output:例如：obj_fund["df_fund_stock_port"] , obj_fund["col_list_stock_port"]  
    
    ### 导入基金财务指标、利润份额、交易和换手率数据|只有6,12月才有该数据
    # type date_q = str
    date_q = obj_fund["dict"]["date_report"] 
    if date_q[4:6] in ["06","12"] :
        print("导入基金财务指标、利润份额、交易和换手率数据|只有6,12月才有该数据")
        obj_fund = data_io_fund_ana_1.import_data_fund_profit_turnover(obj_fund)

    # 6、12月有数据；股票交易金额(元) F_TRADE_STOCKAM;股票交易金额占比(%) F_TRADE_STOCKPRO
    
    ### 导入基金分组、评级 |暂时不需要
    ### obj_fund = data_io_fund_ana_1.import_data_fund_group_rating(obj_fund)

    #####################################################################
    ### 导入市场个股、指数、行业数据
    '''1,个股区间收益；2，市场基准指数成分、3，行业指数或行业内个股的流通市值加权收益
    todo，统计每个基金的前10大重仓股的股票代码、名称、中信一级行业、二级行业、pe_fy0,pe_fy1,roe_fy0,roe_fy1,profit_g_fy0,profit_g_fy1。
    todo，个股仓位变动;中信一级、二级行业变动;剔除新股部分，计算新股收益。
    print("cal_data_fund_holdings_diff |计算基金持仓股票变动  ")

    todo,参考已有的数据导入模块    
    '''
    ### 导入基金持仓股票代码、名称、上市日期、中信一级行业、二级行业
    print("导入基金持仓股票代码、名称、上市日期、中信一级行业")
    obj_fund = data_io_fund_ana_1.import_data_fund_stock_name_listday_ind(obj_fund)
        
    ### 导入基金持仓股票财务指标：pe_fy0,pe_fy1,roe_fy0,roe_fy1,profit_g_fy0,profit_g_fy1
    #notes:会导入3个日期，obj_fund["dict"]：披露日期["date_adj_port"]、季度初["date_report_pre"]、季度末["date_report"]
    print("导入基金持仓股票财务指标")
    obj_fund = data_io_fund_ana_1.import_data_fund_stock_indicators(obj_fund)
    
    '''
    导入季度初期和季度末期2个数据，期初用后缀suffix= _pre 表示
    notes:obj_fund["dict"]["col_list_stock_indicators"] 中的每一个指标，加上后缀"_pre"就是前一个季度的数据
    其中无后缀对应了披露日股价所处百分位和最近季度末财务指标，后缀"_pre"对应前一披露日和上一季度末财务指标
    指标：
    timing:ma_s_16,ma_s_40;abcd3d	indi_short	indi_mid;
    市值：S_DQ_MV	S_VAL_MV;成交额 S_DQ_AMOUNT；
    PE:EST_PE_FY1，EST_PE_FY0，EST_PE_YOY	
    PEG:EST_PEG_FY1，EST_PEG_FY0，EST_PEG_YOY
    ROE：EST_ROE_FY0
    净利润：NET_PROFIT_FY1,NET_PROFIT_FY0；NET_PROFIT_YOY
    收入和毛利：EST_OPER_PROFIT_FY0,EST_OPER_REVENUE_FY0，EST_TOTAL_PROFIT_FY0，ST_OPER_REVENUE_YOY	
    '''
    #####################################################################
    ### 基金行为回顾、分析、预测 |新建 df_fund_sp_ana from df_fund_stock_port
    #####################################################################
    ### 基金分组：基金规模、业绩、公司、重仓行业(中信1、3级)等角度的筛选和分组
    obj_fund = fund_ana_1.fund_filter_group_fund(obj_fund) 
    
    #####################################################################
    ### 基金持仓股票权重、区间涨跌幅的筛选和分组  
    obj_fund = fund_ana_1.fund_filter_group_stock(obj_fund) 

    ### Debug 截至这里，df_fsp文件里是有 citics_ind_code 列
    obj_fund["df_fund_stock_port"].to_csv("D:\\df_fund_stock_port_4.csv")
    #####################################################################
    ### 基金持仓股票变动的分析:仓位变动、买卖时机、行业分布等 
    obj_fund = fund_ana_1.fund_ana_stock_change_ind(obj_fund) 

    obj_fund["df_fund_stock_port"].to_csv("D:\\df_fund_stock_port_5.csv")


    #####################################################################
    ### 基金净值和排名分析
    '''已经分了5档，若第一档内基金数量太多，可以进一步取前50%，
    1，短期排名：全市场前10%，公司等细分维度前20~25%；
    2，中期排名：全市场前10%，公司等细分维度前20~25%；
    3，长期排名：全市场前10%，公司等细分维度前20~25%；
    notes:数据分析部分已经梳理了长期、中期、短期排名；
    
    '''
    obj_fund = fund_ana_1.fund_ana_nav_rank(obj_fund) 
    


    #####################################################################
    ### 基金持仓股票和调仓行为的仿真和预测  
    '''根据持仓和逐日净值，判断基金最可能的调仓行为；
    对股票的聚类降维分组：市值、区间涨跌幅等；可以参考abcd3d_market。
    ref:/基金仓位与风格估计模型及最新配置信息.pdf

    Qs:如何判断区间内股票的平均成交价格？基金重仓股的仓位相对较重，减持一般需要一个过程。
    idea：可以考虑对区间内取高、中、低三个价格供后续分析
    notes：
    1，基金持仓需要匹配基金净值变动。若基金季末前刚调整持仓，则持仓对之前1季度业绩解释度会比较低，若季末之后立即调仓，则持仓对未来1季度业绩解释度会比较低
    2，对于历史和近期换手率较高的基金
    3，3个维度拟合基金净值变动：市场分组、行业分组、成长和价值分组
    3.0，市场分组维度：大中小市值{sz50，csi300,500,创业板}；1~3级行业分组；
    3.1，回归模型计算市场指数和行业指数对净值的解释程度，选择解释度高的
    3.2，若解释度都较低，则进行标注。

    '''
    obj_fund = fund_ana_1.fund_esti_port_stock_adjust(obj_fund)  

    ### export |notes:新的df,list要及时在export模块中设置输出
    obj_fund =  data_io_fund_ana_1.export_data_fund(obj_fund,obj_fund_ana)

    count_date_period = count_date_period + 1 

asd


#####################################################################
### 基金策略分析
#####################################################################

#####################################################################

#####################################################################
### 基金股票组合收益增强策略 
obj_fund = fund_ana_1.fund_stra_port_stock_alpha(obj_fund) 

#####################################################################
### 基金收益增强模拟组合加权和调仓频率、交易成本 
obj_fund = fund_ana_1.fund_port_weighting_adj(obj_fund) 

#####################################################################
### 基金仿真和调仓行为描述、全市场基金 
obj_fund = fund_ana_1.fund_manage_perf_eval_group_esti(obj_fund) 

#####################################################################
### 基金上述指标分析和单指标有效性、相关性测试
'''几个维度：基金过去收益、基金历史换手、重仓基金数量、重仓持股比例、 重仓基金数量变动、增持与减持的基金数量之差共 6 个指标的 Rank 等权（没有纳入基金规模是因为规模效应不显著。
综合指标HSCORE 主要和业绩超预期、分析师预期和盈利能力相关性较高
ref：20180820_东方证券_金融工程专题_朱剑涛王星星_金融工程研究基金重仓股研究.pdf

'''

#####################################################################
### 基金模拟组合构建：
'''基金模拟组合构建：
    1，基于披露持仓模拟组合port_holdings，和基金净值的相关性，收益率偏离程度；
    2，用市场和行业指数独立拟合净值,port_simu_ind_index
    3,用市场热门股票拟合净值，如成交金额、流通市值等，port_simu_ind_index
'''


#####################################################################
### 基金模拟组合业绩评估：
'''1,逐年、季度计算业绩在全部股票和普通股票型、偏股混合型、混合型基金中的百分位
2,

'''










'''

-------------------------------------------------------------------------------------
wds基金数据进一步分析：
3，分析可以获得的基金数据表:
3.1，基础信息
    table=中国共同基金基本资料,ChinaMutualFundDescription
    columns=基金代码 F_INFO_WINDCODE；成立日期 F_INFO_SETUPDATE；到期日期 F_INFO_MATURITYDATE；是否指数基金 IS_INDEXFUND，0:否 1:是；
    退市日期F_INFO_DELISTDATE,
    table=中国共同基金基金经理,ChinaMutualFundManager
    columns= Wind代码 F_INFO_WINDCODE,公告日期 ANN_DATE;姓名 F_INFO_FUNDMANAGER;任职日期,F_INFO_MANAGER_STARTDATE;离职日期 F_INFO_MANAGER_LEAVED;
    基金经理ID,F_INFO_FUNDMANAGER_ID
    notes:应该以基金经理ID为准，同名同姓的基金经理有比较多的例子。
    
3.2，基金净值：如何从部分配置股票的基金净值中提取股票部分收益| 利用债部分收益不会太高、往期股票配置比例
    table=中国共同基金净值，ChinaMutualFundNAV
        公告日期	ANN_DATE,截止日期	PRICE_DATE;单位净值	F_NAV_UNIT;累计净值	F_NAV_ACCUMULATED;复权因子	F_NAV_ADJFACTOR;货币代码	CRNCY_CODE,CNY
        资产净值	F_PRT_NETASSET,这个不定期公布； 
        合计资产净值	NETASSET_TOTAL
        复权单位净值	F_NAV_ADJUSTED{vip},是否净值除权日	IS_EXDIVIDENDDATE,累计单位分配	F_NAV_DISTRIBUTION{部分牛基不分派，分派越多越有利于提高累计净值}
    notes:大部分2016前后表格没有数据，已经下载了全历史的用于复原
    table=中国共同基金业绩表现，ChinaMFPerformance
        1，本月、本季度、本年以来收益率、同类排名
        2，最近1月~5年收益率、同类排名
        3，年化收益率，跟踪偏离度，标准差、夏普、
        path=D:\db_wind\data_wds\ChinaMFPerformance

3.3，基金持仓
    1,中国共同基金投资组合——持股明细,ChinaMutualFundStockPortfolio;
    columns=[持有股票Wind代码,S_INFO_STOCKWINDCODE;持有股票市值(元),F_PRT_STKVALUE ;持有股票数量（股）,F_PRT_STKQUANTITY;持有股票市值占基金净值比例(%),F_PRT_STKVALUETONAV ; ]
    2,中国共同基金投资组合——资产配置,ChinaMutualFundAssetPortfolio;持有股票市值占资产净值比例(%),F_PRT_STOCKTONAV
    3,大变动(报告期),CFundPortfoliochanges,股票代码,S_INFO_WINDCODE;变动类型,CHANGE_TYPE;累计金额,ACCUMULATED_AMOUNT,占期初基金资产净值比例,BEGIN_NET_ASSET_RATIO;
    4,
3.4，基金财务数据：【2019年之后数据较全】
    1，中国共同基金资产负债表，CMFBalanceSheet;【no,用处不大】
    columns=[股票投资估值增值,STOCK_ADD_VALUE;基金资产净值,PRT_NETASSET  ]
    2，中国共同基金公允价值变动收益(报告期),CMFfairvalueChangeProfit【no,公允价值变动收益可以在财务指标表里获得】
    notes:2011年开始有数据
    columns=[股票投资公允价值变动收益,STOCK_CHANGE_FAIR_VALUE]
    3, 中国共同基金财务指标(报告期)，CMFFinancialIndicator
    notes:2017年底开始有数据
    4，中国共同基金利润表，CMFIncome；
    columns=[投资收益合计,INV_INC;股票差价收入,STOCK_INV_INC;股息收入,DVD_INC;未实现利得,CHANGE_FAIR_VALUE;管理费,MGMT_EXP  ]
    5，中国共同基金净值变动表，CMFNAVChange【2019年开始披露数据】

3.5，衍生表
    1，中国Wind基金仓位估算，ChinaMutualFundPosEstimation，主要是区分大、中、小市值在组合内的权重
        columns= "F_EST_DATE"，约等于交易日
    2，中国共同基金席位交易量及佣金，ChinaMutualFundSeatTrading
        columns= 报告期，S_INFO_REPORTPERIOD
        notes:判断换手率：从"股票交易金额(元),F_TRADE_STOCKAM";"股票交易金额占比(%),F_TRADE_STOCKPRO"
    3，中国共同基金份额，ChinaMutualFundShare
        基金合计份额(万份),FUNDSHARE_TOTAL
    4,中国共同基金基金经理业绩表现,ChinaMFMPerformance | 基金经理的业绩表现:周、月、季度、半年、年、3、5、10
    5，中国共同基金第三方评级，CMFundThirdPartyRating【例如：海通证券】
    6，中国共同基金Wind基金评级，CMFundWindRating

-------------
参考资料：
1，研报：基金仓位与风格估计模型及最新配置信息.pdf | 天风证券 吴先兴 20190107
2，定期数据：海通基金排名、中信基金仓位
2.1，海通基金周报里，sheet"股票型"有1400只，若K列剔除指数基金类别，则"主动股票开放型"有415，基本和wind里的460一致。
    N列海通评级5、4、3星各有34~35只，合起来是100只
------------
信息结构梳理{参考天风的方法}：
1，公开基金数据搜集和整理：基金季度和半年度持仓披露；
1.1，对每个基金建立obejct dict，用json保存重要信息；

2，持仓组合模拟和组合调整模拟；是否模拟拟合Top10以外的剩余持仓。
    方法：通过大、小盘规模指数、行业指数、行业龙头股对(月)仓位进行拟合。
3，日常仓位监控；在月以内的频率对潜在的调仓交易进行拟合。{idea：网格交易法}
4，特征提取&风格刻画：大小市值配置区别、市值暴露(应该是相对于中位数市值的成分股市值倍数)、行业配置、股票仓位

5，模型维护分析：
5.1，数据计算上，如果加入个股总成交金额约束、区间内模拟交易对股价影响，可能有助于最优化拟合的
结果更贴近。
5.2，按月计提管理费？

6，统计分析
6.1，股票基金分行业的仓位变动

'''