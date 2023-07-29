# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
################################################
todo： 
### Qs: 如何仿真？
### Ana：经典模型——静态：1，净值与指数拟合；2，持仓的拟合。问题：反映历史，而不是未来
### Idea：动态模型：1，净值的变动预测；2，持仓的变动预测。key：情境分析。
last 201229 | since 200201
derived from rc_data\test_wds_data_transform_fund.py
目录：path=C:\rc_reports_cs\rc_2020论文_课题\0paper_基金持仓研究和基金分类
path=C:\ciss_web\CISS_rc\apps\fund_ana;file=0基金持仓仿真.xlsx
################################################
功能： 股票基金
    1，数据分析：梳理A股基金权益部分的收益和持仓变动
    1.1，生成区间市场分组和基金的日收益率数据并进行分析：
    1.2，导入近3次基金持仓股票数据，分析持仓股票、行业、风格特征

    2，构建预测指标：用模拟组合和股票去匹配基金未来收益率
    2.1，寻找最相关的指标：
    2.2，应用机器学习算法构建基金持仓仿真的策略，实现收益率和波动率的仿真

    3，仿真组合建模和跟踪
    3.1，组合构建：基于相关性、收益率等指标构建模拟组合，跟踪未来收益。
    3.2，组合评估：从超额收益、回撤、稳定性、不同市场环境时期的匹配度等维度评估仿真的效果
    
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
'''
#################################################################################
### Initialization，load configuration 
# Notes: 除了基本的路径和时间，其他脚本导入尽量都放在 config_XXX 里，保存再 obj 对象 
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db；# C:\ciss_web\CISS_rc\config
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

################################################################################
### 0, 导入基金配置文件，初始化基金对象
### 1，时间：对于更新时间t，确定对应季末时间T和上一季度末时间T-1
from config_fund import config_fund
config_fund_1 = config_fund()
from config_data import config_data
config_data_1 = config_data()
################################################################################
### 0，数据获取——基金,个股：...见“指标分析”，file=0基金持仓仿真.xlsx
### 数据和分析指标的计算、IO

### 根据日期，导入基金obj、基金数据df和基金持仓df
obj_in = {}
obj_in["temp_date"] = "20060801" # date_list_after_ann[0]
obj_fund,obj_fund_ana = config_fund_1.load_obj_fund_ana( obj_in )

# notes：这里的date_list_after_ann只是输入日期对应的日期序列
# date_list_after_ann [20060801, 20060831, 20061101, 20070201, 20070402]
# date_list_period [20060731, 20060830, 20061031, 20070131, 20070331]
date_list_after_ann = obj_fund_ana["dict"]["date_list_after_ann"] 
date_list_report  = obj_fund_ana["dict"]["date_list_report"]
print("date_list_after_ann", date_list_after_ann[:5])
print("date_list_period", date_list_report[:5]) 

### 导入df_fund_20070402.csv，df_fund_stock_port_20060801.csv，df_stockpool_fund_20060801.csv，df_fund_company_20180402.csv
# obj_fund_ana["df_fund"] ；obj_fund_ana["df_stockpool_fund"] ；obj_fund_ana["df_fund_stock_port"] 
# obj_fund_ana["df_fund_company"] 


#########################################
### 0,导入市场数据：对于“市场、行业、成长价值”几个维度构建的分组，回溯建立模拟组合净值，用于和基金历史净值进行匹配
### 1，给定T日，导入当日的市场分组文件abcd3d，并回溯计算过去2个季度的各组模拟组合走势，与基金净值比较。

path_temp = "D:\\db_wind\\data_adj\\ashare_ana\\"
file_name = "ADJ_timing_TRADE_DT_" + obj_in["temp_date"] + "_ALL.csv"
df_ashare_ana = pd.read_csv( path_temp + file_name  )
print("Check ...... \n", df_ashare_ana.head().T )

# abcd3d_market_ana_trade_dt_20060112.csv
path_temp = "D:\\CISS_db\\timing_abcd3d\\market_status_group\\"
file_name = "abcd3d_market_ana_trade_dt_" + obj_in["temp_date"] + ".csv"
df_market_ana = pd.read_csv( path_temp + file_name ,encoding="gbk" )
print("Check ...... \n", df_market_ana.head() )

##################################################################################
### 对于df_market_ana中的每个分组，计算模拟组合区间收益率
# 单个交易日abcd3d市场状态计算脚本在 ashares_timing_abcd3d.py
######################################### 
### 1,设置和导入数据：根据市场和行业分组方法，回溯获取分组组合过去20天、60天、120天的日收益率
# 参考脚本： obj_ana = analysis_factor_1.market_status_abcd3d_ana( obj_ana) 

from data_io_pricevol_financial import data_pricevol_financial
data_pricevol_financial_1 = data_pricevol_financial()
# 目标脚本：
obj_data={}
obj_data["dict"] ={}
obj_data["dict"]["latest_date"] = obj_in["temp_date"]
obj_data["dict"]["code_list"] = df_ashare_ana["S_INFO_WINDCODE"].to_list()
obj_data["dict"]["date_len"] = 120 
# 默认是向前取N日，date_len=120天
obj_data["dict"]["date_pre_post"] = "pre" 
### 导入历史行情和abcd3d数据 
obj_data = data_pricevol_financial_1.import_data_ashare_period_change( obj_data)
df_ashare_pctchg = obj_data["df_ashare_pctchg"]
print( df_ashare_pctchg.head() ) 
# output : obj_data["df_ashare_adjclose"],obj_data["df_ashare_pctchg"] 
# :index是股票代码，columns是[ "S_INFO_WINDCODE",20060801,...,20060203 ]
# obj_data["date_list"] = date_list

### 设置输入对象 obj_port 
obj_port ={}
obj_port["date_list"] = obj_data["date_list"]
obj_port["df_ashare_ana"] = df_ashare_ana
obj_port["df_ashare_pctchg"] = df_ashare_pctchg

######################################### 
### 区间收益率的计算过程
from algo_opt import algorithm_port_ret
algorithm_port_ret_1 = algorithm_port_ret()
### 市场分组和行业分组内个股的权重是通过固定的算法计算的，并且没有保存组合的持股，因为持仓股票应该随时间重新计算。
### 算法：algorithm_ashare_weighting_1.algo_port_return_by_weight()

# obj_port = algorithm_port_ret_1.algo_port_by_market_ana(obj_port )
# ### save to output: 
# df_port_ret = obj_port["df_port_ret"]
# df_port_ret.to_csv("D:\\df_port_ret2.csv")    


######################################### 
###TEMP 直接读取数据：read from csv 
df_port_ret = pd.read_csv("D:\\df_port_ret2.csv") 
df_port_ret.index = df_port_ret["Unnamed: 0"]   
df_port_ret = df_port_ret.drop(["Unnamed: 0"],axis=1 )  
print( df_port_ret.head() )

##################################################################################
### 获取基金对应时间区间的净值收益率 | 例子：temp_fund_code = "080001.OF"
# "F_NAV_ADJUSTED", table="ChinaMutualFundNAV",file_name="WDS_ANN_DATE_20210101_ALL.csv"
temp_fund_code = "080001.OF"

from data_io_fund_ana import data_io_fund_ana
data_io_fund_ana_1 = data_io_fund_ana()

obj_fund={}
obj_fund["if_1fund"] = 1 
obj_fund["fund_code"] = temp_fund_code 
obj_fund["date_list"] = obj_data["date_list"]
### 设置要赋值地df，index是基金名称，columns是日期
obj_fund["df_fund_ret"] = df_port_ret
### 功能包括了将基金的日受益于保存至 df_port_ret
obj_fund = data_io_fund_ana_1.import_data_fund_nav_period(obj_fund )

obj_fund["df_fund_ret"].to_csv("D:\\df_port_ret_new.csv")

###TEMP 直接读取数据：read from csv 
df_port_ret = pd.read_csv("D:\\df_port_ret_new.csv") 
df_port_ret.index = df_port_ret["Unnamed: 0"]   
df_port_ret = df_port_ret.drop(["Unnamed: 0"],axis=1 )  
print( df_port_ret.head() )

##################################################################################
### 仿真算法： 
### STRA 1：从市场分组构建仿真组合
##################################################################################
### Cal：计算过去20、60、120天基金收益率和所有市场、行业组合的关系：
from performance_eval import  perf_eval_ashare_port
perf_eval_ashare_port_1 = perf_eval_ashare_port()
### 新建输入对象 obj_perf_eval
obj_perf_eval = {} 

# notes:1,收益率是小数，例如 0.015，而不是百分比 1.5
# 2,矩阵转置是为了df按列计算相关性。 
# 3,df_port_ret的index列是升序排列的日期，columns是不同的基金组合或市场、行业、主题分组
df_port_ret = df_port_ret.T/100
obj_perf_eval["df_port_ret"] = df_port_ret

obj_perf_eval["port_name"] = temp_fund_code 

######################################### 
### 计算不同区间的累计收益率和最大回撤
obj_perf_eval = perf_eval_ashare_port_1.perf_eval_port_ret(obj_perf_eval)

# 量化回测过程中常用到的指标有年化收益率、最大回撤、beta、alpha、夏普比率、信息比率等
df_port_perf_eval = obj_perf_eval["df_port_perf_eval"]
df_corr = obj_perf_eval["df_corr"] 

df_port_perf_eval.to_csv("D:\\df_port_perf_eval.csv")

##################################################################################
### MATCH:按照匹配算法多维度打分，筛选相关性最大的基金
'''打分依据：不同于原来看1或2年的长期业绩或相关性，我们看中短期的指标；
序号，权重，数值方向，指标，命名方式
1，40%，越大越好，累计收益率："ret_end_" + [long,mid,short]
2，30%，越小越好，累计最大回撤 "mdd_short" [long,mid,short] ；
3，15%，越小越好，高点开始最大跌幅，"mdd_fromhigh_long","mdd_fromhigh_mid"
4，15%，越大越好，低点开始最大涨幅, "ret_fromlow_long","ret_fromlow_mid"
5，作为对比：100%，越大越好，相关性：选相关性最高的个组合加权。长短中期
加权方式：
【长期、中期、短期】= 30%，40%，30%
最后结果：
1，筛选：对不同的组合进行赋权，指标前5名的取值5~1；最后对所有组合进行多个指标加权，取前5名
2，加权：对前五名采用总分加权地方式计算权重。
指标：skill_ret:收益率序列学习率  评估：观察拟合结果和目标基金在未来1~2季度的匹配程度
'''
from algo_opt import algorithm_port_weight
algorithm_port_weight_1 = algorithm_port_weight()
### 1,新建obj_port，设置需要配置的指标大类权重和小类权重
obj_port ={} 
dict_weight_indi = {}
### 新建指标权重的df，long,mid,short 
### 1，40%，越大越好，累计收益率："ret_end_" + [long,mid,short]
dict_weight_indi[ "ret_end" + "_" + "long" ] = 0.4 * 0.3
dict_weight_indi[ "ret_end" + "_" + "mid" ] = 0.4* 0.4
dict_weight_indi[ "ret_end" + "_" + "short" ] = 0.4 * 0.3
### 2，30%，越小越好，累计最大回撤 "mdd_short" [long,mid,short] ；
dict_weight_indi[ "mdd" + "_" + "long" ] = 0.3 * 0.3
dict_weight_indi[ "mdd" + "_" + "mid" ] = 0.3 * 0.4
dict_weight_indi[ "mdd" + "_" + "short" ] = 0.3 * 0.3
### 3，15%，越小越好，高点开始最大跌幅，"mdd_fromhigh_long","mdd_fromhigh_mid"
dict_weight_indi[ "ret_fromlow" + "_" + "long" ] = 0.15 * 0.3
dict_weight_indi[ "ret_fromlow" + "_" + "mid" ] = 0.15 * 0.4
dict_weight_indi[ "ret_fromlow" + "_" + "short" ] = 0.15 * 0.3
### 4，15%，越大越好，低点开始最大涨幅, "ret_fromlow_long","ret_fromlow_mid"
dict_weight_indi[ "mdd_fromhigh" + "_" + "long" ] = 0.15 * 0.3
dict_weight_indi[ "mdd_fromhigh" + "_" + "mid" ] = 0.15 * 0.4
dict_weight_indi[ "mdd_fromhigh" + "_" + "short" ] = 0.15 * 0.3

##########################################
### 5，作为对比：100%，越大越好，相关性：选相关性最高的个组合加权。长短中期
# dict_weight_indi_2 = {}
# ### 新建指标权重的df，long,mid,short 
# ### 1，40%，越大越好，累计收益率："ret_end_" + [long,mid,short]
# dict_weight_indi_2[ "ret_end" + "_" + "long" ] = 0.25 *0.33
# dict_weight_indi_2[ "ret_end" + "_" + "mid" ] = 0.25 *0.33
# dict_weight_indi_2[ "ret_end" + "_" + "short" ] = 0.25 *0.33
# ### 2，30%，越小越好，累计最大回撤 "mdd_short" [long,mid,short] ；
# dict_weight_indi_2[ "mdd" + "_" + "long" ] = 0.25 *0.33
# dict_weight_indi_2[ "mdd" + "_" + "mid" ] = 0.25 *0.33
# dict_weight_indi_2[ "mdd" + "_" + "short" ] = 0.25 *0.33
# ### 3，15%，越小越好，高点开始最大跌幅，"mdd_fromhigh_long","mdd_fromhigh_mid"
# dict_weight_indi_2[ "ret_fromlow" + "_" + "long" ] = 0.25 *0.33
# dict_weight_indi_2[ "ret_fromlow" + "_" + "mid" ] = 0.25 *0.33
# dict_weight_indi_2[ "ret_fromlow" + "_" + "short" ] = 0.25 *0.33
# ### 4，15%，越大越好，低点开始最大涨幅, "ret_fromlow_long","ret_fromlow_mid"
# dict_weight_indi_2[ "mdd_fromhigh" + "_" + "long" ] = 0.25 *0.33
# dict_weight_indi_2[ "mdd_fromhigh" + "_" + "mid" ] = 0.25 *0.33
# dict_weight_indi_2[ "mdd_fromhigh" + "_" + "short" ] = 0.25 *0.33

##########################################
### INPUT
obj_port["port_name"] = temp_fund_code
obj_port["dict_weight_indi"] = dict_weight_indi
# 删除不需要的columns 
obj_port["df_port_perf_eval"] = df_port_perf_eval.loc[:, list(dict_weight_indi.keys()) ]

### OUTPUT
obj_port = algorithm_port_weight_1.algo_port_weight_by_indicator(obj_port)
# obj_port["df_port_perf_eval"] 里 多了一列 "score",根据得分取前5名的组合对基金做拟合。
print("Comprehensive score for portforlio: \n",print( obj_port["df_port_perf_eval"].head().T ) )
# 综合得分前5的组合 obj_port["list_port"] 
obj_port["df_port_perf_eval"].to_csv("D:\\df_port_perf_eval_2.csv")
df_port_perf_eval = obj_port["df_port_perf_eval"]
### 构建要计算的组合列表 port_list 
df_port_perf_eval =df_port_perf_eval.sort_values(by="score",ascending=False  )
# notes:通常匹配度最高的都是行业的细分组合，例如industry_41_mvtotal_30p，industry_30_growth，industry_36_mvtotal_30p
# df_port_perf_eval前5行是用市场分组方法market_ana计算出来匹配度最高的5个组合，index是组合的名称
print("portfolio name by marekt ana method  ", df_port_perf_eval.index[0] )
port_list_selected = df_port_perf_eval.index[:5 ] 

################################################################################
### 得到一个由0~5个市场或行业分组的组合构成的模拟组合，用于拟合目标基金或组合未来1-2季度的净值
### 获取 df_port_perf_eval前五名的组合；Qs:如何定位选出组合的持仓股票？
# notes: 市场分组和行业分组内个股的权重是通过固定的算法计算的，并且没有保存组合的持股，因为持仓股票应该随时间重新计算。
# 算法：algorithm_ashare_weighting_1.algo_port_return_by_weight()
# 进度：line 578， def algo_port_return_by_weight(self, obj_port_ret_by_rank ):
obj_port = {} 
###################################
### 获取未来120天股票收益率
obj_data={}
obj_data["dict"] ={}
obj_data["dict"]["latest_date"] = obj_in["temp_date"]
obj_data["dict"]["code_list"] = df_ashare_ana["S_INFO_WINDCODE"].to_list()
obj_data["dict"]["date_len"] = 120 
# 默认是向前取N日，date_len=120天
obj_data["dict"]["date_pre_post"] = "post" 
### 导入历史行情和abcd3d数据 
obj_data = data_pricevol_financial_1.import_data_ashare_period_change( obj_data)
df_ashare_pctchg = obj_data["df_ashare_pctchg"]

# df_ashare_pctchg.to_csv("D:\\df_ashare_pctchg_210214.csv")
###################################
### 导入算法模块
from algo_opt import algorithm_port_ret
algorithm_port_ret_1 = algorithm_port_ret()
###################################
### 设置组合变量
obj_port ={}
obj_port["date_list"] = obj_data["date_list"]
obj_port["df_ashare_ana"] = df_ashare_ana
obj_port["df_ashare_pctchg"] = df_ashare_pctchg

count_port_ret = 0 
for i in range (5) :
    obj_port["port_name_market_ana"] = df_port_perf_eval.index[ i ]
    # 例如industry_41_mvtotal_30p，industry_30_growth，industry_36_mvtotal_30p
    ######################################### 
    # ### 计算过程
    # ### 市场分组和行业分组内个股的权重是通过固定的算法计算的，并且没有保存组合的持股，因为持仓股票应该随时间重新计算。
    # ### 算法：algorithm_ashare_weighting_1.algo_port_return_by_weight()
    obj_port_ret_by_rank = algorithm_port_ret_1.algo_port_by_port_id(obj_port )
    # notes：输出的组合可能有多个行业细分组合，只保留需要的那一个。
    # notes:df_port_ret_post的index会有重复的index值，需要删除
    if count_port_ret == 0 : 
        df_port_ret_post = obj_port_ret_by_rank["df_port_ret"]
        count_port_ret = count_port_ret + 1 
    else :
        df_port_ret_post = df_port_ret_post.append( obj_port_ret_by_rank["df_port_ret"] )
    print("Debug=====3", df_port_ret_post )

###################################
### 只保留前5的组合
df_port_ret_post = df_port_ret_post.loc[ port_list_selected , : ]
# notes:df_port_ret_post的index会有重复的index值，需要删除
df_port_ret_post = df_port_ret_post[ ~ df_port_ret_post.index.duplicated(keep= "first")]

###################################
### 计算5个组合的加权收益率"port_simu"
print("df_port_ret_post,check duplicated index \n",  df_port_ret_post[ df_port_ret_post.index.duplicated()]  )

temp_sum = 0 
# df_port_ret_post.loc["port_simu", : ] =0.0
for i in range (5) :
    temp_weight =  float( df_port_perf_eval["score"].values[ i ] )
    
    temp_port = df_port_perf_eval.index[ i ]
    print("Debug====,", temp_weight , "temp_port ",temp_port   )

    if temp_sum == 0 :
        df_port_ret_post.loc["port_simu",: ] =temp_weight* df_port_ret_post.loc[ temp_port ,: ] 
    else :
        df_port_ret_post.loc["port_simu",: ] =df_port_ret_post.loc["port_simu",: ]+ temp_weight* df_port_ret_post.loc[ temp_port ,: ] 
    temp_sum = temp_sum + temp_weight

df_port_ret_post.loc["port_simu",: ] = df_port_ret_post.loc["port_simu",: ] /temp_sum

# df_port_ret_post.to_csv("D:\\df_port_ret_post.csv")

##################################################################################
### 读取基金未来120天收益率，与上述组合进行比较
obj_fund={}
obj_fund["if_1fund"] = 1 
obj_fund["fund_code"] = temp_fund_code 
obj_fund["date_list"] = obj_data["date_list"]
### 设置要赋值地df，index是基金名称，columns是日期
obj_fund["df_fund_ret"] = df_port_ret_post
# notes:所有日期columns会变成str格式
obj_fund = data_io_fund_ana_1.import_data_fund_nav_period(obj_fund )
#  df_fund_ret_post 后边会用到
df_fund_ret_post = obj_fund["df_fund_ret"]
df_fund_ret_post.to_csv("D:\\df_fund_ret_post.csv")

##################################################################################
### 根据区间收益率，计算绩效指标，skill_ret |skill_ret:收益率序列学习率 评估：观察拟合结果和目标基金在未来1~2季度的匹配程度 
obj_perf_eval_post = {} 

# notes:1,收益率是小数，例如 0.015，而不是百分比 1.5；2,矩阵转置是为了df按列计算相关性。 
# 3,df_fund_ret_post的index列是升序排列的日期，columns是不同的基金组合或市场、行业、主题分组
df_fund_ret_post = df_fund_ret_post.T/100
obj_perf_eval_post["df_port_ret"] = df_fund_ret_post
obj_perf_eval_post["port_name"] = temp_fund_code 
######################################### 
### 计算不同区间的累计收益率和最大回撤
obj_perf_eval_post = perf_eval_ashare_port_1.perf_eval_port_ret(obj_perf_eval_post)

# 量化回测过程中常用到的指标有年化收益率、最大回撤、beta、alpha、夏普比率、信息比率等
df_port_perf_eval_post = obj_perf_eval_post["df_port_perf_eval"]
df_corr_post = obj_perf_eval_post["df_corr"] 
df_port_perf_eval_post.to_csv("D:\\df_port_perf_eval_post.csv")
df_corr_post.to_csv("D:\\df_corr_post.csv")

# TODO,设计skill_ret 指标的计算，应该属于绩效评估的一部分
### 设置长中短期权重，不同指标权重
dict_weight = {} 
dict_weight["weight_period"] = {}
dict_weight["weight_period"]["long"] = 0.3
dict_weight["weight_period"]["mid"] = 0.4
dict_weight["weight_period"]["short"] = 0.3
dict_weight["weight_indi"] = {}
dict_weight["weight_indi"]["ret_end"] = 0.35
dict_weight["weight_indi"]["mdd"] = 0.25
dict_weight["weight_indi"]["ret_fromlow"] = 0.1
dict_weight["weight_indi"]["mdd_fromhigh"] = 0.1
dict_weight["weight_indi"]["sharp"] = 0.2

obj_perf_eval_post["dict_weight"] = dict_weight
obj_perf_eval_post = perf_eval_ashare_port_1.perf_eval_skill_ret(obj_perf_eval_post)
dict_weight_indi = obj_perf_eval_post["dict_weight_indi"] 
df_port_perf_eval = obj_perf_eval_post["df_port_perf_eval"] 
# notes:skill_ret 指标越小越好
df_port_perf_eval.to_csv("D:\\df_port_perf_eval_skill2.csv")
print("Skill_ret calculation done ", df_port_perf_eval.loc[:, "skill_ret" ]   )

##################################################################################
# TODO，skill_stock:持仓个股学习率  评估：观察拟合结果和目标基金在未来1~2季度的匹配程度



##################################################################################
### 仿真算法： skill_stock
### STRA 2：根据个股选择和加权方法构建组合，通过基金排名、股票特征、股票行业分布等维度



asd 
