# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
todo
1,2，财务数据量化：


功能
1,从wds相关数据表计算各类因子指标

last 200517 | since 200513



------------
计算的指标来源：sheet=常用因子列表,file=abm_factors_manage.xlsx

------------
notes:


分析：
1，A股滚动预期数据：
测试一下妙可蓝多的一致预测数据能否满足我们的需要：2019年内，股价在9月初达到高点后持续回调至12月底；
观察净利润FY1的变动，19年底以来可以分为3个区间：1，191205到200220，区间内净利润FY1值一直在下降，但股价从13.09上涨到18.09；
2，200220至200430,0429的净利润FY1值从0.3e大幅上升到1.43e，0430净利润FY1值出现下降，2日收盘价接近，区间内股价从18.09上涨至26.86；
3，200430至200506，0506出现上升，5-6当日股价上涨，5-6至0514股价从28.25上涨至33.05；
如果看未来12个月FTTM的指标，发现NET_PROFIT变动较频繁，191120首次出现超过4%的净利润上调，191220出现5.8%的上调，其余时间在+/-3.9%波动；0115-0120有较频繁的+/-10%幅度变动；
看NET_PROFIT-FY2，200107发生-14%的下降；0220、0325发生2次3%左右的上升；0429因"BENCHMARK_YR"从20181231变为20191231，发生101%的增加，即便是和之前FY2的值比较，也是大幅上升，之后2周股价也确实高位继续上涨
其他指标应用价值不大，特别是ROE、EST_OPER_PROFIT、EST_OPER_COST，在200107-200429期间持续下降，反应严重滞后；其他指标存在缺失值的问题
code=伊利600887：
相比之下，同行业的伊利600887在191101-200514期间股价上下振幅仅10%，滚动预测净利润FY1、FTTM基本上没有出现+/-1%的变动，这和股价的稳健匹配。
不过可以发现1911,1912两月预期净利润逐渐小幅从5.0e上涨到5.35e；日常调升服务在0.04%至0.07%之间
code=北方华创002371.SZ:
20191101-20200514区间：191117，1202，1223都只有0.4%不到的调高，期间股价从71上涨到82左右；
200114出现1.06%的调高，前2日股价刚涨停且破100.0；0223出现第一次-1%幅度的下降，此时距离股价季度顶部0225仅有2天；0305再次发生-2.5%的调降，之后1个月股价基本处于震荡状态；
FTTM在200425、0426分别变动0.6%和-2.0%，之后2周股价从133上涨到155。
数据的反应过程：基本面事件 -->财务数据披露 --> 卖方-预期数据变动 --> 买方-投资决策 --> 股价变动 --> 买方-持仓变动
小结: 002371.SZ:对FTTM-NET_PROFIT 进行持续跟踪有助于发掘股票的上涨机会,特别是注意调升幅度超过0.3%和持续调升超过0.9%的情况；注意持续调减和调减幅度达到-1%的
600882.SH妙可蓝多，200106,0107的FY1超过-6%的下降对应0107股价涨停，反而预示着投资机会；之后0220,0325两次+6%的调升对应了上涨的开始和中间时期；FTTM相邻2天正负值也是后续上涨的暗示。
数据：AShareConsensusRollingData（Wind一致预测个股滚动指标）是根据AShareConsensusData（中国A股盈利预测汇总）每日滚动生成的。其中类型为FY1、FY2、FY3是每日变化的,
而类型为FY0，是根据年报实际披露数据或者快报实际披露数据生成的，而非每日滚动，所以类型为FY0的每股收益需要根据当日的预测基准股本综合值重新计算。
FY0是实际公布值，相对比较准确，其次FY1、FY2的数据覆盖度比较广，所以用FY1、FY2、FY0的数据计算比较好

2，业绩快报，
梳理季度财务数据：业绩快报：主要内容包括当年及上年同期主营业务收入、主营业务利润、利润总额、净利润、总资产、净资产、每股收益净资产收益率等数据和指标，同时披露比上年同期增减变动的百分比，对变动幅度超过30%以上的项目，公司还应当说明原因。
业绩预告：上交所：对于年度报告，如果上市公司预计全年可能出现亏损、扭亏为盈、净利润较前一年度增长或下降50%以上（基数过小的除外），1-31前，不强制其他情况披露。
 深交所-主板：扭亏、亏损、净利润同比上下变动50%,净资产为负或收入小于1kw，不强制其他情况披露，Q1-4的时间0415，0715，1015，0131 ；中小板：所有公司都披露，年报不晚于3-31，其他季度报告要披露年初到下一季度；
 创业板：所有公司都披露，一季度4-10前，半年度，7-15，三季度，10-15，年报，0131；

时间：
对于REPORT_PERIOD=20200331，ANN_DT公告日可能从0408-0512不等，规定最晚0430披露，0501-0512期间有不少是st股或小股票，但也不排除会有重要的公司。

wds表格分类：
1，简要历史和ttm财务指标：中国A股日行情估值指标[AShareEODDerivativeIndicator]；每日更新
2，历史财务指标：中国A股财务指标[AShareFinancialIndicator]；（不如前者）中国A股财务衍生指标表[AShareFinancialderivative]没有披露日期，只有财务开始和截止日ENDDATE
2.1，中国A股财务指标：
2.2，中国A股财务衍生指标表：
3，TTM和MRQ数据：中国A股TTM与MRQ，AShareTTMAndMRQ ；例如：销售费用(TTM)，管理费用(TTM)，财务费用(TTM)

wds说明:
0,一致预测算法：核心指标：每股收益、净利润、营业收入；辅助指标：利润总额、毛利、营业成本、 EBIT、 EBITDA、 ROE、 ROA、每股净资产、每股股利、每股现金流
1，AIndexConsensusData(一致预测指数指标)是股票指数中所有成分股的周期180天的一致预期，根据整体法进行计算，每日滚动生成。常见问题:Q1：指数中成分股未有一致预测数据，是如何计算的？A1：AIndexConsensusData（Wind一致预测指数指标）是根据各成分股AShareConsensusData（中国A股盈利预测汇总）每日滚动生成的。因为券商不是所有成分股都有预测，所以有些成分股是没有一致预测数据。没有一致预测数据成分股是根据最近12个月的TTM值替代，该预测数据只能作为指数估值时的测算数据，不能作为该股票的预测值,只有FY1会根据最近12个月TTM值计算。
2，AIndexConsensusRollingData(一致预测指数滚动指标)是股票指数中所有成分股的周期180天的一致预期，根据整体法进行计算。每日滚动生成，会根据一致预期数据衍生未来12个月一致预期、增长率等指标。

1,预期数据的价值：
1.1,Q1:在1季度数据出来前炒预期，这时看一季度Q4预期数据和上一年末Q4预期数据的变动率值，Q2看的是一季度后数据的延续和稳定性，
1.2,组合权重：影响个股相对于市场的最优权重变动，影响个股相对于行业的最优权重变动，相对于行业锚个股的最优权重变动。||
1.3,净利润同比增长率选股效果最优，一致预期主营业务收入增长率因子优于一致预期净利润增长率因子；综合考虑因子有效性和相关性，将当季净利润同比增长率、 3年主营业务收入复合增长率、未来1年营业收入一致预期增长率纳入到选股指标体系中。


Idea：
1，转换OPDATE，变成另一种ann_date，因为部分财务表格里没有ann_dt但是opdate经常会变更；
2，考察wds-历史财务-季度现金里筹资现金流入，负债的增加占总资产的比例等指标，基于这些数据判断企业是否处于资本开始较大的时期。
3，每周，搜集一篮子财务数据，并用exp平均加权分析股价涨跌和指标x的相关性，提取相关性提升的指标x，并测试历史上该指标相关性提升的稳定性、持续性；如果满足一定条件则返回该指标。

'''
#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

from data_io import data_factor_model
data_factor_model1 = data_factor_model()

#################################################################################
### 给定交易日，计算当日交易的A股
#################################################################################
### 行业、主题、指数分类


#################################################################################
### 行情数据


#################################################################################
### 财务和预期数据
'''步骤：
1，导入包括所有columns的csv文件；注意其中部分是代码和日期、行业分类、指标参数（rollingtype）等
2，对每一个表里提取列，判断列值是否需要加工，是否需要除以总市值或其他；判断预测股价涨跌的方向；

input：
1，多个wds财务相关的表格，
    file=list_financial_columns_wds.csv;path=C:\ciss_web\CISS_rc\apps\active_bm
2，wds表格的部分columns：
"no.","f_class_1","f_class_2","f_name","f_detail","f_algorithm","f_cariable","column_type","table_fi_type","taoble_name_cn","taoble_name","column_name_CN","column_name"
3，column_type：0对应的是基础数据如代码、日期、行业等；1对应的是具体的原始指标值；2是指标类型例如FY1，FTTM等；3是其他
'''
#################################################################################
### step1,导入相关的财务表格列表和相关数据
obj_in = {}
obj_in["dict"] = {}
obj_in["dict"]["file_name_columns_wds"] = "list_financial_columns_wds.csv"
obj_in["dict"]["path_name_columns_wds"] = "active_bm\\"
obj_in = data_factor_model1.import_data_wds(obj_in )

#################################################################################
### step2,对每个表格内的财务指标，计算单个指标的历史月度回测绩效
# 对市场全部股票，按交易金额和流通市值分300、500、1000、其他四个组。

# todo：导入全市场有交易的股票信息；AShareEODPrices
# todo：ann_date划分的数据里还要和 报告日期对应，季度、年度等

asd

temp_path = "D:\\db_wind\\data_wds\\AShareConsensusRollingData\\"
file_name = "WDS_OPDATE_20190101_20200513.csv"

df_esti = pd.read_csv( temp_path + file_name )
temp_code = "600398.SH"
df_esti_sub = df_esti [df_esti["S_INFO_WINDCODE"]== temp_code ]

print("df_esti_sub" ,df_esti_sub.head().T )
print("df_esti_sub" ,df_esti_sub.tail().T )

df_esti_sub.to_csv("D:\\df_esti_sub_"+ temp_code +".csv")

asd









#################################################################################
### 


#################################################################################
### 


'''1,预期数据的价值：
1.1,Q1:在1季度数据出来前炒预期，这时看一季度Q4预期数据和上一年末Q4预期数据的变动率值，Q2看的是一季度后数据的延续和稳定性，
1.2,组合权重：影响个股相对于市场的最优权重变动，影响个股相对于行业的最优权重变动，相对于行业锚个股的最优权重变动。||
1.3,净利润同比增长率选股效果最优，一致预期主营业务收入增长率因子优于一致预期净利润增长率因子；综合考虑因子有效性和相关性，将当季净利润同比增长率、 3年主营业务收入复合增长率、未来1年营业收入一致预期增长率纳入到选股指标体系中。
'''











#################################################################################
### 














