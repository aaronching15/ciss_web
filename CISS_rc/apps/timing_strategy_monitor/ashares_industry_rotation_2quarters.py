# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
数据更新：
1，需要abcd3d_timing的数据表
2，需要在披露截止日下载table=AShareFinancialIndicator，keyword=	REPORT_PERIOD

todo: 
3, 统计分析分行业和分流通市值、成交额不同组的收益差异
4，必须消费产业链；1，过去2个季度财务数据选股；2，择时配置：diff_timing= timing_mvfloat - timing_ew ；
5,生物医药产业链
6，对于多个财务指标，应该采取总分制，对个别指标提高容忍度，因为再好的公司也会在某些方面有瑕疵。追求完美从来不是现实中的最优解

--------------------------------------------
功能：基于过去2个季度财务数据选股
1，过去2个季度财务数据选股；
2，择时配置：diff_timing= timing_mvfloat - timing_ew ；
steps：
1，T日，选取中信一级行业中：31	商贸零售；32，消费者服务；36，食品饮料；37，农林牧渔 
2，导入和获取各类财务数据：roe，roic，净利润增长、行业景气、估值；季报披露截止节点：0430-0501，0831-0901，1031-1101
2.1，天风-景气成长条件：
    1,roe：季度roe_ttm位于行业前30%，最近2季度roe环比增长;
    2,收入：当季营收增速>0%,环比变动>-15%,毛利率环比提升；
    3，净利润：扣非增速位于 0%~400%，环比变动>-15%;
    4,盈利持续性：G -roe(1-D)> 20%
    5,资产质量：造假模型阈值<0.0038,负债率<80%,商誉/总资产<20%,经营性现金流ttm>0;PE_FY1<80;市值所处行业分位数>30%
2.2,ROIC越高，证明企业资金使用效果越好，回报率越高。国投瑞银品牌优势混合拟任基金经理孙文龙认为，高ROIC企业多数为品牌企业，这类企业通过构筑深深的护城河来获取消费者剩余，为股东创造价值。护城河（包括产品力、品牌、渠道、供应链等）足够深的品牌企业，能够抵御行业突发事件及行业景气下行，在一个接一个的景气周期中拾阶而上。
    1，景气向上的行业，预测未来2-3年收入及利润增速在15%-20%以上；增长率：挑选行业空间大、可以持续提高市场份额，三年净利润复合增长高，净利润3-4年可以翻倍的标的；
    2，以ROIC为标准构建组合：以ROIC>10%的股票标的为主；
    3，择时，估值：买入时的估值对未来持有3年的投资回报有重要的影响，预期3年后估值不能明显收缩。
ref:
1, 天风证券策略-财务模型明细：file=财务指标_天风策略_刘晨明_20200505.txt;path=C:\zd_zxjtzq\rc_reports_cs\量化多因子_财务指标
    file=5月换仓以来超额收益显著——关于选股模型细节的进一步说明.pdf
2,国投瑞银孙文龙，ROIC选股；url=https://www.wdzj.com/hjzs/ptsj/20181013/833258-1.html
todo：生物医药产业链：35	医药
--------------------------------------------
last 200901| since 200518 
derived from ashares_timing_profit_express_notice.py
notes: 
1，曾用名：ashares_timing_financial_ana.py
想法：
1，对于多个财务指标，应该采取总分制，对个别指标提高容忍度，因为再好的公司也会在某些方面有瑕疵。追求完美从来不是现实中的最优解


分析：
AShareConsensusRollingData[Wind一致预测个股滚动指标]中FY0数据的分析：
002371.SZ,20191231-20200522
1，200305当天002371.SZ发布了2019年报的业绩快报，
这一天FY0里的基准年BENCHMARK_YR从2018变成2019，记录对应的OPDATE是"2020/3/6 18:08:10"
观察股票价格，行情在191204-200226上涨，0227-0305下跌，0305-0428区间震荡，0428-0521上涨。
小结：之前40个交易日有较大上涨，因此之后的1个月消化估值。
2，200425当天002371.SZ发布了2019年年报，数字上和快报没有区别。
2，200425当天002371.SZ发布了2020一季度业绩快报，净利润和营业收入增长率均大幅下降；

603129.SH,20191231-20200522
200117，年报业绩预告，同比增长42%~62%；股价结束过去5个月190823-200117从21.28上涨至50.68，开始0117-0407的下跌；
    190823的主要事件是当日披露2019中报，
200421，披露年报，同比增长；0407-0421大跌后小幅反弹，0421-0428震荡
200424，披露一季报，同比增长，0428-0525上涨一波。

数据1：中国A股业绩预告 AShareProfitNotice\WDS_S_PROFITNOTICE_DATE_20200521_ALL.csv
S_INFO_WINDCODE	002683.SZ	300763.SZ
S_PROFITNOTICE_DATE	20200325	20200325
S_PROFITNOTICE_PERIOD	20200331	20200331
业绩预告类型代码 S_PROFITNOTICE_STYLE	454003000	454010000
是否变脸 S_PROFITNOTICE_SIGNCHANGE	0	0
预告净利润变动幅度下限（%） S_PROFITNOTICE_CHANGEMIN	15	759.07
预告净利润变动幅度上限（%） S_PROFITNOTICE_CHANGEMAX	35	788.69
预告净利润下限（万元） S_PROFITNOTICE_NETPROFITMIN	3366	5800
预告净利润上限（万 S_PROFITNOTICE_NETPROFITMAX	3951	6000
S_PROFITNOTICE_NUMBER	1	1
S_PROFITNOTICE_FIRSTANNDATE	20200325	20200325
S_PROFITNOTICE_ABSTRACT	预计:净利润3366-3951	预计:净利润5800-6000
OPDATE	2020/3/24 20:10	2020/3/24 20:10
OPMODE	0	0

notes: 业绩预告类型代码 S_PROFITNOTICE_STYLE的数值类型：
    不确定 454001000 略减 454002000 略增 454003000 扭亏 454004000 其他 454005000 
    首亏 454006000 续亏 454007000 续盈 454008000 预减 454009000 预增 454010000
------
数据2：中国A股业绩快报,AShareProfitExpress\WDS_ANN_DT_20200417_ALL.csv
S_INFO_WINDCODE	002473.SZ	000587.SZ
ANN_DT	20200417	20200417
REPORT_PERIOD	20191231	20191231
营业收入(元) OPER_REV	94816415.29	4233894763 
营业利润(元) OPER_PROFIT	-91313356.59	-6060413423
利润总额(元) TOT_PROFIT	-125966564.6	-6061524129
净利润(元) NET_PROFIT_EXCL_MIN_INT_INC	-128917261.5	-5934986345
TOT_ASSETS	235547392	22398183932
股东权益合计(不含少数股东权益)(元) TOT_SHRHLDR_EQY_EXCL_MIN_INT	169123920	366434532.4
EPS_DILUTED	-0.8057	-2.79
净资产收益率-加权(%) ROE_DILUTED	-13.87	-177.46
S_ISAUDIT	0	0
YOYNET_PROFIT_EXCL_MIN_INT_INC	12080879.86	-3099340412
OPDATE	2020/4/16 18:06	2020/4/16 18:06
------
todo，数据3：中国A股季报；中国A股TTM指标历史数据，AShareTTMHis
    营业总收入(TTM) TOT_OPER_REV_TTM
    营业收入(TTM) OPER_REV_TTM
    净利润(TTM) NET_PROFIT_TTM
    归属于母公司的净利润(TTM) NET_PROFIT_PARENT_COMP_TTM
    股东权益(MRQ) S_FA_TOTALEQUITY_MRQ
    归属于母公司的股东权益(MRQ) S_FA_EQUITY_MRQ 
    负债合计(MRQ) S_FA_DEBT_MRQ
------------
假设：
1，季度财务数据发布后40天内会有调整和上涨的可能；
2，业绩预告目标：分析业绩状态：负面：首亏、续亏、预减；正面：预增、扭亏

逻辑：
1，T日，计算近40天内发布业绩预告和快报的个股，搜集指标：预告发布日期date_notice、财务季度REPORT_PERIOD、预告类型notice_style、净利润变动幅度下限PROFITNOTICE_CHANGEMIN
    快报：快报发布日期date_notice、财务季度REPORT_PERIOD、营业利润(元) OPER_PROFIT，净利润(元) NET_PROFIT_EXCL_MIN_INT_INC，净资产收益率-加权(%) ROE_DILUTED
2，计算股价从报告发布日A日到T日的区间内，股价最高和最低涨跌幅，最新股价从mdd日后第5天开始的累计涨跌幅。

参考资料：
1，20200523-天风证券-天风证券策略？财务模型：5月换仓以来超额收益显著——关于选股模型细节的进一步说明.pdf
-------
中信一级行业
10	石油石化 11	煤炭 12	有色金属 20	电力及公用事业 21	钢铁
22	基础化工 23	建筑 24	建材 25	轻工制造 26	机械 27	电力设备及新能源
28	国防军工 30	汽车 31	商贸零售 32	消费者服务  33	家电 34	纺织服装
35	医药 36	食品饮料 37	农林牧渔
40	银行 41	非银行金融 42	房地产 43	综合金融 50	交通运输 
60	电子 61	通信 62	计算机 63	传媒 70	综合
'''

########################################################################
### Initialization 
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

from data_io import data_io
from data_io_pricevol_financial import data_pricevol_financial
from data_io_factor_model import data_factor_model
data_io_1 = data_io()
data_pricevol_financial_1 = data_pricevol_financial()
data_pricevol_financial_1.print_info()
data_factor_model_1 = data_factor_model()
data_factor_model_1.print_info()

from signals import signals_ashare
signals_ashare_1 = signals_ashare()

from analysis_indicators import analysis_factor
analysis_factor_1 = analysis_factor()

from performance_eval import perf_eval_ashare_stra
perf_eval_ashare_stra_1 = perf_eval_ashare_stra()
from algo_opt import algorithm_ashare_weighting
algorithm_ashare_weighting_1 = algorithm_ashare_weighting()

########################################################################
### 设置组合id，是否单一行业"single_industry"，开始和结束日期
obj_financial = {}
obj_financial["dict"] ={}
# "rc_2005"
# input("Type in id for portfolio,such as rc_2005:") 
# obj_financial["dict"]["id_output"] = "rc_0605_med_tail"
### "single_industry"设定是否对单一行业进行测算：35	医药
'''中信一级行业
10	石油石化 11	煤炭 12	有色金属 20	电力及公用事业 21	钢铁
22	基础化工 23	建筑 24	建材 25	轻工制造 26	机械 27	电力设备及新能源
28	国防军工 30	汽车 31	商贸零售 32	消费者服务  33	家电 34	纺织服装
35	医药 36	食品饮料 37	农林牧渔
40	银行 41	非银行金融 42	房地产 43	综合金融 50	交通运输 
60	电子 61	通信 62	计算机 63	传媒 70	
设置长周期或短周期必选行业：如：22.0,24.0,25.0,27.0,30.0,34.0,40.0,41.0,42.0,63.0
'''
print("设置长周期或短周期必选行业：如：22.0,24.0,25.0,27.0,30.0,34.0,40.0,41.0,42.0,63.0  ")
input_ind = input("Type in industry code level1，如果取全市场，则把下行注释 :" )
input_ID = input("Type in ID and rc_2020 as default")

if len(input_ID ) < 4 : 
    input_ID = "rc_2020"
    
# 若单个一级行业，则取行业内前50%的个股;否则默认取前30%的个股
if len( input_ind) >1 :
    obj_financial["dict"]["id_output"] = input_ID+ "_indcode_" + str(int( input_ind )) 
    ### Notes:如果取全市场，则把下行注释
    obj_financial["dict"]["single_industry"] = int( input_ind )

else :
    obj_financial["dict"]["id_output"] = input_ID+ "_indcode_ALL"
    obj_financial["dict"]["single_industry"] = -1



### 若单个一级行业，则取行业内前50%;默认前30%；若indi_quantile_tail=1, 则取尾部指标值，默认值0取指标最大的。
obj_financial["dict"]["indi_quantile_tail"] = 0 # 1
obj_financial["dict"]["date_start"] = "20200531" # input("Type in date start such as 20151101: ")
obj_financial["dict"]["date_end"] ="20200908" # input("Type in date start such as 20190506: ")

########################################################################
### 导入区间日期数据,每年的定期调整时间是 0430,0731,1030三个时间之后的第一个交易日
# date_list_after_ann就是各个季度财务披露日之后第一个交易日
obj_financial = data_io_1.get_after_ann_days( obj_financial )
date_list_after_ann = obj_financial["dict"]["date_list_after_ann"]
# date_list_report = obj_financial["dict"]["date_list_report"]
print("date_list_after_ann ",date_list_after_ann) 

if int(obj_financial["dict"]["date_end"]) > date_list_after_ann[-1] :
    date_list_after_ann = date_list_after_ann +[ int(obj_financial["dict"]["date_end"]) ]

print("Debug==== " )

### 新建df，保存未来一期组合收益：
obj_financial["df_ret_next"]= pd.DataFrame( columns=["date_start","date_end","ret_port"] )

### 获取所有交易日
obj_date={}
obj_date["date"] = "20060101"
obj_date = data_io_1.get_trading_days( obj_date )
date_list_all = obj_date["date_list_post"]
date_list_all.sort()

### 建立PMS权重文件
obj_financial["df_pms"] = pd.DataFrame( columns=["证券代码","持仓权重","成本价格","调整日期","证券类型"] )
obj_financial["count_pms"] =0
### Loop 
obj_data = {} 
count_date_period = 0
# # date_list_after_ann就是各个季度财务披露日之后第一个交易日 
for temp_date_start in date_list_after_ann:
    ########################################################################
    ### Initialization:特别注意几个不同的日期设置
    ''' ### 注意：在调仓日T有3套时间：
    1，T和之前1次披露时间；[date_ann_pre, date_ann ]
    2，T之前的2个季末财务日期；[date_q_pre,date_q]
    3，T日至下一个财务披露日期:[date_ann, date_ann_next ]。
     '''
    obj_data = {} 
    obj_data["dict"] = {} 
    obj_data["dict"]["id_output"] = obj_financial["dict"]["id_output"] 
    ### 财务报告披露日and组合调整日期：obj_data["dict"]["date_adj_port"] = str(temp_date_start)
    obj_data["dict"]["date_adj_port"] = str(temp_date_start)
    obj_data["dict"]["date_adj_port_next"] = str( date_list_after_ann[ date_list_after_ann.index(temp_date_start)+1 ] )
    obj_data["dict"]["date_ann"] = str(temp_date_start)
    obj_data["dict"]["date_ann_next"] =str( date_list_after_ann[ date_list_after_ann.index(temp_date_start)+1 ] )
    # obj_data["dict"]["date"] = str(temp_date_start) #  "20191105"  
    
    ########################################################################
    ### 导入财务分析和财务模型相关指标，"date"
    # notes:def import_data_financial_ana() 中增加了剔除上市不满足40天的个股
    # input :obj_data["dict"]["date"]

    obj_data["dict"]["single_industry"] =obj_financial["dict"]["single_industry"] 
    obj_data = data_factor_model_1.import_data_financial_ana(obj_data)
    ### df数据在 obj_data["df_ashare_ana"]  中
    # obj_data["dict"]["list_para_date"]  
    ### output:返回季度日期数据
    # obj_data["dict"]["date_q"]  
    # obj_data["dict"]["date_q_pre"]   
    # obj_data["df_ashare_ana"]   

    #######################################################################################
    ### 计算交易信号：根据指标筛选条件 
    '''
    案例分析：
    若在20200522，上述5个步骤计算后的股票数量依次为3832 -- 795 -- 216 -- 14 -- 10 -- 5，300738,002214，...
    若在20191105，上述5个步骤计算后的股票数量依次为3703 -- 1306 --781 --132 -- 123 -- 49
    49只股票区间平均涨跌幅 16.27%,[20191105,20200430];12.23%,[20191105,20200201];同期沪深300指数收益率分别为0.66%，3.01%。
    49只股票行业分布最多的12个：62-计算机，4个：20,26,27,30,35，电力及公用事业，机械，汽车，医药。
    ；
    放宽筛选条件后的68个股票，191101至200430的ew平均收益率12.4%，流通市值加权收益率1.5%；
    单指标	前25%股票
    zscore_S_FA_ROA_diff	23.6%
    zscore_S_FA_ROIC_diff	23.0%
    zscore_S_FA_ROE_diff	20.3%
    zscore_S_FA_ROA_q_ave	19.2%
    zscore_S_QFA_CGRGR_diff	19.1%
    zscore_S_QFA_CGRPROFIT_diff	18.6%
    zscore_S_FA_ROE_q_ave	16.8%
    zscore_S_FA_ROIC_q_ave	15.9%
    zscore_S_FA_OCFTOOR_diff	15.3%
    zscore_S_QFA_CGRPROFIT	13.0%
    zscore_S_QFA_YOYPROFIT	11.5%
    zscore_S_FA_OCFTOOPERATEINCOME_diff	10.9%
    zscore_S_QFA_CGRPROFIT_q_pre	10.5%
    zscore_S_FA_OCFTOOR	8.4%
    zscore_S_FA_OCFTOOPERATEINCOME	0.6%
    在68个指标中，收益最好的是roe、roa和roic变动、最差的是现金流变动的股票。
    因此，选择收益好的前6个指标
    '''
    obj_data["dict"]["leverage_para"] = 0.4

    obj_data = signals_ashare_1.get_signal_filter_level_para(obj_data )
    df_ashare_ana = obj_data["df_ashare_ana"]
    
    #######################################################################################
    ### 计算个股单指标和多指标的标准分
    ### 1,对单个指标计算均值和MAD，并代替极端值，col_name +"_mad"
    '''对指标进行mad去极值，考虑极端值进一步控制在正负1个标准差内，以控制单指标异常的影响。'''

    # level=1 意味着上下取约1.4倍标准差，level=3 意味着上下取约3*1.4倍标准差，默认值3
    level = 1 
    ### 原始方案，全部指标
    # col_list_mad =["S_FA_ROE" +"_q_ave", "S_FA_ROE" +"_diff","S_QFA_CGRGR" +"_diff", "S_QFA_CGRPROFIT" +"_diff" ]
    # col_list_mad =col_list_mad +["S_QFA_YOYPROFIT", "S_QFA_CGRPROFIT", "S_QFA_CGRPROFIT" +"_q_pre"]
    # col_list_mad =col_list_mad +["S_FA_ROIC" +"_q_ave",  "S_FA_ROA" +"_q_ave" ,"S_FA_ROIC" +"_diff" ,"S_FA_ROA" +"_diff" ]
    # col_list_mad =col_list_mad +["S_FA_OCFTOOR", "S_FA_OCFTOOPERATEINCOME", "S_FA_OCFTOOR" +"_diff","S_FA_OCFTOOPERATEINCOME" +"_diff"]

    ### 改进方案：根据历史收益回测改进的指标
    col_list_mad =["S_FA_ROE" +"_q_ave", "S_FA_ROE" +"_diff","S_QFA_CGRGR" +"_diff", "S_QFA_CGRPROFIT" +"_diff" ]
    col_list_mad =col_list_mad +["S_FA_ROIC" +"_q_ave",  "S_FA_ROA" +"_q_ave" ,"S_FA_ROIC" +"_diff" ,"S_FA_ROA" +"_diff" ]
    obj_data["col_list_mad"] = col_list_mad

    for col_name in col_list_mad :
        df_ashare_ana =  analysis_factor_1.cal_replace_extreme_value_mad( df_ashare_ana,col_name,level ) 
    ### 计算标准分值 || "zscore_"+col_name 
    df_ashare_ana["wind_code"] =df_ashare_ana[ "S_INFO_WINDCODE"] 
    obj_zscore = analysis_factor_1.indicator_data_adjust_zscore(df_ashare_ana, col_list_mad)
    obj_data["df_ashare_ana"] = obj_zscore["df_factor"]
    # 计算zscore总分
    obj_data["df_ashare_ana"]["zscore_" + "all" ]= 0.0
    for col_name in col_list_mad :
        obj_data["df_ashare_ana"]["zscore_" + "all" ] = obj_data["df_ashare_ana"]["zscore_" + "all" ]+ obj_data["df_ashare_ana"]["zscore_" + col_name  ]
    
    #######################################################################################
    ### 计算上一次披露日至最新披露日的区间收益率："date_ann_pre"，"date_ann"
    if count_date_period == 0 :
        ### 第一个区间不好取上一次财务披露日，因此用date_q 替代
        obj_data["dict"]["date_ann_pre"] = str( int(obj_data["dict"]["date_q_pre"] ) )
        obj_data["dict"]["date_ann"] =  str(temp_date_start) 
        count_date_period = 1
    else :
        obj_data["dict"]["date_ann_pre"] =  str( int( date_list_after_ann[ date_list_after_ann.index(temp_date_start)-1 ] ))
        obj_data["dict"]["date_ann"] =  str(temp_date_start) 
    
    #######################################################################################
    ###  给定区间，计算多因子和单因子分行业分组收益 
    # 因为计算的区间不同，这里用的是上一个披露日到最新披露日区间收益
    obj_data = perf_eval_ashare_stra_1.perf_eval_ashare_factors_ind_group( obj_data)
    ### output obj_data["df_ret_all"] 
    '''
    20191105 - 20200430：
    多指标加总反而不如单指标：
    全市场
                    ret_ew	ret_mvfloat	ret_top_30pct	ret_mid_40pct	ret_bottom_30pct	num_sp
    S_FA_ROE_q_ave	8.93%	9.25%	14.55%	8.94%	6.58%	1203
    S_FA_ROE_diff	8.96%	9.55%	10.15%	9.92%	8.28%	1137
    S_QFA_CGRGR_diff	8.41%	9.02%	5.48%	10.60%	10.48%	885
    S_QFA_CGRPROFIT_diff	9.35%	8.89%	6.34%	10.97%	10.11%	894
    S_FA_ROIC_q_ave	8.98%	9.29%	13.04%	9.60%	7.24%	1205
    S_FA_ROA_q_ave	8.98%	9.29%	14.61%	9.28%	6.56%	1205
    S_FA_ROIC_diff	8.98%	9.29%	12.00%	8.53%	7.68%	1205
    S_FA_ROA_diff	8.98%	9.29%	12.55%	8.60%	7.45%	1205
    all	            7.14%	7.55%	11.27%	9.84%	7.80%	522
    生物医药行业：
            ret_ew	ret_mvfloat	ret_top_30pct	ret_mid_40pct	ret_bottom_30pct	num_sp
    S_FA_ROE_q_ave	20.9%	19.6%	25.8%	25.9%	18.8%	66
    S_FA_ROE_diff	19.5%	17.5%	17.2%	15.3%	22.5%	62
    S_QFA_CGRGR_diff	12.5%	16.5%	14.8%	10.7%	23.5%	46
    S_QFA_CGRPROFIT_diff	13.8%	14.9%	9.6%	19.1%	25.8%	54
    S_FA_ROIC_q_ave	20.9%	19.6%	23.3%	24.4%	19.8%	66
    S_FA_ROA_q_ave	20.9%	19.6%	22.4%	28.3%	20.2%	66
    S_FA_ROIC_diff	20.9%	19.6%	14.3%	20.6%	23.8%	66
    S_FA_ROA_diff	20.9%	19.6%	15.6%	22.2%	23.2%	66
    all	            7.7%	11.3%	16.0%	22.4%	23.0%	27

    总结：1，采用zscore直接加总分的方式肯定不行，应该对单指标取前30%之后汇总成股票池
    2，部分指标要剔除或取bottom，例如：S_QFA_CGRGR_diff，S_QFA_CGRPROFIT_diff
    3，医药行业：总分选股是错的，分数低的反而涨得多。
    ------
    对所有行业的几个指标进行测试并取平均值：发现流通市值标准分加权小幅度优于等权重，取前30%有助于获得alpha；
    最有效的指标是roe平均值、roa平均值、roic平均值。
    平均收益率较弱的单指标中：
    S_FA_ROE_diff、S_QFA_CGRGR_diff、S_QFA_CGRGR_diff最有效的行业都是电子、建材、计算机、医药、基础化工、通信、农林牧渔；60 24 62 35 22 61 37。
    这说明好是行业本身的好。
        最有效指的是"ret_mvfloat"前1/3或者16%以上的区间收益率。
        行标签	        ret_ew	ret_mvfloat	ret_top_30pct	ret_bottom_30pct	ret_mid_40pct
        S_FA_ROE_q_ave	5.3%	5.6%	9.3%	3.7%	5.5%
        S_FA_ROA_q_ave	5.4%	5.6%	8.8%	4.0%	6.0%
        S_FA_ROIC_q_ave	5.4%	5.6%	8.5%	4.1%	5.1%
        S_FA_ROA_diff	5.4%	5.6%	8.0%	4.3%	5.3%
        S_FA_ROIC_diff	5.4%	5.6%	7.4%	4.6%	3.8%
        总计	5.4%	5.7%	7.2%	4.7%	5.3%
        S_FA_ROE_diff	5.4%	5.8%	6.9%	4.7%	6.4%
        S_QFA_CGRPROFIT_diff	6.2%	6.1%	4.8%	6.5%	7.0%
        S_QFA_CGRGR_diff	4.9%	5.9%	4.0%	5.5%	3.4%

    '''

    #################################################################################
    ### 计算行业轮动配置
    '''
    生成组合权重 plan:
    1,行业配置：df_ashare_ind
    1.1，对每个行业取"ret_top_30pct"的单指标最大值，并选取该指标下收益率最高的前N(=7)个行业
    1.2, 对于长期固定配置行业=长期看好的，判断是否在行业内;例如必选行业有大消费、电子计算机和医药、金融；
    1.3，目标行业权重，和上下限；
    2，个股权重：df_ashare_portfolio
    2.1，行业内取前30%进行权重分配；
    2.2，单一个股权重不超过10%
    '''
    '''中信一级行业
    10	石油石化 11	煤炭 12	有色金属 20	电力及公用事业 21	钢铁
    22	基础化工 23	建筑 24	建材 25	轻工制造 26	机械 27	电力设备及新能源
    28	国防军工 30	汽车 31	商贸零售 32	消费者服务  33	家电 34	纺织服装
    35	医药 36	食品饮料 37	农林牧渔
    40	银行 41	非银行金融 42	房地产 43	综合金融 50	交通运输 
    60	电子 61	通信 62	计算机 63	传媒 70	综合
    '''
    ### 设置长周期或短周期必选行业：如：22.0,24.0,25.0,27.0,30.0,34.0,40.0,41.0,42.0,63.0
    obj_data["dict"]["ind_fixed"] = [22.0,24.0,25.0,27.0,30.0,34.0,40.0,41.0,42.0,63.0 ]
    ### 控制入选股票数量：30，50,100，200
    obj_data["dict"]["ashare_weight_max_number"] = 100
    ### 加权方式：ew,mvfloat,weight_ind,others
    obj_data["dict"]["ashare_weight_type"] = "weight_ind" 
    ### 设置个股权重 obj_data["dict"]["ashare_weight_max"],全市场时10%
    obj_data["dict"]["ashare_weight_max"] = 0.08
    if "single_industry" in obj_financial["dict"].keys() :
        obj_data["dict"]["single_industry"] =obj_financial["dict"]["single_industry"]  
    if "indi_quantile_tail" in obj_financial["dict"].keys() :
        obj_data["dict"]["indi_quantile_tail"] =obj_financial["dict"]["indi_quantile_tail"]  
    obj_data = algorithm_ashare_weighting_1.algo_ashare_weight_ind_allo(obj_data)
    

    ################################################################################# 
    ### 计算组合在未来一段时期收益;最后一期会报错
    '''notes:注意！如果直接在原df里计算区间收益，列值会覆盖之前计算前序区间的数值'''

    obj_data_future = {} 
    obj_data_future["dict"]={}

    obj_data_future["dict"]["date_start"] = obj_data["dict"]["date_adj_port"]
    obj_data_future["dict"]["date_end"] =  obj_data["dict"]["date_adj_port_next"]
    obj_data_future["df_ashare_ana"] = obj_data["df_ashare_portfolio"]
    obj_data_future = data_io_1.get_period_pct_chg_codelist( obj_data_future)
    # 注意：输出的df里[adjclose_start	adjclose_end	period_pct_chg]3项会变成未来区间数据

    ### 赋值给 obj_financial
    obj_financial["df_ret_next"].loc[obj_data["dict"]["date_adj_port"], "date_start"]= obj_data["dict"]["date_adj_port"]
    obj_financial["df_ret_next"].loc[obj_data["dict"]["date_adj_port"], "date_end"]= obj_data["dict"]["date_adj_port_next"]
    temp_ret = (obj_data_future["df_ashare_ana"]["period_pct_chg"]*obj_data_future["df_ashare_ana"]["weight_raw"]).sum()
    print("Return next period",obj_data["dict"]["date_adj_port"],obj_data["dict"]["date_adj_port_next"] , temp_ret  )
    # 赋值给 obj_data
    obj_financial["df_ret_next"].loc[obj_data["dict"]["date_adj_port"], "ret_port"]= temp_ret

    obj_data["df_ashare_portfolio"]["period_pct_chg_next"] = obj_data_future["df_ashare_ana"]["period_pct_chg"]
    
    #################################################################################
    ### Export to csv file | todo, export_data_financial_ana
    
    (obj_data,obj_financial) = data_factor_model_1.export_data_financial_ana( obj_data,obj_financial)


asd





 

































########################################################################
### 对特定财务报表数据按报告期处理




########################################################################
### 对特定财务报表数据按报告期处理

# path = "D:\\db_wind\\data_wds\\"
# table_name = "AShareFinancialIndicator"
# file_name = "WDS_full_table_full_table_ALL.csv"
# df_fi_indi = pd.read_csv(path+table_name+"\\"+ file_name   )
# # print( df_fi_indi.drop_duplicates("REPORT_PERIOD")["REPORT_PERIOD"].to_list() )
# for year in range(2002,2020) :
#     for mmdd in ["0331","0630","0930","1231"] :
#         temp_date = str(year) + mmdd 
#         print( temp_date )
#         df_fi_indi_sub = df_fi_indi[ df_fi_indi["REPORT_PERIOD"]== int(temp_date)  ]
        
#         if len(df_fi_indi_sub.index )> 0 :
#             file_output = "WDS_"+ "REPORT_PERIOD" +"_"+temp_date+ "_ALL.csv"
#             df_fi_indi_sub = df_fi_indi_sub.drop("Unnamed: 0",axis=1)
#             df_fi_indi_sub.to_csv( path+table_name+"\\"+ file_output,index=False )
            
# asd
