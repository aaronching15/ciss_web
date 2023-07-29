# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
################################################
todo： 
### 基金股票持仓的数据分析
### Ana：经典模型——静态：1，净值与指数拟合；2，持仓的拟合。问题：反映历史，而不是未来
### Idea：动态模型：1，净值的变动预测；2，持仓的变动预测。key：情境分析。
last 201229 | since 200201
derived from test_fund_ana2.py；rc_data\test_wds_data_transform_fund.py
目录：path=C:\rc_reports_cs\rc_2020论文_课题\0paper_基金持仓研究和基金分类
path=C:\ciss_web\CISS_rc\apps\fund_ana;file=0基金持仓仿真.xlsx

'''
#################################################################################
### Part 0 数据准备 |  Initialization，load configuration 
# Notes: 除了基本的路径和时间，其他脚本导入尽量都放在 config_XXX 里，保存再 obj 对象 
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db；# C:\ciss_web\CISS_rc\config
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\fund_analysis\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )
sys.path.append(path_ciss_rc + "config\\" )
import pandas as pd 
import numpy as np 
import math
### 导入数据IO模块
from data_io import data_io
data_io_1 = data_io()
from data_io_pricevol_financial import data_pricevol_financial
data_pricevol_financial_1 = data_pricevol_financial()
### 
from fund_simulation import fund_simulation
class_fund_simu = fund_simulation()
from data_io_fund_ana import data_io_fund_simulation
data_io_fund_simu_1 = data_io_fund_simulation()
from fund_skill import fund_skill
class_fund_skill = fund_skill()

##########################################
### 判断是否导入数据，还是重新计算
cal_port_fund_unit = input("0:导入已有数据；\n1：首次计算数据;\n3,统计多季度skill均值和平均拟合收益率；\n2,转换对应A股日数据：")
# cal_port_fund_unit = "0" # "1" 
date_start = input("输入开始的季末披露日期，如20060801：") 
date_end = input("输入结束的季末披露日期，如20211231：") 
##########################################
### Part1 导入区间季度数据：2006年~2019年：0801-1101，1101-0201；0201-0501，0501-0801
### derived from test_fund_ana.py
### 设置组合id，是否单一基金"single_fund"，开始和结束日期
obj_date = {}
obj_date["dict"] ={}
# "rc_2005"
# str_year = input("Type in year such as 2012,2016,2019:")
# "20200501"  # "20080401"  # "20060401" 
### if_only_quarter= 1,只抓取4个季度披露日后的日期，默认是4+2，季度和半年度
obj_date["dict"]["if_only_quarter"] = 1
# obj_date["dict"]["date_start"] = str_year + "0701" 
##最早是从 20060801开始
obj_date["dict"]["date_start"] = date_start
# "20211231" # input("Type in date start such as 20190506: ")
obj_date["dict"]["date_end"] = date_end  

obj_date = data_io_1.get_after_ann_days_fund( obj_date ) 
### 关于日期变量的说明
### date_list_past 开始日期之前的所有历史交易日
# date_list_past = obj_date["dict"]["date_list_past"]  
date_list_post = obj_date["dict"]["date_list_post"]  
# date_list_period 所有历史交易日 
### date_list_before_ann 披露日前最后一个交易日| [20060703, 20060731, 20060830,
date_list_before_ann = obj_date["dict"]["date_list_before_ann"] 

### date_list_after_ann 披露日后第一个交易日|[20060703, 20060801, 20060831, 20061101
date_list_after_ann = obj_date["dict"]["date_list_after_ann"]
date_list_after_ann = date_list_after_ann +[ obj_date["dict"]["date_end"] ]
### date_list_report 季末财务报告日期| [20060430, 20060731, 20060830, 20061031
date_list_report = obj_date["dict"]["date_list_report"]
### 季报结束日：0331，0630
date_list_quarter_end = obj_date["dict"]["date_list_quarter_end"]
### 将日期数据合并成df
df_date= pd.DataFrame([date_list_before_ann,date_list_after_ann,date_list_report,date_list_quarter_end] )
df_date=df_date.T
df_date.columns=["before_ann","after_ann","report","quarter_end"]
print("df_date \n",df_date.head()  )

if cal_port_fund_unit == "3" : 
    ##########################################
    ### 统计多季度skill均值和平均拟合收益率 
    obj_skill = {}
    obj_skill["dict"] = {} 
    obj_skill["df_date"] = df_date        
    ### 
    obj_skill =  class_fund_skill.cal_stat_skill_ret( obj_skill )
    obj_skill["df_stat_skill_ret"].to_excel("D:\\df_stat_skill_ret.xlsx")  


for temp_i in df_date.index:
    ### 获取区间日期
    ### 获取date_list
    date_list = [ i for i in date_list_post if i>= df_date.loc[temp_i,"after_ann" ] ] 
    # notes:这里需要小于号，否则会出错
    date_list = [ i for i in date_list if i < df_date.loc[temp_i+1,"after_ann" ] ] 
    print("temp_date: "  ,df_date.loc[temp_i,"after_ann" ]  )  
    
    temp_date = df_date.loc[temp_i,"after_ann" ]  # "20190801"
    
    if cal_port_fund_unit == "2" : 
        ##########################################
        ### 转换对应A股日数据
        ### derived from ashares_timing_abcd3d.py
        obj_in ={}
        obj_in["dict"] ={}
        obj_in["dict"]["date_start"] = str( df_date.loc[temp_i,"after_ann" ] -1)
        obj_in ["dict"]["date_end"] = str( df_date.loc[temp_i,"after_ann" ] +1  )
        obj_in = data_pricevol_financial_1.import_data_ashare_change_amt_period( obj_in)


    if cal_port_fund_unit == "1" : 
        ##########################################
        ### Part1 导入用于计算的基金和个股数据 
        obj_in = {}
        ### obj_in["temp_date"] 一般选基金十大持仓披露日当月月末后的第一个交易日
        obj_in["temp_date"] = str( df_date.loc[temp_i,"after_ann" ] )
        ### quarter_end="20190630" # 基金十大持仓对应季末日期 
        obj_in["quarter_end"] = str( df_date.loc[temp_i,"quarter_end" ] )
        obj_in["date_list"] = date_list 
        obj_fund_ana = class_fund_simu.get_fund_data(obj_in )

        
        ##########################################
        ### Part2 计算市场基础组合，如市值、行业、风格、重要个股等 
        obj_port = class_fund_simu.cal_basic_port_mkt_ind( obj_fund_ana)
        ### output：
        # obj_port["dict"]["date_list"] ; 
        # obj_port["dict"]["col_list_port"] 已有的组合列表
        # obj_port["df_ashare_ana"] 股票信息和组合权重
        # obj_port["df_port_unit"] 同时保存净值和业绩指标
        # obj_port["df_perf_eval"] 只有业绩指标没有净值
                
        ##########################################
        ### Part3  导入基金重仓股票数据，选取近一年业绩前10基金的重仓股，分别构建组合,计算收益率和绩效指标 
        obj_port["dict"]["num_fund_simu"] = 50
        obj_fund_ana,obj_port = class_fund_simu.cal_fund_port_top10stock( obj_fund_ana,obj_port)
        ### 筛选出的基金列表： obj_fund_ana["dict"]["fund_list_short"] 
        # 增加了部分组合： obj_port["dict"]["col_list_port"]  

        ##########################################
        ### Part4 导入基金净值和计算区间业绩指标
        ### 所有交易日，用于匹配历史文件
        obj_port["dict"]["date_list_after_ann"] = date_list_after_ann
        obj_fund_ana,obj_port = class_fund_simu.cal_fund_nav_indi( obj_fund_ana,obj_port)

        ### OUTPUT:obj_port["df_port_unit"]:index组合名称；columns是日期和分析指标
        # 增加了部分组合： obj_port["dict"]["col_list_port"]  
        
        ##########################################
        ### 保存到json和excel文件
        result = data_io_fund_simu_1.export_fund_simulation(obj_fund_ana,obj_port)


    if cal_port_fund_unit == "0" : 
        ### 保存到json和excel文件
        #############################################
        ### 导入基金数据
        obj_port,obj_fund_ana = data_io_fund_simu_1.import_fund_simulation(  temp_date)
        
        print("obj_port.keys:",  obj_port["dict"].keys() )
        print("obj_fund_ana.keys:",  obj_fund_ana["dict"].keys() ) 
        # print("obj_port--df_ashare_ana", obj_port["df_ashare_ana"].head()  )
        print("obj_port--df_perf_eval", obj_port["df_perf_eval"].head()  )
        print("obj_port--df_port_unit", obj_port["df_port_unit"].head()  )
        ### 仅有基础组合业绩指标 ，obj_port["df_basic_perf_eval"] 
        ### 直接导入基金数据 | path_fund_simu = "D:\\CISS_db\\fund_simulation\\output\\"
        # df_port_unit = pd.read_excel( class_fund_simu.path_fund_simu + "df_port_fund_all_perf_eval_"+ obj_in["temp_date"] +".xlsx") 

        #################################################################################
        ### S 2.5，根据基金收益率和重仓股、市场组合日收益率做回归，估计基金股票配置比例！ 

        ### notes:部分季度如20100201、20100802没有数据，因为基金净值缺失，现实中由于基金清算或转型等的变动很正常。
        obj_fund_ana,obj_port = class_fund_simu.cal_fund_stockpct_simu( obj_fund_ana,obj_port ) 
        ## 以10个组合为例，日净值相关系数大多在 73%~ 97%，也有 45%、-43% 说明仓位有较大变动或者完全无关的
        ## 如果只看相关性，全部基础组合相关性在-11% ~ 92%，前5的品种
        ## 基金前海开源稀缺资产001679.OF，全部行业组合都无法解释；也许只有用个股比如宁德时代才行了。
        ### OUTPUT :obj_port["df_perf_eval"] = df_perf_eval
        # obj_port["dict"]["col_list_indi" ] = col_list_indi
        # 拟合组合的名字: obj_port["dict"]["port_list_simu"] = port_list_simu
        ### ??? 不知道为什么 obj_port["df_port_simu"] 在最后边会变没
        path_fund_simu = "D:\\CISS_db\\fund_simulation\\output_50f\\"
        output_type = "next_"
        temp_date = str( df_date.loc[temp_i,"after_ann" ] )
        if "df_port_simu" in obj_port.keys() : 
            obj_port["df_port_simu"].to_excel( path_fund_simu + "df_port_simu_"+output_type+ temp_date +".xlsx") 
        else :
            asd

        ################################### 
        ### s 2.6,对于基金仿真组合"simu_port_list",提取未来一个区间的基金净值和计算股票组合收益。
        ### Notes：提取下一个季度净值时，可能出现历史上有净值，但数据文件里没有，比如184688.SZ在20070801没有净值数据
        temp_date_next = df_date.loc[temp_i +1 ,"after_ann" ]  # "20190801" 
        ### quarter_end="20190630" # 基金十大持仓对应季末日期 
        quarter_end_next = df_date.loc[temp_i+1 ,"quarter_end" ]
        obj_port["dict"]["temp_date_next"] = temp_date_next
        obj_port["dict"]["quarter_end_next"] = quarter_end_next  
        ### NOTES: "_next"后缀的文件输出xlsx文件时药小心，不能覆盖当季数据！！！
        obj_port,obj_fund_ana_next,obj_port_next = class_fund_simu.cal_simu_next_period( obj_fund_ana,obj_port )
        ### 主要变化：obj_port_next["df_port_unit"] ,obj_port["df_port_unit"]

        ################################### 
        ### s 2.7, 预测能力评估：skill_set，计算三类skill 指标
        obj_port = class_fund_skill.cal_fund_skill( obj_port,obj_fund_ana_next,obj_port_next  )
        ### output:
        # obj_port["df_skill"] = df_skill
        # obj_port["df_skill_next"] = df_skill_next
        # obj_port["df_skill_next_ave"] = df_skill_next_ave
        
        
        ##########################################
        ### 保存到json和excel文件        
        ### export_fund_simulation 包括了 export_fund_skill
        result = data_io_fund_simu_1.export_fund_simulation(obj_fund_ana,obj_port)
        
        obj_port_next["dict"]["output_type"] = "next"
        result = data_io_fund_simu_1.export_fund_simulation(obj_fund_ana_next,obj_port_next)
        #############################################
        ### save 导出基金拟合计算过程的skill文件,只保存skill可以避免数据覆盖
        # result = data_io_fund_simu_1.export_fund_skill(obj_fund_ana,obj_port)


asd  






 
asd 
 










































ASD 
################################################################################## 
### BEFORE 20210101 | 之前的单只基金计算方法
### 
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
