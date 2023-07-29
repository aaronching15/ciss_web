# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
功能：导入abcd3d昨日报表，抓取对应季度数据：1，业绩预告快报；2，一致预期。
TODO:
1，还需要获取前一个季度的收入增速和净利润增速
2，

last  | since 20201019

功能：
1，数据导入：基础和对应季度的表
1.1，keyword=季度末：中国A股业绩预告[AShareProfitNotice]，报告期 S_PROFITNOTICE_PERIOD ;中国A股业绩快报[AShareProfitExpress]，报告期，REPORT_PERIOD
1.2，keyword=年度末：中国A股盈利预测明细[AShareEarningEst]，预测报告期，REPORTING_PERIOD；公告日期(内部) ANN_DT
1.3，keyword=无：中国A股机构调研活动[AshareISActivity]：公告日期 ANN_DT
1.4，keyword=季度末：中国共同基金投资组合——持股明细[ChinaMutualFundStockPortfolio]，截止日期，F_PRT_ENDDATE

2，数据匹配：
2.1，业绩快报：净利润(元)，NET_PROFIT_EXCL_MIN_INT_INC ；同比增长率:营业总收入，同比增长率:利润总额，加权平均净资产收益率
2.2，卖方盈利预测：预测净利润(万元)， 预测净利润调整比率
2.3，基金十大重仓股：牛基持股比例，持股市值
3，数据可视化：部分指标列名称切换为中文
4，数据导出

notes:

表数据明细：
1， 中国A股业绩预告[AShareProfitNotice]；S_PROFITNOTICE_PERIOD
文件大小：单季度，250kb
预告净利润变动幅度下限（%）S_PROFITNOTICE_CHANGEMIN
预告净利润变动幅度上限（%）S_PROFITNOTICE_CHANGEMAX
预告净利润下限（万元）S_PROFITNOTICE_NETPROFITMIN
预告净利润上限（万元）S_PROFITNOTICE_NETPROFITMAX
业绩预告摘要 S_PROFITNOTICE_ABSTRACT:这里边的文字有可能是：首亏，续亏；不确定，预增，续盈，略减，略增


2，中国A股盈利预测明细[AShareEarningEst]
数据文件名称：WDS_EST_DT_20200801_20201019.csv，
文件大小：最近3个月：113mb

3，中国共同基金投资组合——持股明细[ChinaMutualFundStockPortfolio]
notes:截止20201019，2020Q3的数据还没出来


'''
#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 

file_path_admin = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
path_data_adj = "D:\\db_wind\\data_adj\\"
path_data_wds = "D:\\db_wind\\data_wds\\"

### 导入wds数据转换模块
from transform_wind_wds import transform_wds
transform_wds1 = transform_wds()
### Print all modules 
transform_wds1.print_info()

from get_wind_wds import wind_wds
wind_wds1 = wind_wds()
wind_wds1.print_info()

### datetime
import time 
import datetime as dt 

### Input object 
obj_in={}
obj_in["date_abcd3d"]      = input("Type in date of abcd3d file ,such as 20201019:  ")
obj_in["date_profitnotice"]= "20200930" # input("Type in quarter end of 业绩预告ProfitNotice,such as 20200930:  ")
# "20200801" to "20201019"
obj_in["date_estimates_lb"]= input("Type in date start of 盈利预测明细,such as 20200801:  ")
obj_in["date_estimates_ub"]= input("Type in date start of 盈利预测明细,such as 20201019:  ")

obj_in["date_esti_year_fy1"]=  str(int(obj_in["date_estimates_lb"][:4])+1) + "1231"

#################################################################################
### 1，数据导入：基础和对应季度的表

#################################################################################
### 1.1，导入最新交易日个股数据文件
path_adj_ashare_ana = path_data_adj + "\\ashare_ana\\"
file_name = "ADJ_timing_TRADE_DT_" + obj_in["date_abcd3d"]   + "_ALL.csv"
df_abcd3d = pd.read_csv(path_adj_ashare_ana +file_name,encoding="gbk" )

### 只需要部分columns
df_abcd3d = df_abcd3d.loc[:, ["S_INFO_WINDCODE","S_DQ_PCTCHANGE","ind_code","S_DQ_MV","S_VAL_MV"] ]


#################################################################################
### 1.2，导入季度末业绩预告ProfitNotice
path_wds_profitnotice = path_data_wds + "\\AShareProfitNotice\\"
file_name = "WDS_S_PROFITNOTICE_PERIOD_" + obj_in["date_profitnotice"]   + "_ALL.csv"
df_profitnotice = pd.read_csv(path_wds_profitnotice +file_name,encoding="gbk" )

#################################################################################
### 1.3，导入近3个月盈利预测明细EarningEst
path_wds_estimates = path_data_wds + "\\AShareEarningEst\\"
file_name = "WDS_EST_DT_" + obj_in["date_estimates_lb"] +"_" + obj_in["date_estimates_ub"] + ".csv"
df_estimates = pd.read_csv(path_wds_estimates +file_name  )

### VIP：注意：卖方数据会有多个预测年度，需要选择未来1年的 | 3个月数据从4.3万下降到1.46万
# print( len(df_estimates.index ),type( df_estimates["REPORTING_PERIOD"].values[0]  ) )
df_estimates = df_estimates[ df_estimates["REPORTING_PERIOD"] == int(obj_in["date_esti_year_fy1"])  ]

#################################################################################
### 2，数据匹配：
#################################################################################
### 2.1，业绩快报：净利润(元)，NET_PROFIT_EXCL_MIN_INT_INC ；同比增长率:营业总收入，同比增长率:利润总额，加权平均净资产收益率

col_list=["S_PROFITNOTICE_CHANGEMIN","S_PROFITNOTICE_CHANGEMAX","S_PROFITNOTICE_NETPROFITMIN","S_PROFITNOTICE_NETPROFITMAX","S_PROFITNOTICE_ABSTRACT"]
# for temp_col in col_list : 
#     df_abcd3d[temp_col ] = 0.0

for temp_i in df_profitnotice.index :
    temp_code = df_profitnotice.loc[temp_i, "S_INFO_WINDCODE" ]
    # find code in df_abcd3d
    df_abcd3d_sub = df_abcd3d[ df_abcd3d["S_INFO_WINDCODE"] == temp_code ]
    if len( df_abcd3d_sub.index  ) == 1 :
        temp_j = df_abcd3d_sub.index[0]
        for temp_col in col_list : 
            df_abcd3d.loc[temp_j, temp_col ] = df_profitnotice.loc[temp_i, temp_col ]

### TODO:还需要获取前一个季度的收入增速和净利润增速

#################################################################################
### 2.2，卖方盈利预测：预测净利润(万元)， 预测净利润调整比率
'''由于可能涉及多个卖方研究员，因此应该选择最近的5个预测值取平均数;
指标包括2类：col_list_numeric对应数值型指标；col_list_str 对应字符串型指标
'''
col_list_numeric = ["EST_NET_PROFIT","EST_MAIN_BUS_INC","S_EST_ROE","S_EST_NPRATE","S_EST_EVEBITDA","S_EST_OPE" ]
# col_list_str = ["RESEARCH_INST_NAME","ANALYST_NAME","EST_DT","REPORTING_PERIOD","REPORT_NAME","REPORT_SUMMARY"]
# TODO 怀疑无法导出gbk编码是因为 报告标题和摘要里有奇怪的字符
col_list_str = ["RESEARCH_INST_NAME","ANALYST_NAME","EST_DT","REPORTING_PERIOD","REPORT_NAME","REPORT_SUMMARY"]
df_estimates = df_estimates.loc[:, ["S_INFO_WINDCODE"] + col_list_numeric+ col_list_str ]

for temp_i in df_abcd3d.index :
    temp_code = df_abcd3d.loc[temp_i, "S_INFO_WINDCODE" ]
    print(temp_i,temp_code ) 
    ### 在盈利预测df里查找公司近3个月的盈利预测数量和数据
    df_estimates_sub = df_estimates[ df_estimates["S_INFO_WINDCODE"] == temp_code ]
    ### 
    ### 1，按预测日期降序排列；2，若数量大于5个，取前5个
    if len( df_estimates_sub.index ) >= 1 :
        if len( df_estimates_sub.index ) > 5 :
            df_estimates_sub = df_estimates_sub.sort_values(by="EST_DT",ascending=False )
            df_estimates_sub = df_estimates_sub.iloc[:5 , :]

        ### Numeric:
        for temp_col in col_list_numeric :
            df_abcd3d.loc[temp_i, temp_col + "_latest"] = df_estimates_sub[temp_col].values[0] 
            df_abcd3d.loc[temp_i, temp_col + "_mean"] = df_estimates_sub[temp_col].mean()

        ### String：机构名称，作者，标题，摘要等        
        for temp_col in col_list_str :
            count_j = 1 
            for temp_j in df_estimates_sub.index :
                # print( df_estimates_sub )
                df_abcd3d.loc[temp_i, temp_col + "_"+ str(count_j) ] = df_estimates_sub.loc[temp_j, temp_col]
                count_j = count_j + 1 

df_abcd3d.to_csv("D:\\test_1.csv" )

#################################################################################
### 2.3，基金十大重仓股：牛基持股比例，持股市值




#################################################################################
### 3，数据可视化：部分指标列名称切换为中文
col_list_keep = ["S_INFO_WINDCODE","S_DQ_PCTCHANGE","ind_code","S_DQ_MV","S_VAL_MV"]
col_list_keep = col_list_keep +["S_PROFITNOTICE_CHANGEMIN","S_PROFITNOTICE_CHANGEMAX","S_PROFITNOTICE_NETPROFITMIN","S_PROFITNOTICE_NETPROFITMAX","S_PROFITNOTICE_ABSTRACT"]
col_list_keep = col_list_keep +["EST_NET_PROFIT_latest","EST_MAIN_BUS_INC_latest","S_EST_ROE_latest","S_EST_NPRATE_latest","S_EST_EVEBITDA_latest","S_EST_OPE_latest"]
col_list_keep = col_list_keep +["EST_NET_PROFIT_mean","EST_MAIN_BUS_INC_mean","S_EST_ROE_mean","S_EST_NPRATE_mean","S_EST_EVEBITDA_mean","S_EST_OPE_mean"]
for temp_str in ["_1","_2","_3","_4","_5"] :
    col_list_keep = col_list_keep +["RESEARCH_INST_NAME" + temp_str,"ANALYST_NAME" + temp_str,"EST_DT" + temp_str,"REPORT_NAME" + temp_str,"REPORT_SUMMARY" + temp_str]

### 只保留需要的columns
df_abcd3d = df_abcd3d.loc[:,col_list_keep ]


### CN中文
col_list_keep_CN = ["股票代码","日涨跌幅","行业代码","流通市值","总市值"]
col_list_keep_CN = col_list_keep_CN +["预告净利润变动下限","预告净利润变动上限","预告净利润下限|万","预告净利润上限|万","预告摘要"]
col_list_keep_CN = col_list_keep_CN +["预测净利润-最新","预测收入-最新","预测ROE-最新","预测净利润调整比率-最新","预测EV/EBITDA-最新","预测主营业务利润率-最新"]
col_list_keep_CN = col_list_keep_CN +["预测净利润-平均","预测收入-平均","预测ROE-平均","预测净利润调整比率-平均","预测EV/EBITDA-平均","预测主营业务利润率-平均"]
for temp_str in ["_1","_2","_3","_4","_5"] :
    col_list_keep_CN = col_list_keep_CN +["机构名称" + temp_str,"分析师" + temp_str,"预测日期" + temp_str,"报告标题" + temp_str,"报告摘要" + temp_str]

# dict_col_change = {}
# for i in range( len(col_list_keep) ):
#     dict_col_change[col_list_keep[i] ] = col_list_keep_CN[i]
# df_abcd3d.rename(dict_col_change,inplace=True) 
### 注意：df1.rename(...) 不能写成 df1 = df1.rename(...)

for i in range( len(col_list_keep) ):
    df_abcd3d[ col_list_keep_CN[i] ] =df_abcd3d[ col_list_keep[i] ] 

df_abcd3d = df_abcd3d.loc[:, col_list_keep_CN  ]
#################################################################################
### 4，数据导出

df_abcd3d.to_excel("D:\\test_"+ obj_in["date_abcd3d"]  +".xlsx" )
print("Debug====== " )
print( df_abcd3d.head().T )

asd