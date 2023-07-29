# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
todo
0，未完成：
1，陈亘斯需求：统计基金公司维度基金份额变动。

功能：
1.梳理基金份额数据
2，将df转换为按基金代码、报告期、的三维df

last | since 200617

derived from test_fund_ana.py
'''

#################################################################################
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
    从20050104开始对于每一年，对于0131、0331、0431、0731、0830、1030六个基金数据披露截止时间，要根据披露的基金持仓
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
### 设置组合id，是否单一基金"single_fund"，开始和结束日期
obj_fund_ana = {}
obj_fund_ana["dict"] ={}
# "rc_2005"
obj_fund_ana["dict"]["id_output"] = "rc_0617_test" 
# input("Type in id for portfolio,such as rc_2005:")  
obj_fund_ana["dict"]["single_fund"] = 1
### 若单个一级行业，则取行业内前50%;默认前30%；若indi_quantile_tail=1, 则取尾部指标值，默认值0取指标最大的。
obj_fund_ana["dict"]["indi_quantile_tail"] = 0 # 1
# "20200401"# input("Type in date start such as 20151101: ")
obj_fund_ana["dict"]["date_start"] = "20060401" #  "20150101" "20141103"
obj_fund_ana["dict"]["date_end"] = "20200527" # input("Type in date start such as 20190506: ")"20150301" # 


obj_fund_ana = data_io_1.get_after_ann_days_fund( obj_fund_ana )
date_list_after_ann = obj_fund_ana["dict"]["date_list_after_ann"]
date_list_report = obj_fund_ana["dict"]["date_list_report"]
print("date_list_after_ann ",date_list_after_ann) 
print("date_list_report ",date_list_report) 
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

# for temp_date_start in date_list_after_ann:
#     ########################################################################
#     ### Initialization:特别注意几个不同的日期设置
#     ''' ### 注意：在调仓日T有3套时间：
#     1，T和之前1次披露时间；[date_ann_pre, date_ann ]
#     2，T之前的2个季末财务日期；[date_q_pre,date_q]
#     3，T日至下一个财务披露日期:[date_ann, date_ann_next ]。
#      '''
#     obj_fund = {} 
#     obj_fund["dict"] = {} 
#     obj_fund["dict"]["id_output"] = obj_fund_ana["dict"]["id_output"] 
#     ### 财务报告披露日and组合调整日期：obj_fund["dict"]["date_adj_port"] = str(temp_date_start)
#     obj_fund["dict"]["date_adj_port"] = str(temp_date_start)
#     obj_fund["dict"]["date_adj_port_next"] = str( date_list_after_ann[ date_list_after_ann.index(temp_date_start)+1 ] )
#     obj_fund["dict"]["date_ann"] = str( date_list_report[ date_list_after_ann.index(temp_date_start) ] )
#     obj_fund["dict"]["date_ann_next"] =str( date_list_report[ date_list_after_ann.index(temp_date_start)+1 ] )
#     # obj_fund["dict"]["date"] = str(temp_date_start) #  "20191105"  
#     ### 获取 之前的1个交易日用于导入最近的预期数据；date_list_all
#     temp_list = [date for date in date_list_all if date< temp_date_start]
#     obj_fund["dict"]["date_adj_port_pre"] = max(temp_list) 
    
#     #####################################################################
#     ### 导入基金基础数据，基金公司、代码、成立日F_INFO_SETUPDATE 等
#     # notes:这部分时间比较久，考虑未来直接读取缩减后的小表
#     print("导入基金基础数据")
#     obj_fund = data_io_fund_ana_1.import_data_fund_ashare_des(obj_fund)

#     ### 导入基金前十大或全部持股
#     print("导入基金前十大或全部持股")
#     obj_fund = data_io_fund_ana_1.import_data_fund_holdings(obj_fund)


#     #####################################################################
#     ### 导入基金份额数据
#     # notes:"CHANGE_DATE"中既有季度末日期，也有非季度末日期
#     # 份额变动原因列"CHANGEREASON"有很多，常用的是 SGSH申购赎回，JJCL基金成立等十几个。
#     obj_fund = data_io_fund_ana_1.import_data_fund_fundshare(obj_fund)

#     ### 保存到csv
#     # 限定特定columns
#     obj_fund["col_list_export_df_fund"]=["fund_code","F_INFO_NAME","F_INFO_CORP_FUNDMANAGEMENTCOMP","F_PRT_ENDDATE","F_PRT_NETASSET","F_PRT_STOCKVALUE","FUNDSHARE"]
#     obj_fund = data_io_fund_ana_1.export_data_fund(obj_fund,obj_fund_ana)

# asd


# "fund_code","F_INFO_NAME","F_INFO_CORP_FUNDMANAGEMENTCOMP","F_PRT_ENDDATE","F_PRT_NETASSET","F_PRT_STOCKVALUE","FUNDSHARE"]
#################################################################################
### csv批量操作：
### 1, 合并所有 df_fund_20200401.csv,df_fund_company_20200401
### 2, 将df转换为按基金代码、报告期、的三维df
'''Panel ：三维的数组。可以理解为DataFrame的容器
Panel的三个维度对应的label分别是：item,major_axis,minor_axis
例子：df_panel = {"df_a":df_a,"df_b": df_b}
items - axis 0，每个项目对应于内部包含的数据帧(DataFrame)。
major_axis - axis 1，它是每个数据帧(DataFrame)的索引(行)。
minor_axis - axis 2，它是每个数据帧(DataFrame)的列。
'''

count= 0 
path = "D:\\CISS_db\\fund_simulation\\rc_0617_test\\"
for temp_date_start in date_list_after_ann:
    file_name = "df_fund_"+ str(temp_date_start) +".csv"
    df_q= pd.read_csv(path + file_name,encoding="gbk" )    
    df_q["date"] = temp_date_start

    file_name = "df_fund_company_"+ str(temp_date_start) +".csv"
    df_q_comp = pd.read_csv(path + file_name,encoding="gbk" )
    df_q_comp["ann_date"] =  str(temp_date_start)
    df_q_comp["date"] = temp_date_start

    if count == 0  :
        df_fund_all = df_q
        df_fund_company = df_q_comp
        count=1 
        ### 创建Panel data，
        df_panel_fund = pd.Panel( major_axis=df_q.index , minor_axis=df_q.columns  )
        df_panel_fund_company = pd.Panel( major_axis=df_q_comp.index , minor_axis=df_q_comp.columns  )
    else :
        ### 新增和上一季的差值 share_diff
        # fund_code
        for temp_i in df_q.index :
            # find fund_code in df_fund_all
            df_sub = df_fund_all[ df_fund_all["fund_code"]== df_q.loc[temp_i, "fund_code"] ]
            df_sub = df_sub [df_sub["date"] == temp_date_pre  ] 
            if len( df_sub.index ) > 0 : 
                df_q.loc[temp_i, "share_diff"] = df_q.loc[temp_i, "FUNDSHARE"] - df_sub["FUNDSHARE"].values[0]
                df_q.loc[temp_i, "date_pre"] =  temp_date_pre
        # fund company 
        for temp_i in df_q_comp.index :
            # find fund company in df_fund_company
            df_sub = df_fund_company[ df_fund_company["F_INFO_CORP_FUNDMANAGEMENTCOMP"]== df_q_comp.loc[temp_i, "F_INFO_CORP_FUNDMANAGEMENTCOMP"] ]
            df_sub = df_sub [df_sub["date"] == temp_date_pre  ] 
            if len( df_sub.index ) > 0 : 
                df_q_comp.loc[temp_i, "share_diff"] = df_q_comp.loc[temp_i, "FUNDSHARE"] - df_sub["FUNDSHARE"].values[0]
                df_q_comp.loc[temp_i, "date_pre"] =  temp_date_pre

        df_fund_all = df_fund_all.append(df_q,ignore_index=True)
        df_fund_company =df_fund_company.append(df_q_comp,ignore_index=True)
        ### save to panel object 
        df_panel_fund[temp_report_date] = df_q
        df_panel_fund_company[temp_report_date] = df_q_comp

    temp_report_date = str( int(df_q["F_PRT_ENDDATE"].values[0]) )
    temp_date_pre = temp_date_start    
    
    # 设置主index和辅助index ？

    ### save to hdf5 
    if count ==1 :
        print(df_panel_fund )

        df_panel_fund.to_hdf(path+"df_fund_all.h5",key= "fund")
        df_panel_fund_company.to_hdf( path+"df_fund_all.h5",key= "fund_company" )

    ### save to csv
    df_fund_all.to_csv(path+"df_fund_all.csv",encoding="gbk")
    df_fund_company.to_csv(path+"df_fund_company_all.csv",encoding="gbk")

    ###



asd


























