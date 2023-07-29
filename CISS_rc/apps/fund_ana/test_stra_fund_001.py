# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
功能：计算基金和基金公司增持组合，并采用预期指标打分。

TODO:
1，还需要获取基金代码对应的基金公司
2，

last 20201104 | since 20201102

基本信息：
策略ID	rc_stra_fund_001	
策略名称	基金公司增持	
大类	机构研究				
小类	基金公司		
细类	增持
描述    季度频率跟踪基金季报增持股票：基金和基金公司维度
策略管理文件：rc_量化投资研究_卖方服务.xlsx

功能：
    0，给定交易日，导入股票池；一般为季度基金十大持仓披露截止日。例如20Q3是20201030
    1, 数据导入：基金信息、基金持仓、股票
    1.1，基金信息：基金代码对应的基金公司、基金分类、
    1.2，基金持仓：导入3季度前十和2季度全部
    1.3，股票：abcd3d最新指标
    2，数据匹配：
    2.0，匹配所属的基金公司
    2.1，计算最近2个季度持仓市值加权变动
    2.2,数据汇总：按股票总市值前20和市值变动前20 获取基金公司名单
    2.3，对于每个基金公司维度分组，提取增持股票组合，按照财务预期等指标构建组合
    2.4，用基金指标和股票预期指标进行打分和加权

    3，数据可视化：部分指标列名称切换为中文
    4，数据导出

notes: 

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
sys.path.append(path_ciss_rc + "db\\analysis_indicators\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

file_path_admin = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
path_data_adj = "D:\\db_wind\\data_adj\\"
path_data_wds = "D:\\db_wind\\data_wds\\"

### data_io中的 del_columns | 删除列值中包括特定字符的列
from data_io import data_io 
data_io_1 = data_io()
col_str = "Unnamed" 

###  
from config_data import config_data
config_data_1 = config_data()

# from get_wind_wds import wind_wds
# wind_wds1 = wind_wds()
# wind_wds1.print_info()

### 导入财务指标分析模块
from analysis_indicators_financial import indicators_financial
indicators_financial_1 = indicators_financial()


### datetime
import time 
import datetime as dt 

#################################################################################
### 0，给定交易日，导入股票池；一般为每年季度财务数据发布截至日或之前时间（发布多少算多少）；Input object 
obj_in={}
obj_in["date_abcd3d"]  = "20201102" # input("Type in date of abcd3d file ,such as 20201019:  ")
obj_in["date_quarter_end"] = "20200930" # input("Type in quarter end of fund stock portfolio,such as 20200930:  ")
obj_in["date_halfyear_end"] = "20200630" # input("Type in half-year end of fund stock portfolio,such as 20200930:  ")
obj_in["path_output"] = "D:\\CISS_db\\apps\\fund_ana\\stra_fund_001\\" + obj_in["date_quarter_end"] + "\\"

# obj_in["date_estimates_lb"]= input("Type in date start of 盈利预测明细,such as 20200801:  ")
# obj_in["date_estimates_ub"]= input("Type in date start of 盈利预测明细,such as 20201019:  ")

# obj_in["date_esti_year_fy1"]=  str(int(obj_in["date_estimates_lb"][:4])+1) + "1231"

### 导入最新交易日个股数据文件
path_adj_ashare = path_data_adj + "\\ashare_ana\\"
file_name = "ADJ_timing_TRADE_DT_" + obj_in["date_abcd3d"]   + "_ALL.csv"

try :
    df_abcd3d = pd.read_csv(path_adj_ashare +file_name,encoding="gbk" )
except :
    df_abcd3d = pd.read_csv(path_adj_ashare +file_name,encoding="gbk" )
### 删除部分列
# df_abcd3d = data_io_1.del_columns( df_abcd3d,col_str)

### 只需要部分columns
# df_abcd3d = df_abcd3d.loc[:, ["S_INFO_WINDCODE","S_DQ_PCTCHANGE","ind_code","S_DQ_MV","S_VAL_MV"] ]

#################################################################################
### 1, 数据导入：个股最近8个季度和5个年度的主要财务数据
obj_fi = {}
obj_fi["dict"] = {}
### 数据的ID
obj_fi["dict"]["data_id"] ="rc_data_fund_001"
### 最新日期
obj_fi["dict"]["date"] =  obj_in["date_abcd3d"]

obj_fi["df_ashare"] = df_abcd3d

from data_io_financial_indicator import data_io_financial_indicator
data_io_financial_indicator_1 = data_io_financial_indicator()
data_io_financial_indicator_1.print_info()


#################################################################################
### 1.1，基金信息：基金代码对应的基金公司、基金分类、
'''
notes: ChinaMutualFundDescription 全历史表的数据量150mb较大，
基金代码： F_INFO_WINDCODE F_INFO_FRONT_CODE F_INFO_BACKEND_CODE
基金公司=管理人=F_INFO_CORP_FUNDMANAGEMENTCOMP
存续状态 F_INFO_STATUS ：有效(L):101001000 发行(N):101003000 摘牌(D):101002000
基金类型 F_INFO_TYPE 契约型封闭式
是否指数基金 IS_INDEXFUND ；0:否 1:是
'''

# col_list= ["F_INFO_CORP_FUNDMANAGEMENTCOMP","F_INFO_STATUS","IS_INDEXFUND","F_INFO_WINDCODE","F_INFO_FRONT_CODE","F_INFO_BACKEND_CODE"]

# table_name = "ChinaMutualFundDescription"
# path_temp = config_data_1.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
# file_name = "WDS_full_table_full_table_ALL.csv"

# df_fund_des = pd.read_csv( path_temp + file_name  )

# ### 保留部分columns
# df_fund_des = df_fund_des.loc[:,col_list]

# ### 剔除指数基金等
# df_fund_des = df_fund_des[df_fund_des["F_INFO_STATUS"] == 101001000 ]
# df_fund_des_index = df_fund_des[df_fund_des["IS_INDEXFUND"] == 1 ]
# df_fund_des = df_fund_des[df_fund_des["IS_INDEXFUND"] == 0 ]
# fund_code_list = df_fund_des["F_INFO_WINDCODE"].to_list() + df_fund_des["F_INFO_FRONT_CODE"].to_list()+ df_fund_des["F_INFO_BACKEND_CODE"].to_list()

# print("fund_code_list ", fund_code_list[:3],fund_code_list[-3:])
# ### 截止20201102，有效记录约16697条
# print("df_fund_des  ", df_fund_des.describe()  )

# #################################################################################
# ### 1.2，基金持仓：导入3季度前十和2季度全部
# ### 1.2.1, 导入当季度基金十大持仓股票
# table_name = "ChinaMutualFundStockPortfolio"
# path_temp = config_data_1.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
# # WDS_F_PRT_ENDDATE_20200930_ALL.csv
# file_name = "WDS_F_PRT_ENDDATE_"+ obj_in["date_quarter_end"]  +"_ALL.csv"
# df_fund_q_stock = pd.read_csv( path_temp + file_name  )
# ### 要提出指数基金 || column=基金Wind代码,S_INFO_WINDCODE
# print("df_fund_q_stock \n", df_fund_q_stock.describe() )

# df_fund_q_stock = df_fund_q_stock[ df_fund_q_stock["S_INFO_WINDCODE"].isin( fund_code_list ) ]
# print("df_fund_q_stock \n", df_fund_q_stock.describe() )

# ##########################################
# ### 2020Q3季度十大重仓，大概有48387条持仓记录，加指数6.42万
# fund_code_list_q =  df_fund_q_stock["S_INFO_WINDCODE"].to_list()

# ### 1.2.2, 导入前一季度基金全部持仓股票
# file_name2 = "WDS_F_PRT_ENDDATE_"+ obj_in["date_halfyear_end"]  +"_ALL.csv"
# df_fund_h_stock = pd.read_csv( path_temp + file_name2  )

# print("df_fund_h_stock \n", df_fund_h_stock.describe() )
# df_fund_h_stock = df_fund_h_stock[ df_fund_h_stock["S_INFO_WINDCODE"].isin( fund_code_list_q ) ]
# ##########################################
# ### 2020Q2 半年度全部持仓，大概有260527条持仓记录，加指数43.9万
# print("df_fund_h_stock \n", df_fund_h_stock.describe() )

# #################################################################################
# ### 1.3，股票：abcd3d最新指标


# #################################################################################
# ### 2，数据匹配：
# ### 2.0，匹配所属的基金公司
# ### 2.1，计算最近2个季度持仓市值加权变动
# # 对于每只基金当季度的持仓，计算和上一半年度相比，占组合净值的变化、变化率，
# col_list_weight=["mv","weight","weight_pre","weight_diff","weight_diffpct" ,"F_PRT_STKVALUE","mv_diff" ]
# col_list_weight_CN = ["市值","权重-最新","权重-上期","权重变动","权重变动百分比","市值","市值变动"  ]
# count=0
# temp_len = len( df_fund_q_stock.index )
# for temp_i in df_fund_q_stock.index :

#     temp_code_fund = df_fund_q_stock.loc[temp_i, "S_INFO_WINDCODE" ]
#     temp_code_s =  df_fund_q_stock.loc[temp_i, "S_INFO_STOCKWINDCODE" ]
#     ### 2.0，匹配所属的基金公司 | "F_INFO_WINDCODE","F_INFO_FRONT_CODE","F_INFO_BACKEND_CODE"
#     df_fund_des_sub = df_fund_des[ df_fund_des["F_INFO_WINDCODE"]== temp_code_fund ]
#     if len(df_fund_des_sub.index) > 0 :
#         temp_fund_comp = df_fund_des_sub["F_INFO_CORP_FUNDMANAGEMENTCOMP"].values[0]
#     else :
#         df_fund_des_sub = df_fund_des[ df_fund_des["F_INFO_FRONT_CODE"]== temp_code_fund ]
#         if len(df_fund_des_sub.index) > 0 :
#             temp_fund_comp = df_fund_des_sub["F_INFO_CORP_FUNDMANAGEMENTCOMP"].values[0]
#         else :
#             df_fund_des_sub = df_fund_des[ df_fund_des["F_INFO_BACKEND_CODE"]== temp_code_fund ]
#             if len(df_fund_des_sub.index) > 0 :
#                 temp_fund_comp = df_fund_des_sub["F_INFO_CORP_FUNDMANAGEMENTCOMP"].values[0]
#             else :
#                 temp_fund_comp =""

#     df_fund_q_stock.loc[temp_i, "fund_comp" ] = temp_fund_comp

#     print("Working on ",count,round(count/ temp_len*100,2),"% ",temp_len )
#     ### locate stock and fund in previous quarter
#     df_fund_h_stock_sub = df_fund_h_stock[ df_fund_h_stock["S_INFO_WINDCODE"] == temp_code_fund ]
#     if_pre_q = 0 
#     if len( df_fund_h_stock_sub.index  ) > 0 :
#         df_fund_h_stock_sub = df_fund_h_stock[ df_fund_h_stock["S_INFO_STOCKWINDCODE"] == temp_code_s ]
#         if len( df_fund_h_stock_sub.index  ) > 0 :
#             if_pre_q = 1 

#     df_fund_q_stock.loc[temp_i, "weight" ] = df_fund_q_stock.loc[temp_i, "F_PRT_STKVALUETONAV" ]
#     df_fund_q_stock.loc[temp_i, "mv" ] = df_fund_q_stock.loc[temp_i, "F_PRT_STKVALUE" ]
#     if if_pre_q == 1 :
#         ### 持有股票市值占基金净值比例(%) F_PRT_STKVALUETONAV        
#         if df_fund_h_stock_sub["F_PRT_STKVALUETONAV"].values[0] > 0.0 :
#             df_fund_q_stock.loc[temp_i, "weight_pre" ] = df_fund_h_stock_sub["F_PRT_STKVALUETONAV"].values[0]
#             df_fund_q_stock.loc[temp_i, "weight_diff" ] = df_fund_q_stock.loc[temp_i, "F_PRT_STKVALUETONAV" ] - df_fund_h_stock_sub["F_PRT_STKVALUETONAV"].values[0]
#             df_fund_q_stock.loc[temp_i, "weight_diffpct" ] = df_fund_q_stock.loc[temp_i, "weight_diff" ]/ df_fund_h_stock_sub["F_PRT_STKVALUETONAV"].values[0]
#             ### 绝对金额变动 | 持有股票市值(元) F_PRT_STKVALUE
#             df_fund_q_stock.loc[temp_i, "mv_diff" ] = df_fund_q_stock.loc[temp_i, "F_PRT_STKVALUE" ] - df_fund_h_stock_sub["F_PRT_STKVALUE"].values[0]

#         else :
#             df_fund_q_stock.loc[temp_i, "weight_pre" ] = 0.0
#             df_fund_q_stock.loc[temp_i, "weight_diff" ] = df_fund_q_stock.loc[temp_i, "F_PRT_STKVALUETONAV" ] 
#             df_fund_q_stock.loc[temp_i, "weight_diffpct" ] = 1.00
#             df_fund_q_stock.loc[temp_i, "mv_diff" ] = df_fund_q_stock.loc[temp_i, "F_PRT_STKVALUE" ]                
#     else :
#         ### 假设新买入，则增幅100%
#         df_fund_q_stock.loc[temp_i, "weight_pre" ] = 0.0
#         df_fund_q_stock.loc[temp_i, "weight_diff" ] = df_fund_q_stock.loc[temp_i, "F_PRT_STKVALUETONAV" ]
#         df_fund_q_stock.loc[temp_i, "weight_diffpct" ] = 1.00
#         df_fund_q_stock.loc[temp_i, "mv_diff" ] = df_fund_q_stock.loc[temp_i, "F_PRT_STKVALUE" ]

#     ###
#     count=count+1 

# df_fund_q_stock.to_csv("D:\\df_fund_q_stock2.csv",encoding="gbk")

# #################################################################################
# ### 数据汇总：按股票总市值前25的机构 持有股票市值(元) F_PRT_STKVALUE || 和市值变动前20 获取基金公司名单
# df_fund_comp = df_fund_q_stock.groupby("fund_comp").sum()
# df_fund_comp["fund_comp"] = df_fund_comp.index()
# df_fund_comp = df_fund_comp.sort_values(by="F_PRT_STKVALUE",ascending=False )
# df_fund_comp_sub = df_fund_comp.iloc[:25,: ]

# # notes:列是基金公司名称，行主要看当期股票市值：
# df_fund_comp.to_csv("D:\\df_fund_comp2.csv",encoding="gbk")

### 对每只股票，计算季度之间的涨跌幅? 

#################################################################################
### 2.4，用股票预期指标进行30%程度的控制
'''分析指标：
1,基金公司汇总组合市值权重：{"mv_diff"}，组合权重的权重{"weight_diff"}市值加权权重{S_VAL_MV}；
2，市盈率{升序，区间【0,100】，EST_PE_FY1}
3，PEG{升序，区间【0,3】，EST_PEG_FY1}
4，ROE{降序，区间【0.05,3】，EST_ROE_FY0}
5，净利润增长率{降序，区间【0.05,3】，NET_PROFIT_YOY}

'''
#################################################################################
### Import existed data 
df_fund_q_stock = pd.read_csv("D:\\df_fund_q_stock2.csv",encoding="gbk")
df_fund_comp = pd.read_csv("D:\\df_fund_comp2.csv",encoding="gbk")
# df_fund_comp_sub = df_fund_comp.iloc[:25,: ]
df_fund_comp_sub = df_fund_comp

#################################################################################
### 2.3,对于每个基金公司维度分组，提取增持股票组合，按照财务预期等指标构建组合
for temp_i in df_fund_comp_sub.index :
    temp_fund_comp = df_fund_comp.loc[temp_i, "fund_comp"]
    df_fund_q_stock_1f = df_fund_q_stock[ df_fund_q_stock["fund_comp"]== temp_fund_comp ]
    print("Debug====== ",temp_fund_comp,len(df_fund_q_stock_1f.index) )
    
    if len( df_fund_q_stock_1f.index ) > 0 :
        #################################################################################
        ### TODO 汇总重复的个股 || notes:单一基金公司内同一股票会有很多重复的：
        ### notes：df_fund_q_stock，df_fund_q_stock_1f里边"S_INFO_WINDCODE"对应基金代码，"S_INFO_STOCKWINDCODE"对应股票代码
        df_1f_stock_sum = df_fund_q_stock_1f.groupby("S_INFO_STOCKWINDCODE").sum()
        df_1f_stock_sum.to_csv("D:\\df_1f_stock_sum.csv")
        ### groupby之后原列会变没，需要重新赋值：
        df_1f_stock_sum["S_INFO_STOCKWINDCODE"] =df_1f_stock_sum.index
        ### 2.4.1,提取相关的个股指标：
        for temp_i in df_1f_stock_sum.index :
            temp_code = df_1f_stock_sum.loc[temp_i,  "S_INFO_STOCKWINDCODE" ]  
            
            #################################################################################
            ### find code in df_abcd3d  ["S_INFO_WINDCODE","S_DQ_MV","S_VAL_MV"] 
            df_abcd3d_sub = df_abcd3d[df_abcd3d["S_INFO_WINDCODE"] == temp_code]
            if len( df_abcd3d_sub.index ) > 0 :                
                ### 1,基金公司汇总组合市值权重：{"mv_diff"}，组合权重的权重{"weight_diff"}市值加权权重{S_VAL_MV}；
                df_1f_stock_sum.loc[temp_i, "S_DQ_MV" ]   = df_abcd3d_sub[ "S_DQ_MV"].values[0]  
                ### 2，市盈率{升序，区间【0,100】，EST_PE_FY1}
                df_1f_stock_sum.loc[temp_i, "EST_PE_FY1" ]   = df_abcd3d_sub[ "EST_PE_FY1"].values[0]  
                ### 3，PEG{升序，区间【0,3】，EST_PEG_FY1}
                df_1f_stock_sum.loc[temp_i, "EST_PEG_FY1" ]   = df_abcd3d_sub[ "EST_PEG_FY1"].values[0]
                ### 4，ROE{降序，区间【0.05,3】，EST_ROE_FY0}
                df_1f_stock_sum.loc[temp_i, "EST_ROE_FY0" ]   = df_abcd3d_sub[ "EST_ROE_FY0"].values[0]  
                ### 5，净利润增长率{降序，区间【0.05,3】，NET_PROFIT_YOY}
                df_1f_stock_sum.loc[temp_i, "NET_PROFIT_YOY" ]   = df_abcd3d_sub[ "NET_PROFIT_YOY"].values[0]  

            else :
                ### 港股什么的就无法识别：
                print("temp_code ",temp_code )
                df_1f_stock_sum.loc[temp_i, "if_HK" ] = 1 

            
        #################################################################################
        ### 2.4.2,计算加权指标 | notes:单一基金公司内同一股票会有很多重复的：
        ### 股票市值和财务等指标
        # notes:有的指标不适合求和，有的指标有负值。
        for temp_col in ["S_DQ_MV"] :
            df_1f_stock_sum["w_"+temp_col ]   = df_1f_stock_sum[temp_col ]/df_1f_stock_sum[temp_col ].sum()
        
        ### 个股持仓市值和权重指标
        for temp_col in ["mv","mv_diff","weight","weight_diff"] :
            df_1f_stock_sum["w_"+temp_col ]   = df_1f_stock_sum[temp_col ]/df_1f_stock_sum[temp_col ].sum()

        df_1f_stock_sum.to_csv("D:\\df_1f_stock_sum_201104.csv")
        #################################################################################
        ### 2.4，用基金指标和股票预期指标进行打分和加权
        '''TODO，保存进基金策略和股票策略模块：
        
        基金股票加权比例：
        1，股票组合权重，60%：其中"mv","mv_diff","weight","weight_diff" 各25%
        2，股票相对市值权重：10%："w_mv"-"w_S_DQ_MV"
        3，财务指标权重，30%：其中"EST_PEG_FY1","EST_ROE_FY0","NET_PROFIT_YOY"各30%，"EST_PE_FY1" 10%
        ''' 
        obj_par ={}
        obj_par["df"] = df_1f_stock_sum

        ### 计算指标得分
        def score_indicator_partition( obj_par): 
            ### 对df内某一列column指标，基于样本数量进行分档。notes：不是基于指标数值的平均10档。
            ''' para_list=[指标名称，分档数量，取值下限，取值上限,越大越好 ]
            para_list=[column_name,num_partition,value_lb,value_ub，if_decending ]
            obj_par 包括 df,para_list
            '''
            col_name = obj_par["para_list"][0]
            num_partition = obj_par["para_list"][1]
            value_lb = obj_par["para_list"][2]
            value_ub = obj_par["para_list"][3]
            if_decending = obj_par["para_list"][4]

            df0 = obj_par["df"]
            # df0 = score_col_quantile(df0,col_name,num_partition )
            ### 判断column取值 是否处于lb和ub之间，如果超过，则用lb或ub替代
            df0["score_"+col_name ] = 0
            for i in range(num_partition) :
                # i=0,1,...9, for num_p=10 
                temp_pct = i+1
                value_quantile = df0[col_name].quantile( (i+1)/num_partition )
                ### value_quantile 取值从小到大。
                ### 新增一列，如果数值越大越好，则i=0对应最低的10%，得分为1;数值越大越好对应if_decending == 1
                if if_decending == 1 : 
                    ### notes：df.apply是对df的每一列的每一个元素进行操作,df[col1].apply是对列col1的每一个元素进行操作
                    ### 每满足一档条件加1
                    df0["score_"+col_name ] = df0["score_"+col_name ] + df0[col_name].apply(lambda x : 1 if x >= value_quantile else 0) 
                else :
                    df0["score_"+col_name ] = df0["score_"+col_name ] + df0[col_name ].apply(lambda x : 1 if x<= value_quantile else 0)


            ### drop for lower bound and upper bound value 
            ### 数值越大越好对应if_decending == 1；因为有可能出现50%的值都小于最小值或都大于最大值
            if if_decending == 1 : 
                # 数值越大越好 |若小于lower bound 则分值归0，若大于upper bound 则分值仍取upperbound
                df0["score_"+col_name ] =df0["score_"+col_name ]* df0[col_name ].apply(lambda x : 0 if x < value_lb else 1 )
                # 获取上限对应的分档值num_partition,因为有可能上限以上的数值对应的不是最高分档10，而是6或者7这样的
                temp_num_par = df0[ df0[col_name]>value_ub ]["score_"+col_name ].min()
                
                df0["score_"+col_name ] = df0["score_"+col_name ].apply(lambda x : temp_num_par if x > temp_num_par else x )
                # df0["score_"+col_name ] + df0[col_name ].apply(lambda x : (temp_num_par-num_partition) if x > value_ub else 0 )
            else :
                # 数值越小越好,例如PE,PEG
                # 获取下限对应的分档值num_partition,因为有可能下限以下的数值对应的不是最高分档10，而是6或者7这样的
                temp_num_par = df0[ df0[col_name] <value_lb ]["score_"+col_name ].min()
                df0["score_"+col_name ] = df0["score_"+col_name ].apply(lambda x : temp_num_par if x > temp_num_par else x )

                # df0["score_"+col_name ] =df0["score_"+col_name ] +  df0[col_name ].apply(lambda x : num_partition if x < value_lb else 1 )
                df0["score_"+col_name ] =df0["score_"+col_name ] *df0[col_name ].apply(lambda x : 0 if x > value_ub else 1  )

            ### save to output
            obj_par["df"] = df0

            return obj_par

        '''基金股票池指标筛选和分档：
        1，股票组合权重
            0，指标名称,分档，取值区间，下限替代，上限替代,备注
            1，"w_"+"mv"，10， [0,~], 0, 1,
            2，"w_"+"mv_diff",10,[-1,1],-1, 1,
            3,"w_"+"weight",10， [0,~], 0, 1,
            4,"w_"+"weight_diff",10,[-1,1],-1, 1,
        2, 股票相对市值权重：10%："w_mv"-"w_S_DQ_MV"
            5,"w_"+ "S_DQ_MV",10， [0,~], 0, 1,
        3,财务类："EST_PE_FY1","EST_PEG_FY1","EST_ROE_FY0","NET_PROFIT_YOY"
            6,"EST_PE_FY1",10, 0, 85, FY1超过80倍的剔除
            7,"EST_PEG_FY1",10, 0, 3, PEG超过3的剔除
            8,"EST_ROE_FY0",10, 0.04, ~,ROE不低于4%
            9,"NET_PROFIT_YOY"，10，0，~， 不为负值
        '''
        # 注意取值区间的设置 
        ### 数值越大越好对应if_decending == 1
        if_decending = 1
        ### "w_mv","w_mv_diff","w_weight","w_weight_diff"
        obj_par["para_list"] = ["w_mv", 10, 0, 1, if_decending ] 
        obj_par = score_indicator_partition(obj_par)
        obj_par["para_list"] = ["w_mv_diff", 10, -1, 1,if_decending ] 
        obj_par = score_indicator_partition(obj_par)
        obj_par["para_list"] = ["w_weight", 10, 0, 1, if_decending ] 
        obj_par = score_indicator_partition(obj_par)
        obj_par["para_list"] = ["w_weight_diff", 10, -1, 1, if_decending ] 
        obj_par = score_indicator_partition(obj_par)
        ### "w_"+ "S_DQ_MV" | "S_DQ_MV"单位是万元， "w_"+ "S_DQ_MV" 介于0.000125~0.174
        obj_par["para_list"] = ["w_S_DQ_MV", 10, 0, 1 , if_decending ] 
        obj_par = score_indicator_partition(obj_par)
        ### "EST_ROE_FY0"取值 -42~47 是百分位下的数字； "NET_PROFIT_YOY"取值 -238~ 3293，也是百分位下的数字
        # 注意取值区间的设置 
        obj_par["para_list"] = ["EST_ROE_FY0", 10, 0, 100 , if_decending ] 
        obj_par = score_indicator_partition(obj_par)
        obj_par["para_list"] = ["NET_PROFIT_YOY", 10, 0, 200 , if_decending ] 
        obj_par = score_indicator_partition(obj_par)

        ### 数值越小越好对应 if_decending == 0 
        if_decending =0 
        ### "EST_PE_FY1"百分位，4.8~540；"EST_PEG_FY1",0.012~25.15
        obj_par["para_list"] = ["EST_PE_FY1", 10, 0, 85 , if_decending ] 
        obj_par = score_indicator_partition(obj_par)
        obj_par["para_list"] = ["EST_PEG_FY1", 10, 0, 3.5 , if_decending ] 
        obj_par = score_indicator_partition(obj_par)
        
        #################################################################################
        ### 2.4，用基金指标和股票预期指标进行打分和加权
        # notes：要注意港股的权重，之间按组合占比给分？或者全部剔除
        '''指标加权打分
        0，总分=组合权重 50% +财务预期 40% + 股票市值 10%
        para_sum=[0.5,0.4,0.1]
        1，财务预期:score_EST_ROE_FY0	score_NET_PROFIT_YOY	score_EST_PE_FY1	score_EST_PEG_FY1
        para_esti= [1,1,0.5,1]
        2, 组合权重类：score_w_mv	score_w_mv_diff	score_w_weight	score_w_weight_diff
        para_w_port= [1,1,1,1]
        3，股票市值类：score_w_S_DQ_MV	
        para_s_mv= [1]
        '''
        para_esti= [1,1,0.5,1]
        df_1f_stock_sum["score_esti"] = para_esti[0]*df_1f_stock_sum["score_EST_ROE_FY0"] +para_esti[1]*df_1f_stock_sum["score_NET_PROFIT_YOY"] +para_esti[2]*df_1f_stock_sum["score_EST_PE_FY1"] +para_esti[3]*df_1f_stock_sum["score_EST_PEG_FY1"]
        df_1f_stock_sum["score_esti"] =df_1f_stock_sum["score_esti"]/df_1f_stock_sum["score_esti"].sum()
        para_w_port= [1,1,1,1]
        df_1f_stock_sum["score_w_port"] = para_w_port[0]*df_1f_stock_sum["score_w_mv"] +para_w_port[1]*df_1f_stock_sum["score_w_mv_diff"] +para_w_port[2]*df_1f_stock_sum["score_w_weight"] +para_w_port[3]*df_1f_stock_sum["score_w_weight_diff"]
        df_1f_stock_sum["score_w_port"] =df_1f_stock_sum["score_w_port"]/df_1f_stock_sum["score_w_port"].sum()
        para_s_mv= [1]
        df_1f_stock_sum["score_s_mv"] = df_1f_stock_sum["score_w_port"]/df_1f_stock_sum["score_w_port"].sum()
        para_sum=[0.5,0.4,0.1]
        df_1f_stock_sum["score_sum"] = para_sum[0]*df_1f_stock_sum["score_w_port"] +para_sum[1]*df_1f_stock_sum["score_esti"] +para_sum[2]* df_1f_stock_sum["score_s_mv"] 
        df_1f_stock_sum["score_sum"] = df_1f_stock_sum["score_sum"]/df_1f_stock_sum["score_sum"].sum()
        
        ### 降序排列，取前100名
        df_1f_stock_sum = df_1f_stock_sum.sort_values(by="score_sum" ,ascending=False )
        ### 最终权重只取前100名
        count = 0 
        for temp_i in df_1f_stock_sum.index : 
            if count<100 :
                df_1f_stock_sum.loc[temp_i,"weight_final"] = df_1f_stock_sum.loc[temp_i,"score_sum"]
            else :
                df_1f_stock_sum.loc[temp_i,"weight_final"] = 0.0
            count =count +1
        
        df_1f_stock_sum["weight_final"] = df_1f_stock_sum["weight_final"]*0.95/df_1f_stock_sum["weight_final"].sum()

        #################################################################################
        ### 3，数据可视化：部分指标列名称切换为中文
        # 证券代码	持仓权重	成本价格	调整日期	证券类型
        df_1f_stock_sum["证券代码"] = df_1f_stock_sum["S_INFO_STOCKWINDCODE"]  
        # 百分位
        df_1f_stock_sum["持仓权重"] = df_1f_stock_sum["weight_final"]
        df_1f_stock_sum["调整日期"] = obj_in["date_abcd3d"]
        df_1f_stock_sum["证券类型"] = "股票"

        #################################################################################
        ### 4，数据导出
        ### 设置输出文件夹和目录，要和策略代码一一匹配。
        '''要素：1，文件目录： CISS_db\\apps\\fund_ana\\stra_fund_001\\20200930\\fund_comp
        '''
        path_output =obj_in["path_output"]       
        file_name = temp_fund_comp+"_"+ "df_1f_stock_sum.csv" 
        df_1f_stock_sum.to_csv(path_output + file_name,encoding="gbk" )
# TODO
asd













