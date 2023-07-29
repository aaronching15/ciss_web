# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo:

功能：管理基金月末、季末基金指标，完全基于windapi获取基金指标数据
###
last 2023 | since 230414
derived from ..
################################################
################################################
指标分类
    0，基金基本信息
    1，绩效分析、持仓分析、基准数据。 
    

################################################
数据分析步骤：
    1，时间：对于更新时间t，确定对应季末时间T和上一季度末时间T-1
        1.1，时间：确定每年基金数据披露日t_report in [1,2,...,T]，给定t日，定位最近的t_report日;
        基金数据的发布时间分析：
        对于每一年，对于0131、0331、0430、0731、0830、1030六个基金数据披露截止时间，要根据披露的基金持仓
        信息补全。基金数据披露截至时间是0430、0830、1030，基本上可以和上述6个对应起来。
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
    

Notes: 
1,
===============================================
'''

from calendar import c
from re import A
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
from config_data import config_data
class_config_data = config_data() 
path_ciss_rc = class_config_data.obj_config["dict"]["path_ciss_rc"]
path_ciss_web = class_config_data.obj_config["dict"]["path_ciss_web"] 
path_fund = class_config_data.obj_config["dict"]["path_fundpool"]
path_fund_indi = class_config_data.obj_config["dict"]["path_fund_indi"] 

# from config_data import config_data_fund_ana
# config_data_fund_ana_1 = config_data_fund_ana()

from data_io import data_io 
data_io_1 = data_io()
from times import times
times_1 = times()
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


################################################### 
###################################################
class fund_indicator():
    def __init__(self ):
        #######################################################################
        ### 导入配置文件对象，例如path_db_wind等
        from config_data import config_data 
        self.obj_config = config_data().obj_config
        self.path_fundpool = self.obj_config["dict"]["path_fundpool"]
        self.path_fund_indi = self.obj_config["dict"]["path_fund_indi"]
        # self.data_io_1 = data_io()
        #######################################################################

    def print_info(self):        
        print(" ")
        ###
        print("manage_fund_indi | 检查基金指标完整性  ")   
        print("get_fund_index_return | 导入基金基准指数收益率 ") 
        print("get_fund_indi_data |给定基金、基准、日期，获取基金指标对应的所有数据指标") 
        print("get_fund_indi_ret_nav |给定基金日期，获取月收益率、复权净值、同类排名等") 
        print("cal_fund_indi | 在基础数据完整的基础上，计算基金所有数据指标 ")  


        # print("cal_basic_port_stock |给定股票列表作为单股票组合，计算收益率等指标 ")        
        # print("cal_fund_port_top10stock |导入部分基金重仓股票，分别构建组合    ")   
        # print("cal_fund_port_top10stock_given| 基于给定的基金代码（在下一个季度），构建top10组合，比 cal_fund_port_top10stock 简单")
        # print("cal_fund_nav_indi |导入基金净值和计算区间业绩指标 ")   
        # print("cal_fund_stockpct_simu |计算股票配置比例和主成分组合拟合 ")   
        # print("cal_fund_skill | 预测能力评估：skill_set，计算三类skill 指标")

    def manage_fund_indi(self,obj_fund):
        ### manage_fund_indi | 检查基金指标完整性
        ### 功能1：根据输入的月末交易日和基金列表，检查所有指标是否有数据，没有则用windapi下载。
        ### 功能2：根据输入的月末交易日和指标列表，检查所有基金是否有数据，没有则用windapi下载。
        ### 功能3：根据开始和结束日期、基金列表、指标列表，用windapi下载数据。
        ### output： 
        ### notes:需要在 "fund_indi_manage.xlsx"设置好 基金代码、基准指数代码、指标。
        
        ################################################################################
        ### 功能1：根据输入的月末交易日和基金列表，检查所有指标是否有数据，没有则用windapi下载
        ### manage_type=1，给定基金列表
        if obj_fund["manage_type"] == "1" :
            ################################################################################
            ### Input：
            temp_date = obj_fund["date_m_end"] 
            ### 方式一：单月末+单个基金代码
            ### 方式二：单月末+基金代码列表 有2个日期，
            list_code = obj_fund["list_code"] 


            ################################################################################
            ### 导入日期数据  获取日期参数：如最近月末、季度末、半年度末数据
            from times import times
            times1 = times()
            if len( str(temp_date ) ) == 8 :
                obj_date = times1.get_date_pre_post( temp_date )
            else :
                ### 取系统最新日期
                obj_date = times1.get_date_pre_post(  )

            ### output: pre 给定日期之前的最近月末和2个季末、2个半年末；str和dt格式
            # obj_date["date_pre_1m_end"] obj_date["date_pre_1m_end_str"]
            # obj_date["date_pre_1q_end_str"] obj_date["date_pre_1q_end"] 
            # obj_date["date_pre_2q_end_str"] obj_date["date_pre_2q_end"] 
            ### 最近4个半年末交易日
            # obj_date["date_pre_1halfyear_end_str"] obj_date["date_pre_1halfyear_end"]
            # obj_date["date_pre_2halfyear_end_str"] obj_date["date_pre_2halfyear_end"]
            ### 最近4个半年末财务报表日 | 半年末一般延迟4个月， 4-30只能获得上一年底的年报，8-31获得半年报
            # obj_date["date_report_pre_1halfyear_str"] ,obj_date["date_report_pre_2halfyear_str"] 
            # obj_date["date_report_pre_3halfyear_str"] ,obj_date["date_report_pre_4halfyear_str"]

            ################################################
            ### 取近13个月末的交易日，按升序排列,date_list_ascend 有13个数据是因为涉及期初和期末日期
            len_m =12            
            # obj_date["date_list_m"] :...20220930, 20221031, 20221130, 20221230, 20230131, 20230228, 20230331]
            date_list_ascend = obj_date["date_list_m"][-1*(1+len_m):]
            ### 按从小到大排序 list1.sort() ;不能list1=list1.sort()
            date_list_ascend.sort() 
            print("date_list_ascend \n", date_list_ascend ) 

            ################################################################################################
            ### 导入基金基准指数收益率
            temp_date = obj_date["date_pre_1m_end_str"] 
            date_pre_2y = obj_date["date_pre_2y_str"] 
            obj_i = {}
            obj_i["date"] = obj_date["date_pre_1m_end_str"] 
            obj_i["len_m"] = len_m
            obj_i["date_list_ascend"] = date_list_ascend
            obj_i = self.get_fund_index_return( obj_i)  
            df_index_excel = obj_i["df_index_excel"]
            

            ################################################################################################
            ### para 参数准备
            ### input变量分别是：当前日期、日期相关数据、月末指数收益率、基金代码列表
            obj_fund["date"] = temp_date
            obj_fund["date_pre_1y"] = obj_date["date_pre_1y_str"] 
            obj_fund["date_pre_2y"] = obj_date["date_pre_2y_str"]             
            obj_fund["date_list_ascend"] = date_list_ascend
            obj_fund["obj_date"] = obj_date
            obj_fund["df_index"] =df_index_excel 
            ### 导入给定月末的基金列表 
            obj_fund["list_code"] = list_code
            
            ################################################################################################
            ### 收益率和净值：给定基金日期，检查和下载最近12个月 取月收益率、复权净值、同类排名等；检查每个基金代码是否有数据
            obj_fund["len_m"] =12
            obj_fund = self.get_fund_indi_ret_nav( obj_fund )

            ### obj_fund["df_nav_chg"]只对应最近1个月的数据
            ### obj_fund["df_nav_chg"] ; saved to file= fund_nav_chg_month_" 文件里需要获取数据的指标
            ### 下载的指标 = ["nav","NAV_adj","return_1m","peer_fund_return_rank_prop_per"]    

            ################################################################################################
            ### 导入或下载所有基金指标 get_fund_indi_data ||  
            
            obj_fund = self.get_fund_indi_data( obj_fund )

            # output： obj_fund["df_fund_indi"] 
            ################################################################################################
            ### 计算所有需要分析的指标
            ### 将计算需要的指数收益率数据放入obj
            obj_fund["df_index"] = df_index_excel
            obj_fund["len_m"] = len_m
            obj_fund["date_list_ascend"] = date_list_ascend
            ###
            obj_fund = self.cal_fund_indi( obj_fund ) 

            ### output obj_fund["df_fund_indi"]



        ################################################################################
        ### 



        
        return obj_fund

    def get_fund_index_return(self,obj_i ):
        ### 导入基金基准指数收益率
        ### 指数代码的列表在sheet=benchmark，file=fund_indi_manage.xlsx
        len_m = obj_i["len_m"]
        date_list_ascend = obj_i["date_list_ascend"]


        from get_wind_api_fund import wind_api_fund
        class_wind_api_fund = wind_api_fund()
        ################################################################################################
        ################################################################################################
        ### 导入或下载基准指数月收益率数据：判断是否已经有该数据
        file_name_bond_index = "bond_index.xlsx"
        if os.path.exists( self.path_fund_indi + file_name_bond_index ) :
            df_index_excel = pd.read_excel( self.path_fund_indi + file_name_bond_index ) 
            ### drop_duplicates(),默认剔除重复行
            df_index_excel = df_index_excel.drop_duplicates()
            if "date" not in df_index_excel.columns :
                df_index_excel["date"] = df_index_excel.index 
            else :
                df_index_excel.index = df_index_excel["date"]
            ### 默认如果该列有日期，只要有1个指数没有数据，那么全部代码都要重新下载  
            ### 检查近12个月的数据是否都有，没有的需要添加
            count_month = 0 
            for i in range( len_m  ) :
                # date_list_ascend共有m+1个月，第一个值是第一个月初的日期，这里只需要月末日期
                temp_date = date_list_ascend[i+1 ]
                # TYPE: temp_date<class 'int'>; df_index.index[0]<class 'int'>
                temp_list = list( df_index_excel.index ) 
                if not int(temp_date ) in temp_list :
                    print("Working on ", temp_date ,"||", temp_list  )
                    ###############################################
                    ### 需要下载该日期有数据 
                    obj_index = {}
                    obj_index["date_start"] = date_list_ascend[i ]
                    obj_index["date_end"] =date_list_ascend[i+1 ]
                    obj_index = class_wind_api_fund.get_index_indi_data( obj_index ) 
                    ### notes: df_index,index是日期，column是代码+日期
                    if "df_index" in obj_index.keys() :
                        ### 说明正常返回数据
                        if count_month == 0 :
                            df_index = obj_index["df_index"] 
                        else :
                            df_index = df_index.append( obj_index["df_index"] ) 
                        count_month = count_month+1  

                    ###############################################
                    ### 不好使：if df_index in locals(): 
                    # UnboundLocalError: local variable 'df_index' referenced before assignment
                    if count_month > 0 :
                        ### 将下载的 df_index 添加到df_index_excel
                        df_index_excel = df_index_excel.append( df_index  )
                        ### 去除重复项，有可能出现index里有相同日期，去除完全重复的行数据
                        df_index_excel = df_index_excel.drop_duplicates()
                        ###############################################
                        ### 保存到excel文件 
                        df_index_excel.to_excel( self.path_fund_indi + file_name_bond_index ,index=0 ) 
        else :
            ###############################################
            ### 给定月末日期，下载所有指数近12个月的收益率 
            count_month = 0 
            for i in range( len_m  ) :
                ### 0,1,2,3,4,5...,11  
                ###############################################
                ### 1,检查指数基准收益率 || 需要对近12个月每个月下载数据
                obj_index = {}
                obj_index["date_start"] = date_list_ascend[i ]
                obj_index["date_end"] =date_list_ascend[i+1 ]
                obj_index = class_wind_api_fund.get_index_indi_data( obj_index ) 
                if "df_index" in obj_index.keys() :
                    ### 说明正常返回数据
                    if count_month == 0 :
                        df_index = obj_index["df_index"] 
                    else :
                        df_index = df_index.append( obj_index["df_index"] ) 
                    count_month = count_month+1  
                
                ###############################################
                ### 保存到excel文件
                ### 去除重复项，有可能出现index里有相同日期，去除完全重复的行数据
                df_index = df_index.drop_duplicates()
                print("type of df_index :", type(df_index ), df_index )
                df_index.to_excel( self.path_fund_indi + file_name_bond_index ,index=0 )
                df_index_excel = df_index

        ###############################################
        ### 
        obj_i["df_index_excel"] = df_index_excel
        return obj_i


    def get_fund_indi_data(self,obj_fund):
        ### 给定基金、基准、日期，获取基金指标对应的所有数据指标
        ####################################################
        ### get_fund_indi_data |给定基金、基准、日期，获取基金指标对应的所有数据指标 
        ### output： 
        temp_date = obj_fund["date"] 
        ### date_pre_2y 用于设置date_start,用于基金经理绩效等指标的计算
        date_pre_2y = obj_fund["date_pre_2y"] 
        obj_date = obj_fund["obj_date"] 
        df_index_excel = obj_fund["df_index"] 
        date_list_ascend = obj_fund["date_list_ascend"] 
        
        ####################################################
        ### 给定月末的基金列表  obj_fund["list_code"] 
        list_code = obj_fund["list_code"] 

        ####################################################
        ### 单独导入基金代码列表sheet=indicators ,file="fund_indi_manage.xlsx"
        temp_file = "fund_indi_manage.xlsx"
        path_file = self.obj_config["dict"]["path_data_pms"]
        df_indicators = pd.read_excel( path_file + temp_file ,sheet_name="indicators")
        ### 筛选有效指标 
        df_indicators = df_indicators[ df_indicators["if_active"] ==1 ] 
        
        ### notes:不能直接在这里获取全部指标，因为有的指标涉及多个时期，需要改名如 xxx,xxx_pre1
        list_indi_all = []
        ### 都是int类型： if_active ， order_formula
        # print("df_indicators \n", df_indicators ) 
        ################################################################################
        ### 导入基金指标数据：判断是否已经有该日期对应的指标数据文件 
        file_name_fund_indi = "fund_indi_bond_" + str(temp_date) + ".xlsx"
        # notes:df_fund_indi 有可能不存在
        if os.path.exists( self.path_fund_indi + file_name_fund_indi ) :
            df_fund_indi = pd.read_excel( self.path_fund_indi + file_name_fund_indi  )  
            df_fund_indi.index = df_fund_indi["code"]
            ### 去除重复项
            df_fund_indi = df_fund_indi.drop_duplicates( subset="code", keep="last"  )
        
        ################################################################################################
        ################################################################################################
        ### 根据设置的不同windapi公式，提取指标数据 
        ### Step 1，获取基金基本信息 | order_formula in [1,2]
        ### sheet=windapi基金指标 ,file=2023Q2-债基分析模板.xlsx || derived from 202303-单只债基分析模板.xlsx
        ''' 例子
        w.wss("270044.OF", "fund_info_name,fund_existingyear,fund_minholdingperiod,fund_ptmyear,fund_benchmark,fund_benchindexcode,fund_setupdate,fund_investtype")

        ''' 
        ### count_order 是用来统计用了几个"order_formula"
        count_order = 0 

        ### notes: 这里有几个数字，取决于 column=order_formula中有几个数字， sheet=indicators，file=fund_indi_manage.xlsx
        ### Debug时，可以只选部分indicator [1,2,3,4,5,6]
        list_number = [1,2,3,4,5,6]
        for temp_order in list_number : 
            ##########################################
            ### temp_order ;order_formula 导入时是 int格式
            df_indicators_sub = df_indicators[ df_indicators["order_formula"] == temp_order ] 
            list_indicator = list( df_indicators_sub["col_name"] )
            # print("df_indicators \n", df_indicators )
            print(temp_order ,"list_indicator \n", list_indicator ) 
            
            ##########################################
            ### 判断要下载的指标是否已经存在:
            ### 逻辑：对list_code里每个代码，先判断是否在df_fund_indi["code"]；如果在，再判断每个指标有没有数据，有1个指标没有，该代码就要下载list_indicator所有指标 
            list_code_modify = []
            ### 要判断指标是否在导入的数据表里 | notes:df_fund_indi 有可能不存在
            if "df_fund_indi" in locals() :
                ##########################################
                ### 对list_code里每个代码，先判断是否在df_fund_indi["code"]
                for temp_code in list_code :  
                    if temp_code in df_fund_indi["code"] :
                        ### 再判断每个指标有没有数据，有1个指标没有，该代码就要下载list_indicator所有指标 
                        count_temp = 0 
                        for temp_indi in list_indicator :
                            if temp_indi not in df_fund_indi.columns :
                                list_code_modify = list_code_modify + [temp_code ]
                            else :
                                ### pd.isnull() 判断dataframe某一个单元格的值是否是空的， np.isnan()遇到非数字格式会报错。
                                if pd.isnull( df_fund_indi.loc[temp_code, temp_indi] ) :
                                    ### notes： df_fund_indi.loc[temp_code, temp_indi] == np.nan 不行，这样无法识别
                                    if count_temp == 0 :
                                        ### 避免重复赋值
                                        list_code_modify = list_code_modify + [temp_code ]
                                        count_temp = 1 
                    else :
                        list_code_modify = list_code_modify + [temp_code ]

            else:
                ### 不存在外部文件，需要下载所有数据
                list_code_modify = list_code
            ##########################################
            ### 去除重复项
            list_code_modify =  list(set( list_code_modify ))

            ### DEBUG 避免过多占用windapi资源，只取前5个 
            # print("DEBUG= list_code_modify", list_code_modify )

            ### 更新 基金列表  obj_fund["list_code"] 
            obj_fund["list_code"] = list_code_modify
            
            
            ##########################################
            ### str_para这一列字符长度最长的那一个
            temp_str_para = ""
            for temp_str in df_indicators_sub["str_para"] :
                print("temp_str=",temp_str,type(temp_str)  )
                ### 判断是否 float类型的nan， 用 np.isnan(temp_str)，其他格式会报错。
                if type( temp_str) == str and len( temp_str)>0 :
                    temp_str_para = temp_str 

            print("Debug temp_str_para=", temp_str_para, " temp_order=", temp_order )
            
            ##########################################
            ### 对参数内的日期变量进行替换，如yyyymmdd_trade，yyyymmdd_start,yyyymmdd_end    
            str_para = ""
            date_type = ""
            if "yyyymmdd_trade" in temp_str_para :
                str_para  =temp_str_para.replace( "yyyymmdd_trade", str(temp_date) )
            ### notes:一个公式里可能同时有 yyyymmdd_start 和 yyyymmdd_end
            if "yyyymmdd_start" in temp_str_para :
                str_para  =temp_str_para.replace( "yyyymmdd_start", str(date_pre_2y) )
                ### 如果有 yyyymmdd_start ，那么默认也会有 yyyymmdd_end
                if "yyyymmdd_end" in temp_str_para :
                    str_para  =str_para.replace( "yyyymmdd_end" , str(temp_date) )
            
            ### 其他日期类型
            if "yyyymmdd_report_pre1" in temp_str_para :
                ### yyyymmdd_report_pre1是最近2期的半年报日期,一般是用于和最近1期的数值比较变化 
                # notes:yyyymmdd_report_pre1 包括了 yyyymmdd_report_pre1,需要再次赋值
                date_type = "pre1"
                print( "最近2个半年末财务报表日", obj_date["date_report_pre_2halfyear_str"] )
                str_para  =temp_str_para.replace( "yyyymmdd_report_pre1" , obj_date["date_report_pre_2halfyear_str"] )
            
            elif "yyyymmdd_report" in temp_str_para :
                ### notes:这里要判断用的是最近1个季末还是半年末| yyyymmdd_report_pre1 包括了 yyyymmdd_report_pre1
                ### 最近4个半年末财务报表日 | 半年末一般延迟4个月， 4-30只能获得上一年底的年报，8-31获得半年报
                # obj_date["date_report_pre_1halfyear_str"] ,obj_date["date_report_pre_2halfyear_str"] 
                # obj_date["date_report_pre_3halfyear_str"] ,obj_date["date_report_pre_4halfyear_str"] 
                print( "最近1个半年末财务报表日", obj_date["date_report_pre_1halfyear_str"] )
                str_para  =temp_str_para.replace( "yyyymmdd_report" , obj_date["date_report_pre_1halfyear_str"] )
                                          
            ##########################################
            ### 剔除str内可能存在的双引号, 获取基础信息时，可能没有参数，即str_para=""
            if len( str_para ) > 8 :
                str_para  = str_para.replace("\"","")
                str_para  = str_para.replace("\'","")
            
            print("Adjusted str_para=", str_para ) 
            ##########################################
            ### 导入模块
            from get_wind_api_fund import wind_api_fund
            class_wind_api_fund = wind_api_fund()

            # for fund_code in list_code:
            obj_f = {}
            ### list_code 是所有要获取指标的基金代码，wss一次对一个fundcode获取所有指标
            obj_f["list_code"] = list_code_modify
            obj_f["date"] = temp_date   
            obj_f["col_list"] = list_indicator 
            obj_f["str_para"] = str_para 
            ### 设置保存到excel
            # print("Debug obj_f=", obj_f )
            
            obj_f = class_wind_api_fund.get_wss_fund_date( obj_f) 

            ########################################## 
            ### 添加api获取的指标到整个列表
            ### 判断是否已经有历史df=df_fund_indi ，and drop duplicates
            if "df_fund_indi" in locals() :
                ### 判断是否df_fund 是否在 obj_f里 
                # notes: 因为新旧df都有code列，用append 会导致df里边错位，相同基金的不同指标在不同行index                    
                if "df_fund" in obj_f.keys() :
                    for temp_code in obj_f["df_fund"].index :
                        ##########################################
                        ### 要判断temp_code 是否在 df_fund_indi.index 里
                        if not temp_code in df_fund_indi.index :
                            df_fund_indi.loc[temp_code, "code"] = temp_code
                        
                        ##########################################
                        ### 用最稳妥的赋值方式
                        ### notes:如果涉及前1、2.期的指标数据，有可能会导致不同时期相同指标的重叠，需要进行判断
                        if date_type in ["pre1"] : 
                            ### list_indicator_modify 包括需要 调整命名的指标，作为列名
                            # 加了pre1 ,新增指标 prt_foundleverage_pre1 
                            for temp_indi in list_indicator :
                                temp_indi_modify = temp_indi+"_"+  date_type 
                                df_fund_indi.loc[temp_code, temp_indi_modify ] =  obj_f["df_fund"].loc[temp_code , temp_indi ] 

                                list_indi_all = list_indi_all + [temp_indi_modify ]
                        else :
                            ### 正常最新日期的情况，原始指标名称就是列名
                            for temp_indi in list_indicator :
                                ### debug 
                                # print("Debug: temp_indi=",temp_indi,temp_code,obj_f["df_fund"].loc[temp_code , temp_indi ]  )
                                # df_fund_indi.to_excel("D:\\df_fund_indi.xlsx")
                                # obj_f["df_fund"].to_excel("D:\\df_fund.xlsx")

                                df_fund_indi.loc[temp_code, temp_indi ] =  obj_f["df_fund"].loc[temp_code , temp_indi ]
                                list_indi_all = list_indi_all + [temp_indi ] 

                    ### 

            else :
                if "df_fund" in obj_f.keys() :
                    df_fund_indi =  obj_f["df_fund"]
            
            ############################################
            ### save to excel
            df_fund_indi= df_fund_indi.drop_duplicates()
            df_fund_indi.to_excel( self.path_fund_indi + file_name_fund_indi )
            count_order = count_order +1  

        ############################################
        ### list_keep 是指标文件中需要保存的所有指标和代码
        list_keep = ["code" ] + list_indi_all
        list_keep = [ i for i in df_fund_indi.columns if i in list_keep ]
        ### df_fund_indi 这里已经剔除了非本次提取的其他历史数据;df_fund_indi_all 包括历史数据
        df_fund_indi_all = df_fund_indi
        df_fund_indi = df_fund_indi.loc[:,  list_keep ]
        ############################################### 
        ### 赋值给 obj_fund
        ### notes：df_fund_indi 这里已经剔除了非本次提取的其他历史数据;df_fund_indi_all 包括历史数据
        obj_fund["df_fund_indi_all"] = df_fund_indi_all
        obj_fund["df_fund_indi"] = df_fund_indi
        obj_fund["path_out"] = self.path_fund_indi + file_name_fund_indi


        return obj_fund

    def get_fund_indi_ret_nav(self,obj_fund ):
        ### 给定基金日期，获取月收益率、复权净值、同类排名等
        ### 导入或下载基金月收益率数据：判断是否已经有该数据
        ### 参考funds import fund_exhi；fund_exhi.get_fund_exhi(obj_f)             
        ### input:
        date_list_ascend = obj_fund["date_list_ascend"] 
        list_code = obj_fund["list_code"] 
        ### output:obj_fund["df_nav_chg"]

        from get_wind_api_fund import wind_api_fund
        class_wind_api_fund = wind_api_fund()
        from get_wind_api import wind_api
        class_wind_api = wind_api()
        
        ### 需要匹配过去12个月的数据
        ### 给定月末日期，下载所有指数近12个月的收益率,分别按月末保存数据文件
        len_m= obj_fund["len_m"]  
        count_month = 0 
        
        ### 设置 "fund_nav_chg_month_" 文件里需要获取数据的指标
        list_indicator = ["nav","NAV_adj","return_1m","peer_fund_return_rank_prop_per"] 
        for i in range( len_m  ) :
            ### 0,1,2,3,4,5...,11  
            date_start = date_list_ascend[i ]
            date_end   = date_list_ascend[i+1 ] 
            print("Month =", date_end )
            ###############################################
            ### 判断是否已经有数据文件； 如果有，还要判断是否基金列表里所有基金的记录
            fund_nav_chg_month = "fund_nav_chg_month_" + str( date_end ) + ".xlsx"
            print("Working on file=",fund_nav_chg_month ) 
            if os.path.exists( self.path_fund_indi + fund_nav_chg_month ) :
                ### notes:如果这里有index_col=0,会导致nav变成index，每次都要重复下载所有数据
                df_index_excel = pd.read_excel( self.path_fund_indi + fund_nav_chg_month ) 
                if "Unnamed: 0" in df_index_excel.columns:
                    df_index_excel = df_index_excel.drop( ["Unnamed: 0"] , axis=1)
                if "code" in df_index_excel.columns :
                    df_index_excel.index = df_index_excel["code"] 
                else :
                    df_index_excel["code"] = df_index_excel.index
                ### drop_duplicates(),默认剔除重复行
                df_index_excel = df_index_excel.drop_duplicates()
                ### 去除重复 code
                df_index_excel = df_index_excel.drop_duplicates(subset="code", keep="last"  )

                # print("Debug=df_index_excel, \n", df_index_excel.T )  
                ###############################################
                ### 判断是否基金列表里所有基金的记录.
                list_include = []
                list_exist = list( df_index_excel["code"] )
                for temp_code in list_code:
                    if temp_code in list_exist :
                        temp_i = df_index_excel[df_index_excel["code"] == temp_code ].index[0]
                        ###############################################
                        ### 判断要分析的指标是否有数值，如果有一项未空值，则重新下载，不加入 剔除列表
                        if_include = 0 
                        for temp_col in list_indicator:
                            ### pd.isnull() 判断dataframe某一个单元格的值是否是空的
                            if pd.isnull( df_index_excel.loc[temp_i, temp_col] ) :
                                ### 只要有一个指标是空的，就删除这一列，重新下载数据 
                                if_include= 1 
                        ###############################################
                        ### 对于当前code，判断完了所有指标
                        ### drop current index
                        if if_include == 1 :
                            df_index_excel = df_index_excel.drop(temp_i, axis=0 ) 
                            list_include = list_include + [ temp_code ]
                    else:
                        ### 如果不存在该代码，则自动加入
                        list_include = list_include + [ temp_code ]

                ###############################################
                ### 如果 list_include 长度大于0，才下载，否则不用下载数据
                if len( list_include ) > 0 : 
                    print("Debug 2,list of code to download=", list_include )   
                    ###############################################
                    ### 对给定的code list,每次下载1个指标，并添加到 df_data
                    df_data_raw = pd.DataFrame( list_include, columns=["code" ]  )
                    for temp_col in list_indicator :
                        obj_data = {}
                        obj_data["trade_date"] = date_end
                        obj_data["date_start"] = date_start
                        obj_data["date_end"] = date_end
                        ### 必须有一列是 code
                        obj_data["df_data"] = df_data_raw
                        # col_name是输出df中column的名称，默认可以和windapi指标一样，indicator_name
                        obj_data["col_name"] = temp_col
                        obj_data["indicator_name"] = temp_col
                        
                        obj_data = class_wind_api.get_wss_fund_nav_rank( obj_data) 
                        ### 更新 df_data_raw
                        df_data_raw = obj_data["df_data"] 
                        
                    ###############################################
                    ### 所有指标下载后，将数据保存到 df_index_excel 
                    ### 并添加df_data到输入的df_index_excel里
                    df_index_excel = df_index_excel.append(  df_data_raw) 

                    list_keep = list_indicator + ["code"] 
                    df_data = df_index_excel.loc[:, list_keep]
                    ### 用merge容易出错
                    # df_data = df_index_excel.merge(  df_data,how="left", left_index=True, right_index=True)   
                
                ###############################################
                ### 检查数据清洁度
                list_del = []
                for temp_i in df_index_excel.index :
                    # print("Debug=",df_index_excel.loc[temp_i, "code"],type(df_index_excel.loc[temp_i, "code"]) )
                    if not type( df_index_excel.loc[temp_i, "code"] ) == str : 
                       list_del =  list_del +[temp_i ] 
                df_data = df_index_excel.drop( list_del, axis=0   ) 
                ### 去除重复index
                df_data = df_data.drop_duplicates(subset="code", keep="last"  )


            else:
                ###############################################
                ### 不存在本地文件数据
                ### wss更适合，wss是多个基金每次1个指标；  wsd模式是给定区间，下载一只基金的多个日期的多个指标 
                #用 get_wss_fund_nav_rank(),好像直接就可以保存基金月度净值和月收益率数据 
                ### 不包含区间 nav	NAV_adj	return_1m,return_1m指标需要annualized=0，表示不需要年化计算
                ### 包含区间 peer_fund_return_rank_prop_per
                df_data= pd.DataFrame( list_code, columns=["code" ]  )
                for temp_col in list_indicator :
                    obj_data = {}
                    obj_data["trade_date"] = date_end
                    obj_data["date_start"] = date_start
                    obj_data["date_end"] = date_end
                    ### 必须有一列是 code
                    obj_data["df_data"] = df_data
                    # col_name是输出df中column的名称，默认可以和windapi指标一样，indicator_name
                    obj_data["col_name"] = temp_col
                    obj_data["indicator_name"] = temp_col

                    obj_data = class_wind_api.get_wss_fund_nav_rank( obj_data)
                    ### 单次提取的数据保存到 df_data 
                    df_data = obj_data["df_data"]
            
            ###############################################
            ### 每个月数据搜集完成后，保存到excel文件
            count_month = count_month + 1 
            ### 判断df_data如果不在变量，说明不需要下载
            if "df_data" in locals() :
                df_data.to_excel( self.path_fund_indi + fund_nav_chg_month, index=0 )   

        ###############################################
        ### save to output，保存最新一个月的基金净值数据
        ### notes:这里对应的是最新日期的净值和月收益率数据
        obj_fund["df_nav_chg"] = df_data


        return obj_fund

    def cal_fund_indi(self,obj_fund ):
        ################################################################################
        ### cal_fund_indi | 在基础数据完整的基础上，计算基金所有数据指标 
        ### reference:sheet=计算过程,file=202303-单只债基分析模板.xlsx
        
        ### notes:df_fund_indi 这里已经剔除了非本次提取的其他历史数据;df_fund_indi_all 包括历史数据
        df_fund_indi_all = obj_fund["df_fund_indi_all"]    
        df_fund_indi = obj_fund["df_fund_indi"]    
        list_keep = list( df_fund_indi.columns )
        ### list_code是所有需要计算指标的代码列表； 输入的 obj_fund["list_code"] 已经被调整后的 list_code_modify 替代
        list_code = obj_fund["list_code"]
        
        ### 导入基金月收益率数据: df_nav_chg 是净值和收益率数据
        df_nav_chg = obj_fund["df_nav_chg"] 
        df_index = obj_fund["df_index"]
        df_index.index = df_index["date"]

        len_m = obj_fund["len_m"]
        date_list_ascend = obj_fund["date_list_ascend"]

        ################################################################################
        ### 计算过去12个月的相关相对收益率指标
        list_indi = []
        #####################################################
        ### 配置基准指数
        ### 利率债、信用债、可转债、货币基金指数
        code_index_rate = "CBA02501.CS"
        code_index_credit = "CBA02701.CS"
        code_index_convert = "000832.CSI"
        code_index_moneymarket = "CBA02201.CS"
        ### 利率债-短 利率债-长 利率债-中
        code_index_rate_long = "CBA02551.CS"
        code_index_rate_mid = "CBA02531.CS"
        code_index_rate_short = "CBA02521.CS"
        ### 信用债-短 信用债-长 信用债-中
        code_index_credit_long = "CBA02751.CS"
        code_index_credit_mid = "CBA02731.CS"
        code_index_credit_short = "CBA02721.CS"

        #####################################################
        ### 按过去12个月逐个导入数据 
        count_month = 0 
        for i in range( len_m  ) :
            ### date_list_ascend共有m+1个月，第一个值是第一个月初的日期，这里只需要月末日期
            temp_date = date_list_ascend[i+1 ]

            # print("1 debug df_index\n", df_index)     
            ### notes： temp_date 这里一般是 str格式，df_index.index的值有可能是str, 也有可能是 'numpy.int64'
            if not temp_date in df_index.index :
                temp_date = int( temp_date  )
            print("temp_date=",temp_date ," code_index_rate", code_index_rate , temp_date in df_index.index )

            ### 利率债 月超额收益-利率债顺境
            temp_index_rate = df_index.loc[temp_date, code_index_rate ] 
            temp_index_credit = df_index.loc[temp_date, code_index_credit ] 
            temp_index_convert = df_index.loc[temp_date, code_index_convert ] 
            temp_index_moneymarket = df_index.loc[temp_date, code_index_moneymarket ] 
            ### 取长、中、短期的最大值，用于判断久期管理能力
            temp_index_rate_max = max( df_index.loc[temp_date, code_index_rate_long ] ,df_index.loc[temp_date, code_index_rate_mid ]   )
            temp_index_rate_max = max( temp_index_rate_max, df_index.loc[temp_date, code_index_rate_short ] )
            temp_index_credit_max = max( df_index.loc[temp_date, code_index_credit_long ] ,df_index.loc[temp_date, code_index_credit_mid ]   )
            temp_index_credit_max = max( temp_index_credit_max, df_index.loc[temp_date, code_index_credit_short ] )
            ### 加杠杆收益 =国开债1-3 - 货币基金 
            temp_rate_leverage = df_index.loc[temp_date, code_index_rate_short ] - df_index.loc[temp_date, code_index_moneymarket ] 
                        
            ################################################################################
            ### 计算利率、信用市场顺境、逆境收益能力、久期管理能力、加杠杆收益
            list_indi = ["return_rate_positive","return_rate_negative","return_credit_positive","return_credit_negative"]
            list_indi = list_indi +["return_convert_positive","return_convert_negative" ]
            list_indi = list_indi +["rate_duration_skill","credit_duration_skill","return_leverage" ]
            ### list_fund没用，是给定月末的所有基金代码，这里要计算的只有给定的 list_code || list_fund = list( df_nav_chg.index )
            ### list_code 是所有需要计算指标的代码列表； 输入的 obj_fund["list_code"] 已经被调整后的 list_code_modify 替代
            for temp_fund in list_code : 
                ### 月收益率对应指标为 return_1m
                temp_fund_ret = df_nav_chg.loc[temp_fund, "return_1m" ]
                # notes: 有的基金会没有近1个月收益率数据
                try :
                    ##########################################
                    ### 利率债
                    if temp_index_rate >= 0 :
                        df_nav_chg.loc[temp_fund, "return_rate_positive" ] = temp_fund_ret - temp_index_rate
                    else :
                        df_nav_chg.loc[temp_fund, "return_rate_negative" ] = temp_fund_ret - temp_index_rate

                    ### 信用债
                    if temp_index_credit >= 0 :
                        df_nav_chg.loc[temp_fund, "return_credit_positive" ] = temp_fund_ret - temp_index_credit
                    else :
                        df_nav_chg.loc[temp_fund, "return_credit_negative" ] = temp_fund_ret - temp_index_credit
    
                    ### 可转债
                    if temp_index_convert >= 0 :
                        df_nav_chg.loc[temp_fund, "return_convert_positive" ] = temp_fund_ret - temp_index_convert
                    else :
                        df_nav_chg.loc[temp_fund, "return_convert_negative" ] = temp_fund_ret - temp_index_convert
                    ##########################################
                    ### 利率债，信用债久期管理能力	基金收益率减去不同久期的最大值，判断收益率是否持续高于最大值                
                    df_nav_chg.loc[temp_fund, "rate_duration_skill" ] = temp_fund_ret - temp_index_rate_max
                    df_nav_chg.loc[temp_fund, "credit_duration_skill" ] = temp_fund_ret - temp_index_credit_max
                
                    ##########################################
                    ### 加杠杆收益 =国开债1-3 - 货币基金 
                    df_nav_chg.loc[temp_fund, "return_leverage" ] = temp_fund_ret - df_index.loc[temp_date, code_index_rate_short ] -temp_rate_leverage 

                except:
                    print("Error calculating temp_fund=", temp_fund ,"temp_index_rate=", temp_index_rate)
            ################################################################################
            ### 6~12个月保存到同一个df里
            ### save to excel
            file_nav_chg_month = "fund_nav_chg_month_" + str( temp_date ) + ".xlsx"
            df_nav_chg.to_excel( self.path_fund_indi + file_nav_chg_month )
            ### for debug use 
            # df_nav_chg.to_excel("D:\\df_nav_chg.xlsx")
            if count_month == 0 :
                df_nav_chg["date"] = temp_date
                df_nav_chg_all = df_nav_chg
                
            else :
                df_nav_chg["date"] = temp_date
                df_nav_chg_all = df_nav_chg_all.append( df_nav_chg )
            ### 
            count_month = count_month+1

        ################################################################################
        ### 数据先保存到全历史 df_fund_indi_all 里，最后再转成 df_fund_indi= df_fund_indi_all.loc[list_code, :]
        ### 12个月都计算完后，统计平均数并保存指标list_indi到文件df_fund_indi里。
        for temp_fund in list_code :
            df_sub = df_nav_chg_all[ df_nav_chg_all["code"] == temp_fund ]
            for temp_indi in list_indi :
                df_fund_indi_all.loc[temp_fund, temp_indi ] =df_sub[temp_indi].mean()

            ### save to excel
            file_name_fund_indi = "fund_indi_bond_" + str(temp_date) + ".xlsx"
            df_fund_indi_all.to_excel( self.path_fund_indi + file_name_fund_indi )

        ################################################################################
        ### 同指标跨日期计算，如基金杠杆率 prt_foundleverage - prt_foundleverage_pre1
        ### 基金杠杆率变化 prt_foundleverage_diff
        if "prt_foundleverage_pre1" in df_fund_indi_all.columns :
            df_fund_indi_all.loc[list_code,"prt_foundleverage_diff"] = df_fund_indi_all.loc[list_code,"prt_foundleverage"] - df_fund_indi_all.loc[list_code,"prt_foundleverage_pre1"] 
            df_fund_indi_all.loc[list_code,"prt_foundleverage_chg"] = df_fund_indi_all.loc[list_code,"prt_foundleverage_diff"] / df_fund_indi_all.loc[list_code,"prt_foundleverage_pre1"] 

            ### 杠杆管理能力 return_leverage_skill = 基金杠杆率变化 prt_foundleverage_diff * 加杠杆收益 return_leverage_skill || 降杠杆时看（国开债-货币基金） 
            # 加杠杆收益 return_rate_positive = 基金收益率- 基准收益率 -（国开债1-3 - 货币基金); CBA02521.CS - CBA02201.CS
            for temp_fund in list_code : 
                df_fund_indi_all.loc[temp_fund, "return_leverage_skill" ] = df_fund_indi_all.loc[temp_fund, "prt_foundleverage_diff" ] * df_fund_indi_all.loc[temp_fund, "return_leverage" ]
                ### 
                print("Debug,  return_leverage_skill =",df_fund_indi_all.loc[temp_fund, "return_leverage_skill" ]   )

            ### save to excel
            file_name_fund_indi = "fund_indi_bond_" + str(temp_date) + ".xlsx"
            df_fund_indi_all.to_excel( self.path_fund_indi + file_name_fund_indi )

        ################################################################################
        ### 水平因子,斜率因子,凸性因子,信用因子,违约因子,转债因子,货币因子,  等
        ### 水平因子,CBA02501.CS,国开债总财富	CBA02501.CS
        temp_code_index = "CBA02501.CS"
        if temp_code_index in df_index.columns :
            for temp_fund in list_code : 
                df_fund_indi_all.loc[temp_fund, "factor_benchmark" ] = df_index.loc[temp_date, temp_code_index ] 

            ### save to excel
            file_name_fund_indi = "fund_indi_bond_" + str(temp_date) + ".xlsx"
            df_fund_indi_all.to_excel( self.path_fund_indi + file_name_fund_indi )

        #################################################### 
        ### 斜率因子,中债总财富7-10 - 中债总财富1-3	CBA00321.CS - CBA00351.CS
        temp_code_index = "CBA00321.CS"
        temp_code_index_2 = "CBA00351.CS"
        if temp_code_index in df_index.columns and temp_code_index_2 in df_index.columns:
            for temp_fund in list_code : 
                df_fund_indi_all.loc[temp_fund, "factor_duration" ] = df_index.loc[temp_date, temp_code_index ] - df_index.loc[temp_date, temp_code_index_2 ] 
            ### save to excel
            file_name_fund_indi = "fund_indi_bond_" + str(temp_date) + ".xlsx"
            df_fund_indi_all.to_excel( self.path_fund_indi + file_name_fund_indi )

        #################################################### 
        # 凸性因子,0.25*(国开债1-3-国开债3-5-0.5*(国开债3-5-国开债5-7)+0.25*(国开债5-7-国开债7-10)
        # factor_convexity	0.25*(CBA02521.CS-CBA02531.CS-0.5*(CBA02531.CS-CBA02541.CS)+0.25*(CBA02541.CS-CBA02551.CS) 
        temp_code_i1 = "CBA02521.CS"
        temp_code_i2 = "CBA02531.CS"
        temp_code_i3 = "CBA02541.CS"
        temp_code_i4 = "CBA02551.CS"
        if temp_code_i1 in df_index.columns and temp_code_i2 in df_index.columns:
            for temp_fund in list_code : 
                df_fund_indi_all.loc[temp_fund, "factor_convexity" ] =0.25*( df_index.loc[temp_date, temp_code_i1 ] - df_index.loc[temp_date, temp_code_i2 ] )  
                df_fund_indi_all.loc[temp_fund, "factor_convexity" ] =df_fund_indi_all.loc[temp_fund, "factor_convexity" ] -0.5*( df_index.loc[temp_date, temp_code_i2 ] - df_index.loc[temp_date, temp_code_i3 ] )  
                df_fund_indi_all.loc[temp_fund, "factor_convexity" ] =df_fund_indi_all.loc[temp_fund, "factor_convexity" ] +0.25*( df_index.loc[temp_date, temp_code_i3 ] - df_index.loc[temp_date, temp_code_i4 ] )  
            ### save to excel
            file_name_fund_indi = "fund_indi_bond_" + str(temp_date) + ".xlsx"
            df_fund_indi_all.to_excel( self.path_fund_indi + file_name_fund_indi )
        
        ####################################################      
        # 信用因子,企业债总财富-国开债总财富	CBA02001.CS - CBA02501.CS
        temp_code_i1 = "CBA02001.CS"
        temp_code_i2 = "CBA02501.CS"
        for temp_fund in list_code : 
            df_fund_indi_all.loc[temp_fund, "factor_credit" ] = df_index.loc[temp_date, temp_code_i1 ] - df_index.loc[temp_date, temp_code_i2 ]  
          

        #################################################### 
        # 违约因子,高收益企业债总财富-企业债总财富	CBA03801.CS - CBA02001.CS
        temp_code_i1 = "CBA03801.CS"
        temp_code_i2 = "CBA02001.CS"
        for temp_fund in list_code : 
            df_fund_indi_all.loc[temp_fund, "factor_default" ] = df_index.loc[temp_date, temp_code_i1 ] - df_index.loc[temp_date, temp_code_i2 ]  
        
        #################################################### 
        # 转债因子,中证可转债 - 国开债总财富	000832.CSI - CBA02501.CS
        temp_code_i1 = "000832.CSI"
        temp_code_i2 = "CBA02501.CS"
        for temp_fund in list_code : 
            df_fund_indi_all.loc[temp_fund, "factor_convert" ] = df_index.loc[temp_date, temp_code_i1 ] - df_index.loc[temp_date, temp_code_i2 ]  
        
        #################################################### 
        # 货币因子,货币基金可投债总财富	CBA02201.CS
        for temp_fund in list_code : 
            df_fund_indi_all.loc[temp_fund, "factor_convert" ] = df_index.loc[temp_date, "CBA02201.CS" ] 
        
        
        ################################################################################
        ### notes:计算过程可能会有一些无用column如“code.1，code.1.1。。。” ，需要剔除
        list_keep = list_keep + list_indi
        list_keep = [ i for i in df_fund_indi_all.columns if i in list_keep ]
        
        ### save to excel
        file_name_fund_indi = "fund_indi_bond_" + str(temp_date) + ".xlsx"
        df_fund_indi_all.to_excel( self.path_fund_indi + file_name_fund_indi )
        
        ################################################################################
        ### 数据先保存到全历史 df_fund_indi_all 里，最后再转成 df_fund_indi= df_fund_indi_all.loc[list_code, :]
        df_fund_indi= df_fund_indi_all.loc[list_code, :]
        ### 
        obj_fund["df_fund_indi"] = df_fund_indi
        obj_fund["df_fund_indi_all"] = df_fund_indi_all

        return obj_fund 


######################################################################################################
### BEFORE
######################################################################################################

 