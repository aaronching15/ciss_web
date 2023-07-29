# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
TODO：对A股各类财务指标进行分析和计算

功能：对A股各类财务指标进行分析和计算
last  | since 201025

Class列表：
父类：class analysis_indicators_financial():
子类1：class indicators_financial():A股财务指标 
    3，个股：价格和成交量；ashares_stock_price_vol
    4，个股：财务和财务预测指标；ashares_stock_funda
    5，个股：股东、机构投资者、收购兼并等事件；ashares_stock_holder_events
    6，基金、机构指标和因子；ashares_fund_nav_port 
子类2：class analysis_factor_financial():财务因子数据分析     
        print("指标和因子数据处理") 
        print("indicator_data_adjust_zscore  |指标数据清洗调整：去异常值和缺失值；标准化") 
        print("indicator_indicator_orthogonal  |因子指标正交处理") 
        print("indicator_indicator_icir  |因子指标IC和ICIR计算 ") 
子类3： 

classification:财务指标和因子匹配
    财务指标分类	因子
    收入和利润	成长
    现金流	质量
    费用管理	质量
    资产和投资收益	价值
    负债和利息	质量


Notes: 
related files: 
0，财务指标管理：file=wds_表格列匹配.xlsx;path=C:\ciss_web\CISS_rc\apps\rc_data
1,财务指标文件：file=wds_table_column_financial_indicators.csv,path=C:\ciss_web\CISS_rc\config

===============================================
'''
import sys,os
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

# sys.path.append("..")
### Import config
from config_data import config_data
config_data_1 = config_data()
from config_indicator import config_indi_financial
config_indi_financial_1 = config_indi_financial()

from data_io_financial_indicator import data_io_financial_indicator
data_io_financial_indicator_1 = data_io_financial_indicator()
# 交易日list： data_io_financial_indicator_1.obj_data_io["dict"]["tradingday"]
# 交易周list： data_io_financial_indicator_1.obj_data_io["dict"]["date_list_tradingday"]

#########################################################
class indicators_financial():
    def __init__(self ):
        #########################################################
        ### 早期对象地址
        ######################################################################
        ### level 1 目录； ['C:\\zd_zxjtzq\\ciss_web', 
        #  设置数据文件位置
        self.path_ciss_web = config_data_1.obj_config["dict"]["path_ciss_web"]
        self.file_path_admin = self.path_ciss_web + "apps\\rc_data\\"

        ######################################################################
        ### level 1 目录:设置db_wind 数据文件位置，只读取数据
        self.path0 = config_data_1.obj_config["dict"]["path_db_wind"]
        ### level 2 目录
        self.path_wind_adj = config_data_1.obj_config["dict"]["path_wind_adj"]
        self.path_wind_wds = config_data_1.obj_config["dict"]["path_wind_wds"]

        ### level 3 目录
        # 导入Wind全历史行业分类数据 || df_600151.SH
        self.path_wind_adj_ind = config_data_1.obj_config["dict"]["path_rc_ind"] 

        ######################################################################

    def print_info(self):
        ### print all modules for current script
        print("### 标准化计算模块================================================") 
        ### 个股季度财务指标整理和计算：例如多季度数据合并、均值、波动率等
        print("cal_indicators_financial | 个股季度财务指标整理和计算 ") 

        ### 个股年度卖方预测财务指标整理和计算：例如多季度数据合并、均值、波动率等
        print("cal_indicators_estimate | 卖方预测财务指标整理和计算 ") 
        
        print("### 标准化计算模块================================================")
        ### 计算差异指标：c=a/b-1
        print("cal_diffpct | 计算差异指标：c=a/b-1")

        print(" ")
        return 1 

    def cal_indicators_financial(self,obj_fi):
        ### 个股季度财务指标整理和计算

        ### 初始信息
        para_date_N = obj_fi["dict"]["para_date_N"] 
        date_list_q_N = obj_fi["dict"]["date_list_q_N"]
        date_list_y_N = obj_fi["dict"]["date_list_y_N"]
        df_col_fi_indi_CN_factor = obj_fi["dict"]["df_col_fi_indi_CN_factor"] 
        # col_list_basic=["S_INFO_WINDCODE","WIND_CODE","ANN_DT","REPORT_PERIOD" ]
        # 
        TODO




        return obj_fi

    def cal_indicators_estimate(self,obj_fi):
        ### 卖方预测财务指标整理和计算 
        # 分析师年度预测净利润的季度化分解方法
        '''In obj_fi:
        AShareEarningEst 对应df=df_esti_detail
        AShareConsensusData 对应df=df_esti_stat
        CONSEN_DATA_CYCLE_TYP:综合值周期类型: 263001000:30天 263002000:90天 263003000:180天 263004000:大事后180天
        steps:1,提取最新盈利预测明细和时间：if_esti_last,date_esti_last ;
        2,最新预测和30天平均值比较，看差异，前缀"diffpct_last_30d" ;
        3,30d vs 90d
        notes:AShareConsensusData单日一般只有几十到200,400个股票，因此需要提取最近30个交易日的数据
        参考 file=20200930-国信证券-国信证券金融工程专题研究：超预期投资全攻略.pdf
        '''
        ##################################################################################
        ### 导入当日股票列表
        df_ashare = obj_fi["df_ashare"]

        ##################################################################################
        ### AShareEarningEst指标：
        # 1,预测报告期,REPORTING_PERIOD,20201231;
        # 报告类型 REPORT_TYPECODE:{基本只有4002和一点4001} 806004002（公司类型），还会加工806004003（行业研究）806004005（策略研究）以及 806004001（晨会纪要）
        df_esti_detail = obj_fi["df_esti_detail"]       

        ### AShareConsensusData指标：
        # 1，预测报告期，EST_REPORT_DT,20211231;需要匹配；2，预测年度类型，S_EST_YEARTYPE,FY0,FY1；3，预测基准股本综合值，S_EST_BASESHARE
        df_esti_stat = obj_fi["df_esti_stat"] 
        ### 263001000:30天 | 例子：201016,单个披露日期看，30天统计值有的股票数量172个。 
        # 若提取最近30天的30天统计值，则30天和90天统计值分别有1712和1406个。
        df_esti_stat_30d = df_esti_stat[ df_esti_stat["CONSEN_DATA_CYCLE_TYP"]== 263001000 ]
        ### 剔除多只股票
        df_esti_stat_30d = df_esti_stat_30d.drop_duplicates(subset=["S_INFO_WINDCODE"],keep="last")
        
        ### 263002000:90天
        df_esti_stat_90d = df_esti_stat[ df_esti_stat["CONSEN_DATA_CYCLE_TYP"]== 263002000 ]
        ### 剔除多只股票
        df_esti_stat_90d = df_esti_stat_90d.drop_duplicates(subset=["S_INFO_WINDCODE"],keep="last")
        
        # print("Check number of stock for 30,90days ",len( df_esti_stat_30d.index ) ,len( df_esti_stat_90d.index )  )
        ##################################################################################
        ### 1,对股票池的每只股票,获取近30,90天的预测数据，提取几个指标：
        # code_list = df_ashare["S_INFO_WINDCODE"].to_list()
        ### 1.1,提取最新盈利预测明细和时间：if_esti_last,date_esti_last ;
        for temp_i in df_ashare.index :
            temp_code =df_ashare.loc[temp_i,"S_INFO_WINDCODE" ]
            ### 获取最近的盈利预测 
            df_esti_detail_1 = df_esti_detail[ df_esti_detail["S_INFO_WINDCODE"] == temp_code ]
            ### 获取最近的盈利统计30，90days 
            df_esti_stat_30d_1 = df_esti_stat_30d[ df_esti_stat_30d["S_INFO_WINDCODE"] == temp_code ]
            df_esti_stat_90d_1 = df_esti_stat_90d[ df_esti_stat_90d["S_INFO_WINDCODE"] == temp_code ]
            print("Check number of stock for 30,90days ",len( df_esti_stat_30d_1.index ) ,len( df_esti_stat_90d_1.index ) ,len(df_esti_detail_1.index ) )

            if_pass = 0
            if len(df_esti_detail_1.index) >= 1 :
                ### 有盈利预测明细数据 
                if len( df_esti_stat_30d_1.index ) >= 1 :
                    ### 近30天有预测数据
                    if_pass = 1 
                else :
                    if len( df_esti_stat_90d_1.index ) >= 1 :
                        ### 近90天有预测数据
                        if_pass = 1 
                        df_esti_stat_30d_1 = df_esti_stat_90d_1

            ### 可以计算数据的情况
            if if_pass == 0 :
                df_ashare.loc[temp_i,"if_esti_last" ] = 0
            else :
                df_ashare.loc[temp_i,"if_esti_last" ] = 1 
                ### notes:同一个日期"EST_DT"可能会有多个预测数据，df.sort_values(by= 默认升序排列
                # 第一次更新日期：FIRST_OPTIME,默认升序排列，改成降序排列
                df_esti_detail_1 = df_esti_detail_1.sort_values(by="FIRST_OPTIME",ascending=False )
                # 取第一行
                df_esti_detail_1 = df_esti_detail_1.iloc[1,:]
                df_ashare.loc[temp_i,"date_esti_last" ] = df_esti_detail_1["EST_DT"] 

                ##################################################################################
                ### 预测净利润(万元)	EST_NET_PROFIT	|| 净利润平均值(万元)	NET_PROFIT_AVG
                '''
                type_financial	factor	esti_detail_CN	esti_detail	esti_stat_CN	esti_stat	type_stat	indi_prefix
                收入和利润	成长	预测净利润(万元)	EST_NET_PROFIT	净利润平均值(万元)	NET_PROFIT_AVG	平均值	esti_diffpct_
                收入和利润	成长	预测主营业务收入(万元)	EST_MAIN_BUS_INC	主营业务收入平均值(万元)	MAIN_BUS_INC_AVG	平均值	esti_diffpct_
                notes:市盈率PE指标需要自己用预测净利润计算，
                '''
                df_col_mathch_estimate = config_data_1.obj_config["df_col_mathch_estimate"]
                for temp_i in df_col_mathch_estimate.index :
                    # "EST_NET_PROFIT"
                    temp_col = df_col_mathch_estimate.loc[temp_i,"esti_detail" ] 
                    # "NET_PROFIT_AVG"
                    temp_col_stat = df_col_mathch_estimate.loc[temp_i,"esti_stat" ]  
                    #  指标前缀= "esti_diffpct_" ,"_avg" | "_median" | "_std"
                    temp_col_diffpct = df_col_mathch_estimate.loc[temp_i,"indi_prefix" ] + temp_col +  df_col_mathch_estimate.loc[temp_i,"indi_sufix" ] 
                    df_ashare.loc[temp_i, temp_col ] = df_esti_detail_1[temp_col]
                    df_ashare.loc[temp_i, temp_col_stat ] = df_esti_detail_1[temp_col_stat ]
                    ### notes:type(df_esti_detail_1[temp_col])=float,  type(df_esti_stat_30d_1[temp_col_stat].values[0])==np.float64 
                    df_ashare.loc[temp_i, temp_col_diffpct ] = self.cal_diffpct( df_esti_detail_1[temp_col]  , df_esti_stat_30d_1[temp_col_stat].values[0]  )  
                    

                # TODO: sheet=跨表格匹配;file=wds_表格列匹配.xlsx  6268 4004

                print( df_ashare.head().T )
                asd=input("Check.............") 
                

                



        
        ### 计算差异






        print("Debug==== \n",df_esti_stat_30d.head().T, df_esti_stat_30d.tail().T )
        asd
        ### 导出当日股票列表
        obj_fi["df_ashare"] = df_ashare


        return obj_fi

    ########################################################################################
    ### 标准化计算模块
    ########################################################################################
    def cal_diffpct(self,a,b ):
        ### 计算差异指标：c=a/b-1; 分子，Numerator;分母 denominator
        # 首先判断a是否数值和a是否0；其次判断b是否正值、0，负值
        # case 1 ：
        try :
            if b > 0 :
                ### 这是最正常的情况
                c = a/b-1
            elif b< 0 :
                if a > 0 :
                    ### 不考虑减亏，只考虑扭亏
                    c=1.0
                elif a -b > 0 :
                    c= 0.0
                else :
                    c= -1.0
            else :
                ### b== 0 
                if a > 0 :
                    c= 1.0
                else :
                    c=0.0
        except :
            ### 这种情况表示出现了a，b非数值的情况
            c = -1.0 

        return c 















