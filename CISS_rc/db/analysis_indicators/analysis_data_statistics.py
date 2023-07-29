# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
TODO：对A股各类指标进行统计分析：个股、行业、风格、市场维度

功能：对A股各类指标进行统计分析：个股、行业、风格、市场维度
承接部分analysis_indicators.py 功能
last  | since 220123 
Notes: 
related files: 
0，时点股票数据文件：C:\rc_202X\rc_202X\data_pms\data_adj\ah_shares.xlsx
1, 

===============================================
'''
import sys,os
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

# 当前目录 C:\rc_2023\rc_202X\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )


# sys.path.append("..")
### Import config
# from config_data import config_data
# config_data_1 = config_data()
# from config_indicator import config_indi_financial
# config_indi_financial_1 = config_indi_financial()
# from data_io_financial_indicator import data_io_financial_indicator
# data_io_financial_indicator_1 = data_io_financial_indicator()
# 交易日list： data_io_financial_indicator_1.obj_data_io["dict"]["tradingday"]
# 交易周list： data_io_financial_indicator_1.obj_data_io["dict"]["date_list_tradingday"]

#########################################################
class data_stat():
    def __init__(self ):
        #########################################################
        ###  
        self.nan = np.nan 
        
        self.file= "pms_manage.xlsx"
        # 当前目录 C:\rc_2023\rc_202X\ciss_web\CISS_rc\db 
        path_rc_202X = os.getcwd().split("ciss_web")[0]
        print("path_rc_202X=", path_rc_202X )
        
        self.path0 = path_rc_202X
        self.path = path_rc_202X +"data_pms\\"
        self.path_wpf = self.path + "wpf\\" 
        self.path_data = self.path + "wind_terminal\\" 
        self.path_adj = self.path +  "data_adj\\"
        import datetime as dt  
        self.time_now = dt.datetime.now()
        self.time_now_str = dt.datetime.strftime(self.time_now   , "%Y%m%d")
        self.time_pre_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=1) , "%Y%m%d")
        self.time_pre10_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=10) , "%Y%m%d")
        ######################################################################
        # file= "pms_manage.xlsx"
        self.df_pms_manage = pd.read_excel(self.path + self.file , sheet_name="组合列表"  ) 

        ######################################################################

    def print_info(self):
        ### print all modules for current script
        print("### 标准化计算模块================================================")  
        print("get_list_pms_port | 给定组合列表，下载最新PMS持仓数据 ")  
        print("cal_AH_momentum_abcd3d  | 计算AH股策略指标:中短期趋势abcd3d ") 
        print("  |  ") 
        print("  |  ") 

        return 1 

    def get_list_pms_port(self ):
        ### 个股季度财务指标整理和计算
        file= "pms_manage.xlsx"
        df_pms_manage = pd.read_excel(self.path+ file , sheet_name="组合列表"  )
        print("df_pms_manage", df_pms_manage ) 

        count_pms = 0 
        from get_wind_api import wind_api
        class_wind_api = wind_api()

        for temp_i in df_pms_manage.index :
            ### 判断是否是有效组合
            print( type(df_pms_manage.loc[temp_i, "if_active" ]), df_pms_manage.loc[temp_i, "if_active" ] )
            
            if df_pms_manage.loc[temp_i, "if_active" ] in [1,"1"] :
                dict_in={}
                # "固收加FOF20" #  "FOF期权9901" 
                dict_in["pms_name"] = df_pms_manage.loc[temp_i, "port_name"  ]
                ### 1,股票；2，基金；3，债券
                dict_in["col_type"] = df_pms_manage.loc[temp_i, "asset_type"  ]
                dict_in["date_start"] = "20201231"
                dict_in["date_end"] = self.time_pre_str
                dict_in["if_excel"] = 1
                dict_in["path"] = self.path
                ### 下载最新PMS持仓数据
                df_data= class_wind_api.get_wpf( dict_in )
                ### 需要增加一列，组合名称
                df_data["port_name"] = dict_in["pms_name"]
                
                ### 保存所有组合的持仓！！
                if count_pms == 0  :
                    df_data_all = df_data
                    count_pms = 1 
                else :
                    df_data_all = df_data_all.append(df_data)
                ### 
                file_name = "wpf_all" + "_" + self.time_pre_str + ".xlsx"
                df_data_all.to_excel(self.path_wpf + file_name,index=False ) 
                file_name = "wpf_all" + ".xlsx"
                df_data_all.to_excel(self.path_wpf + file_name,index=False ) 
        #################################################
        obj_data = {}
        obj_data["df_pms_manage"] = df_pms_manage
        obj_data["df_data_all"] = df_data_all
        return obj_data 

    def cal_AH_momentum_abcd3d(self, input_date ):
        ### 计算AH股策略指标:中短期趋势abcd3d 
        obj_data = {} 
        #################################################
        if len(input_date) == 6 :
            # 220107 to 20220107
            date_latest = "20" +input_date
        elif len(input_date) == 8 and input_date[:2] =="20" :
            date_latest =  input_date
        else :
            print("Error input date...")
            date_latest = input("Input date such as 20220220:")
        
        if len(input_date) == 6 :
            # 220107 to 20220107
            date_latest = "20" +input_date 
        for temp_key in ["a","h" ]:
            ### 2.1，导入数据
            # temp_key+"_shares_"+ date_latest +".xlsx"
            file_name = temp_key +"_shares_" + date_latest +".xlsx" 

            df_shares = pd.read_excel(self.path_adj + file_name )
            # print("df_shares \n", df_shares.head().T )
            
            ################################################
            ### 数据准备
            ### 替换异常值 ||  
            df_shares.replace(np.inf, 0.0 ) #替换正inf为-1

            ### 把成交额和市值单位从元改为亿元 | 流通市值	总市值1
            df_shares["mv_float"] = df_shares["流通市值"] /100000000
            df_shares["mv"] = df_shares["总市值1"] /100000000
            
            ################################################
            ### 区间成交额=amt_per,输出指标=amt_per_1m,m_ave_amt
            ### 区间日均市值 avg_MV_per,输出指标=m_ave_mv 
            df_shares["m_ave_amt"] = df_shares["m_ave_amt"] /100000000
            df_shares["m_ave_mv"] = df_shares["mv"]/100000000

            ### 备用before 2306: 平均市值用最新市值和近20日涨跌幅计算；不提取avg_MV_per，因为avg_MV_per无法正常提取，file=get_wind_py\\get_wss_ma_amt_mv
            # df_shares["m_ave_mv"] = df_shares["mv"]*( 1+ df_shares["20日涨跌幅"]*0.5  )  
            # df_shares["m_ave_amt"] = df_shares["成交额"]*( 1+ df_shares["20日涨跌幅"]*0.5 ) /100000000
            
            ################################################
            ### 2.2，计算动量趋势和成交额、市值指标。avg_MV_per
            # ma_short,ma_short_pre,pre_close,ma_mid,m_ave_amt,m_ave_mv
            df_shares["trend_short"] = df_shares["ma_short"]/ df_shares["ma_short_pre"] -1 
            df_shares["trend_mid"] = df_shares["pre_close"]/df_shares["ma_mid"] -1 
            ### 计算市值加权的指标
            # 20日涨跌幅,60日涨跌幅,120日涨跌幅,年初至今,流通市值,总市值1,市盈率(TTM),基金持股比例,
            # 净资产收益率(TTM),归母净利润同比增长率,中信一级行业,中信二级行业,中信三级行业,申万一级行业
            col_list = ["trend_short","trend_mid","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)","20日涨跌幅","60日涨跌幅","120日涨跌幅" ]
            col_list_mvAVE = []
            for temp_col in col_list  :
                ### 去极端值和非数字值 | 净利润有可能同比增长 500% 或更多
                df_shares[ temp_col] = df_shares[temp_col].apply(lambda x : x if (x>-150 and x <500 ) else 0.0 )
                df_shares["mvAVE_"+ temp_col] = df_shares[ "m_ave_mv" ] * df_shares[ temp_col] 
                col_list_mvAVE = col_list_mvAVE + [ "mvAVE_"+ temp_col ]

            ################################################
            ### 如果是A股，要进行行业分类匹配 | 22年3月开始，A股导出的行业分类用不了了；也许以后会修复
            temp_nan = float('nan')
            if temp_key == "a" :
                import math
                ### 导入match文件
                file_temp = "pms_manage.xlsx"
                df_match  = pd.read_excel(self.path + file_temp,sheet_name="match_ind_a" )
                for temp_i in df_shares.index :
                    ### 对每个股票，都使用 "match_ind_a" 里的数据，因为2203开始原始wind导出数据有问题。
                    df_temp = df_match[ df_match["Wind代码"] == df_shares.loc[temp_i,"代码"] ]
                    if len( df_temp.index ) > 0 :
                        df_shares.loc[temp_i,"中信一级行业"] = df_temp["中信一级行业"].values[0]
                        df_shares.loc[temp_i,"中信二级行业"] = df_temp["中信二级行业"].values[0]
                        df_shares.loc[temp_i,"中信三级行业"] = df_temp["中信三级行业"].values[0]

            df_shares.to_excel(self.path_adj + file_name,index=False )

            ################################################
            ### Notes:如果是港股，要进行行业分类匹配
            temp_nan = float('nan')
            if temp_key == "h" :
                import math
                ### 导入match文件
                file_temp = "pms_manage.xlsx"
                df_match  = pd.read_excel(self.path + file_temp,sheet_name="match_ind_hk" )
                for temp_i in df_shares.index :
                    df_temp = df_match[ df_match["Wind代码"] == df_shares.loc[temp_i,"代码"] ]
                    if len( df_temp.index ) > 0 :
                        df_shares.loc[temp_i,"中信一级行业"] = df_temp["中信一级行业"].values[0]
                        df_shares.loc[temp_i,"中信二级行业"] = df_temp["中信二级行业"].values[0]
                        df_shares.loc[temp_i,"中信三级行业"] = df_temp["中信三级行业"].values[0]

            df_shares.to_excel(self.path_adj + file_name,index=False )

            ################################################
            ### 3，一级行业统计：按中信一级行业分组，除以行业内股票市值之和
            df_stat = df_shares.loc[:,  col_list_mvAVE + [ "m_ave_mv","m_ave_amt" ] ].groupby( df_shares["中信一级行业"] ).sum()
            print( df_stat.head()  )
            for temp_col in col_list :
                df_stat[  temp_col] = df_stat[ "mvAVE_"+ temp_col]/ df_stat["m_ave_mv"]

            ### 数据格式调整
            df_stat["总市值"] = df_stat["m_ave_mv"] 
            df_stat["月均成交额"] = df_stat["m_ave_amt"] 
            ### 只保留部分columns
            df_stat = df_stat.loc[:, ["月均成交额","总市值"] + col_list ]
            ### 
            df_stat = df_stat.sort_values(by="月均成交额",ascending=False ) 

            ### save to excel
            file_name2 = temp_key +"_shares_ind1_" + date_latest  +".xlsx"
            ### notes:必须带index，index是行业名称
            df_stat.to_excel(self.path_adj + file_name2  )
            file_name2 = temp_key +"_shares_ind1" +".xlsx"
            ### notes:必须带index，index是行业名称
            df_stat.to_excel(self.path_adj + file_name2  )
            
            df_ind1 = df_stat
            ###################################################
            ### 三级行业统计
            df_stat = df_shares.loc[:,  col_list_mvAVE + [ "m_ave_mv","m_ave_amt" ] ].groupby( df_shares["中信三级行业"] ).sum()
            print( df_stat.head()  )
            for temp_col in col_list :
                df_stat[  temp_col] = df_stat[ "mvAVE_"+ temp_col]/ df_stat["m_ave_mv"]

            ### 数据格式调整
            df_stat["总市值"] = df_stat["m_ave_mv"] 
            df_stat["月均成交额"] = df_stat["m_ave_amt"] 
            ### 只保留部分columns
            df_stat = df_stat.loc[:, ["月均成交额","总市值"] + col_list ]
            ### 
            df_stat = df_stat.sort_values(by="月均成交额",ascending=False ) 
            
            ### save to excel
            file_name2 = temp_key +"_shares_ind3_" + date_latest  +".xlsx"
            ### notes:必须带index，index是行业名称
            df_stat.to_excel(self.path_adj + file_name2 )
            file_name2 = temp_key +"_shares_ind3" +".xlsx"
            ### notes:必须带index，index是行业名称
            df_stat.to_excel(self.path_adj + file_name2 )

            df_ind3 = df_stat
            ###################################################
            obj_data["df_shares_"+ temp_key] = df_shares
            obj_data["df_ind1_"+ temp_key] = df_ind1
            obj_data["df_ind3_"+ temp_key ] = df_ind3
            
        ###################################################
        ### 合并A股和H股数据 
        temp_key ="a"
        file_name = temp_key +"_shares_" + date_latest +".xlsx"
        df_shares = pd.read_excel(self.path_adj + file_name )
        df_shares.replace(np.inf, 0.0 ) #替换正inf为-1

        temp_key ="h"
        file_name = temp_key +"_shares_" + date_latest +".xlsx"
        df_shares2 = pd.read_excel(self.path_adj + file_name )
        df_shares2.replace(np.inf, 0.0 ) #替换正inf为-1

        df_shares2=df_shares2.append(df_shares,ignore_index=True)

        ### save to excel
        file_output = "ah_shares_" + date_latest +".xlsx"
        df_shares2.to_excel( self.path_adj + file_output, sheet_name="ah_shares",index=False )
        file_output = "ah_shares" + ".xlsx"
        df_shares2.to_excel( self.path_adj + file_output, sheet_name="ah_shares",index=False )


        #################################################
        obj_data["df_shares_ah"] = df_shares2
        
        return obj_data 

    ########################################################################################
    ### 标准化计算模块
    ########################################################################################
     















