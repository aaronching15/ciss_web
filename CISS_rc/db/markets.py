# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
功能：
1,class markets_monitor:市场指数监控和个股统计

last update 220209 | since 181114
Menu :
绝对回报 	-0.29%
相对回报	4.17%
本周回报	0.19%
本月回报	0.20%
本季回报	-0.29%
本年回报	-0.29% 
Sharpe 	-3.25
Alpha 	1.90%  
最大回撤	-0.70%

refernce:  
===============================================
'''
import sys,os 
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"\\CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )
import pandas as pd
import numpy as np 
import json 
sys.path.append("..") 

###################################################
class markets():
    def __init__(self):
        self.nan = np.nan
        self.path_pms = "C:\\rc_202X\\rc_202X\\data_pms\\"
        self.path_wpf = self.path_pms + "wpf\\"


###################################################
class markets_monitor(): 
    def __init__(self ):  
        self.nan = np.nan
        self.path_pms = "C:\\rc_202X\\rc_202X\\data_pms\\"
        self.path_wpf = self.path_pms + "wpf\\"
        

    def print_info(self):        
        print("市场指数监控和个股统计：   ")
        print("market_index_main | 中港美主要指数监控")
        print("market_index_stock | 市场指数监控和个股统计")
        print("market_stat_stock | 市场个股统计")
        print("")
        ##########################################
        
    def market_index_main(self,obj_m = {}  ) :
        ### 中港美主要指数监控
        temp_index_type = obj_m[ "temp_index_type"]
        ##########################################
        ### 读取指数列表：A股、港股、美股、债券、基金
        file_name = "data_adj.xlsx"
        df_index_list = pd.read_excel( self.path_pms + file_name, sheet_name= "list_index" )
        # print("df_index_list \n", df_index_list )

        ### 默认情况
        if temp_index_type == "all" :
            obj_m["df_index_list"] = df_index_list
        else : 
            obj_m["df_index_list"] = df_index_list[ df_index_list["type"] == temp_index_type ]
        
        ##########################################
        ### 1，data：维护指数的历史日涨跌幅度 ；最少近2年 todo

        ##########################################
        ### 2，data：直接用Wind-api获取区间涨跌幅
        if "date_begin" in obj_m.keys() :
            if temp_index_type == "all" :
                obj_m["df_chg"] = df_index_list
            else : 
                obj_m["df_chg"] = df_index_list[ df_index_list["type"] == temp_index_type ]

            # input: obj_in.keys 包括：df_chg, dict_date
            # 近一周	近一个月	近6个月	近一年	年初至今	上年初至今
            ### input：obj_w =w.wss("000300.SH", "pct_chg_per","startDate=20220111;endDate=20220211")
            ### output:obj_w = .ErrorCode=0 ;.Codes=[000300.SH] ; .Fields=[PCT_CHG_PER]
            # .Times=[20220211 16:39:01]  ; .Data=[[-3.389656101963534]] 
            # obj_m["date_begin"] , obj_m["date_end"]  
            ##########################################
            ### wind_api获取数据
            from get_wind_api import wind_api
            class_wind_api = wind_api()
            obj_m = class_wind_api.get_wss_pct_chg_period( obj_m )
            ### output: obj_m["df_chg"] 
            
        
        ##########################################
        ### 3，



        return obj_m

    def market_index_stock(self ) :
        ### 市场指数监控和个股统计
        ##########################################
        ### TODO：导入市场主要指数，用Wind-API提取区间收益率和周收益率






        obj_m = {}
        return obj_m

    def market_stat_stock(self ) :
        ### 市场个股统计
        ##########################################
        ### TODO：导入个股、行业、市场时点数据进行统计，file=a_shares_20220121.xlsx
        
        ##########################################
        ### 按市值分组统计，
        # col：市场状态分析,交易信号,中期趋势	短期趋势	市值占比	市值|亿	数量	参数1	参数2
        ### 超大市值	5000亿以上 大市值	2000~5000 中市值	500~2000 小市值	100~500亿 小微市值	100亿以下

        ##########################################
        ### 按行业统计

        ##########################################
        ### 按风格统计

        ##########################################
        ### 按基金持仓统计

        obj_m = {}
        return obj_m



