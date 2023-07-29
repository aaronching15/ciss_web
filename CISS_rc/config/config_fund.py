# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
TODO


功能：配置基金分析所需要的信息
last update  | since 201230
Menu :

Notes:
===============================================
'''
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db；# C:\ciss_web\CISS_rc\config
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

### 导入数据配置信息,大部分配置地址都在config_data里
from config_data import config_data
config_data_1 = config_data()

from config_IO import config_IO
config_IO_1 = config_IO()

###################################################.
class config_fund():
    def __init__(self ):
        ### 
        self.obj_config_fund = {}
        ### 导入 config_data 里的数据配置          
        self.obj_config_fund["obj_config_data"] = config_data_1.obj_config 

        ### 导入 config_IO 里的IO配置
        self.obj_config_fund["obj_config_IO"] = config_IO_1.obj_config
        
        ### 设置基金分析地址
        self.path_fund_ana = self.obj_config_fund["obj_config_IO"]["dict"]["path_ciss_db"] + "fund_simulation\\rc_fund_ana\\"
                
        #################################################

    def print_info(self ):        
        print("gen_obj_fund_ana | 初始化基金分析管理对象 ")
        print("load_obj_fund_ana | 给定日期，导入基金分析管理对象 ")


        # todo
        print("gen_obj_fund | 初始化基金管理对象 ")
        print("get_fund_datetime | 获取基金日期和时间对象 ")
        print(" | ")

        return 1 

    def gen_obj_fund_ana(self) :
        ### 初始化基金分析对象 fund_obj
        ### 设置组合id，是否单一基金"single_fund"，开始和结束日期
        obj_fund_ana = {}
        obj_fund_ana["dict"] ={}
        # "rc_2005"
        str_year_start = input("Type in year start such as 2012,2016,2019:")
        str_year_end = input("Type in year end such as 2019:")
        
        obj_fund_ana["dict"]["id_output"] = "rc_start_" + str_year_start  
        # input("Type in id for portfolio,such as rc_2005:")  
        obj_fund_ana["dict"]["single_fund"] = 1
        ### 若单个一级行业，则取行业内前50%;默认前30%；若indi_quantile_tail=1, 则取尾部指标值，默认值0取指标最大的。
        obj_fund_ana["dict"]["indi_quantile_tail"] = 0 # 1
        # "20200401"# input("Type in date start such as 20151101: ")
        obj_fund_ana["dict"]["date_start"] = str_year_start + "0101" # "20200501"  # "20080401"  # "20060401" 
        obj_fund_ana["dict"]["date_end"] = str_year_end + "1231" # input("Type in date start such as 20190506: ")
        
        print("Dict of obj_fund_ana ", obj_fund_ana["dict"] )

        ### basc基础配置对象：
        obj_fund_ana["sys"] = sys
        obj_fund_ana["obj_config_data"] = self.obj_config_fund["obj_config_data"]
        obj_fund_ana["obj_config_IO"] = self.obj_config_fund["obj_config_IO"] 

        ########################################################################
        ### 导入区间日期数据,0131、0331、0430、0731、0830、1030六个基金数据披露截止时间之后的第一个交易日
        ### todo: 要生成匹配的季度末日期 WDS_F_PRT_ENDDATE_20120630_ALL.csv
        from data_io import data_io
        data_io_1 = data_io()
        obj_fund_ana = data_io_1.get_after_ann_days_fund( obj_fund_ana )
        
        # date_list_period 指的是全部区间交易日
        date_list_period = obj_fund_ana["dict"]["date_list_period"] 
        date_list_before_ann = obj_fund_ana["dict"]["date_list_before_ann"]
        date_list_after_ann = obj_fund_ana["dict"]["date_list_after_ann"]
        date_list_report = obj_fund_ana["dict"]["date_list_report"]

        date_list_after_ann = date_list_after_ann +[ obj_fund_ana["dict"]["date_end"] ]
        obj_fund_ana["date_list_after_ann"] = date_list_after_ann
        ########################################################################
        ### 
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

        ################################################# 

        return obj_fund_ana


    def load_obj_fund_ana(self,obj_in):
        ### 给定日期，导入基金分析管理对象    
        # obj_in至少应当包括披露日期 obj_in["temp_date"]  
        #notes:如果读取json加了 "w+",会导致json数据文件变0
        import json
        obj_fund = {}
        obj_fund["dict"] ={}

        
        ###############################################################
        ### "obj_fund_" + obj_in["temp_date"] + ".json"
        file_name = "obj_fund_" + obj_in["temp_date"] + ".json"
        with open( self.path_fund_ana + file_name ) as obj_dict : 
            ### str转dict
            obj_fund["dict"] = eval( json.load( obj_dict ) )            
            
        print("Dict of obj_fund \n", type( obj_fund["dict"] ) ,obj_fund["dict"].keys() )
        
        ###############################################################
        ### obj_fund_ana_" + obj_in["temp_date"] + ".json"
        obj_fund_ana = {}
        obj_fund_ana["dict"] ={}
        

        file_name = "obj_fund_ana_" + obj_in["temp_date"] + ".json"
        
        with open( self.path_fund_ana + file_name) as obj_dict2 :

            obj_fund_ana["dict"] = eval( json.load( obj_dict2 ) )
        
        print("Dict of obj_fund_ana \n", obj_fund_ana["dict"].keys() )        
        
        ###############################################################
        ### obj_fund_list_20150202.json
        obj_fund_list = {}
        obj_fund_list["dict"] ={}
        file_name = "obj_fund_list_" + obj_in["temp_date"] + ".json"
        
        with open( self.path_fund_ana + file_name) as obj_dict2 :

            obj_fund_list["dict"] = eval( json.load( obj_dict2 ) )
        

        ###############################################################
        ### 导入df_fund_20070402.csv，
        file_name = "df_fund_" + obj_in["temp_date"] +".csv"
        try :
            obj_fund_ana["df_fund"] = pd.read_csv( self.path_fund_ana + file_name ,encoding="gbk"  )
        except :
            obj_fund_ana["df_fund"] = pd.read_csv( self.path_fund_ana + file_name   )
        
        ### 导入df_fund_stock_port_20060801.csv
        file_name = "df_fund_stock_port_" + obj_in["temp_date"] +".csv"
        try :
            obj_fund_ana["df_fund_stock_port"] = pd.read_csv( self.path_fund_ana + file_name ,encoding="gbk"  )
        except :
            obj_fund_ana["df_fund_stock_port"] = pd.read_csv( self.path_fund_ana + file_name   )
        
        ### 导入df_stockpool_fund_20060801.csv
        file_name = "df_stockpool_fund_" + obj_in["temp_date"] +".csv"
        try :
            obj_fund_ana["df_stockpool_fund"] = pd.read_csv( self.path_fund_ana + file_name ,encoding="gbk"  )
        except :
            obj_fund_ana["df_stockpool_fund"] = pd.read_csv( self.path_fund_ana + file_name   )
        
        ### 导入df_fund_company_20180402.csv
        file_name = "df_fund_company_" + obj_in["temp_date"] +".csv"
        try :
            obj_fund_ana["df_fund_company"] = pd.read_csv( self.path_fund_ana + file_name ,encoding="gbk"  )
        except :
            obj_fund_ana["df_fund_company"] = pd.read_csv( self.path_fund_ana + file_name   )
        
        #################################################
        ### save to object 

        ###############################################################
        ### 继承输入的日期变量 
        obj_fund_ana["dict"]["temp_date"] = obj_in["temp_date"]
        obj_fund_ana["dict"]["quarter_end"] = obj_in["quarter_end"] 
        obj_fund_ana["dict"]["date_list"] = obj_in["date_list"] 

        #################################################
        return obj_fund,obj_fund_ana
        




#################################################



