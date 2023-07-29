# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
功能：根据通联数据datayes的接口，抓取A股、港股、基金等数据
数据来源： https://mall.datayes.com/datapreview/80
derived from get_hs_data.py
last update  | since 211221
/ 
===============================================
'''
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc)
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

from data_io import data_io
data_io_1 = data_io()
from config_data import config_data
class_config_data = config_data()
### class_config_data.obj_config["dict"]["path_data_yes"] = "D:\\db_wind\\data_yes\\" 
#  self.obj_config["dict"]["path_disk"] = "D:\\"   

###########################################################
### 这里导入没用 import hs_udata

class data_yes():
    # 类的初始化操作
    def __init__(self):
        
        #################################################################################
        ### Initialization   
        self.path_out = class_config_data.obj_config["dict"]["path_disk"]
        self.path_data_yes = class_config_data.obj_config["dict"]["path_data_yes"]
        self.path_data_hs = class_config_data.obj_config["dict"]["path_data_hs"]
        ### 调整后的数据
        # self.obj_config["dict"]["path_wind_adj"]  
        
        #############################################
        ### 
        self.path_log_table = os.getcwd()[:2] +"\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\"
        if  os.path.exists( os.getcwd()[:2] +"\\zd_zxjtzq\\ciss_web\\" ) :
            self.path_rc_data = os.getcwd()[:2] +"\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
        else :
            self.path_rc_data = os.getcwd()[:2] +"\\ciss_web\\CISS_rc\\apps\\rc_data\\"
        ### 
        self.nan = np.nan 
        #############################################
        ### 通联数据的token 
        self.token = '08b8e4bd50638f2ad23b92e6be3ce8180ea08e744825f07ce27f93deb1f75e12'
        ### 
        from dataapi_win36 import Client
        self.client = Client()
        self.client.init( self.token ) 


    def print_info(self):
        #############################################
        ### Part 1 每日维护数据表格        
        print("data_manage |manage_data_yes:日常维护所有数据表格  ")
        print("data_manage |update_log_date_code:导入数据目录，更新交易日和股票代码等基础信息 ")
        
        #############################################
        ### Part 2 标准化获取数据
        print("data_fetch |get_table_by_stock: ") 
        
        #############################################
        ### Part 3 标准化读取和写入数据 |  解决不了代码内导入不同模块的需求
        # print("data_io | export_log_table_1shot : 导出一次性获取的数据df_table和日志df_log")
        
        #############################################
        ### Part 4 转换和维护数据
        print("data_transeform | trans_table_code2date: 将基于代码的数据改成基于日期的数据文件") 
        # print("data_fetch |get_table_full_input:get_table_full带参数版本")
        # print("data_fetch |get_table_primekey_input: 根据table_name,prime_key ,prime_key_value获取表格数据")
        # print("data_fetch |get_table_opdate: 根据最近一次opdate时间获取opdate时间之后的增量数据，并可以按给定prime_key关键词更新数据文件， ")
        
        return 1 
 
    ##########################################################################################
    ### Part 2 每日维护数据表格   
    def manage_data_yes(self) :
        ### 
        obj_data ={}

        return obj_data
    

        
    def update_log_date_code(self ) :
        ###########################################
        ### 导入数据目录，更新交易日和股票代码等基础信息
        obj_data = {}
        obj_data["dict"] = {}
        ###########################################
        ### Part 1 导入数据目录的日志和数据核对表 
        ### 设置目录
        # path_yes = "D:\\db_wind\\data_yes\\"
        path_yes = "C:\\rc_HUARONG\\rc_HUARONG\\data_hs\\"
        
        path_log = path_yes +"log\\"
        file_log = "log_stock_quote_daily_list.xlsx"
        if not os.path.exists( path_log ) :
            os.makedirs(path_log )

        path_table = path_yes +"table\\"
        if not os.path.exists( path_table) :
            os.makedirs( path_table )
        
        ### save to obj_data
        obj_data["dict"]["path_yes"] = path_yes
        obj_data["dict"]["path_log"] = path_log
        obj_data["dict"]["path_table"] = path_table
        #########################################
        ### 读取log文件，判断是否到了需要更新日期序列的时候
        df_log = pd.read_excel(path_yes +  "manage_yes.xlsx" )
        ### full_table  

        #########################################################################
        ### 导入和更新日期序列 | notes:1次只能取接近1年的日期 
        ### # 获取当前时间;'%Y%m%d=20211208, '%y%m%d=211208
        # temp_time_begin =  time.localtime(time.time()) 
        # print( time.strftime('%Y%m%d %H%M%S',temp_time_begin) )  
        time_begin = dt.datetime.now()
        time_begin_str = dt.datetime.strftime(time_begin, "%Y%m%d")
        print( time_begin_str )
        #########################################
        ### 日期格式：2013-01-01 "trading_date", trading_calendar.xlsx,D:\db_wind\data_hs\log
        file_table = "trading_calendar"  + ".xlsx"
        path_log = self.path_data_hs +  "log\\"

        
        ########################################
        ### notes:港股交易日和A股交易日要区分开
        ### 读取交易日文件
        ### 读取,notes:日历数据的读取不能把第一列当作index        
        df_calendar =pd.read_excel(path_log + file_table  )
        file_table = "trading_calendar_hk"  + ".xlsx"
        df_calendar_hk =pd.read_excel(path_log + file_table  )
        ### save to obj_data
        obj_data["dict"]["time_begin"] = time_begin
        obj_data["dict"]["time_begin_str"] = time_begin_str
        obj_data["df_calendar"] = df_calendar 
        df_date = df_calendar[df_calendar["if_trading_day"] =="是" ]
        date_list = df_date["trading_date"].values
        ### notes:日期格式是str，且 2002-01-14
        obj_data["date_list_str"] = date_list
        ### 2002-01-14 to 20020114
        date_list = [ int(i[:4]+i[5:7]+i[-2:])  for i in date_list  ]
        ### 可能包含未来日期，需要剔除
        date_list = [i for i in date_list if i <  int(time_begin_str) ]
        obj_data["date_list"] = date_list
        
        ########################################
        ### date HK
        df_date_hk = df_calendar_hk[df_calendar_hk["if_trading_day"] =="是" ]
        date_list_hk = df_date_hk["trading_date"].values
        obj_data["date_list_hk_str"] = date_list_hk
        ### 2002-01-14 to 20020114
        date_list_hk = [ int(i[:4]+i[5:7]+i[-2:])  for i in date_list_hk  ]
        obj_data["date_list_hk"] = date_list_hk

        #########################################################################
        ### 导入股票列表 | notes:股票上市和退市状态是会改变的！

        # temp_table = "stock_list"
        # index_date =  df_log[df_log["table_name"]== temp_table  ].index[0]
        # date_last = df_log[df_log["table_name"]== temp_table  ]["date_last"].values[0]
        # temp_freq = df_log[df_log["table_name"]== temp_table  ]["frequency_full_table"].values[0]
        # # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
        # time_diff = time_begin - dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  
        # print("date_last:" ,date_last, "time_diff=",time_diff.days  )
        # ######################################
        # ### 设置表的目录
        # path_table_sub = path_table + temp_table +"\\"
        # if not os.path.exists( path_table_sub ) :
        #     os.makedirs( path_table_sub )
        # # if temp_freq == "quarter":
        # if time_diff > dt.timedelta(days= 15  ) :
        #     from hs_udata import stock_list
        #     df_table = stock_list()
        #     file_table = "stock_list"  + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        #     file_table = "stock_list_" +  time_begin_str + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        #     #########################################
        #     ### 更新 df_log 
        #     df_log.loc[index_date, "date_last" ] = time_begin_str
        # else :
        #     ### 读取股票列表
        #     file_table = "stock_list"  + ".xlsx"
        #     df_table = pd.read_excel(path_table_sub + file_table  )
        #     df_table =df_table[ df_table["listed_state"] == "上市"]

        # ### 获取股票代码 # hs_code
        # code_list = list( df_table["hs_code"].values )

        #########################################################################
        ### 导入基金列表 | notes:211210数据，返回的数据是全部基金代码，超过5000的
        # 
        # temp_table = "fund_list"
        # index_date =  df_log[df_log["table_name"]== temp_table  ].index[0]
        # date_last = df_log[df_log["table_name"]== temp_table  ]["date_last"].values[0]
        # temp_freq = df_log[df_log["table_name"]== temp_table  ]["frequency_full_table"].values[0]
        # # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
        # time_diff = time_begin - dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  
        # print("date_last:" ,date_last, "time_diff=",time_diff.days  )
        # ######################################
        # ### 设置表的目录
        # path_table_sub = path_table + temp_table +"\\"
        # if not os.path.exists( path_table_sub ) :
        #     os.makedirs( path_table_sub )
            
        # ######################################
        # ### 基金数据每个月更新一次 if temp_freq == "month":
        # if time_diff > dt.timedelta(days= 20  ) :
        #     from hs_udata import fund_list
        #     ### 1，开放式基金，截至211210大约有5600个。可以查到部分FOF基金，但是没有ETF
        #     df_table= fund_list(foundation_type="2" ) 
        #     file_table = "fund_list_open"  + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        #     file_table = "fund_list_open_" +  time_begin_str + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        #     time.sleep(0.8)  
        #     ### 获取基金代码列表  
        #     fund_list_open = list( df_table["secu_code"].values)
        #     ### 1，ETF，截至211210大约有700+ 
        #     df_table= fund_list(foundation_type="4" ) 
        #     file_table = "fund_list_etf"  + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        #     file_table = "fund_list_etf_" +  time_begin_str + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        #     time.sleep(0.8)
        #     ### 获取基金代码列表  
        #     fund_list_etf = list( df_table["secu_code"].values)
        #     ### 1，场内基金，包括所有ETF和他们的A\B类，还有一些封闭基金，截至211210大约有1000+ 
        #     df_table= fund_list(float_type="1" )
        #     file_table = "fund_list_shsz"  + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        #     file_table = "fund_list_shsz_" +  time_begin_str + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        #     time.sleep(0.8)
        #     ### 获取基金代码列表  
        #     fund_list_shsz = list( df_table["secu_code"].values)
        #     #########################################
        #     ### 更新 df_log 
        #     df_log.loc[index_date, "date_last" ] = time_begin_str
        # else :
        #     ### 读取基金列表
        #     file_table = "fund_list_open"  + ".xlsx"
        #     df_table = pd.read_excel(path_table_sub + file_table  )
        #     fund_list_open = list( df_table["secu_code"].values)
        #     file_table = "fund_list_etf"  + ".xlsx"
        #     df_table = pd.read_excel(path_table_sub + file_table  )
        #     fund_list_etf = list( df_table["secu_code"].values)
        #     file_table = "fund_list_shsz"  + ".xlsx"
        #     df_table = pd.read_excel(path_table_sub + file_table  )
        #     fund_list_shsz = list( df_table["secu_code"].values) 
        
        ######################################################################### 
        ### 导入港股股票列表 TODO:避免频繁下载代码数据：10天一次
        ### 5.1.1 港股上市列表	3	0	month	0	1	hk_list		获取所有港股信息
        
        # temp_table = "hk_list"  
        # index_date =  df_log[df_log["table_name"]== temp_table  ].index[0]
        # date_last = df_log[df_log["table_name"]== temp_table  ]["date_last"].values[0]
        # temp_freq = df_log[df_log["table_name"]== temp_table  ]["frequency_full_table"].values[0]
        # # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
        # time_diff = time_begin - dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  
        # print("date_last:" ,date_last, "time_diff=",time_diff.days  )
        # ######################################
        # ### 设置表的目录
        # path_table_sub = path_table + temp_table +"\\"
        # if not os.path.exists( path_table_sub ) :
        #     os.makedirs( path_table_sub )
        # # if temp_freq == "quarter":
        # if time_diff > dt.timedelta(days= 15  ) :         
        #     from hs_udata import hk_list
        #     df_table = hk_list()
        #     ### 只保留主板股票
        #     df_table = df_table[ df_table["listed_sector"] == "主板" ]
        #     # df_table = df_table[ df_table["listed_state"] == "上市" ]
        #     file_table = "hk_list"  + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        #     file_table = "hk_list_" +  time_begin_str + ".xlsx"
        #     df_table.to_excel(path_table_sub + file_table ,index=False )
        # else :
        #     file_table = "hk_list"  + ".xlsx"
        #     df_table =pd.read_excel(path_table_sub + file_table )
        #     df_table = df_table[ df_table["listed_sector"] == "主板" ]
        #     # df_table = df_table[ df_table["listed_state"] == "上市" ]
        

        # #########################################
        # ### notes:历史回溯时可能要用到已经退市的代码： 上市状态：listed_state="终止","上市"
        # ### 获取股票代码 # hs_code:00065AA,	140
        # code_list_hk  = list( df_table["secu_code"].values )
        # ### 000065AA to 000065.HK
        # code_list_hk = [ i[:5]+".HK" for i in code_list_hk ]
        # df_table_delisted = df_table[ df_table["listed_state"]=="终止" ]
        # code_list_hk_delisted = list( df_table_delisted["secu_code"].values )
        # code_list_hk_delisted = [ i[:5]+".HK" for i in code_list_hk_delisted ]
        # df_table_sub = df_table[ df_table["listed_state"]=="上市" ]
        # code_list_hk_list = list( df_table_sub["secu_code"].values )
        # code_list_hk_list = [ i[:5]+".HK" for i in code_list_hk_list ]
        
        ######################################### 
        ######################################################################### 
        ### save to obj_data

        ### 股票代码表
        # obj_data["dict"]["code_list"] = code_list
        # ### 获取基金代码列表  
        # obj_data["dict"]["fund_list_open"] =fund_list_open
        # obj_data["dict"]["fund_list_etf"] = fund_list_etf 
        # obj_data["dict"]["fund_list_shsz"] =fund_list_shsz

        # ### 港股列表
        # obj_data["code_list_hk"] = code_list_hk
        # obj_data["code_list_hk_delisted"] = code_list_hk_delisted
        # obj_data["code_list_hk_list"] = code_list_hk_list

        ### save to obj, csv ； obj_data["dict"]["path_yes"] 
        obj_data["df_log"] = df_log
        df_log.to_excel(path_yes +  "manage_yes.xlsx" ,index=False )
        
        return obj_data

    ##########################################################################################
    ### Part 3  标准化读取和写入数据 || 解决不了代码内导入不同模块的需求

    ##########################################################################################
    ### Part 2 标准化获取数据
    ############################################
    ### 获取股票数据
    def get_table_by_stock(self, obj_data ):
        ############################################################################
        ### 1,导入基础数据；
        
        
        ########################################
        ### df_log.index是股票；根据输入表格和参数，获取表格数据
        table_name = obj_data["dict"]["table_name"] # "stock_key_indicator"
        
        ### 目标是近1年交易日         
        date_list = obj_data["date_list"] 
        date_list_hk = obj_data["date_list_hk"]
        path_yes = obj_data["dict"]["path_yes"] 
        path_log = obj_data["dict"]["path_log"] 
        path_table  = obj_data["dict"]["path_table"]  
        df_log = obj_data["df_log"]
        # code_list = obj_data["dict"]["code_list"] 
        time_begin = obj_data["dict"]["time_begin"] 
        time_begin_str = obj_data["dict"]["time_begin_str"] 

        ######################################
        ### 试用账号只能下载近1年的。
        time_begin_str_pre1 =  str(int(time_begin_str[:4])-1) + time_begin_str[-4: ]  
        date_list_1y = [ i for i in date_list if i > int(time_begin_str_pre1) ]
        date_list_1y =date_list_1y[1:]
        ### notes:通联数据不是365天，而是当年开始算，所以在211221，用201223取不到数据。
        ######################################
        ### 导入表对应的日志文件，判断需要更新的内容
        file_log_table = "log_" + table_name +".xlsx"
        # 列是股票代码，column是开始日期，上一次更新日期，对应季度末
        if os.path.exists( path_log + file_log_table ) :
            ### 读取文件
            df_log_table = pd.read_excel( path_log + file_log_table  ) 
            ### 对index临时赋值
            df_log_table.index = df_log_table["date"]
            ### 检查是否要加入新的代码
            date_list_new = [ i for i in df_log_table.index if i not in date_list_1y ]
            for temp_date in date_list_new :
                df_log_table.loc[temp_date, "frequency"] = "day"
                df_log_table.loc[temp_date, "date"]= temp_date 
                df_log_table.loc[temp_date, "if_data"]= 0
                ### 对每个股票，需要获取上市日期，避免浪费时间取上市前的日期
            
        else :
            ### 首次为表格 新建df_log_table
            df_log_table = pd.DataFrame( index=date_list_1y ,columns=["date","frequency","if_data" ] )
            df_log_table["frequency"] = "day"
            df_log_table["date"] = df_log_table.index
            df_log_table["if_data"] = 0
            df_log_table.to_excel( path_log + file_log_table,index=False ) 

        ### 去重复项
        df_log_table = df_log_table.drop_duplicates()
        ######################################
        ### 2,按交易日下载每日A股行情数据  
        ######################################
        ### 设置表的目录
        path_table_sub = path_table + table_name +"\\"
        if not os.path.exists( path_table_sub ) :
            os.makedirs( path_table_sub )

        ################################################################################################################
        ### 股票基础信息
        # 沪深股票前复权行情 getMktEqudAdj
        # if table_name == "getMktEqud" :     
        if table_name[:3] == "get" :     
            index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
            date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
            temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
            # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
            time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
            print("date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
            ################################################
            ### 每30天更新一次基础信息
            count_date = 0 
            for temp_date in df_log_table.index :
                ### 判断该日期是否更新过
                # print("Debug==" , type(df_log_table.loc[temp_date, "if_data"] ),df_log_table.loc[temp_date, "if_data"] ) 
                if df_log_table.loc[temp_date, "if_data"] == 0 :
                    print("table_name",table_name, "Update date:",temp_date,    )
                    ### 每个交易日一次性获取全部股票数据
                    ######################################
                    ### 构建输入参数 str_input;url_json= 
                    #"/api/market/getMktEqud.json?field=&beginDate=&endDate=&secID=&ticker=688001,000001&tradeDate=20210723"
                    # /api/market/getMktEqudAdj.json?field=&secID=&ticker=688001&beginDate=&endDate=&tradeDate=20190816
                    # /api/market/getMktEqudEval.json?field=&secID=&ticker=688001&tradeDate=20190821&beginDate=&endDate=
                    # /api/market/getMktEquFlowOrder.json?field=&secID=&ticker=688001&beginDate=20190823&endDate=20190823
                    url_head = "/api/market/"
                    url_table_name = table_name
                    url_mid = ".json?field=&beginDate=&endDate=&secID=&ticker=&tradeDate="
                    url_date = str( temp_date )
                    url_json= url_head +url_table_name +url_mid +url_date
                    print("url_json=",url_json) 
                    ######################################
                    ### 获取数据
                    ### status_code=200 表示成功，
                    status_code, result_bytes = self.client.getData(url_json)
                    ### bytes to dict 
                    temp_dict = json.loads( result_bytes )
                    ### dict to DataFrame
                    if not temp_dict["retCode"] == 1 :
                        print( temp_dict )
                    else :
                        df_data = pd.DataFrame( temp_dict["data"] )

                        ######################################
                        ### 从本地目录读取文件
                        file_table = table_name +"_" + str(temp_date)  + ".xlsx"
                        if os.path.exists( path_table_sub +file_table ) :
                            df_input = pd.read_excel( path_table_sub + file_table )
                            ### 将数据去重复项后加入总序列。
                            df_input = df_input.append( df_data,ignore_index=True )
                            df_input =df_input.drop_duplicates()
                            #################################
                            ### 保存到xlsx 
                            df_input.to_excel( path_table_sub + file_table,index=False )
                            file_table = "stock_Info_" + time_begin_str + ".xlsx"
                            df_input.to_excel( path_table_sub + file_table,index=False )                        
                        else:
                            df_data.to_excel( path_table_sub + file_table,index=False )
                        #################################
                        ### 更新 log_table
                        df_log_table.loc[temp_date, "if_data"]= 1
                        df_log_table.to_excel( path_log + file_log_table ,index=False ) 
                        time.sleep(0.1)  

                count_date = count_date +1  
        ############################################
        ### 1.1.7 ST股票列表，st_stock_list()	


        ################################################################################################################
        ### 按个股-开始和结束时间-返回季度财务数据
        ######################################
        ### "stock_key_indicator" 
                 
        
        ##################################################################################################################
        ### 按个股-季度末：
        ######################################
        ### 获取交易日和财报日期的匹配
        
        #####################################################         
        ### 1.4.7 利润表-一般企业(单季)	4	0	quarter	| notes:financial_gene_qincome所有股票数据从20160930才有！
        # financial_gene_qincome(en_prod_code="600570.SH",report_date="2020-12-31")	en_prod_code=证劵代码，默认参数为：600570.SH，支持同时输入多个股票代码；report_date申报日期，默认参数为：2020-12-31；report_type财报类型，默认参数为：0 ，0-合并未调整,1-合并调整,2-母公司未调整，3-母公司调整	
        # 1.根据2007年新会计准则制定的一般企业利润表（单季度）模板，收录自公布季报以来公司的单季利润表情况。2.科目的计算方法：第一、三季度直接取公布值；第二季度数据＝半年度数据－第一季度数据；第四季度数据＝年度数据－前三季度数据。各期的原始数据均取合并后的最新数据（有调整的为最新调整后数据）；
        
        
        ##################################################################################################################
        ### 按个股-区间交易日：
        ###############################################
        ### 1.2.17 日行情序列 ； 单只股票一次只能返回200条数据！
        # if table_name == "stock_quote_daily_list" :
        #     index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
        #     date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
        #     temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
        #     # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
        #     time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
        #     print("date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
        #     ################################################
        #     ### 每30天更新一次基础信息
        #     if time_diff0 >= dt.timedelta(days= 1 ) :
        #         df_log.loc[index_date,"date_last"] = time_begin
        #         from hs_udata import stock_quote_daily_list
        #         count_code = 0 
        #         for temp_code in df_log_table.index :
        #             ### 判断距离上一次更新时间是否超过1个季度
        #             date_last = df_log_table.loc[temp_code, "date_last"]
        #             print("date_last",date_last)
        #             time_diff = time_begin - dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  
        #             print("time_diff ,",time_diff ,time_begin ,dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  )
        #             if time_diff >= dt.timedelta(days= 1.2 ) :
        #                 print("Update code:",count_code, temp_code ,"date_last:" ,date_last, "time_diff=",time_diff.days  )
        #                 ### 对于季度数据，每超过30天就抓取一次数据 | 结束日期默认最新日期
        #                 ######################################
        #                 ### 获取交易日的数据,确定需要下载的交易日|为了避免错过交易日，每次多下载几天
        #                 date_pre5 = dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d") - dt.timedelta(days= 5 )
        #                 date_pre5 = dt.datetime.strftime(date_pre5, "%Y%m%d")
        #                 date_list_sub = [ i for i in date_list if i>= int( date_pre5 ) ]
        #                 ### int 20030301 to str "2003-03-01" 
        #                 date_list_sub_str = [ str(i)[:4] + "-" + str(i)[4:6]+ "-"+str(i)[-2:] for i in date_list if (i<=int(time_begin_str)-1 and i> int(date_last ) )]
        #                 ### 将交易日切片，分别获取数据
        #                 len_date = len( date_list_sub  )
        #                 if len_date > 1 :
        #                     temp_para1 = len_date // 180
        #                     temp_para2 = len_date % 180
        #                     count_quarter = 0
        #                     for j in range( temp_para1 ) :
        #                         # (0,180),(180,360)....
        #                         date_begin = date_list_sub_str[j*180] 
        #                         date_end = date_list_sub_str[j*180+180]
        #                         print("Working on ",date_begin ,date_end )

        #                         df_data = stock_quote_daily_list(en_prod_code= temp_code ,begin_date=date_begin ,end_date=date_end,adjust_way=0)
        #                         ### notes:这个脚本很抠搜，居然返回列表没有股票代码
        #                         df_data["code"] = temp_code
        #                         if count_quarter == 0 :
        #                             df_table = df_data
        #                         else :
        #                             df_table = df_table.append(df_data,ignore_index=True )
        #                         count_quarter = count_quarter + 1 
        #                         time.sleep(0.4) 
        #                     ### 下载最后的尾数
        #                     if temp_para2 > 0 and temp_para1 > 0 :
        #                         ### 日期有余数，还要再下载一次
        #                         date_begin = date_list_sub_str[j*180+180 ] 
        #                         date_end = date_list_sub_str[-1]
        #                         print("Working on ",date_begin ,date_end )

        #                         df_data = stock_quote_daily_list(en_prod_code= temp_code ,begin_date=date_begin ,end_date=date_end,adjust_way=0)
        #                         df_data["code"] = temp_code
        #                         if count_quarter == 0 :
        #                             df_table = df_data
        #                         else :
        #                             df_table = df_table.append(df_data,ignore_index=True )
        #                         count_quarter = count_quarter + 1 
        #                         time.sleep(0.4) 
        #                     ### 需要下载的全部交易日不到180天
        #                     if temp_para2 > 0 and temp_para1 == 0 :
        #                         ### 日期有余数，还要再下载一次
        #                         date_begin = date_list_sub_str[0 ] 
        #                         date_end = date_list_sub_str[-1]
        #                         print("Working on ",date_begin ,date_end )
        #                         df_data = stock_quote_daily_list(en_prod_code= temp_code ,begin_date=date_begin ,end_date=date_end,adjust_way=0)
        #                         df_data["code"] = temp_code
        #                         df_table = df_data                                
        #                         time.sleep(0.4) 

        #                     ### 剔除股票s所有季度的重复数据
        #                     df_table = df_table.drop_duplicates()
        #                     ######################################
        #                     ### 将每行数据按日期格式保存 ；从本地目录读取文件
        #                     temp_date_list = list( df_table["trading_date"].drop_duplicates() )
        #                     for temp_date in temp_date_list :
        #                         file_table = str(temp_date) + ".xlsx"
        #                         df_table_date = df_table[ df_table["trading_date"]==temp_date ]
        #                         if os.path.exists( path_table_sub +file_table ) :
        #                             df_data = pd.read_excel( path_table_sub + file_table  )
        #                             ### 将数据去重复项后加入总序列。
        #                             df_data =df_data.append( df_table_date,ignore_index=True )
        #                             df_data =df_data.drop_duplicates()
        #                             ### 保存到xlsx 
        #                             df_data.to_excel( path_table_sub + file_table,index=False )
        #                         else :
        #                             df_table_date.to_excel( path_table_sub + file_table,index=False )
        #                 ######################################
        #                 ### 更新 log_table
        #                 df_log_table.loc[temp_code, "date_last"] = time_begin_str
 
        #             ###################################### 
        #             ### 每50个股票导出一次
        #             if count_code % 50 == 0 :
        #                 ### 保存log_table
        #                 df_log_table.to_excel( path_log + file_log_table,index=False ) 
        #             count_code = count_code + 1       
        
        # ############################################################################
        # ### 保存log_table,df_log-所有表格
        # df_log_table.to_excel( path_log + file_log_table,index=False ) 
        # df_log.to_excel( path_yes +  "manage_yes.xlsx" ,index=False ) 

        return obj_data

    ############################################
    ### 获取基金数据
    def get_table_by_fund(self, obj_data ):
        ############################################################################
        ### 1,导入基础数据；
                
        ########################################
        ### df_log.index是股票；根据输入表格和参数，获取表格数据
        table_name = obj_data["dict"]["table_name"] # "stock_key_indicator"
        
        ### 目标是近1年交易日         
        date_list = obj_data["date_list"] 
        date_list_hk = obj_data["date_list_hk"]
        path_yes = obj_data["dict"]["path_yes"] 
        path_log = obj_data["dict"]["path_log"] 
        path_table  = obj_data["dict"]["path_table"]  
        df_log = obj_data["df_log"]
        # code_list = obj_data["dict"]["code_list"] 
        time_begin = obj_data["dict"]["time_begin"] 
        time_begin_str = obj_data["dict"]["time_begin_str"] 

        ######################################
        ### 试用账号只能下载近1年的。
        time_begin_str_pre1 =  str(int(time_begin_str[:4])-1) + time_begin_str[-4: ]  
        date_list_1y = [ i for i in date_list if i > int(time_begin_str_pre1) ]
        date_list_1y =date_list_1y[1:]
        ### notes:通联数据不是365天，而是当年开始算，所以在211221，用201223取不到数据。
        ######################################
        ### 导入表对应的日志文件，判断需要更新的内容
        file_log_table = "log_" + table_name +".xlsx"
        # 列是股票代码，column是开始日期，上一次更新日期，对应季度末
        if os.path.exists( path_log + file_log_table ) :
            ### 读取文件
            df_log_table = pd.read_excel( path_log + file_log_table  ) 
            ### 对index临时赋值
            df_log_table.index = df_log_table["date"]
            ### 检查是否要加入新的代码
            date_list_new = [ i for i in df_log_table.index if i not in date_list_1y ]
            for temp_date in date_list_new :
                df_log_table.loc[temp_date, "frequency"] = "day"
                df_log_table.loc[temp_date, "date"]= temp_date 
                df_log_table.loc[temp_date, "if_data"]= 0
                ### 对每个股票，需要获取上市日期，避免浪费时间取上市前的日期
            
        else :
            ### 首次为表格 新建df_log_table
            df_log_table = pd.DataFrame( index=date_list_1y ,columns=["date","frequency","if_data" ] )
            df_log_table["frequency"] = "day"
            df_log_table["date"] = df_log_table.index
            df_log_table["if_data"] = 0
            df_log_table.to_excel( path_log + file_log_table,index=False ) 

        ### 去重复项
        df_log_table = df_log_table.drop_duplicates()
        ######################################
        ### 2,按交易日下载每日A股行情数据  
        ######################################
        ### 设置表的目录
        path_table_sub = path_table + table_name +"\\"
        if not os.path.exists( path_table_sub ) :
            os.makedirs( path_table_sub )

        ################################################################################################################
        ### 股票基础信息
        # 沪深股票前复权行情 getMktEqudAdj
        # if table_name == "getMktEqud" :     
        if table_name[:3] == "get" :     
            index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
            date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
            temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
            # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
            time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
            print("date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
            ################################################
            ### 每30天更新一次基础信息
            count_date = 0 
            for temp_date in df_log_table.index :
                ### 判断该日期是否更新过
                # print("Debug==" , type(df_log_table.loc[temp_date, "if_data"] ),df_log_table.loc[temp_date, "if_data"] ) 
                if df_log_table.loc[temp_date, "if_data"] == 0 :
                    print("table_name",table_name, "Update date:",temp_date,    )
                    ### 每个交易日一次性获取全部股票数据
                    ######################################
                    ### 构建输入参数 str_input;url_json= 
                    #"/api/market/getMktEqud.json?field=&beginDate=&endDate=&secID=&ticker=688001,000001&tradeDate=20210723"
                    # /api/market/getMktEqudAdj.json?field=&secID=&ticker=688001&beginDate=&endDate=&tradeDate=20190816
                    # /api/market/getMktEqudEval.json?field=&secID=&ticker=688001&tradeDate=20190821&beginDate=&endDate=
                    # /api/market/getMktEquFlowOrder.json?field=&secID=&ticker=688001&beginDate=20190823&endDate=20190823
                    url_head = "/api/market/"
                    url_table_name = table_name
                    url_mid = ".json?field=&beginDate=&endDate=&secID=&ticker=&tradeDate="
                    url_date = str( temp_date )
                    url_json= url_head +url_table_name +url_mid +url_date
                    print("url_json=",url_json) 
                    ######################################
                    ### 获取数据
                    ### status_code=200 表示成功，
                    status_code, result_bytes = self.client.getData(url_json)
                    ### bytes to dict 
                    temp_dict = json.loads( result_bytes )
                    ### dict to DataFrame
                    if not temp_dict["retCode"] == 1 :
                        print( temp_dict )
                    else :
                        df_data = pd.DataFrame( temp_dict["data"] )

                        ######################################
                        ### 从本地目录读取文件
                        file_table = table_name +"_" + str(temp_date)  + ".xlsx"
                        if os.path.exists( path_table_sub +file_table ) :
                            df_input = pd.read_excel( path_table_sub + file_table )
                            ### 将数据去重复项后加入总序列。
                            df_input = df_input.append( df_data,ignore_index=True )
                            df_input =df_input.drop_duplicates()
                            #################################
                            ### 保存到xlsx 
                            df_input.to_excel( path_table_sub + file_table,index=False )
                            file_table = "stock_Info_" + time_begin_str + ".xlsx"
                            df_input.to_excel( path_table_sub + file_table,index=False )                        
                        else:
                            df_data.to_excel( path_table_sub + file_table,index=False )
                        #################################
                        ### 更新 log_table
                        df_log_table.loc[temp_date, "if_data"]= 1
                        df_log_table.to_excel( path_log + file_log_table ,index=False ) 
                        time.sleep(0.1)  

                count_date = count_date +1  
        ############################################
        ### 1.1.7 ST股票列表，st_stock_list()	


        ################################################################################################################
        ### 按个股-开始和结束时间-返回季度财务数据
        ######################################
        ### "stock_key_indicator" 
                 
        
        ##################################################################################################################
        ### 按个股-季度末：
        ######################################
        ### 获取交易日和财报日期的匹配
        
        #####################################################         
        ### 1.4.7 利润表-一般企业(单季)	4	0	quarter	| notes:financial_gene_qincome所有股票数据从20160930才有！
        # financial_gene_qincome(en_prod_code="600570.SH",report_date="2020-12-31")	en_prod_code=证劵代码，默认参数为：600570.SH，支持同时输入多个股票代码；report_date申报日期，默认参数为：2020-12-31；report_type财报类型，默认参数为：0 ，0-合并未调整,1-合并调整,2-母公司未调整，3-母公司调整	
        # 1.根据2007年新会计准则制定的一般企业利润表（单季度）模板，收录自公布季报以来公司的单季利润表情况。2.科目的计算方法：第一、三季度直接取公布值；第二季度数据＝半年度数据－第一季度数据；第四季度数据＝年度数据－前三季度数据。各期的原始数据均取合并后的最新数据（有调整的为最新调整后数据）；
        
        
        ##################################################################################################################
        ### 按个股-区间交易日：
        ###############################################
        ### 1.2.17 日行情序列 ； 单只股票一次只能返回200条数据！
        # if table_name == "stock_quote_daily_list" :
        #     index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
        #     date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
        #     temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
        #     # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
        #     time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
        #     print("date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
        #     ################################################
        #     ### 每30天更新一次基础信息
        #     if time_diff0 >= dt.timedelta(days= 1 ) :
        #         df_log.loc[index_date,"date_last"] = time_begin
        #         from hs_udata import stock_quote_daily_list
        #         count_code = 0 
        #         for temp_code in df_log_table.index :
        #             ### 判断距离上一次更新时间是否超过1个季度
        #             date_last = df_log_table.loc[temp_code, "date_last"]
        #             print("date_last",date_last)
        #             time_diff = time_begin - dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  
        #             print("time_diff ,",time_diff ,time_begin ,dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  )
        #             if time_diff >= dt.timedelta(days= 1.2 ) :
        #                 print("Update code:",count_code, temp_code ,"date_last:" ,date_last, "time_diff=",time_diff.days  )
        #                 ### 对于季度数据，每超过30天就抓取一次数据 | 结束日期默认最新日期
        #                 ######################################
        #                 ### 获取交易日的数据,确定需要下载的交易日|为了避免错过交易日，每次多下载几天
        #                 date_pre5 = dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d") - dt.timedelta(days= 5 )
        #                 date_pre5 = dt.datetime.strftime(date_pre5, "%Y%m%d")
        #                 date_list_sub = [ i for i in date_list if i>= int( date_pre5 ) ]
        #                 ### int 20030301 to str "2003-03-01" 
        #                 date_list_sub_str = [ str(i)[:4] + "-" + str(i)[4:6]+ "-"+str(i)[-2:] for i in date_list if (i<=int(time_begin_str)-1 and i> int(date_last ) )]
        #                 ### 将交易日切片，分别获取数据
        #                 len_date = len( date_list_sub  )
        #                 if len_date > 1 :
        #                     temp_para1 = len_date // 180
        #                     temp_para2 = len_date % 180
        #                     count_quarter = 0
        #                     for j in range( temp_para1 ) :
        #                         # (0,180),(180,360)....
        #                         date_begin = date_list_sub_str[j*180] 
        #                         date_end = date_list_sub_str[j*180+180]
        #                         print("Working on ",date_begin ,date_end )

        #                         df_data = stock_quote_daily_list(en_prod_code= temp_code ,begin_date=date_begin ,end_date=date_end,adjust_way=0)
        #                         ### notes:这个脚本很抠搜，居然返回列表没有股票代码
        #                         df_data["code"] = temp_code
        #                         if count_quarter == 0 :
        #                             df_table = df_data
        #                         else :
        #                             df_table = df_table.append(df_data,ignore_index=True )
        #                         count_quarter = count_quarter + 1 
        #                         time.sleep(0.4) 
        #                     ### 下载最后的尾数
        #                     if temp_para2 > 0 and temp_para1 > 0 :
        #                         ### 日期有余数，还要再下载一次
        #                         date_begin = date_list_sub_str[j*180+180 ] 
        #                         date_end = date_list_sub_str[-1]
        #                         print("Working on ",date_begin ,date_end )

        #                         df_data = stock_quote_daily_list(en_prod_code= temp_code ,begin_date=date_begin ,end_date=date_end,adjust_way=0)
        #                         df_data["code"] = temp_code
        #                         if count_quarter == 0 :
        #                             df_table = df_data
        #                         else :
        #                             df_table = df_table.append(df_data,ignore_index=True )
        #                         count_quarter = count_quarter + 1 
        #                         time.sleep(0.4) 
        #                     ### 需要下载的全部交易日不到180天
        #                     if temp_para2 > 0 and temp_para1 == 0 :
        #                         ### 日期有余数，还要再下载一次
        #                         date_begin = date_list_sub_str[0 ] 
        #                         date_end = date_list_sub_str[-1]
        #                         print("Working on ",date_begin ,date_end )
        #                         df_data = stock_quote_daily_list(en_prod_code= temp_code ,begin_date=date_begin ,end_date=date_end,adjust_way=0)
        #                         df_data["code"] = temp_code
        #                         df_table = df_data                                
        #                         time.sleep(0.4) 

        #                     ### 剔除股票s所有季度的重复数据
        #                     df_table = df_table.drop_duplicates()
        #                     ######################################
        #                     ### 将每行数据按日期格式保存 ；从本地目录读取文件
        #                     temp_date_list = list( df_table["trading_date"].drop_duplicates() )
        #                     for temp_date in temp_date_list :
        #                         file_table = str(temp_date) + ".xlsx"
        #                         df_table_date = df_table[ df_table["trading_date"]==temp_date ]
        #                         if os.path.exists( path_table_sub +file_table ) :
        #                             df_data = pd.read_excel( path_table_sub + file_table  )
        #                             ### 将数据去重复项后加入总序列。
        #                             df_data =df_data.append( df_table_date,ignore_index=True )
        #                             df_data =df_data.drop_duplicates()
        #                             ### 保存到xlsx 
        #                             df_data.to_excel( path_table_sub + file_table,index=False )
        #                         else :
        #                             df_table_date.to_excel( path_table_sub + file_table,index=False )
        #                 ######################################
        #                 ### 更新 log_table
        #                 df_log_table.loc[temp_code, "date_last"] = time_begin_str
 
        #             ###################################### 
        #             ### 每50个股票导出一次
        #             if count_code % 50 == 0 :
        #                 ### 保存log_table
        #                 df_log_table.to_excel( path_log + file_log_table,index=False ) 
        #             count_code = count_code + 1       
        
        # ############################################################################
        # ### 保存log_table,df_log-所有表格
        # df_log_table.to_excel( path_log + file_log_table,index=False ) 
        # df_log.to_excel( path_yes +  "manage_yes.xlsx" ,index=False ) 

        return obj_data

    
    

    ##########################################################################################
    ### Part 4 转换和维护数据 data_transeform
    def trans_table_code2date(self,obj_data) :
        ### 将基于代码的数据改成基于日期的数据文件
        ### column_date= "report_date"，是表格里用于区分日期的列名
        column_date = obj_data["dict"]["column_date"] #
        table_name = obj_data["dict"]["table_name"]  
        ### 
        path_yes = obj_data["dict"]["path_yes"] 
        path_log = obj_data["dict"]["path_log"] 
        path_table  = obj_data["dict"]["path_table"]  
        df_log = obj_data["df_log"]
        code_list = obj_data["dict"]["code_list"] 
        time_begin = obj_data["dict"]["time_begin"] 
        time_begin_str = obj_data["dict"]["time_begin_str"] 
        date_list = obj_data["date_list"] 
        ### 获取基金列表
        fund_list_open = obj_data["dict"]["fund_list_open"] 
        fund_list_etf = obj_data["dict"]["fund_list_etf"] 
        fund_list_all = fund_list_open + fund_list_etf 
        ######################################
        ### 设置表的目录
        path_table_sub = path_table + table_name +"\\"
        if not os.path.exists( path_table_sub ) :
            os.makedirs( path_table_sub )

        ######################################
        ### 1,读取log文件
        file_log_table = "log_" + table_name +".xlsx"
        # 列是股票代码，column是开始日期，上一次更新日期，对应季度末
        df_log_table = pd.read_excel( path_log + file_log_table ) 
        ### 对index临时赋值
        if "code" in df_log_table.columns :
            df_log_table.index = df_log_table["code"]
        elif "fund" in df_log_table.columns :
            df_log_table.index = df_log_table["fund"]

        ###########################################
        ### 2,对log文件里的每个代码，读取文件
        count_code = 0 
        for temp_i in df_log_table.index :
            print("Code=",temp_i ,count_code )
            file_table = temp_i + ".xlsx"
            if os.path.exists( path_table_sub + file_table ) :
                df_excel = pd.read_excel( path_table_sub + file_table  )  
                ############################################
                ### notes： 1.2.17 日行情序列, "stock_quote_daily_list"保存时不包括个股代码，需要调整
                if table_name == "stock_quote_daily_list":
                    df_excel["code"] = temp_i
                ############################################
                ### append to df 
                if count_code == 0 :
                    df_all = df_excel
                else :
                    df_all = df_all.append( df_excel,ignore_index=True )
                count_code = count_code + 1 
            ############################################
            ### 每50至100个代码，集体输出一次！提高效率
            if count_code % 500 == 0 : 
                df_all= df_all.drop_duplicates()
                ###########################################
                ### 获取所有日期列表，有的是交易日，有的是季末
                column_date_list = list( df_all[column_date].drop_duplicates() )
                for temp_date in column_date_list :
                    print("Date=",temp_date )
                    ### find same item in df_all
                    df_all_date = df_all[ df_all[column_date]==temp_date ]
                    ### find file in dir
                    file_name = str( temp_date ) +".xlsx"
                    if os.path.exists( path_table_sub + file_name ) : 
                        df_excel = pd.read_excel( path_table_sub + file_name  )  
                        ### 合并、去重
                        df_excel = df_excel.append( df_all_date ,ignore_index=True ) 
                        df_excel = df_excel.drop_duplicates() 
                        df_excel.to_excel( path_table_sub + file_name ,index=False ) 

                    else :                   
                        df_all_date.to_excel( path_table_sub + file_name,index=False ) 
                ###########################################
                ### 
                df_all = df_excel
        ############################################
        ### 全部结束后，再算一次
        df_all= df_all.drop_duplicates()
        ###########################################
        ### 获取所有日期列表，有的是交易日，有的是季末
        column_date_list = list( df_all[column_date].drop_duplicates() )
        for temp_date in column_date_list :
            print("Date=",temp_date )
            ### find same item in df_all
            df_all_date = df_all[ df_all[column_date]==temp_date ]
            ### find file in dir
            file_name = str( temp_date ) +".xlsx"
            if os.path.exists( path_table_sub + file_name ) :
                df_excel = pd.read_excel( path_table_sub + file_name  )  
                ### 合并、去重
                df_excel = df_excel.append( df_all_date ,ignore_index=True )
                df_excel = df_excel.drop_duplicates()
                df_excel.to_excel( path_table_sub + file_name ,index=False ) 

            else :                   
                df_all_date.to_excel( path_table_sub + file_name,index=False )



        return obj_data