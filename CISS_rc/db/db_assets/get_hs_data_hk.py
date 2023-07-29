# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
功能：抓取A股、港股、基金等数据
数据来源： 恒生量化数据接口

last update  | since  211207
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
### class_config_data.obj_config["dict"]["path_data_hs"] = "D:\\db_wind\\data_hs\\" 
#  self.obj_config["dict"]["path_disk"] = "D:\\"   

###########################################################
### 这里导入没用 import hs_udata

class data_hs_hk():
    # 类的初始化操作
    def __init__(self):
        
        #################################################################################
        ### Initialization   
        self.path_out = class_config_data.obj_config["dict"]["path_disk"]
        self.path_data_hs = class_config_data.obj_config["dict"]["path_data_hs"]
        ### 调整后的数据
        # self.obj_config["dict"]["path_wind_adj"]  

        #############################################
        ### 
        self.path_log_table = os.getcwd()[:2] +"\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\"
        # self.col_list= ["No.","table_name","prime_key","prime_key_value","datetime_range","last_update"]
        self.col_list= ["No.","table_name","prime_key","prime_key_value","datetime_range"]
        # 数据更新的日志表格
        self.log_table_name = "wind_wds_tables.csv"

        if  os.path.exists( os.getcwd()[:2] +"\\zd_zxjtzq\\ciss_web\\" ) :
            self.path_rc_data = os.getcwd()[:2] +"\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
        else :
            self.path_rc_data = os.getcwd()[:2] +"\\ciss_web\\CISS_rc\\apps\\rc_data\\"
        ### 
        self.nan = np.nan 
        #############################################
        ### 设置token，恒有数登录成功后，点击右上角 → 总览，进入用户个人中心
        self.token = "saPMz6Ha_5LA6dJrzgMttOmUvQ0escUMloF62PBFWn10inIO32vePkNARtSs36Ay"
        ### 
        from hs_udata import set_token
        set_token(token = self.token )


    def print_info(self): 
        #############################################
        ### Part 2 标准化获取数据
        print("data_fetch |get_table_by_hk: df_log.index是股票，按股票下载") 
        
        #############################################
        ### Part 3 标准化读取和写入数据 |  解决不了代码内导入不同模块的需求
        # print("data_io | export_log_table_1shot : 导出一次性获取的数据df_table和日志df_log") 

        return 1 
    ##########################################################################################
    ### Part 3  标准化读取和写入数据 || 解决不了代码内导入不同模块的需求

    ##########################################################################################
    ### Part 2 标准化获取数据 
     

    ############################################
    ### 获取港股股票数据
    def get_table_by_hk(self, obj_data ):
        ############################################################################
        ### df_log.index是股票；根据输入表格和参数，获取表格数据
        table_name = obj_data["dict"]["table_name"] # "stock_key_indicator"

        path_hs = obj_data["dict"]["path_hs"] 
        path_log = obj_data["dict"]["path_log"] 
        path_table  = obj_data["dict"]["path_table"]  
        df_log = obj_data["df_log"]
        time_begin = obj_data["dict"]["time_begin"] 
        time_begin_str = obj_data["dict"]["time_begin_str"] 
        date_list_hk = obj_data["date_list_hk"] 
        ### 港股股票列表
        code_list_hk = obj_data["code_list_hk"] 
        code_list_hk_delisted = obj_data["code_list_hk_delisted"] 
        ### 只包括港股主板股票
        code_list_hk_list = obj_data["code_list_hk_list"] 

        ######################################
        ### 导入表对应的日志文件，判断需要更新的内容
        file_log_table = "log_" + table_name +".xlsx"
        # 列是股票代码，column是开始日期，上一次更新日期，对应季度末
        
        ######################################
        ### 港股垃圾股太多，只选择sh_hk_flag沪港通标的、sz_hk_flag深港通标的；value:否,是；"listed_state"=="上市"
        file_table = "hk_secu" + ".xlsx"
        temp_path = path_table + "hk_secu\\"
        df_hk_secu = pd.read_excel( temp_path + file_table  )

        if os.path.exists( path_log + file_log_table ) :
            ### 读取文件
            df_log_table = pd.read_excel( path_log + file_log_table  ) 
            ### 对index临时赋值
            df_log_table.index = df_log_table["code"]
            ### 只更新继续上市的主板股票
            code_list_hk_list_new = [i for i in code_list_hk_list if i not in df_log_table.index ] 
            ### 检查是否要加入新的代码 
            for temp_code in code_list_hk_list_new :
                df_log_table.loc[temp_code, "frequency"] = "quarter"
                df_log_table.loc[temp_code, "code"]= temp_code
                ### 
                df_temp = df_hk_secu[df_hk_secu["prod_code"]==temp_code ]
                if len(df_temp.index) > 0 :
                    temp_list_date = df_temp["listed_date"].values[0]
                    temp_int = int( temp_list_date.replace("-","") )
                    df_log_table.loc[temp_code, "date_init"]= max(20030101,temp_int)
                    df_log_table.loc[temp_code, "date_last"]= max(20030101,temp_int)
                else :
                    df_log_table.loc[temp_code, "date_init"]= 20030101
                    df_log_table.loc[temp_code, "date_last"]= 20030101
                ### 对每个股票，需要获取上市日期，避免浪费时间取上市前的日期

        else :
            ### 首次为表格 新建df_log_table
            df_log_table = pd.DataFrame( index=code_list_hk_list,columns=["code","frequency","date_init","date_last"] )
            df_log_table["frequency"] = "quarter"
            for temp_code in df_log_table.index : 
                df_log_table.loc[temp_code, "code"]= temp_code
                ### 
                df_temp = df_hk_secu[df_hk_secu["prod_code"]==temp_code ]
                if len(df_temp.index) > 0 : 
                    temp_list_date = df_temp["listed_date"].values[0]
                    temp_int = int( temp_list_date.replace("-","") )
                    df_log_table.loc[temp_code, "date_init"]= max(20030101,temp_int)
                    df_log_table.loc[temp_code, "date_last"]= max(20030101,temp_int)
                else :
                    df_log_table.loc[temp_code, "date_init"]= 20030101
                    df_log_table.loc[temp_code, "date_last"]= 20030101

            ### log文件导出时需要保留index
            df_log_table.to_excel( path_log + file_log_table )    

        ######################################
        ### 设置表的目录
        path_table_sub = path_table + table_name +"\\"
        if not os.path.exists( path_table_sub ) :
            os.makedirs( path_table_sub )

        ################################################################################################################
        ### 1.1.8 沪深港通成分股,quarter;shszhk_stock_list();截至211215，总数量377只
        # etfcomponent_type=默认取值1，1：港股通(沪)；				20030101	20030101	0
        if table_name == "shszhk_stock_list" :
            index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
            date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
            temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
            # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
            
            time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
            print("date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
            ################################################
            ### 每30天更新一次基础信息
            if time_diff0 >= dt.timedelta(days= 60 ) :
                df_log.loc[index_date,"date_last"] = time_begin
                from hs_udata import shszhk_stock_list 
                path_table_sub = path_table + table_name +"\\"
                if not os.path.exists( path_table_sub ) :
                    os.makedirs( path_table_sub )
                ### 
                df_table = shszhk_stock_list()
                ### 筛选 
                file_table = "shszhk_stock_list"  + ".xlsx"
                df_table.to_excel(path_table_sub + file_table ,index=False )
                file_table = "shszhk_stock_list_" +  time_begin_str + ".xlsx"
                df_table.to_excel(path_table_sub + file_table ,index=False )
            
                ### save to df_data
                df_data = df_table



        ################################################################################################################
        ### 5.2.1 港股日行情	5	0	month	0	1	hk_daily_quote(en_prod_code="00700.HK")	
        # en_prod_code，证劵代码，默认参数设置为：00700.HK；trading_date，交易日期，默认参数设置为：2021-05-20；
        # adjust_way，复权方式，默认参数设置为：1，0-不复权，1-前复权，2-后复权	港股日行情的指标，
        # 比如前收盘价、开盘价、最高价、最低价、收盘价、均价、涨跌、涨跌幅等指标；	hk_daily_quote		20030101	20030101	0
        if table_name == "hk_daily_quote" :            
            index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
            date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
            temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
            # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
            ### notes:如果 type(date_last0)== dt.datetime，则 int(date_last0)会报错
            if type(date_last0)== dt.datetime :
                time_diff0 = time_begin - date_last0
            else :
                time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
            print("date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
            ################################################
            ### 每30天更新一次基础信息
            if time_diff0 >= dt.timedelta(days= 5 ) :
                from hs_udata import hk_daily_quote
                df_log.loc[index_date,"date_last"] = time_begin                       
                #######################################
                ### 导入沪深港通列表： 
                # file_table_temp = "shszhk_stock_list"  + ".xlsx"
                # path_table_temp = path_table + "shszhk_stock_list" +"\\"
                # ### pd.read_excel,保留原始的数字字符串 dtype=str
                # df_shszhk = pd.read_excel(path_table_temp + file_table_temp ,dtype=str  )
                # code_list_hk = list( df_shszhk["secu_code"] )
                # ### 00700 to 00700.HK
                # code_list_hk = [ str(i) +".HK" for i in code_list_hk ]
                #######################################
                ### 导入每个股票上市日期，避免浪费时间取上市前的日期；"prod_code"；listed_date
                file_table_temp = "hk_secu"  + ".xlsx"
                path_table_temp = path_table + "hk_secu" +"\\"
                df_list_date = pd.read_excel(path_table_temp + file_table_temp  )  
                code_list_all = list( df_list_date["prod_code"])

                count_code = 0
                num_code = len( code_list_all )
                ### notes:df_log_table.index 是梳理过的按照绩优基金优先的顺序排列
                for temp_code in code_list_all : 
                # for temp_code in code_list_hk : 
                    #######################################
                    ### 日期分析1：对每个股票，获取上市日期，避免浪费时间取上市前的日期
                    ### notes:只有部分股票代码时需要;find list date of code:  hk_secu:"listed_date", 1999-02-03
                    df_temp = df_list_date[ df_list_date["prod_code"]== temp_code ]
                    if len( df_temp.index ) > 0 :
                        # print("df_temp,",df_temp["listed_date"],type( df_temp["listed_date"] ) )
                        temp_listed_date_str = df_temp["listed_date"].values[0]
                        temp_listed_date = int( temp_listed_date_str.replace("-","") )
                        if temp_listed_date > df_log_table.loc[temp_code, "date_init"] :
                            df_log_table.loc[temp_code, "date_init"] = temp_listed_date
                        date_init = df_log_table.loc[temp_code, "date_init"]
                        #######################################
                        ### 日期分析2：获取需要更新的区间
                        date_last = df_log_table.loc[temp_code, "date_last"]
                        time_diff = time_begin - dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d") 
                        print("time_diff ,",time_diff ,time_begin ,dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  )
                    
                    ### 每14天下载一次就好了
                    if len( df_temp.index ) > 0  and time_diff >= dt.timedelta(days= 14 ) : 
                        ### 获取日期区间 
                        ### int 20030301 to str "2003-03-01"  | notes:港股通开始于2014-3
                        date_list_hk = [i for i in date_list_hk if i >20140101 ]
                        date_list_hk_sub_str = [ str(i)[:4] + "-" + str(i)[4:6]+ "-"+str(i)[-2:] for i in date_list_hk if (i<=int(time_begin_str)-1 and i> int(date_last ) )]
                        count_date =0
                        
                        ######################################
                        ### NOTES：为了避免频繁更新日期文件，数据只保存在单个代码的文件里。
                        for temp_date_sub in  date_list_hk_sub_str : 
                            print("count_code=",round( count_code/num_code,4)   ,"Update code:", temp_code ,temp_date_sub,";date_last:" ,date_last, ";lastest=",time_begin_str )
                            ### 只能一个一个交易日获取 
                            df_data = hk_daily_quote(en_prod_code = temp_code,trading_date= temp_date_sub  ) 
                            time.sleep(0.4) 
                            if count_date == 0 :
                                df_table = df_data
                            else :
                                df_table = df_table.append(df_data,ignore_index=True )   
                            ### 
                            count_date = count_date + 1 

                            ######################################
                            ### 分别保存到本地按日期命名的目录，每个基金每个净值日都下载，也可以消耗时间
                            # Notes: temp_date_sub=2020-02-19,需要改成 20200219
                            
                            # temp_date_short = temp_date_sub.replace("-","")
                            # file_table = temp_date_short + ".xlsx" 
                            # if os.path.exists( path_table_sub +file_table ) :
                            #     df_excel = pd.read_excel( path_table_sub + file_table  )  
                            #     ### 将数据去重复项后加入总序列。### 仅保存单一代码单一日期的数据 
                            #     df_excel = df_excel.append( df_data,ignore_index=True )  
                            #     ### notes:按 "prod_code" 去除重复项
                            #     df_excel = df_excel.drop_duplicates(subset=["prod_code"] ,keep="last" )  
                            #     ### 保存到xlsx 
                            #     df_excel.to_excel( path_table_sub + file_table,index=False )
                                
                            # else :
                            #     print("file_table ",file_table)
                            #     ### 仅保存单一代码单一日期的数据 
                            #     df_data.to_excel( path_table_sub + file_table,index=False )
                                                       
                        
                        ###################################### 
                        ### 遇到周末的2天，可能 df_table是空的
                        if "df_table" in vars() :
                            ### 剔除股票s所有季度的重复数据
                            df_table = df_table.drop_duplicates()
                            ######################################
                            ### 从本地目录读取文件
                            file_table = temp_code + ".xlsx"
                            if os.path.exists( path_table_sub +file_table ) :
                                df_excel = pd.read_excel( path_table_sub + file_table  )
                                ### 将数据去重复项后加入总序列。
                                df_excel =df_excel.append( df_table,ignore_index=True )
                                df_excel =df_excel.drop_duplicates()
                                ### 保存到xlsx 
                                df_excel.to_excel( path_table_sub + file_table,index=False )
                                
                            else :
                                df_table.to_excel( path_table_sub + file_table,index=False )

                        ######################################
                        ### notes：只有全部code都更新完才能改
                        ### 更新 log_table | 不管数据有没有更新，都当作已经更新了一次数据
                        df_log_table.loc[temp_code, "date_last"] = time_begin_str
                        count_code = count_code +1 
                        # ######################################
                        # ### save log file for every 50 funds 
                        # if count_code % 50 == 0 :
                        df_log_table.to_excel( path_log + file_log_table,index=False ) 
        



        ################################################################################################################
        ### 5.1.4 港股股票基本信息	3	0	month	0	1	hk_secu(en_prod_code="00700.HK")	
        # en_prod_code，证劵代码，默认参数为：00700.HK；trading_date,默认参数为：2021-05-20	股票交易代码、股票简称、上市时间、上市状态、上市板块、所属概念板块及可能有的同公司A股、B股信息等信息；
        # 需要上市日期来确保不浪费下载时间 ||   hk_secu(en_prod_code="00700.HK")        

        if table_name == "hk_secu" :
            index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
            date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
            temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
            # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
            time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
            print("date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
            ################################################
            ### 每30天更新一次基础信息
            if time_diff0 >= dt.timedelta(days= 90 ) :
                df_log.loc[index_date,"date_last"] = time_begin
                from hs_udata import hk_secu
                count_code = 0 
                for temp_code in code_list_hk_list :
                    ### 判断距离上一次更新时间是否超过1个季度
                    print("Debug=",temp_code ,df_log_table.loc[temp_code, "date_last"] )
                    date_last = df_log_table.loc[temp_code, "date_last"]
                    time_diff = time_begin - dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  
                    if time_diff >= dt.timedelta(days= 30 )  :
                        print("Update code:",count_code, temp_code ,"date_last:" ,date_last, "time_diff=",time_diff.days  )
                        ### 对于季度数据，每超过30天就抓取一次数据 | 结束日期默认最新日期
                        df_table = hk_secu(en_prod_code= temp_code )	 
                        if count_code == 0  :
                            df_data =df_table
                        else :
                            df_data =df_data.append( df_table,ignore_index=True )
                        count_code =count_code +1  
                        #################################
                        ### 更新 log_table
                        df_log_table.loc[temp_code, "date_last"] = time_begin_str
                ### 只保留 上市状态的主板公司 
                df_data= df_data[ df_data["listed_sector" ] == "主板" ]
                df_data =df_data[ df_data[ "listed_state" ] == "上市" ]
                ##################################################################
                ### 从本地目录读取文件
                file_table = "hk_secu" + ".xlsx"
                if os.path.exists( path_table_sub +file_table ) :
                    df_input = pd.read_excel( path_table_sub + file_table  )
                    ### 将数据去重复项后加入总序列。
                    df_input = df_input.append( df_data,ignore_index=True )
                    df_input =df_input.drop_duplicates()
                    #################################
                    ### 保存到xlsx 
                    df_input.to_excel( path_table_sub + file_table,index=False )
                    
                else:
                    df_data.to_excel( path_table_sub + file_table,index=False )
                
                time.sleep(0.4)  
                ### log文件导出时需要保留index
                df_log_table.to_excel( path_log + file_log_table  )  

        ################################################################################################################
        ### 按个股-开始和结束时间-返回季度财务数据
        ######################################
        ### 准备季度数据等：获取交易日和财报日期的匹配
        from data_io import data_io
        class_data_io = data_io()
        obj_date = {}
        obj_date["dict"] ={}
        obj_date["dict"]["if_only_quarter"] = 1
        obj_date["dict"]["date_start"] = "20030101"
        obj_date["dict"]["date_end"] = time_begin_str
        obj_date = class_data_io.get_after_ann_days_fund( obj_date ) 
        ### 将日期数据合的df,df_date.columns=["before_ann","after_ann","report","quarter_end"] 
        df_date = obj_date["df_date"]   
        

        ######################################
        ### 5.3.4 港股盈利能力	4	0	quarter	0	1	hk_profit_ability(en_prod_code="00700.HK")	en_prod_code，证劵代码，默认参数为：00700.HK；report_date，申报日期，默认参数为：2020-12-31；report_type，财报类型，默认参数为：0，0-合并未调整，1-合并调整，2-母公司未调整，3-母公司调整；trading_date，交易日期，默认参数为：系统时间	港股上市公司的盈利能力的指标，如净资产收益率、总资产报酬率、销售毛利率、净利润/营业总收入、销售费用/营业总收入、销售毛利率等；
        if table_name == "hk_profit_ability" :
            index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
            date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
            temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
            # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
            time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
            print("date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
            ################################################
            ### 每30天更新一次基础信息
            date_init = 20160630 
            
            if time_diff0 >= dt.timedelta(days= 90 ) :
                df_log.loc[index_date,"date_last"] = time_begin
                from hs_udata import hk_profit_ability
                for temp_code in df_log_table.index :  
                    ### 判断距离上一次更新时间是否超过1个季度
                    date_last = df_log_table.loc[temp_code, "date_last"]
                    print("date_last",date_last)
                    time_diff = time_begin - dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  
                    print("time_diff ,",time_diff ,time_begin ,dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  )
                    if time_diff >= dt.timedelta(days= 30 ) :
                        print("Update code:",count_code, temp_code ,"date_last:" ,date_last, "time_diff=",time_diff.days  )
                        ### 对于季度数据，每超过30天就抓取一次数据 | 结束日期默认最新日期
                        ######################################
                        ### 需要有一个交易日和财报日期的转换
                        df_date_sub = df_date[ df_date["before_ann"] >= date_init ]
                        df_date_sub = df_date_sub[ df_date_sub["before_ann"] >= int(date_last ) ]
                        list_quarter_end = list(df_date_sub["quarter_end"].drop_duplicates().values ) 
                        # print("Debug=list_quarter_end,",list_quarter_end)
                        count_quarter = 0 
                        for temp_quarter_end in list_quarter_end : 
                            print("Working on ",temp_code,temp_quarter_end )
                            # "en_prod_code=证劵代码 默认参数为：600570.SH；report_date=申报日期	默认参数为：2020-12-31；report_type=财报类型	默认参数为0，0-合并未调整，1-合并调整，2-母公司未调整，3-母公司调整"
                            # df_data = accounting_data(en_prod_code= temp_code ,report_date= str(temp_quarter_end) )	
                            date_str = str(temp_quarter_end)
                            date_str = date_str[:4] +"-" + date_str[4:6]+"-" + date_str[-2:]
                            df_data = hk_profit_ability(en_prod_code= temp_code ,report_date= date_str )	
                            if count_quarter == 0 :
                                df_table = df_data
                            else :
                                df_table = df_table.append(df_data,ignore_index=True )
                            ######################################
                            ### 分别保存到本地按日期命名的目录
                            file_table = date_str + ".xlsx"
                            if os.path.exists( path_table_sub +file_table ) :
                                df_excel = pd.read_excel( path_table_sub + file_table  )
                                ### 将数据去重复项后加入总序列。
                                df_excel = df_excel.append( df_data,ignore_index=True )
                                df_excel =df_data.drop_duplicates()
                                ### 保存到xlsx 
                                df_excel.to_excel( path_table_sub + file_table,index=False )
                                
                            else :
                                df_data.to_excel( path_table_sub + file_table,index=False )
                            ### 
                            count_quarter = count_quarter + 1 
                            time.sleep(0.4) 
                            
                        ### 剔除股票s所有季度的重复数据
                        df_table = df_table.drop_duplicates()
                        ######################################
                        ### 将每行数据按日期格式保存 ；从本地目录读取文件
                        temp_date_list = list( df_table["report_date"].drop_duplicates() )
                        for temp_date in temp_date_list :
                            file_table = str(temp_date) + ".xlsx"
                            df_table_date = df_table[ df_table["report_date"]==temp_date ]
                            if os.path.exists( path_table_sub +file_table ) :
                                df_data = pd.read_excel( path_table_sub + file_table  )
                                ### 将数据去重复项后加入总序列。
                                df_data =df_data.append( df_table_date,ignore_index=True )
                                df_data =df_data.drop_duplicates()
                                ### 保存到xlsx 
                                df_data.to_excel( path_table_sub + file_table,index=False )
                            else :
                                df_table_date.to_excel( path_table_sub + file_table,index=False )
                    
                    ### 更新 log_table
                    df_log_table.loc[temp_code, "date_last"] = time_begin_str 
                    df_log_table.loc[temp_code, "date_init"] = date_init
                ########################################################################
                ### log文件导出时需要保留index
                df_log_table.to_excel( path_log + file_log_table  )  

        ###############################################
        ### 保存 df_data
        # obj_data["df_data"]  = df_data 
        ### 保存log_table,df_log-所有表格
        df_log_table.to_excel( path_log + file_log_table,index=False ) 
        df_log.to_excel( path_hs +  "manage_hs.xlsx" ,index=False ) 

        return obj_data

