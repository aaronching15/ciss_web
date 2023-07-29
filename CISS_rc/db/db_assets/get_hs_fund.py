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

class data_hs_fund():
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
        print("data_fetch |get_table_by_fund: df_log.index是基金，按基金下载")
        
         
        return 1 
    
    ##########################################################################################
    ### Part 2 标准化获取数据
    
    ############################################
    ### 获取基金数据
    def get_table_by_fund(self, obj_data ):
        ############################################################################
        ### df_log.index是股票；根据输入表格和参数，获取表格数据
        table_name = obj_data["dict"]["table_name"] # "stock_key_indicator"

        path_hs = obj_data["dict"]["path_hs"] 
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
        ### 导入表对应的日志文件，判断需要更新的内容
        file_log_table = "log_" + table_name +".xlsx"
        # 列是股票代码，column是开始日期，上一次更新日期，对应季度末
        if os.path.exists( path_log + file_log_table ) :
            ### 读取文件
            df_log_table = pd.read_excel( path_log + file_log_table ) 
            ### 对index临时赋值
            df_log_table.index = df_log_table["fund"]
            ### 检查是否要加入新的代码
            fund_list_new = [ i for i in df_log_table.index if i not in fund_list_open ]
            for temp_fund in fund_list_new :
                df_log_table.loc[temp_fund, "frequency"] = "quarter"
                df_log_table.loc[temp_fund, "fund"]= temp_fund
                df_log_table.loc[temp_fund, "date_init"]= 20030101
                df_log_table.loc[temp_fund, "date_last"]= 20030101

        else :
            ### 首次为表格 新建df_log_table
            df_log_table = pd.DataFrame( index=fund_list_open,columns=["fund","frequency","date_init","date_last"] )
            df_log_table["frequency"] = "quarter"
            df_log_table["date_init"] = 20030101
            df_log_table["date_last"] = 20030101
            df_log_table["fund"] = fund_list_open
            df_log_table.to_excel( path_log + file_log_table,index=False ) 

        ################################################################################################################
        ### 1，交易日类
        ##########################################################
        ### 2.2.6 基金净值指标	5	0	day	1	1	fund_net_value(en_prod_code = "500039.SH")	
        # "en_prod_code，证劵代码	默认参数为：112002.OF；trading_date，交易日期	默认参数为：2020-12-31"	分开放式和封闭式基金净值
        if table_name == "fund_net_value" :            
            index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
            date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
            temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
            # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
            time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
            print("time_begin ",time_begin ,"date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
            ################################################
            ### 每30天更新一次基础信息
            if time_diff0 >= dt.timedelta(days= 5 ) :
                from hs_udata import fund_net_value
                df_log.loc[index_date,"date_last"] = time_begin                       
                #######################################
                ### 
                from hs_udata import fund_profile
                count_fund = 0
                ### notes:df_log_table.index 是梳理过的按照绩优基金优先的顺序排列
                num_all = len( df_log_table.index )
                for temp_fund in df_log_table.index : 
                # for temp_fund in fund_list_open : 
                    #######################################
                    ### 日期分析1：对每个基金，寻找成立日期；获取上市日期，避免浪费时间取上市前的日期
                    ### 为了避免多次重复取成立日，用date_init 判断
                    date_init_hs = df_log_table.loc[temp_fund, "date_init"] #  str( date_init )
                    df_fund_profile = fund_profile(en_prod_code = temp_fund )
                    # date_init 格式=2013-04-24
                    date_init_str = df_fund_profile.loc[0, "establishment_date"]
                    date_init = int( date_init_str.replace("-","") ) 
                    if date_init_hs > date_init :
                        ### 对基金初始日期重新赋值给
                        df_log_table.loc[temp_fund, "date_init"] = date_init
                    
                    #######################################
                    ### 日期分析2：获取需要更新的区间
                    date_last = df_log_table.loc[temp_fund, "date_last"]
                    ### 如果基金成立日晚于 2003001，则修该需要提取的日期
                    if dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d") < dt.datetime.strptime( str(date_init) , "%Y%m%d") :
                        df_log_table.loc[temp_fund, "date_last"] = str( date_init )
                        date_last = df_log_table.loc[temp_fund, "date_last"]
                    
                    time_diff = time_begin - dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d") 
                    print("time_diff ,",time_diff ,time_begin ,dt.datetime.strptime( str( int(date_last) ) , "%Y%m%d")  )
                    
                    if time_diff >= dt.timedelta(days= 14 ) : 
                        ### 获取日期区间 
                        ### int 20030301 to str "2003-03-01" 
                        date_list = [i for i in date_list if i >20140101 ]
                        date_list_sub_str = [ str(i)[:4] + "-" + str(i)[4:6]+ "-"+str(i)[-2:] for i in date_list if (i<=int(time_begin_str)-1 and i> int(date_last ) )]
                        count_quarter =0
                        
                        ######################################
                        ### NOTES：为了避免频繁更新日期文件，数据只保存在单个代码的文件里。
                        for temp_date_sub in  date_list_sub_str : 
                            print("count_fund=",round(count_fund/num_all,4),"Update fund:", temp_fund ,temp_date_sub,";date_last:" ,date_last, ";lastest=",time_begin_str )
                            ### 只能一个一个交易日获取 
                            df_data = fund_net_value(en_prod_code = temp_fund,trading_date= temp_date_sub  ) 
                            if count_quarter == 0 :
                                df_table = df_data
                            else :
                                df_table = df_table.append(df_data,ignore_index=True )   
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
                            
                            ### 
                            count_quarter = count_quarter + 1 
                            time.sleep(0.4) 
                        
                        ######################################
                        ### save log file for every 50 funds 
                        df_log_table.to_excel( path_log + file_log_table,index=False ) 

                        ### 遇到周末的2天，可能 df_table是空的
                        if "df_table" in vars() :
                            ### 剔除股票s所有季度的重复数据
                            df_table = df_table.drop_duplicates()

                            ######################################
                            ### 从本地目录读取文件
                            file_table = temp_fund + ".xlsx"
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
                        ### 更新 log_table | 不管数据有没有更新，都当作已经更新了一次数据
                        df_log_table.loc[temp_fund, "date_last"] = time_begin_str
                        count_fund = count_fund +1 
                        # ######################################
                        # ### save log file for every 50 funds 
                        # if count_fund % 50 == 0 :
                        df_log_table.to_excel( path_log + file_log_table,index=False ) 
        
        
        ##########################################################
        ### 2.2.1 基金日行情历史数据	4	0	day	1	1	
        # fund_quote_daily_history(en_prod_code = "150022.SZ",trading_date="2015-12-31")	
        # "en_prod_code，证劵代码	默认参数为：150022.SZ；trading_date，交易日期	默认参数为：2021-05-12"	ETF等场内基金基金的历史日行情，及行情表现数据

        # 开始和结束时间-返回季度财务数据


        ################################################################################################################
        ### 0，基金基础信息
        ##########################################################
        ### 获取基金概况 成立日 || publish_date	最新基金资产净值披露日期，不能为空值
        # notes:publish_date 有问题，很多基金在 211210的最近发布日期210930，
        if table_name == "fund_profile" :       
            index_date =  df_log[df_log["table_name"]== table_name  ].index[0]
            date_last0 = df_log[df_log["table_name"]== table_name  ]["date_last"].values[0]
            temp_freq = df_log[df_log["table_name"]== table_name  ]["frequency_full_table"].values[0]            
            # time.strptime("30 Nov 00", "%d %b %y") || d3 = d1 + datetime.timedelta(days=10)
            time_diff0 = time_begin - dt.datetime.strptime( str( int(date_last0) ) , "%Y%m%d")  
            print("date_last0:" ,date_last0, "time_diff=",time_diff0.days  )
            ################################################
            ### 每30天更新一次基础信息
            if time_diff0 >= dt.timedelta(days= 30 ) :
                df_log.loc[index_date,"date_last"] = time_begin     
                from hs_udata import fund_profile
                #######################################
                ### 开放式基金,ETF基金 
                count_quarter = 0
                for temp_fund in fund_list_open :
                    ### 只有一行数据                
                    df_data = fund_profile(en_prod_code = temp_fund )
                    if count_quarter == 0 :
                        df_table = df_data
                    else :
                        df_table = df_table.append(df_data,ignore_index=True )
                    
                    count_quarter = count_quarter + 1 
                    time.sleep(0.4) 
                    print( df_data.loc[0, [ "prod_code","chi_name_abbr","establishment_date","publish_date" ] ] )
                ### 剔除股票s所有季度的重复数据
                df_table = df_table.drop_duplicates()
                
                ######################################
                ### 从本地目录读取文件 
                file_table = "fund_profile"+ "_open" + ".xlsx"
                if os.path.exists( path_table_sub +file_table ) :
                    df_data = pd.read_excel( path_table_sub + file_table )
                    ### 将数据去重复项后加入总序列。
                    df_data =df_data.append( df_table,ignore_index=True )
                    df_data =df_data.drop_duplicates()
                    ### 保存到xlsx 
                    df_data.to_excel( path_table_sub + file_table,index=False )
                    
                else :
                    df_table.to_excel( path_table_sub + file_table,index=False )
                
                #######################################
                ### ETF基金
                count_quarter = 0
                for temp_fund in fund_list_etf :
                    df_data = data = fund_profile(en_prod_code = temp_fund )
                    if count_quarter == 0 :
                        df_table = df_data
                    else :
                        df_table = df_table.append(df_data,ignore_index=True )
                    
                    count_quarter = count_quarter + 1 
                    time.sleep(0.4) 
                    print( df_data.loc[0, [ "prod_code","chi_name_abbr","establishment_date","publish_date" ] ] )

                ### 剔除股票s所有季度的重复数据 
                df_table  = df_table.drop_duplicates()
                ######################################
                ### 从本地目录读取文件 
                file_table = "fund_profile"+ "_open" + ".xlsx"
                file_table_date = "fund_profile"+ "_open_"+time_begin_str + ".xlsx"
                if os.path.exists( path_table_sub +file_table ) :
                    df_data = pd.read_excel( path_table_sub + file_table  )
                    ### 将数据去重复项后加入总序列。
                    df_data =df_data.append( df_table,ignore_index=True )
                    df_data =df_data.drop_duplicates()
                    ### 保存到xlsx 
                    df_data.to_excel( path_table_sub + file_table,index=False )
                    df_data.to_excel( path_table_sub + file_table_date,index=False )
                    
                else :
                    df_table.to_excel( path_table_sub + file_table,index=False )
                    df_table.to_excel( path_table_sub + file_table_date,index=False )
        ###################################### 

        ###############################################
        ### 保存 df_data
        obj_data["df_data"]  =df_data
        ### 保存log_table,df_log-所有表格
        df_log_table.to_excel( path_log + file_log_table,index=False ) 
        df_log.to_excel( path_hs +  "manage_hs.xlsx" ,index=False ) 

        return obj_data

