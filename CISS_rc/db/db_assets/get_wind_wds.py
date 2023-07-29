# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo: 

功能：IP：10.10.232.197；port: 1521；sid: wind；user/pwd: wind/wind
self:
connection = cx_Oracle.connect("wind", "wind", ""10.10.232.195:1521/wind")
from Yang： 
外部咨询数据库登录用户为wind,各咨询数据分放在不同用户下：
1。老版本的万得数据放在winddb用户下，访问方式如下：
    select * from winddb.tb_object_1090;--注老版本数据早已停止同步更新
2。新版本的万得数据放在windn用户下，访问方式如下：
    select * from windn.AShareEODPrices;
3。天象咨询数据放在txdb用户下，访问方式如下：
    select * from txdb.t_asset_sum;

derived from test_oracle_wind.py
数据来源： Wind-wds 万得落地数据库
last update  20200211 | since 190830
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
# 删除列值中包括特定字符的列，例如 col_str="Unnamed"
# data_io_1.del_columns(df,"Unnamed")

class wind_wds():
    # 类的初始化操作
    def __init__(self):
        
        #################################################################################
        ### Initialization 
        import cx_Oracle
        # Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
        # connection = cx_Oracle.connect("wind", "wind", "10.10.10.195:1521/wind")
        # notes:CS-IT在2019-10左右：调整ODA的IP，由10.10.10.193-197调整到10.10.232.193-197
        # 200122，IP地址调整为IP：10.10.232.197；port: 1521；sid: wind；user/pwd: wind/wind 
        connection = cx_Oracle.connect("wind", "wind", "10.10.232.197:1521/wind",encoding = "UTF-8", nencoding = "UTF-8") 
        self.cursor = connection.cursor()
        
        # self.path_out = os.getcwd()[:2] + "\\db_wind\\"
        if  os.path.exists( "D:\\db_wind\\" ) :
            self.path_out = "D:" + "\\db_wind\\"
        else :
            self.path_out = "F:" + "\\db_wind\\"
        # 
        self.folder_name = "data_wds"
        self.path_wds = self.path_out + "data_wds\\"
        self.path_adj = self.path_out + "data_adj\\"

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
        import numpy
        self.nan = np.nan 

    def print_info(self):
        ### print all modules for current clss
        print("data_fetch |get_table_columns: 只获取表格的列信息")
        print("data_fetch |get_table_full: 获取整张表格，无参数")
        print("data_fetch |get_table_primekey:根据主键primekey和日期范围等获取表格") 
        
        print("data_fetch |get_table_full_input:get_table_full带参数版本")
        print("data_fetch |get_table_primekey_input: 根据table_name,prime_key ,prime_key_value获取表格数据")
        print("data_fetch |get_table_opdate: 根据最近一次opdate时间获取opdate时间之后的增量数据，并可以按给定prime_key关键词更新数据文件， ")

        print("data_manage |manage_data_check_anndates:根据给定的核对表格(index:公布日期,columns:wds表格)   ")
        print("data_manage |manage_opdata_to_anndates:根据opdate区间数据作为增量，更新基于prime_key|发布日期的逐个数据表格  ")

        print("data_manage |   ")
        return 1 

    def get_table_columns(self ) :
        ### 只获取表格的列信息
        # notes:200122新增+ folder_name = "EQUIYU_raw",可能有些代码还没有更新
        table_name = input("table_name: e,g.中国A股日行情[AShareEODPrices]...")
        prime_key =  input("prime_key: e,g. S_INFO_WINDCODE or TRADE_DT...")    
        prime_key_value =  input("prime_key: e,g. 600036.SH or 20190829...")    
        datetime_range = "ALL" # input("datetime_range: e.g. ALL or [201907 to 201908]...")
        
        # input("last_update:e.g. 20180930 ...")
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 

        ### Input parameters for database 
        # prime_key used to be date_name
        # prime_key used to be params = {'TEMP_DATE':20190807 }
        params = {'TEMP_DATE': prime_key_value }

        print("Working on table name ", table_name) 
        temp_sql = "SELECT * FROM windn." + table_name +" WHERE "+ prime_key +  "=:temp_date "
        temp_table1 = self.cursor.execute(temp_sql,params )

        temp_data = temp_table1.fetchall()
        ### 获取列信息 || https://blog.csdn.net/qq_41797451/article/details/80230399
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)

        temp_df= pd.DataFrame( temp_data,columns= temp_cols )
        print("Length of data ",len(temp_data) )
        print( temp_df.tail(1) ) 
        
        ### define path out  
        if not os.path.isdir( self.path_out + self.folder_name+ '\\'+ table_name ) :
            os.mkdir( self.path_out + self.folder_name + '\\'+ table_name )
        
        file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range +  ".csv"
        # 删除列值中包括特定字符的列，例如 col_str="Unnamed"
        temp_df = data_io_1.del_columns( temp_df,"Unnamed")
        temp_df.to_csv(self.path_out + self.folder_name+ '\\'+ table_name+ '\\' +file_name,encoding="gbk"    )

        pd.DataFrame(temp_cols).to_csv(self.path_out + self.folder_name+ '\\'+ table_name+ '\\' +"columns.csv"  )
        
        return temp_cols

    def get_table_full(self,table_name,if_CN=0 ) :
        ### 获取整张表格，无参数
        ### last 200113 | sicne 190830
        ### derived from test_oracle_wind.py 

        if table_name =="" :
            table_name = input("table_name: e,g.中国A股日行情[AShareEODPrices]...")

        prime_key =  "full_table"
        prime_key_value =  "full_table"
        datetime_range = "ALL" # input("datetime_range: e.g. ALL or [201907 to 201908]...")
        # input("last_update:e.g. 20180930 ...")
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 
     
        print("Working on table name ", table_name)
        temp_sql = "SELECT * FROM windn." + table_name
        temp_table = self.cursor.execute(temp_sql   )
        temp_data = temp_table.fetchall()
        ### 获取列信息 || https://blog.csdn.net/qq_41797451/article/details/80230399
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)

        df_data = pd.DataFrame( temp_data,columns= temp_cols )
        
        print("Length of data ",len(temp_data) )
        print( df_data.tail(2) ) 

        file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range  + ".csv"
        
        if not os.path.isdir( self.path_out+ self.folder_name+ '\\' + table_name ) :
            os.mkdir( self.path_out + self.folder_name+ '\\'+ table_name )
        
        # 删除列值中包括特定字符的列，例如 col_str="Unnamed"
        df_data = data_io_1.del_columns( df_data,"Unnamed")

        if if_CN == 1: 
            ### 导出时要用 gbk格式
            df_data.to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name   )
            pd.DataFrame(df_data.columns).to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +"columns.csv" )
            # df_data.to_csv(self.path_out+ table_name+ '\\' +file_name ,encoding="gbk" )
        else :
            df_data.to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name   )

        ### Find table name in list and Save to log of table 
        # log_table_name = "wind_wds_tables.xlsx"
        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_excel(self.path_log_table+log_table_name, sheet_name="wind_wds_tables" )

        ##############################################################################
        ### 判断是否要更新到log文件

        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_csv(self.path_log_table+self.log_table_name )
        # ## keep only specific columns
        # df_log_table = df_log_table[ self.col_list ]

        # ### 对于多个股票，需要对一个表格多次取值
        # if len( df_log_table.index ) > 0 :
        #     temp_df = df_log_table[df_log_table["table_name"] == table_name  ]

        # if len(temp_df.index) < 1 :
        #     # add to new row 
        #     temp_i = df_log_table.index[-1]+1
            
        # elif len(temp_df.index) == 1 :
        #     temp_i = temp_df.index[0]
        #     print("Current item", temp_df )
        # else:
        #     print("More than 1 row and should delete redundant rows...")
        #     print( temp_df )
        #     temp_i = temp_df.index[0]

        # df_log_table.loc[ temp_i,"table_name" ] = table_name
        # df_log_table.loc[ temp_i,"prime_key" ] = prime_key
        # df_log_table.loc[ temp_i,"prime_key_value" ] = prime_key_value
        # df_log_table.loc[ temp_i,"datetime_range" ] = datetime_range
        # df_log_table.loc[ temp_i,"last_updatee" ] = last_update
        # print(df_log_table.tail(3))
        # # header=0) #不保存列名
        # df_log_table.to_csv(self.path_log_table+self.log_table_name,encoding="gbk")
        # print( self.path_log_table+ self.log_table_name )
        # return df_data,df_log_table

        result = 1 
        return result

    def get_table_primekey(self ) :
        ### 根据主键primekey和日期范围等获取表格
        ### last | sicne 190830
        ### derived from test_oracle_wind.py

        # table_name = "AShareEODPrices"
        # prime_key = "S_INFO_WINDCODE"
        # prime_key_value = "600036.SH"
        # datetime_range = "ALL"

        table_name = input("table_name: e,g.中国A股日行情[AShareEODPrices]...")
        prime_key =  input("prime_key: e,g. S_INFO_WINDCODE or TRADE_DT...")    
        prime_key_value =  input("prime_key: e,g. 600036.SH or 20190829...")    
        datetime_range = "ALL" # input("datetime_range: e.g. ALL or [201907 to 201908]...")
        # input("last_update:e.g. 20180930 ...")
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 

        ### Input parameters for database 
        # prime_key used to be date_name
        # prime_key used to be params = {'TEMP_DATE':20190807 }
        params = {'TEMP_DATE': prime_key_value }

        print("Working on table name ", table_name)
        
        # 中国A股日行情 || AShareEODPrices || 交易日期 TRADE_DT,20190805
        # 香港股票日行情|| HKshareEODPrices || TRADE_DT
        # table_name = "AshareMSCIMembers"
        # date_name = "ENTRY_DT" 
        temp_sql = "SELECT * FROM windn." + table_name +" WHERE "+ prime_key +  "=:temp_date "
        temp_table1 = self.cursor.execute(temp_sql,params )

        temp_data = temp_table1.fetchall()
        ### 获取列信息 || https://blog.csdn.net/qq_41797451/article/details/80230399
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)

        temp_df= pd.DataFrame( temp_data,columns= temp_cols )

        if not os.path.isdir( self.path_out+ self.folder_name+ '\\' + table_name ) :
            os.mkdir( self.path_out+ self.folder_name+ '\\' + table_name )

        file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range +  ".csv"
        print( self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name )
        # 删除列值中包括特定字符的列，例如 col_str="Unnamed"
        temp_df = data_io_1.del_columns( temp_df,"Unnamed")

        temp_df.to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name,encoding="gbk"    )
        
        pd.DataFrame(temp_df.columns).to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +"columns.csv",encoding="gbk"  )

        ### Find table name in list and Save to log of table 
        # log_table_name = "wind_wds_tables.xlsx"
        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_excel(self.path_log_table+log_table_name, sheet_name="wind_wds_tables" )

        
        # 不指定sheet时，默认读取第一个sheet
        df_log_table = pd.read_csv(self.path_log_table+self.log_table_name )
        ## keep only specific columns
        df_log_table = df_log_table[ self.col_list ]

        ### 对于多个股票，需要对一个表格多次取值
        if len( df_log_table.index ) > 0 :
            temp_df = df_log_table[df_log_table["table_name"] == table_name  ]
            if len( temp_df.index ) > 0 :
                temp_df = temp_df[temp_df["prime_key"] == prime_key  ]
                temp_df = temp_df[temp_df["prime_key_value"] == prime_key_value  ]

        if len(temp_df.index) < 1 :
            # add to new row 
            temp_i = df_log_table.index[-1]+1
            
        elif len(temp_df.index) == 1 :
            temp_i = temp_df.index[0]
            print("Current item", temp_df )
        else:
            print("More than 1 row and should delete redundant rows...")
            print( temp_df )
            temp_i = temp_df.index[0]

        df_log_table.loc[ temp_i,"table_name" ] = table_name
        df_log_table.loc[ temp_i,"prime_key" ] = prime_key
        df_log_table.loc[ temp_i,"prime_key_value" ] = prime_key_value
        df_log_table.loc[ temp_i,"datetime_range" ] = datetime_range
        df_log_table.loc[ temp_i,"last_updatee" ] = last_update
        print(df_log_table.tail(3))
        # header=0) #不保存列名
        
        df_log_table.to_csv(self.path_log_table+self.log_table_name,encoding="gbk"  )
        print( self.path_log_table+ self.log_table_name )

        return temp_df,df_log_table


    def get_table_full_input(self,table_name) :
        ### 获取整张表格，无参数
        ### last 200316 | sicne 190830
        ### derived from test_oracle_wind.py 
        
        prime_key =  "full_table"
        prime_key_value =  "full_table"
        datetime_range = "ALL" # input("datetime_range: e.g. ALL or [201907 to 201908]...")
        # input("last_update:e.g. 20180930 ...")
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 
     
        print("Working on table name ", table_name)
        temp_sql = "SELECT * FROM windn." + table_name
        temp_table = self.cursor.execute(temp_sql   )
        temp_data = temp_table.fetchall()
        
        ### 获取列信息 || https://blog.csdn.net/qq_41797451/article/details/80230399
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)

        temp_df= pd.DataFrame( temp_data,columns= temp_cols ) 
        
        file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range  + ".csv"
        
        if not os.path.isdir( self.path_out + self.folder_name+ '\\'+ table_name ) :
            os.mkdir( self.path_out+ self.folder_name+ '\\' + table_name )

        # 删除列值中包括特定字符的列，例如 col_str="Unnamed"
        temp_df = data_io_1.del_columns( temp_df,"Unnamed")

        temp_df.to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name,encoding="gbk"    )

        ### Find table name in list and Save to log of table 
        # log_table_name = "wind_wds_tables.xlsx"
        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_excel(self.path_log_table+log_table_name, sheet_name="wind_wds_tables" )

        
        # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_csv(self.path_log_table+self.log_table_name )
        # ## keep only specific columns
        # df_log_table = df_log_table[ self.col_list ]

        # ### 对于多个股票，需要对一个表格多次取值
        # if len( df_log_table.index ) > 0 :
        #     temp_df = df_log_table[df_log_table["table_name"] == table_name  ]

        # if len(temp_df.index) < 1 :
        #     # add to new row 
        #     temp_i = df_log_table.index[-1]+1
            
        # elif len(temp_df.index) == 1 :
        #     temp_i = temp_df.index[0]
        #     print("Current item", temp_df )
        # else:
        #     print("More than 1 row and should delete redundant rows...")
        #     print( temp_df )
        #     temp_i = temp_df.index[0]

        # df_log_table.loc[ temp_i,"table_name" ] = table_name
        # df_log_table.loc[ temp_i,"prime_key" ] = prime_key
        # df_log_table.loc[ temp_i,"prime_key_value" ] = prime_key_value
        # df_log_table.loc[ temp_i,"datetime_range" ] = datetime_range
        # df_log_table.loc[ temp_i,"last_updatee" ] = last_update
        # print(df_log_table.tail(3))
        # # header=0) #不保存列名
        # df_log_table.to_csv(self.path_log_table+self.log_table_name ,encoding="gbk"  )
        # print( self.path_log_table+ self.log_table_name )

        ### output object 
        data_obj = {}
        data_obj["wds_df"] = temp_df
        data_obj["file_path"] =  self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' 
        data_obj["file_name"] = file_name

        return data_obj


    def get_table_primekey_input(self,table_name,prime_key ,prime_key_value ) :
        ### 根据table_name,prime_key ,prime_key_value获取表格数据
        ### last 200316| sicne 190830
        ### derived from def get_table_primekey(self ) ;test_oracle_wind.py
        # folder name ="data_wds"
        # table_name = "AShareEODPrices"
        # prime_key = "S_INFO_WINDCODE"
        # prime_key_value = "600036.SH"
        # datetime_range = "ALL"
        # input("last_update:e.g. 20180930 ...")
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d")         

        print("Working on table name ", table_name) 
        
        ### Input parameters for database  
        # prime_key used to be params = {'TEMP_DATE':20190807 }
        params = {'TEMP_DATE': prime_key_value }
         
        temp_sql = "SELECT * FROM windn." + table_name +" WHERE "+ prime_key +  "=:temp_date "
        temp_table1 = self.cursor.execute(temp_sql,params )

        temp_data = temp_table1.fetchall()
        ### 获取列信息 || https://blog.csdn.net/qq_41797451/article/details/80230399
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)

        temp_df= pd.DataFrame( temp_data,columns= temp_cols )
 
        if not os.path.isdir( self.path_out+ self.folder_name+ '\\' + table_name ) :
            os.mkdir( self.path_out+ self.folder_name+ '\\' + table_name )

        datetime_range = "ALL"
        file_name = "WDS_"+ prime_key +"_"+ prime_key_value + "_"+datetime_range +   ".csv"
        print( self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name )

        # 删除列值中包括特定字符的列，例如 col_str="Unnamed"
        temp_df = data_io_1.del_columns( temp_df,"Unnamed")

        try :
            temp_df.to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name,encoding="gbk"    )
        except :
            temp_df.to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name    )
        try :
            pd.DataFrame(temp_df.columns).to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +"columns.csv"    )
        except:
            pd.DataFrame(temp_df.columns).to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +"columns.csv",encoding="gbk"    )
        ### Find table name in list and Save to log of table 
        # log_table_name = "wind_wds_tables.xlsx"
        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_excel(self.path_log_table+log_table_name, sheet_name="wind_wds_tables" )
        
        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_csv(self.path_log_table+self.log_table_name )
        # ## keep only specific columns
        # df_log_table = df_log_table[ self.col_list ]

        # ### 对于多个股票，需要对一个表格多次取值
        # if len( df_log_table.index ) > 0 :
        #     temp_df = df_log_table[df_log_table["table_name"] == table_name  ]
        #     if len( temp_df.index ) > 0 :
        #         temp_df = temp_df[temp_df["prime_key"] == prime_key  ]
        #         temp_df = temp_df[temp_df["prime_key_value"] == prime_key_value  ]

        # if len(temp_df.index) < 1 :
        #     # add to new row 
        #     temp_i = df_log_table.index[-1]+1
            
        # elif len(temp_df.index) == 1 :
        #     temp_i = temp_df.index[0]
        #     print("Current item", temp_df )
        # else:
        #     print("More than 1 row and should delete redundant rows...")
        #     print( temp_df )
        #     temp_i = temp_df.index[0]

        # df_log_table.loc[ temp_i,"table_name" ] = table_name
        # df_log_table.loc[ temp_i,"prime_key" ] = prime_key
        # df_log_table.loc[ temp_i,"prime_key_value" ] = prime_key_value
        # df_log_table.loc[ temp_i,"datetime_range" ] = ""
        # df_log_table.loc[ temp_i,"last_updatee" ] = last_update
        # print(df_log_table.tail(3))
        # # header=0) #不保存列名
        # df_log_table.to_csv(self.path_log_table+self.log_table_name,encoding="gbk")
        # print( self.path_log_table+ self.log_table_name )

        data_obj = {}
        data_obj["wds_df"] = temp_df
        data_obj["file_path"] = self.path_out+ self.folder_name+ '\\'+ table_name+ '\\'
        data_obj["file_name"] = file_name 

        return data_obj

    def get_table_opdate(self,obj_in ):
        ### 根据更新日期区间OPDATE下载数据，并按给定prime_key关键词更新数据文件，比较获取数据；wds里绝大部分row数据对应的"OPDATE"都是2017年以后，这说明wind不定期会对历史数据进行调整。
        '''
        last 20201026 | since 20200410 
        obj_in={}
        obj_in["dict"]["table_name"]
        obj_in["dict"]["datetime_key"]:基本上就是"OPDATE"；
        obj_in["dict"]["datetime_value_lb"]："OPDATE"对应的日期区间左侧；
        obj_in["dict"]["datetime_value_ub"]："OPDATE"对应的日期区间右侧；
        
        策略是用opdate只下载近期（比如3~10天）有更新的数据
        1，日期转字符串，需要先了解OPDATE的格式，例如to_char():将日期转按一定格式换成字符类型 ， SQL> select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') time from dual;
        2，字符串转日期，SELECT TO_DATE('2006-05-01 19:25:34', 'YYYY-MM-DD HH24:MI:SS') FROM DUAL
        我的版本：temp_date="TO_DATE('2020-04-08 16:00:00', 'YYYY-MM-DD HH24:MI:SS')" ; temp_sql = "SELECT * FROM windn." + table_name +" WHERE "+ prime_key + ">= "+temp_date
        或者 temp_date="TO_DATE('20200409', 'YYYYMMDD')"；
        url = https://www.cnblogs.com/macT/p/10214944.html ;https://www.cnblogs.com/kuangwong/p/6192480.html
        
        Ana:
        1,OPDATE 不一定对应公告日，有可能是对历史部分日期的更新
        
        notes:有可能出现 2018年对2014年的某条数据进行更新的情况
        '''
        ###########################################################################
        ### Initialization
        

        table_name = obj_in["dict"]["table_name"]
        datetime_key = obj_in["dict"]["datetime_key"] 
        datetime_value_lb = obj_in["dict"]["datetime_value_lb"]
        datetime_value_ub = obj_in["dict"]["datetime_value_ub"]
        # key_column = obj_in["dict"]["key_column"]        

        # FOR OPDATE || datetime_value or temp_date = 20200409
        if datetime_key == "OPDATE" :
            date_lb_4sql ="TO_DATE('"+ datetime_value_lb + "', 'YYYYMMDD')"
            date_ub_4sql ="TO_DATE('"+ datetime_value_ub + "', 'YYYYMMDD')"
        # FOR TRADE_DT
        if datetime_key in ["TRADE_DT","TRADE_DATE"]  :
            date_lb_4sql = datetime_value_lb
            date_ub_4sql =  datetime_value_ub
        # FOR 预测日期EST_DT
        if datetime_key == "EST_DT" :
            date_lb_4sql = datetime_value_lb
            date_ub_4sql =  datetime_value_ub
        
        # FOR 预测日期EST_DT
        if datetime_key == "ANN_DT" :
            date_lb_4sql = datetime_value_lb
            date_ub_4sql =  datetime_value_ub

        temp_sql = "SELECT * FROM windn." + table_name +" WHERE "+ datetime_key + ">= "+date_lb_4sql +" AND " + datetime_key + "< "+date_ub_4sql
        
        temp_table1 = self.cursor.execute(temp_sql )

        temp_data = temp_table1.fetchall()
        ### 获取列信息 || https://blog.csdn.net/qq_41797451/article/details/80230399
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)

        temp_df= pd.DataFrame( temp_data,columns= temp_cols )

        ### TODO 将其保存至对应的文件,没想好怎么保存到相应的文件
        # 1,对于全量保存的表，可以用这个将其添加进表格。比如  test_wds_manage_1shot_auto.py 

        ### Save to csv file 
        if not os.path.isdir( self.path_out+ self.folder_name+ '\\' + table_name ) :
            os.mkdir( self.path_out+ self.folder_name+ '\\' + table_name )

        # datetime_range = "ALL"
        file_name = "WDS_"+ datetime_key +"_"+ datetime_value_lb + "_"+datetime_value_ub + ".csv"
        
        # 删除列值中包括特定字符的列，例如 col_str="Unnamed"
        temp_df = data_io_1.del_columns( temp_df,"Unnamed")

        if obj_in["dict"]["df2csv"] in [1,"1"] :
            print( self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name )
            try :
                temp_df.to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name,encoding="gbk"    )
            except :
                temp_df.to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +file_name    )
            try :
                pd.DataFrame(temp_df.columns).to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +"columns.csv"    )
            except:
                pd.DataFrame(temp_df.columns).to_csv(self.path_out+ self.folder_name+ '\\'+ table_name+ '\\' +"columns.csv",encoding="gbk"    )

        ### 保存下载的数据df=obj_in["wds_df"]
        obj_in["wds_df"] = temp_df
        obj_in["dict"]["file_path"] = self.path_out+ self.folder_name+ '\\'+ table_name+ '\\'
        obj_in["dict"]["file_name"] = file_name 
        
        if "if_opdate_2_prime_key" in obj_in["dict"].keys() and obj_in["dict"]["if_opdate_2_prime_key"] == "1" :
            ### 获取每个表格对应的prime_key 关键词
            file_tables ="log_data_wds_tables.csv"
            df_table_names = pd.read_csv( self.path_rc_data + file_tables,encoding="gbk"  )

            df_table = df_table_names[ df_table_names["name_table"] == table_name ]
            obj_in["dict"]["prime_key"]  = df_table["keyword_anndate"].values[0] 
            obj_in["dict"]["table_name"]  = table_name
            obj_in = self.manage_opdata_to_anndates(obj_in)

        return obj_in

    ##################################################
    ### data_manage |
    
    def manage_data_check_anndates(self,file_data_check_anndates):
        ### 每个交易日根据给定的核对表格(index:公布日期,columns:wds表格)  
        '''
        last 201026 | since 191128
        input:  
        file_data_check_anndates:"data_check_anndates.csv" or "data_check_anndates_specific.csv"
            path :"..\\ciss_web\\CISS_rc\\apps\\rc_data\\"
        output: 
        步骤：1，下载 GlobalWorkingDay 整个表格，交易时间 || notes：三个月更新一次就行了
        2，导入 df_data_check_anndates，若不存在则创建。
        3，根据opdate区间数据更新基于anndates的表格 | since 200514
        4，用df_dates内更新的日期 更新表格列信息： data_check_anndates.csv  
        5，按照 df_data_check_anndates内的更新记录下载表格数据

        notes:1，20200514新增用OPDATE方式更新近5日数据
        2，20201026，OPDATE更新近15日数据：新股上市等事件发生时会更新历史数据，例如688128.SH的半年报20180630发布时间ANN_DT=20191017，这需要在最近15个交易日（国庆放假7~9天加5天出差的情况）OPDATE
        '''
        ### Initialization
        import datetime as dt 

        ### Import :读取要跟踪的wds表格名称信息
        file_tables ="log_data_wds_tables.csv"
        df_table_names = pd.read_csv( self.path_rc_data + file_tables,encoding="gbk"  )
        print("df_table_names \n" ,df_table_names.head(5) )

        #################################################################################
        ### 1，下载 GlobalWorkingDay 整个表格，交易时间 || notes：三个月更新一次就行了，下一次20200331
        '''
        # path = "G:\db_wind\GlobalWorkingDay\\"
        # file = "WDS_full_table_full_table_ALL_20191125.csv"
        # 20191125抓取数据，获得的日期范围是
        # USA	20180102	20191231 | HKG	20180102	20191231
        # CHN	20180102	20201231
        '''
        ### 判断是否重新下载全球交易日数据，有时候各地交易所会临时调整交易日，例如20200131改为非交易日
        # notes:建议1个季度下载一次
        print("是否更新GlobalWorkingDay表格")
        # if_update_tradingday = input("type 1 means update trading day for CHN market|suggest every quarter end")
        if_update_tradingday = 1
        
        if if_update_tradingday == "1" :
            ######################################################################
            ### 下载全球交易日表格
            table_name = "GlobalWorkingDay"
            (df_date_2years ,df_log_table) =self.get_table_full(table_name)

            ### 导入本地 GlobalWorkingDay 表格
            path_workingday = self.path_rc_data + "GlobalWorkingDay"+"\\"
            # file_workingday = "WDS_full_table_full_table_ALL.csv" 
            file_workingday = "WDS_full_table_full_table_ALL.csv"
            # header=0，表示把第一行作为列名
            df_date_2years = pd.read_csv(path_workingday+file_workingday,encoding="gbk")
            df_date_2years = df_date_2years.drop("Unnamed: 0",axis=1)

            ### 替换columns
            df_date_2years.columns=["id","date","market","update_date","notes"]
            # df_date_2years["date"] # int64 type
            df_date_2years= df_date_2years[df_date_2years["market"]=="CHN" ]
            df_date_2years = df_date_2years.sort_values(by="date" )

            ######################################################################
            ### 用新获取的表格更新本地A股的交易日列表
            # 这个文件里的日期都是正常的格式 "rc_WDS_indexdates_20050101_anndate.csv"
            # 交易日：   rc_WDS_indexdates_200501_20190831_ANN-DT.csv | col="2"
            # month:    rc_WDS_indexdates_200501_20190831_month.csv
            # quarter : rc_WDS_indexdates_200501_20190831_quarter.csv
            file_dates = "rc_WDS_indexdates_20050101_anndate.csv"
            df_dates = pd.read_csv( self.path_out + file_dates ,header=None  )
            # columns 只有"2",改为date 
            temp_col = df_dates.columns[0]
            df_dates = df_dates.sort_values(by= temp_col )
            date_last = df_dates[temp_col].values[-1]
            print(" date_last \n", int(date_last), type(df_date_2years["date"].values[-1] )  )

            df_date_2years = df_date_2years[ df_date_2years["date"]> int(date_last)  ]
            df_dates = df_dates[temp_col].append( df_date_2years["date"] ,ignore_index=True)
            # df_dates 是没有column的 pd series
            # print("df_dates \n" ,df_dates.head()  )
            print("df_dates \n" ,df_dates.tail()  )
            #Values: 5689    20201230;5690    20201231
            ### Save to csv file without index 
            ###  index=False 意味着没有columns，再次导入时是 0 
            # 删除列值中包括特定字符的列，例如 col_str="Unnamed"
            df_dates = data_io_1.del_columns( df_dates,"Unnamed")

            df_dates.to_csv(self.path_out+ file_dates, index=False  ) 
        else :
            ### just import local date list  
            file_dates = "rc_WDS_indexdates_20050101_anndate.csv"
            df_dates = pd.read_csv( self.path_out + file_dates ,header=None  )

        print("Hist and future trading days saved to :")
        print( self.path_out + file_dates )

        #################################################################################
        ### 2，导入 df_data_check_anndates，若不存在则创建。
        import os
        if os.path.exists( self.path_rc_data + file_data_check_anndates ) :
            df_data_check_anndates = pd.read_csv(self.path_rc_data + file_data_check_anndates ,encoding="gbk",index_col="date" )
            # check if it contains all column in df_table_names.name_table
            for temp_col in df_table_names.name_table :
                if not temp_col in df_data_check_anndates.columns :
                    ### notes:all values in int type, 判断按最新更新日期算数据是否完整：1：已有；0：未下载；2：当日无数据；
                    df_data_check_anndates[temp_col] = 0 

        else :
            ### 读取要跟踪的表格、日期，构建csv文件
            df_data_check_anndates = pd.DataFrame(columns=df_table_names.name_table,index= df_dates[temp_col]  )
            print( df_data_check_anndates.head(3) )
            print( df_data_check_anndates.tail(3) )
            
            df_data_check_anndates.to_csv( self.path_rc_data + file_data_check_anndates ,encoding="gbk"    )

        ########################################################################
        ### 3，根据opdate区间数据更新基于anndates的表格 | since 200514
        
        date_now = dt.datetime.strftime( dt.datetime.now(), "%Y%m%d")
        print("date_now ",date_now )
        ### 每个temp_col就是1个table_name
        for temp_col in df_data_check_anndates.columns :
            
            ### 下载opdate区间数据
            temp_date = date_now
            table_name = temp_col
            print("OPDATE method working on table ",table_name ,date_now )
            obj_in = {}
            obj_in["dict"] = {}
            obj_in["dict"]["table_name"] = table_name  
            obj_in["dict"]["datetime_key"] ="OPDATE"

            ### 确定更新日期：往前推5~10天 || datetime.timedelta(days=1));datetime.timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0)
            period_dt_update = 15
            # period_dt_update = int( input(temp_col + " Check num days to download for table ") )
            # "%Y%m%d %H:%M:%S"
            # date_time_update = "20200509" 
            # temp_dt = dt.datetime.strptime( date_time_update, "%Y%m%d") - dt.timedelta(days= period_dt_update )
            date_time_update = dt.datetime.now() - dt.timedelta(days= period_dt_update )

            dt_update_file =dt.datetime.strptime( str(temp_date), "%Y%m%d")
            datetime_value =  min( date_time_update,dt_update_file)
            datetime_value = dt.datetime.strftime( datetime_value, "%Y%m%d")
            obj_in["dict"]["datetime_value_lb"] = datetime_value
            obj_in["dict"]["datetime_value_ub"] = "20991231"
            ### ["if_opdate_2_prime_key"] = "1"意味着按日期保存至对应的文件
            obj_in["dict"]["if_opdate_2_prime_key"] = "1"
            # opdate文件不保存
            obj_in["dict"]["df2csv"] = 0 
            
            ### 下载的区间数据表,逐个保存成具体的交易日文件
            try :
                obj_in = self.get_table_opdate( obj_in )
                ##################################################################################
                ### 把用opdate下载的区间数据按照key_word交易日，逐个保存成具体的交易日文件
                ### obj_in["wds_df"]
                temp_table = df_table_names[ df_table_names["name_table"] == table_name ]
                prime_key  = temp_table["keyword_anndate"].values[0] 
                obj_in["dict"]["prime_key"] = prime_key
                obj_in["dict"]["table_name"] = table_name
                obj_in = self.manage_opdata_to_anndates(obj_in)
            except :
                pass 
            

        print("We have updated all dates for tables. by opdate ") 

        #################################################################################
        ### 4，用df_dates内更新的日期 更新表格列信息： data_check_anndates.csv  
        date_now = dt.datetime.strftime( dt.datetime.now(), "%Y%m%d")
        print("date_now ",date_now )
        # input1= input("Check last date to update for df_check")

        ### df_dates to list_dates
        list_dates = list( df_dates[0] )
        date_0 =  df_data_check_anndates.index[-1] 
         
        if date_0 > 0 and date_0 < int(date_now) :
            print( "data_0,date_now",date_0,date_now ) 
            temp_list =  [temp_date for temp_date in list_dates if temp_date > int(date_0) ]
            temp_list =  [temp_date for temp_date in temp_list if temp_date <= int(date_now) ]
            print( temp_list[:3] ,temp_list[-3:]  )
            
            # print( temp_list ) | 20190903 to 20191125 
            # append temp_list to df_data_check_anndates
            # check if it contains all date(index) in df_table_names.name_table
            for temp_date in temp_list :
                if not temp_date in df_data_check_anndates.index :
                    ### notes:all values in int type, 判断按最新
                    # 更新日期算数据是否完整：1：已有；0：未下载；2：当日无数据；
                    df_data_check_anndates.loc[temp_date,:] = 0 
            
            print( df_data_check_anndates.tail(3) )
            ### Save table df_data_check_anndates into csv
            df_data_check_anndates.to_csv(self.path_rc_data + file_data_check_anndates ,encoding="gbk"   )
        else :
            print("Alreaedy latest dates ")
        
        
        #################################################################################
        ### 5，按照 df_data_check_anndates内的更新记录下载表格数据
        ### cell values:1：已有；0：未下载；2：当日无数据
        for temp_col in df_data_check_anndates.columns :
            # temp_col correspondes to table name 
            ### 获取列中数值不等于1或2的部分
            # pandas 通过~取反，选取不包含数字1的行 | https://blog.csdn.net/luocheng7430/article/details/80330566
            # 如果某列都是数值【1,2】，则以下这行会把所有列都删了
            # temp_df = df_data_check_anndates[~ df_data_check_anndates[temp_col].isin([1,2])  ]
            # new temp_df is a series
            temp_df = df_data_check_anndates[temp_col]	
            temp_df = temp_df[~ temp_df.isin([1,2])  ] 

            for temp_index in temp_df.index :  
                ### temp_date should be like 20190103,which is in index but not value!!
                temp_date =  temp_index 
                # temp_col correspondes to date
                print("Working on table "+ temp_col +",date "+ str(temp_date) )
            
                table_name = temp_col
                ###需要去log_data_wds_tables.csv列表里找到表格的关键词
                
                temp_table = df_table_names[ df_table_names["name_table"] == table_name ]
            
                # if len( temp_table.index) >= 1:
                try :
                    # prime_key  = "TRADE_DT"
                    prime_key  = temp_table["keyword_anndate"].values[0] 
                    print("prime_key is ",prime_key )
                    ### 这里主要是公募基金描述没有关键字~~~ 
                    # 其实可以用这个： 公告日期 F_INFO_ANNDATE  VARCHAR2(8)
                    # http://wds.wind.com.cn/rdf/?#/main 

                    ### type(prime_key) = float 时，prime_key 会等于 nan
                    if prime_key == np.nan :
                        print("Debug===================")
                        print( temp_table )
                        print("temp_table \n", temp_table )

                except :
                    print("Error: multiple matched tables ","table_name ",table_name) 
                    print("Debug==",temp_table )
                    print("注意：data_check_anndates.csv里所有column表格需要在log_data_wds_tables.csv有对应的关键词keyword ")
                    asd
                ### 
                prime_key_value = str(temp_date)
                datetime_range = "ALL"

                data_obj = self.get_table_primekey_input( table_name,prime_key ,prime_key_value ) 
                
                ### 更新数值
                if len( temp_table.index ) >= 1 :
                    ### 表示我们有正常返回的数据
                    df_data_check_anndates.loc[temp_index, temp_col] = 1 
                else :
                    df_data_check_anndates.loc[temp_index, temp_col] = 2

                ### 对于每个column更新的每个日期，都要及时存csv
                df_data_check_anndates.to_csv(self.path_rc_data + file_data_check_anndates ,encoding="gbk"   )
        
        print("We have updated all dates for tables. by trading date ") 
        

        return df_data_check_anndates

    def manage_opdata_to_anndates(self,obj_in):
        ### 根据opdate区间数据作为增量，更新基于prime_key|发布日期的逐个数据表格
        '''
        obj_in["dict"]["key_column']：用于分日期输出保存单个文件用。
        obj_in["df_opdate"]  是从wds根据opdate获取的dataFrame
        notes:由于wds里绝大部分row数据对应的"OPDATE"都是2017年以后，这说明wind不定期会对历史数据进行调整。
        '''
        ############################################################
        ### Choice 1:导入已下载好csv的opdate数据文件： 读取 opdate数据
        # file_name = obj_in["dict"]["file_name_opdate"]
        # try :
        #     obj_in["df_opdate"] = pd.read_csv( self.path_wds +obj_in["dict"]["table_name"]+"\\"+ file_name )
        # except :
        #     obj_in["df_opdate"] = pd.read_csv( self.path_wds +obj_in["dict"]["table_name"]+"\\"+ file_name,encoding="gbk" )
        # # print( obj_in["df_opdate"].head().T  )
        # key_column =obj_in["dict"]["key_column"]
        # table_name = obj_in["dict"]["table_name"] 
        # df0 = obj_in["df_opdate"] 
        
        ###########################################################
        ### Choice 2:df数据文件保存在 obj_in["wds_df"]
        df0 = obj_in["wds_df"]
        key_column = obj_in["dict"]["prime_key"]   
        table_name = obj_in["dict"]["table_name"]
      
        print("table_name", table_name )
        # Get columns ,avoid "Unnamed：0"
        df_cols = pd.read_csv( self.path_wds+"\\"+ table_name+"\\"+ "columns.csv" ,index_col=0 )
        col_list =list(df_cols["0"])

        ### 获取 key_column 对应的日期值或者其他值：
        date_list = list( df0[key_column].drop_duplicates().values) 

        for temp_date in date_list :
            try :
                # type is int64, temp_date
                # if temp_date >= 20150917 :    
                temp_df = df0[ df0[key_column]==temp_date ]
                # print( temp_df.head().T  )
                if len(temp_df.index) > 0 :
                    file_name_date= "WDS_"+ key_column +"_" + str( int(temp_date) ) +"_ALL.csv" 

                    ##############################################################################
                    ### temp method 
                    # if temp_date >= 20200000 :    
                    #     print(temp_date, len( temp_df.index) )
                    #     temp_df.loc[:,col_list].to_csv( self.path_wds+table_name+"\\"+ file_name_date  )
                    
                    ##############################################################################
                    ### 合并文件：
                    ## 查看是否已经有该文件
                    if os.path.exists( self.path_wds+table_name+"\\"+ file_name_date ) :
                        # 不带index值读取csv，避免 "Unnamed: 0"
                        # print("Debug===",self.path_wds+table_name+"\\"+ file_name_date )
                        try :
                            df_in = pd.read_csv( self.path_wds+table_name+"\\"+ file_name_date  ,error_bad_lines=False)
                        except:
                            df_in = pd.read_csv( self.path_wds+table_name+"\\"+ file_name_date ,index_col=0,encoding="gbk",error_bad_lines=False)

                        ### 把新增数据df_sub和原有数据df_in合并
                        # print(temp_date,"Debug=== len:df_in",len(df_in.index), "len_temp_df ",len( temp_df.index  ) )
                        # input1= input("Check to proceed......")
                        
                        # 历史文件容易出现行列不匹配的问题old versoin: df_in = df_in.append( temp_df,ignore_index=True )
                        if len(temp_df.index) > len( df_in.index) :
                            if len( df_in.index) > 2 :
                                ### 直接放弃 df_in if <= 2                                     
                                temp_df = temp_df.append( df_in,ignore_index=True )
                        
                        ### 剔除重复项：
                        # Before : df_in = df_in.drop_duplicates(subset=["OBJECT_ID"],keep="last")
                        # since 20210101
                        temp_df = temp_df.drop_duplicates(subset=["OBJECT_ID"],keep="last")
                        # len_pre:历史文件长度
                        len_pre = len(df_in.index)

                        if len( temp_df.index) > len_pre and len( temp_df.index)>1 :
                            print(temp_date,"length of rows:",len(df_in.index),len_pre ) 
                            temp_df.loc[:,col_list].to_csv( self.path_wds+table_name+"\\"+ file_name_date  )               
                    else :
                        temp_df.loc[:,col_list].to_csv( self.path_wds+table_name+"\\"+ file_name_date  )
            except :
                pass
            
        return obj_in



