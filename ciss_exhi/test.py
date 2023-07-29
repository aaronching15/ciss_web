### rc 测试models.py 数据脚本等。
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

import sys,os 
# test.py当前目录=C:\\rc_HUARONG\\rc_HUARONG\\ciss_web\\ciss_exhi
path_ciss_web = os.getcwd().split("ciss_exhi")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc)
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )

print("current path: \n",  sys.path ) 
#################################################################################
### 输入类型
print("search","get","给定筛选条件，查询表格、表格内column、row数据"  ) 
print("alter 改变数据 alter table data") 
print("ins,insert 插入数据=在table里新增行") 
print("excel，df  导入Excel批量插入数据，输入df，根据index，逐行输入 ") 
print("gen,generat 创建新表 |如果已经存在，需要先删除再新建 ") 
print("del，删除表:  drop删除表结构和表数据 ") 
print("select，表格数据筛选") 
print("up，表格数据筛选和更新；| alter update ") 
print("create，新建数据库 ") 

input_str = input("输入数字、类型、type对应脚本：")

#################################################################################
### 连接SQLite数据库
from database import db_sqlite
db_sqlite_1 = db_sqlite()

#################################################################################
### create 新建数据库
if input_str in  ["create"]  :
    obj_db = {}
    obj_db["db_name"] = input("Type in name of database for sqlite3:")
    
    obj_db = db_sqlite_1.create_database( obj_db )

    asd 


#################################################################################
### 给定筛选条件，查询表格、表格内column、row数据 
if input_str in  ["search","get"]  :
    obj_db = {}
    obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"
    obj_db["get_type"] = "table"
    obj_db["col_name"] = "id"
    obj_db["select_value"] = "=99" # "<10"
    obj_db = db_sqlite_1.get_table_data( obj_db )

    asd 

#################################################################################
### 改变数据 alter table data
# 例如：把
if input_str == "alter"  :
    obj_db = {}
    obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"
    ############################################
    ### alter_type: "column_type"，"column_name"
    obj_db["alter_type"] = "column_type"
    obj_db["alter_column_name"] = "code"
    obj_db["alter_column_type"] = "varchar(20)"




    asd 

#################################################################################
### 插入数据=在table里新增行
if input_str in ["ins","insert" ]  :
    obj_db = {}
    obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"
    ### insert_type 类型: "1row","1col","N"
    obj_db["insert_type"] = "1r"
    dict_1r={} 
    dict_1r["code"] = "630008.OF"
    dict_1r["name"] = "华商策略精选"
    dict_1r["date"] = "20210823"
    dict_1r["style_fund"] = "价值" 
    dict_1r["theme_fund"] = "周期" 
    dict_1r["ind_1"] = "有色金属"
    dict_1r["ind_2"] = "交通运输"
    dict_1r["ind_3"] = "钢铁"
    dict_1r["ind_num"] = "4"
    score_performance = (7+7+5)/30
    dict_1r["score_performance"] = str( round(score_performance*100,1) )
    dict_1r["s_down_market"] = "7"
    dict_1r["s_flat_market"] = "7"
    dict_1r["s_up_market"] = "5"
    dict_1r["abstract_analysis"] = "周海栋干了4年研究员，14年开始做基金经理。早期主要配交运、电子、计算机，最新变成有色金属、交运、钢铁、计算机"
    dict_1r["fund_namager"] = "周海栋,王毅文"
    dict_1r["note"] = ""
    dict_1r["date_lastmodify"] = ""

    obj_db["dict_1r"] = dict_1r
    ###
    obj_db = db_sqlite_1.insert_table_data( obj_db )

    asd 

#################################################################################
### 导入Excel批量插入数据，输入df，根据index，逐行输入
if input_str in ["excel","df" ]  :
    
    
    #######################################################
    ###
    obj_db = {}
    obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"
    ### insert_type 类型: "1row","1col","N"
    obj_db["insert_type"] = "df" 
    
    ### 外部数据信息
    obj_db["path_excel"] = "C:\\rc_HUARONG\\rc_HUARONG\\ciss_web\\ciss_exhi\\"
    obj_db["file_name"] =  "db_manage.xlsx"
    obj_db["sheet_name"] ="temp"
    ###    
    obj_db = db_sqlite_1.insert_table_data( obj_db )


    asd


#################################################################################
### 创建新表 |如果已经存在，需要先删除再新建
# Notes:一定要新建主键，否则后续新增麻烦；不能使用ALTER TABLE语句来创建主键。在SQLite中需要先创建一个与原表一样的新表，并在这个新表上创建主键，然后复制旧表中的所有数据到新表中就可以了。
if input_str in ["gen","generate" ] :
    obj_db = {}
    obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"

    ##############################################
    ### Type 1：根据excel文件的定义生成表格
    ### 提取表格数据结构： sheet=table_column ;file=db_manage.xlsx
    obj_db["table_name"] = "event_view_multiasset_macro"
    obj_db["gen_type"] = "excel"
    # TODO 导入excel file=db_manage.xlsx


    ##############################################
    ### Type 2：通过字典变量 dict_column直接定义
    # obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"
    # obj_db["gen_type"] = "dict"
    # # notes:默认"_id" 会自动生成
    # ### dict_column has a "key-value" structure
    # # notes:text才能储存非unicode的汉字！varchar在sql新版本才接受中文。
    # dict_column={}
    # ### notes: char是整数的一个子集,不包含小数点".";要用varchar；sqlite3.OperationalError: unrecognized token: "630008.OF"
    # dict_column["code"] = "varchar(20)"
    # dict_column["name"] = "text"
    # dict_column["date"] = "date"
    # dict_column["style_fund"] = "text"
    # dict_column["theme_fund"] = "text"
    # dict_column["ind_1"] = "text"
    # dict_column["ind_2"] = "text"
    # dict_column["ind_3"] = "text"
    # dict_column["ind_num"] = "int"
    # dict_column["score_performance"] = "float(255,2)"
    # dict_column["s_down_market"] = "float(255,2)"
    # dict_column["s_flat_market"] = "float(255,2)"
    # dict_column["s_up_market"] = "float(255,2)"
    # dict_column["abstract_analysis"] = "text"
    # dict_column["fund_namager"] = "text"
    # dict_column["note"] = "text"
    # dict_column["date_lastmodify"] = "datetime"

    # obj_db["dict_column"] = dict_column

    ##############################################
    ### 
    obj_db = db_sqlite_1.generate_table( obj_db )

    asd 

#################################################################################
### 删除表:  drop删除表结构和表数据，truncate删除表数据，delete删除某一行
### 慎重！！！
if input_str in ["del","delete"]   :
    obj_db = {}
    obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"
    obj_db["delete_type"] = "table"
    obj_db = db_sqlite_1.delete_table( obj_db )
    asd


#################################################################################
### 表格数据筛选
# 例如：判断'nan'异常值和对'date_lastmodify'填入系统最新日期；3，将'date'里的日期数字转换成日期？
if input_str == "select"  :
    ########################################
    obj_db = {}
    ### step 1 查询
    obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"
    # 判断'nan'异常值 |   "select  from where  is Null"
    
    ########################################
    # obj_db["select_type"] = input("Type in matching value,such as null,nan.0,...") 
    # obj_db["col_name"] = input("Type in matching column name,such as name...") 
    
    ########################################
    ### 匹配具体数值 "nan" # 注意，不能用"nan.0"
    obj_db["select_value"] = "nan"  
    obj_db["col_name"] = "name"

    ########################################
    ### 匹配多列
    # obj_db["col_name"] = "name"
    # ### dict as input 
    # obj_db["dict_select"] = { }
    # obj_db["dict_select"]["code"] = "630008.OF"
    # obj_db["dict_select"]["date"] = "20210823"
    # print("dict_select:" , obj_db["dict_select"] )

    

    
    obj_db = db_sqlite_1.select_table_data( obj_db )
    asd 

#################################################################################
### 表格数据筛选和更新；| alter update
if input_str in ["","update" ] :
    ########################################
    obj_db = {}
    ### step 1 查询
    obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"
    # 判断'nan'异常值 |   "select  from where  is Null"
    
    ########################################
    ### 对某一列简单操作 : 百分位转换：匹配业绩得分小于1的，乘以100 
    # obj_db["update_type"] = "1col" 
    # obj_db["select_value"] = "<1"  
    # obj_db["operation"] = "*100"  
    # obj_db["col_name"] = "score_performance"
    # obj_db = db_sqlite_1.update_table_data( obj_db )

    ########################################
    ### df对整个df进行更新
    # obj_db["update_type"] = "df" 
    # obj_db = db_sqlite_1.update_table_data( obj_db )


    ############################################
    ### TODO 匹配基金名称，根据id进行update更新每个基金的名称
    obj_db["update_type"] = "fund_name"

    ###
    obj_db = db_sqlite_1.update_table_data( obj_db )


    
    asd 




    ### 日期调整 | sqlite获得系统日期 date('now')


#################################################################################
### MySql 
### 1,MySQLdb 是用于Python链接Mysql数据库的接口 import MySQLdb
### 2,安装 MySQL 驱动程序；python -m pip install mysql-connector
#################################################################################
### 操作mysql数据库
if input_str in ["mysql" ] :
    ########################################
    ### 检查数据库是否存在
    import mysql.connector

    mydb=mysql.connector.connect(
        host="localhost",
        user="rc",
        passwd="rc"
    )

    mycursor=mydb.cursor()
    mycursor.execute("create database zdz")

    


    ########################################
    ### 检查数据库是否存在
    # import mysql.connector

    # mydb=mysql.connector.connect(
    #     host="localhost",
    # user="yourusername",
    # passwd="yourpassword"
    # )

    # mycursor=mydb.cursor()
    # mycursor.execute("show databases")

    # for x in mycursor:
    #     print(x)



    obj_db = {}
    ### step 1 查询
    obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis"
    # 判断'nan'异常值 |   "select  from where  is Null"




    asd 


















    # table_name = "ciss_exhi"+ "_" + "strategy" 

    # # 创建一个Cursor:
    # cursor = conn.cursor()
    # # 执行查询语句:     
    # ########################################
    # ###    
    # col_name = "stra_name"
    # col_value = "cs_industry_medical"

    # ### 如果 SQL 语句带有参数，那么需要把参数按照位置传递给 execute() 方法，有几个 ? 占位符就必须对应几个参数，例如：
    # # cursor.execute('select*from user where name=?and pwd=?',('abc','password')) 
    # ### 查询name包含“张三”的所有记录 ;  '%"+"张三"+"%';"% "+ col_value +" %"
    # str_all = "select * from " + table_name +" where "+col_name +" like ?"
    # # notes:列的名称不能用？代替。
    # cursor.execute( str_all ,  (col_value,) )
    # # str_all = "select * from " + table_name +" where "+col_name +" like ?"
    # # print(str_all)
    # # # cursor.execute( str_all )
    # # cursor.execute( str_all ,  (col_value,))

    # # 获得查询结果集:
    # # type of result,list;result[0]=tuple;
    # result = cursor.fetchall()
    # print("Debug=================================1")

    # print( result)

    # temp_cols = [i[0] for i in cursor.description  ]
    # print("temp_cols \n", temp_cols)
    # import pandas as pd 
    # df1 = pd.DataFrame( result ,columns= temp_cols )
    # print( "df1" ) 
    # print( df1 )

    # ########################################
    # ### 
    # col_name = "stra_intro"
    # col_value = "产业链" # "intro" # "2019" #  
    # ### 查询name包含“张三”的所有记录 ;  '%"+"张三"+"%';"% "+ col_value +" %"
    # str_all = "select * from " + table_name +" where "+col_name +" = ?"
    # # notes:列的名称不能用？代替。
    # cursor.execute( str_all ,  ( col_value,) )

    # ### 获得查询结果集:
    # result = cursor.fetchall()
    # print("Debug=================================2")
    # print( result )

    # temp_cols = [i[0] for i in cursor.description  ]
    # print("temp_cols \n", temp_cols)
    # import pandas as pd 
    # df1 = pd.DataFrame( result ,columns= temp_cols )
    # print( "df1" ) 
    # print( df1["stra_intro"] )


    # ########################################
    # ### Python的DB-API,要搞清楚Connection和Cursor对象，打开后一定记得关闭
    # # 关闭Cursor:
    # cursor.close()
    # # 提交事务:
    # conn.commit()
    # # 关闭Connection:
    # conn.close()