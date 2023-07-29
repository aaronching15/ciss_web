# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo 

Function: 功能：
1，web网页和sql数据库联动。excel模型来源:file=rc_个股推荐行业事件.xlsx;
2，研究信息网页&表格信息；分类：【1,个股事件；2，股票池；3，市场行业风格；4，基金时间序列-主动和指数；5，大类资产】；【】
3，通过网页提交的request读取、写入、修改sqlite或mysql中的数据。 

3，关联脚本：对应配置文件 | config\config_db.py

4,OUTPUT:
    1,obj_1["dict"]，字典信息,json
    1.1，把所有变量的中英文注释存在字典的"notes"里
    2,obj_1["df"]，表格信息,dataframe  

5,分析：目标是所有数据变量以object类型作为输入输出，其中主要是2个key:
    1,obj_1["dict"]:字典格式，数据io都采用json的字典格式。
    2,obj_1["df"]:DataFrame格式

6，Notes: 
6.1，输出文件统一保存至子目录 \\......\\
    refernce: rC_Data_Initial.py；similar with get_wind.py
date:last | since 220824
===============================================
'''
import sys,os 
import pandas as pd
import numpy as np

import sqlite3

#######################################################################
### 操作 sqlite类型数据库
class db_sqlite():
    def __init__(self):                
        ##########################################
        ###  
        self.nan = np.nan 
        import os
        # "C:\\rc_202X\\rc_202X\\ciss_web\\"
        self.path_ciss_web = os.getcwd().split("ciss_web")[0]+"ciss_web\\"
        self.path_ciss_rc = self.path_ciss_web +"CISS_rc\\"
        # self.path_db = self.path_ciss_rc + "db\\"
        self.path_dt = self.path_ciss_rc + "db\\db_times\\"
        ### 
        self.path_pms =  os.getcwd().split("ciss_web")[0]+"\\data_pms\\"  
        self.path_wind_terminal = self.path_pms + "wind_terminal\\" 
        self.path_wss = self.path_pms + "wss\\"
        self.path_wpf = self.path_pms + "wpf\\"
        self.path_wpd = self.path_pms + "wpd\\"
        self.path_wsd = self.path_pms + "wsd\\"

        # 导入配置文件对象，例如path_db_wind等
        sys.path.append( self.path_ciss_rc+ "config\\")
        from config_data import config_db
        config_db_1 = config_db()
        
        ########################################
        ### 链接数据库
        ### 导入SQLite驱动 | import sqlite3
        # "C:\\rc_2023\\rc_202X\\ciss_web\\" 
        self.path_db = self.path_ciss_web  
        
        
        ########################################
        ### 连接到默认SQLite数据库 | path_db = C:\rc_202X\rc_202X\ciss_web 
        # self.conn = sqlite3.connect( self.path_db  + 'db_funda.sqlite3')
        # self.cursor = self.conn.cursor()

    def print_info(self):         
        ################################################################################
        ### Database operation
        print("get_table_data | 给定筛选条件，获取表格数据")
        print("insert_table_data | 插入数据 insert table data")
        ### 筛选查询和更新数值
        print("select_table_data | 筛选数据 select table data")
        print("update_table_data | 更新表格数据 update table data")

        ### 改变数据表的columns类型
        print("alter_table_data | 改变数据表的columns类型 alter table data")
        ### 删除部分index，如重复列
        print("delete_table_index | 删除表格 ")
        ########################################
        ### 创建新表 ;删除表格
        print("generate_table | 创建新表 ")
        print("delete_table | 删除表格 ")
        
        ########################################
        ### 新建数据库文件
        print("create_database | 创建新数据库 ")
        

    def get_table_data(self,obj_db ):
        ################################################################################
        ### 给定筛选条件，查询表格数据 
        db_name = obj_db["db_name"] # "db_funda.sqlite3"
        ########################################
        ### 连接到SQLite数据库 | path_db = C:\rc_202X\rc_202X\ciss_web 
        self.conn = sqlite3.connect( self.path_db  + db_name )
        self.cursor = self.conn.cursor()

        # 给表添加datetime类型列，并设置默认值：表名：score_record,列名：operate_datetime,类型：datetime,默认值：getdate()
        # 插入语句: insert into score_record(user_acc,operate_value,current_value) values('123',100,9570) 
        # 更改表的字段名 execute sp_rename 'score_record.operate_datetime','operate_time' 
        ### insert into scorebak select * from socre where neza='neza'   --插入一行,要求表scorebak 必须存在
        # select *  into scorebak from score  where neza='neza'  --也是插入一行,要求表scorebak 不存在

        ### 例子2：create table student( id int,name char(16),born_year year,birth date, class_time time, reg_time datetime );
        # 插入数据2： insert into student values(1,'egon','2000','2000-01-27','08:30:00','2013-11-11 11:11:11');
        
        ### 例子3：create table teacher(id int,name char(16),sex enum('male','female','others'),hobbies set('play','read','music','piao') );
        # insert into teacher values(1,'egon','male','play,piao');
        table_name = obj_db["table_name"] 

        if obj_db["get_type"] == "table" :
            # "ciss_exhi"+ "_" + "fund_analysis" 
            col_name = obj_db["col_name"] # "id"
            select_value = obj_db["select_value"]
            str_sql = "select * from " + table_name + " where " +  col_name + select_value


        ################################################################################
        ### Execute command
        ### 创建一个Cursor:
        temp_data = self.cursor.execute( str_sql ).fetchall()   
        ### 获取列信息 || https://blog.csdn.net/qq_41797451/article/details/80230399
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)

        df_data= pd.DataFrame( temp_data,columns= temp_cols )
        print("Length of data ",len(temp_data) )
        print( df_data.tail(5) )  

        ### 关闭Cursor:
        self.conn.commit()
        self.conn.close()  

        return obj_db

    def insert_table_data(self,obj_db ):
        ################################################################################
        ### 插入数据 insert row_data
        ### Notes:SQL 语句在传参时，应该避免使用 Python 的字符串操作 '%s',而是
        # sql_add = 'insert into images(id, g, c, l) values(?, ?, ?, ?, ?, ?)'
        # cur.execute(sql_add, (str_id, str_g, str_c, str_l))
        db_name = obj_db["db_name"] # "db_funda.sqlite3"
        ########################################
        ### 连接到SQLite数据库 | path_db = C:\rc_202X\rc_202X\ciss_web 
        self.conn = sqlite3.connect( self.path_db  + db_name )
        self.cursor = self.conn.cursor()

        table_name = obj_db["table_name"] 
        ### insert_type 类型: "1row","1col","N"。 "N":按照dataframe逐行输入。
        insert_type = obj_db["insert_type"] 
                
        ################################################################################
        ### insert_type == "1r" 导入一行 | 注意sql语句中的文本如果含有空格，sql查询文本会导致数据出错；需要进行替代
        if insert_type == "1r"  :
            str_incert_key = ""
            str_incert_value = ""
            dict_1r = obj_db["dict_1r"]
            for temp_key in dict_1r.keys() :
                print( temp_key, dict_1r[temp_key] )
                ### 构造sql-table的 columns部分 ； + " " +
                str_incert_key  = str_incert_key +"'"+  temp_key + "'," 
                str_incert_value= str_incert_value +"'"+ str(dict_1r[temp_key]) + "'," 

            ### 去掉最前的空格和最后的逗号,否则报错 sqlite3.OperationalError: near ")": syntax error
            str_incert_key = str_incert_key[:-1]
            str_incert_value= str_incert_value[:-1]
            print("str_incert_key \n", str_incert_key )
            print("str_incert_value \n", str_incert_value )
            ### 例子：insert into score_record(user_acc,operate_value,current_value) values('123',100,9570)
            str_sql = "insert into "+ table_name +"(" + str_incert_key + ")"+" values(" + str_incert_value + ")"
            # str_sql = "insert into "+ table_name + " values(" + str_incert_value + ")"
            print("str_sql \n", str_sql )
            ################################################################################
            ### Execute command
            ### 创建一个Cursor: 
            result = self.cursor.execute( str_sql ) 
            ### 提交到数据库
            self.conn.commit()


        ################################################################################
        ### insert_type == "1r" 导入1个df，df.columns 需要和数据库的columns一致        
        if insert_type == "df"  :
            ################################################################################
            ### 2种方式：1，获取外部Excel数据表导入 ；2，输入项已经是 df
            if "path_excel" in obj_db.keys() :
                ### 1，获取外部Excel数据表导入 ；
                path_excel = obj_db["path_excel"] 
                file_name = obj_db["file_name"] 
                sheet_name = obj_db["sheet_name"]

                ### 导入excel形成df | C:\rc_202X\rc_202X\ciss_web\ciss_exhi\ ,"db_manage.xlsx",temp 
                df_table = pd.read_excel(path_excel+file_name, sheet_name= sheet_name  )
            elif "df_table" in obj_db.keys() :
                ### 2，输入项已经是 df
                df_table = obj_db["df_table"] 
            
            ### notes: 有时候df.index 可能不是数字，而是代码code、date等;原来的index会被保存在名为"index"那一列
            # df.reset_index(）加上drop=True不保留原来的index,否则index列会被存入sql报错
            df_table = df_table.reset_index( drop=True )
            
            ################################################################################
            ### 设置 id 列 | notes:含有id的列，要加入id  
            ###########################################
            ### step1 获取最大的id值；因为部分id有可能被删除，不是按顺序排列
            # 不能用cout：  str_sql = "select Count(*) from " + table_name
            str_sql = "select max(id) from " + table_name

            print("str_sql=", str_sql )
            
            ### type of temp_data =list,temp_data[0] is tuple
            # 例如： temp_data= [(2192,)] ;如果没有记录则 temp_data= [(None,)]
            # temp_data[0][0] = None , <class 'NoneType'>
            # 判断None这2种都可以：temp_data[0][0] == None , temp_data[0][0] is None
            temp_data = self.cursor.execute( str_sql ).fetchall()    
            # temp_cols = [i[0] for i in self.cursor.description  ]
            # df_data= pd.DataFrame( temp_data,columns= temp_cols ) # df_data.loc[0,"max(id)"] 
            ### id_num 表示当前有多少个id，下一个赋值应该是 id_num+1
            if temp_data[0][0] == None :  
                id_num = 0
                print( "temp_data=" , temp_data[0][0] )
                print("Error for no id,CHECK if no record in table or other errors.....") 
            else :
                id_num = temp_data[0][0]
            print("id_num \n", id_num, type(id_num ) )
            
            ###########################################
            ### step2，对id进行赋值
            # 如果数据库是空的，插入时会报错： UNIQUE constraint failed: event_view_multiasset_macro.id
            df_table["id"] = df_table.index + id_num +  1 
            
            ################################################################################
            ### 
            count = 0 
            for temp_i in df_table.index :
                temp_id = str( df_table.loc[temp_i, "id"] )
                ### for every new row 
                str_incert_col = ""
                str_incert_value = ""
                # str_incert_col = "'id',"
                # str_incert_value = "'" + temp_id +"',"
                for temp_col in df_table.columns :
                    print( temp_col, df_table.loc[temp_i, temp_col] )
                    ### 构造sql-table的 columns部分 ； + " " +
                    str_incert_col  = str_incert_col +"'"+  temp_col + "'," 
                    temp_str = str(df_table.loc[temp_i, temp_col])
                    ### 去除空格和单引号，避免sql语言识别错误
                    temp_str = temp_str.replace(" ","")
                    temp_str = temp_str.replace("'","^")
                    str_incert_value = str_incert_value +"'"+ temp_str + "'," 
                ### 去掉最前的空格和最后的逗号,否则报错 sqlite3.OperationalError: near ")": syntax error
                str_incert_col = str_incert_col[:-1]
                str_incert_value= str_incert_value[:-1]
                print("str_incert_col \n", str_incert_col )
                print("str_incert_value \n", str_incert_value )
                ### 例子：insert into score_record(user_acc,operate_value,current_value) values('123',100,9570)
                str_sql = "insert into "+ table_name +"(" + str_incert_col + ")"+" values(" + str_incert_value + ")"
                ### 必须保证列的顺序，否则  UNIQUE constraint failed: event_view_multiasset_macro.id
                # str_sql = "insert into "+ table_name + " values(" + str_incert_value + ")"
                print("str_sql=",str_sql )

                print("count =  \n", count  )
                count = count + 1  
                ################################################################################
                ### Execute command
                ### 创建一个Cursor: 
                result = self.cursor.execute( str_sql )  
                ### 提交到数据库
                self.conn.commit()
        
        ################################################################################         
        ### 获取列信息  
        if insert_type == "1r"  :
            # 只获取最新的5行数据：SELECT * FROM TableName ORDER BY rowid DESC LIMIT 5;"    
            str_sql = "select * from " + table_name + " ORDER BY rowid DESC LIMIT 10 "
            temp_data = self.cursor.execute( str_sql ).fetchall()    
        if insert_type == "df"  :
            str_sql = "select * from " + table_name + " ORDER BY rowid DESC LIMIT 50 "
            temp_data = self.cursor.execute( str_sql ).fetchall()    

        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)
        df_data= pd.DataFrame( temp_data,columns= temp_cols )
        print("Tail of data ",df_data.tail(5).T )  

        ### 返回数据库中的最新N条记录
        obj_db["df_data"] = df_data

        ################################################################################         
        ### 关闭Cursor:
        self.conn.commit()
        self.conn.close()  

        return obj_db

    def select_table_data(self,obj_db ):
        ################################################################################
        ### 筛选数据 sleclt table data | 所有字符串的值都要加单引号'' ！！！
        ### Notes:SQL 语句在传参时，应该避免使用 Python 的字符串操作 '%s',而是
        # SQL = "SELECT COURSE_DICT,TITLE_LIST FROM {table} WHERE STU_ID =? and number=? ".format(table=table_name)
        # arg = (stu_id, number1)    # cursor.execute(SQL, arg)
        db_name = obj_db["db_name"] # "db_funda.sqlite3"
        ########################################
        ### 连接到SQLite数据库 | path_db = C:\rc_202X\rc_202X\ciss_web 
        import sqlite3
        self.conn = sqlite3.connect( self.path_db  + db_name )
        self.cursor = self.conn.cursor() 

        ### 查询所有table名称 | select name from sqlite_master where type = 'table' order by name
        table_list = self.cursor.execute( "select name from sqlite_master where type = 'table' order by name " )
        temp_cols = [i[0] for i in self.cursor.description  ] 
        table_list = pd.DataFrame( table_list,columns= temp_cols )
        
        # print("table_list=",table_list ) 

        table_name = obj_db["table_name"] 
        ##################################################################################
        ### dict_select || 多个列的模糊匹配
        if "dict_select" in obj_db.keys(): 
            dict_select = obj_db["dict_select"] 
            str_select =""
            for temp_key in dict_select.keys() :
                ### 精确匹配 | 所有字符串的值都要加单引号'' ！！！
                # str_select = str_select + temp_key + "='" + dict_select[temp_key] +"' and "
                ### 模糊匹配 | 避免 temp_key="" 的情况
                if len( temp_key ) > 0 :
                    str_select = str_select + temp_key + " like '%" + dict_select[temp_key] +"%' and "
                
            ### 去掉尾部
            str_select = str_select[:-5]
            print("str_select=", str_select ) 

            ##############################################
            ### 组装 str_sql
            # str_sql = "select "+ "*" + " from "+table_name+" where " + str_select
            if len( str_select ) > 1 :
                str_sql = "select "+ "*" + " from "+table_name+" where " + str_select
            else :
                str_sql = "select "+ "*" + " from "+table_name
            
            print("str_sql \n",str_sql ) 
            temp_data = self.cursor.execute( str_sql ).fetchall()  
            
        ##################################################################################
        ### 只匹配一列的特定值 || 单个列的精确匹配
        if "select_value" in obj_db.keys(): 
            select_value = obj_db["select_value"]  
            col_name = obj_db["col_name"]
            #####################################         
            ### 检查列值里为 nan.0的，要用'nan' ; 如果用is null 或 nan.0 都返回空值
            if select_value == "nan" : 
                # "select  from where  is Null"
                str_sql = "select "+ "*"  + " from " + table_name +" where " +col_name +  " ='" + select_value + "' "
                print("str_sql \n",str_sql )
                
                ### 
                temp_data = self.cursor.execute( str_sql ).fetchall() 
            #####################################         
            ### 检查字符串包含某一字符,如包= apple | " like '% "+ select_value +" %' "
            # 还没成功：  where match( col_name) against( 'nan.0' )
            ### TODO:报错：sqlite3.OperationalError: near ".0": syntax error
            else :
                ### 精确匹配字符串
                # str_sql = "select "+ "*"  + " from "+table_name+" where " +col_name +  " = '"+ select_value +"' "
                ### 模糊匹配是否包含字符串 || WHERE SALARY LIKE '%200%'	查找任意位置包含 200 的任意值
                # 可以得到结果： select * from ciss_exhi_fund_analysis where ind_1 like '%食品%'
                str_sql = "select "+ "*"  + " from "+table_name+" where " +col_name +  " like '%"+ select_value +"%' "
                
                print("str_sql \n",str_sql )

                # str_sql = "select "+ col_name + " from "+table_name+"where match(" +col_name +  ") against(?) "
                # arg = (select_value )
                # temp_data = self.cursor.execute( str_sql,arg ).fetchall() 
                ### 
                temp_data = self.cursor.execute( str_sql  ).fetchall() 
                print("temp_data:\n", temp_data )

        ##################################################################################
        ### 只匹配多个列的特定值 || 多个列的精确匹配
        if "col_list" in obj_db.keys(): 
            col_list = obj_db["col_list"]
            value_list = obj_db["value_list"]
            str_list = ""
            for j in range( len(col_list) ) : 
                ### 多个 col_name +  " like '% "+ select_value +" %' "
                str_list = col_list[j] +  " ='" + str(value_list[j])  + "' and "
            ###
            str_list = str_list[:-4]

            str_sql = "select "+ "*"  + " from "+table_name+" where " + str_list

            ##############################################
            ### 组装 str_sql
            # str_sql = "select "+ "*" + " from "+table_name+" where " + str_select
            if len( str_select ) > 1 :
                str_sql = str_sql + " and " + str_select
            else :
                ### 相当于搜索全部数据,去除" where "字段,只保留前边
                str_sql = str_sql.split("where")[0]
                
            print("str_sql \n",str_sql )

            temp_data = self.cursor.execute( str_sql  ).fetchall() 
            print("temp_data:\n", temp_data )

        ##################################################################################
        ### 匹配日期区间：需要确定日期column的列名称（通常是 date ）和开始、结束日期 
        # # obj_db["col_name_date"], 日期column的列名称（通常是 date ）
        if "col_name_date" in obj_db.keys(): 
            col_name_date = obj_db["col_name_date"] 
            date_begin = obj_db["date_begin"] 
            date_end = obj_db["date_end"] 

            str_list_date = ""
            str_list_date = col_name_date +  " >='" + date_begin 
            if len( date_end ) >= 6 :
                # 不一定有 date_end | 220914 or 20220904
                str_list_date = str_list_date + "' and " + col_name_date +  " <='" + date_end + "' "

            str_sql = "select "+ "*"  + " from "+table_name+" where " + str_list_date

            ##############################################
            ### 在日期区间的基础上，判断是否有精确匹配的 
            if "dict_select" in obj_db.keys(): 
                dict_select = obj_db["dict_select"] 
                str_select =""
                for temp_key in dict_select.keys() :
                    ### 精确匹配 | 所有字符串的值都要加单引号'' ！！！
                    # str_select = str_select + temp_key + "='" + dict_select[temp_key] +"' and "
                    ### 模糊匹配 str_select=  like '%%'
                    if len( temp_key ) > 0 :
                        str_select = str_select + temp_key + " like '%" + dict_select[temp_key] +"%' and " 
                    
                ### 去掉尾部
                str_select = str_select[:-5]
                print("str_select=", str_select ) 

                ##############################################
                ### 组装 str_sql
                # str_sql = "select "+ "*" + " from "+table_name+" where " + str_select
                if len( str_select ) > 1 :
                    str_sql = str_sql + " and " + str_select
                else :
                    ### 相当于搜索全部数据,去除" where "字段,只保留前边
                    str_sql = str_sql.split("where")[0]


            print("str_sql \n",str_sql ) 
            temp_data = self.cursor.execute( str_sql ).fetchall() 
            print("temp_data:\n", temp_data )
        
        ################################################################################         
        ### 获取列信息          
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)
        df_data = pd.DataFrame( temp_data,columns= temp_cols )
        # print("Tail of data ",df_data.tail(5).T )  

        ### 关闭Cursor:
        self.conn.commit()
        self.conn.close()  

        ################################################################################         
        ### 返回数据库中的全部记录 
        obj_db["df_data"] = df_data 
        obj_db["path_db "] = "C:\\rc_2023\\rc_202X\\ciss_web\\"

        return obj_db

    def update_table_data(self,obj_db ):
        ################################################################################
        ### 更新表格数据 update table data | 
        # 例子：update table1 set score_performance=score_performance*100 where score_performance <1
        # 成功了的代码：update ciss_exhi_fund_analysis set score_performance = score_performance*100  where score_performance<1
        db_name = obj_db["db_name"] # "db_funda.sqlite3"
        ########################################
        ### 连接到SQLite数据库 | path_db = C:\rc_202X\rc_202X\ciss_web 
        self.conn = sqlite3.connect( self.path_db  + db_name )
        self.cursor = self.conn.cursor()

        table_name = obj_db["table_name"]          
        update_type = obj_db["update_type"] # = "1col" 

        ########################################
        ### "update_type" = "id" ：对某一列id的值进行更新
        ### 例子： update fundpool_stockpool_weight set weight=0.001,note='20221013' where id=139
        if update_type == "id" :
            id = obj_db["id"] 
            ###  
            dict_1r = obj_db["dict_1r"] 
            str_update = ""
            for temp_key in dict_1r.keys() :
                print( temp_key, dict_1r[temp_key] ) 
                str_update  = str_update + temp_key + "='" + dict_1r[temp_key] + "',"  
            ### 去掉尾巴
            str_update = str_update[:-1]
            ###
            str_sql = "update "+ table_name +" set " +str_update + " where "+ "id=" + id 
            print("str_sql \n",str_sql )
            temp_data = self.cursor.execute( str_sql ).fetchall()    

        ########################################
        ### "update_type" = "1col" ：对某一列简单操作
        if update_type == "1col" :
            ### 
            col_name = obj_db["col_name"] # "score_performance"
            select_value = obj_db["select_value"] # "<1"  
            operation = obj_db["operation"] # "*100 "
            ###
            str_sql = "update "+ table_name +" set " +col_name +" = "+ col_name + operation + " where "+ col_name + select_value
            print("str_sql \n",str_sql )
            temp_data = self.cursor.execute( str_sql ).fetchall()    
        ########################################
        ### obj_db["update_type"] = "df" ：对几列进行求和等操作；先导出为dataframe，计算后再存入
        if update_type == "df" :
            ########################################
            ### step1:导出df
            str_sql = "select * from " + table_name  
            temp_data = self.cursor.execute( str_sql ).fetchall()   
            temp_cols = [i[0] for i in self.cursor.description  ]
            print("temp_cols \n", temp_cols)
            df_data= pd.DataFrame( temp_data,columns= temp_cols )

            ########################################
            ### step2:求和计算 
            ### TODO:根据3列数据，重新计算总业绩得分。1，nan值用0代替；2,算法：(a+b+c)/30
            # for col_name in ["s_down_market","s_flat_market","s_up_market"] :
            #     str_sql = "update "+ table_name +" set " +col_name +" = "+ "0 "+ " where "+ col_name +" = "+ "'nan' "
            #     temp_data = self.cursor.execute( str_sql ).fetchall()  

            # ### calculate score_total
            # col_name = "score_performance"
            # str_sql = "update "+ table_name +" set " +col_name +" = "+"( "+ "s_down_market"+" " + "s_flat_market"+" " + "s_up_market" +")/30 "
            # print("str_sql \n",str_sql ) 
            ### 
            temp_data = self.cursor.execute( str_sql ).fetchall()    
      
        
        ################################################################################
        ### 匹配基金名称，根据id进行update
        if update_type == "fund_name" :
            ################################################################################
            ### 先统一导入所有基金代码
            ### 依次在 FF-基金研究-主动股票-220812.xlsx 里匹配
            path_fund_data = "C:\\rc_202X\\rc_202X\\data_pms\\wind_terminal\\"
            fund_type_list = ["主动股票", "偏股混合", "偏债混合", "纯债", "股票指数", "可转债", "纯债", "FOF"]
            count_f = 0 
            for fund_type in fund_type_list :
                file_name = "FF-基金研究-" + fund_type +"-220812.xlsx"
                df_temp = pd.read_excel( path_fund_data+ file_name, sheet_name="概况" )
                ###
                if count_f == 0 :
                    df_f_all = df_temp
                    count_f = 1
                else :
                    df_f_all = df_f_all.append(df_temp )

            print( "df_f_all \n", df_f_all.head().T  )

            ################################################################################    
            ### 获取列信息 | notes:update模式下，不会返回数据
            str_sql = "select * from " + table_name 
            temp_data = self.cursor.execute( str_sql ).fetchall()   
            temp_cols = [i[0] for i in self.cursor.description  ]
            print("temp_cols \n", temp_cols)
            df_data = pd.DataFrame( temp_data,columns= temp_cols )
            print("Tail of data ",df_data.head(5).T )  

            #############################################         
            ### 对于每一列，取id和code、name
            count_code = 0 
            for temp_i in df_data.index :
                temp_id = df_data.loc[temp_i, "id"]
                temp_code = df_data.loc[temp_i, "code"]
                temp_name = df_data.loc[temp_i, "name"]
                print("BEFORE:temp_id=",temp_id, "temp_code=",temp_code , "temp_name=", temp_name )
                if temp_name == "nan" :
                    print("Working on code=",temp_code ,temp_name,count_code  )
                    ### find fund name in df_f_all by matching fund_code 
                    df_sub = df_f_all[ df_f_all["基金代码"] == temp_code ]
                    if len(df_sub.index) > 0 :
                        temp_name = df_sub["基金名称"].values[0]
                        print("temp_id=",temp_id, type(temp_id) , "temp_name=", temp_name )
                        
                        ################################################################################
                        ### save to table in sql
                        str_sql = ""
                        # note：update后边没有'table',set后边也没有column 
                        str_sql= str_sql+"update "+ table_name +" set " + "name" + "='"+ temp_name +"' where id='"+ str( int(temp_id) ) +"'"
                        print("str_sql \n", str_sql )

                        ### Execute command 
                        result = self.cursor.execute( str_sql ) 
                        
                        ###
                        count_code = count_code +1 
        

        ################################################################################         
        ### 获取列信息 | notes: update模式下，不会返回数据
        str_sql = "select * from " + table_name  
        temp_data = self.cursor.execute( str_sql ).fetchall()   
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)
        df_data= pd.DataFrame( temp_data,columns= temp_cols )
        print("Tail of data ",df_data.tail(5).T )  


        ### 关闭Cursor:
        self.conn.commit()
        self.conn.close()  

        return obj_db

    ################################################################################         
    ### 
    def alter_table_data(self,obj_db ):
        ################################################################################
        ### 改变数据表的columns类型 alter table data
        db_name = obj_db["db_name"] # "db_funda.sqlite3"
        ########################################
        ### 连接到SQLite数据库 | path_db = C:\rc_202X\rc_202X\ciss_web 
        self.conn = sqlite3.connect( self.path_db  + db_name )
        self.cursor = self.conn.cursor()


        # NOTE:SQLite中，除了重命名表和在已有的表中添加列，ALTER TABLE 命令不支持其他操作,不能改变列的某一行的值，要用update。
        # alter table 表名 alter column 字段 数据类型
        # alter table score_record alter column code varchar(20) |需要换行 ？？
        table_name = obj_db["table_name"]  
        alter_type = obj_db["alter_type"] 
        str_sql = ""
        ################################################################################
        ### 
        if alter_type == "column_type" :
            ### 改变column地类型，例如char(20) to varchar(20)
            column_name = obj_db["alter_column_name"]  
            column_type = obj_db["alter_column_type"]  
            
            str_sql= str_sql+"alter table "+ table_name +"\nalter column " + column_name +" "+ column_type
            print("str_sql \n", str_sql )
            ### Execute command 
            result = self.cursor.execute( str_sql ) 
        
  


        ################################################################################
        ### Notes: sqlite很多功能和mysql等不一样 || 转换日期和数字等类型：SELECT CAST(‘9.5’ AS decimal) 解决无效数字,其他符号会转义成null。cast((now) as date)
        ### 报错：near "alter": syntax error ； sqllite不支持直接使用语句alter table tablename modify column columnname varchar(400)；进行修改
        ''' -- 修改表名： ALTER TABLE 旧表名 RENAME AS 新表名 
        -- 增加表的字段： ALTER TABLE 表名 ADD 字段名 列属性 
        -- 修改约束：ALTER TABLE 表名 MODIFY 字段名 列属性[] 
        -- 字段重名： ALTER TABLE 表名 CHANGE 旧名字  新名字  列属性[] 
        -- 删除表的字段：ALTER TABLE 表名 DROP 字段名

        ALTER TABLE customer MODIFY c_card  CHAR(18);
        ALTER TABLE customer MODIFY c_sex  CHAR(2);
        ALTER TABLE customer CHANGE c_create  c_createtime TIMESTAMP;
        # INSERT into:中指定所有字段名
        ### 批量插入数据        
        INSERT INTO  admin (a_id,a_name,a_sex,a_phone) VALUES ('18101','zhang','男','1122'),
        ('18102','LI','女','1124'),('18103','TIAN','男','1123'),('18104','WANG','女','1125'); 
        # 将表中所有数据的指定字段全部更新: UPDATE admin SET a_sex ='男',a_phone='1111';
        '''
         
        
        
        ################################################################################
        
        
        ### 获取列信息  
        str_sql = "select * from " + table_name
        temp_data = self.cursor.execute( str_sql ).fetchall()    
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)
        df_data= pd.DataFrame( temp_data,columns= temp_cols )
        print("Tail of data ",df_data.tail(5).T )  

        ### 关闭Cursor:
        self.conn.commit()
        self.conn.close()  

        return obj_db

    def delete_table_index(self,obj_db ):
        ################################################################################
        ### 删除部分index，如重复列 
        db_name = obj_db["db_name"]  
        ########################################
        ### 连接到SQLite数据库 | path_db = C:\rc_202X\rc_202X\ciss_web 
        self.conn = sqlite3.connect( self.path_db  + db_name )
        self.cursor = self.conn.cursor()

        table_name = obj_db["table_name"]   
        
        ################################################################################
        ### 删除重复项 ： delete rows that match only 1 criteria
        if obj_db["delete_type"] == "duplicates"  :
            ################################################################################ 
            ### step 1:选出某几列数值相同的列的最大的id值
            if "col_list_str" in obj_db.keys():
                str_list = obj_db["col_list_str"] 
            
            elif "col_list" in obj_db.keys():
                col_list = obj_db["col_list"] 
                str_list = ""
                for j in range( len(col_list) ) : 
                    ### 多个 col_name +  " like '% "+ select_value +" %' "
                    str_list = col_list[j] +  ", "
            
                str_list = str_list[:-2]
            ######  
            str_sql = "select * from " +table_name+" where id in (select max(id) from " +table_name + " group by " 
            str_sql = str_sql + str_list + " )"
            print("str_sql \n",str_sql )   
            temp_data = self.cursor.execute( str_sql ).fetchall()   
            ### 获取列信息 || https://blog.csdn.net/qq_41797451/article/details/80230399
            temp_cols = [i[0] for i in self.cursor.description  ] 
            df_data= pd.DataFrame( temp_data,columns= temp_cols ) 
            # print( df_data.tail(5) )  

            ### 保存要删除的id对应df
            obj_db["df_del"] = df_data

            #####################################################
            ### step 2:根据返回的列的id，逐一删除sql中的记录
            for temp_i in df_data.index:
                temp_id = df_data.loc[temp_i,"id"]
                str_sql = "delete from "+table_name+" where " + "id=" + str( temp_id )
                print("str_sql \n",str_sql )    
                temp_data = self.cursor.execute( str_sql )
                
        ################################################################################ 
        ### 根据给定id 删除某一行
        elif obj_db["delete_type"] == "id"  :
            id_table = obj_db["id_table"] 

            str_sql = "delete from "+table_name+" where " + "id=" + str( id_table )
            print("str_sql \n",str_sql )    
            temp_data = self.cursor.execute( str_sql )
        
        
        ################################################################################
        ### 获取列信息  
        str_sql = "select * from " + table_name
        temp_data = self.cursor.execute( str_sql ).fetchall()    
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)
        obj_db["df_data"] = pd.DataFrame( temp_data,columns= temp_cols )
        # print("Tail of data ",obj_db["df_data"].tail(5).T )  
        
        ### 关闭Cursor:
        self.conn.commit()
        self.conn.close()  

        return obj_db

################################################################################
################################################################################
    def generate_table(self,obj_db ):
        ################################################################################
        ### 创建新表 |如果已经存在，需要先删除再新建
        # table_name = "ciss_exhi"+ "_" + "fund_analysis"
        db_name = obj_db["db_name"] # "db_funda.sqlite3"
        ########################################
        ### 连接到SQLite数据库 | path_db = C:\rc_202X\rc_202X\ciss_web 
        self.conn = sqlite3.connect( self.path_db  + db_name )
        self.cursor = self.conn.cursor()


        table_name = obj_db["table_name"]  
        ################################################################################
        ### 表格生成方式gen_type：2，excel:根据sheet里导入的df表格构建 
        if obj_db["gen_type"] == "excel" :
            ### example:event_view_multiasset_macro
            ###########################################
            ### step 1:根据给定table名称，导入column的定义
            path_ciss_exhi = self.path_db + "ciss_exhi\\"
            print("path_ciss_exhi=", path_ciss_exhi )
            file_name = "db_manage.xlsx"
            sheet = "table_column"
            df_col = pd.read_excel(path_ciss_exhi+file_name , sheet_name=sheet  )
            
            ### 匹配数据库名称
            df_sub = df_col[ df_col["db"] == db_name ]
            ### 匹配表格名称
            df_sub = df_sub[ df_sub["table"] == table_name ]
            print("df_sub \n",  df_sub )
            if len( df_sub.index ) > 1 : 
                ###########################################
                ### 生成sql命令： 
                str_col_mid = "id integer primary key not null,"
                for i in df_sub.index :
                    print( df_sub.loc[i,"column"], df_sub.loc[i,"column_type"] )
                    ### 构造sql-table的 columns部分
                    str_col_mid = str_col_mid + " " + df_sub.loc[i,"column"] + " " + df_sub.loc[i,"column_type"] + "," 
                ###########################################
                ### 去掉最后的逗号,否则报错 sqlite3.OperationalError: near ")": syntax error
                str_col_mid = str_col_mid[:-1]
                ### 最后要加括号
                # str_col = "(" + "_id integer primary key autoincrement," + str_col_mid + ")"
                str_col = "(" + str_col_mid + ")"
                print("str_col_mid \n", str_col_mid )
                
            else :
                print("Error,Excel文件无对应列column信息：" , path_ciss_exhi  , file_name,sheet )


        ################################################################################
        ### 表格生成方式gen_type：1，dict,根据输入的字典dict_column构建  
        if obj_db["gen_type"] == "dict" :
            dict_column = obj_db["dict_column"] 

            str_col_mid = "id integer primary key not null,"
            for temp_key in dict_column.keys() :
                print( temp_key, dict_column[temp_key] )
                ### 构造sql-table的 columns部分
                str_col_mid = str_col_mid + " " + temp_key+ " " +dict_column[temp_key]+ "," 
            
            ###########################################
            ### 去掉最后的逗号,否则报错 sqlite3.OperationalError: near ")": syntax error
            str_col_mid = str_col_mid[:-1]
            ### 最后要加括号
            # str_col = "(" + "_id integer primary key autoincrement," + str_col_mid + ")"
            str_col = "(" + str_col_mid + ")"
            print("str_col_mid \n", str_col_mid )

        ################################################################################
        ### Execute command
        ### 创建一个Cursor: cursor = self.conn.cursor()
        # self.conn = sqlite3.connect( self.path_db  + 'db_funda.sqlite3')
        # self.cursor = self.conn.cursor()
        result = self.cursor.execute( "create table " + table_name  + str_col   ) 
        
        ### 获取列信息  
        str_sql = "select * from " + table_name
        temp_data = self.cursor.execute( str_sql ).fetchall()    
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)
        df_data= pd.DataFrame( temp_data,columns= temp_cols )
        print("Tail of data ",df_data.tail(1) )  


        ################################################################################
        ### 根据需要新建的列名称和约束，生成对应的sql语句
        # 例子1：( _id integer primary key autoincrement, code text, name text, date )
        # str_para = "(" + "_id integer primary key autoincrement,"+ "code text " + "name text" + ")"
        ### 给表添加datetime类型列，并设置默认值：表名：score_record,列名：operate_datetime,类型：datetime,默认值：getdate()
        # alter table score_record add operate_datetime datetime default getdate()        

        ### 例子2：create table student( id int,name char(16),born_year year,birth date, class_time time, reg_time datetime );
        # 插入数据2： insert into student values(1,'egon','2000','2000-01-27','08:30:00','2013-11-11 11:11:11');
        
        ### 例子3：create table teacher(id int,name char(16),sex enum('male','female','others'),hobbies set('play','read','music','piao') );
        # insert into teacher values(1,'egon','male','play,piao'); set可以取多个值,enum只能取一个值

        ### 根据 dict_column里的col名称、类型、参数逐个补全sql语句 | 字段名1 类型[(宽度) 约束条件]
        # notes:1、在同一张表中，字段名不能重复 2、宽度和约束条件可选，字段名和类型是必须的 3、最后一个字段后不加逗号
        ### 定义变量类型：url=https://article.itxueyuan.com/WQ3nGM
        ### 数值类型：table1( x float(255,2));   #255最大总数位,2最大的小数位;精度的排序从低到高：float, double, decimal 2、float与double类型能存放的整数位比decimal更多
        # table1( x double(255,30)); t11(x decimal(65,30)),#65最大的总数位    30最大的小数位
        ### 字符串类型：(x char(4)；(y varchar(4) |text才能储存非unicode的汉字！ | 针对char类型,mysql在存储时会将数据用空格补全存放到硬盘中,但会在读出结果时自动去掉末尾的空格
        # insert into t12 values('hello')； select char_length(x) from t12; #output= 4
        # 查找效率: char查找效率会很高,varchar查找效率会更低。char最大长度是255字符,varchar最大长度是65535个;char会浪费空间,varchar会更加节省空间。
        
        ### notes: char是整数的一个子集,不包含小数点".";要用varchar；sqlite3.OperationalError: unrecognized token: "630008.OF"
        ### Notes:一定要新建主键，否则后续新增麻烦; url=http://www.zyiz.net/tutorial/detail-7594.html
        # notes：id主键在数据表Data里看不到

        ################################################################################
        ### 获取列信息  
        str_sql = "select * from " + table_name
        temp_data = self.cursor.execute( str_sql ).fetchall()    
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)
        df_data= pd.DataFrame( temp_data,columns= temp_cols )
        print("Tail of data ",df_data.tail(5).T )  
        obj_db["table_data"] = df_data

        ### 关闭Cursor:
        self.conn.commit()
        self.conn.close()   

        return obj_db
        
    def delete_table(self, obj_db):
        ################################################################################
        ### 删除表格部分列或整个表格，默认用drop模式全删
        # drop删除表结构和表数据，truncate删除表数据，delete删除某一行
        # 执行速度，一般来说: drop> truncate > delete
        # 也可以判断是否有数据表，有的话就执行删除命令，然后创建数据表db.execute("""
        # IF OBJECT_ID('数据表名称','U') IS NOT NULL      DROP TABLE 数据表名称
        # table_name = "ciss_exhi"+ "_" + "fund_analysis"
        db_name = obj_db["db_name"] # "db_funda.sqlite3"
        ########################################
        ### 连接到SQLite数据库 | path_db = C:\rc_202X\rc_202X\ciss_web 
        self.conn = sqlite3.connect( self.path_db  + db_name )
        self.cursor = self.conn.cursor()

        table_name = obj_db["table_name"]  
        ### 
        if "delete_type" in obj_db.keys() :
            ################################################################################
            ### 慎重！删除整个表格
            if obj_db["delete_type"] == "table" :
                check = input("慎重！删除整个表格 | Check if delete the whole table...")
                str_sql = "drop table "+table_name 
                print("str_sql \n",str_sql )
                self.cursor.execute( str_sql  ) 

        elif "col_list" in obj_db.keys() :
            ################################################################################
            ### delete rows that match given criterias
            # 例子 删除 ID 为 7 的客户：；sqlite> DELETE FROM COMPANY WHERE ID = 7;
            col_list = obj_db["col_list"]
            value_list = obj_db["value_list"]
            str_list = ""
            for j in range( len(col_list) ) : 
                ### 多个 col_name +  " like '% "+ select_value +" %' "
                str_list = col_list[j] +  " ='" + str(value_list[j])  + "' and "
            ###
            str_list = str_list[:-4]

            str_sql = "delete from "+table_name+" where " + str_list
            print("str_sql \n",str_sql )

            temp_data = self.cursor.execute( str_sql  ) 


        elif "col_name" in obj_db.keys() :
            ################################################################################
            ### delete rows that match only 1 criteria
            # 例子 删除 ID 为 7 的客户：；sqlite> DELETE FROM COMPANY WHERE ID = 7;
            col_name = obj_db["col_name"] 
            select_value = obj_db["select_value"]
            str_list = col_name +  " ='" + str(select_value )  + "' "
            str_sql = "delete from "+table_name+" where " + str_list
            print("str_sql \n", str_sql )

            temp_data = self.cursor.execute( str_sql  ) 

        ################################################################################
        ### 获取列信息  
        str_sql = "select * from " + table_name
        temp_data = self.cursor.execute( str_sql ).fetchall()    
        temp_cols = [i[0] for i in self.cursor.description  ]
        print("temp_cols \n", temp_cols)
        obj_db["df_data"] = pd.DataFrame( temp_data,columns= temp_cols )
        # print("Tail of data ",obj_db["df_data"].tail(5).T )    

        ################################################################################
        ### 获取列信息  
        # notes 删除表格后，就不能提取数据了。     
        ### 关闭Cursor:
        self.conn.commit()
        self.conn.close()   
                

        ################################################################################         
        ###  

        return obj_db


        
    def create_database(self, obj_db):
        ################################################################################
        ### 创建新数据库
        db_name = obj_db["db_name"]

        ### 例子：conn = sqlite3.connect("test.db") #连接数据库，若test.db不存在，则会创建该数据库
        self.conn = sqlite3.connect( self.path_db  + db_name +'.sqlite3')
        self.cursor = self.conn.cursor() 

        return obj_db