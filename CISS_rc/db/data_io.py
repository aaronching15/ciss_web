# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo
1，pd.read_csv时，可以用use_cols读取部分columns

功能：数据读写处理，主要基于Wind落地数据库：
1.2, class data_factor_model：子类：多因子模型相关
1.3, class_timing_abcd3d:管理市场和行业择时策略 ashares_timing_abcd3d.py
1.4,class data_wind:子类：管理来自Wind-API的数据

2,子目录：\\data_io\\
    2.1,因子模型：data_io_factor_model.py ;class data_factor_model() ;
    2.2，择时动量，市场状态：data_io_timing_abcd3d.py ;class data_timing_abcd3d();
    2.3，wind数据:data_io_wind.py;class data_wind()
    2.4,todo,组合数据：data_io_portfolio;

3，关联脚本：对应配置文件 | config\config_data.py

4,OUTPUT:
    1,obj_1["dict"]，字典信息,json
    1.1，把所有变量的中英文注释存在字典的"notes"里
    2,obj_1["df"]，表格信息,dataframe  

5,分析：目标是所有数据变量以object类型作为输入输出，其中主要是2个key:
    1,obj_1["dict"]:字典格式，数据io都采用json的字典格式。
    2,obj_1["df"]:DataFrame格式

6，Notes: 
6.1，输出文件统一保存至子目录 \\export\\
    refernce: rC_Data_Initial.py；similar with get_wind.py
date:last 200526 | since 180601
===============================================
'''
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
import pandas as pd
import numpy as np

#######################################################################
class data_io():
    def __init__(self):        
        # 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")
        from config_data import config_data
        config_data_1 = config_data()
        self.obj_config = config_data_1.obj_config
        # print( self.obj_config["dict"] )

    def print_info(self):        
        print("del_columns | 删除列值中包括特定字符的列")
        print("get_date_period | 给定交易日，获取历史区间日期")
        print("get_trading_days | 给定交易日，获取之前和之后的交易日列表")
        print("get_list_delist_day | 给定代码，获取上市日和退市日")
        print("get_trading_week | 给定交易周，获取之前和之后的周末交易日列表")
        print("get_trading_month | 给定交易月，获取之前和之后的月末交易日列表")
        print("get_stock_des_name_listday | 导入股票名称、上市日期等基础信息")
        print("get_report_date | 给定日期，获取最近的2~N个季度财务日期及所属第1~4个季度数量")
        print("get_report_date_fund| 给定日期，获取最近的2~N个季度基金持仓披露日期及所属第1~4个季度数量")
        print("get_period_pct_chg_codelist | 给定起止日期，获取股票代码对应的区间涨跌幅")
        print("get_after_ann_days | 获取区间内，3个财务披露日之后的交易日列表; 每年季度财务报告最晚披露日为0430,0731,1030。")
        print("get_after_ann_days_fund | 获取区间内，6个基金持仓披露日之后的交易日列表; [0131、0331、0430、0731、0830、1030]")

    def del_columns(self,df,col_str):
        ### 删除列值中包括特定字符的列，例如 col_str="Unnamed"
        col_list = df.columns.to_list()
        col_list_keep = [ x for x in col_list if not (type(x)== str and col_str in x) ]
        df = df.loc[:,col_list_keep]    

        return df
    
    def get_trading_days(self,obj_date) :
        ### 给定交易日，获取之前和之后的交易日列表
        temp_date = int( obj_date["date"] ) 
         
        # \db_wind\data_adj
        path_dates = self.obj_config["dict"]["path_dates"] 
        if "date_frequency" in obj_date.keys() :
            if  obj_date["date_frequency"] == "month" :
                df_date_ashares = pd.read_csv( path_dates+ self.obj_config["dict"]["file_date_month"] )
            elif  obj_date["date_frequency"] == "week" :
                df_date_ashares = pd.read_csv( path_dates+ self.obj_config["dict"]["file_date_week"] )
            elif  obj_date["date_frequency"] == "quarter" :
                df_date_ashares = pd.read_csv( path_dates+ self.obj_config["dict"]["file_date_quarter"] )

        else :
            ### 默认是交易日            
            df_date_ashares = pd.read_csv( path_dates+ self.obj_config["dict"]["file_date_tradingday"] )

        # date list        
        date_list= list( df_date_ashares["date"].values )
        date_list.sort()
        obj_date["date_list"]=date_list
        #notes: type is "numpy.int64"
        date_list_pre = [i for i in date_list if i <=temp_date]
        date_list_post = [i for i in date_list if i >temp_date ]
        obj_date["date_list_pre"]=date_list_pre
        obj_date["date_list_post"]=date_list_post  
        
        
        ###若有结束日期，获取区间的日期列表:季、月、周、默认日
        if "date_end" in obj_date.keys() :            
            ### 
            temp_date_end = int( obj_date["date_end"] )
            obj_date["date_list_period"] = [i for i in date_list_post if i <= temp_date_end ]
            
        
        return obj_date
    
    def get_list_delist_day(self,obj_date):
        ### 给定代码，获取上市日和退市日
        
        # 
        path_wind_adj = self.obj_config["dict"]["path_wind_adj"]
        # wds_ashares_list_delist.csv derived from \AShareEODPrices
        file_name = "wds_ashares_list_delist.csv"
        df_date_ashares = pd.read_csv( path_wind_adj+file_name ) 
        ### 只保存上市的A股
        df_date_ashares["filter"] = df_date_ashares["S_INFO_WINDCODE"].apply(lambda x : 1 if (len(x)==9 and x[0] in ["6","0","3"]) else 0 )
        df_date_ashares = df_date_ashares[ df_date_ashares["filter"]==1 ]
        col_list = ["S_INFO_WINDCODE","S_INFO_LISTDATE","S_INFO_DELISTDATE"]
        df_date_ashares = df_date_ashares.loc[:, col_list]

        ### 针对个股
        if "wind_code" in obj_date.keys() :
            temp_code = obj_date["wind_code"] 
            df_date_ashares_sub = df_date_ashares[df_date_ashares["S_INFO_WINDCODE"] ==temp_code ]
            obj_date["list_date"] = df_date_ashares_sub["S_INFO_LISTDATE"].values[0]
            obj_date["delist_date"] = df_date_ashares_sub["S_INFO_DELISTDATE"].values[0]

        ###输出所有个股的上市和退市日期            
        obj_date["df_list_delist_date"] = df_date_ashares 
        # "date"

        return obj_date

    def get_trading_week(self,obj_date) :
        ### 给定交易周，获取之前和之后的周末交易日列表
        temp_date = int( obj_date["date"] ) 
        # \db_wind\data_adj
        path_dates = self.obj_config["dict"]["path_dates"] 
        df_date_ashares = pd.read_csv( path_dates+ self.obj_config["dict"]["file_date_week"] )
        # "date"
        date_list= list( df_date_ashares["date"].values )
        #notes: type is "numpy.int64"
        date_list_pre = [i for i in date_list if i <=temp_date]
        date_list_post = [i for i in date_list if i >temp_date ]
        obj_date["date_list_pre"]=date_list_pre
        obj_date["date_list_post"]=date_list_post 

        return obj_date

    def get_trading_month(self,obj_date) :
        ### 给定交易月，获取之前和之后的月末交易日列表
        temp_date = int( obj_date["date"] ) 
        # \db_wind\data_adj
        path_dates = self.obj_config["dict"]["path_dates"] 
        df_date_ashares = pd.read_csv( path_dates+ self.obj_config["dict"]["file_date_month"] )
        # "date"
        date_list= list( df_date_ashares["date"].values )
        #notes: type is "numpy.int64"
        date_list_pre = [i for i in date_list if i <=temp_date]
        date_list_post = [i for i in date_list if i >temp_date ]
        obj_date["date_list_pre"]=date_list_pre
        obj_date["date_list_post"]=date_list_post 

        return obj_date


    def get_ind_date(self,object_ind ) :
        ### 给定交易日获取清单中股票的所属行业，基于已经计算好了的数据
        '''
        Part 1: 匹配全部行业分类数据，并保存至csv；
        object_ind["if_all_codes"] = 1 or 0 
        
        Part 2: 只匹配特定行业内的代码。
        若只对 object_ind["column_ind"] 设定的一种行业分类提取，则需要设置：
        object_ind["if_column_ind"]=1  ；object_ind["column_ind"] = "citics_ind_code_s_1"
        或者 object_ind["if_column_ind"] == "citics"

        last 200618 | since 200110
        notes:1，只能识别上海或深圳股票。
            1,if_all_codes="1" means directly import all code list from inside
            2,旧方法是先计算所有个股的全历史行业分类文件，文件较大且效率低；200321改成从梳理过的股票行业分类进出表df_ind_code_stock_io.csv中判断
            file_name = "df_ind_code_stock_io.csv"
            file_path = "C:\db_wind\data_adj\industries_class"
            3，600087.SH于2014年退市，对应行业分类只有sw，并且纳入日期20080602，却是2005年的指数成分；对于这种情况，将其归类于 其他 行业分类
            4，date_start should be "20150331"
            5，如果没有行业分类数据怎么办？  
        derived from def get_ind_date at transform_wind_wds.py；def get_ind_period()
        '''

        ### 导入变量、对象、参数 || object_ind ={}
        code_list = object_ind["code_list"]
        date_end = object_ind["date_end"]
        if_all_codes = object_ind["if_all_codes"]
        
        path_adj_ind = self.obj_config["dict"]["path_wind_adj"] + "industries_class\\"

        ###########################################################################################
        ### Part 1: 匹配全部行业分类数据，并保存至csv
        ### code_list 要剔除非上市交易状态的A股，如"T00018.SH"
        list_del = []
        for temp_code in code_list : 
            if temp_code[-3:] not in [".SH",".SZ"]:
                list_del.append( temp_code )
            elif temp_code[0] not in ["6","3","0"]:
                list_del.append( temp_code )
        for j1 in list_del : 
            code_list.remove(j1) 
        
        ###########################################################################################
        ### 导入基础信息
        if if_all_codes == "1" :
            # df with column as code 
            code_df = pd.read_csv(self.file_path_admin + "code_list.csv" )            
            code_list = list( code_df["code"].values )
            ### code_list 要剔除非A股 
            list_del = []
            for temp_code in code_list : 
                if temp_code[-3:] not in [".SH",".SZ"]:
                    list_del.append( temp_code )
                elif temp_code[0] not in ["6","3","0"]:
                    list_del.append( temp_code )
            for j1 in list_del : 
                code_list.remove(j1) 
        
        ###########################################################################################
        ### 判断是否已经有该行业分类文件，如果有则导入；如果文件内包括所需股票代码，则停止
        if_exist_data_ind = 0 
        file_name = "df_code_list_"+ str(date_end) + ".csv" 
        print("file_name ", file_name )
        if os.path.exists(path_adj_ind+file_name ) :
            
            df_ind =pd.read_csv (path_adj_ind + file_name ,encoding="gbk") 
            # 先计算股票代码长度，
            # if len(df_ind.index) >= len( code_list ):
            # 判断个股是否都在df_ind里
            list_df_ind = list( df_ind["wind_code"].values )
            list_sub =[x for x in code_list if x in  list_df_ind ]
            if len(list_sub) == len(code_list):
                print("Including same number of codes ",len(list_sub) ,len(code_list))
                
                object_ind["df_s_ind_out"] = df_ind
                df_s_ind_out =object_ind["df_s_ind_out"] 
                if_exist_data_ind = 1 
            else :
                print("Not the same number of codes ",len(list_sub) ,len(code_list) )
                if len(list_sub) /len(code_list) >= 0.98 :
                    if_exist_data_ind = 1 
                    object_ind["df_s_ind_out"] = df_ind
                    df_s_ind_out =object_ind["df_s_ind_out"] 

        ###########################################################################################
        ### 若if_exist_data_ind不为1，则需要逐个代码计算，并保存至csv
        if not if_exist_data_ind == 1 :
            
            ### 导入梳理过的股票行业分类进出表df_ind_code_stock_io.csv
            file_ind_io_name = "df_ind_code_stock_io.csv"
            
            df_ind_code_stock_io = pd.read_csv( path_adj_ind + file_ind_io_name,encoding="gbk" )
            print(path_adj_ind + file_ind_io_name )

            ### for all codes
            count_stock =0 
            for code_stock in code_list :
                print("Working on code_stock",count_stock, code_stock)
                df_ind_code_stock_io_sub = df_ind_code_stock_io[ df_ind_code_stock_io["wind_code"]==code_stock ]
                
                # print("date_end ", date_end )
                ### 匹配日期 ，date_end（type是str）要和"ENTRY_DT"（type=float）比较
                # 纳入日期"ENTRY_DT"按升序排列
                df_ind_code_stock_io_sub = df_ind_code_stock_io_sub.sort_values(by="ENTRY_DT") 
                # print("df_ind_code_stock_io_sub ",df_ind_code_stock_io_sub.head().T )
                ### 由于不同行业分类纳入时间不一定一样，因此要依次剔除含NaN的列
                ###########################################################################################
                ### notes:纳入和剔除通常只有2种情况，"REMOVE_DT"有值和无值：
                # 1,中信  citics_ind_name_1, citics_ind_code_s_1=1
                df_ind_code_stock_io_sub_citics =df_ind_code_stock_io_sub[ df_ind_code_stock_io_sub["citics_ind_code_s_1"]>0 ]
                
                df_ind_code_stock_io_sub1 = df_ind_code_stock_io_sub_citics[ df_ind_code_stock_io_sub_citics["ENTRY_DT" ]<= float(date_end) ]
                
                if len(df_ind_code_stock_io_sub1.index)>0  :
                    temp_index= df_ind_code_stock_io_sub1.index[-1 ] 
                    # transfrom from series to Dataframe
                    df_out = df_ind_code_stock_io_sub1.loc[temp_index,:].to_frame().T
                    # 设置固定index值，避免后续赋值给其他index
                    temp_index_fix = temp_index
                else :
                    print("df_ind_code_stock_io_sub |citics",df_ind_code_stock_io_sub.T )
                    ### 为了后续计算，需要对df_out及其列temp_index_fix初始化
                    temp_index_fix = 0 
                    df_out = pd.DataFrame(index=[temp_index_fix], columns= df_ind_code_stock_io_sub_citics.columns )

                ###########################################################################################
                # 2,Wind,gics  "wind_ind_name_1","wind_ind_name_2", wind_ind_code_1=1
                df_ind_code_stock_io_sub_gics =df_ind_code_stock_io_sub[ df_ind_code_stock_io_sub["wind_ind_code_1"]>0 ]
                df_ind_code_stock_io_sub2 = df_ind_code_stock_io_sub_gics[ df_ind_code_stock_io_sub_gics["ENTRY_DT" ]<= float(date_end) ]
                
                col_list = ["wind_ind_code_4","wind_ind_code_s_4","wind_ind_name_4","wind_used_4"]
                col_list = col_list + ["wind_ind_code_3","wind_ind_code_s_3","wind_ind_name_3","wind_used_3"]
                col_list = col_list + ["wind_ind_code_2","wind_ind_code_s_2","wind_ind_name_2","wind_used_2"]
                col_list = col_list + ["wind_ind_code_1","wind_ind_code_s_1","wind_ind_name_1","wind_used_1"]
                if len(df_ind_code_stock_io_sub2.index)>0  :
                    temp_index= df_ind_code_stock_io_sub2.index[-1 ] 
                    for item in col_list  :
                        # "wind_ind_code_3
                        df_out.loc[ temp_index_fix ,item] =  df_ind_code_stock_io_sub.loc[temp_index,item]
                else :
                    print("df_ind_code_stock_io_sub |gics",df_ind_code_stock_io_sub.T )
                    for item in col_list  : 
                        df_out.loc[ temp_index_fix ,item] =  np.nan
                
                ###########################################################################################
                # 3 申万 sw, "sw_ind_name_1",sw_ind_code_1
                df_ind_code_stock_io_sub_sw =df_ind_code_stock_io_sub[ df_ind_code_stock_io_sub["sw_ind_code_1"]>0 ]
                df_ind_code_stock_io_sub3 = df_ind_code_stock_io_sub_sw[ df_ind_code_stock_io_sub_sw["ENTRY_DT" ]<= float(date_end) ]
                
                col_list = ["sw_ind_code_3","sw_ind_code_s_3","sw_ind_name_3","sw_used_3"]
                col_list = col_list + ["sw_ind_code_2","sw_ind_code_s_2","sw_ind_name_2","sw_used_2"]
                col_list = col_list + ["sw_ind_code_1","sw_ind_code_s_1","sw_ind_name_1","sw_used_1"]
                if len(df_ind_code_stock_io_sub3.index)>0  :
                    temp_index= df_ind_code_stock_io_sub3.index[-1 ] 
                    for item in col_list   :
                        df_out.loc[ temp_index_fix ,item] =  df_ind_code_stock_io_sub.loc[temp_index,item]

                else :
                    print("df_ind_code_stock_io_sub |sw",df_ind_code_stock_io_sub.T )
                    
                ### notes:对于港股和美股，会出现无法识别的情况；判断 df_out 是否存在
                if "df_out" in locals() or "df_out" in globals() :
                    ### we need ind_list=["wind_ind_name_1","wind_ind_name_2","citics_ind_name_1","sw_ind_name_1"]
                    if count_stock == 0 :
                        # df_out 是 Series ;pandas series to df, df_out.to_frame() || df_s_ind_out = df_out.to_frame().T  
                        df_s_ind_out = df_out
                        count_stock =1  
                    else :
                        df_s_ind_out = df_s_ind_out.append( df_out, ignore_index=True )
                else :
                    print("No matching industrial class for code_list" )
            
            ### drop duplicates 600085.SH 会出现相同index的情况
            df_s_ind_out =df_s_ind_out.drop_duplicates( ["wind_code"] )

            ### save to csv
            if "df_s_ind_out" in locals() or "df_s_ind_out" in globals() :
                ### save df to specific directory 
                df_s_ind_out.to_csv(path_adj_ind + "df_code_list_"+ str(date_end) + ".csv",index=False ,encoding="gbk")  
                ### Initialize output industry object             
                object_ind["df_s_ind_out"] = df_s_ind_out
                object_ind["temp_index"] = temp_index

        ###########################################################################################
        ### Part 2: 只匹配特定行业内的代码。
        ####################################################################################
        ### Case1:根据给定的行业分类column值,对个股列表构建df_s_ind_sub；例如中信一级行业分类"citics_ind_code_s_1"，
        if object_ind["if_column_ind"] == 1 :
            temp_col = object_ind["column_ind"]
            df_s_ind_sub = pd.DataFrame(code_list,columns=["wind_code"], index=code_list  )
            df_s_ind_sub ["ind_code"] = 0 
            for temp_code in code_list :
                df_sub = df_s_ind_out[ df_s_ind_out["wind_code"] ==temp_code ]
                if len( df_sub.index ) > 0 :
                    df_s_ind_sub.loc[temp_code, "ind_code" ] = df_sub[temp_col].values[0]
            #
            object_ind["df_s_ind_sub"] = df_s_ind_sub

        ####################################################################################
        ### Case2: 将所有citicis代码赋值给df，且"ind_code"为一级行业 
        '''citics_ind_code_s_1	citics_ind_code_s_2	citics_ind_code_s_3	
        citics_ind_name_1	citics_ind_name_2	citics_ind_name_3        '''
        col_list_ind=["citics_ind_code_s_1","citics_ind_code_s_2","citics_ind_code_s_3","citics_ind_name_1","citics_ind_name_2","citics_ind_name_3"]
        if object_ind["if_column_ind"] == "citics" :
            df_s_ind_sub = pd.DataFrame(code_list,columns=["wind_code"], index=code_list  )
            df_s_ind_sub ["ind_code"] = 0 
            
            for temp_code in code_list :
                df_sub = df_s_ind_out[ df_s_ind_out["wind_code"] ==temp_code ]
                if len( df_sub.index ) > 0 :
                    for temp_col in col_list_ind :
                        df_s_ind_sub.loc[temp_code, temp_col ] = df_sub[temp_col].values[0] 
                    # ind_code
                    temp_col = "citics_ind_code_s_1"
                    df_s_ind_sub.loc[temp_code, "ind_code" ] = df_sub[temp_col].values[0]


        # save to output
        object_ind["col_list_ind"] = col_list_ind
        object_ind["df_s_ind_sub"] = df_s_ind_sub
        # 逐渐从 df_s_ind_sub 改名为 df_s_ind
        object_ind["df_s_ind"] = df_s_ind_sub

        return object_ind 

    def get_stock_des_name_listday(self, obj_stock_des={} ):
        ### 导入股票名称、上市日期等基础信息
        table_name = "AShareDescription"
        file_name = "WDS_full_table_full_table_ALL.csv"
        df_stock_des = pd.read_csv( self.obj_config["dict"]["path_wind_wds"]+ table_name+"\\" + file_name)
        col_list = ["S_INFO_WINDCODE","S_INFO_NAME","S_INFO_LISTBOARDNAME","S_INFO_LISTDATE","S_INFO_DELISTDATE"]
        ### 缩小范围
        ### 1,仅需要沪深股票 "6","0","3"
        df_stock_des["filter"] = df_stock_des["S_INFO_WINDCODE"].apply(lambda str1: 1 if str1[0] in ["6","0","3"] else 0 )
        df_stock_des = df_stock_des [df_stock_des["filter"] ==1 ] 
        df_stock_des = df_stock_des.loc[:,col_list]
        # 17       300830.SZ        N金现代                  创业板       20200506.0                NaN
                
        obj_stock_des["col_list_stock_des"] = col_list
        obj_stock_des["df_stock_des"] = df_stock_des

        return obj_stock_des

    def get_report_date(self,obj_date):
        ### 给定日期，获取最近的2~N个季度上市公司财务日期及所属第1~4个季度数量；
        ### 设定用于不同财务季度之间的计算:para_date:当季度数值、上一季度数值
        # 关于数据：如果是第三季度S_FA_ROE是前3个季度的合计roe，如果是Q2择时前2个季度合计roe，需要用para_list前2项计算
        print("Debug ====1 " )
        temp_date = obj_date["date"]       
        str_year = temp_date[:4]
        str_year_pre = str(int(str_year)-1)
        str_year_pre2 = str(int(str_year)-2)
        # notes:降序排列
        mmdd_list_q = ["1231","0930","0630", "0331"] 
        quarter_end_list =[]
        quarter_end_list_pre = []
        quarter_end_list_pre2 = []
        for mmdd in mmdd_list_q :
            quarter_end_list  = quarter_end_list + [str_year + mmdd ]
            quarter_end_list_pre  = quarter_end_list_pre + [str_year_pre + mmdd ]
            quarter_end_list_pre2 = quarter_end_list_pre2 + [str_year_pre2 + mmdd ]

        para_date =[ ]

        ### 获取最近的2个财务季度日期
        if int(temp_date[-4:]) <= 430 :
            ### q2,q3 in previous year 
            date_q = str_year_pre + "0930"
            date_q_pre = str_year_pre + "0630"
            para_date =[3,2 ]
        elif int(temp_date[-4:]) <= 831 :
            ### q4 in previous year and q1 in current year 
            date_q = str_year + "0331"
            date_q_pre = str_year_pre + "1231"
            para_date =[1,4 ]
        elif int(temp_date[-4:]) <= 1031 :
            ### q1,q2 in current year 
            date_q = str_year + "0630"
            date_q_pre = str_year + "0331"
            para_date =[2,1 ]
        else :
            ### q2,q3 in current year 
            date_q = str_year + "0930"
            date_q_pre = str_year + "0630"
            para_date =[3,2 ]
        # print("date_q ",temp_date,date_q,date_q_pre  )
        obj_date["list_para_date"] = para_date
        obj_date["date_q"] = date_q
        obj_date["date_q_pre"] = date_q_pre

        ########################################################################
        ### 默认取最近8个季度和5个年度的日期
        # obj_date["N_quarter"] = 8 obj_date["N_year"] = 5  
        if "N_quarter" in obj_date.keys() : 
            ### notes:date_list_q_N 降序排列
            date_list_q_N = []
            if obj_date["N_quarter"] == 8 : 
                ### 构建过去8个季度
                if int(temp_date[-4:]) <= 430 :
                    ### q2,q3 in previous year 
                    para_date_N =[3,2,1,4,3,2,1,4 ]
                    date_list_q_N = quarter_end_list[1:] + quarter_end_list_pre + [ quarter_end_list_pre2[0] ]

                elif int(temp_date[-4:]) <= 831 :
                    ### q4 in previous year and q1 in current year 
                    para_date_N =[1,4,3,2,1,4,3,2 ]
                    date_list_q_N = [ quarter_end_list[3] ]+ quarter_end_list_pre + quarter_end_list_pre2[:3]

                elif int(temp_date[-4:]) <= 1031 :
                    ### q1,q2 in current year 
                    para_date_N =[2,1,4,3,2,1,4,3 ]
                    date_list_q_N = quarter_end_list[2:] + quarter_end_list_pre + quarter_end_list_pre2[:2]

                else :
                    ### q2,q3 in current year 
                    para_date_N =[3,2,1,4,3,2,1,4 ]
                    date_list_q_N = quarter_end_list[1:] + quarter_end_list_pre + [ quarter_end_list_pre2[:1] ]

            # print("date_q ",temp_date,date_q,date_q_pre  )
            obj_date["para_date_N"] = para_date_N
            obj_date["date_list_q_N"] = date_list_q_N
        
        ### 获取过去N年年末
        if "N_year" in obj_date.keys() :

            date_list_y_N = []
            for i in range( obj_date["N_quarter"]  ) :
                ### N=5 means: 0,1,2,3,4
                date_list_y_N = date_list_y_N+ [ str(int(str_year)-1-i )+"1231" ] 
            ### notes:date_list_q_N 降序排列
            obj_date["date_list_y_N"] = date_list_y_N


        return obj_date
    
    def get_report_date_fund(self,obj_date):
        ### 给定日期，获取最近的2~N个季度基金持仓披露日期及所属第1~4个季度数量；
        ''' 1,设定用于不同财务季度之间的计算:para_date:当季度数值、上一季度数值
        2,关于数据：如果是第三季度S_FA_ROE是前3个季度的合计roe，如果是Q2择时前2个季度合计roe，需要用para_list前2项计算
        3,前1~2年（4、8个季度）时间,选有披露全部持仓的季末:date_q_pre_1y,date_q_pre_2y
        '''
        
        temp_date = obj_date["date"]       
        
        mmdd_list_ann =    [ 131 , 331, 430, 731,830,1031]
        mmdd_list_report = [1231 ,1231, 331, 630,630, 930]

        para_date =[ ]
        ### 获取最近的2个基金数据披露季度日期
        if int(temp_date[-4:]) <= 331 :
            ### q4,q3
            date_q = str(int(temp_date[:4])-1) + "1231"
            date_q_pre = str(int(temp_date[:4])-1) + "0930"
            date_q_pre2 = str(int(temp_date[:4])-1) + "0630"
            para_date =[4,3,2,1 ]
            ### 前1~2年（4、8个季度）时间,选有披露全部持仓的季末
            date_q_pre_1y = str(int(temp_date[:4])-2) + "1231"
            date_q_pre_2y = str(int(temp_date[:4])-3) + "1231"
            # 最近一个披露全部持仓的季度
            date_q_6or12m = str(int(temp_date[:4])-1) + "1231"

        elif int(temp_date[-4:]) <= 430 :
            ### q1,q4
            date_q = str(int(temp_date[:4])-1) + "1231"
            date_q_pre = str(int(temp_date[:4])-1) + "0930"
            date_q_pre2 = str(int(temp_date[:4])-1) + "0630"
            para_date =[1,4,3,2 ]
            ### 前1~2年（4、8个季度）时间,选有披露全部持仓的季末
            date_q_pre_1y = str(int(temp_date[:4])-2) + "1231"
            date_q_pre_2y = str(int(temp_date[:4])-3) + "1231"
            # 最近一个披露全部持仓的季度
            date_q_6or12m = str(int(temp_date[:4])-1) + "1231"

        elif int(temp_date[-4:]) <= 830 :
            ### q2,q1 in current year 
            date_q = str(int(temp_date[:4])) + "0630"
            date_q_pre = str(int(temp_date[:4])) + "0331"
            date_q_pre2 = str(int(temp_date[:4])-1) + "1231"
            para_date =[2,1,4,3 ]
            ### 前1~2年（4、8个季度）时间,选有披露全部持仓的季末
            date_q_pre_1y = str(int(temp_date[:4])-1) + "0630"
            date_q_pre_2y = str(int(temp_date[:4])-2) + "0630"
            # 最近一个披露全部持仓的季度
            date_q_6or12m = str(int(temp_date[:4])) + "0630"

        else :
            ### q3,q2 in current year 
            date_q = str(int(temp_date[:4])) + "0930"
            date_q_pre = str(int(temp_date[:4])) + "0630"
            date_q_pre2 = str(int(temp_date[:4])) + "0331"            
            para_date =[3,2,1,4 ]
            ### 前1~2年（4、8个季度）时间,选有披露全部持仓的季末
            date_q_pre_1y = str(int(temp_date[:4])-1) + "1231"
            date_q_pre_2y = str(int(temp_date[:4])-2) + "1231"
            # 最近一个披露全部持仓的季度
            date_q_6or12m = str(int(temp_date[:4])) + "0630"

        ### save to obj_date
        obj_date["date_quarter_pastN"] = para_date
        obj_date["date_q"] = date_q
        obj_date["date_q_pre"] = date_q_pre
        obj_date["date_q_pre2"] = date_q_pre2
        ### 前1~2年（4、8个季度）时间,选有披露全部持仓的季末
        obj_date["date_q_pre_1y"] = date_q_pre_1y
        obj_date["date_q_pre_2y"] = date_q_pre_2y
        # 最近一个披露全部持仓的季度
        obj_date["date_q_6or12m"] = date_q_6or12m  

        return obj_date

    def get_period_pct_chg_codelist(self,obj_data ):
        ### 给定起止日期，获取股票代码对应的区间涨跌幅
        '''input:obj_data["dict"] ,obj_data["df_ashare_ana"] '''
        df_ashare_ana = obj_data["df_ashare_ana"]

        obj_date={}
        obj_date["date"] = obj_data["dict"]["date_start"]
        obj_date["date_end"] = obj_data["dict"]["date_end"]
        ###获取对应的交易日 
        obj_date=  self.get_trading_days(obj_date)
        
        if len( obj_date["date_list_period"] ) > 0 :
            date_start = obj_date["date_list_period"][0]
            date_end = obj_date["date_list_period"][-1]
            # print( "date_start ",date_start,"date_end ",date_end )
            
            ### 读取期初调整收盘价
            path_data = self.obj_config["dict"]["path_wind_wds"] + "AShareEODPrices\\"
            file_name = "WDS_TRADE_DT_"+ str(int(date_start)) +"_ALL.csv"
            df_wds= pd.read_csv(path_data+file_name ,encoding ="gbk")
            for temp_i in df_ashare_ana.index :
                temp_code = df_ashare_ana.loc[temp_i,  "S_INFO_WINDCODE"]  
                # find code in df_wds
                df_wds_sub = df_wds[ df_wds[ "S_INFO_WINDCODE"] ==temp_code ]
                if len(df_wds_sub.index) > 0 :
                    df_ashare_ana.loc[temp_i,  "adjclose_start"] =df_wds_sub["S_DQ_ADJCLOSE"].values[0] 
            
            ### 读取期末调整收盘价
            file_name = "WDS_TRADE_DT_"+ str(int(date_end)) +"_ALL.csv"
            df_wds= pd.read_csv(path_data+file_name ,encoding ="gbk")
            for temp_i in df_ashare_ana.index :
                temp_code = df_ashare_ana.loc[temp_i,  "S_INFO_WINDCODE"] 
                # find code in df_wds
                df_wds_sub = df_wds[ df_wds[ "S_INFO_WINDCODE"] ==temp_code ]
                if len(df_wds_sub.index) > 0 :
                    df_ashare_ana.loc[temp_i,  "adjclose_end"] =df_wds_sub["S_DQ_ADJCLOSE"].values[0] 
                    if df_ashare_ana.loc[temp_i,  "adjclose_start"] > 0 :
                        ### 计算区间涨跌幅
                        df_ashare_ana.loc[temp_i,  "period_pct_chg"]= df_ashare_ana.loc[temp_i,  "adjclose_end"]/df_ashare_ana.loc[temp_i,  "adjclose_start"]-1
            ###
        obj_data["df_ashare_ana"]=df_ashare_ana 

        return obj_data

    def get_after_ann_days(self,obj_date) :
        ### 获取区间内，3个财务披露日以及之后的交易日列表; 每年季度财务报告最晚披露日为0430,0731,1030。
        #notes:若开始时间20070101，则需要能获得之后的最近一期开始算 
        mmdd_list_ann =  [430,830,1031]

        date_start = float(obj_date["dict"]["date_start"] )
        date_end = float( obj_date["dict"]["date_end"] )
        
        year_start = int(obj_date["dict"]["date_start"][:4])
        mmdd_start = int(obj_date["dict"]["date_start"][4:])
        year_end =  int(obj_date["dict"]["date_end"][:4])
        mmdd_end =  int(obj_date["dict"]["date_end"][4:])

        ### 导入历史交易日            
        path_dates = self.obj_config["dict"]["path_dates"] 
        df_date_ashares = pd.read_csv( path_dates+ self.obj_config["dict"]["file_date_tradingday"] )
        # date list        
        date_list= list( df_date_ashares["date"].values )
        date_list.sort()
        #notes: type is "numpy.int64"
        # Notes：date_list_period是开始和结束期间的所有交易日
        date_list_post = [i for i in date_list if i >= date_start ]
        date_list_period = [i for i in date_list_post if i <= date_end ]
        date_list_period.sort()
        ###  
        date_list_after_ann =[]
        date_list_report = []
        ### 获取第一个配置日期之前的2个季度披露日期
        temp_list_1st = []
        for mmdd in mmdd_list_ann :
            temp_list_1st = temp_list_1st +[ year_start*10000 + mmdd ]
        temp_list_1st = [ x for x in temp_list_1st if x<= min(date_list_period) ]
        if len(temp_list_1st) > 0 :
            date_list_after_ann =[ min(date_list_period) ]
            date_list_report = [ max(temp_list_1st) ]
        
        ### 获取第二个配置日期开始至最后一期
        for temp_year in range(year_start,year_end+1 ) :
            ### get for 0430,0731,1030
            for mmdd in mmdd_list_ann :
                temp_date = temp_year*10000 + mmdd
                # print(temp_date,min(date_list_period) ,max(date_list_period)  )
        
                # Notes：date_list_period是开始和结束期间的所有交易日
                # 对于第一个交易日，需要获取之前的2个季度，即temp_date< min(date_list_period)
                ### 需要确保 temp_date 不早于给定日期区间初期，不晚于区间末期
                if temp_date >= min(date_list_period) and temp_date <= max(date_list_period) :
                    # notes:必须大于，不能等于，因为可能是当日晚上披露
                    temp_list = [i for i in date_list_period if i > temp_date ]
                    
                    if len( temp_list ) > 0  :
                        date_list_after_ann =date_list_after_ann + [ temp_list[0] ] 
                        date_list_report = date_list_report +[ temp_date ] 
                        # print( date_list_after_ann )
        ### Save to obj
        obj_date["dict"]["date_list_period"] =  date_list_period
        obj_date["dict"]["date_list_after_ann"] = date_list_after_ann
        # 财务报告日期
        obj_date["dict"]["date_list_report"] = date_list_report
        return obj_date

    def get_after_ann_days_fund(self,obj_date):
        ### 关于日期变量的说明
        ### date_list_past 开始日期之前的所有历史交易日
        ### date_list_period 所有历史交易日
        ### date_list_after_ann 披露日后第一个交易日 |[20060703, 20060801, 20060831, 20061101
        ### date_list_before_ann 披露日前最后一个交易日 | [20060703, 20060731, 20060830,
        ### ### date_list_report 季末财务报告日期| [20060430, 20060731, 20060830, 20061031
        ### 获取区间内，6个基金持仓披露日之后的交易日列表; [0131、0331、0430、0731、0830、1031]
        # notes:同 get_after_ann_days()
        # date_list_before_ann 未被测试。
        #############################################################################
        ### mmdd_list是最晚披露日期，mmdd_list_report是对应的季末财务报告日期
        mmdd_list_ann =    [ 131 , 331, 430,731,830,1031]
        mmdd_list_report = [1231 ,1231, 331,630,630, 930]
        ### if_only_quarter= 1,只抓取4个季度披露日后的日期，默认是4+2，季度和半年度
        if obj_date["dict"]["if_only_quarter"] == 1 :
            mmdd_list_ann =    [ 131 ,430,731,1031]
            mmdd_list_report = [1231 ,331,630,930]
        
        date_start = float(obj_date["dict"]["date_start"] )
        date_end = float( obj_date["dict"]["date_end"] )
        
        year_start = int(obj_date["dict"]["date_start"][:4])
        mmdd_start = int(obj_date["dict"]["date_start"][4:])
        year_end =  int(obj_date["dict"]["date_end"][:4])
        mmdd_end =  int(obj_date["dict"]["date_end"][4:])

        #############################################################################
        ### 导入历史交易日            
        path_dates = self.obj_config["dict"]["path_dates"] 
        df_date_ashares = pd.read_csv( path_dates+ self.obj_config["dict"]["file_date_tradingday"] )
        # date list        
        date_list= list( df_date_ashares["date"].values )
        date_list.sort()
        # date_list_past时开始日期之前的所有历史交易日
        date_list_past = [i for i in date_list if i < date_start ]
        #notes: type is "numpy.int64"
        # Notes：date_list_period是开始和结束期间的所有交易日
        date_list_post = [i for i in date_list if i >= date_start ]
        date_list_period = [i for i in date_list_post if i <= date_end ]
        date_list_period.sort()

        #############################################################################
        ### 输入季报披露year和mmdd，获得季度末year和 mmdd
        def get_quarter_end(year,mmdd) :
            
            if mmdd == 131 :
                date_r = (year-1)*10000 + 1231
            if mmdd == 331 :
                date_r = (year-1)*10000 + 1231
            if mmdd == 430 :
                date_r = (year)*10000 + 331
            if mmdd == 731 :
                date_r = (year)*10000 + 630
            if mmdd == 830 :
                date_r = (year)*10000 + 630
            if mmdd == 1031 :
                date_r = (year)*10000 + 930
            
            return date_r 


        #############################################################################
        ### 披露日后第一个交易日要和最晚披露日、对应的报告的季度末日期对应
        # 3个list：date_list_after_ann、date_list_report、date_list_report
        date_list_after_ann =[]
        date_list_before_ann =[]
        date_list_report = []
        ### 季报结束日
        date_list_quarter_end =[]

        ### 获取第一个配置日期之前的2个季度,有可能对应前一年
        # 用于匹配交易日前最近的2个披露日和季度财报日
        temp_list_1st = []
        for mmdd in mmdd_list_ann :
            temp_list_1st = temp_list_1st +[ (year_start-1)*10000 + mmdd ]
        for mmdd in mmdd_list_ann :
            temp_list_1st = temp_list_1st +[ year_start*10000 + mmdd ]

        temp_list_1st = [ x for x in temp_list_1st if x<= min(date_list_period) ] 
        if len(temp_list_1st) > 0 :
            date_list_after_ann =[ min(date_list_period) ]
            # 季度末披露前的第一个交易日
            date_list_before_ann =[ min(date_list_period) ]
            
            #######################################
            ### 每个季报结束日：需要匹配年份
            date_list_report = [ max(temp_list_1st) ]
            temp_year = int( max(temp_list_1st)) // 10000 
            temp_mmdd = int( max(temp_list_1st)) % 10000  
            date_list_quarter_end = [ get_quarter_end(temp_year,temp_mmdd) ]

            ### notes:date_list_report是最晚披露日 和 date_list_quarter_end是季度末。
        
        #######################################
        ### 获取第二个配置日期开始至最后一期
        for temp_year in range(year_start,year_end+1 ) :
            #######################################
            ### 每个季报披露日的list
            for mmdd in mmdd_list_ann :
                temp_date = temp_year*10000 + mmdd
                # print(temp_date,min(date_list_period) ,max(date_list_period)  )
        
                # Notes：date_list_period是开始和结束期间的所有交易日
                # 对于第一个交易日，需要获取之前的2个季度，即temp_date< min(date_list_period)
                ### 需要确保 temp_date 不早于给定日期区间初期，不晚于区间末期
                if temp_date >= min(date_list_period) and temp_date <= max(date_list_period) :
                    # notes:必须大于，不能等于，因为可能是当日晚上披露
                    temp_list = [i for i in date_list_period if i > temp_date ]
                    temp_list_before = [i for i in date_list_period if i <= temp_date ]
                    if len( temp_list ) > 0  :
                        #######################################
                        ### 披露日之后
                        date_list_after_ann =date_list_after_ann + [ temp_list[0] ] 
                        #######################################
                        ### 披露日
                        date_list_report = date_list_report +[ temp_date ] 
                        if len(temp_list_before) > 0 :
                            #######################################
                            ### 披露日之前：季度末披露前的第一个交易日,必须和date_list_after_ann一一对应
                            date_list_before_ann = date_list_before_ann + [ temp_list_before[-1] ] 
                        #######################################
                        ### 每个季报结束日：需要匹配年份
                        date_list_quarter_end = date_list_quarter_end + [ get_quarter_end( temp_year, mmdd  ) ] 

        ##############################################################################
        ### 将日期数据合并成df
        df_date= pd.DataFrame([date_list_before_ann,date_list_after_ann,date_list_report,date_list_quarter_end] )
        df_date=df_date.T
        df_date.columns=["before_ann","after_ann","report","quarter_end"]
        obj_date["df_date"] = df_date 
        ##############################################################################
        ### Save to obj
        # date_list_past时开始日期之前的所有历史交易日
        obj_date["dict"]["date_list_past"]  =date_list_past
        obj_date["dict"]["date_list_post"]  =date_list_post
        ###
        obj_date["dict"]["date_list_period"] =  date_list_period
        obj_date["dict"]["date_list_after_ann"] = date_list_after_ann
        obj_date["dict"]["date_list_before_ann"] = date_list_before_ann
        # 财务报告日期
        obj_date["dict"]["date_list_report"] = date_list_report
        # 季报结束日
        obj_date["dict"]["date_list_quarter_end"] = date_list_quarter_end
        
        
        return obj_date

        

#######################################################################
### notes:开始迁移至 data_io\\
class data_factor_model():
    def __init__(self):
        #######################################################################
        ### 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")
        from config_data import config_data_factor_model
        config_data_1 = config_data_factor_model()
        self.obj_config = config_data_1.obj_config

        self.data_io_1 = data_io()
        #######################################################################
        ### factor_model 目录位置：
        # self.obj_config["dict"]["path_factor_model"] = dict_config["path_ciss_db"] +"factor_model\\"
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        #######################################################################
        ### 导入日期list，日、周、月 | date_list_tradingday.csv  ...     
        file_name_month = self.obj_config["dict"]["file_date_month"]    
        # file_name_tradingday = self.obj_config["dict"]["file_date_tradingday"] 
        # file_name_week = self.obj_config["dict"]["file_date_week"]   

        df_date_month = pd.read_csv(self.obj_config["dict"]["path_dates"] + file_name_month )
        date_list_month = df_date_month["date"].values

        date_list_month.sort()
        # 日期升序排列
        self.obj_data_io = {}
        self.obj_data_io["dict"] = {}
        self.obj_data_io["dict"]["date_list_month"] = date_list_month


    def print_info(self):        
        print("   ")
        print("import_data_wds |导入wds表格列表和对应的公布日期列 keyword_anndate ")
        ### 多因子模型数据I/O
        print("import_data_opt |导入单一时期优化模型需要的数据，obj_para包括了基准指数和最新日期等信息 ")
        print("export_data_opt |输出优化模型结果和重要过程数据  ")

        ### 导入指标和因子数据I/O
        print("import_data_factor |导入已经下载的指标和因子数据 ")
        print("export_data_factor |输出已经下载的指标和因子数据 ")
        
        print("export_data_1factor |输出单因子分组模型结果和重要过程数据 ")

        ### 导入财务分析和财务模型相关指标I/O
        print("import_data_financial_ana |导入财务分析和财务模型相关指标 ")
        print("export_data_financial_ana |输出财务分析和财务模型相关指标 ")

        return 1

    def import_data_wds(self, obj_in ) :
        ### 导入wds表格列表和对应的公布日期列 keyword_anndate
        ########################################################################
        ### 导入表格列表
        file_name = obj_in["dict"]["file_name_columns_wds"] # "list_financial_columns_wds.csv"
        # C:\ciss_web\CISS_rc\apps\active_bm
        # path_name = self.obj_config["dict"]["path_active_bm"]
        path_name = self.obj_config["dict"]["path_apps"] + obj_in["dict"]["path_name_columns_wds"] 
        # including chinese char
        df_cols = pd.read_csv( path_name + file_name,encoding="gbk" )

        # print("df_cols \n " , df_cols.head() )

        table_list = list(df_cols["table_name"].drop_duplicates() )
        '''  ['AShareFinancialIndicator', 'AShareProfitExpress', 'AShareProfitNotice', 
        'AShareTTMAndMRQ', 'AShareTTMHis', 'AIndexConsensusData', 'AIndexConsensusRolling', 
        'AShareConsensusindex', 'AShareConsensusRollingData']
        '''
        # print(" table_list\n " ,  table_list )
        ########################################################################
        ### 对每个表格，导入关键词分类 "keyword_anndate"
        file_4_keyword_anndate = "log_data_wds_tables.csv"
        df_keyword_anndate = pd.read_csv( self.obj_config["dict"]["path_rc_data"] + file_4_keyword_anndate,encoding="gbk" )
        df_keyword_anndate =df_keyword_anndate[df_keyword_anndate["name_table"].isin(table_list)]
        print( df_keyword_anndate  )

        # notes: AShareTTMAndMRQ 是没有特定的anndate
        # for temp_table in table_list :
        #     print(temp_table )
        #     temp_keyword_anndate = df_keyword_anndate[ df_keyword_anndate["name_table"]==temp_table ]["keyword_anndate"].values[0]
        #     print( temp_keyword_anndate )

        ########################################################################
        ### 导入月末日期列表,一般默认从200501开始
        date_list_month = self.obj_data_io["dict"]["date_list_month"]
        # 'numpy.int64'> type(date_list_month[0])
        date_list_month_050101 = [ t for t in date_list_month if t >= 20050101 ]
        
        obj_in["dict"]["df_fi_cols"] = df_cols
        obj_in["dict"]["table_list_fi"] = table_list
        obj_in["dict"]["df_keyword_anndate"] = df_keyword_anndate
        obj_in["dict"]["date_list_month_050101"] = date_list_month_050101
        notes = "df_fi_cols,wds财务相关表格列表分类;table_list_fi，表格列表；df_keyword_anndate,用于匹配的日期csv文件的列关键词；date_list_month_050101，2005年开始的月末交易日。"
        obj_in["dict"]["notes"] = notes
        

        return obj_in

    def import_data_opt(self,obj_para):
        ### 导入单一时期优化模型需要的数据，obj_para包括了基准指数和最新日期等信息
        
        ########################################################################
        code_index = obj_para["dict"]["code_index"]
        # 20060228是第一次有factor weight的月份
        date_last_month = obj_para["dict"]["date_last_month"]

        ### 设定变量和导入数据，从factor_model\\文件夹获取
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        # 日期升序排列
        date_list_month = self.obj_data_io["dict"]["date_list_month"]
        # print("date_list_month ",date_list_month)
        path_factor_model_sub = path_factor_model+ code_index+ "\\"

        ########################################################################
        ### 给定日期，导入相关数据，20151231时count_month=6了，
        # 导入t时期指数成分 w_b=w_stock_bm,df_factor_weight，导入t+1，下个月末20160131时的股票收益率 ret_stock_change_np
        # notes:假定factor_weight_np第一行factor_weight_np[0] 对应的是每个股票在市值因子上的暴露
        # date_list_month_pre 是已经计算过的月份
        date_list_month_pre = [date for date in date_list_month if date<= date_last_month ]
        # date_list_month_pre 是根据最新日期还未计算的月份
        date_list_month = [date for date in date_list_month if date> date_last_month ]

        count_month = len(date_list_month_pre )
        # 取最后一个日期
        temp_date = date_list_month_pre[-1]
        temp_date_pre = date_list_month_pre[-2]
        if count_month > 6 :
            temp_date_pre_6m = date_list_month_pre[-6]
        else :
            temp_date_pre_6m = date_list_month_pre[0]

        ########################################################################
        ### 导入指数成分 df_index_consti

        from analysis_indicators import indicator_ashares,analysis_factor
        indicator_ashares_1 = indicator_ashares()
        analysis_factor_1 = analysis_factor()

        obj_in_index={} 
        obj_in_index["date_start"] = obj_para["dict"]["date_last_month"]
        obj_in_index["code_index"] = obj_para["dict"]["code_index"]
        obj_in_index["table_name"] = "AIndexHS300FreeWeight"

        obj_out_index = indicator_ashares_1.ashares_index_constituents(obj_in_index) 
        # code_list = obj_out_index["df_ashares_index_consti"]["S_CON_WINDCODE"].values
        # 获取指数成分股、权重
        df_index_consti = obj_out_index["df_ashares_index_consti"].loc[:, ["S_CON_WINDCODE","I_WEIGHT","TRADE_DT" ] ]
        ### 注意，将wind_code 按升序排列
        df_index_consti = df_index_consti.sort_values(by="S_CON_WINDCODE"  )
        print("df_index_consti   \n", df_index_consti.head().T )

        ### 指数权重成分 w_index_consti ,np.array | notes: "I_WEIGHT"的值7.5等，需要除以100
        w_index_consti = df_index_consti["I_WEIGHT"].values /100
        print("w_index_consti ", type(w_index_consti ) ) 

        ###########################################################################
        ### 仅保留沪深300成分股，保留仅属于本期的项目
        code_list_csi300 = list( df_index_consti[ "S_CON_WINDCODE"].values )

        ########################################################################
        ### 导入因子权重矩阵 factor_weight_np,df_factor_weight_20060228_000300.SH.csv
        '''
        因子列表：由factor_weight_np和len_factor决定
        Factor list: [
        'zscore_S_DQ_MV', 流通市值标准分
        'f_w_ic_ir_S_VAL_PCF_OCFTTM', 经营性现金流
        'f_w_ic_ir_amt_ave_1m_6m', 过去1个月和6个月的平均成交金额比
        'f_w_ic_ir_close_pct_52w', 收盘价在52周价格区间百分比
        'f_w_ic_ir_ep_ttm', 市盈率倒数
        'f_w_ic_ir_ma_20d_120d', 20天和120天均线比值
        'f_w_ic_ir_ret_accumu_20d', 20天累计收益率
        'f_w_ic_ir_ret_accumu_20d_120d', 20天和120天累计收益率比值
        'f_w_ic_ir_ret_alpha_ind_citic_1_120d', 相对于中信一级行业的120天相对收益
        'f_w_ic_ir_ret_alpha_ind_citic_1_20d', 相对于中信一级行业的20天相对收益
        'f_w_ic_ir_ret_averet_ave_20d_120d', 20天和120天平均收益率比值
        'f_w_ic_ir_ret_mdd_20d', 20天最大回撤
        'f_w_ic_ir_ret_mdd_20d_120d', 20天和120天最大回撤比值
        'f_w_ic_ir_roe_ttm', 净资产收益率
        'f_w_ic_ir_turnover_ave_1m_6m', 20天和120天换手率比值
        'f_w_ic_ir_volatility_std_1m_6m'，20天和120天波动率比值 ]
        '''
        file_name_output= "df_factor_weight_" +str( date_last_month) +"_"+ code_index +".csv"
        df_factor_weight  = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )
        df_factor_weight = df_factor_weight[ df_factor_weight["date"]== date_last_month ]

        df_factor_weight = df_factor_weight[ df_factor_weight["wind_code"].isin(code_list_csi300 ) ]
        
        print("df_factor_weight_",len(df_factor_weight ) ) 
        # input1= input("Check to proceed......  ")
        if len(df_factor_weight ) > len(code_list_csi300) :
            asd
        # df_factor_weight to factor_weight_np
        # a.remove(value):删除列表a中第一个等于value的值；a.pop(index):删除列表a中index处的值；del(a[index]):删除列表a中index处的值
        # sub step 1:wind_code 升序排列：
        df_factor_weight = df_factor_weight.sort_values(by="wind_code")
        # sub step 2:剔除非因子值；因子icir不包括市值，需要先导入！
        col_list = list( df_factor_weight.columns.values ) 
        col_list.remove( "wind_code" )
        col_list.remove( "date" )
        print("col_list",col_list )

        # col_list 是有因子权重数据的代码列表，col_list_csi300是当期指数成分股
        ######################################################################## 
        ### 导入总市值因子、中信一级行业和其他因子，原始指标等 | "zscore_S_DQ_MV" | df_factor_20060228_000300.SH_20060228.csv
        file_name_output= "df_factor_" +str( date_last_month) +"_"+ code_index +"_"+str( date_last_month) +".csv"
        try:
            temp_df_factor = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )
        except:
            # notes:20191129开始后缀没有日期了
            file_name_output= "df_factor_" +str( date_last_month) +"_"+ code_index +".csv"
            temp_df_factor = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )
        
        ######################################################################## 
        ### 设置因子列表col_list ；第一行要设置为市值因子; notes: szcore不是ic_ir,可能会有点问题。
        col_list= [ "zscore_S_DQ_MV" ] + col_list
        '''第一个流通市值zscore_S_DQ_MV值在-0.59到7之间，均值0.0，这个对我们来说没有必要进行限制
        第一个必须是这个值.
        notes:200506选出以下9个因子:
        'zscore_S_DQ_MV', 流通市值标准分;'f_w_ic_ir_close_pct_52w', 收盘价在52周价格区间百分比
        'f_w_ic_ir_ep_ttm', 市盈率倒数;'f_w_ic_ir_ma_20d_120d', 20天和120天均线比值
        'f_w_ic_ir_ret_accumu_20d', 20天累计收益率；'f_w_ic_ir_ret_mdd_20d', 20天最大回撤
        'f_w_ic_ir_roe_ttm', 净资产收益率；'f_w_ic_ir_S_VAL_PCF_OCFTTM', 经营性现金流
        'f_w_ic_ir_turnover_ave_1m_6m', 20天和120天换手率比值;
        '''
        col_list= ['zscore_S_DQ_MV','f_w_ic_ir_close_pct_52w','f_w_ic_ir_ep_ttm','f_w_ic_ir_ma_20d_120d']
        col_list= col_list+['f_w_ic_ir_ret_accumu_20d','f_w_ic_ir_ret_mdd_20d','f_w_ic_ir_roe_ttm' ]
        col_list= col_list+['f_w_ic_ir_S_VAL_PCF_OCFTTM','f_w_ic_ir_turnover_ave_1m_6m' ]
        
        ### 用于组合优化设置的其他指标值,df_4opt,col_list_4opt
        '''ret_accumu_20d，ret_accumu_120d，20天和120天累计收益
        ret_alpha_ind_citic_1_120d: 120天相对于中信一级行业的收益
        ret_mdd_20d，ret_mdd_20d_mad
        ret_mdd_120d,这个没计入标准因子，只有原始指标值
        ret_mdd_20d_120d：20天内最大回撤比120天内最大回撤 =  (1+ret_mdd_20d)/((1+ret_mdd_120d)
        '''
        col_list_4opt = ["ret_accumu_20d","ret_accumu_120d","ret_mdd_20d","ret_mdd_120d"]
        
        ###
        for temp_i in df_factor_weight.index :
            temp_code = df_factor_weight.loc[temp_i, "wind_code" ]
            # find code and "zscore_S_DQ_MV" in temp_df_factor
            # print("temp_code ",temp_code )
            temp_df_factor_sub = temp_df_factor[ temp_df_factor["wind_code"]==temp_code ]
            if len( temp_df_factor_sub.index ) > 0 :
                temp_j = temp_df_factor_sub.index[0]
                df_factor_weight.loc[ temp_i, "zscore_S_DQ_MV" ] = temp_df_factor.loc[temp_j, "zscore_S_DQ_MV"  ]
                for temp_col in col_list_4opt:
                    df_factor_weight.loc[ temp_i, temp_col ] = temp_df_factor.loc[temp_j, temp_col ]
            else :
                print("No record for code ", temp_code  )
                df_factor_weight.loc[ temp_i, "zscore_S_DQ_MV" ] = -1
                for temp_col in col_list_4opt:
                    df_factor_weight.loc[ temp_i, temp_col ] = 0.0
        ### df填充空值,取较小值
        df_factor_weight = df_factor_weight.fillna(-0.00001 )

        df_4opt = df_factor_weight.loc[:,col_list_4opt ]
        df_4opt.index = df_factor_weight["wind_code"]

        ### 和其他因子
        df_factor_weight_values = df_factor_weight.loc[:,col_list ]
        df_factor_weight_values.index = df_factor_weight["wind_code"]
        # print( "df_factor_weight_values"  )
        # example,from 300*16 to 16*300,shape of factor_weight_np 16*300
        factor_weight_np = df_factor_weight_values.T.values

        len_factor = np.size(factor_weight_np,0 )

        # 把 factor_weight_np 中nan替换为较低值 -0.5
        # where_are_NaNs = np.isnan(d)  >>> d[where_are_NaNs] = 0
        where_nan = np.isnan( factor_weight_np ) 
        factor_weight_np[ where_nan] = -0.5 

        print("Shape of factor_weight_np" ,factor_weight_np.shape )

        # 中信一级行业 || df_ind_code 是代码升序排列的行业分类
        df_ind_code = temp_df_factor[ temp_df_factor["date"]==date_last_month ]  
        df_ind_code = df_ind_code.loc[:, ["wind_code","citics_ind_code_s_1"] ]  
        df_ind_code = df_ind_code.sort_values(by="wind_code")

        ########################################################################
        ### 2,ret_stock_change_np 当月的收益率| 给定日期，导入指数成分股、权重和当月的收益率
        obj_in_index={} 
        obj_in_index["date_pre"] = temp_date_pre
        obj_in_index["date"] = temp_date
        obj_in_index["df_change"] = df_index_consti
        obj_in_index["df_change"]["wind_code"] = obj_in_index["df_change"]["S_CON_WINDCODE"]
        # print("df_index_consti" ,obj_in_index["df_change"].head()  ) 
        obj_in_index = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )

        ret_stock_change_np =  obj_in_index["df_change"]["s_change_adjclose"].values

        ### ret_stock_change_6m_np,近120天收益率;temp_date_pre_6m
        obj_in_index["date_pre"] = temp_date_pre_6m
        obj_in_index = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )
        ret_stock_change_6m_np =  obj_in_index["df_change"]["s_change_adjclose"].values

        ########################################################################
        ### 构建行业暴露矩阵ind_code_np和上下限ind_lb,ind_ub;ind_code_np矩阵取值都是1或0，其中每一行对应一个行业，
        # 每一行与最优权重变量相乘得到最优组合在某一行业的权重，ind_lb[i]对应了第i个行业的权重下限
        # ind_ub[i]对应第i个行业的权重上限。ind_bm:基准组合的行业权重。
        # 从 df_factor_20060228.csv 里获取行业分类 "citics_ind_code_s_1"
        # df_ind_code.columns ["wind_code","citics_ind_code_s_1"]
        # notes: df_ind_code一般多少有几个code是空值，np.nan,也应该给与一定的权重。例如200602的600087.SH长油。
        # notes:ind_code_list数值从小到大排列，最后一个可能是nan
        ind_code_list = list( df_ind_code["citics_ind_code_s_1" ].drop_duplicates() )
        ind_code_list.sort()
        print("ind_code_list",ind_code_list ) 

        len_stock = len(df_ind_code.index )
        ind_code_np = np.zeros( (len(ind_code_list),len_stock ) )

        df_ind_code_m = pd.DataFrame(ind_code_np ,index=ind_code_list,columns = df_ind_code["wind_code"] )

        for temp_i in df_ind_code.index :
            temp_code = df_ind_code.loc[temp_i, "wind_code"] 
            temp_ind_code = df_ind_code.loc[temp_i, "citics_ind_code_s_1"] 
            df_ind_code_m.loc[temp_ind_code,temp_code] = 1 

        print("ind_code_np from df_ind_code_m"  )
        # from df to np.arrays 
        ind_code_np = df_ind_code_m.values

        ########################################################################
        ### 构建个股权重矩阵 w_stock_np，对于N个股票，N*N矩阵，每行仅对角线1个值为1，其余为0
        w_stock_np = np.zeros( (len_stock ,len_stock ) )
        for temp_i in range( len_stock ) :
            w_stock_np[temp_i][temp_i] = 1 

        print("w_stock_np")

        ########################################################################
        ### 保存变量到相关对象
        obj_out = obj_para
        obj_out["date_list_month_pre"] = date_list_month_pre
        obj_out["date_list_month"] = date_list_month
        obj_out["df_index_consti"] = df_index_consti
        obj_out["w_index_consti"] = w_index_consti
        obj_out["code_list_csi300"] = code_list_csi300
        obj_out["col_list"] = col_list
        obj_out["factor_weight_np"] = factor_weight_np
        obj_out["len_factor"] = len_factor
        obj_out["df_ind_code"] = df_ind_code
        obj_out["ret_stock_change_np"] = ret_stock_change_np
        obj_out["ret_stock_change_6m_np"] = ret_stock_change_6m_np
        obj_out["ind_code_list"] = ind_code_list
        obj_out["len_stock"] = len_stock
        obj_out["ind_code_np"] = ind_code_np
        obj_out["w_stock_np"] = w_stock_np
        obj_out["df_4opt"] = df_4opt
        obj_out["col_list_4opt"] = col_list_4opt
        # 单因子回测用
        obj_out["df_factor_weight"] = df_factor_weight

        return obj_out


    def export_data_opt(self,obj_perf_eval, obj_opt) :
        ### 输出优化模型结果和重要过程数据
        '''obj_opt 中包括了优化模型结果和计算过程的各类信息
        1,优化模型结果和输出信息result：res.fun,success,x
        2，优化模型参数para:
        3,优化模型输入变量input:
        4,优化模型输入变量参数input_para:

        notes:
        obj_para 包括了重要的输入参数入指数代码、日期等。
        obj_opt["dict"] == obj_para["dict"]
        obj_perf_eval["df_perf_eval"]，每一期的组合未来1m和6m收益
        obj_perf_eval[ 20060228 ]："df_"+ str(temp_index),当期组合列表
        obj_opt["dict"]["id_output"] 指的是输出文件夹目录名称

        '''
        ########################################################################
        ### 设置文件命名和导出路径等
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        # 日期升序排列
        date_last_month = obj_opt["dict"]["date_last_month"]
        # print("date_last_month ",date_last_month)
        path_factor_model_sub = path_factor_model+ obj_opt["dict"]["code_index"] + "\\"

        path_export = path_factor_model_sub +"\\export\\"+obj_opt["dict"]["id_output"]+ "\\"
        if not os.path.exists( path_export  ) :
            os.mkdir( path_export  )
        print("path_export: ",path_export  )
        ########################################################################
        '''obj_opt主要内容：
        1，"dict":字典，包括["code_index"]，["date_last_month"]等；
        2，"date_list_month_pre"：2005年以来到给定交易日的月份list
        2，"date_list_month" ：给定交易日到最新交易日例如202003的月份list
        2，"df_index_consti": df,指数成分
        3,"w_index_consti":array
        4,"code_list_csi300":list,
        5,"col_list": column list ,
        6,"factor_weight_np"
        7,"len_factor";8,"df_ind_code"
        9,"ret_stock_change_np"
        10,"ret_stock_change_6m_np"
        11,"ind_code_list"
        12~15,opt_model:"cons","fun","bnds","w_init"
        16,"res":{fun,jac,... }    
            jac：返回梯度向量的函数,array
            message: 'Positive directional derivative for linesearch'
            nfev: 27355
            nit: 92
            njev: 88
            status: 8
            success: False
            x: array()

        1,优化模型结果和输出信息result：res.fun,success,x
        2，优化模型参数para:
        3,优化模型输入变量input:
        4,优化模型输入变量参数input_para:
        '''
        import json
        
        ########################################################################
        ### 整合数据字典信息 obj_opt["dict"] ||把所有变量的中英文注释存在字典的"notes"里
        # list to string || string to list, list(str1 )
        obj_opt["dict"]["col_list_str"]  = ",".join( obj_opt["col_list"] )
        obj_opt["dict"]["len_factor"] = obj_opt["len_factor"] # 16个

        ## notes obj_opt["ind_code_list"]都是float类型，如 12.0，nan等
        # ind_code_list = []
        # for item in obj_opt["ind_code_list"]:
        #     try :
        #         ind_code_list = ind_code_list +[str(int(item) )]
        #     except :
        #         ind_code_list = ind_code_list +[ "nan" ]
        
        # # 直接用 ",".join( obj_opt["ind_code_list"]) 会出问题 
        # obj_opt["dict"]["ind_code_list"] = ",".join( ind_code_list )

        obj_opt["dict"]["keys_opt_model"] = "Keys in obejct obj_opt:cons,bnds,w_init,obj_fun"
        ### 1,优化模型结果和输出信息result：res.fun,res.success,res.message,x等 
        obj_opt["dict"]["keys_result"] = "Keys in obejct obj_opt.res: fun,message,success,x,status,nfev,nit,njev"
        # # TypeError: Object of type function is not JSON serializable
        # obj_opt["dict"]["result_x"] = obj_opt["res"]["x"]

        str_notes = ""
        str_notes = str_notes + "前缀keys_xxxx对应变量xxxx的主要keys；"
        str_notes = str_notes + "前缀opt_model_对应优化模型设置；前缀result_对应模型计算结果，其中fun是最小化的目标方程值，x是最优配置权重按股票代码升序排列,success对应是否找到最优解。"
        obj_opt["dict"]["notes"] = str_notes 

        # notes:TypeError: Object of type int64 is not JSON serializable；因为里边有int64，np.nan等
        # ans：先str()变成字符串 ||还没试过的方法二：直接把dict直接序列化为json对象）加上 cls=NpEncoder,data就可以正常序列化了
        file_name = "temp_obj_opt.json"
        with open( path_export+ file_name,"w+") as f :
            json.dump( str(obj_opt["dict"]) ,f  )
        print("Dict data saved in Json file \n",path_export )

        ### save dataframe to csv
        # 多期组合绩效
        temp_index = obj_opt["dict"]["date_last_month"]
        file_name ="df_perf_eval.csv"
        obj_perf_eval["df_perf_eval"].to_csv( path_export+ file_name,index=False)
        # 单期个股收益
        file_name ="df_"+ str(temp_index)+".csv"
        obj_perf_eval["df_"+ str(temp_index)].to_csv( path_export + file_name,index=False)
        # 单期中信行业收益
        file_name ="df_ind_"+ str(temp_index)+".csv"
        obj_perf_eval["df_ind_"+ str(temp_index)].to_csv( path_export + file_name,index=False)

        return obj_opt


    def import_data_factor(self,obj_port):
        ### 导入已经下载的指标和因子数据
        '''
        path = D:\db_wind\data_adj\ashare_ana\\
        file = ADJ_timing_TRADE_DT_20200522_ALL.csv
        '''
        import datetime as dt 
        ### parameters
        temp_date = obj_port["dict"]["date"] 
        ### 设定组合类型，例如成长内行业轮动、市场内大小流通市值轮动、不同行业成长和价值轮动
        # "market","value_growth","industry","mixed"=所有分组都考虑
        group_type = obj_port["dict"]["group_type"] 

        ### 导入T日股票类的分析指标
        path_ashare_ana = self.obj_config["dict"]["path_wind_adj"] + "ashare_ana\\"
        file_ashare_ana = "ADJ_timing_TRADE_DT_"+ str(temp_date ) +"_ALL.csv"
        obj_port["dict"]["path_ashare_ana"] = path_ashare_ana

        try :
            df_ashare_ana = pd.read_csv( path_ashare_ana+file_ashare_ana,encoding="gbk"  )
        except :
            df_ashare_ana = pd.read_csv( path_ashare_ana+file_ashare_ana)

        obj_port["df_ashare_ana"] =df_ashare_ana

        ### 剔除上市不足60天的股票；假设公司上市20天后价格恢复正常，因此T日时上市应满20+40天。
        ### 导入个股上市时间
        # ["S_INFO_WINDCODE","S_INFO_NAME","S_INFO_LISTBOARDNAME","S_INFO_LISTDATE","S_INFO_DELISTDATE"]
        # 17,300830.SZ,N金现代,创业板,20200506.0,NaN
        df_stock_des = obj_port["df_stock_des"]  
        col_list_stock_des = obj_port["col_list_stock_des"]  

        for temp_i in df_ashare_ana.index :
            temp_code = df_ashare_ana.loc[temp_i,"S_INFO_WINDCODE" ]
            # temp_date 是统一的，不需要获取， df_ashare_ana.loc[temp_i,"TRADE_DT" ]
            # 差temp_code的上市日期
            df_stock_des_sub = df_stock_des[ df_stock_des["S_INFO_WINDCODE"]==temp_code ]
            if len(df_stock_des_sub.index) > 0 :
                listdate = df_stock_des.loc[ df_stock_des_sub.index[0], "S_INFO_LISTDATE" ]
                # 20200506.0
                temp_date_dt = dt.datetime.strptime( str(temp_date) ,"%Y%m%d" )
                listdate_dt = dt.datetime.strptime( str(int(listdate)) ,"%Y%m%d" )
                # print( temp_date_dt ,listdate_dt,  temp_date_dt-listdate_dt   )
                if temp_date_dt-listdate_dt >= dt.timedelta(days=60):
                    df_ashare_ana.loc[temp_i,"filter"] = 1 
            else :
                print(temp_code  )
        # 以20060105为例，股票数量从1350变成1349，少了一个上市不足60天的股票 000043.SZ
        df_ashare_ana =df_ashare_ana[ df_ashare_ana["filter"]==1 ]

        obj_port["df_ashare_ana"] =df_ashare_ana
        
        ### 导入T日市场状态
        path_abcd3d = self.obj_config["dict"]["path_ciss_db"] +  "timing_abcd3d\\"
        path_market_ana = path_abcd3d +  "market_status_group\\"
        obj_port["dict"]["path_abcd3d"] = path_abcd3d
        obj_port["dict"]["path_market_ana"] = path_market_ana

        file_market_ana = "abcd3d_market_ana_trade_dt_" +str(temp_date ) + ".csv"
        try :
            df_market_ana = pd.read_csv( path_market_ana+file_market_ana,encoding="gbk" )
        except :
            df_market_ana = pd.read_csv( path_market_ana+file_market_ana )

        obj_port["df_market_ana"] =df_market_ana
                
        return obj_port

    def export_data_factor(self,obj_port):
        ### 输出已经下载的指标和因子数据

        ### ���置输出文件夹
        port_name = obj_port["dict"]["port_name"] # "rc01"
        port_id = obj_port["dict"]["port_id"] # "200523"
        # 加权方式：mvfloat=市值加权,ew=等权重,growth = 成长加权,value=价值加权
        weighting_type = obj_port["dict"]["weighting_type"]
        # 定义股票池限定的范围
        sp_column = obj_port["dict"]["sp_column"] # ""
        ### 设定组合类型，例如成长内行业轮动、市场内大小流通市值轮动、不同行业成长和价值轮动
        # "market","value_growth","industry","mixed"=所有分组都考虑
        group_type = obj_port["dict"]["group_type"] # "industry"
        
        dir_export = port_name +"_" + port_id+ "_"+weighting_type +"_" + group_type +"_" + str(obj_port["dict"]["len_rebalance"] ) +"\\"
        path_export = obj_port["dict"]["path_abcd3d"] + dir_export
        if not os.path.exists( path_export  ) :
            os.mkdir( path_export  )
        print("path_export: ",path_export  )

        ### save obj_port["df_port_weight"] to csv as portfolio weight 
        file_export = "df_port_weight_" + str(obj_port["dict"]["date"]) + ".csv"
        obj_port["df_port_weight"].to_csv(path_export+file_export,index=False,encoding="gbk")
        obj_port["dict"]["path_export"] = path_export

        return obj_port

    def export_data_1factor(self,obj_perf_eval,obj_port):
        ### 输出单因子分组模型结果和重要过程数据
        ########################################################################
        ### 设置文件命名和导出路径等
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        # 日期升序排列
        date_last_month = obj_port["date"]
        # 增加因子名称
        path_factor_model_sub = path_factor_model+ obj_port["dict"]["code_index"]+"_"+ obj_port["dict"]["1factor"] + "\\"
        if not os.path.exists( path_factor_model_sub ) :
            os.mkdir( path_factor_model_sub )
        path_export = path_factor_model_sub +"\\export\\"
        if not os.path.exists( path_export  ) :
            os.mkdir( path_export  )
        print("path_export: ",path_export  )

        ########################################################################
        ### save dataframe to csv
        # 多期组合绩效
        temp_index = date_last_month
        file_name ="df_perf_eval.csv"
        obj_perf_eval["df_perf_eval"].to_csv( path_export+ file_name,index=False)
        # 单期个股收益
        file_name ="df_"+ str(temp_index)+".csv"
        obj_perf_eval["df_"+ str(temp_index)].to_csv( path_export + file_name,index=False) 

        return obj_perf_eval

    def import_data_financial_ana(self,obj_data):
        ### 导入财务分析和财务模型相关指标
        '''input :obj_data["dict"]["date_adj_port"]
        output:返回季度日期数据: obj_data["dict"]["date_q"]  
            obj_data["dict"]["date_q_pre"]; obj_data["df_ashare_ana"]   
        notes:为了避免股票上市初期大幅上涨的异常，需要剔除上市不足40天的股票
        '''
        data_io_1 = data_io()
        col_str = "Unnamed"
        ########################################################################
        ### step1 导入日期、行业、个股数据
        obj_date = {}
        obj_date["date"] = obj_data["dict"]["date_adj_port"]        
        temp_date = obj_data["dict"]["date_adj_port"]
                
        path_ashare_ana = self.obj_config["dict"]["path_wind_adj"]+"ashare_ana\\"
        file_ashre_ana_date = "ADJ_timing_TRADE_DT_" + temp_date + "_ALL.csv"
        try :
            df_ashare_ana = pd.read_csv(path_ashare_ana +file_ashre_ana_date ,encoding="gbk" )
        except :
            df_ashare_ana = pd.read_csv(path_ashare_ana +file_ashre_ana_date  )
        
        df_ashare_ana = data_io_1.del_columns( df_ashare_ana,col_str)

        ########################################################################
        ### 判断是否单一行业"single_industry",主要保存简码和中文名称
        if "single_industry" in obj_data["dict"].keys() :
            df_ashare_ana = df_ashare_ana[ df_ashare_ana["ind_code"] == float( obj_data["dict"]["single_industry"] ) ]
            ### 导入中信二级行业 
            from db_assets.transform_wind_wds import transform_wds
            transform_wds_1 = transform_wds()
            ### 给定交易日获取清单中股票的所属行业，基于已经计算好了的数据
            code_list = df_ashare_ana[ "S_INFO_WINDCODE"].to_list()
            if_all_codes="1"
            object_ind = transform_wds_1.get_ind_date( code_list, obj_data["dict"]["date_adj_port"] ,if_all_codes )
            df_s_ind_out = object_ind["df_s_ind_out"]
            col_list_temp = ["citics_ind_code_s_3","citics_ind_code_s_2","citics_ind_code_s_1"]
            col_list_temp = col_list_temp +["citics_ind_name_1","citics_ind_name_2","citics_ind_name_3"  ]
            for temp_i in df_ashare_ana.index :
                temp_code = df_ashare_ana.loc[temp_i, "S_INFO_WINDCODE"]
                # find ind in object_ind["df_s_ind_out"]
                df_temp = df_s_ind_out[ df_s_ind_out["wind_code"]==temp_code ]
                if len( df_temp.index ) > 0 :
                    for temp_col in col_list_temp :
                        df_ashare_ana.loc[temp_i, temp_col] = df_temp[temp_col].values[0]
            ### 把ind_code 用中信二级行业 "citics_ind_code_2" 代替
            df_ashare_ana["ind_code"] = df_ashare_ana["citics_ind_code_s_2"]

        ########################################################################
        ### notes:为了避免股票上市初期大幅上涨的异常，需要剔除上市不足40天的股票
        # col_list = ["S_INFO_WINDCODE","S_INFO_LISTDATE","S_INFO_DELISTDATE"]
        obj_date = self.data_io_1.get_list_delist_day(obj_date)
        df_list_date = obj_date["df_list_delist_date"]   
        df_ashare_ana["filter"] = 0 
        # obj_date["df_list_delist_date"]         
        for temp_i in df_ashare_ana.index :
            temp_code = df_ashare_ana.loc[temp_i, "S_INFO_WINDCODE"]
            ### 计算个股上市日期
            df_sub = df_list_date[ df_list_date["S_INFO_WINDCODE"]==temp_code ]
            if len( df_sub.index )> 0 : 
                listing_dates =  float(temp_date) - df_sub["S_INFO_LISTDATE"].values[0]
                if listing_dates >=40 : 
                    df_ashare_ana.loc[temp_i, "filter"] = 1 
                else :
                    print("New stock? ", temp_code,temp_date, df_sub["S_INFO_LISTDATE"].values[0],listing_dates )
            
        df_ashare_ana = df_ashare_ana[ df_ashare_ana["filter"] == 1 ]
        df_ashare_ana = df_ashare_ana.drop("filter" ,axis=1 )
        
        ########################################################################
        ### 获取最近的2个财务披露日期

        obj_date = self.data_io_1.get_report_date(obj_date)    
        para_date = obj_date["list_para_date"] 
        date_q = obj_date["date_q"] 
        date_q_pre = obj_date["date_q_pre"] 
        
        ########################################################################
        ### step2 导入各类财务指标，若有则直接使用，若没有则按照分类从data_wds目录下获取
        '''
        指标列表：
        序号，简称，算法，对应指标，对应指标所在表格，重要参数列
        1，季度roe_ttm, 前2个季度的S_FA_ROE,S_FA_ROE,中国A股财务指标AShareFinancialIndicator，报告期，REPORT_PERIOD=20060930
        2，季度收入环比变动，{}，{营业收入同比增长率(%) S_FA_YOY_OR
        单季度.营业总收入同比增长率(%) S_QFA_YOYGR
        单季度.营业总收入环比增长率(%) S_QFA_CGRGR
        单季度.营业收入同比增长率(%) S_QFA_YOYSALES
        单季度.营业收入环比增长率(%) S_QFA_CGRSALES}，中国A股财务指标AShareFinancialIndicator，报告期，REPORT_PERIOD=20060930
        3，季度净利润扣非增速，{}，{单季度.净利润同比增长率(%) S_QFA_YOYPROFIT
        单季度.净利润环比增长率(%) S_QFA_CGRPROFIT}，中国A股财务指标AShareFinancialIndicator，报告期，REPORT_PERIOD=20060930
        4，季度盈利持续性,ROIC,"S_FA_ROIC",环比增长率,用ROIC替代-环比必须增加，总资产净利率,S_FA_ROA
            notes:永辉超市常年roic在4.7-12.2之间，2015年开始低于8.0；妙可蓝多在1.42~2.96之间；
            立讯精密10.02-19.93；北方华创3.8-6.89；
            201806圣农发展roic跌到历史记录地低3.83之后到年底反弹，股价在18年11月披露的3季报增长到8.41，股价开始起飞，到次年4-30时已经没有什么超额收益了。
            notes:圣农发展在20180213的FY1的预期净利润就9.78e多，之后慢慢增加到20180831的10.8e，再到181101的11.34e.
        5，季度资产质量：近2季度经营性现金流/总资产 or 经营性现金流/总负债;
            经营活动产生的现金流量净额/营业收入,S_FA_OCFTOOR;经营活动产生的现金流量净额/经营活动净收益,S_FA_OCFTOOPERATEINCOME
            单季度.经营活动产生的现金流量净额／营业收入,S_QFA_OCFTOSALES;单季度.经营活动产生的现金流量净额／经营活动净收益,S_QFA_OCFTOOR
            发展性指标(知识、资金、行业景气)：研发投入、员工增长；负债或股东权益增加；行业内收入增加
        6，估值：PE_FY1 <= 60，50 或 PE_FY1 <=,45，40；或者用同业前35%
        数据处理：
        1，对AShareFinancialIndicator表要按特定规则转换数据

        notes:1,未来每个行业要适用不同的指标参数，例如超市等消费者服务行业可能就不适合用roic指标。
        idea：从AShareFinancialIndicator表格里获取每个交易日所有股票的最近6个季度和5个年度的财务数据。
        '''
        ### 中国A股财务指标AShareFinancialIndicator，报告期，REPORT_PERIOD=20060930
        path = "D:\\db_wind\\data_wds\\"
        table_name = "AShareFinancialIndicator"
        file_name = "WDS_REPORT_PERIOD_" + date_q + "_ALL.csv" 
        df_fi_indi = pd.read_csv(self.obj_config["dict"]["path_wind_wds"]+table_name+"\\"+ file_name   )
        file_name = "WDS_REPORT_PERIOD_" + date_q_pre + "_ALL.csv"
        df_fi_indi_q_pre = pd.read_csv(self.obj_config["dict"]["path_wind_wds"]+table_name+"\\"+ file_name   )

        col_list_add =[]
        ### 1,季度roe_ttm, 前2个季度的S_FA_ROE,S_FA_ROE, 净资产收益率 S_FA_ROE
        col_list_add = col_list_add + ["S_FA_ROE"    ]
        ### 2,单季度.营业总收入环比增长率(%) S_QFA_CGRGR;单季度.营业总收入同比增长率(%) "S_QFA_YOYGR"
        col_list_add = col_list_add + ["S_QFA_CGRGR","S_QFA_YOYGR"  ]
        ### 3,单季度.净利润同比增长率(%) S_QFA_YOYPROFIT;单季度.净利润环比增长率(%) S_QFA_CGRPROFIT
        col_list_add = col_list_add + ["S_QFA_YOYPROFIT","S_QFA_CGRPROFIT"  ]
        ### 4，季度盈利持续性:"S_FA_ROIC",ROIC环比增长率,总资产净利率,S_FA_ROA
        col_list_add = col_list_add + ["S_FA_ROIC","S_FA_ROA"  ]
        ### 5，季度资产质量：近2季度经营性现金流/总资产 or 经营性现金流/总负债，
            # 经营活动产生的现金流量净额/营业收入,S_FA_OCFTOOR;经营活动产生的现金流量净额/经营活动净收益,S_FA_OCFTOOPERATEINCOME
            # 单季度.经营活动产生的现金流量净额／营业收入,S_QFA_OCFTOSALES;
        col_list_add = col_list_add + ["S_FA_OCFTOOR","S_FA_OCFTOOPERATEINCOME","S_QFA_OCFTOSALES","S_QFA_OCFTOOR"    ]
        ### (已有数据)6，估值：PE_FY1 <= 60，50 或 PE_FY1 <=,45，40；或者用同业前35%

        ### Loop for code
        for temp_i in df_ashare_ana.index :
            temp_code = df_ashare_ana.loc[temp_i,"S_INFO_WINDCODE" ]
            
            ### 当季 date_q
            df_fa = df_fi_indi[ df_fi_indi["S_INFO_WINDCODE"]== temp_code ]
            if len(df_fa.index )>0 :
                for temp_col in col_list_add :
                    df_ashare_ana.loc[temp_i, temp_col ] =  df_fa[temp_col].values[0]
                    # if temp_col == "S_FA_ROE":
                    #     print("Debug==== ",temp_code,df_fa[temp_col].values[0])
            ### 上一季 date_q
            df_fa_q_pre = df_fi_indi_q_pre[ df_fi_indi_q_pre["S_INFO_WINDCODE"]== temp_code ]
            if len(df_fa_q_pre.index )>0 :
                for temp_col in col_list_add :
                    df_ashare_ana.loc[temp_i, temp_col +"_q_pre" ] =  df_fa_q_pre[temp_col].values[0]

            #######################################################################################
            ### 计算我们需要的值 ，"_diff" 和 "_diff_pct"
            ### 1，roe，关于数据：如果是第三季度S_FA_ROE是前3个季度的合计roe，如果是Q2择时前2个季度合计roe，需要用para_list前2项计算
            df_ashare_ana.loc[temp_i, "S_FA_ROE" +"_q_ave" ] = df_ashare_ana.loc[temp_i, "S_FA_ROE" ]/para_date[0]
            df_ashare_ana.loc[temp_i, "S_FA_ROE" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_ROE" +"_q_ave" ] - df_ashare_ana.loc[temp_i, "S_FA_ROE" +"_q_pre" ]/para_date[1]
            
            ### 2,单季度.营业总收入环比增长率(%) S_QFA_CGRGR;单季度.营业总收入同比增长率(%) "S_QFA_YOYGR";都是单季度值，直接求差值即可
            df_ashare_ana.loc[temp_i, "S_QFA_CGRGR" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_QFA_CGRGR" ] - df_ashare_ana.loc[temp_i, "S_QFA_CGRGR" +"_q_pre" ] 
            df_ashare_ana.loc[temp_i, "S_QFA_YOYGR" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_QFA_YOYGR" ] - df_ashare_ana.loc[temp_i, "S_QFA_YOYGR" +"_q_pre" ] 

            ### 3,单季度.净利润同比增长率(%) S_QFA_YOYPROFIT;单季度.净利润环比增长率(%) S_QFA_CGRPROFIT
            df_ashare_ana.loc[temp_i, "S_QFA_YOYPROFIT" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_QFA_YOYPROFIT" ] - df_ashare_ana.loc[temp_i, "S_QFA_YOYPROFIT" +"_q_pre" ] 
            df_ashare_ana.loc[temp_i, "S_QFA_CGRPROFIT" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_QFA_CGRPROFIT" ] - df_ashare_ana.loc[temp_i, "S_QFA_CGRPROFIT" +"_q_pre" ] 

            ### 4，季度盈利持续性:"S_FA_ROIC",ROIC环比增长率,总资产净利率,S_FA_ROA，需要用para_list前2项计算
            df_ashare_ana.loc[temp_i, "S_FA_ROIC" +"_q_ave" ] = df_ashare_ana.loc[temp_i, "S_FA_ROIC" ]/para_date[0]
            df_ashare_ana.loc[temp_i, "S_FA_ROIC" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_ROIC" ]/para_date[0] - df_ashare_ana.loc[temp_i, "S_FA_ROIC" +"_q_pre" ]/para_date[1]
            df_ashare_ana.loc[temp_i, "S_FA_ROA" +"_q_ave" ] = df_ashare_ana.loc[temp_i, "S_FA_ROA" ]/para_date[0]
            df_ashare_ana.loc[temp_i, "S_FA_ROA" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_ROA" ]/para_date[0] - df_ashare_ana.loc[temp_i, "S_FA_ROA" +"_q_pre" ]/para_date[1]

            ### 5，季度资产质量：近2季度经营性现金流/总资产 or 经营性现金流/总负债，
            # 经营活动产生的现金流量净额/营业收入,S_FA_OCFTOOR;经营活动产生的现金流量净额/经营活动净收益,S_FA_OCFTOOPERATEINCOME
            # 单季度.经营活动产生的现金流量净额／营业收入,S_QFA_OCFTOSALES;单季度.经营活动产生的现金流量净额／经营活动净收益,S_QFA_OCFTOOR
            df_ashare_ana.loc[temp_i, "S_FA_OCFTOOR" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_OCFTOOR" ] - df_ashare_ana.loc[temp_i, "S_FA_OCFTOOR" +"_q_pre" ] 
            df_ashare_ana.loc[temp_i, "S_FA_OCFTOOPERATEINCOME" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_OCFTOOPERATEINCOME" ] - df_ashare_ana.loc[temp_i, "S_FA_OCFTOOPERATEINCOME" +"_q_pre" ] 


        ### save to obj
        obj_data["dict"]["list_para_date"]  =  obj_date["list_para_date"] 
        obj_data["dict"]["date_q"]  = date_q
        obj_data["dict"]["date_q_pre"]  =  date_q_pre 
        obj_data["df_ashare_ana"]  =  df_ashare_ana
        
        return obj_data

    def export_data_financial_ana(self,obj_data,obj_financial):
        ### 输出财务分析和财务模型相关指标,obj_data是单期数据，obj_financial是跨期日期数据和pms数据存放处
        ''' ### 注意：在调仓日T有3套时间：
        1，T和之前1次披露时间；[date_ann_pre, date_ann ]
        2，T之前的2个季末财务日期；[date_q_pre,date_q]
        3，T日至下一个财务披露日期:[date_ann, date_ann_next ]。
        注意：在调仓日T有3套时间：1，T之前的2个披露时间；2，T之前的2个季末财务日期；3，T日至下一个财务披露日期。
        obj_data["dict"]["date_adj_port"] 
        obj_data["dict"]["date_adj_port_next"] 
        obj_data["dict"]["date_ann"] 
        obj_data["dict"]["date_ann_next"] 
        obj_data["dict"]["date_ann_pre"]
        '''
        ### 组合调整日期：
        
        date_q = obj_data["dict"]["date_q"] 
        date_q_pre = obj_data["dict"]["date_q_pre"] 
        ### 
        data_io_1 = data_io() 
        
        ### 设置文件命名和导出路径等
        path_financial_ana = self.obj_config["dict"]["path_ciss_db"] +"financial_ana\\" 
        path_export = path_financial_ana + obj_data["dict"]["id_output"]+ "\\"
        if not os.path.exists( path_export  ) :
            os.mkdir( path_export  )
        print("path_export: ",path_export  )
        obj_data["dict"]["path_export"] =path_export

        #############################################################################
        ### 行业分布：df_ashare_ind
        file_name = "df_ashare_ind_" + date_q + "_" + date_q_pre +".csv" 

        obj_data["df_ashare_ind"].to_csv( path_export+file_name, index=False )

        ### 个股权重和分析指标信息 df_ashare_portfolio ；df_ashare_portfolio包括了 df_ashare_ana
        # 上一次
        file_name = "df_ashare_portfolio_" + obj_data["dict"]["date_ann"] +".csv"
        # obj_data["df_ashare_portfolio"] = data_io_1.del_columns( obj_data["df_ashare_portfolio"],col_str)
        obj_data["df_ashare_portfolio"].to_csv( path_export+file_name, index=False )

        ### 分组区间收益 df_ret_all
        
        file_name = "df_ret_all_" + obj_data["dict"]["date_ann_pre"] + "_" + obj_data["dict"]["date_ann"] +".csv"
        obj_data["df_ret_all"].to_csv( path_export+file_name, index=False )

        ### PMS 文件
        ### 保存至 PMS权重文件
        # obj_financial["df_pms"] = pd.DataFrame( columns=["证券代码","持仓权重","成本价格","调整日期","证券类型"] )
        temp_df = obj_data["df_ashare_portfolio"][ obj_data["df_ashare_portfolio"]["weight_raw"]>0.003 ] 
        temp_df = temp_df.loc[:,["S_INFO_WINDCODE","weight_raw"] ]
        temp_df["weight_raw"] = temp_df["weight_raw"] 
        temp_df.columns=[ "证券代码","持仓权重" ]
        ### 下一个交易日调仓
        temp_df["成本价格" ] = ""
        temp_df["调整日期" ] = obj_data["dict"]["date_ann"] 
        temp_df["证券类型" ] = "股票"

        if obj_financial["count_pms"]  == 0 :
            obj_financial["df_pms"] =temp_df 
            obj_financial["count_pms"]  = 1 
        else :
            obj_financial["df_pms"] = obj_financial["df_pms"].append(temp_df,ignore_index=True)

        ### save df_pms to file 
        file_pms="pms_"+ obj_data["dict"]["date_ann"] +".csv"
        temp_df.to_csv( obj_data["dict"]["path_export"] +file_pms, index=False, encoding="gbk" )
        file_pms="pms.csv"
        obj_financial["df_pms"].to_csv( obj_data["dict"]["path_export"] +file_pms, index=False, encoding="gbk" )

        ### save return for next period 
        file_ret_next = "df_ret_next_"+obj_data["dict"]["date_ann"] +"_" + obj_data["dict"]["date_adj_port_next"]  +".csv"
        obj_financial["df_ret_next"].to_csv( obj_data["dict"]["path_export"] +file_ret_next, index=False  )

        return obj_data,obj_financial


#######################################################################