# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
transform_wind_wds.py
todo: 

数据来源： get_wind_wds.py
last update  201009 | since 200116

功能： 每日/每周数据下载更新好后，需要及时地转化成带有列名称的csv文件。对每个股票/基金/指数，用交易日数据维护个券的记录.

Function：
1，把给定时间区间的原始wds数据保存成标准化格式，加列名称和加_adj后缀；把列名是1，2...的数据文件改成对应的列名，并重新命名csv文件
2，path_rc = "C:\\db_wind\\data_adj\\"
3,把原始表格中一些不常用的列删去、把历史期间数据有问题的行删去可以显著减小数据大小

output:
1,df_1d:每个交易日所有股票的行业归属
2,df_1s:每个股票的历史行业归属

计算逻辑：
1，对于20050601开始到20200114的每个交易日：
2，对于第一个交易日，导入股票信息df_A_des、按照3个行业分类计算所属行业。
2.1,寻找在20050601前已经上市的且 未退市的公司，并计算上市的时间；
3，对每个股票，用行业数据生成全历史行业分类数据
3.1，对于Wind行业，获取在20050601前已经上市的公司，并计算当日的行业分类；
3.2，匹配中信行业分类；
3.3，匹配申万行业分类；
4,对于每个行业分类，匹配新旧2种行业分类
4.1，Wind：1~4级行业代码、行业名称、
4.2，citics：1~3级新行业代码、新行业名称、旧行业代码、旧行业。
4.3，sw:1~3 级行业代码、行业名称、行业名称英文。


notes:600061.SH国投资本发生行业分类变化时，
中信、Wind在20150127变更行业、但申万竟然在20160630才变更，说明了申万行业分类的不靠谱。

行业代码[AShareIndustriesCode] ：
62：万得全球行业分类标准
04：证监会行业分类
61：申万行业分类
67：GICS(全球行业分类标准)
66：中证行业分类
b1：中信行业分类
12: 证监会行业分类(2012版)03: 地域板块02: 概念板块a0: 股票板块 72: 中证行业分类(2016)
74: 国民经济行业分类
2001: 万得基金分类
0808: 中债分类
2003: 银河基金分类

notes：
1,必须在下载时就给table对应好列名columns ；
2,原则是不改变原文件，可以在原文件后边加_ 后缀。

===============================================
'''
import pandas as pd
import numpy as np
import json
import time
import datetime as dt
import os


class transform_wds():
    # 类的初始化操作
    def __init__(self):
        
        #################################################################################
        ### Initialization 
        # 设置
        self.path_disk = "C:\\"   # "D:\\"  

        if os.path.exists( "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\" ) : 
            self.file_path_admin = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
        else :
            self.file_path_admin = "C:\\ciss_web\\CISS_rc\\apps\\rc_data\\"

        if not os.path.exists( self.path_disk +"db_wind\\" ) : 
            self.path_disk = "D:\\"   
        
        if os.path.exists( self.path_disk +"db_wind\\" ) :
            self.path0 = self.path_disk +"db_wind\\"
        else :
            print("path of db_wind")
            self.path0 = "F:\\" + "db_wind\\"
        
        self.path_rc = self.path0 + "data_adj\\" 
        
        self.path_wds = self.path0 +"data_wds\\"
        self.path_adj = self.path0 +"data_adj\\"

        ### 导入Wind全历史行业分类数据 || df_600151.SH
        self.path_rc_ind =self.path_rc + "industries_class\\" 


    def print_info(self):
        ### print all modules for current script
        print("2.1,文件目录、文件名、csv文件列名称批量处理")
        print("rename_folder |按命名规则对文件夹内满足条件的文件改名,并对columns赋值 ")
        print("add_columns2table  | 对文件夹内文件名字进行统一处理，并讲列名称赋值") 

        print("2.2,A股股票行业分类处理")
        print("cal_df_ind | 计算和导出行业分类必备的数据文件，old_name=import_df_ind")
        print("match_ind_name | 根据行业代码匹配行业分类表格内的行业名称、行业级数和是否弃用")        
        print("get_ind_period |根据期初和期末日期获取股票所属行业 ")
        print("get_ind_date |给定交易日获取股票所属行业 ")
        print("cal_stock_indclass |计算个股历史行业变动和最新行业分类:将3种行业分类代码和中文值赋给对应的股票 ")

        print("2.3,导入历史交易日数据和交易日数据填入单证券数据")
        print("import_df_dates | 导入历史交易日数据数据文件")  
        print("update_date_pass_code |给定table,对于现有个券文件，用WDS_TRADE_DT_20200210_ALL.csv 填入 WDS_S_INFO_WINDCODE_000001.SZ_ALL.csv")
        print("update_newcode_from_date |对于给定交易日内，若有新股票无个券csv文件则新建该文件")
        print("trans_ALL_to_1date | 从一次性下载的表格按单个交易日转存数据")

        print("2.4,导入基金数据和持仓数据")
        print("import_fund_list | 根据季末日期获取基金基础信息列表")
        print("import_df_fund | 给定基金，导入与基金数据相关的数据表格")

        print("2.5,计算基金持仓股票多维度变动")
        print("cal_diff_stockport | 计算基金持仓股票与前一季度之间的差异值，但不进行分析 ")
        print("manage_fund_sp_change | 导入基金持仓列表,计算持仓股票与上季度同比的变动")
        print("get_fund_period_diff | 获取区间基金净值价格、涨跌幅、成交额等变动 ")

        print("2.6,计算指数定期调整")
        print("cal_index_constituents_adjustment| 计算指数调整 ")
        print("get_index_period_diff | 获取区间指数价格、涨跌幅、成交额等变动 ")

        print("2.7,计算个股区间变动数据")
        print("get_stock_period_diff | 获取区间股票价格、市值股本、财务指标变动和涨跌幅 ")

        print("data_manage |manage_opdata_to_anndates:根据opdate区间数据作为增量，更新基于prime_key|发布日期的逐个数据表格  ")
        return 1 
    
    def rename_folder(self,table_name, para_dict ) :
        ### 按命名规则对文件夹内满足条件的文件改名,并对columns赋值
        ''' 
        notes:csv文件名末尾统一 "_ALL.csv"，其中date_range = "ALL"
        1，中国共同基金投资组合——持股明细[ChinaMutualFundStockPortfolio]
            temp_str[4:12]=="ANN_DATE"   || temp_str[:25]+temp_str[-4:]
            temp_str[4:17]=="F_PRT_ENDDATE" || temp_str[:30]+temp_str[-4:]
        2，#2.1，WDS_S_INFO_WINDCODE_688008.SH_ALL_20190830 TO WDS_S_INFO_WINDCODE_688008.SH_ALL
        ### 股票日行情 AShareEODPrices | 股票日衍生行情 AShareEODDerivativeIndicator
        ### step:1 读取columns.csv，对所有列名是0，1，2...的文件按columns.csv改成带英文列名的。
        ### step:2 读取文件夹内的文件名列表，按给定匹配规则保留file_list
        ### step:3 将文件名最后的"_20190830"去掉，并重命名
        ''' 
        file_path = self.path_wds + table_name +"\\"
        file_list = os.listdir( file_path )
        
        #################################################################################
        ### 例1：中国共同基金投资组合——持股明细[ChinaMutualFundStockPortfolio]
        # file_list =[temp_str for temp_str in file_list if temp_str[-12:-8]=="2019" and temp_str[4:17]=="F_PRT_ENDDATE" ]
        # for temp_str in file_list :
            
        #     print( temp_str,temp_str[:25]+temp_str[-4:] )
        #     ### 如果文件一级存在就不进行操作，并删除原有文件
        #     # notes:慎用 os.remove()
        #     # os.remove(file_path+ temp_str)
            
        #     if  not os.path.exists(file_path+ temp_str[:30]+temp_str[-4:] ) : 
        #         os.rename(file_path+ temp_str,file_path+ temp_str[:30]+temp_str[-4:]  )  
        ### 为基金持股文件的列赋值,需要事先自己匹配好列的名字
        # col_list = ["OBJECT_ID","S_INFO_WINDCODE","F_PRT_ENDDATE","CRNCY_CODE","S_INFO_STOCKWINDCODE","F_PRT_STKVALUE","F_PRT_STKQUANTITY","F_PRT_STKVALUETONAV","F_PRT_POSSTKVALUE","F_PRT_POSSTKQUANTITY","F_PRT_POSSTKTONAV","F_PRT_PASSTKEVALUE","F_PRT_PASSTKQUANTITY","F_PRT_PASSTKTONAV","ANN_DATE","STOCK_PER","FLOAT_SHR_PER","op_date","useless"]
        
        ### 例2：个股日行情
        #2.1， WDS_S_INFO_WINDCODE_688008.SH_ALL_20190830 TO WDS_S_INFO_WINDCODE_688008.SH_ALL
        #2.2，WDS_TRADE_DT_20050217_ALL_20190902 TO WDS_TRADE_DT_20050217_ALL
        #################################################################################
        #2.1，WDS_S_INFO_WINDCODE_688008.SH_ALL_20190830 TO WDS_S_INFO_WINDCODE_688008.SH_ALL
        ### 股票日行情 AShareEODPrices | 股票日衍生行情 AShareEODDerivativeIndicator
        ### step:1 读取columns.csv，对所有列名是0，1，2...的文件按columns.csv改成带英文列名的。
        ### step:2 读取文件夹内的文件名列表，按给定匹配规则保留file_list
        ### step:3 将文件名最后的"_20190830"去掉，并重命名
        df_cols = pd.read_csv(file_path+ "columns.csv",index_col=0  )
        col_list =list(df_cols["0"])
        print( "df_cols", len(col_list),col_list )  
        '''1,
        temp_str[-12:-8]=="2019" and temp_str[:19]=="WDS_S_INFO_WINDCODE" 
        
        len(temp_str)== 46 and temp_str[:19]=="WDS_S_INFO_WINDCODE"
        len(temp_str)== 38 and temp_str[:12]=="WDS_TRADE_DT"   
        case: file=WDS_ANN_DATE_20191205_ALL_20191206.csv | len(temp_str)== 38 and temp_str[:12]=="WDS_ANN_DATE" 
        2,CASE WDS_TRADE_DT_20200311_ALL.csv to WDS_TRADE_DT_20200311.csv in AIndexEODPrices
            filter= len(temp_str)== 29 and temp_str[-7：]=="ALL.csv"
            temp_str2 = temp_str[:-8] +".csv"
        3,CASE WDS_TRADE_DT_20191210_ALL_20191212.csv;len 38, temp_str[:-13]+ ".csv"
        4,CASE WDS_TRADE_DT_20170320_ALL_20191211.csv
        5,CASE WDS_TRADE_DT_20050131_ALL_20190902.csv
        6,CASE WDS_ANN_DATE_20200320.csv to WDS_ANN_DATE_20200320_ALL.csv

        '''
        ############################################################
        ### PARA 1 
        file_list =[temp_str for temp_str in file_list if len(temp_str)==25 and  temp_str[:12] =="WDS_ANN_DATE"  ]
        # file_list =[temp_str for temp_str in file_list if temp_str[-12:-8]=="2019" ]
        count=0
        for temp_str in file_list : 
            # index_col=0意味着会将第一列ID值作为Index，导致少了一列
            # notes:历史数据中，有的有 "Unnamed:0"序号列，导致多一列
            ############################################################
            ### PARA 2
            # temp_df = pd.read_csv(file_path+ temp_str   )  
            temp_df = pd.read_csv(file_path+ temp_str ,index_col=0) 
            # ,encoding="gbk"

            # print(temp_df.columns[0])            
            if len( temp_df.index ) > 0 :
            # if len( temp_df.index ) > 0 and (not temp_df.columns[0]=="OBJECT_ID"):
                print(count,temp_str )
                # 对columns赋值的情况
                temp_df.columns = col_list

                ############################################################
                ### PARA 3  计算需要保留的str长度，
                temp_str2 =  temp_str[:-4]+ "_ALL.csv"

                # 按新的名字保存文件
                temp_df.to_csv( file_path+ temp_str2 ,index=0)
                # 删除旧文件
                os.remove(file_path+ temp_str )  

        ### Warning! About to delete all files
        ### 使用os.remove 时要非常小心
        # for temp_str in file_list : 
        #     print(temp_str)
        #     os.remove(file_path+ temp_str )
            
        #     count = count+1

        ### 
        return 1 


    def import_df_dates(self ) :
        ### 导入历史交易日数据，取决于 指数00000.SH的日期数据是否完整
        # "AIndexEODPrices","WDS_S_INFO_WINDCODE_000001.SH_ALL.csv"
        # notes:不能用000300.SH地日期数据，因为虽然从20020823开始，但20040616之后就是20050601。缺失了1年地数据！
        # 发现浦发银行和平安银行都是1991年上市，毕竟适合作为A股地交易日。
        # WDS_S_INFO_WINDCODE_000001.SZ_ALL
        ##########################################################################
        ### 导入交易日日期数据 | 一个主要用处是和季度末日期进行匹配
        # table_name = "AShareEODPrices"  
        # code="000001.SZ"
        # file_name = "WDS_S_INFO_WINDCODE_"+ code+"_ALL.csv"
        table_name = "AIndexEODPrices"  
        code="000001.SH"
        file_name = "WDS_S_INFO_WINDCODE_"+ code+"_ALL.csv"
        # ,encoding="gbk"
        temp_df =  pd.read_csv( self.path_wds +table_name +"\\"+file_name,index_col=False)
        col_list = ["S_INFO_WINDCODE","TRADE_DT","S_DQ_CLOSE","S_DQ_CHANGE","S_DQ_PCTCHANGE"]
        temp_df=temp_df.loc[:,col_list]

        date_list = list(temp_df["TRADE_DT"]) 
        ###save to dict object
        obj_dates = {}
        obj_dates["code"] = temp_df
        obj_dates["date_start"] = date_list[0]
        obj_dates["date_end"] = date_list[-1]
        obj_dates["date_list"] = date_list
        
        return obj_dates
    
    def update_date_pass_code(self,table_name) :
        ### 给定table,对于现有个券文件，用WDS_TRADE_DT_20200210_ALL.csv 填入 WDS_S_INFO_WINDCODE_000001.SZ_ALL.csv
        '''
        1,导入data_check_anndates.csv的dates，
        2，获取 code list
        3，和 WDS_S_INFO_WINDCODE_000001.SZ_ALL.csv比较，获取需要更新的日期序列
        4，保存更新后的date_list,code_list 
        假设：同一个表内所有个股都有统一的最新更新日期，因此从下一个交易日开始，如果交易日表有个股不在code_list
        中，则要对该个股新建个股表格
        notes:当前程序存在bug，对于1991上市，2002退市的000003.SZ，会取匹配2002~2020年的交易日数据，这部分无意义的计算
        占用了大部分算力。
        last | since 200212
        '''
        import datetime as dt 
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 
        ### 1,导入data_check_anndates.csv的dates
        file_name = "data_check_anndates.csv"
        df_check_anndates = pd.read_csv(self.file_path_admin+file_name  )
        # type of date_list is numpy.int64
        date_list = df_check_anndates["date"].values
        date_list.sort()
        print("date_list",date_list[:3],date_list[-3:] )

        ### 2，获取表格目录内 code list | from "WDS_S_INFO_WINDCODE_000423.SZ_ALL.csv"
        file_path = self.path_wds + table_name +"\\"
        file_list = os.listdir( file_path )
        # 筛选条件 1：文件名称 
        file_list =[temp_str for temp_str in file_list if len(temp_str)== 37 and temp_str[:19]=="WDS_S_INFO_WINDCODE" ]
        
        ### 筛选条件 2：修改时间 | notes：这部分代码筛选出来的都是退市股票
        # file_list2 =[]
        # import time 
        # for temp_file in file_list :
        #     # 获取文件修改时间和创建时间 "%Y%m%d_%H%M%S"
        #     # time.strftime("%Y%m%d",os.path.getctime(file_path +temp_file) ) 
        #     # dt.datetime.strftime( os.path.getctime(file_path +temp_file) ,format="%Y%m%d") 
        #     # filedate_create = time.strftime("%Y%m%d",time.localtime( os.path.getctime(file_path +temp_file) ) )
            
        #     filedate_modify = time.strftime("%Y%m%d",time.localtime( os.path.getmtime(file_path +temp_file) ) )
        #     if filedate_modify == "20200210" :
        #         file_list2 = file_list2 +[temp_file ]
        # file_list = file_list2       

        ### Get code list 
        code_list0 =[temp_code[20:29] for temp_code in file_list ]
        print("code_list0 ",code_list0[:3],code_list0[-3:]  )
        
        count=0
        for temp_code in code_list0 : 
            ### 读取股票的最新交易日，
            file_name_code = "WDS_S_INFO_WINDCODE_"+ temp_code  + "_ALL.csv"
            try :
                df_stock= pd.read_csv(file_path+ file_name_code )
            except:
                df_stock= pd.read_csv(file_path+ file_name_code ,encoding="gbk" )
            # date_start = df_stock["TRADE_DT"].min()
            date_end = df_stock["TRADE_DT"].max()
            if_change= 0 
            ### 匹配表格下载日期内的
            ### 3，和 WDS_S_INFO_WINDCODE_000001.SZ_ALL.csv比较，获取需要更新的日期序列
            date_list_new = [date for date in date_list if date>date_end ]
            print("date_list_new ", date_list_new[:3] , date_list_new[-3:] )
            if len(date_list_new ) > 0 :
                df_stock= df_stock.sort_values("TRADE_DT")
                # 这里是默认date_list_new 日期从小到大
                for temp_date in date_list_new : 
                    file_name_date= "WDS_TRADE_DT_" + str(temp_date) +"_ALL.csv"
                    #notes :可能出现 WDS_TRADE_DT_20190831_ALL.csv' does not exist
                    if os.path.exists( file_path+ file_name_date ) :  
                        print(file_path+ file_name_date )
                        df_date= pd.read_csv(file_path+ file_name_date ,encoding="gbk" )
                        # notes:对于 WDS_TRADE_DT_20200107_ALL.csv属于交易日，但是可能因下载过程出错导致无数据，需要重新下载
                        # 若交易日无数据，用 test_wds_manage.py\Choice 2 重新下载

                        df_date_s = df_date[ df_date["S_INFO_WINDCODE"] == temp_code  ]
                        
                        if not df_date_s.empty :
                            ### Append df_date_s to df_stock
                            df_stock = df_stock.append(df_date_s,ignore_index=True )
                            # print(df_stock.tail(3).T )
                            if_change = 1
                            
            if if_change == 1 :
                if "Unnamed: 0" in df_stock.columns :
                    df_stock = df_stock.drop("Unnamed: 0",axis=1)            
                df_stock.to_csv(file_path+ file_name_code, index=False  )

        ### 4，保存更新后的date_list，code_list里保存有对应csv文件地codes
        df_check_anndates["date"].to_csv(self.file_path_admin+"date_list.csv",index=False ,encoding="gbk" )
        pd.DataFrame(code_list0).to_csv( self.file_path_admin+"date_list.csv" ,encoding='utf-8')

        df_output = {}
        df_output["date_list"] = date_list
        df_output["date_list_new"] = date_list_new
        df_output["code_list"] = code_list0

        return df_output

    def update_newcode_from_date(self, date_start,table_name  ):
        ### 对于给定交易日内，若有新股票无个券csv文件则新建该文件
        # date_start 20190830
        ### 1, 获取 date_list中在 date_start之后的日期列表
        # 导入data_check_anndates.csv的dates
        file_name = "data_check_anndates.csv"
        df_check_anndates = pd.read_csv(self.file_path_admin+file_name  )
        # type of date_list is numpy.int64
        date_list = df_check_anndates["date"].values
        date_list.sort()
        date_list_new = [date for date in date_list if date> int(date_start)  ]
        print("date_list_new",date_list_new[:3],date_list_new[-3:] )

        ### 2，对于每个日期列表，判断交易日内的股票都有对应的个券csv文件，若没有则新建个券文件
        # 获取表格目录内的code list | from "WDS_S_INFO_WINDCODE_000423.SZ_ALL.csv"
        file_path = self.path_wds + table_name +"\\"
        file_list = os.listdir( file_path )

        file_list =[temp_str for temp_str in file_list if len(temp_str)== 37 and temp_str[:19]=="WDS_S_INFO_WINDCODE" ]
        code_list0 =[temp_code[20:29] for temp_code in file_list ]
        print("code_list0 ",code_list0[:3],code_list0[-3:]  )
        ### code_list_new_acuumulation
        code_list_new_accu = []
        for temp_date in date_list_new : 
            print("Working on date ",temp_date )
            # import date csv 
            file_name_date= "WDS_TRADE_DT_" + str(temp_date) +"_ALL.csv" 
            #notes :可能出现 WDS_TRADE_DT_20190831_ALL.csv' does not exist
            if os.path.exists( file_path+ file_name_date ) :  
                print(file_path+ file_name_date )
                df_date= pd.read_csv(file_path+ file_name_date ,encoding="gbk" )
                code_list_from_date = df_date["S_INFO_WINDCODE"].values
                print("code_list_from_date ",code_list_from_date[:3] ,code_list_from_date[-3:]  )
                
                ### 将当前交易日新增的股票加入需要更新的累计新增股票列表
                code_list_new = [code for code in code_list_from_date if code not in code_list0 ]
                if len(code_list_new) >0 :
                    print("code_list_new",code_list_new  )
                    # add to code_list
                    code_list0 = code_list0 + code_list_new
                    code_list_new_accu =code_list_new_accu+code_list_new
                
                for temp_code in code_list_new_accu :
                    print("Working on code ",temp_code )
                    df_stock_1d = df_date[ df_date["S_INFO_WINDCODE"] == temp_code ]                   

                    # save to file 
                    file_output = "WDS_S_INFO_WINDCODE_"+ temp_code  + "_ALL.csv"
                    ### 判断是否存在该文件 
                    if os.path.exists( file_path+ file_output ) :  
                        df_stock = pd.read_csv(file_path+ file_output ,encoding="gbk" )
                        df_stock = df_stock.append(df_stock_1d,ignore_index=True)
                        if "Unnamed: 0" in df_stock.columns :
                            df_stock = df_stock.drop("Unnamed: 0",axis=1)  
                        df_stock.to_csv( file_path+ file_output ,encoding="gbk" )
                    else :
                        df_stock_1d.to_csv( file_path+ file_output ,encoding="gbk" )
                    print(file_path+ file_output )
                    ### 对于temp_code 更新所有后续交易日

        print("Following codes has been add to directory:",code_list_new_accu )

        return code_list_new_accu

    def trans_ALL_to_1date(self,table_name,keyword ) :
        ### 从一次性下载的表格按单个交易日转存数据
        # keyword ="TRADE_DT","S_CON_INDATE"

        file_check_anndates = "data_check_anndates_2003.csv"
        df_check_anndates = pd.read_csv( self.file_path_admin+ file_check_anndates )
        date_list = list( df_check_anndates.date)
        print("date_list", date_list[:5] )
        
        ### 导入wds表格
        file_ALL = "WDS_full_table_full_table_ALL.csv"
        # ,encoding="gbk"
        df_ALL = pd.read_csv( self.path_wds + table_name+"\\"+file_ALL)
        print("df_ALL"   )
        for temp_i in df_check_anndates.index :
            temp_date = df_check_anndates.loc[temp_i,"date"]
            
            # e.g. df_ALL["TRADE_DT"] type=int.. 
            df_sub = df_ALL [ df_ALL[ keyword ]== temp_date ]  
            if len(df_sub.index) > 0 :
                print( temp_date )
                # save to csv file 
                file_name = "WDS_"+ keyword + "_"+ str(temp_date)+"_ALL.csv"
                df_sub.to_csv( self.path_wds + table_name +"\\"+ file_name, index=False )
                # 1 means exists data and 2 means empty data
                df_check_anndates.loc[temp_i,table_name ] = 1 
            else :
                print("No record for date", temp_date )
                # 1 means exists data and 2 means empty data
                df_check_anndates.loc[temp_i,table_name ] = 2


        
        ### save to csv 
        df_check_anndates.to_csv( self.file_path_admin+ file_check_anndates,index=False )

        return df_check_anndates

    def cal_df_ind(self ) :
        ### cal_df_ind | 计算和导出行业分类必备的数据文件，old_name=import_df_ind
        #################################################################################
        ### df_A_des || 历史个股行业分类
        ### step1 新建df，基于股票基础信息
        # 对20050101开始，每个交易日存续的股票，计算所属的3个行业分类代码：
        ''' 
        OUTPUT:
        object_df={} 包括
        ### 输出各个df数据文件  
        # df_A_des date_list || 
        # 股票行业分类进出 ： df_ind_wind_stocks df_ind_citics_stocks df_ind_sw_stocks
        # 3个行业分类的行业代码信息： df_indclass_wind，df_indclass_citics ，df_indclass_sw
        # df_csi300 || 沪深300的历史行情文件，包括了20050601以来的交易日
        ### list 格式的变量
        # code_list 上市股票代码表 list
        # date_list 交易日期记录 list
        # df_1d  || 寻找在20050601前已经上市的且 未退市的公司信息，并计算上市的时间；
        ------------
        数据分析：
        中国A股基本资料[AShareDescription]
        3个关键词：股票代码，上市日期，退市日期
        S_INFO_WINDCODE	 	000005.SZ
        S_INFO_EXCHMARKET	 SZSE	 	
        S_INFO_LISTDATE	19901210	
        S_INFO_DELISTDATE
        S_INFO_LISTBOARDNAME 主板、中小、创业、科创

        中国A股Wind行业分类[AShareIndustriesClass]
        WIND_IND_CODE 6240101010
        ENTRY_DT 19910403
        S_INFO_WINDCODE 

        中国A股科创板所属新兴产业分类[AShareSTIBEmergingIndustries]
        WIND_SEC_NAME 下一代信息网络产业

        AShareConseption 概念分类 
        notes:截至200113，wind概念具体个数有2018个，估计还在快速增长。
        数据上，总记录27w，现存梳理4.7w，还包括了"全A"，"陆股通买入前二十","特斯拉" 等

        中国A股中信行业分类[AShareIndustriesClassCITICS]
        申万行业分类[AShareSWIndustriesClass]
        SW_IND_CODE ： 	6106010600
        申万指数成份明细[SWIndexMembers]
        S_INFO_WINDCODE ：801862.SI
            申万行业分类通常在股票上市后一周左右申万才会公布。
        
        '''
        object_wds = {}
        
        table_A_des = "AShareDescription"
        path_A_des = self.path_wds + table_A_des
        file_name = "WDS_full_table_full_table_ALL.csv"
        # 不带列index值读取。
        try :
            df_A_des = pd.read_csv( path_A_des +"\\"+ file_name,index_col=0  )
        except :
            df_A_des = pd.read_csv( path_A_des +"\\"+ file_name,encoding="gbk",index_col=0  )


        ### 剔除未上市股票
        df_A_des["S_INFO_LISTDATE"] =pd.to_numeric( df_A_des["S_INFO_LISTDATE"],errors="coerce" )
        df_A_des["S_INFO_LISTDATE"] =pd.to_numeric( df_A_des["S_INFO_LISTDATE"],errors="coerce" )

        df_A_des = df_A_des[ df_A_des["S_INFO_LISTDATE"] > 0  ]
        df_A_des = df_A_des.sort_values(by="S_INFO_LISTDATE",ascending=True )
        print("df_A_des")
        # print( df_A_des.head().T )
        print( df_A_des.info() )
        ### notes:截至20200113，有3877个股票有上市记录，有111个股票有退市记录。

        col_list=["S_INFO_WINDCODE","S_INFO_NAME","S_INFO_EXCHMARKET", "S_INFO_LISTDATE","S_INFO_DELISTDATE","CRNCY_CODE","S_INFO_PINYIN","S_INFO_LISTBOARDNAME"]
        df_A_des = df_A_des.loc[:,col_list]
        ### 保存到csv,不带列index输出
        
        file_name =  "AShare_des_industry.csv"
        df_A_des.to_csv(self.path_rc + file_name ,encoding="gbk" ,index= False )
                    
        #################################################################################
        ### 导入行业分类进出数据 
        # df_ind_wind Wind股票进出 || 中国A股Wind行业分类[AShareIndustriesClass]  || GICS和wind1的差异在于wind=62+GICS
        temp_table = "AShareIndustriesClass"
        temp_path = self.path_wds+ temp_table
        file_name = "WDS_full_table_full_table_ALL.csv"
        # notes:如果不带列index值读取，,index_col=0 ，则第一列会变成index，但第一列是中信行业代码
        df_ind_wind = pd.read_csv( temp_path +"\\"+ file_name,encoding="gbk" )
        print("df_ind_wind")
        print( df_ind_wind.head().T )

        # "S_INFO_WINDCODE","WIND_CODE" 两个是一样的
        col_list = ["S_INFO_WINDCODE","WIND_IND_CODE","ENTRY_DT","REMOVE_DT","CUR_SIGN"]
        df_ind_wind_stocks = df_ind_wind.loc[:, col_list ]
        # print("df_ind_wind")
        # print( df_ind_wind.head().T )

        #################################################################################
        ### df_ind_citics || 中信行业股票进出：CITICS_IND_CODE：b10l020100；
        # 中信行业分类标准v2.0.xlsx || C:\db_wind\data_adj
        # 中国A股中信行业分类[AShareIndustriesClassCITICS]
        temp_table = "AShareIndustriesClassCITICS"
        temp_path = self.path_wds+ temp_table
        file_name = "WDS_full_table_full_table_ALL.csv"
        # notes:如果不带列index值读取，,index_col=0 ，则第一列会变成index，但第一列是中信行业代码
        df_ind_citics = pd.read_csv( temp_path +"\\"+ file_name,encoding="gbk"  )
        print("df_ind_citics")
        print( df_ind_citics.head().T )

        # "S_INFO_WINDCODE","WIND_CODE" 两个是一样的 
        col_list = ["S_INFO_WINDCODE","CITICS_IND_CODE","ENTRY_DT","REMOVE_DT","CUR_SIGN"]
        df_ind_citics_stocks = df_ind_citics.loc[:, col_list ]

        # print("df_ind_citics")
        # print( df_ind_citics.head().T )

        #################################################################################
        ### df_ind_sw 申万行业股票进出：计算；|| 申万行业分类[AShareSWIndustriesClass]

        temp_table = "AShareSWIndustriesClass"
        temp_path = self.path_wds + temp_table
        file_name = "WDS_full_table_full_table_ALL.csv"
        # notes:如果不带列index值读取，,index_col=0 ，则第一列会变成index，但第一列是中信行业代码
        df_ind_sw = pd.read_csv( temp_path +"\\"+ file_name,encoding="gbk"  )
        print("df_ind_sw")
        print( df_ind_sw.head().T )

        # "S_INFO_WINDCODE","WIND_CODE" 两个是一样的 
        col_list = ["S_INFO_WINDCODE","SW_IND_CODE","ENTRY_DT","REMOVE_DT","CUR_SIGN"]
        df_ind_sw_stocks = df_ind_sw.loc[:, col_list ]

        # print("df_ind_sw" ) 
        # print( df_ind_sw.head().T )
        #####################################################################
        ### 导入3个行业分类的行业代码信息 | 截至202001的最新值
        ### Industry classification: Wind and gics
        '''
        indcode_wind	indname_CN	notes
        原始数据文件：Wind行业分类标准（2017版）.xls
            path= D:\TOUYAN\行业分类_产业链量化
        数据来源：http://wds.wind.com.cn/rdf/?#/main || 中国A股Wind行业分类[AShareIndustriesClass]
        整理后的数据文件：industries_class_wind.csv || 1234级分类同属一列
        file_name = industries_class_wind.csv 
        file_path = C:\db_wind\data_adj\industries_class
        '''
        file_wind = "industries_class_wind.csv"
        file_path = self.path_rc + "\\industries_class\\"
        df_indclass_wind = pd.read_csv(file_path + file_wind,encoding="gbk"   )

        ### Industry classification: citics 中信
        '''
        notes:中信行业分类2019的变更，会导致有的行业分类变没，例如2410，2420，2430及细分都变没，转成了2440，2450，2460
        Qs:是将历史所有行业分类用最新行业分类计算、还是可以沿用旧版本
        Ana：
        1,对所有中信新行业，找出对应的旧行业代码； 
            ||代码都存在indcode_wind，但弃用的行业要标识remove_status
        2,
        原始数据文件：industries_class_citics.csv；industries_class_citics.xlsx；中信行业分类标准v2.0.xlsx
        原始数据文件地址： C:\zd_zxjtzq\rc_reports_cs\行业分类_产业链量化
        3,columns:  
        citics_indcode_new citics_ind_new  citics_indcode_old citics_ind_old  
        4,notes:

        '''
        file_citics = "industries_class_citics.csv"
        file_path = self.path_rc + "\\industries_class\\"
        df_indclass_citics = pd.read_csv(file_path + file_citics,encoding="gbk"   )

        ### Industry classification: sw 申万
        '''
        1,columns:
        ClassCode	Name1	Name2	Name3	一级行业	二级行业	三级行业
        2,
        原始数据文件：industries_class_sw.csv；申万行业分类中英对照2014.xlsx
        原始数据文件地址： C:\zd_zxjtzq\rc_reports_cs\行业分类_产业链量化

        '''
        file_sw = "industries_class_sw.csv"
        file_path = self.path_rc + "\\industries_class\\"
        df_indclass_sw = pd.read_csv(file_path + file_sw , encoding="gbk"   )

        ### 导入沪深300指数数据 || # 中国A股指数日行情[AIndexEODPrices]
        ### notes:000300.SH 的价格数据是从20050601开始的，之前的S_DQ_CLOSE 数据不对但涨跌百分比S_DQ_PCTCHANGE是对的
        ### 但是从交易日的角度，可以从20020823开始计算。
        temp_table = "AIndexEODPrices"
        temp_path = self.path_wds + temp_table
        code_wind = "000300.SH"
        file_name = "WDS_S_INFO_WINDCODE_"+code_wind  +"_ALL.csv"
        # 不带列index值读取。
        df_csi300 = pd.read_csv( temp_path +"\\"+ file_name,encoding="gbk",index_col=0  )
        df_csi300 =df_csi300.drop(["OBJECT_ID","SEC_ID","OPDATE","OPMODE"],axis=1 )
        ### 主要是用于对应无市场行情的交易日，筛选后从20050601开始算。
        df_csi300 = df_csi300[ df_csi300["S_DQ_HIGH"] > 0  ]
        # "S_INFO_WINDCODE","TRADE_DT","CRNCY_CODE","S_DQ_PRECLOSE","S_DQ_OPEN","S_DQ_HIGH","S_DQ_LOW","S_DQ_CLOSE","S_DQ_CHANGE","S_DQ_PCTCHANGE"

        df_csi300.to_csv( self.path_rc+"csi300.csv",index=False )
        # print("df_csi300")
        # print( df_csi300.head().T )

        #################################################################################
        ### df_1s:每个股票的历史行业归属,"S_INFO_WINDCODE"没用只是为了避免df_1s只有1列变成series
        ### df_1s.index = 历史交易日 
        ### notes: df_csi300 的日期可能是乱序的
        df_csi300  = df_csi300.sort_values(by="TRADE_DT", ascending=True)
        df_1s = df_csi300.loc[:,["TRADE_DT","S_INFO_WINDCODE"] ]
        df_1s.index = df_csi300["TRADE_DT"]
        date_list = list( df_csi300["TRADE_DT"] )
        #################################################################################
        # 另一种date_list 计算方法，但"data_check_anndates.csv"包括周末
        ### date_list 导入20050101以来交易日，对每个交易日计算样本空间、可交易股票，按交易日保存
        # notes: data_check_anndates.csv 这里好像包括了周末和节假日，需要剔除掉。但也不好说，因为有的财务报表和
        # 公告披露日期是在周末，也许未来会有用的。
        # file_dates = "data_check_anndates.csv"
        # df_dates = pd.read_csv(self.file_path_admin + file_dates ,encoding="gbk")
        # date_list= df_dates["date"].values

        #####################################################################
        ### df_1d  || 寻找在20050601前已经上市的且 未退市的公司信息，todo并计算上市的时间；
        # pd.isnull() 判断是否为空值
        temp_date = int("20050601")

        for temp_i in df_A_des.index :
            temp_date2 = df_A_des.loc[temp_i, "S_INFO_DELISTDATE" ]
            temp_date3 = df_A_des.loc[temp_i, "S_INFO_LISTDATE" ]
            # print("temp_date2 ",temp_date2 ,pd.isnull(temp_date2) )
            # if (not pd.isnull(temp_date2) ) and  temp_date2 < temp_date :
            # 1, 剔除20050601前退市股票
            if temp_date2 < temp_date :
                df_A_des = df_A_des.drop(temp_i, axis=0 )
            # elif temp_date3 > temp_date : 
            #     df_A_des = df_A_des.drop(temp_i, axis=0 )
        
        ### 把列表内的股票分配给当日日数据文件 df_1d
        df_1d = df_A_des
        ### df_1d.index = 股票代码
        df_1d.index = df_A_des["S_INFO_WINDCODE"]
        code_list = list(df_A_des["S_INFO_WINDCODE"] )

        ### 输出各个df数据文件  
        # df_A_des date_list || 
        # 股票行业分类进出 ： df_ind_wind_stocks df_ind_citics_stocks df_ind_sw_stocks
        # 3个行业分类的行业代码信息： df_indclass_wind，df_indclass_citics ，df_indclass_sw
        # df_csi300 || 沪深300的历史行情文件，包括了20050601以来的交易日
        ### list 格式的变量
        # code_list 上市股票代码表 list
        # date_list 交易日期记录 list
        # 
        object_wds["df_A_des"] = df_A_des
        object_wds["date_list"] = date_list
        object_wds["code_list"] = code_list        
        # 
        object_wds["df_ind_wind_stocks"] = df_ind_wind_stocks
        object_wds["df_ind_citics_stocks"] = df_ind_citics_stocks
        object_wds["df_ind_sw_stocks"] = df_ind_sw_stocks
        # 
        object_wds["df_indclass_wind"] = df_indclass_wind
        object_wds["df_indclass_citics"] = df_indclass_citics
        object_wds["df_indclass_sw"] = df_indclass_sw
        # 
        object_wds["df_csi300"] = df_csi300
        object_wds["df_1d"] = df_1d

        return object_wds

    def match_ind_name(self, ind_code, indclass ) :
        ### 根据行业代码匹配行业分类表格内的行业名称、行业级数和是否弃用
        '''
        notes:输入的行业代码有可能不是最小一级的，这时候需要先判断所属的行业分类！~
        notes: ind_code 需要是str格式。
        str: ind_code ||
        str: indclass in [wind,citics,sw ]
        例子： wind ：6240401020 ； citics：'b10n010200' ；sw:6118010100
        OUTPUT:
        ind_code_4 = 6240401020 | 用于在wds行业表格里查询
        ind_code_s_4 = 40401020 | 用于在Excel和后续统计中方便汇总分析
        ind_code_3 = 6240401000 | 用于在wds行业表格里查询 "00"
        ind_code_s_4 = 404010 | 用于在Excel和后续统计中方便汇总分析
        
        wind_ind_code 有可能属于Wind、citics、sw的任何一个。
        在 行业代码[AShareIndustriesCode] 表里，有用的列有4个：
        INDUSTRIESCODE	INDUSTRIESNAME	LEVELNUM	USED
        行业代码 INDUSTRIESCODE
        行业名称 INDUSTRIESNAME
        级数     LEVELNUM || 2表示一级,3表示二级,4表示三级,5表四级
        是否有效 USED || 1=有效，0=无效
        ------
        Wind行业分类以62开头，
        6200000000  WIND一级，啥也没用
        6225000000	可选消费，对应LEVELNUM=2
        6225100000	汽车与汽车零部件	3	1
        6225101000	汽车零配件	4	1
        6225101010	机动车零配件与设备	5 1 
        ''' 

        ### 初始化输出变量
        dict_ind= {} 

        ### 原始wds 行业变动表 "AShareIndustriesCode\\"
        file_name = "WDS_full_table_full_table_ALL.csv"
        file_path = self.path_wds +"AShareIndustriesCode\\"
        # df_indclass_ALL = pd.read_csv( file_path + file_name ,encoding="gbk" )
        df_indclass_ALL = pd.read_csv( file_path + file_name  )
        # notes: wind对应的"INDUSTRIESALIAS"是中文拼音缩写，没用.但是中信行业分类citics里是有用的。
        # 中信"INDUSTRIESALIAS"的LEVELNUM=2、3、4对应的是36，3610，361010        
        col_list= ["INDUSTRIESCODE","INDUSTRIESNAME","LEVELNUM","USED","INDUSTRIESALIAS","MEMO"]         
        self.df_indclass_ALL = df_indclass_ALL.loc[:,col_list ]
        ##  这时有 11646行，占 364kb

        ### 细分不同"LEVELNUM"方便匹配
        df_indclass_5 = self.df_indclass_ALL[ self.df_indclass_ALL["LEVELNUM"]== 5 ]
        df_indclass_4 = self.df_indclass_ALL[ self.df_indclass_ALL["LEVELNUM"]== 4 ]
        df_indclass_3 = self.df_indclass_ALL[ self.df_indclass_ALL["LEVELNUM"]== 3 ]
        df_indclass_2 = self.df_indclass_ALL[ self.df_indclass_ALL["LEVELNUM"]== 2 ]
        
        ############################################################################
        ### 1，匹配Wind行业分类 
        # notes: wind对应的"INDUSTRIESALIAS"是中文拼音缩写，没用
        if indclass == "wind" :
            ### 确定输入的ind_code所属行业分类
            temp_df0 = self.df_indclass_ALL [self.df_indclass_ALL["INDUSTRIESCODE"] ==  ind_code  ]
            
            temp_levelnum = temp_df0["LEVELNUM"].values[0]
            # type(temp_levelnum) is numpy.int64 
            # print("temp_levelnum ", temp_levelnum ,type(temp_levelnum)  )

            if temp_levelnum == 5 :
                ### 匹配Wind四级 |四级在wds表格里对应的是5，
                
                # 5561 lines, 217kb            
                ### 在 wind里匹配代码，如果找不到，则将其改成个股最新的行业代码，如果再出错，则显示错误信息
                temp_df = df_indclass_5 [df_indclass_5["INDUSTRIESCODE"] ==  ind_code  ]

                temp_i = temp_df.index.values[0]
                ### todo 由于Wind行业分类对应的是四级最细的那个，因此在 df_indclass_ALL 里匹配LEVELNUM=5 
                dict_ind["wind_ind_code_4"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["wind_ind_code_s_4"]= ind_code[2:]
                dict_ind["wind_ind_name_4"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["wind_used_4"]= temp_df.loc[temp_i, "USED" ]
                temp_levelnum = 4 

            if temp_levelnum == 4 : 
                ### 匹配Wind三级 || 把最后2个代码替换成 00 
                ind_code3= ind_code[:-2] + "00" 
                
                temp_df = df_indclass_4 [df_indclass_4["INDUSTRIESCODE"] ==  ind_code3 ]
                temp_i = temp_df.index.values[0]
                ### todo 由于Wind行业分类对应的是四级最细的那个，因此在 df_indclass_ALL 里匹配LEVELNUM=5 
                dict_ind["wind_ind_code_3"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["wind_ind_code_s_3"]= ind_code[2:-2]
                dict_ind["wind_ind_name_3"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["wind_used_3"]= temp_df.loc[temp_i, "USED" ]
                temp_levelnum = 3 
            
            if temp_levelnum == 3 :
                ### 匹配Wind二级 || 把最后4个代码替换成 0000 
                ind_code2= ind_code[:-4] + "0000" 
                
                temp_df = df_indclass_3 [df_indclass_3["INDUSTRIESCODE"] ==  ind_code2 ]
                temp_i = temp_df.index.values[0]
                ### todo 由于Wind行业分类对应的是四级最细的那个，因此在 df_indclass_ALL 里匹配LEVELNUM=5 
                dict_ind["wind_ind_code_2"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["wind_ind_code_s_2"]= ind_code[2:-4]
                dict_ind["wind_ind_name_2"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["wind_used_2"]= temp_df.loc[temp_i, "USED" ]
                temp_levelnum = 2 

            if temp_levelnum == 2 :
                ### 匹配Wind一级 || 把最后6个代码替换成 000000 
                ind_code1= ind_code[:-6] + "000000" 
                
                temp_df = df_indclass_2 [df_indclass_2 ["INDUSTRIESCODE"] ==  ind_code1 ]
                temp_i = temp_df.index.values[0]
                ### todo 由于Wind行业分类对应的是四级最细的那个，因此在 df_indclass_ALL 里匹配LEVELNUM=5 
                dict_ind["wind_ind_code_1"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["wind_ind_code_s_1"]= ind_code[2:-6]
                dict_ind["wind_ind_name_1"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["wind_used_1"]= temp_df.loc[temp_i, "USED" ]

        ############################################################################
        ### 2，匹配中信行业分类 citics 
        '''中信的行业分类中，"INDUSTRIESALIAS"对应36、3610、361020，分别对应LEVELNUM=2,3,4
        '''
        if indclass == "citics" :    
            ''' 匹配中信三级 | 三级在wds表格里对应的是4
            # 例子：新的中信行业分类第4个字符n等字母，但旧版本是4等数字
                    S_INFO_WINDCODE CITICS_IND_CODE  ENTRY_DT   REMOVE_DT  CUR_SIGN
            2949       000005.SZ      b104020500  20191201         NaN         1
            3763       000005.SZ      b104020100  20180227  20191130.0         0
            4273       000005.SZ      b10n010200  20030101  20180226.0         0
            例子2：
            b10n000000	房地产		2	1	42
            b10n020000000000	房地产服务	3	1	4220
            b10n010100000000	住宅物业开发	4	1	421010
            ind_code ~对应 CITICS_IND_CODE
            识别数字方法： ind_code[3].isdigit() ||用不着
            # 在 df_indclass_ALL 里对应的是 "b10j030300" , "b10n010200"
            规律是在尾部加6个0, "000000"，注意，6个0的代码通常都是弃用的。
            '''
            ### 确定输入的ind_code所属行业分类
            temp_df0 = self.df_indclass_ALL [self.df_indclass_ALL["INDUSTRIESCODE"] ==  ind_code  ]
            if len( temp_df0.index ) < 1 :
                # for case temp_indcode_citics= b10n010200, we neew b10n010200000000
                temp_ind_code = ind_code + "000000"
                temp_df0 = self.df_indclass_ALL [self.df_indclass_ALL["INDUSTRIESCODE"] ==  temp_ind_code  ]
                temp_levelnum = temp_df0["LEVELNUM"].values[0]
            else : 
                temp_levelnum = temp_df0["LEVELNUM"].values[0]
            # type(temp_levelnum) is numpy.int64 
            print("temp_levelnum ", temp_levelnum ,type(temp_levelnum)  )

            if temp_levelnum == 4 : 
                ### 匹配中信三级 | 三级在wds表格里对应的是4
                ind_code3 = ind_code 
                
                ### 在 wind里匹配代码，如果找不到，则将其改成个股最新的行业代码，如果再出错，则显示错误信息
                temp_df = df_indclass_4 [df_indclass_4["INDUSTRIESCODE"] ==  ind_code3  ]
                # 对于部分行业，如住宅物业开发，必须要加"000000"才能匹配到代码。
                if len(temp_df.index ) < 1 :
                    ind_code3 = ind_code + "000000"
                    temp_df = df_indclass_4 [df_indclass_4["INDUSTRIESCODE"] ==  ind_code3  ]

                print("ind_code ",ind_code3  ) 
                # print("temp_df ",temp_df  )
                temp_i = temp_df.index.values[0]
                ### citics行业分类3级对应的是 df_indclass_ALL 里 LEVELNUM=4
                dict_ind["citics_ind_code_3"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["citics_ind_code_s_3"]= temp_df.loc[temp_i, "INDUSTRIESALIAS" ]
                dict_ind["citics_ind_name_3"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["citics_used_3"]= temp_df.loc[temp_i, "USED" ]
                temp_levelnum = 3 

            if temp_levelnum ==3 :
                ### 匹配中信二级 | 二级在wds表格里对应的是3
                # notes：如果是已经不用的三级行业分类，下边方法换成二级会找不到??
                #2: from b10j010100000000 白酒 to b10j010000000000 酒类，把01换成00
                # b10n020000000000	房地产服务 3||	
                # b10n010100000000	住宅物业开发 4
                ind_code2 = ind_code[:-4] + "0000"   
                temp_df = df_indclass_3 [df_indclass_3["INDUSTRIESCODE"] ==  ind_code2 ]
                if len(temp_df.index ) < 1 :
                    ind_code2 = ind_code[:-4] + "0000" + "000000"
                    temp_df = df_indclass_3 [df_indclass_3["INDUSTRIESCODE"] ==  ind_code2  ]
                print("ind_code ",ind_code3  ) 
                # print("temp_df ",temp_df  )
                temp_i = temp_df.index.values[0]
                dict_ind["citics_ind_code_2"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["citics_ind_code_s_2"]= temp_df.loc[temp_i, "INDUSTRIESALIAS" ]
                dict_ind["citics_ind_name_2"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["citics_used_2"]= temp_df.loc[temp_i, "USED" ]
                temp_levelnum =2 
            
            if temp_levelnum == 2 :
                ### 匹配中信一级 | 一级在wds表格里对应的是2
                #1: from b10j010100000000 白酒 to b10j000000	食品饮料
                ind_code1 = ind_code[:4] + "000000" 
                # equals to ind_code1 = ind_code[:-6] + "000000"  
                
                temp_df = df_indclass_2 [df_indclass_2["INDUSTRIESCODE"] ==  ind_code1 ]
                if len(temp_df.index ) < 1 :
                    ind_code1 = ind_code[:4] + "000000" + "000000"
                    temp_df = df_indclass_2 [df_indclass_2["INDUSTRIESCODE"] ==  ind_code1  ]          
                print("ind_code ",ind_code1  ) 
                # print("temp_df ",temp_df  )
                temp_i = temp_df.index.values[0]
                ### todo 由于Wind行业分类对应的是四级最细的那个，因此在 df_indclass_ALL 里匹配LEVELNUM=5 
                dict_ind["citics_ind_code_1"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["citics_ind_code_s_1"]= temp_df.loc[temp_i, "INDUSTRIESALIAS" ]
                dict_ind["citics_ind_name_1"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["citics_used_1"]= temp_df.loc[temp_i, "USED" ]
            
        ############################################################################
        ### 3，匹配申万行业分类 sw
        '''申万的行业分类中，不能用"INDUSTRIESALIAS"，应该用"MEMO"

        INDUSTRIESCODE	INDUSTRIESNAME	LEVELNUM	USED	INDUSTRIESALIAS	SEQUENCE
        	MEMO
        6118000000	房地产	2	1	801180	21	430000
        6118010000	房地产开发Ⅱ	3	1	801181	1	430100
        6118010100	房地产开发Ⅲ	4	1	851811	1	430101
        6118020000	园区开发Ⅱ	3	1	801182	2	430200
        6118020100	园区开发Ⅲ	4	1	851821	1	430201
         
        sw:6118010100
        '''
        if indclass == "sw" : 
            ### 确定输入的ind_code所属行业分类
            temp_df0 = self.df_indclass_ALL [self.df_indclass_ALL["INDUSTRIESCODE"] ==  ind_code  ]
            temp_levelnum = temp_df0["LEVELNUM"].values[0]
            # type(temp_levelnum) is numpy.int64 
            print("temp_levelnum ", temp_levelnum ,type(temp_levelnum)  )

            if temp_levelnum == 4 :
                ### 匹配申万三级 | 三级在wds表格里对应的是4
                ind_code3 = ind_code  
                
                temp_df = df_indclass_4 [df_indclass_4["INDUSTRIESCODE"] ==  ind_code3  ] 

                print("ind_code ",ind_code3  ) 
                # print("temp_df ",temp_df  )
                temp_i = temp_df.index.values[0]
                ### citics行业分类3级对应的是 df_indclass_ALL 里 LEVELNUM=4
                dict_ind["sw_ind_code_3"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["sw_ind_code_s_3"]= temp_df.loc[temp_i, "MEMO" ]
                dict_ind["sw_ind_name_3"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["sw_used_3"]= temp_df.loc[temp_i, "USED" ] 
                temp_levelnum = 3 

            if temp_levelnum ==3 :
                ### 匹配申万二级 | 二级在wds表格里对应的是3
                # notes:sw三级换二级是对应最后四位数字
                ind_code2 = ind_code[:-4] +"0000" 
                
                temp_df = df_indclass_3 [df_indclass_3["INDUSTRIESCODE"] ==  ind_code2 ] 

                print("ind_code ",ind_code2  ) 
                # print("temp_df ",temp_df  )
                temp_i = temp_df.index.values[0]
                ### citics行业分类3级对应的是 df_indclass_ALL 里 LEVELNUM=4
                dict_ind["sw_ind_code_2"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["sw_ind_code_s_2"]= temp_df.loc[temp_i, "MEMO" ]
                dict_ind["sw_ind_name_2"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["sw_used_2"]= temp_df.loc[temp_i, "USED" ] 
                temp_levelnum = 2 
            
            if temp_levelnum ==2 :
                ### 匹配申万一级 | 一级在wds表格里对应的是3
                # notes:sw二级换一级是对应第5，6位数字，或者说最后6，5位数字
                ind_code1 = ind_code[:-6] +"000000"             
                temp_df = df_indclass_2 [df_indclass_2["INDUSTRIESCODE"] ==  ind_code1 ] 

                print("ind_code ",ind_code1  ) 
                # print("temp_df ",temp_df  )
                temp_i = temp_df.index.values[0]
                ### citics行业分类3级对应的是 df_indclass_ALL 里 LEVELNUM=4
                dict_ind["sw_ind_code_1"]= temp_df.loc[temp_i, "INDUSTRIESCODE" ]
                dict_ind["sw_ind_code_s_1"]= temp_df.loc[temp_i, "MEMO" ]
                dict_ind["sw_ind_name_1"]= temp_df.loc[temp_i, "INDUSTRIESNAME" ]
                dict_ind["sw_used_1"]= temp_df.loc[temp_i, "USED" ] 


        # 返回一个字典，包含行业信息
        return dict_ind


    def get_ind_period(self,obj_dates,code_stock,date_start,date_end ) :
        ### 根据期初和期末日期获取股票所属行业，基于已经计算好了的数据 
        '''
        1,读取date_list
        2,获取date_start 之后的最近交易日；获取date_end之前的最近交易日
        3,读取date_start~date_end 之间的行业属性

        date_start should be "20150331"
        notes:如果没有行业分类数据怎么办？
        '''
        ### 导入日期信息
        de_code = obj_dates["code"]
        date_list_0 = obj_dates["date_start"]
        date_list_1 = obj_dates["date_end"]
        date_list = obj_dates["date_list"]
        ### 根据date_list 找到date_start前第一个交易日
        date_list_sub = [temp_date for temp_date in date_list if temp_date >= int(date_start) ]
        date_list_sub = [temp_date for temp_date in date_list_sub if temp_date <= int(date_end) ]
        
        ### 导入股票代码全历史行业分类 ||  df_600151.SH
        file_name = "df_"+ code_stock + ".csv"
        ### Initialize output industry object 
        object_ind ={}

        # print("Debug====",code_stock, date_start ,date_end  )
        # print( self.path_rc_ind + file_name )
        # print( date_list_sub[:3],date_list_sub[-3:] )
        # print( date_list[:3],date_list[-3:] )

        ### 判断文件是否存在：
        import os
        if os.path.exists(self.path_rc_ind + file_name ) : 
            df_s_ind = pd.read_csv(self.path_rc_ind + file_name,encoding="gbk")
            # print("df_s_ind ", df_s_ind.head().T  )

            ### get industry info for date_start
            # 估计 "TRADE_DT" 这里是int or float type
            
            date_start_trade = date_list_sub[0] 
            date_end_trade = date_list_sub[-1] 

            #notes：对于开始日期，从精确匹配转换成最近日期匹配，200313，对于190930，股票601077上市首日为191029
            df_s_ind_start = df_s_ind[df_s_ind["TRADE_DT"]>= date_start_trade ]
            print(" df_s_ind_start",  df_s_ind_start)
            # 对于结束日期，通常已经有了所属行业分类
            df_s_ind_end = df_s_ind[df_s_ind["TRADE_DT"]== date_end_trade ]
            
            ### notes:对于002050.SZ，要提取地日期是20050401-20050631
            # print(code_stock,date_start,date_end, date_start_trade,date_end_trade )

            ### get industry info from date_list_sub[0] to date_list_sub[-1]
            # 估计 "TRADE_DT" 这里是int or float type
            date_start_trade = date_list_sub[0] 
            date_end_trade = date_list_sub[-1]      
            df_s_ind_period = df_s_ind[df_s_ind["TRADE_DT"].isin( date_list_sub) ]
            # print("df_s_ind_period ", df_s_ind_period.head().T  )
            if df_s_ind_start.empty :
                object_ind["is_ind_start"] = 0
            else :
                object_ind["is_ind_start"] = 1
                object_ind["df_s_ind_start"] = df_s_ind_start

            if df_s_ind_end.empty :
                object_ind["is_ind_end"] = 0
            else :
                object_ind["is_ind_end"] = 1
                object_ind["df_s_ind_end"] = df_s_ind_end
            
            object_ind["df_s_ind_period"] = df_s_ind_period

        else :
            object_ind["is_ind_start"] = 0
            object_ind["is_ind_end"] = 0            
            

        return object_ind


    def get_ind_date(self,code_list,date_end,if_all_codes="1" ) :
        ### 给定交易日获取清单中股票的所属行业，基于已经计算好了的数据
        '''
        notes:只能识别上海或深圳股票。
        1,if_all_codes="1" means directly import all code list from inside
        2,旧方法是先计算所有个股的全历史行业分类文件，文件较大且效率低；200321改成从梳理过的股票行业分类进出表df_ind_code_stock_io.csv中判断
        file_name = "df_ind_code_stock_io.csv"
        file_path = "C:\db_wind\data_adj\industries_class"
        3，600087.SH于2014年退市，对应行业分类只有sw，并且纳入日期20080602，却是2005年的指数成分
            对于这种情况，将其归类于 其他 行业分类
        
        last 200330 | since 200110
        1,对于给定股票，获取
        2,获取date_start 之后的最近交易日；获取date_end之前的最近交易日
        3,读取date_start~date_end 之间的行业属性

        date_start should be "20150331"
        notes:如果没有行业分类数据怎么办？ | date_in=date_end
        derived from def get_ind_period()
        '''
        ###########################################################################################
        ### 导入基础信息
        if if_all_codes == "1" :
            # df with column as code 
            code_df = pd.read_csv(self.file_path_admin + "code_list.csv" )
            
            code_list = list( code_df["code"].values )

        ### 导入日期信息| 新方法下似乎不需要了
        # file_date= "date_list.csv"
        # # 原始文件无column，若直接导入会把第一个当作column，若index_col=0则日期会变成index
        # date_list= pd.read_csv(self.file_path_admin + file_date ,index_col=0)
        # ### transform from df to list 
        # date_list = list( date_list.index )
                
        # ### 根据date_list 找到date_start前第一个交易日
        # date_list_sub = [temp_date for temp_date in date_list if temp_date <= int(date_end ) ]
        # date_list_sub.sort()
        # date_match = date_list_sub[-1]
        # print("date_list ", date_end,date_match ) 
        
        ###########################################################################################
        ### 导入梳理过的股票行业分类进出表df_ind_code_stock_io.csv
        file_ind_io_name = "df_ind_code_stock_io.csv"
        file_ind_io_path = self.path_rc + "industries_class\\"
        df_ind_code_stock_io = pd.read_csv( file_ind_io_path + file_ind_io_name,encoding="gbk" )
        print(file_ind_io_path + file_ind_io_name )

        ### code_list 要剔除非A股 
        # list_del = []
        # for temp_code in code_list : 
        #     if temp_code[-3:] not in [".SH",".SZ"]:
        #         list_del.append( temp_code )
        # for j1 in list_del : 
        #     code_list.remove(j1) 

        ### for all codes
        count_stock =0 
        for code_stock in code_list :
            # print("Working on code_stock",code_stock)
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
                for item in col_list  :
                    # "wind_ind_code_3
                    df_out.loc[ temp_index_fix ,item] =  np.nan
                
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

        object_ind ={}
        object_ind["code_list"] = code_list
        if "df_s_ind_out" in locals() or "df_s_ind_out" in globals() :
            ### save df to specific directory 
            df_s_ind_out.to_csv(self.path_rc_ind + "df_code_list_"+ str(date_end) + ".csv" ,encoding="gbk")  
            ### Initialize output industry object             
            object_ind["df_s_ind_out"] = df_s_ind_out
            object_ind["temp_index"] = temp_index

        return object_ind 


    def cal_stock_indclass(self,output="list") :
        '''计算个股历史行业变动和最新行业分类:将3种行业分类代码和中文值赋给对应的股票
        derived from test_wds_data_transform.py line 176-425
        对3个行业分类计算1~4级的代码，并单独保存成列值 

        output:
        1,df_stock_indclass:将3种行业分类代码和中文值赋给对应的股票
        2,df_1d:每个交易日所有股票的行业归属
        3,df_1s:每个股票的历史行业归属;3,df_ind_ALL:所有股票的行业归属

        逻辑：
        1，对于20050601开始到20200114的每个交易日：
        2，对于第一个交易日，导入股票信息df_A_des、按照3个行业分类计算所属行业。
        2.1,寻找在20050601前已经上市的且 未退市的公司，并计算上市的时间；
        3，对每个股票，用行业数据生成全历史行业分类数据
        3.1，对于wind行业，获取在20050601前已经上市的公司，并计算当日的行业分类；
        3.2，匹配中信行业分类；
        3.3，匹配申万行业分类；
        4,对于每个行业分类，匹配新旧2种行业分类
        4.1，Wind：1~4级行业代码、行业名称、
        4.2，citics：1~3级新行业代码、新行业名称、旧行业代码、旧行业。
        4.3，sw:1~3 级行业代码、行业名称、行业名称英文。
    
        notes:output="list" or "":仅计算全部股票进出表，不计算和保存个股全历史数据
            output="by_dates" :计算全部股票进出表，也计算和保存个股全历史数据
        notes:600061.SH国投资本发生行业分类变化时，
        中信、Wind在20150127变更行业、但申万竟然在20160630才变更，说明了申万行业分类的不靠谱。
        '''
        if output== "" :
            output="list" 
        elif not output in ["list", "by_dates"] :
            print("Error for wrong output parameters.")
            asd
        #####################################################################
        ### 导入各类df ：1，日期序列、股票代码序列；2，个股的3种行业分类变动表{wind,citics,sw};3,人工梳理后的3种行业分类，例如
        # industries_class_wind.csv；D:\db_wind\data_adj\industries_class
        object_wds = self.cal_df_ind()

        code_list = object_wds["code_list"]
        print("Length of code_list", len(code_list)  ) 

        date_list = object_wds["date_list"]
        date_last = date_list[-1]
        ### 每种行业分类进出数据都有单独的文件
        df_ind_wind_stocks = object_wds["df_ind_wind_stocks"] 
        df_ind_citics_stocks = object_wds["df_ind_citics_stocks"] 
        df_ind_sw_stocks = object_wds["df_ind_sw_stocks"] 
        # df_1d  || 寻找在20050601前已经上市的且 未退市的公司信息 
        df_1d = object_wds["df_1d"] 

        ### 定义所有股票在不同行业分类进出表中的记录
        # col_list 股票代码、行业分类、行业分类代码、纳入日期、调出日期
        col_list = ["wind_code","ind_class","ind_code","ENTRY_DT","REMOVE_DT"]
        df_ind_code_stock_io = pd.DataFrame(index=[0], columns=col_list )
        index_io = 0 
        ### 定义最新日期所有股票的不同级别行业分类 | col_list = ["wind_code","date_last"]
        df_ind_code_stock_last = pd.DataFrame(code_list, columns=["wind_code"],index=code_list )
        df_ind_code_stock_last["date_last"] = date_last
        
        #####################################################################
        ### 2，按照3个行业分类计算所属行业。
        ### 3，对每个股票，用行业数据生成全历史行业分类数据
        ### 4,对于每个行业分类，匹配新旧2种行业分类 
        ### notes:是有股票存在没有行业分类记录的，citic、sw都有。
        count= 0
        count_stock = 0 
        # code_list = code_list[3640:]
        # input1= input("Check here since choose only part of stocks")
        path_ind_class = self.path_rc +"industries_class\\"

        for temp_code in code_list :
            print("Working on code ", temp_code)
            #####################################################################
            ### 3.1，对于Wind|gics行业，获取在20050601前已经上市的公司，并计算当日的行业分类； 
            '''
            notes 注意：
            1,000005.SZ的早期行业分类就遇到了没有匹配行业代码的问题，6240401020
            之后该股票换成了 6220201050。由于匹配表用的是wind2017年版本，因此2017年之前的行业代码如果弃用
            我们都没有。因此需要先判断历史行业分类是否在2017版wind行业表里。
            2,temp_gics 可能有1、2、3···列，第一列是纳入+退出、第二是纳入+退出，···最后一列是纳入+NaN
            3,603739.SH遇到了上市前有行业变动的情况，通常默认只有最后一列是有NaN的移入值。
            解决办法，判断"ENTRY_DT"是否NaN
            '''

            temp_wind = df_ind_wind_stocks[  df_ind_wind_stocks["S_INFO_WINDCODE"]== temp_code ]
            ### 3.1.1 赋值：单只股票、全日期
            # 要按纳入日期升序排列
            # notes："ENTRY_DT"\"REMOVE_DT"都可能是周末或节假日！！！
            if len( temp_wind.index ) > 0 :
                temp_wind = temp_wind.sort_values(by="ENTRY_DT", axis=0, ascending=True)
                # print("temp_wind ", temp_wind)
                # initilize df_1s 
                df_1s= pd.DataFrame( index = date_list  )
                for temp_i in temp_wind.index :  
                    # 每个 temp_wind.loc[temp_i, "ENTRY_DT" ] 对应一个日期
                    if output== "list" :
                        ### 只需要对进出单日的code计算所属行业即可
                            # 获取字符串版本的wind行业代码
                            temp_indcode_wind = str( temp_wind.loc[temp_i, "WIND_IND_CODE" ] ) 
                            
                            ### 对3个行业分类计算1~4级的代码，匹配对应的中文行业分类名
                            ### 匹配行业代码、简化代码、名称
                            dict_ind = self.match_ind_name( temp_indcode_wind, "wind" ) 
                            
                            ### 将行业代码、简化代码、名称存入 行业分类进出表和 最新日期个股行业分类
                            for key_word in dict_ind :                
                                #  行业分类进出表
                                df_ind_code_stock_last.loc[temp_code, key_word ] = dict_ind[key_word]
                                # 最新日期个股行业分类
                                df_ind_code_stock_io.loc[index_io,key_word ] = dict_ind[key_word]
                            
                            # 最新日期个股行业分类
                            # col_list = ["wind_code","ind_class","ind_code","ENTRY_DT","REMOVE_DT"]
                            df_ind_code_stock_io.loc[index_io,"wind_code" ] = temp_wind.loc[temp_i, "S_INFO_WINDCODE" ]
                            df_ind_code_stock_io.loc[index_io,"ind_class" ] = "wind"
                            df_ind_code_stock_io.loc[index_io,"ind_code" ] = temp_indcode_wind
                            df_ind_code_stock_io.loc[index_io,"ENTRY_DT"] = temp_wind.loc[temp_i, "ENTRY_DT" ]
                            df_ind_code_stock_io.loc[index_io,"REMOVE_DT" ] = temp_wind.loc[temp_i,"REMOVE_DT" ]
                            # notes：index_io是每一条进出记录就要更新一次
                            index_io +=1 

                    elif output== "by_dates" :
                        ### 对每一次行业分类变动，计算对应的日期区间 date_list2 
                        # notes:大部分股票应该只有一列，意味着上市以来属于同一个行业；那么date_out=NaN
                        # df_1d里的股票在df_1s的日期范围内，不一定都是上市状态，但先进行行业划分是ok的。
                        if pd.isnull( temp_wind.loc[temp_i, "REMOVE_DT" ] ) :  
                            date_in = int(temp_wind.loc[temp_i, "ENTRY_DT" ])
                            # print(date_in )

                            date_list_wind = [x for x in date_list if x >= date_in   ]   
                            # print("date_list_wind ", date_list_wind[:3] ,date_list_wind[-3:] )
                        else:
                            if not pd.isnull( temp_wind.loc[temp_i, "ENTRY_DT" ] ) :
                                ### 还有后续的行业变动
                                ### 需要先判断历史行业分类是否在2017版wind行业表里。如果匹配不了，则需要使用下一列的行业分类标准
                                # ex. 600061.SH     6225203030  19970519.0  20150127.0
                                date_in = int(temp_wind.loc[temp_i, "ENTRY_DT" ])
                                date_out= int(temp_wind.loc[temp_i, "REMOVE_DT" ])
                                # print(date_in,date_out )
                                date_list_wind = [x for x in date_list if x >= date_in   ]
                                date_list_wind = [x for x in date_list_wind if x <= date_out   ]
                                # print("date_list_wind ", date_list_wind[:3] ,date_list_wind[-3:] )

                        if not pd.isnull( temp_wind.loc[temp_i, "ENTRY_DT" ] ) :  
                            df_1s.loc[date_list_wind, "S_INFO_WINDCODE" ] =  temp_code
                            df_1s.loc[date_list_wind, "TRADE_DT" ] =  date_list_wind
                            ### 行业代码  "WIND_IND_CODE" 
                            df_1s.loc[date_list_wind, "WIND_IND_CODE" ] =  temp_wind.loc[temp_i, "WIND_IND_CODE" ] 

                            # 获取字符串版本的wind行业代码
                            temp_indcode_wind = str( temp_wind.loc[temp_i, "WIND_IND_CODE" ] ) 
                            
                            ### 对3个行业分类计算1~4级的代码，匹配对应的中文行业分类名
                            ### 匹配行业代码、简化代码、名称
                            dict_ind = self.match_ind_name( temp_indcode_wind, "wind" ) 
                            # print("dict_ind "  )

                            ### 将行业代码、简化代码、名称存入df_1s
                            for key_word in dict_ind :                
                                print( key_word,dict_ind[key_word]) 
                                df_1s.loc[date_list_wind, key_word ] = dict_ind[key_word]
                                
                                #  行业分类进出表
                                df_ind_code_stock_last.loc[temp_code, key_word ] = dict_ind[key_word]
                                # 最新日期个股行业分类
                                df_ind_code_stock_io.loc[index_io,key_word ] = dict_ind[key_word]
                                
                            # 最新日期个股行业分类
                            # col_list = ["wind_code","ind_class","ind_code","ENTRY_DT","REMOVE_DT"]
                            df_ind_code_stock_io.loc[index_io,"wind_code" ] = temp_wind.loc[temp_i, "S_INFO_WINDCODE" ]
                            df_ind_code_stock_io.loc[index_io,"ind_class" ] = "wind"
                            df_ind_code_stock_io.loc[index_io,"ind_code" ] = temp_indcode_wind
                            df_ind_code_stock_io.loc[index_io,"ENTRY_DT"] = temp_wind.loc[temp_i, "ENTRY_DT" ]
                            df_ind_code_stock_io.loc[index_io,"REMOVE_DT" ] = temp_wind.loc[temp_i,"REMOVE_DT" ]
                            index_io +=1 
                            

            else :
                print( "temp_wind ", temp_wind)


            #####################################################################
            ### 3.2，匹配中信行业分类；||结构和GICS一样的|| df_ind_citics_stocks.head().T
            temp_citics = df_ind_citics_stocks[  df_ind_citics_stocks["S_INFO_WINDCODE"]== temp_code ] 
            print("temp_citics ")
            print(temp_citics  )
            # notes："ENTRY_DT"\"REMOVE_DT"都可能是周末或节假日！！！
            if len( temp_citics.index ) > 0 :
                temp_citics = temp_citics.sort_values(by="ENTRY_DT", ascending=True)

                ### 继续沿用  df_1s    
                for temp_i in temp_citics.index :  
                     # 每个 temp_wind.loc[temp_i, "ENTRY_DT" ] 对应一个日期
                    if output== "list" :
                        ### 只需要对进出单日的code计算所属行业即可
                        temp_indcode_citics = str( temp_citics.loc[temp_i, "CITICS_IND_CODE" ] ) 
                        ### 匹配行业代码、简化代码、名称
                        dict_ind = self.match_ind_name( temp_indcode_citics, "citics" ) 
                                                
                        ### 将行业代码、简化代码、名称存入 行业分类进出表和 最新日期个股行业分类
                        for key_word in dict_ind :                
                            #  行业分类进出表
                            df_ind_code_stock_last.loc[temp_code, key_word ] = dict_ind[key_word]
                            # 最新日期个股行业分类
                            df_ind_code_stock_io.loc[index_io,key_word ] = dict_ind[key_word]
                        
                        # 最新日期个股行业分类
                        # col_list = ["wind_code","ind_class","ind_code","ENTRY_DT","REMOVE_DT"]
                        df_ind_code_stock_io.loc[index_io,"wind_code" ] = temp_citics.loc[temp_i, "S_INFO_WINDCODE" ]
                        df_ind_code_stock_io.loc[index_io,"ind_class" ] = "citics"
                        df_ind_code_stock_io.loc[index_io,"ind_code" ] = temp_indcode_citics
                        df_ind_code_stock_io.loc[index_io,"ENTRY_DT"] = temp_citics.loc[temp_i, "ENTRY_DT" ]
                        df_ind_code_stock_io.loc[index_io,"REMOVE_DT" ] = temp_citics.loc[temp_i,"REMOVE_DT" ]
                        # notes：index_io是每一条进出记录就要更新一次
                        index_io +=1 

                    elif output== "by_dates" :
                        ### notes:大部分股票应该只有一列，意味着上市以来属于同一个行业；那么date_out=NaN
                        ### df_1d里的股票在df_1s的日期范围内，不一定都是上市状态，但先进行行业划分是ok的。
                        if pd.isnull( temp_citics.loc[temp_i, "REMOVE_DT" ] ) :  
                            #  600061.SH     6240203020  20150128.0         NaN         1
                            date_in = int(temp_citics.loc[temp_i, "ENTRY_DT" ])
                            # print(date_in )
                            date_list_citics = [x for x in date_list if x >= date_in   ]          

                        else:
                            if not pd.isnull( temp_citics.loc[temp_i, "ENTRY_DT" ] ) :  
                                ### 还有后续的行业变动
                                # ex.  600061.SH     6225203030  19970519.0  20150127.0         0
                                date_in = int(temp_citics.loc[temp_i, "ENTRY_DT" ])
                                date_out= int(temp_citics.loc[temp_i, "REMOVE_DT" ])
                                # print(date_in,date_out )
                                date_list_citics = [x for x in date_list if x >= date_in   ]
                                date_list_citics = [x for x in date_list_citics if x <= date_out   ] 
                        
                        if not pd.isnull( temp_citics.loc[temp_i, "ENTRY_DT" ] ) :  
                            df_1s.loc[date_list_citics, "S_INFO_WINDCODE" ] =  temp_code
                            df_1s.loc[date_list_citics, "TRADE_DT" ] =  date_list_citics
                            df_1s.loc[date_list_citics, "CITICS_IND_CODE" ] =  temp_citics.loc[temp_i, "CITICS_IND_CODE" ]
                            
                            # 获取字符串版本的wind行业代码
                            temp_indcode_citics = str( temp_citics.loc[temp_i, "CITICS_IND_CODE" ] )
                            print("temp_indcode_citics ") 
                            print( temp_indcode_citics  ) 
                            ### 匹配行业代码、简化代码、名称
                            dict_ind = self.match_ind_name( temp_indcode_citics, "citics" ) 
                            print("dict_ind "  )
                            ### 将行业代码、简化代码、名称存入df_1s
                            for key_word in dict_ind :                
                                print( key_word,dict_ind[key_word]) 
                                df_1s.loc[date_list_citics, key_word ] = dict_ind[key_word]
                                        
                                #  行业分类进出表
                                df_ind_code_stock_last.loc[temp_code, key_word ] = dict_ind[key_word]
                                # 最新日期个股行业分类
                                df_ind_code_stock_io.loc[index_io,key_word ] = dict_ind[key_word]
                            
                            # 最新日期个股行业分类
                            # col_list = ["wind_code","ind_class","ind_code","ENTRY_DT","REMOVE_DT"]
                            df_ind_code_stock_io.loc[index_io,"wind_code" ] = temp_citics.loc[temp_i, "S_INFO_WINDCODE" ]
                            df_ind_code_stock_io.loc[index_io,"ind_class" ] = "citics"
                            df_ind_code_stock_io.loc[index_io,"ind_code" ] = temp_indcode_citics
                            df_ind_code_stock_io.loc[index_io,"ENTRY_DT"] = temp_citics.loc[temp_i, "ENTRY_DT" ]
                            df_ind_code_stock_io.loc[index_io,"REMOVE_DT" ] = temp_citics.loc[temp_i,"REMOVE_DT" ]
                            # notes：index_io是每一条进出记录就要更新一次
                            index_io +=1 

            else :
                print( "temp_citics ", temp_citics)

            #####################################################################
            ### 3.3，匹配申万行业分类；||申万行业分类的变动相比其他两个可能会比较慢
            temp_sw = df_ind_sw_stocks[  df_ind_sw_stocks["S_INFO_WINDCODE"]== temp_code ] 
            # notes："ENTRY_DT"\"REMOVE_DT"都可能是周末或节假日！！！
            if len( temp_sw.index ) > 0 :
                temp_sw = temp_sw.sort_values(by="ENTRY_DT", ascending=True)
                print("temp_sw")
                print(temp_sw ) 
                ### 继续沿用  df_1s    
                for temp_i in temp_sw.index :  
                    if output== "list" :
                        ### 只需要对进出单日的code计算所属行业即可
                        temp_indcode_sw = str( temp_sw.loc[temp_i, "SW_IND_CODE" ] ) 
                        ### 匹配行业代码、简化代码、名称
                        dict_ind = self.match_ind_name( temp_indcode_sw, "sw" ) 
                                                
                        ### 将行业代码、简化代码、名称存入 行业分类进出表和 最新日期个股行业分类
                        for key_word in dict_ind :                
                            #  行业分类进出表
                            df_ind_code_stock_last.loc[temp_code, key_word ] = dict_ind[key_word]
                            # 最新日期个股行业分类
                            df_ind_code_stock_io.loc[index_io,key_word ] = dict_ind[key_word]
                        
                        # 最新日期个股行业分类
                        # col_list = ["wind_code","ind_class","ind_code","ENTRY_DT","REMOVE_DT"]
                        df_ind_code_stock_io.loc[index_io,"wind_code" ] = temp_sw.loc[temp_i, "S_INFO_WINDCODE" ]
                        df_ind_code_stock_io.loc[index_io,"ind_class" ] = "sw"
                        df_ind_code_stock_io.loc[index_io,"ind_code" ] = temp_indcode_sw
                        df_ind_code_stock_io.loc[index_io,"ENTRY_DT"] = temp_sw.loc[temp_i, "ENTRY_DT" ]
                        df_ind_code_stock_io.loc[index_io,"REMOVE_DT" ] = temp_sw.loc[temp_i,"REMOVE_DT" ]
                        # notes：index_io是每一条进出记录就要更新一次
                        index_io +=1 

                    elif output== "by_dates" :
                        ### notes:大部分股票应该只有一列，意味着上市以来属于同一个行业；那么date_out=NaN
                        ### df_1d里的股票在df_1s的日期范围内，不一定都是上市状态，但先进行行业划分是ok的。
                        if pd.isnull( temp_sw.loc[temp_i, "REMOVE_DT" ] ) :  
                            #  600061.SH     6240203020  20150128.0         NaN         1
                            date_in = int(temp_sw.loc[temp_i, "ENTRY_DT" ])
                            # print(date_in )
                            date_list_sw = [x for x in date_list if x >= date_in   ]          
                            
                        else:
                            if not pd.isnull( temp_sw.loc[temp_i, "ENTRY_DT" ] ) :  
                                ### 还有后续的行业变动
                                # ex.  600061.SH     6225203030  19970519.0  20150127.0         0
                                date_in = int(temp_sw.loc[temp_i, "ENTRY_DT" ])
                                date_out= int(temp_sw.loc[temp_i, "REMOVE_DT" ])
                                # print(date_in,date_out )
                                
                                date_list_sw = [x for x in date_list if x >= date_in   ]
                                date_list_sw = [x for x in date_list_sw if x <= date_out   ] 
                        
                        if not pd.isnull( temp_sw.loc[temp_i, "ENTRY_DT" ] ) :  
                            df_1s.loc[date_list_sw, "S_INFO_WINDCODE" ] =  temp_code
                            df_1s.loc[date_list_sw, "TRADE_DT" ] =  date_list_sw
                            df_1s.loc[date_list_sw, "SW_IND_CODE" ] =  temp_sw.loc[temp_i, "SW_IND_CODE" ]
                            
                            # 获取字符串版本的wind行业代码
                            temp_indcode_sw = str( temp_sw.loc[temp_i, "SW_IND_CODE" ] )
                            print("temp_indcode_sw ") 
                            print( temp_indcode_sw  ) 
                            ### 匹配行业代码、简化代码、名称
                            dict_ind = self.match_ind_name( temp_indcode_sw, "sw" ) 
                            print("dict_ind "  )
                            ### 将行业代码、简化代码、名称存入df_1s
                            for key_word in dict_ind :                
                                print( key_word,dict_ind[key_word]) 
                                df_1s.loc[date_list_sw, key_word ] = dict_ind[key_word]           
                                #  行业分类进出表
                                df_ind_code_stock_last.loc[temp_code, key_word ] = dict_ind[key_word]
                                # 最新日期个股行业分类
                                df_ind_code_stock_io.loc[index_io,key_word ] = dict_ind[key_word]
                            
                            # 最新日期个股行业分类
                            # col_list = ["wind_code","ind_class","ind_code","ENTRY_DT","REMOVE_DT"]
                            df_ind_code_stock_io.loc[index_io,"wind_code" ] = temp_sw.loc[temp_i, "S_INFO_WINDCODE" ]
                            df_ind_code_stock_io.loc[index_io,"ind_class" ] = "sw"
                            df_ind_code_stock_io.loc[index_io,"ind_code" ] = temp_indcode_sw
                            df_ind_code_stock_io.loc[index_io,"ENTRY_DT"] = temp_sw.loc[temp_i, "ENTRY_DT" ]
                            df_ind_code_stock_io.loc[index_io,"REMOVE_DT" ] = temp_sw.loc[temp_i,"REMOVE_DT" ]
                            # notes：index_io是每一条进出记录就要更新一次
                            index_io +=1 

            else :
                print( "temp_sw ", temp_sw )

            #####################################################################
            ### df_1s 保存到个股信息文件，{全部股票保存到一个大的历史df_ind_all里,df_ind_all太大没必要}
            # if count == 0 :
            #     df_ind_ALL =df_1s
            #     count=1 
            # else :
            #     #notes:由于index都是一样的，因此必须忽略index
            #     df_ind_ALL = df_ind_ALL.append( df_1s ,ignore_index = True )

            #####################################################################
            ### Saved to csv file 

            if output== "by_dates" :
                df_1s.to_csv(path_ind_class+"df_"+ temp_code +".csv",index=False,encoding="gbk") 

            # 行业分类进出表, 最新日期个股行业分类            
            # notes:modified 201009,不应该每个股票代码io，应该每100只股票io
            if count_stock % 100 ==0 : 
                df_ind_code_stock_io.to_csv(path_ind_class+"df_ind_code_stock_io.csv",index=False,encoding="gbk") 
                df_ind_code_stock_last.to_csv(path_ind_class+"df_ind_code_stock_last.csv",index=False,encoding="gbk")  
                df_ind_code_stock_last.to_csv(path_ind_class+"df_ind_code_stock_"+ str(date_last) +".csv",index=False,encoding="gbk")  
            
            count_stock = count_stock + 1 

        ### 设置 object_wds作为输出对象 
        # ### 定义所有股票在不同行业分类进出表中的记录 | col_list 股票代码、行业分类、行业分类代码、纳入日期、调出日期 
        object_wds["df_ind_code_stock_io"] = df_ind_code_stock_io  
        ### 定义最新日期所有股票的不同级别行业分类 
        object_wds["df_ind_code_stock_last"] = df_ind_code_stock_last  
        
        ### 将 object_wds 中不同的变量保存至 D:\db_wind\data_adj\industries_class，并做好每个对象的说明文件 
        
        return object_wds

    def add_columns2table(self,table_name,temp_cols,file_list,file_path, para_keyword ) :
        '''对文件夹内文件名字进行统一处理，并讲列名称赋值
        notes：2001013发现用oracle数据库里可以直接获取列的英文名，因此这个就变得不那么重要了。
        before:
        1，股票日行情是在倒数第二个里插入“wds更新日期”
        2，股票日衍生行情时在倒数第二个插入“wds更新日期”，并且最后要删除一项
        ''' 
        for temp_file_name in file_list :
            ### 判断文件名及读取具体文件 
            print( temp_file_name )
            # print( temp_file_name[:4] == "WDS_" )
            # print(  temp_file_name[4:12] == "TRADE_DT" )
            # 判断是否wds的数据文件 且未经过调整{即不带_adj后缀}
            if_continue = 0
            if temp_file_name[:4] == "WDS_" and ("_adj" not in temp_file_name) :
                if_continue = 0
                ### notes:第一列Unnamed: 0
                temp_df = pd.read_csv( file_path + temp_file_name, index_col=0 )
                if len( temp_df.index ) > 10 :
                    if_continue = 1 

            if if_continue == 1 :
                if len( temp_df.index  ) >=1 :
                    temp_df.columns = temp_cols 

                    temp_df.to_csv(  file_path + temp_file_name, index=False) 
                    temp_file_name2 =  temp_file_name[:-4] +"_adj"+ temp_file_name[-4:]
        return 1

    def import_fund_list(self,temp_date_obs,date_q_end,date_q_start) :
        ### 根据季末日期获取基金基础信息列表
        '''主要是拆分import_df_fund的功能 
        ALL type of funds  ['混合型', '债券型', '货币市场型', '股票型', '保本型', '商品型', '另类投资型']
        notes:
        df_fund_des 只反映最新日期的基金信息，但2002年上市的基金x，在2019年的属性和2005年可能完全不同，因此还是要以持仓数据里的为准
        col_list2=["F_INFO_WINDCODE","F_INFO_NAME","F_INFO_CORP_FUNDMANAGEMENTCOMP","F_INFO_FIRSTINVESTTYPE","IS_INDEXFUND"]
        
        '''
        #####################################################################
        ### 读取基金基础信息，例如所属基金公司 || 中国共同基金基本资料[ChinaMutualFundDescription]
        temp_table= "ChinaMutualFundDescription"
        temp_file_name = "WDS_full_table_full_table_ALL.csv"
        # notes 用,encoding="gbk"反而乱码。
        df_fund_des = pd.read_csv(self.path_wds +temp_table+"\\"+temp_file_name )
        # print("ALL type of funds ", list(df_fund_des["F_INFO_FIRSTINVESTTYPE"].drop_duplicates()) )
        
        '''第八列 是粗的基金类型：
        df_fund_type= df_fund_des["8"].drop_duplicates()
        df_fund_type：{  混合型, 债券型,货币市场型,股票型,保本型,商品型,另类投资型}
        第四十列是：是否指数基金
        26th：benchmark
        27th：存续状态：'101001000' 已上市; '101002000' 摘牌; '101003000' 发行
        共有10937条记录，保留已经上市后9138只，剔除指数基金后7601
        股票型基金：df_fund_des3[df_fund_des3["8"]=="股票型" ] 共有465只
        混合型：3505 ； 债券型：2879
        notes:部分基金代码是会有重合，例如163417.OF 和163417.SZ
        '''

        ### TODO:要匹配在 date_q_start 前成立的基金 || # dt.datetime.strptime(temp_date_str,"%Y%m%d")
        # 例：005156.OF在180316成立并开始建仓，基本上3个月后净值显示建仓完毕
        # 成立日期 "F_INFO_SETUPDATE" 加3个月！！
        # <class 'numpy.float64'> 0    20180316.0
        # to int,to str, then change to datetime ,pd.to_datetime(normal_dates['Date'],format='%m/%d/%Y')
        import datetime as dt 
        df_fund_des["date"] = df_fund_des["F_INFO_SETUPDATE"].map(lambda x: -1 if str(x)=='nan' else int(x))
        df_fund_des =df_fund_des[ df_fund_des["date"] != -1 ]
        df_fund_des["date"] = df_fund_des["date"].map(lambda x: "20990101" if str(x)=='nan' else str(x))
        
        df_fund_des =df_fund_des[ df_fund_des["date"] != "20990101" ] 
        df_fund_des["date"] = pd.to_datetime(df_fund_des["date"], format='%Y%m%d')
        ### 要新增column"date_plus3m" |
        # https://www.jianshu.com/p/96ea42c58abe
        df_fund_des["date_plus3m"] = df_fund_des["date"] +pd.DateOffset(months=3)
        ### "date_plus3m" :from dt to str to num
        df_fund_des["date_plus3m_int"] = df_fund_des["date_plus3m"].apply(lambda x: int(x.strftime("%Y%m%d")) )
        
        ### 保留在date_end前满足上市3个月的基金
        df_fund_des = df_fund_des[ df_fund_des["date_plus3m_int"] <= int(date_q_end) ]

        df_fund_des2 = df_fund_des [df_fund_des["F_INFO_STATUS"] == 101001000 ]
        ### we want fund for 3 types:"股票型"+index,
        df_fund_des_index = df_fund_des2 [df_fund_des2["IS_INDEXFUND"] == 1 ]
        df_fund_des_stock_index = df_fund_des_index [df_fund_des_index["F_INFO_FIRSTINVESTTYPE"] == "股票型" ]

        ### "股票型"+not index,"混合型"+not index
        df_fund_des2 = df_fund_des2 [df_fund_des2["IS_INDEXFUND"] == 0 ] 
        df_fund_des_stock_active = df_fund_des2[df_fund_des2["F_INFO_FIRSTINVESTTYPE"]== "股票型" ]
        df_fund_des_mixed_active = df_fund_des2[df_fund_des2["F_INFO_FIRSTINVESTTYPE"]== "混合型" ]

        #####################################################################
        ### 读取基金持仓信息，读取文件判断文件内是否有个目标基金的更新日期
        ### 根据日期获取基金持仓
        '''
        1，基金分类：
        1.1，中国共同基金基本资料[ChinaMutualFundDescription] 中只有最基础的分类
        1.2，细致的分类和变动应该参考：F_INFO_FIRSTINVESTTYPE，对应:ChinaMutualFundSector(中国Wind基金分类)
            分类依据：银河基金分类 。 idea：按最新的分类好了；不想按股票行业分类那样花太多时间。

        2，按日期获取基金持仓信息
        C:\db_wind\ChinaMutualFundStockPortfolio

        col_list = ["OBJECT_ID","S_INFO_WINDCODE","F_PRT_ENDDATE","CRNCY_CODE","S_INFO_STOCKWINDCODE","F_PRT_STKVALUE","F_PRT_STKQUANTITY","F_PRT_STKVALUETONAV","F_PRT_POSSTKVALUE","F_PRT_POSSTKQUANTITY","F_PRT_POSSTKTONAV","F_PRT_PASSTKEVALUE","F_PRT_PASSTKQUANTITY","F_PRT_PASSTKTONAV","ANN_DATE","STOCK_PER","FLOAT_SHR_PER","op_date","useless"]
        '''
        table_name = "ChinaMutualFundStockPortfolio"

        # 按持仓日期
        #Qs:最后会有日期，比较讨厌！！ 
        # file_date1 = "WDS_F_PRT_ENDDATE_20190930_ALL.csv" 
        file_date_q_start = "WDS_F_PRT_ENDDATE_"+ date_q_start +"_ALL.csv"
        file_date_q_end = "WDS_F_PRT_ENDDATE_"+ date_q_end     +"_ALL.csv"

        # 按披露日期 WDS_ANN_DATE_20090316_ALL_20191202 |  
        df_stockport_q_start  =pd.read_csv(self.path_wds+table_name+"\\"+file_date_q_start )
        df_stockport_q_end  =pd.read_csv(self.path_wds+table_name+"\\"+file_date_q_end )  
        print("df_stockport_q_end",df_stockport_q_end.head().T  )
        
        #在date_end有股票持仓披露的基金列表
        fund_list_sp_end = list(df_stockport_q_end["S_INFO_WINDCODE"].drop_duplicates() )
        ### 剔除长度不等于9的代码，例如206001!1.OF
        print("Fund list with stocks at date_end", fund_list_sp_end )

        ### 只需要部分columns，缩小数据大小： 
        '''
        "F_PRT_STKVALUE", 股票市值
        "F_PRT_STKQUANTITY",股票数量
        "F_PRT_STKVALUETONAV",股票市值占净值比例
        "STOCK_PER",占股票市值比
        "FLOAT_SHR_PER"，占流通股本比例(%)'''
        list_cols_wds =["ANN_DATE","F_PRT_ENDDATE","S_INFO_WINDCODE","CRNCY_CODE","S_INFO_STOCKWINDCODE","F_PRT_STKVALUE","F_PRT_STKQUANTITY","F_PRT_STKVALUETONAV","STOCK_PER","FLOAT_SHR_PER"]
        df_stockport_q_start =df_stockport_q_start.loc[:,list_cols_wds]
        df_stockport_q_end =df_stockport_q_end.loc[:,list_cols_wds]

        #####################################################################
        ### only need fund_code,fund_name,fund_company,fund_type,is_index
        col_list=["F_INFO_WINDCODE","F_INFO_NAME","F_INFO_CORP_FUNDMANAGEMENTCOMP"]
        col_list2=["F_INFO_WINDCODE","F_INFO_NAME","F_INFO_CORP_FUNDMANAGEMENTCOMP","F_INFO_FIRSTINVESTTYPE","IS_INDEXFUND"]
        object_fund_list = {}
        object_fund_list["df_fund_des"] ={}
        object_fund_list["df_fund_des"]["ALL"] = df_fund_des.loc[:,col_list2]
        object_fund_list["df_fund_des"]["stock_index"] = df_fund_des_stock_index.loc[:,col_list]
        object_fund_list["df_fund_des"]["stock_active"] = df_fund_des_stock_active.loc[:,col_list]
        object_fund_list["df_fund_des"]["mixed_active"] = df_fund_des_mixed_active.loc[:,col_list]
        object_fund_list["list_fund_code"] = {}
        object_fund_list["list_fund_code"]["stock_index"] =  list( df_fund_des_stock_index["F_INFO_WINDCODE"].values )
        object_fund_list["list_fund_code"]["stock_active"] = list( df_fund_des_stock_active["F_INFO_WINDCODE"].values )
        object_fund_list["list_fund_code"]["mixed_active"] = list( df_fund_des_mixed_active["F_INFO_WINDCODE"].values )
        ### stock portofolio of all funds for date_start and date_end 
        object_fund_list["df_stockport_q_start"] = df_stockport_q_start
        object_fund_list["df_stockport_q_end"] = df_stockport_q_end
        ### 在date_end有股票持仓披露的基金列表
        object_fund_list["fund_list_sp_end"] = fund_list_sp_end 
        
        return object_fund_list

    def import_df_fund(self,object_fund_x,object_fund_list) :
        ### 给定基金，导入与基金数据相关的数据表格
        '''
        input:
        object_fund_x：读取基金x的基础信息，例如所属基金公司 
            date_q_end =  "20050331"
            date_q_start = "20041231"
        
        object_fund_list:包括所有基金季度持仓信息
        '''
        #####################################################################
        ### 读取基金基础信息，
        temp_date_obs = object_fund_x["temp_date_obs"]
        date_q_end = object_fund_x["date_q_end"]
        date_q_start = object_fund_x["date_q_start"] 
        fund_code = object_fund_x["fund_code"]
        fund_name = object_fund_x["fund_name"]
        fund_company = object_fund_x["fund_company"]
        fund_type = object_fund_x["fund_type"]
        fund_isindex  = object_fund_x["fund_isindex"]
        ### stock portofolio of all funds for date_start and date_end 
        df_stockport_q_start = object_fund_list["df_stockport_q_start"]
        df_stockport_q_end = object_fund_list["df_stockport_q_end"]

        #####################################################################
        ### 匹配日期：基金季度top10披露时间、半年度全部持仓披露时间、上市公司季度披露时间
        # 不知道为什么date_begin_q赋值有问题
        
        # temp_list = [["0331","0430"],["0630","0731"],["0930","1031"],["0101","0131"]]
        # df_dates_quarter = pd.DataFrame(temp_list,columns=["date_begin","date_end"], index=["0331","0630","0930","1231"]  )
        # temp_list = [ ["0630","0830"], ["0101","0331"]]
        # df_dates_halfyear= pd.DataFrame(temp_list,columns=["date_begin","date_end"], index=["0630","1231"]  )
        # # “年报公布时间是每年1月1日——4月30日；半年报公布时间是每年7月1日——8月30日；一季报公布时间是每年4月1日——4月30日。三季报：每年10月1日——10月31日。”
        # temp_list = [["0331","0430"],["0630","0830"],["0930","1031"],["0101","0430"]]
        # df_dates_quarter_stock = pd.DataFrame(temp_list,columns=["date_begin","date_end"], index=["0331","0630","0930","1231"]  )
        
        # 给定日期要匹配搜搜的文件范围 | 
        # date_q_end =  "20050331"  || date_q_start = "20041231"
        
        # temp_mmdd = date_q_end[4:] 
        # if temp_mmdd == "1231" :
        #     temp_year = str(int(date_q_end[4:])+1) 
        # else :
        #     temp_year = date_q_end[4:] 
        
        # print( date_q_end[:4] +df_dates_quarter.loc[temp_mmdd, "date_begin" ] )
        
        # date_begin_q = date_q_end[:4] +df_dates_quarter.loc[temp_mmdd, "date_begin" ]
        # date_end_q   = date_q_end[:4] +df_dates_quarter.loc[temp_mmdd, "date_end" ]

        # date_begin_h = date_q_end[:4] +df_dates_halfyear.loc[temp_mmdd, "date_begin" ]
        # date_end_h   = date_q_end[:4] +df_dates_halfyear.loc[temp_mmdd, "date_end" ]

        # date_begin_q_s = date_q_end[:4] +df_dates_quarter_stock.loc[temp_mmdd, "date_begin" ]
        # date_end_q_s = date_q_end[:4] +df_dates_quarter_stock.loc[temp_mmdd, "date_end" ]

        # ### 数据披露日期有可能在周末，因此需要使用全部日期
        # df_date_raw = pd.read_csv(self.file_path_admin + "data_check_anndates.csv" )
        # # type is int 
        # date_list = list( df_date_raw["date"] )
        # date_list_q =  [x for x in date_list if x<= int(date_end_q) and x>=int(date_start_q)]
        # date_list_h =  [x for x in date_list if x<= int(date_end_h) and x>=int(date_start_h)]
        # date_list_q_s =  [x for x in date_list if x<= int(date_end_q_s) and x>=int(date_start_q_s)]
        
        ### TODO for temp_date in date_list_q : 看哪一天有更新
        # temp_date_obs:用户观察到数据的时间
        # temp_date_obs = "20050429"
        
        #####################################################################
        ### notes:这里用 163417.OF匹配不出数据，需要用 163417.SZ,未来再看看怎么自动匹配。
        df_stockport_q_start_x = df_stockport_q_start[df_stockport_q_start["S_INFO_WINDCODE"]== fund_code ]
        df_stockport_q_end_x = df_stockport_q_end[df_stockport_q_end["S_INFO_WINDCODE"]== fund_code ]
        
        # print("df_stockport_q_start_x" , df_stockport_q_start_x.head().T )
        # print("df_stockport_q_end_x" , df_stockport_q_end_x.head().T )
        ### 确定发布日期，如果只有1个，说明是3月和9月，如果有2个，说明是6和12月，其他报错
        # notes: 有可能出现基金持仓股票低于10个的情况！！！ 因此不能用数量而是用发布日期
        list_anndate_q_start = df_stockport_q_start_x["ANN_DATE"].drop_duplicates().values
        print("list_anndate_q_start", list_anndate_q_start )

        list_anndate_q_end = df_stockport_q_end_x["ANN_DATE"].drop_duplicates().values
        print("list_anndate_q_end", list_anndate_q_end )
        
        ### notes:半年报\年报对应的是全部持仓+前10持仓，两者披露时间不同。
        ### TODO目前只使用季度持仓，未利用滞后的半年持仓
        # 看披露时间，14th：前者0829，后者0716
        if len( list_anndate_q_start ) == 2 :
            # halfyear and quarterly values 
            if int(list_anndate_q_start[0]) < int(list_anndate_q_start[1]) :
                df_stockport_q_start_x_q = df_stockport_q_start_x[df_stockport_q_start_x["ANN_DATE"]== list_anndate_q_start[0] ]
                df_stockport_q_start_x_h = df_stockport_q_start_x[df_stockport_q_start_x["ANN_DATE"]== list_anndate_q_start[1] ]
            else :
                df_stockport_q_start_x_q = df_stockport_q_start_x[df_stockport_q_start_x["ANN_DATE"]== list_anndate_q_start[1] ]
                df_stockport_q_start_x_h = df_stockport_q_start_x[df_stockport_q_start_x["ANN_DATE"]== list_anndate_q_start[0] ]
        else :
            # only quarterly holdings
            df_stockport_q_start_x_q = df_stockport_q_start_x

        if len( list_anndate_q_end ) == 2 :
            # halfyear and quarterly values 
            # halfyear and quarterly values 
            if int(list_anndate_q_end[0]) < int(list_anndate_q_end[1]) :
                df_stockport_q_end_x_q = df_stockport_q_end_x[df_stockport_q_end_x["ANN_DATE"]== list_anndate_q_end[0] ]
                df_stockport_q_end_x_h = df_stockport_q_end_x[df_stockport_q_end_x["ANN_DATE"]== list_anndate_q_end[1] ]
            else :
                df_stockport_q_end_x_q = df_stockport_q_end_x[df_stockport_q_end_x["ANN_DATE"]== list_anndate_q_end[1] ]
                df_stockport_q_end_x_h = df_stockport_q_end_x[df_stockport_q_end_x["ANN_DATE"]== list_anndate_q_end[0] ]
        else :
            # only quarterly holdings
            df_stockport_q_end_x_q = df_stockport_q_end_x

        #####################################################################
        ### 保存至输出的对象object
        object_fund = {} 
        object_fund["fund_name"] = fund_name
        object_fund["fund_company"] = fund_company
        object_fund["fund_type"] =  fund_type
        object_fund["fund_isindex"] = fund_isindex
        object_fund["date_ann_end"] = df_stockport_q_end_x_q["ANN_DATE"].values[0]
        # notes:基金有可能在上一季度无数据
        if df_stockport_q_start_x_q.empty :
            object_fund["date_ann_start"] = ""
            ### 判断是否存在date_start持仓
            object_fund["is_date_start"] = 0
        else :
            object_fund["date_ann_start"] = df_stockport_q_start_x_q["ANN_DATE"].values[0] 
            print("Debug===fund_name",fund_name )
            df_stockport_q_start_x_q.loc[:,"fund_name"] = fund_name
            df_stockport_q_start_x_q.loc[:,"fund_company"] = fund_company
            df_stockport_q_start_x_q.loc[:,"fund_type"] = fund_type
            df_stockport_q_start_x_q.loc[:,"fund_isindex"] = fund_isindex

            object_fund["is_date_start"] = 1
        
        df_stockport_q_end_x_q.loc[:,"fund_name"] = fund_name
        df_stockport_q_end_x_q.loc[:,"fund_company"] = fund_company
        df_stockport_q_end_x_q.loc[:,"fund_type"] = fund_type
        df_stockport_q_end_x_q.loc[:,"fund_isindex"] = fund_isindex
        ### stock portofolio of fund for date_start and date_end for fund x 
        object_fund["df_stockport_q_start_x_q"] = df_stockport_q_start_x_q
        object_fund["df_stockport_q_end_x_q"] = df_stockport_q_end_x_q
        #
        object_fund["date_q_end"] = date_q_end
        object_fund["date_q_start"] =date_q_start 
        return object_fund


    def cal_diff_stockport(self,object_fund) :
        ### 计算基金持仓股票与前一季度之间的差异值，但不进行分析 || df_stockport2_x_q , df_stockport1_x_q ~ pre q
        ''' 分析的几个维度:
        1，基金持仓数据
        1.1,个股行业分类
        1.2, 个股绝对值：个股种类的变动、持仓市值变动，仓位比例，仓位倒退基金净值
        1.3，个股相对值：行业同比、个股同比，市值同比、仓位同比、

        2,导入个股区间数据
        3,导入3个行业分类的行业数据：绝对值和相对值：市值和持仓变动：分类，首先用中信一级行业分类划分
        notes:
        截至20200118，已经有2700个基金披露2019q4持仓，但很多是债券基金货币基金等。
        例如兴全、工银瑞信
        163417.OF	兴全合宜
        007119.OF 睿远成长价值

        S_INFO_STOCKWINDCODE 601012.SH
        F_PRT_STKVALUE       3.27055e+09
        F_PRT_STKQUANTITY    1.24687e+08
        F_PRT_STKVALUETONAV  9.19
        "F_PRT_STKVALUE", 股票市值
        "F_PRT_STKQUANTITY",股票数量
        "F_PRT_STKVALUETONAV",股票市值占净值比例
        "STOCK_PER",占股票市值比
        "FLOAT_SHR_PER"，占流通股本比例(%)
        todo:
        1,时间上，从基金上市3个月开始跟踪季度持仓的披露时间，在每年季报披露时间1、4、7、10和
        半年报披露时间3、8
        '''
        ### 
        df_stockport_q_start_x_q =  object_fund["df_stockport_q_start_x_q"] 
        df_stockport_q_end_x_q = object_fund["df_stockport_q_end_x_q"]         
        date_q_end = object_fund["date_q_end"]
        date_q_start = object_fund["date_q_start"]  
        
        ### Import date_start and date_end for further calculation
        obj_dates = self.import_df_dates()
        date_start = date_q_start
        date_end = date_q_end
        date_ann_end = object_fund["date_ann_end"]

        ### 1, 个股变动分类：
        # 1.1，在最新季度持仓的基础上，和上一季度持仓比较
        # object_fund["is_date_start"] ==1 means exist stocks at date_start,==0 mean no stocks records at date_start
        col_sp_diff =["diff_mv","diff_num","diff_mv_pct","diff_num_pct","diff_pct_nav","diff_pct_stockcap""diff_pct_stockcap_float" ]
        for temp_col in col_sp_diff :
            df_stockport_q_end_x_q[temp_col ] = 0.0
        # 判断基金在date_start是否持有该股票 , by object_fund["is_date_start"]
        if object_fund["is_date_start"] ==1 :
            for temp_i_q_pre in df_stockport_q_start_x_q.index :
                temp_code = df_stockport_q_start_x_q.loc[temp_i_q_pre, "S_INFO_STOCKWINDCODE" ]
                # print( df_stockport_q_start_x_q.loc[temp_i_q_pre, : ].T )
                # check if exists in df_stockport2_x_q
                temp_df = df_stockport_q_end_x_q[df_stockport_q_end_x_q["S_INFO_STOCKWINDCODE"]== temp_code ] 
                if len(temp_df ) == 1 :
                    ###########################################################################
                    ### data of fund holding differences
                    ### 新增列计算绝对和相对变动、
                    # print("temp_df", temp_df.T )
                    ''' 
                    1,绝对值变动：市值、数量、
                    2,相对值变动
                    3，需要引入这段时间股票价格的变动，仿真计算：市值变动=复权的股价涨跌+数量变动
                    input：股票代码；开始和结束时间-可能不是交易日
                    output：区间涨跌幅、开始和结束日期是否停牌、
                    '''
                    temp_i_q =temp_df.index.values[0]
                    df_stockport_q_end_x_q.loc[temp_i_q,"diff_mv" ] = df_stockport_q_end_x_q.loc[ temp_i_q,"F_PRT_STKVALUE"] - df_stockport_q_start_x_q.loc[ temp_i_q_pre,"F_PRT_STKVALUE"] 
                    df_stockport_q_end_x_q.loc[temp_i_q,"diff_num" ] = df_stockport_q_end_x_q.loc[ temp_i_q,"F_PRT_STKQUANTITY"] - df_stockport_q_start_x_q.loc[ temp_i_q_pre,"F_PRT_STKQUANTITY"] 
                    df_stockport_q_end_x_q.loc[temp_i_q,"diff_mv_pct" ] = df_stockport_q_end_x_q.loc[temp_i_q,"diff_mv" ] / df_stockport_q_start_x_q.loc[ temp_i_q_pre,"F_PRT_STKVALUE"] 
                    df_stockport_q_end_x_q.loc[temp_i_q,"diff_num_pct" ] = df_stockport_q_end_x_q.loc[temp_i_q,"diff_num" ]/ df_stockport_q_start_x_q.loc[ temp_i_q_pre,"F_PRT_STKQUANTITY"] 

                    df_stockport_q_end_x_q.loc[temp_i_q,"diff_pct_nav" ] = df_stockport_q_end_x_q.loc[ temp_i_q,"F_PRT_STKVALUETONAV"] - df_stockport_q_start_x_q.loc[ temp_i_q_pre,"F_PRT_STKVALUETONAV"] 
                    df_stockport_q_end_x_q.loc[temp_i_q,"diff_pct_stockcap" ] = df_stockport_q_end_x_q.loc[ temp_i_q,"STOCK_PER"] - df_stockport_q_start_x_q.loc[ temp_i_q_pre,"STOCK_PER"] 
                    df_stockport_q_end_x_q.loc[temp_i_q,"diff_pct_stockcap_float" ] = df_stockport_q_end_x_q.loc[ temp_i_q,"FLOAT_SHR_PER"] - df_stockport_q_start_x_q.loc[ temp_i_q_pre,"FLOAT_SHR_PER"] 
                  
        ###########################################################################
        # 判断股票在date_start,date_end是否有行业分类 by object_fund["is_date_start"]
        
        for temp_i_q in df_stockport_q_end_x_q.index :    
            code_stock = df_stockport_q_end_x_q.loc[temp_i_q, "S_INFO_STOCKWINDCODE" ]
            ###########################################################################
            ### 引入股票行业属性                          
            ind_list=["wind_ind_name_1","wind_ind_name_2","citics_ind_name_1","sw_ind_name_1"]
            
            if not code_stock[-3:] in [".SH",".SZ"] :
                print("Code_stock is not end with .SH or .SZ : ", code_stock)
                for temp_col in ind_list :
                    df_stockport_q_end_x_q.loc[temp_i_q,temp_col  ]= "" 
            else :
                object_ind = self.get_ind_date([code_stock],date_end,"" )
                if "df_s_ind_out" in object_ind.keys() :
                    # notes：object_ind["df_s_ind_out"]类型是series,temp_index是其之前在df里对应的列值
                    # object_ind["code_list"] = code_list   object_ind["df_s_ind_out"] = df_s_ind_out ,object_ind["temp_index"] =temp_index
                    ### New method  
                    for temp_col in ind_list :
                        # notes:有可能上一期时没有持仓，若有持仓用期初start，否则用期末的
                        
                        if not len(object_ind["df_s_ind_out"]) <1  : 
                            print("Debug====temp_co" ,temp_col,  object_ind["df_s_ind_out"] )    
                            df_stockport_q_end_x_q.loc[temp_i_q,temp_col  ]= object_ind["df_s_ind_out"][ temp_col ]
                        else :
                            print("Error,no industry record before data_in at file df_ind_code_stock_io.csv. ")
                            df_stockport_q_end_x_q.loc[temp_i_q,temp_col  ]= "" 
                else :
                    for temp_col in ind_list :
                        df_stockport_q_end_x_q.loc[temp_i_q,temp_col  ]= "" 
            
            ### Old method: 
            # object_ind = self.get_ind_period(obj_dates,code_stock,date_start,date_end )
            # df_s_ind_period = object_ind["df_s_ind_period"] 
            
            ## 初步将wind,citics,sw 一级分类赋值
            # ind_list=["wind_ind_name_1","wind_ind_name_2","citics_ind_name_1","sw_ind_name_1"]
            # for temp_col in ind_list :
            #     # notes:有可能上一期时没有持仓，若有持仓用期初start，否则用期末的
            #     if object_ind["is_ind_start"] ==1 :
            #         df_s_ind_start = object_ind["df_s_ind_start"] 
            #         df_stockport_q_end_x_q.loc[temp_i_q,temp_col  ]= df_s_ind_start[temp_col].values[0]
            #     elif object_ind["is_ind_end"] ==1 : 
            #         df_s_ind_end = object_ind["df_s_ind_end"] 
            #         df_stockport_q_end_x_q.loc[temp_i_q,temp_col  ]= df_s_ind_end[temp_col].values[0]
            #     else :
            #         print("Error,no industry record for both date_start and date_end ")
            #         df_stockport_q_end_x_q.loc[temp_i_q,temp_col  ]= ""

            ###########################################################################
            ### data of stock,股票相关数据
            ### 引入这段时间股票区间股票价格、市值股本、财务指标变动和涨跌幅 
            object_code = self.get_stock_period_diff(obj_dates,code_stock,date_start,date_end)
            
            ### object_code["if_data_prices"] = 1 means there exists prices for given periods and 0 mean no reocrds
            if object_code["if_data_prices"] == 1 :
                chg_pct = object_code[ "adjclose" ]["diff_pct"] 
                print("adjclose_chg_pct % ", round(chg_pct*100,2),code_stock,date_start,date_end ) 
                # object_code["cols_rc"]自定义了相关wds列名
                for temp_col in object_code["cols_rc"]:
                    ### save values to stockport 
                    # notes:有可能上一期时没有持仓，若有持仓用期初start，否则用期末的;似乎之前object_code已经处理过了
                    
                    df_stockport_q_end_x_q.loc[temp_i_q,temp_col+"_start" ]= object_code[temp_col ]["start"]
                    df_stockport_q_end_x_q.loc[temp_i_q,temp_col+"_end" ]= object_code[temp_col ]["end"]
                    df_stockport_q_end_x_q.loc[temp_i_q,temp_col+"_diff" ]= object_code[temp_col ]["diff"]
                    df_stockport_q_end_x_q.loc[temp_i_q,temp_col+"_diff_pct" ]= object_code[temp_col ]["diff_pct"]
            else :
                chg_pct = 0.0
                print("adjclose_chg_pct ", chg_pct ) 
                # object_code["cols_rc"]自定义了相关wds列名

            ###########################################################################
            ###  TODO |导入一致预期数据

        #####################################################################
        ### 保存至输出的对象object
        object_diff = {}
        object_diff["df_sp_diff"] = df_stockport_q_end_x_q
        
        object_diff["col_sp_diff"] = col_sp_diff
        # object_code["cols_rc"] with _start,_end,_diff,_diff_pct
        object_diff["col_stock_diff"] = object_code["cols_rc"]
        object_diff["col_ind"] = ind_list
        
        ### 将基金的季度数据保存到特定文件夹
        folder_fund =self.path_rc + "fund_ana" +"\\" +str(date_end)+"\\" 
        object_diff["dir_df"] = folder_fund
        if not os.path.exists(folder_fund) :
            os.mkdir( folder_fund )
        # fund_name, date_start,date_end
        file_name = object_fund["fund_name"] +"_" + str(date_end) +"_" + str(date_ann_end)+".csv"
        object_diff["df_sp_diff"].to_csv(folder_fund +file_name,index=False,encoding="gbk"   )
        # also save columns of object_diff["df_sp_diff"] to folder_fund.
        file_name_col = "columns" +"_" + str(date_end) +"_" + str(date_ann_end)+".csv"
        


        return object_diff
    
    def manage_fund_sp_change(self,temp_date_obs,date_q_end,date_q_start) :
        ### 导入基金持仓列表,计算持仓股票与上季度同比的变动
        count_sp = 0
        ### 如果要从第 n-th 个基金记录开始，需要在这里先导入df_sp_diff，否则不需要
        #####################################################################
        ### 1，判断季度基金列表是否存在，并读取
        file_output2 = "ALL_funds"  +"_" + str(date_q_end)  +".csv"
        folder_fund = self.path_rc + "fund_ana" +"\\" +str(date_q_end)+"\\" 
        import os 
        if not os.path.exists(folder_fund) :
            os.mkdir( folder_fund ) 
        if os.path.exists(folder_fund + file_output2 ) : 
            df_sp_diff = pd.read_csv(folder_fund + file_output2,encoding="gbk" )
            count_sp = 1
            print(df_sp_diff.tail().T )
        
        #####################################################################
        ### 2，导入基金持仓列表,根据季末日期获取基金基础信息列表
        object_fund_list = self.import_fund_list(temp_date_obs,date_q_end,date_q_start)
        print("list_fund_code for stock_active \n",len(object_fund_list["list_fund_code"]["stock_active"]) )
        ### step 2，对每个主动股票型基金"stock_active"，计算持仓数据变动
        # fund_code,fund_name,fund_company,fund_type,is_index
        object_fund_x={}
        object_fund_x["temp_date_obs"]= temp_date_obs
        object_fund_x["date_q_end"] =  date_q_end
        object_fund_x["date_q_start"] = date_q_start
        df_fund_des_stock_active = object_fund_list["df_fund_des"]["stock_active"]

        #####################################################################
        ### 在date_end有股票持仓披露的基金列表
        # notes：df_fund_des 只反映最新日期的基金信息，但2002年上市的基金x，在2019年的属性和2005年可能完全不同，因此还是要以持仓数据里的为准
        fund_list_sp_end = object_fund_list["fund_list_sp_end"] 
        df_fund_des = object_fund_list["df_fund_des"]["ALL"]
        
        count_fund_number = 0 
        print("Number of funds ",len(fund_list_sp_end) )
        for fund_code in fund_list_sp_end :
            ### 如果要从第 n-th 个基金记录开始，需要在这里设置;-1表示从0开始
            # input1 = input("Check if start from n-th fund and setting para for count_fund_number.")
            if count_fund_number > -1 :
                ### find fund_type,fund_name,fund_company in df_fund_des
                temp_df = df_fund_des[df_fund_des["F_INFO_WINDCODE"]== fund_code ]
                if len(temp_df.index ) < 1 :
                    fund_type = "unclear"
                    fund_isindex ="unclear"
                    fund_name = fund_code
                    fund_company = fund_code
                else :        
                    fund_type = temp_df["F_INFO_FIRSTINVESTTYPE"].values[0]
                    fund_isindex = temp_df["IS_INDEXFUND"].values[0]
                    fund_name = temp_df["F_INFO_NAME"].values[0]
                    fund_company = temp_df["F_INFO_CORP_FUNDMANAGEMENTCOMP"].values[0]
                
                print("fund_code fund_name", fund_code,fund_name)
                
                object_fund_x["fund_code"]= fund_code   
                object_fund_x["fund_name"]= fund_name
                object_fund_x["fund_company"]= fund_company
                object_fund_x["fund_type"]= fund_type
                object_fund_x["fund_isindex"]= fund_isindex

                #####################################################################
                ### 给定基金，导入与基金数据相关的数据表格
                object_fund = self.import_df_fund(object_fund_x,object_fund_list)
                
                object_diff = self.cal_diff_stockport(object_fund) 
                # 计算季度之间的差异值，但不进行分析 
                print("Directory of csv file for fund df ",   object_diff["dir_df"] )
                
                ### TODO,将同一季度内所有基金持仓变动数据合并在1个df内
                ### 将基金的季度数据保存到特定文件夹 
                # TODO目前只使用季度持仓，未利用滞后的半年持仓
                date_ann_end = object_fund["date_ann_end"]
                file_output = "ALL_funds"  +"_" + str(date_q_end) +"_" + str(date_ann_end)+".csv"
                
                if count_sp == 0 :
                    df_sp_diff = object_diff["df_sp_diff"]
                    count_sp = 1 
                else :
                    df_sp_diff = df_sp_diff.append( object_diff["df_sp_diff"], ignore_index=True)

                df_sp_diff.to_csv(folder_fund + file_output,index=False,encoding="gbk"   )
                df_sp_diff.to_csv(folder_fund + file_output2,index=False,encoding="gbk"   )

                # input1 = input("Check to continue...")
            
            count_fund_number = count_fund_number +1 

        return df_sp_diff  

    def get_index_period_diff(self,code_index,date_start,date_end): 
        ### 获取区间指数价格、涨跌幅、成交额等变动 
        '''
        idea:date_list.csv 不靠谱，可以从“000001.SH”上证综指获取交易日，因为这是最早的指数
        notes:20070102 是没有上证综指和沪深300指数的行情，因为是A股修市，仅有港股台湾股票指数有行情。
        上证综指开始日期为19911031，但20070102是节假日没有行情，20070104才有
        沪深300指数开始日期20020823，但在【20061229，20070709】区间是没有行情的
        we need date_start and date_end to be like "20050301" format 
        '''
        ### date_list.csv会包含周末和节假日，这时因为有些公告是在周末披露
        df_000001 = pd.read_csv( self.path_wds +"AIndexEODPrices\\" +"WDS_S_INFO_WINDCODE_000001.SH_ALL.csv",encoding="gbk" )       

        tradingdate_list = df_000001.loc[:,["TRADE_DT","S_DQ_CLOSE"]]
        tradingdate_list.columns=["date","S_DQ_CLOSE"]
        tradingdate_list = tradingdate_list.sort_values(by="date")
        print("tradingdate_list  ",tradingdate_list )


        #tradingdate_list = pd.read_csv(self.file_path_admin + "date_list.csv")
        # tradingdate_list.columns=["date"]  

        tradingdate_list_sub = tradingdate_list[ tradingdate_list.date <= int(date_start) ]
        print("666=",tradingdate_list_sub.date.iloc[-1] ) 
        date_0 = tradingdate_list_sub.date.iloc[-1]
        tradingdate_list_sub = tradingdate_list[ tradingdate_list.date <= int(date_end) ]
        print("666=",tradingdate_list_sub.date.iloc[-1] )
        date_1 = tradingdate_list_sub.date.iloc[-1] 
        
        table_name ="AIndexEODPrices"
        ### 读取date_start from tradingdate_list 
        file_name_0 = "WDS_TRADE_DT_"+  str( int(date_0) ) +"_ALL.csv"
        df_0 = pd.read_csv( self.path_wds+table_name +"\\"+ file_name_0 )

        # df_0 = df_0[df_0["S_INFO_WINDCODE"] == code_index ]
        # "1" "S_INFO_WINDCODE" ; "8" "S_DQ_CLOSE"
        df_0 = df_0[df_0["S_INFO_WINDCODE"] == code_index ]
        index_close_0 = df_0["S_DQ_CLOSE"].values[0]

        ### 读取date_end from tradingdate_list 
        file_name_0 = "WDS_TRADE_DT_"+  str( int(date_1) ) +"_ALL.csv"
        df_0 = pd.read_csv( self.path_wds+table_name +"\\"+ file_name_0 )
        
        df_0 = df_0[df_0["S_INFO_WINDCODE"] == code_index ]
        # df_0 = df_0[df_0["1"] == code_index ]
        index_close_1 = df_0["S_DQ_CLOSE"].values[0]
        
        period_chg = index_close_1/index_close_0-1 
        print("Price change ",code_index,date_start,date_end ,period_chg )
        

        return period_chg


    def get_stock_period_diff(self,obj_dates,code_stock,date_start,date_end) :
        ### 获取区间股票价格、市值股本、财务指标变动和涨跌幅
        ''' 
        Input:
        1,需要读取个股的日行情数据和日衍生行情数据；背后是从基于交易日到基于个股的数据表的及时转换计算。
        2,obj_dates 目前没用。

        Steps:
        1,读取日期序列数据，tradingdate_list
        2,获取date_start 之后的最近交易日；获取date_end之前的最近交易日
        3,
        notes:
        1,未来可能会遇到在日期区间内股票代码或行业发生变动的情况，例如股票的收购兼并或流动性受限的情况
        date_start should be "20150331"
        object_code["cols_rc"]自定义了相关wds列名
        2,001872.SZ因发生过合并事件，日衍生数据初始日是20171225，但日行情数据从1993年就有了
        3,个股历史数据里，可能并不是按照"TRADE_DT"交易日升序排列，有可能非常杂乱，导入时需要ascending
        '''
        #####################################################################
        ### 导入日期信息
        # de_code = obj_dates["code"]
        # date_list_0 = obj_dates["date_start"]
        # date_list_1 = obj_dates["date_end"]
        # date_list = obj_dates["date_list"]
        # ### 根据date_list 找到date_start前第一个交易日
        # date_list_sub = [temp_date for temp_date in date_list if temp_date >= int(date_start) ]
        # date_list_sub = [temp_date for temp_date in date_list_sub if temp_date <= int(date_end) ]
        # # notes:个股历史数据里，可能并不是按照"TRADE_DT"交易日升序排列，有可能非常杂乱，导入时需要 sort list  
        # date_list_sub.sort() 
        # print("Debug===",date_list_sub[:3] ,date_list_sub[-3:] )

        #####################################################################
        ### Initialize code object 
        ### 对dict对象赋值
        object_code = {}
        ### 命名匹配列表：原始列名 link to 我们需要的列名
        object_code["cols_wds"] = []
        object_code["cols_rc"] = []
        
        ### import historical price for single stock
        table_name = "AShareEODPrices"
        # notes:还是需要按照个股读取数据，因为单个交易日无法反映一段时间内个股复权涨跌幅
        # WDS_S_INFO_WINDCODE_000001.SZ_ALL 
        # WDS_TRADE_DT_20060331_ALL
        file_name = "WDS_S_INFO_WINDCODE_"+code_stock+"_ALL.csv"
        ### 判断文件是否存在：
        import os
        if os.path.exists(self.path_wds + table_name+"\\"+file_name  ) : 
            # df_stock = pd.read_csv(self.path_wds + table_name+"\\"+file_name ,encoding="gbk" ) 
            df_stock = pd.read_csv(self.path_wds + table_name+"\\"+file_name   ) 
            # notes:个股历史数据里，可能并不是按照"TRADE_DT"交易日升序排列，有可能非常杂乱，导入时需要 sort list  
            
            date_list = list( df_stock["TRADE_DT"].values )
            date_list_sub = [temp_date for temp_date in date_list if temp_date >= int(date_start) ]
            date_list_sub = [temp_date for temp_date in date_list_sub if temp_date <= int(date_end) ]
            date_list_sub.sort()   
            if_exist_df_stock = 1 
        else :
            if_exist_df_stock = 0
            date_list_sub = []

        # notes:有可能出现date_list_sub 是空的情况，601628有记录的价量日期是20070109开始，但有的基金在20161231就持有了。
        if if_exist_df_stock ==0 :
            ### object_code["if_data_prices"] = 0 means no prices for given periods
            object_code["if_data_prices"] = 0
            print("No value ",date_start,date_end,code_stock,date_list_sub )
        elif len( date_list_sub ) < 2 :
            object_code["if_data_prices"] = 0
            print("No value ",date_start,date_end,code_stock,date_list_sub )
        else :
            ### object_code["if_data_prices"] = 1 means there exists prices for given periods
            object_code["if_data_prices"] = 1
            ### get price change from date_list_sub[0] to date_list_sub[-1]
            date_start_trade = date_list_sub[0] 
            date_end_trade = date_list_sub[-1]
            
            ### import historical derivative indicators
            table_name_deriv = "AShareEODDerivativeIndicator" 
            file_name = "WDS_S_INFO_WINDCODE_"+code_stock+"_ALL.csv"
            # print("Debug====", self.path_wds + table_name_deriv+"\\"+file_name )
            df_stock_deriv = pd.read_csv(self.path_wds + table_name_deriv+"\\"+file_name  ) 
            # notes:个股历史数据里，可能并不是按照"TRADE_DT"交易日升序排列，有可能非常杂乱，导入时需要 sort list   

            
            object_code["date_start"] = date_start_trade
            object_code["date_end"] = date_end_trade
            ### 股票复权的收盘价格涨跌幅度
            temp_cols_wds = ["S_DQ_ADJCLOSE"]
            temp_cols_rc = ["adjclose"]
            ### 1st time:命名匹配列表：原始列名 link to 我们需要的列名
            object_code["cols_wds"] =object_code["cols_wds"]+ temp_cols_wds
            object_code["cols_rc"] = object_code["cols_rc"] +temp_cols_rc
            temp_len = len(temp_cols_wds)

            for i in range( temp_len ) : 
                # col_rc 1:1 col_wds
                if df_stock[ df_stock["TRADE_DT"] == date_start_trade ].empty :
                    temp_start = 0.0
                else :
                    temp_start = df_stock[ df_stock["TRADE_DT"] == date_start_trade ][temp_cols_wds[i] ].values[0]
                temp_end = df_stock[ df_stock["TRADE_DT"] == date_end_trade ][temp_cols_wds[i] ].values[0]

                object_code[temp_cols_rc[i] ]={}
                object_code[temp_cols_rc[i]]["start"]= temp_start 
                object_code[temp_cols_rc[i]]["end"]= temp_end
                object_code[temp_cols_rc[i]]["diff"]= temp_end - temp_start
                object_code[temp_cols_rc[i]]["diff_pct"]= (temp_end - temp_start) / temp_start 

            ### 需要跟踪的指标：historical derivative indicators
            # notes：001872.SZ因发生过合并事件，日衍生数据初始日是20171225，但日行情数据从1993年就有了
            # 如果 df_stock_deriv.empty, 则对数据都填写0.0
            # locate 1 row df for starting date and end date respectively,

            df_start = df_stock_deriv[ df_stock_deriv["TRADE_DT"] == date_start_trade ]         
            df_end = df_stock_deriv[ df_stock_deriv["TRADE_DT"] == date_end_trade ]
            if not df_end.empty :        
                if_empty_df_start = 0
                if df_start.empty :
                    if_empty_df_start = 1 
                
                # locate specific key values by :[temp_cols_wds[i] ].values[0]
                '''
                1，股本和价格：
                1.1，总市值变动；2，流通市值变动，3，52周复权价格百分比；4，总股本变动；5，流通股本变动；6,换手率
                2，财务：
                2.1，PE_ttm,PB_ttm；2.2，净利润ttm变动、经营性现金流ttm变动、收入ttm变动、'''
                # "FREE_SHARES_TODAY",当日自由流通股本：自由流通股本中不包括那些控股股东、公司管理层、战略性股东等持有的长期不流通的股份，因此，自由流通量股本较为真实地反映了市场上流通股份的情况，是投资者实际能够交易的股份数量。
                # 例：剔除持股5%以上(含5%)减持需要公告的股份，持股5%以上(含5%)的高管持股。
                temp_cols_wds =["S_VAL_MV","S_DQ_MV","S_PQ_ADJHIGH_52W","S_PQ_ADJLOW_52W","TOT_SHR_TODAY","FLOAT_A_SHR_TODAY","FREE_SHARES_TODAY" ] 
                temp_cols_rc = ["s_mv_total","s_mv_float","s_adjhigh_52w","s_adjlow_52w","s_num_total","s_num_float","s_num_freefloat"]
                ### 命名匹配列表：原始列名 link to 我们需要的列名
                object_code["cols_wds"] =object_code["cols_wds"]+ temp_cols_wds
                object_code["cols_rc"] = object_code["cols_rc"] +temp_cols_rc
                temp_len = len(temp_cols_wds)
                for i in range(temp_len) :
                    # col_rc 1:1 col_wds
                    if if_empty_df_start == 1 :
                        temp_start = 0.0
                    else :
                        temp_start = df_start[temp_cols_wds[i] ].values[0]
                    temp_end = df_end[temp_cols_wds[i] ].values[0]

                    object_code[temp_cols_rc[i]]={}
                    object_code[temp_cols_rc[i]]["start"]= temp_start 
                    object_code[temp_cols_rc[i]]["end"]= temp_end 
                    object_code[temp_cols_rc[i]]["diff"]=  temp_end - temp_start
                    object_code[temp_cols_rc[i]]["diff_pct"]= (temp_end - temp_start) / temp_start 

                ### 2，财务： 
                cols_wds_financial=["S_VAL_PE_TTM","S_VAL_PCF_OCFTTM","S_VAL_PCF_NCFTTM","S_VAL_PS_TTM","S_PRICE_DIV_DPS" ]
                cols_wds_financial=cols_wds_financial+["NET_PROFIT_PARENT_COMP_TTM","NET_ASSETS_TODAY","NET_CASH_FLOWS_OPER_ACT_TTM","OPER_REV_TTM","NET_INCR_CASH_CASH_EQU_TTM","NET_PROFIT_PARENT_COMP_TTM"]
                temp_cols_rc = ["s_pe_ttm","s_pcf_oper_ttm","s_pcf_net_ttm","s_ps_ttm","s_dps_pct"]
                temp_cols_rc = temp_cols_rc +["s_netprofit_ttm","s_netassets_ttm","s_cf_oper_ttm","s_revenue_ttm","s_cf_increase_ttm","s_netprofit_ttm" ]
                # 归属母公司净利润(TTM);当日净资产;经营活动产生的现金流量净额(TTM);营业收入(TTM);现金及现金等价物净增加额(TTM)
                ### 命名匹配列表：原始列名 link to 我们需要的列名
                object_code["cols_wds"] =object_code["cols_wds"]+ cols_wds_financial
                object_code["cols_rc"] = object_code["cols_rc"] + temp_cols_rc
                temp_len = len( cols_wds_financial )
                for i in range(temp_len) :
                    # col_rc 1:1 col_wds
                    if if_empty_df_start == 1 :
                        temp_start = 0.0
                    else :
                        temp_start = df_start[cols_wds_financial[i] ].values[0]
                    temp_end = df_end[cols_wds_financial[i] ].values[0]

                    object_code[temp_cols_rc[i]]={}
                    object_code[temp_cols_rc[i]]["start"]= temp_start 
                    object_code[temp_cols_rc[i]]["end"]= temp_end 
                    object_code[temp_cols_rc[i]]["diff"]=  temp_end - temp_start
                    object_code[temp_cols_rc[i]]["diff_pct"]= (temp_end - temp_start) / temp_start 
            else :
                ### Just fill 0.0 for all
                temp_cols_wds =["S_VAL_MV","S_DQ_MV","S_PQ_ADJHIGH_52W","S_PQ_ADJLOW_52W","TOT_SHR_TODAY","FLOAT_A_SHR_TODAY","FREE_SHARES_TODAY" ] 
                temp_cols_rc = ["s_mv_total","s_mv_float","s_adjhigh_52w","s_adjlow_52w","s_num_total","s_num_float","s_num_freefloat"]
                ### 命名匹配列表：原始列名 link to 我们需要的列名
                object_code["cols_wds"] =object_code["cols_wds"]+ temp_cols_wds
                object_code["cols_rc"] = object_code["cols_rc"] +temp_cols_rc
                temp_len = len(temp_cols_wds)
                for i in range(temp_len) : 
                    object_code[temp_cols_rc[i]]={}
                    object_code[temp_cols_rc[i]]["start"]= 0.0
                    object_code[temp_cols_rc[i]]["end"]= 0.0
                    object_code[temp_cols_rc[i]]["diff"]=  0.0
                    object_code[temp_cols_rc[i]]["diff_pct"]= 0.0
                
                ### 2，财务： 
                cols_wds_financial=["S_VAL_PE_TTM","S_VAL_PCF_OCFTTM","S_VAL_PCF_NCFTTM","S_VAL_PS_TTM","S_PRICE_DIV_DPS" ]
                cols_wds_financial=cols_wds_financial+["NET_PROFIT_PARENT_COMP_TTM","NET_ASSETS_TODAY","NET_CASH_FLOWS_OPER_ACT_TTM","OPER_REV_TTM","NET_INCR_CASH_CASH_EQU_TTM","NET_PROFIT_PARENT_COMP_TTM"]
                temp_cols_rc = ["s_pe_ttm","s_pcf_oper_ttm","s_pcf_net_ttm","s_ps_ttm","s_dps_pct"]
                temp_cols_rc = temp_cols_rc +["s_netprofit_ttm","s_netassets_ttm","s_cf_oper_ttm","s_revenue_ttm","s_cf_increase_ttm","s_netprofit_ttm" ]
                # 归属母公司净利润(TTM);当日净资产;经营活动产生的现金流量净额(TTM);营业收入(TTM);现金及现金等价物净增加额(TTM)
                ### 命名匹配列表：原始列名 link to 我们需要的列名
                object_code["cols_wds"] =object_code["cols_wds"]+ cols_wds_financial
                object_code["cols_rc"] = object_code["cols_rc"] + temp_cols_rc
                temp_len = len( cols_wds_financial )
                for i in range(temp_len) : 
                    object_code[temp_cols_rc[i]]={}
                    object_code[temp_cols_rc[i]]["start"]= 0.0
                    object_code[temp_cols_rc[i]]["end"]= 0.0
                    object_code[temp_cols_rc[i]]["diff"]= 0.0
                    object_code[temp_cols_rc[i]]["diff_pct"]= 0.0

            # for key in object_code:
            #     print(key,object_code[key] )

        return object_code
    
    ###
    def get_fund_period_diff(self,code_fund,date_start,date_end):
        ### 获取区间基金净值价格、涨跌幅、成交额等变动
        '''
        参考 get_index_period_diff
        不复权净值= "F_NAV_ACCUMULATED"
        复权净值："F_NAV_ADJUSTED"，包括累计净值"F_NAV_ACCUMULATED"和分红"F_NAV_DISTRIBUTION"
        ''' 

        ### date_list.csv会包含周末和节假日，这时因为有些公告是在周末披露
        df_000001 = pd.read_csv( self.path_wds +"AIndexEODPrices\\" +"WDS_S_INFO_WINDCODE_000001.SH_ALL.csv",encoding="gbk" )       

        tradingdate_list = df_000001.loc[:,["TRADE_DT","S_DQ_CLOSE"]]
        tradingdate_list.columns=["date","S_DQ_CLOSE"]
        tradingdate_list = tradingdate_list.sort_values(by="date")
        print("tradingdate_list  ",tradingdate_list )

        #tradingdate_list = pd.read_csv(self.file_path_admin + "date_list.csv")
        # tradingdate_list.columns=["date"]  

        tradingdate_list_sub = tradingdate_list[ tradingdate_list.date <= int(date_start) ]
        print("666=",tradingdate_list_sub.date.iloc[-1] ) 
        date_0 = tradingdate_list_sub.date.iloc[-1]
        tradingdate_list_sub = tradingdate_list[ tradingdate_list.date <= int(date_end) ]
        print("666=",tradingdate_list_sub.date.iloc[-1] )
        date_1 = tradingdate_list_sub.date.iloc[-1] 
        
        table_name ="ChinaMutualFundNAV"

        ### 读取date_start from tradingdate_list 
        file_name_0 = "WDS_ANN_DATE_"+  str( int(date_0) ) +"_ALL.csv"
        df_0 = pd.read_csv( self.path_wds+table_name +"\\"+ file_name_0 )

        df_0 = df_0[df_0["F_INFO_WINDCODE"] == code_fund ]
        index_close_0 = df_0["F_NAV_ADJUSTED"].values[0]

        ### 读取date_end from tradingdate_list 
        file_name_0 = "WDS_ANN_DATE_"+  str( int(date_1) ) +"_ALL.csv"
        df_0 = pd.read_csv( self.path_wds+table_name +"\\"+ file_name_0 )
        
        df_0 = df_0[df_0["F_INFO_WINDCODE"] == code_fund ]
        index_close_1 = df_0["F_NAV_ADJUSTED"].values[0]
        
        period_chg = index_close_1/index_close_0-1 
        print("Price change ",code_fund,date_start,date_end ,period_chg )
        

        return period_chg 

    ### 2.6,计算指数定期调整
    def cal_index_constituents_adjustment(self,file_index_consti_end,file_consti_io,file_path):
        ### 
        '''
        input:
        1,nasdaq100_20200310.csv有2列：code
        2,nasdaq100_in_out_1997_2020.csv：有3列：year	in_out	code

        notes:
        1,从最新年份往旧年份计算时，t年的成分股应该先将t-1年纳入股票减去，再将t-1年剔除股票加回。
        2, raw data from rc_标普500成分股及的行业权重变化_1960_2015.xlsx
        '''
        ### 
        import pandas as pd 
        
        index_consti_end = pd.read_csv(file_path +file_index_consti_end  )
        file_consti_io = pd.read_csv(file_path + file_consti_io )
        print("index_consti_end  ",index_consti_end.head() )
        print("file_consti_io ", file_consti_io.head()  )
        
        year_list= file_consti_io["year"].drop_duplicates().values
        # [2019 2018 2017 2016 2015 2014 2013 2012 2011 2010 2009 2008 2007 2006
        # 2005 2004 2003 2002 2001 2000 1999 1998 1997]
        # print(year_list )
        index_consti_end_temp = index_consti_end
        len_index = len(index_consti_end_temp.index)
        count_year = 0 

        for temp_year in year_list :
            print("year : ", temp_year )
            temp_io = file_consti_io [ file_consti_io["year"]==temp_year  ]
            
            ### t年的成分股应该先将t-1年纳入股票减去
            temp_io_in = temp_io [temp_io["in_out"] == 1 ]

            for temp_i in temp_io_in.index :
                temp_code = temp_io_in.loc[temp_i,"code"]
                
                index_consti_end_temp = index_consti_end_temp[ index_consti_end_temp["code"] != temp_code ]
                
                len_index_pre = len_index
                len_index = len(index_consti_end_temp.index)
                print("temp_code", temp_code , len_index_pre, len_index )

            # for year 2019,symbol drop from 103 to 
            ### 再将t-1年剔除股票加回。 
            temp_io_out = temp_io [temp_io["in_out"] == -1 ]
            for temp_i in temp_io_out.index :
                temp_code = temp_io_out.loc[temp_i,"code"]
                index_new = index_consti_end_temp.index.max()
                index_consti_end_temp.loc[ index_new+1,"code"] = temp_code
                
                len_index_pre = len_index
                len_index = len(index_consti_end_temp.index)
                print("temp_code", temp_code , len_index_pre, len_index )
            
            ### 剔除重复的代码
            index_consti_end_temp = index_consti_end_temp.drop_duplicates("code")

            ### add current index constituents df to whole df 
            index_consti_end_temp["year"] = temp_year 

            if count_year == 0 :
                df_year_all = index_consti_end_temp
                count_year  = 1 
            else :
                df_year_all = df_year_all.append(index_consti_end_temp, ignore_index=True)
            
            ### Save to csv file 
            file_output = file_index_consti_end[:-4] + "_output.csv"
            df_year_all.to_csv(file_path +file_output)



        return index_constituents_hist


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

                        if len( temp_df.index) >= len_pre and len( temp_df.index)>=1 :
                            print(temp_date,"length of rows:",len(df_in.index),len_pre ) 
                            temp_df.loc[:,col_list].to_csv( self.path_wds+table_name+"\\"+ file_name_date  )               
                    else :
                        temp_df.loc[:,col_list].to_csv( self.path_wds+table_name+"\\"+ file_name_date  )
            except :
                pass
            
        return obj_in

























































































