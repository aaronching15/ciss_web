# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo
1，pd.read_csv时，可以用use_cols读取部分columns

功能:导入基金分析相关数据；基于Wind落地数据库的csv数据

derived from data_io.py
date:last 200526 | since 200608
===============================================
'''
from operator import index
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

import pandas as pd
import numpy as np
import json 
#######################################################################
### 导入配置文件对象，例如path_db_wind等
from config_data import config_data_fund_ana
config_data_fund_ana_1 = config_data_fund_ana()
from data_io import data_io 

#######################################################################
class data_io_fund_ana():
    def __init__(self):        
        self.obj_config = config_data_fund_ana_1.obj_config
        self.data_io_1 = data_io()
        #######################################################################
        ### 转换后的基金wds数据表
        path_wds_fund = self.obj_config["dict"]["path_wds_fund"]
        
        ### 基金分析数据输出目录
        path_ciss_db_fund = self.obj_config["dict"]["path_ciss_db_fund"]
        ### simulation 数据输出目录
        self.path_fund_simu = "D:\\CISS_db\\fund_simulation\\output\\"
        
    def print_info(self):        
        print("TODO   ")
        ### 股票基金列表和基础信息
        print("import_data_fund_ashare_des |导入股票基金列表和基础信息 ")        
        ### 给定基金列表，基金复权净值和排名
        print("import_data_fund_nav |根据基金列表，导入基金净值和排名数据 ") 
        ### 导入区间基金复权净值,给定基金代码或list
        print("import_data_fund_nav_period | 给定基金代码或list，导入基金净值 ") 

        ### 持仓数据:重仓个股;
        print("import_data_fund_holdings |导入基金前十大或全部持股 ")
        ### 交易换手率数据：历史换手率高低； 
        print("import_data_fund_profit_turnover |导入基金财务指标、利润份额、交易和换手率数据 ")
        ### 基金份额
        print("import_data_fund_fundshare |导入基金份额 ")

        ###基金分组：全市场、基金公司、基金风格、基金业绩、股票持仓、股票持仓分行业-中信1、2级
        print("import_data_fund_group_rating |导入基金分组、评级 ")

        ### 股票代码、名称、上市日期、中信一级行业、二级行业、
        print("import_data_fund_stock_name_listday_ind |导入基金持仓股票代码、名称、上市日、中信一级行业、二级行业 ")

        ### 基金持仓股票财务指标：pe_fy0,pe_fy1,roe_fy0,roe_fy1,profit_g_fy0,profit_g_fy1。
        print("import_data_fund_stock_indicators |导入基金持仓股票财务指标：pe_fy0,pe_fy1,roe_fy0,roe_fy1,profit_g_fy0,profit_g_fy1。 ")

        ### Brinson建模分析，基于披露的持仓股票：1，基准组合股票权重：沪深300、中证500、创业板；
        # 2，基金组合股票权重，和基准组合的差异
        print("import_data_fund_brinson |导入基金Brinson相对行业和市场基准组合分析 ")

        ### 输出Export：保存到csv文件
        print("export_data_fund |导出基金对象 ")

        return 1

    def import_data_fund_ashare_des(self,obj_fund):
        ### 导入股票基金列表和基础信息
        '''基础信息
        table=中国共同基金基本资料,ChinaMutualFundDescription
        columns=基金代码 F_INFO_WINDCODE；成立日期 F_INFO_SETUPDATE；到期日期 F_INFO_MATURITYDATE；是否指数基金 IS_INDEXFUND，0:否 1:是；
        退市日期F_INFO_DELISTDATE,
        table=中国共同基金基金经理,ChinaMutualFundManager
        columns= Wind代码 F_INFO_WINDCODE,公告日期 ANN_DATE;姓名 F_INFO_FUNDMANAGER;任职日期,F_INFO_MANAGER_STARTDATE;离职日期 F_INFO_MANAGER_LEAVED;
        基金经理ID,F_INFO_FUNDMANAGER_ID
        notes:应该以基金经理ID为准，同名同姓的基金经理有比较多的例子。
        '''
        ### Initialization 
        ### 最新披露日期和下一个披露日期
        date_adj_port = obj_fund["dict"]["date_adj_port"] 
        date_adj_port_next = obj_fund["dict"]["date_adj_port_next"] 
        ### 最新季报截止日和下一个季报截止日
        date_ann = obj_fund["dict"]["date_ann"] 
        # date_ann_next = obj_fund["dict"]["date_ann_next"] 
        ### 总共需要前推4~8个季度

        ### 给定日期，获取最近的2~N个季度财务日期及所属第1~4个季度数量
        obj_date ={} 
        obj_date["date"] = date_ann 
        obj_date = self.data_io_1.get_report_date_fund(obj_date)
        ### 给定日期，获取最近的2~N个季度财务日期及所属第1~4个季度数量
        date_q = obj_date["date_q"] 
        date_q_pre = obj_date["date_q_pre"] 
        date_q_pre2 = obj_date["date_q_pre2"] 

        # 前1~2年（4、8个季度）时间,选有全部持仓披露的季末
        date_q_pre_1y = obj_date["date_q_pre_1y"] 
        date_q_pre_2y = obj_date["date_q_pre_2y"] 
        # 最近一个披露全部持仓的季度
        date_q_6or12m = obj_date["date_q_6or12m"]

        print("date_q, date_q_pre ",date_q,date_q_pre  )  
        # obj_date["list_para_date"] =[4,3],是对应过去2个季度[1,4]的list
        date_quarter_pastN =obj_date["date_quarter_pastN"]

        ###############################################################################
        ### 获取 date_ann 当期披露持仓的基金代码列表 |本模块中暂时不需要
        # 中国共同基金投资组合——持股明细， ChinaMutualFundStockPortfolio
        # table_name = "ChinaMutualFundStockPortfolio"
        # path_table = self.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
        # file_name = "WDS_F_PRT_ENDDATE_" + date_q + "_ALL.csv"
        # df_fund_stocks = pd.read_csv( path_table + file_name )
        
        ###############################################################################
        ### 导入基金基础数据，获取中文、管理人，基金经理等
        table_name = "ChinaMutualFundDescription"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
        file_name = "WDS_full_table_full_table_ALL.csv"
        # 截止2006约有13000个基金信息，0509时约有200个记录
        df_fund_des = pd.read_csv( path_table + file_name )
        
        '''
        name value note  
        F_INFO_BENCHMARK 基准构成  沪深300指数收益率*70%+中证全债指数收益率*30%
        后端代码	F_INFO_BACKEND_CODE
        前端代码	F_INFO_FRONT_CODE
        Wind代码	F_INFO_WINDCODE
        名称	F_INFO_FULLNAME
        简称	F_INFO_NAME
        管理人	F_INFO_CORP_FUNDMANAGEMENTCOMP
        成立日期	F_INFO_SETUPDATE
        到期日期	F_INFO_MATURITYDATE
        发行份额	F_ISSUE_TOTALUNIT
        货币代码	CRNY_CODE
        发行日期	F_INFO_ISSUEDATE
        是否为初始基金	F_INFO_ISINITIAL
        退市日期	F_INFO_DELISTDATE
        上市时间	F_INFO_LISTDATE
        ---notes：
        1，是否指数基金，IS_INDEXFUND，0:否 1:是
        2，基金WIND代码中带"!"感叹号的，是什么意思?A11：老的基金代码被新基金复用，所以加上表示老基金。一般这种老基金是到期的或者转型了。
        3，F_INFO_ISINITIAL判断的逻辑是什么？A1：是按照出现最早的代码自行设定的
        '''
        col_list=["IS_INDEXFUND","F_INFO_BACKEND_CODE","F_INFO_FRONT_CODE","F_INFO_WINDCODE","F_INFO_FULLNAME","F_INFO_NAME"]
        col_list=col_list +[ "F_INFO_CORP_FUNDMANAGEMENTCOMP","F_INFO_SETUPDATE","F_INFO_MATURITYDATE"]
        col_list=col_list +[ "F_ISSUE_TOTALUNIT","CRNY_CODE","F_INFO_ISSUEDATE","F_INFO_ISINITIAL","F_INFO_DELISTDATE","F_INFO_LISTDATE","F_INFO_BENCHMARK"]
        
        df_fund_des = df_fund_des.loc[:, col_list ]
        ######################################################################################
        ### 筛选条件，需要剔除指数基金、重复项基金等：
        ### 1，只选取非指数基金 | 2020Q1的情况，总共13244只基金，剔除指数后10824，有指数基金 2420个。 
        df_fund_des = df_fund_des[ df_fund_des["IS_INDEXFUND"]== 0 ] 
        ### 2,剔除联结基金，例如：160706.SZ--嘉实300;160724.OF--嘉实沪深300ETF联接(LOF)C,两者全名F_INFO_FULLNAME一样；
        # 但成立日期不同，160706.SZ--20050829；160724.OF--20180808.F_INFO_ISINITIAL前者为1，后者为0
        # 发行份额（亿份）不同：F_ISSUE_TOTALUNIT：160706.SZ=8.6713；160724.OF=""
        # 要保留先成立的基金，先将df_fund_des 按照日升序排列，下一步去重复项时保留第一项
        df_fund_des = df_fund_des.sort_values(by="F_INFO_SETUPDATE",ascending=True  )
        # inplace: True: 原来的df会被修改, 同时不会返回新的df False: 原来的df不会被修改, 会返回新的df  
        df_fund = df_fund_des.drop_duplicates(subset=["F_INFO_FULLNAME"], keep="first",inplace=False   )
        # 仅保留发行份额大于 0 亿的基金
        df_fund = df_fund[ df_fund["F_ISSUE_TOTALUNIT"]>0 ]        
        
        ### 新增columns "fund_code"
        df_fund[ "fund_code"] = df_fund[ "F_INFO_WINDCODE"]

        ### 获取满足筛选条件的，所有基金列表 fund_list
        fund_list = df_fund["F_INFO_WINDCODE"].drop_duplicates().to_list()

        ### save to output and csv
        df_fund.to_csv(self.obj_config["dict"]["path_wind_adj"]+"fund_ana\\fund_des_ashares.csv" ,encoding="gbk" )

        # 最新季报截止日和下一个季报截止日
        obj_fund["dict"]["date_report"] = date_q
        obj_fund["dict"]["date_report_pre"] = date_q_pre
        obj_fund["dict"]["date_report_pre2"] = date_q_pre2
        obj_fund["dict"]["date_report_pre_1y"] = date_q_pre_1y
        obj_fund["dict"]["date_report_pre_2y"] = date_q_pre_2y
        obj_fund["dict"]["date_q_6or12m"] = date_q_6or12m
        obj_fund["dict"]["date_quarter_pastN"] = date_quarter_pastN 
        obj_fund["fund_list"] = fund_list
        obj_fund["df_fund"] = df_fund
        obj_fund["col_list_fund_des"] = col_list

        return obj_fund


    def import_data_fund_nav(self,obj_fund):
        ### 根据基金列表，导入基金净值和排名数据 
        # notes:fund_list是不考虑时间的所有基金列表
        date_q = obj_fund["dict"]["date_report"] 
        date_q_pre = obj_fund["dict"]["date_report_pre"] 
        fund_list = obj_fund["fund_list"] 
        df_fund = obj_fund["df_fund"] 
        col_list = obj_fund["col_list_fund_des"] 
        # 所有交易日 obj_fund["date_list_period"]
        date_list_period = obj_fund["date_list_period"]

        ###############################################################################
        ### 导入基金年初至今和近1年排名数据
        table_name = "ChinaMutualFundNAV"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
        file_name = "WDS_ANN_DATE_"+ obj_fund["dict"]["date_adj_port"] + "_ALL.csv"
        # 截止2006约有13000个基金信息，0509时约有200个记录
        ### 判断是否存在该文件
        if os.path.exists( path_table + file_name ) :
            df_fund_nav = pd.read_csv( path_table + file_name )
        else :
            temp_date = obj_fund["dict"]["date_adj_port"]
            ### 获取 temp_date 之前的交易日,注意要将string 变成int
            date_list_period_sub =[x for x in date_list_period if x < int(temp_date) ]
            print("Debug==== ",temp_date  ,  " date_list_period " )
            print(  date_list_period[:3], date_list_period[-3:] )
            
            # 假设前一个交易日肯定有数据，否则要多找几个交易日。
            temp_date = str(int(max(date_list_period_sub) ))
            file_name = "WDS_ANN_DATE_"+temp_date  + "_ALL.csv"
            df_fund_nav = pd.read_csv( path_table + file_name )
        
        ### 复权单位净值 F_NAV_ADJUSTED 
        df_fund_nav_sub = df_fund_nav[ df_fund_nav["F_INFO_WINDCODE"].isin(fund_list) ]
        for temp_i in df_fund.index :
            temp_fund_code = df_fund.loc[temp_i, "F_INFO_WINDCODE"]
            # find code in 
            temp_df = df_fund_nav_sub[ df_fund_nav_sub["F_INFO_WINDCODE"]==temp_fund_code ]
            if len(temp_df.index) > 0 :
                df_fund.loc[temp_i, "F_NAV_ADJUSTED"] = temp_df["F_NAV_ADJUSTED"].values[0]

        ###############################################################################
        ### 导入基金年初至今和近1年排名数据,table=中国共同基金业绩表现，ChinaMFPerformance
        # notes:原始数据中，基金会包括股票、债券、货币等不同分类的排名，需要
        table_name = "ChinaMFPerformance"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
        file_name = "WDS_TRADE_DT_"+ obj_fund["dict"]["date_adj_port"] + "_ALL.csv"
        df_fund_rank = pd.read_csv( path_table + file_name )

        '''例子：
        今年以来同类排名	F_SFRANK_THISYEAR
        最近三月同类排名	F_SFRANK_RECENTQUARTER
        最近六月同类排名	F_SFRANK_RECENTHALFYEAR
        最近一年同类排名	F_SFRANK_RECENTYEAR
        最近两年同类排名	F_SFRANK_RECENTTWOYEAR
        最近三年同类排名	F_SFRANK_RECENTTHREEYEAR
        最近五年同类排名	F_SFRANK_RECENTFIVEYEAR        
        '''
        ### 更新 df_fund,必须在当年有排名
        # print("Debug===",file_name, df_fund_rank.columns  )
        fund_list_rank = df_fund_rank["S_INFO_WINDCODE"].to_list()
        df_fund = df_fund[ df_fund["F_INFO_WINDCODE"].isin( fund_list_rank ) ]
        
        ### 基金排名数据
        df_fund_rank_sub = df_fund_rank[ df_fund_rank["S_INFO_WINDCODE"].isin(fund_list) ]

        ### 只保存包含"F_" 前缀的列名
        col_list_fund_rank =  df_fund_rank_sub.columns
        col_list_fund_rank = [col for col in col_list_fund_rank if "F_" in col  ]

        for temp_i in df_fund.index :
            temp_fund_code = df_fund.loc[temp_i, "F_INFO_WINDCODE"]
            # find code in 
            temp_df = df_fund_rank_sub[ df_fund_rank_sub["S_INFO_WINDCODE"]==temp_fund_code ]
            if len(temp_df.index) > 0 :
                for temp_col in col_list_fund_rank :
                    df_fund.loc[temp_i, temp_col] = temp_df[temp_col].values[0]

        ### 更新fund_list
        fund_list_sub = df_fund["F_INFO_WINDCODE"].to_list()
        obj_fund["fund_list"] = [code for code in fund_list if code in fund_list_sub]
        
        ### save to obj,csv
        obj_fund["df_fund"] =df_fund
        obj_fund["col_list_fund_rank"]  = col_list_fund_rank

        return obj_fund

    def import_data_fund_nav_period(self,obj_fund):
        ########################################################################
        ### 给定基金代码或list，导入基金净值
        ### wind or "tushare"
        if obj_fund["data_source"] == "wind" :
            # "F_NAV_ADJUSTED", table="ChinaMutualFundNAV",file_name="WDS_ANN_DATE_20210101_ALL.csv"
            table_name = "ChinaMutualFundNAV"
            path_table = self.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
        elif obj_fund["data_source"] == "tushare" :
            path_table = self.obj_config["dict"]["path_tushare"] + "fund_nav\\"
            ### 提取导入api
            sys.path.append( os.getcwd()[:2] +"\\rc_HUARONG\\rc_HUARONG\\ciss_web\\CISS_rc\\db\\data_io" )
            from data_io_tushare import data_ts
            data_ts_1 = data_ts()
            obj_data ={}
            obj_data["type"] = "nav" # basic 对应基础信息

        ###  日期升序排列，方便计算收益率
        obj_fund["date_list"].sort()

        ########################################################################
        ###  判断是否单一基金
        if obj_fund["if_1fund"] == 1 :
            ########################################################################            
            ### 为了避免df_fund_ret中不同格式的日期column出现重复，要把所有columns变成str格式
            obj_fund["df_fund_ret"].columns = [str(temp_date) for temp_date in obj_fund["df_fund_ret"].columns  ]
            
            nav_adj_pre = -1
            fund_code = obj_fund["fund_code"] 
            for temp_date in obj_fund["date_list"]  :
                file_name = "WDS_ANN_DATE_"+ str(int(temp_date)) + "_ALL.csv"

                print("file-name ",file_name , str(int(temp_date)) )
                ### 判断是否存在该文件
                if os.path.exists( path_table + file_name ) :
                    df_fund_nav = pd.read_csv( path_table + file_name )
                    temp_df = df_fund_nav[ df_fund_nav["F_INFO_WINDCODE"]== fund_code ]
                    if len(temp_df.index) > 0 :
                        if nav_adj_pre < 0 :
                            ### 第一个交易日收益率设置为 0.0 
                            # notes:日期格式如果不是str可能会报错
                            obj_fund["df_fund_ret"].loc[fund_code, str(int(temp_date)) ] = 0.0 
                            # obj_fund["df_fund_ret"].loc[fund_code, int(temp_date) ] = 0.0 
                            nav_adj_pre = temp_df["F_NAV_ADJUSTED"].values[0]
                        else :
                            # notes:日期格式如果不是str可能会报错
                            obj_fund["df_fund_ret"].loc[fund_code, str(int(temp_date)) ] =100*( temp_df["F_NAV_ADJUSTED"].values[0] /nav_adj_pre -1)
                            # obj_fund["df_fund_ret"].loc[fund_code, int(temp_date) ] =100*( temp_df["F_NAV_ADJUSTED"].values[0] /nav_adj_pre -1)
                            ### 更新上一交易日净值，keep previous unit_adj
                            nav_adj_pre = temp_df["F_NAV_ADJUSTED"].values[0] 
        
        ########################################################################
        ### 多个基金 
        fund_list = obj_fund["fund_list"]   

        ### notes:fund_list 里有可能 有部分基金刚成立或者马上要清盘没有净值。
        count_days = 0
        nav_adj_pre = -1 
        ### df_fund_ret:index是基金代码，columns是交易日期
        
        ### 对于给定日期没有对应的数据文件，需要用历史之前的日期替代
        # 给定交易日，获取之前和之后的交易日列表
        obj_date={}
        obj_date["date"] = obj_fund["date_list"][0]
        obj_date = self.data_io_1.get_trading_days(obj_date)
        
        date_list_before =  obj_date["date_list"]
        date_list_before = [int(i) for i in date_list_before  ]
        date_list_before = [i for i in date_list_before if  i >= int( obj_fund["date_list"][0])-100 ]
        # ### 日期序列默认是从远期到近期，要改成从近期到远期
        date_list_before.sort()
        date_list = date_list_before[::-1] 

        ###################################
        ### Wind数据
        if not obj_fund["if_1fund"] == 1 and  obj_fund["data_source"] == "wind" :           
            
            for temp_date in obj_fund["date_list"]  :               
                file_name = "WDS_ANN_DATE_"+ str(int(temp_date)) + "_ALL.csv"
                print("file-name ",file_name , str(int(temp_date)) )
                ### 判断是否存在该文件
                if os.path.exists( path_table + file_name ) :
                    df_fund_nav = pd.read_csv( path_table + file_name )
                    ###########################################################################
                    ### 如果拼接所有基金数据量太大，只需要符合的代码
                    df_fund_nav = df_fund_nav [ df_fund_nav["F_INFO_WINDCODE"].isin( fund_list ) ]
                    # print("Debug=== Num of funds to fetch ",len( df_fund_nav.index )  ) 

                    ### 赋值给 大的 obj_fund["df_fund_ret"]
                    if count_days == 0 :
                        date_1st= temp_date
                        # nav
                        df_all_nav = df_fund_nav.loc[:, ["F_INFO_WINDCODE","F_NAV_ADJUSTED" ] ] 
                        df_all_nav = df_all_nav.rename( columns={"F_NAV_ADJUSTED": temp_date }  ) 
                        count_days = count_days +1     
                    else :
                        df_temp = df_fund_nav.loc[:,  ["F_INFO_WINDCODE","F_NAV_ADJUSTED" ] ]
                        df_temp = df_temp.rename( columns={"F_NAV_ADJUSTED": temp_date }  ) 
                        # ,on="S_INFO_WINDCODE" 表示基于股票代码链接2个df
                        # notes: df_temp 比df_all 有更多的codes; how="outer"指的是用2个df列的并集
                        # # how="left"指的是以左侧df_all_nav为准
                        ### how="left",左连接是保留所有左表的信息，把右表中主键与左表一致的信息拼接进来，标签不能对齐的部分，用NAN进行填充：
                        # df_all_nav = pd.merge( df_all_nav, df_temp,how="left" ,on="F_INFO_WINDCODE"   ) 
                        df_all_nav = pd.merge( df_all_nav, df_temp ,how="left" ,on="F_INFO_WINDCODE"   ) 

                    ### 去除重复项 || 当subset是传入很多个值时, 要多个字段联合起来都是一样的才删除.  
                    df_all_nav = df_all_nav.drop_duplicates(subset=["F_INFO_WINDCODE", temp_date ] ,keep='first' )
                
                if not os.path.exists( path_table + file_name ) and count_days > 0 :
                    ########################################### 
                    ### 可能没有数据文件，那只能用前一天的净值数据
                    df_all_nav[temp_date] = df_all_nav[ date_pre] 
                    
                if not os.path.exists( path_table + file_name )  and count_days == 0 :
                    ### 说明第一天就没有数据，这时候用temp_date之前的1个交易日文件替代
                    for temp_date_pre in date_list_before :
                        file_pre = "WDS_ANN_DATE_"+ str(int(temp_date_pre)) + "_ALL.csv"
                        if os.path.exists( path_table + file_pre ) :
                            df_fund_nav = pd.read_csv( path_table + file_pre )
                            ### 将文件保存至文件夹
                            df_fund_nav.to_csv( path_table + file_name )
                            ###########################################################################
                            ### 如果拼接所有基金数据量太大，只需要符合的代码
                            df_fund_nav = df_fund_nav [ df_fund_nav["F_INFO_WINDCODE"].isin( fund_list ) ]
                            # print("Debug=== Num of funds to fetch ",len( df_fund_nav.index )  ) 

                            ### 赋值给 大的 obj_fund["df_fund_ret"]
                            date_1st= temp_date
                            # nav
                            df_all_nav = df_fund_nav.loc[:, ["F_INFO_WINDCODE","F_NAV_ADJUSTED" ] ] 
                            df_all_nav = df_all_nav.rename( columns={"F_NAV_ADJUSTED": temp_date }  ) 
                            count_days = count_days +1     
        
                
                ########################################### 
                ###  
                date_pre = temp_date
            ### 

        ########################################### 
        ### tushare数据
        if not obj_fund["if_1fund"] == 1 and  obj_fund["data_source"] == "tushare" :  
            for temp_date in obj_fund["date_list"]  :     
                temp_date = str(temp_date)
                file_name = "ts_fund_nav_" + str(temp_date) + ".xlsx"
                ##################################
                ### 判断是否存在该文件
                if os.path.exists( path_table + file_name ) :
                    df_fund_nav = pd.read_excel( path_table + file_name )
                    df_fund_nav = df_fund_nav.drop( ["Unnamed: 0"], axis=1 )
                
                else :
                    ##################################
                    ### 导入 tushare api下载数据
                    obj_data["temp_date"] = temp_date # "20211119"
                    obj_data = data_ts_1.get_ts_fund_basic( obj_data)
                    df_fund_nav = obj_data["df_nav_date"]
                
                ### columns改名
                df_fund_nav = df_fund_nav.rename( columns={"ts_code": "F_INFO_WINDCODE"}  )  
                
                ###########################################################################
                ### 如果拼接所有基金数据量太大，只需要符合的代码       
                df_fund_nav = df_fund_nav [ df_fund_nav["F_INFO_WINDCODE"].isin( fund_list ) ] 
                ##################################
                ### 拼接数据
                if count_days == 0 :
                    date_1st= temp_date
                    # nav
                    df_all_nav = df_fund_nav.loc[:, ["F_INFO_WINDCODE","accum_nav" ] ] 
                    df_all_nav = df_all_nav.rename( columns={"accum_nav": temp_date }  ) 
                    count_days = count_days +1   
                else :
                    df_temp = df_fund_nav.loc[:,  ["F_INFO_WINDCODE","accum_nav" ] ]
                    df_temp = df_temp.rename( columns={"accum_nav": temp_date }  )  
                    ### how="left",左连接是保留所有左表的信息，把右表中主键与左表一致的信息拼接进来，标签不能对齐的部分，用NAN进行填充：
                    # df_all_nav = pd.merge( df_all_nav, df_temp,how="left" ,on="F_INFO_WINDCODE"   )  
                    df_all_nav = pd.merge( df_all_nav, df_temp ,on="F_INFO_WINDCODE"   ) 
                ##################################
                
        ###########################################################################
        ### 1, 保留对应代码地基金；2，把nav转换成 ret
        ### 填充nan： method参数的取值 ： {‘pad’, ‘ffill’,‘backfill’, ‘bfill’, None}, default None
        ### pad/ffill：用前一个非缺失值去填充该缺失值；backfill/bfill：用下一个非缺失值填充该缺失值
        ### axis=1意味着每一行按左边第一个数值向右边每一个填充。
        df_all_nav = df_all_nav.fillna(method="ffill",axis=1)     

        ### notes: 千万不能 df/df[0],那是用df每一列分别除以df[0]的每个值 
        ### notes： fund_list 里有些可能没有净值
        df_all_nav.index   = df_all_nav ["F_INFO_WINDCODE"]
        fund_list_real = [ i for i in fund_list if i in df_all_nav["F_INFO_WINDCODE"]    ]
        print("Num of funds to fetch vs NUm of .. Exists",len(fund_list) , len(fund_list_real ) )
        df_all_nav = df_all_nav.loc[fund_list_real, :  ]
        
        obj_fund["fund_list_error"] = []
        ### 
        df_all_nav.to_excel("D:\\df_all_nav.xlsx")
        # 弃用的方法： obj_fund["date_list"]  固定第一列的数值   series_nav = df_all_nav[date_1st]
        print("Debug==date_1st:",date_1st )
        for temp_i in df_all_nav.index :   
            ### notes:ERROR报错一般是wind基金净值文件缺失| 例如净值文件WDS_ANN_DATE_20081103_ALL.csv没有，那么只能用20081101代替
            # 解决方法：用临近净值数据文件替代
            df_all_nav.loc[temp_i, obj_fund["date_list"] ] = df_all_nav.loc[temp_i, obj_fund["date_list"] ] /df_all_nav.loc[temp_i, date_1st ]
            
        ### debug  
        obj_fund["df_fund_ret"]= df_all_nav
        
        
        return obj_fund

    def import_data_fund_holdings(self,obj_fund ):
        ### 导入基金前十大或全部持股
        '''1,中国共同基金投资组合——持股明细,ChinaMutualFundStockPortfolio;
        columns=[持有股票Wind代码,S_INFO_STOCKWINDCODE;持有股票市值(元),F_PRT_STKVALUE ;持有股票数量（股）,F_PRT_STKQUANTITY;持有股票市值占基金净值比例(%),F_PRT_STKVALUETONAV ; ]
        2,中国共同基金投资组合——资产配置,ChinaMutualFundAssetPortfolio;持有股票市值占资产净值比例(%),F_PRT_STOCKTONAV
        3,大变动(报告期),CFundPortfoliochanges,股票代码,S_INFO_WINDCODE;变动类型,CHANGE_TYPE;累计金额,ACCUMULATED_AMOUNT,占期初基金资产净值比例,BEGIN_NET_ASSET_RATIO;
        4,
        '''
        ###############################################################################
        ### 导入基金代码及排名列表 df_fund
        df_fund = obj_fund["df_fund"] 

        ###############################################################################
        ### 导入基金股票市值占比和资产配置指标：
        '''
        截止日期	F_PRT_ENDDATE
        资产净值(元)	F_PRT_NETASSET
        持有股票市值(元)	F_PRT_STOCKVALUE
        持有股票市值占资产净值比例(%)	F_PRT_STOCKTONAV
        持有国债及现金占资产净值比例(%)	F_PRT_GOVCASHTONAV
        持有债券市值(不含国债)占资产净值比例(%)	F_PRT_BDTONAV_NOGOV
        持有基金市值占资产净值比例(%)	F_PRT_FUNDTONAV

        '''
        ###
        fund_list = obj_fund["fund_list"] 

        ###对给定column赋值，仅仅保留市值大于等于4千万的基金，并更新基金列表fund_list和df_fund
        col_list_asset_allo = ["F_PRT_ENDDATE","F_PRT_NETASSET","F_PRT_STOCKVALUE","F_PRT_STOCKTONAV","F_PRT_GOVCASHTONAV","F_PRT_BDTONAV_NOGOV","F_PRT_FUNDTONAV"]
        table_name = "ChinaMutualFundAssetPortfolio"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
        file_name = "WDS_F_PRT_ENDDATE_"+ obj_fund["dict"]["date_report"] + "_ALL.csv"
        # 若数据文件不存在，则返回错误码 obj_fund["if_missing_file"]
        try :
            df_fund_asset_allo = pd.read_csv( path_table + file_name )
            df_fund_asset_allo = df_fund_asset_allo[ df_fund_asset_allo["S_INFO_WINDCODE"].isin(fund_list) ]
            ### 更新fund_list ：剔除股票市值小于3千万的,F_PRT_STOCKVALUE
            para_filter = 40000000.0
            df_fund_asset_allo = df_fund_asset_allo[ df_fund_asset_allo["F_PRT_STOCKVALUE"]>= para_filter ]
            fund_list_filter = df_fund_asset_allo["S_INFO_WINDCODE"].drop_duplicates().to_list()
            # notes:即便剔除后，基金类型"F_FUNDTYPE"还有 8种，也就是“同类排名”项目下，会有多个第一名1。
            # [普通股票型基金(封闭式),平衡混合型基金(封闭式),灵活配置型基金,平衡混合型基金,平衡混合型基金,偏股混合型基金,混合债券型二级基金,偏股混合型基金]

            ### 更新基金列表fund_list和df_fund
            fund_list = [ code for code in fund_list if code in fund_list_filter  ]
            df_fund = df_fund[ df_fund["F_INFO_WINDCODE"].isin( fund_list ) ]
            
            for temp_i in df_fund.index :
                temp_fund_code = df_fund.loc[temp_i, "F_INFO_WINDCODE"]
                # find code in 
                temp_df = df_fund_asset_allo[ df_fund_asset_allo["S_INFO_WINDCODE"]==temp_fund_code ]
                if len(temp_df.index) > 0 :
                    for temp_col in col_list_asset_allo :
                        df_fund.loc[temp_i, temp_col] = temp_df[temp_col].values[0]

            ### save to obj,csv
            obj_fund["fund_list"] = fund_list
            obj_fund["df_fund"] = df_fund
            obj_fund["col_list_asset_allo"]  = col_list_asset_allo
            obj_fund["if_missing_ap"] = 0 
        except:
            # missing asset portfolio
            print("Missing file= " , path_table + file_name  )
            obj_fund["if_missing_ap"] = 1 

        ###############################################################################
        ### 导入基金单个股票市值指标：| 每个基金会有多个股票持仓，似乎应该单独保存在1个df里
        # notes:半年报和年报会有2个公告日期，分别对应前十大重仓和全部持仓。
        '''持有股票市值(元)	F_PRT_STKVALUE
        持有股票市值占基金净值比例(%)	F_PRT_STKVALUETONAV
        占股票市值比	STOCK_PER
        占流通股本比例(%)	FLOAT_SHR_PER
        公告日期 ANN_DATE
        '''
        table_name = "ChinaMutualFundStockPortfolio"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
        # 若数据文件不存在，则返回错误码 obj_fund["if_missing_file"]
        try :
            df_fund_asset_allo = pd.read_csv( path_table + file_name )
        
            file_name = "WDS_F_PRT_ENDDATE_"+ obj_fund["dict"]["date_report"] + "_ALL.csv"
            df_fund_stock_port = pd.read_csv( path_table + file_name )
            ### 仅保留 fund_list 内的持仓：| 基金Wind代码，S_INFO_WINDCODE
            
            df_fund_stock_port =df_fund_stock_port[ df_fund_stock_port["S_INFO_WINDCODE"].isin(fund_list) ]

            col_list_stock_port = ["ANN_DATE","F_PRT_STKVALUE","F_PRT_STKVALUETONAV","STOCK_PER","FLOAT_SHR_PER"]
            
            ### save to obj,csv | "stock_list_fund" 应该是基金持仓的股票代码list
            obj_fund["stock_list_fund"] = df_fund_stock_port["S_INFO_STOCKWINDCODE"].drop_duplicates().to_list() 
            df_fund_stock_port["fund_code"] = df_fund_stock_port["S_INFO_WINDCODE"]
            obj_fund["df_fund_stock_port"] = df_fund_stock_port 
            obj_fund["col_list_stock_port"]  = col_list_stock_port
            obj_fund["if_missing_sp"] = 0
        except:
            # # missing stock portfolio
            print("Missing file= " , path_table + file_name  )
            obj_fund["if_missing_sp"] = 1 
        
        ###
        
        return obj_fund

    def import_data_fund_profit_turnover(self,obj_fund):
        ### 导入基金份额、交易和换手率数据 
        '''
        notes:
        1,只有6,12月才有该数据
        2,"ChinaMutualFundSeatTrading"对单一期，单一基金会有多个券商的记录，需要求和

        基金财务数据：
        3, 中国共同基金财务指标(报告期)，CMFFinancialIndicator
        notes:20171231开始每半年发布一次。
        4，中国共同基金利润表，CMFIncome；
        columns=[投资收益合计,INV_INC;股票差价收入,STOCK_INV_INC;股息收入,DVD_INC;未实现利得,CHANGE_FAIR_VALUE;管理费,MGMT_EXP  ]
        1，中国Wind基金仓位估算，ChinaMutualFundPosEstimation，主要是区分大、中、小市值在组合内的权重
        2，中国共同基金席位交易量及佣金，ChinaMutualFundSeatTrading
            notes:判断换手率：从"股票交易金额(元),F_TRADE_STOCKAM";"股票交易金额占比(%),F_TRADE_STOCKPRO"
        3，中国共同基金份额，ChinaMutualFundShare
            基金合计份额(万份),FUNDSHARE_TOTAL
        
        '''
        ###############################################################################
        ### 导入基金代码及排名列表 df_fund
        df_fund = obj_fund["df_fund"] 

        ### todo，1，中国共同基金财务指标(报告期)，CMFFinancialIndicator【20171231开始才有较全的数据】
        # notes:公允价值变动损益 STOCK_CHANGE_FAIR_VALUE = ANAL_INCOME - ANAL_NETINCOME
        # '''本期利润扣减本期公允价值变动损益后的净额	ANAL_NETINCOME
        # 本期利润	ANAL_INCOME
        # 加权平均基金份额本期利润	ANAL_AVGNETINCOMEPERUNIT
        # 加权平均净值利润率(%)	ANAL_AVGNAVRETURN
        # 净值增长率(%)	ANAL_NAV_RETURN
        # 累计净值增长率(%)	NAV_ACC_RETURN
        # 期末可供分配基金收益	ANAL_DISTRIBUTABLE
        # 期末可供分配单位基金收益	ANAL_DISTRIBUTABLEPERUNIT
        # 资产净值	PRT_NETASSET
        # 单位净值	PRT_NETASSETPERUNIT
        # 资产总值	PRT_TOTALASSET
        # 期末基金总份额(份)	UNIT_TOTAL
        # '''

        ### 中国共同基金席位交易量及佣金，ChinaMutualFundSeatTrading | 只有6、12月有数据
        '''股票交易金额(元) F_TRADE_STOCKAM;股票交易金额占比(%) F_TRADE_STOCKPRO
        '''
        # columns= 报告期，S_INFO_REPORTPERIOD
        table_name = "ChinaMutualFundSeatTrading"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
        file_name = "WDS_S_INFO_REPORTPERIOD_" + obj_fund["dict"]["date_report"] + "_ALL.csv"
        df_fund_asset_allo = pd.read_csv( path_table + file_name )

        col_list_fund_trade = df_fund_asset_allo.columns
        col_list_fund_trade = [x for x in col_list_fund_trade if "F_" in x  ]

        for temp_i in df_fund.index :
            temp_fund_code = df_fund.loc[temp_i, "F_INFO_WINDCODE"]
            # find code in 
            temp_df = df_fund_asset_allo[ df_fund_asset_allo["S_INFO_WINDCODE"]==temp_fund_code ]
            if len(temp_df.index) > 0 :
                for temp_col in col_list_fund_trade :
                    # df_fund.loc[temp_i, temp_col] = temp_df[temp_col].values[0]
                    df_fund.loc[temp_i, temp_col] = temp_df[temp_col].sum()
        
        ### save to csv
        obj_fund["df_fund"] = df_fund
        obj_fund["dict"]["col_list_fund_trade"] = col_list_fund_trade

        return obj_fund
    
    def import_data_fund_fundshare(self,obj_fund):
        ### 导入基金份额

        ### 导入基金代码及排名列表 df_fund
        df_fund = obj_fund["df_fund"] 

        # columns= CHANGE_DATE,份额变动日期；其中有很多变动原因，"CHANGEREASON"最常见的是SGSH申购赎回。
        table_name = "ChinaMutualFundShare"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name +"\\"
        file_name = "WDS_CHANGE_DATE_" + obj_fund["dict"]["date_report"] + "_ALL.csv"
        df_fundshare = pd.read_csv( path_table + file_name )

        # 基金份额(万份),FUNDSHARE | 非分级基金，基金合计份额(万份)（FUNDSHARE_TOTAL）=基金份额(万份)（FUNDSHARE）；
        for temp_i in df_fund.index :
            temp_fund_code = df_fund.loc[temp_i, "F_INFO_WINDCODE"]
            # find code in 
            temp_df = df_fundshare[ df_fundshare["F_INFO_WINDCODE"]==temp_fund_code ]
            if len(temp_df.index) > 0 :
                df_fund.loc[temp_i, "FUNDSHARE"] = temp_df["FUNDSHARE"].values[0]
        
        ### save to obj_fund
        obj_fund["df_fund"] = df_fund

        return obj_fund

    def import_data_fund_group_rating(self,obj_fund):
        ### 导入基金分组、评级
        todo
        '''中国共同基金第三方评级，CMFundThirdPartyRating【例如：海通证券】
        # notes:数据从200605开始；有银河、海通、上海证券；从200606开始就有的只有银河
        中国共同基金Wind基金评级，CMFundWindRating
        notes:评级会综合看不只看长短期业绩，还会看夏普，选股等其他因素
        上海证券-二市场风险因素分析模型：风险-夏普，选股-股票的超额收益，择时-超过基准收益稳定性。
        
        
        '''
        # file_name = WDS_RPTDATE_201801_ALL.csv | 按月


        return obj_fund

    def import_data_fund_stock_name_listday_ind(self,obj_fund):
        ### 导入基金持仓股票代码、名称、上市日、中信一级行业、二级行业
        #notes:基金持仓股票列表 df_fund_stock_port中单一股票会在多行出现
        df_fund_stock_port = obj_fund["df_fund_stock_port"]
        # notes:基金代码对应列名S_INFO_WINDCODE，需要重新复制给fund_code
        df_fund_stock_port["fund_code"] = df_fund_stock_port["S_INFO_WINDCODE"]

        stock_code_list = df_fund_stock_port["S_INFO_STOCKWINDCODE"].drop_duplicates().to_list()
        print("stock_code_list ",len(stock_code_list) )
        # obj_fund["dict"]["date_adj_port"]

        ### 1，导入股票代码、名称、上市日
        # col_list_stock_des="S_INFO_WINDCODE","S_INFO_NAME","S_INFO_LISTBOARDNAME","S_INFO_LISTDATE","S_INFO_DELISTDATE"
        obj_stock_des = self.data_io_1.get_stock_des_name_listday({})
        # obj_stock_des["col_list_stock_des"] = col_list
        df_stock_des = obj_stock_des["df_stock_des"] 
        col_list_stock_des = obj_stock_des["col_list_stock_des"]

        for temp_stock_code in stock_code_list :
            ### 定位该股票在基金持仓df内的index列表
            df_fund_stock_port_sub = df_fund_stock_port[ df_fund_stock_port["S_INFO_STOCKWINDCODE"]==temp_stock_code ]
            # 定位股票的基础信息
            df_stock_des_sub = df_stock_des[ df_stock_des["S_INFO_WINDCODE"] == temp_stock_code ]
            
            if len( df_stock_des_sub.index ) >0 :
                # 把每个描述信息column的值写入
                for temp_col_des in col_list_stock_des :
                    df_fund_stock_port.loc[df_fund_stock_port_sub.index, temp_col_des ] = df_stock_des_sub[temp_col_des].values[0]
        
        ### 导入行业,个股当时所属的行业分类 || get_ind_date(self,object_ind ) 
        object_ind={}
        object_ind["code_list"] = stock_code_list
        object_ind["date_end"] =  obj_fund["dict"]["date_adj_port"]
        object_ind["if_all_codes"] = 0 # 1 means inport all codes 
        object_ind["if_column_ind"] = "citics" # 1 means only match "column_ind","citics" means all citics
        # object_ind["column_ind"] = "citics_ind_code_s_1"
        object_ind = self.data_io_1.get_ind_date(object_ind )
        # object_ind["df_s_ind_sub"]的index是code_list,columns=["wind_code","ind_code"]
        df_s_ind =  object_ind["df_s_ind_sub"]
        col_list_ind = object_ind["col_list_ind"] 
        ### 把行业分类赋值给df
        for temp_stock_code in stock_code_list :
            ### 定位该股票在基金持仓df内的index列表
            df_fund_stock_port_sub = df_fund_stock_port[ df_fund_stock_port["S_INFO_STOCKWINDCODE"]==temp_stock_code ]
            # 定位股票的基础信息
            df_s_ind_sub = df_s_ind[ df_s_ind["wind_code"] == temp_stock_code ]
            
            if len( df_s_ind_sub.index ) >0 :
                # 把每个描述信息column的值写入
                for temp_col_ind in col_list_ind :
                    df_fund_stock_port.loc[df_fund_stock_port_sub.index, temp_col_ind ] = df_s_ind_sub[temp_col_ind].values[0]
        
                # assign "ind_code"
                df_fund_stock_port.loc[df_fund_stock_port_sub.index,"ind_code"] = df_s_ind_sub[ "ind_code"].values[0]

        ### save to obj 
        obj_fund["df_fund_stock_port"] = df_fund_stock_port

        return obj_fund

    def import_data_fund_stock_indicators(self,obj_fund):
        ### 导入基金持仓股票财务指标：pe_fy0,pe_fy1,roe_fy0,roe_fy1,profit_g_fy0,profit_g_fy1
        '''timing:ma_s_16,ma_s_40;abcd3d	indi_short	indi_mid;
        市值：S_DQ_MV	S_VAL_MV;成交额 S_DQ_AMOUNT；
        PE:EST_PE_FY1，EST_PE_FY0，EST_PE_YOY	
        PEG:EST_PEG_FY1，EST_PEG_FY0，EST_PEG_YOY
        ROE：EST_ROE_FY0
        净利润：NET_PROFIT_FY1,NET_PROFIT_FY0；NET_PROFIT_YOY
        收入和毛利：EST_OPER_PROFIT_FY0,EST_OPER_REVENUE_FY0，EST_TOTAL_PROFIT_FY0，ST_OPER_REVENUE_YOY	
        '''
        #notes:基金持仓股票列表 df_fund_stock_port中单一股票会在多行出现        
        df_fund_stock_port = obj_fund["df_fund_stock_port"]
        stock_code_list = df_fund_stock_port["S_INFO_STOCKWINDCODE"].drop_duplicates().to_list()

        print("stock_code_list ",len(stock_code_list) )
        # 最新交易日：obj_fund["dict"]["date_adj_port"]
        # 最近季度末日期： obj_fund["dict"]["date_report"]  
        from data_io_pricevol_financial import data_pricevol_financial
        data_pricevol_financial_1 = data_pricevol_financial()

        #######################################################################
        ### 1，导入披露日期"date_adj_port"的A股基础指标 for df_fund_stock_port from obj_data
        obj_data={}
        obj_data["dict"] ={}
        obj_data["dict"]["date_start"] = obj_fund["dict"]["date_adj_port"]
        # 价量：计算一段时间内每个交易日的abcd3d择时数据和财务指标
        obj_data = data_pricevol_financial_1.import_data_ashare_change_amt(obj_data)
        # output： obj_data["dict"]["date_tradingdate"]

        # 导入前1交易日obj_data["dict"]["date_tradingdate"] 的市值、财务指标ttm 、预期数据
        # date_pre取值季末日期["date_report"]用于导入预期数据；之前用的是["date_adj_port_pre"]
        obj_data["dict"]["date_pre"] = obj_fund["dict"]["date_report"]
        obj_data = data_pricevol_financial_1.import_data_ashare_mv_fi_esti(obj_data)
        # output: obj_data["df_mom_eod_prices"]
        df_mom_eod_prices = obj_data["df_mom_eod_prices"]
        ## 去除Unnamed
        col_str = "Unnamed"
        df_mom_eod_prices = self.data_io_1.del_columns(df_mom_eod_prices,col_str) 
        col_str = "OBJECT_ID"
        df_mom_eod_prices = self.data_io_1.del_columns(df_mom_eod_prices,col_str) 

        col_list = df_mom_eod_prices.columns

        ###  obj_data["df_mom_eod_prices"] 赋值给  df_fund_stock_port
        for temp_code in stock_code_list :
            #在 df_fund_stock_port 中定位index
            df_fund_stock_port_sub = df_fund_stock_port [df_fund_stock_port["S_INFO_STOCKWINDCODE"]==temp_code  ]

            # 个股指标
            df_sub = df_mom_eod_prices[df_mom_eod_prices["S_INFO_WINDCODE"]==temp_code ]
            if len(df_sub.index) > 0 :        
                for temp_col in col_list :
                    df_fund_stock_port.loc[df_fund_stock_port_sub.index, temp_col ] = df_sub[temp_col].values[0]

        #######################################################################
        ### 1，导入季度初["date_report_pre"]的A股基础指标 for df_fund_stock_port from obj_data
        obj_data={}
        obj_data["dict"] ={}
        obj_data["dict"]["date_start"] = obj_fund["dict"]["date_adj_port_pre"]
        # 价量：计算一段时间内每个交易日的abcd3d择时数据和财务指标
        obj_data = data_pricevol_financial_1.import_data_ashare_change_amt(obj_data)
        # output： obj_data["dict"]["date_tradingdate"]
        
        # 导入前1交易日obj_data["dict"]["date_tradingdate"] 的市值、财务指标ttm 、预期数据
        # date_pre取值季末日期["date_report"]用于导入预期数据；之前用的是["date_adj_port_pre"]
        obj_data["dict"]["date_pre"] = obj_fund["dict"]["date_report_pre"]
        obj_data = data_pricevol_financial_1.import_data_ashare_mv_fi_esti(obj_data)
        # output: obj_data["df_mom_eod_prices"]
        df_mom_eod_prices = obj_data["df_mom_eod_prices"]
        ## 去除Unnamed
        col_str = "Unnamed"
        df_mom_eod_prices = self.data_io_1.del_columns(df_mom_eod_prices,col_str) 
        col_str = "OBJECT_ID"
        df_mom_eod_prices = self.data_io_1.del_columns(df_mom_eod_prices,col_str) 

        
        col_list =[x for x in df_mom_eod_prices.columns if x ]
        
        ###  obj_data["df_mom_eod_prices"] 赋值给  df_fund_stock_port
        for temp_code in stock_code_list :
            #在 df_fund_stock_port 中定位index
            df_fund_stock_port_sub = df_fund_stock_port [df_fund_stock_port["S_INFO_STOCKWINDCODE"]==temp_code  ]
            # 个股指标
            df_sub = df_mom_eod_prices[df_mom_eod_prices["S_INFO_WINDCODE"]==temp_code ]
            if len(df_sub.index) > 0 :        
                for temp_col in col_list :
                    df_fund_stock_port.loc[df_fund_stock_port_sub.index, temp_col+"_pre" ] = df_sub[temp_col].values[0]
        
        ### save to output
        obj_fund["dict"]["col_list_stock_indicators"] = col_list 
        obj_fund["df_fund_stock_port"] = df_fund_stock_port

        return obj_fund
    
    def export_data_fund(self,obj_fund,obj_fund_ana):
        ### 导出基金对象
        '''
        要导出的对象：
        1，df类：obj_fund["df_fund"]；obj_fund["df_fund_stock_port"]
        2，dict类:obj_fund["dict"]
        3,list类：
            obj_fund["col_list_fund_rank"]            
            obj_fund["stock_list_fund"] = df_fund_stock_port["S_INFO_WINDCODE"].drop_duplicates().to_list() 
            obj_fund["fund_list"] = fund_list
            obj_fund["col_list_stock_port"]  = col_list_stock_port
            obj_fund["col_list_asset_allo"]  = col_list_asset_allo
        
        '''
        ########################################################################
        ### 判断输出目录是否存在
        # 1，转换后的基金wds数据表，self.obj_config["dict"]["path_wds_fund"]
        # 2，基金分析数据输出目录， self.obj_config["dict"]["path_ciss_db_fund"]
        path_export = self.obj_config["dict"]["path_ciss_db_fund"] + obj_fund_ana["dict"]["id_output"] +"\\"
        if not os.path.exists( path_export  ) :
            os.mkdir( path_export  )
        print("path_export: ",path_export  )

        ########################################################################
        ### Save obj_fund["dict"], and list 类
        
        file_name = "obj_fund_" + obj_fund["dict"]["date_adj_port"] + ".json"
        with open( path_export+ file_name,"w+") as f :
            json.dump( str(obj_fund["dict"]) ,f  )
        print("Dict data saved in Json file \n",path_export+ file_name )
        
        ### Save dict_list, 把list类保存至json
        file_name = "col_list_fund_rank_" + obj_fund["dict"]["date_adj_port"] + ".json"
        obj_fund["dict_list"] = obj_fund["dict"]
        obj_fund["dict_list"]["col_list_fund_rank"] = obj_fund["col_list_fund_rank"]
        obj_fund["dict_list"]["stock_list_fund"] = obj_fund["stock_list_fund"]
        obj_fund["dict_list"]["fund_list"] = obj_fund["fund_list"]
        obj_fund["dict_list"]["col_list_stock_port"] = obj_fund["col_list_stock_port"]
        obj_fund["dict_list"]["col_list_asset_allo"] = obj_fund["col_list_asset_allo"]

        file_name = "obj_fund_list_" + obj_fund["dict"]["date_adj_port"] + ".json"
        with open( path_export+ file_name,"w+") as f :
            json.dump( str(obj_fund["dict_list"]) ,f  )
        print("Dict_list data saved in Json file \n",path_export+ file_name )

        ########################################################################
        ### Save df_fund_stock_port; notes:有可能没有
        if "df_fund_stock_port" in obj_fund.keys() :
            file_name = "df_fund_stock_port_" + obj_fund["dict"]["date_adj_port"] + ".csv"
            obj_fund["df_fund_stock_port"].to_csv(path_export +file_name ,encoding="gbk")
        
        ########################################################################
        ### Save df_fund ；限定特定columns
        # col_list = obj_fund["col_list_export_df_fund"]
        # df_fund = obj_fund["df_fund"].loc[:,col_list] 
        file_name = "df_fund_" + obj_fund["dict"]["date_adj_port"] + ".csv"
        obj_fund["df_fund"].to_csv(path_export +file_name ,encoding="gbk")

        ########################################################################
        ### Save df_stockpool_fund 
        if "df_stockpool_fund" in obj_fund.keys() :
            file_name = "df_stockpool_fund_" + obj_fund["dict"]["date_adj_port"] + ".csv"
            obj_fund["df_stockpool_fund"].to_csv(path_export +file_name ,encoding="gbk")
        
        ########################################################################
        ### Save df_fund_ind_weight,df_fund_ind_ret
        if "df_fund_ind_weight" in obj_fund.keys() :
            file_name = "df_fund_ind_weight_" + obj_fund["dict"]["date_adj_port"] + ".csv"
            obj_fund["df_fund_ind_weight"].to_csv(path_export +file_name ,encoding="gbk")
        
        # 还有 df_fund_ind_weigt_pre_1q，df_fund_ind_weigt_pre_1y,df_fund_ind_weigt_pre_2y

        ########################################################################
        ### Save df_fund_compan；按基金公司统计
        col_list_sum = ["F_INFO_CORP_FUNDMANAGEMENTCOMP","F_PRT_NETASSET","F_PRT_STOCKVALUE","F_PRT_GOVCASHTONAV","F_PRT_BDTONAV_NOGOV"]
        ### 判断是否是6,12月，有无对应column
        if  "F_TRADE_STOCKAM" in obj_fund["df_fund"].columns :
            col_list_sum = col_list_sum + ["F_TRADE_STOCKAM","F_COMMISSIONAM"]
        
        df_fund_company = obj_fund["df_fund"].loc[:, col_list_sum ].groupby(["F_INFO_CORP_FUNDMANAGEMENTCOMP"]).sum()
        file_name = "df_fund_company_" + obj_fund["dict"]["date_adj_port"] + ".csv"
        df_fund_company.to_csv(path_export +file_name ,encoding="gbk")

        ########################################################################
        ### Save obj_fund_ana["dict"]
        file_name = "obj_fund_ana_" + obj_fund["dict"]["date_adj_port"] + ".json"
        with open( path_export+ file_name,"w+") as f :
            json.dump( str(obj_fund_ana["dict"] ) ,f  )
        print("Dict data saved in Json file \n",path_export+ file_name )


        return obj_fund


#######################################################################
class data_io_fund_simulation():
    def __init__(self):        
        self.obj_config = config_data_fund_ana_1.obj_config
        self.data_io_1 = data_io()
        #######################################################################
        ### 转换后的基金wds数据表
        path_wds_fund = self.obj_config["dict"]["path_wds_fund"]
        
        ### 基金分析数据输出目录
        path_ciss_db_fund = self.obj_config["dict"]["path_ciss_db_fund"]
        ### simulation 数据输出目录
        self.path_fund_simu = "D:\\CISS_db\\fund_simulation\\output\\"
        
        self.path_fund_simu = "D:\\CISS_db\\fund_simulation\\output_50f\\"
        
    def print_info(self):        
        print("data_io_fund_simulation  ")
        ### 导入import：读取json和excel文件
        print("import_fund_simulation |导入基金拟合计算过程的对象字典和df数据表 ")    

        ### 输出Export：保存到json和excel文件
        print("export_fund_simulation |导出基金拟合计算过程的对象字典和df数据表 ")
        print("export_fund_skill |导出基金拟合计算过程的skill文件  ")
        print("import_fund_skill |导入基金拟合计算过程的skill文件  ")

        return 1

    def export_fund_simulation(self,obj_fund_ana,obj_port):
        ########################################################################
        ### 导出基金拟合计算过程的对象    
        if "output_type" in obj_port["dict"].keys() and obj_port["dict"]["output_type"] == "next" :
            ###             
            output_type = "next_"
        else :
            output_type = ""

        temp_date = obj_fund_ana["dict"]["temp_date"]
        ######################################################################## 
        ### 判断输出目录是否存在 || "D:\\CISS_db\\fund_simulation\\output\\"
        if not os.path.exists( self.path_fund_simu  ) :
            os.mkdir( self.path_fund_simu  )
        print("path_fund simulation : ",self.path_fund_simu  )

        ########################################################################
        ### 保存dict字典变量:obj_port["dict"]
        import json 
        file_name = "obj_port_" +output_type+ temp_date  + ".json"

        with open(self.path_fund_simu + file_name,"w+") as f :
            json.dump( str(obj_port["dict"]) ,f  )
        print("Dict data saved in Json file \n",self.path_fund_simu + file_name )
        
        ########################################################################        
        ### 保存dict字典变量:obj_fund_ana["dict"] 
        file_name = "obj_fund_ana_" +output_type + temp_date  + ".json"

        with open(self.path_fund_simu + file_name,"w+") as f :
            json.dump( str(obj_fund_ana["dict"]) ,f  )
        print("Dict data saved in Json file \n",self.path_fund_simu + file_name ) 

        ########################################################################        
        ### 所有组合个股基本信息和 权重
        obj_port["df_ashare_ana"].to_excel(self.path_fund_simu + "df_ashare_ana_short_"+output_type+ temp_date +".xlsx") 

        ########################################################################        
        ### 保存所有组合业绩指标
        obj_port["df_perf_eval"].to_excel(self.path_fund_simu + "df_perf_eval_"+output_type+ temp_date +".xlsx") 
        ###  
        obj_port["df_basic_port_unit"].to_excel(self.path_fund_simu + "df_basic_port_unit_"+output_type+ temp_date +".xlsx") 
        obj_port["df_basic_perf_eval"].to_excel(self.path_fund_simu + "df_basic_perf_eval_"+output_type+ temp_date +".xlsx") 

        
        ########################################################################        
        ### 保存所有组合净值和业绩指标
        obj_port["df_port_unit"].to_excel(self.path_fund_simu + "df_port_fund_all_perf_eval_"+output_type+ temp_date +".xlsx") 
        
        ########################################################################        
        ### 保存拟合组合的权重
        ### ??? 不知道为什么 obj_port["df_port_simu"] 在最后边会变没
        
        ########################################################################        
        ### 保存skill 文件
        if "df_skill" in obj_port.keys(): 
            obj_port["df_skill"].to_excel(self.path_fund_simu + "df_skill_"+output_type+ temp_date +".xlsx") 
        if "df_skill_next" in obj_port.keys(): 
            obj_port["df_skill_next"].to_excel(self.path_fund_simu + "df_skill_next_"+output_type+ temp_date +".xlsx") 
        if "df_skill_next_ave" in obj_port.keys(): 
            obj_port["df_skill_next_ave"].to_excel(self.path_fund_simu + "df_skill_next_ave_"+output_type+ temp_date +".xlsx") 

        return 1


    def export_fund_skill(self,obj_fund_ana,obj_port):
        ########################################################################
        ### 导出基金拟合计算过程的skill文件  
        ### 只保存skill可以避免数据覆盖
        temp_date = obj_fund_ana["dict"]["temp_date"]  
        ### 保存skill 文件
        if "df_skill" in obj_port.keys(): 
            obj_port["df_skill"].to_excel(self.path_fund_simu + "df_skill_"+ temp_date +".xlsx") 
        if "df_skill_next" in obj_port.keys(): 
            obj_port["df_skill_next"].to_excel(self.path_fund_simu + "df_skill_next_"+ temp_date +".xlsx") 
        if "df_skill_next_ave" in obj_port.keys(): 
            obj_port["df_skill_next_ave"].to_excel(self.path_fund_simu + "df_skill_next_ave_"+ temp_date +".xlsx") 

        return 1

    def import_fund_skill(self,temp_date):
        ########################################################################
        ### 读取基金拟合计算过程的skill文件  
        ### 只保存skill可以避免数据覆盖
        # temp_date = obj_fund_ana["dict"]["temp_date"]  
        ### skill 文件 || df_skill_next_20060801.xlsx    
        temp_date= str( temp_date )
        obj_port={}
        obj_port["df_skill"]=pd.read_excel(self.path_fund_simu + "df_skill_"+ temp_date +".xlsx",index_col=0 ) 
        obj_port["df_skill_next"]=pd.read_excel(self.path_fund_simu + "df_skill_next_next_"+ temp_date +".xlsx",index_col=0 ) 
        obj_port["df_skill_next_ave"]=pd.read_excel(self.path_fund_simu + "df_skill_next_ave_next_"+ temp_date +".xlsx",index_col=0 ) 
        
        ### 导入下一季度净值和绩效文件： df_port_fund_all_perf_eval_next_20091102.xlsx
        obj_port["df_all_perf_eval_next"]=pd.read_excel(self.path_fund_simu + "df_port_fund_all_perf_eval_next_"+ temp_date +".xlsx",index_col=0 ) 
        ### 导入当季度净值和绩效文件  |  df_port_fund_all_perf_eval_20060801.xlsx
        obj_port["df_all_perf_eval"]=pd.read_excel(self.path_fund_simu + "df_port_fund_all_perf_eval_"+ temp_date +".xlsx",index_col=0 ) 

        ### 导入当季度基金列表 fund_list_short from obj_fund_ana_20060801.json
        file_name = "obj_fund_ana_" + str( temp_date) + ".json"
        
        with open( self.path_fund_simu + file_name) as obj_dict :
            obj_port["dict"] = eval( json.load( obj_dict ) )
        
        # obj_port["dict"]["fund_list_short"]  
        
        return obj_port 

    def import_fund_simulation(self,temp_date):
        ########################################################################
        ### 导入基金拟合计算过程的对象     
        
        ########################################################################
        ### 导入dict字典变量:obj_port["dict"]
        import json 
        obj_port = {}
        obj_port["dict"] ={}
        file_name = "obj_port_" + str( temp_date) + ".json"

        with open( self.path_fund_simu + file_name) as obj_dict :
            obj_port["dict"] = eval( json.load( obj_dict ) )
        
        print("Load dict of obj_port \n", obj_port["dict"].keys() )        
        
        ########################################################################        
        ### 导入dict字典变量:obj_fund_ana["dict"]
        obj_fund_ana = {}
        obj_fund_ana["dict"] ={}
        file_name = "obj_fund_ana_" + str( temp_date) + ".json"
        
        with open( self.path_fund_simu + file_name) as obj_dict :
            obj_fund_ana["dict"] = eval( json.load( obj_dict ) )
        
        print("Load dict of obj_fund_ana \n", obj_fund_ana["dict"].keys() )        

        ########################################################################        
        ### 导入时，需要将第一列设置为index | index_col=0 
        ### 所有组合个股基本信息和 权重
        obj_port["df_ashare_ana"] = pd.read_excel(self.path_fund_simu + "df_ashare_ana_short_"+ str( temp_date) +".xlsx",index_col=0 ) 
        
        ### notes:可能存在错误的column名称：S_INFO_WINDCODE.1
        if "S_INFO_WINDCODE.1" in obj_port["df_ashare_ana"].columns: 
            obj_port["df_ashare_ana"] = obj_port["df_ashare_ana"].rename( columns={ "S_INFO_WINDCODE.1": "S_INFO_WINDCODE" }  ) 

        ########################################################################        
        ### 导入所有组合业绩指标
        obj_port["df_perf_eval"] = pd.read_excel(self.path_fund_simu + "df_perf_eval_"+ str( temp_date) +".xlsx",index_col=0 )  
        ### 导入仅有基础组合业绩指标 || df_basic_perf_eval_20180201.xlsx
        obj_port["df_basic_perf_eval"] = pd.read_excel(self.path_fund_simu + "df_basic_perf_eval_"+ str( temp_date) +".xlsx",index_col=0 ) 
        obj_port["df_basic_port_unit"] = pd.read_excel(self.path_fund_simu + "df_basic_port_unit_"+ str( temp_date) +".xlsx") 
        
        ########################################################################        
        ### 导入所有组合净值和业绩指标
        obj_port["df_port_unit"] = pd.read_excel(self.path_fund_simu + "df_port_fund_all_perf_eval_"+ str( temp_date) +".xlsx",index_col=0 ) 

        ### 导入基础组合净值和业绩指标
        obj_port["df_basic_port_unit"] = pd.read_excel(self.path_fund_simu + "df_basic_port_unit_"+ str( temp_date) +".xlsx",index_col=0 ) 

        return obj_port,obj_fund_ana






