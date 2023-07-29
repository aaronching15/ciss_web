# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 

功能：用WindPy模块获取API数据,基金相关指标和数据;
    1,class wind_api : 获取wsq,wss,wset,wsd等数据
    2,class wind_api_pms :获取wpf等数据

数据来源： Wind-API 万得量化数据接口
last update  | since 230420
derived from  get_wind_api.py
===============================================
'''
from calendar import c
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )
sys.path.append(path_ciss_rc + "db\\fund_analysis\\" )

import pandas as pd
import numpy as np
import math
import time 

class wind_api_fund():
    ### 获取Wind的PMS相关数据:wpf,wps,wpd,wupf
    def __init__(self):
        ### 获取wpf相关数据
        ##########################################
        ### 导入配置文件对象，例如path_db_wind等
        from config_data import config_data 
        self.obj_config = config_data().obj_config

        self.nan = np.nan 
        self.path_ciss_web = os.getcwd().split("ciss_web")[0]+"ciss_web\\"
        self.path_ciss_rc = self.path_ciss_web +"CISS_rc\\"
        self.path_dt = self.path_ciss_rc + "db\\db_times\\"
        ### 
        self.path_pms =  os.getcwd().split("ciss_web")[0]+"\\data_pms\\"  
        self.path_wpf = self.path_pms + "wpf\\"
        self.path_wpd = self.path_pms + "wpd\\"
        self.path_wsd = self.path_pms + "wsd\\"

        ##########################################
        ### 时间相关变量
        import datetime as dt  
        self.time_now = dt.datetime.now()
        self.time_pre =  self.time_now - dt.timedelta(days=1) , "%Y%m%d" 
        self.time_pre10 =  self.time_now - dt.timedelta(days=10) , "%Y%m%d" 
        ### 获取近1w、1m、3m、6m、1year日期
        self.time_now_str = dt.datetime.strftime(self.time_now   , "%Y%m%d")
        self.time_now_str2 = dt.datetime.strftime(self.time_now   , "%Y-%m-%d")
        ###
        self.time_pre_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=1) , "%Y%m%d")
        self.time_pre10_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=10) , "%Y%m%d")
        ### 导入日期数据  获取日期参数：如最近月末、季度末、半年度末数据
        from times import times
        times1 = times()
        self.obj_date = times1.get_date_pre_post( self.time_now_str ) 
        ### output: pre 给定日期之前的最近月末和2个季末、2个半年末；str和dt格式
        # obj_date["date_pre_1m_end"] obj_date["date_pre_1m_end_str"]
        # obj_date["date_pre_1q_end_str"] obj_date["date_pre_1q_end"] 
        # obj_date["date_pre_2q_end_str"] obj_date["date_pre_2q_end"] 
        # obj_date["date_pre_1halfyear_end_str"] obj_date["date_pre_1halfyear_end"]
        # obj_date["date_pre_2halfyear_end_str"] obj_date["date_pre_2halfyear_end"]
        

        ##########################################
        ### 每次调用API，要保存数据提取数量，保存到外部文件 
        self.api_count ={} 

    def print_info(self):
        ###################################################
        ### quote and indicators 
        ###################################################
        ### WSS 多维数据 | 单一时点，多代码多指标  
        print("get_wss_fund_date | 给定1个基金代码及日期，获取多个不同基金指标 ") 
        print("---------------------------------------------------------------------- ")  

        ###################################################
        ### fund performance | 基金相关
        print("---------------------------------------------------------------------- ")
        print("get_wss_fund_perf | 给定基金代码、区间、获取基金和基金经理绩效指标 ")
        print("get_wsd_fund_unit | 给定基金代码、区间、获取基金净值和收益率 ") 
        print("---------------------------------------------------------------------- ")
        
        ###################################################
        ### Index benchmark performance | 指数，基准相关
        print("get_index_indi_data |给定基金基准指数代码、日期，获取月度收益率") 


        ###################################################
        ### time  
        print("get_tdays | 获取日期 ")
        ###################################################

        print("save_api_count | 保存api_count 指标到excel ")
    
    ######################################################################################################
    def save_api_count(self):
        ### 保存api_count 指标到excel 

        ##########################################
        ### 每次调用API，要保存数据提取数量，保存到外部文件 
        file_name = "api_count.xlsx"
        sheet = "api_count" 
        df_api = pd.read_excel(self.path_pms + file_name,sheet_name= sheet ) 
        ### columns date wds	wss	wset；wps；wpd
        df_api = df_api.drop_duplicates(subset=["date"], keep="last"  )
        df_api.index = df_api["date"] 

        ##########################################
        ###
        for temp_key in self.api_count.keys() :
            if self.time_now_str in df_api.index :
                ### 判断 "wsd" 是否在columns里
                if temp_key in df_api.columns :
                    df_api.loc[self.time_now_str,temp_key] = df_api.loc[self.time_now_str,temp_key] + self.api_count[temp_key]
                else :
                    df_api[temp_key] = 0 
                    df_api.loc[self.time_now_str,temp_key] = df_api.loc[self.time_now_str,temp_key] + self.api_count[temp_key]
                
            else :
                if temp_key in df_api.columns :
                    ### define new index  
                    df_api.loc[self.time_now_str, : ] = 0 
                    df_api.loc[self.time_now_str,temp_key] = self.api_count[temp_key]
                else :
                    ### define new index and new column
                    df_api.loc[self.time_now_str, : ] = 0 
                    df_api[temp_key] = 0 
                    df_api.loc[self.time_now_str,temp_key] =  self.api_count[temp_key]
        
        ### save date 
        df_api.loc[self.time_now_str, "date"] = self.time_now_str

        df_api.to_excel( self.path_pms + "api_count.xlsx",sheet_name="api_count" ,index=False)


        return 1
    

    def get_wss_fund_date(self, obj_f) :
        ### 给定1个基金代码及日期，获取多个不同基金指标 
        str_para =""
        ##########################################
        ### 基金代码
        if "fund_code" in obj_f.keys():
            fund_code = obj_f["fund_code"]
        elif "list_code" in obj_f.keys():
            list_code = obj_f["list_code"]

        ##########################################
        ### 日期
        if "date" in obj_f.keys():
            ### 单一日期
            date = obj_f["date"]
            ### 如果obj_f没有str_para，就用这个
            str_para = "tradeDate="+date
        elif "date_start" in obj_f.keys():
            ### 开始和结束日期
            date_start = obj_f["date_start"]
            date_end   = obj_f["date_end"]
            ### 如果obj_f没有str_para，就用这个
            str_para = "startDate="+ date_start +";endDate=" + date_end

        ##########################################
        ### 指标列表
        col_list = obj_f["col_list"] 
        str_col = ""
        for temp_col in col_list :
            str_col = str_col + temp_col +","
        str_col = str_col[:-1]
        
        ##########################################
        ### 参数设置 
        if "str_para" in obj_f.keys(): 
            str_para = obj_f["str_para"]  
        ##########################################
        ###        
        count_col = 0 
        from WindPy import w
        w.start()

        if "fund_code" in obj_f.keys():
            print("w.wss ", fund_code , str_col , str_para  )
            obj_w = w.wss( fund_code , str_col , str_para )
            ### 判断是否报错 
            ### obj_w .ErrorCode=0  .Codes=[110011.OF] .Fields=[FUND_FUNDMANAGEROFTRADEDATE]   .Times=[20220902 17:31:20]   
            # .Data=[[张坤]]
            if obj_w.ErrorCode == 0 :
                ### 合并数据，创建 df 
                obj_f["list_data"] = obj_w.Data[0]
                print("Debug=== ", obj_w.Data )
                df_fund = pd.DataFrame(obj_w.Data )
                df_fund.columns=  [fund_code ]
                df_fund.index= col_list
                df_fund =df_fund.T
                df_fund["code"] = fund_code 
                obj_f["df_fund"] = df_fund 
                
                print("df_fund1, \n", df_fund.T  )
                ##########################################
                ### save to excel
                file_name = "fund_" + fund_code +".xlsx"
                obj_f["df_fund"].to_excel("D:\\"+ file_name )
                
                ### 指标用量
                count_col = count_col + len( col_list  )
            else :
                print( "Error 1 \n", obj_w ,"\n",fund_code , str_col , str_para )

        elif "list_code" in obj_f.keys():
            count_code = 0 
            list_code = obj_f["list_code"] 
            for fund_code in list_code : 
                print("w.wss ", fund_code , str_col , str_para  )
                obj_w = w.wss( fund_code , str_col , str_para ) 
                ### 判断是否报错
                if obj_w.ErrorCode == 0 :
                    ### 合并数据
                    obj_f["list_data"] = obj_w.Data[0]
                    ### 添加到df 
                    if count_code == 0 :
                        df_fund = pd.DataFrame(obj_w.Data )
                        df_fund.columns= [fund_code ] 
                        df_fund.index= col_list 
                        df_fund =df_fund.T
                        df_fund["code"] = fund_code 
                        obj_f["df_fund"] = df_fund  
                        # print("df_fund2, \n", df_fund.T  )
                        
                    else :
                        df1 = pd.DataFrame(obj_w.Data )
                        df1.columns=  [fund_code ]
                        df1.index= col_list 
                        df1 =df1.T
                        df1["code"] = fund_code 
                        ### 
                        df_fund = df_fund.append( df1 )

                        obj_f["df_fund"] = df_fund
                        # print("df_fund2, \n", df_fund.T  )
                        ##########################################
                        ### save to excel
                        file_name = "fund_" + date +".xlsx"
                        obj_f["df_fund"].to_excel("D:\\"+ file_name )
                    ###
                    count_code = count_code +1
                    ### 指标用量
                    count_col = count_col + len( col_list  )
                else :
                    print( "Error 2 \n", obj_w,"\n",fund_code , str_col , str_para )
                    asd 
                
                ### 
                time.sleep(0.3)
            

        ##########################################
        ### save to excel
        if "excel_file" in obj_f.keys():
            if "excel_sheet" in obj_f.keys():
                obj_f["df_fund"].to_excel( obj_f["excel_path"] +obj_f["excel_file"], sheet_name= obj_f["excel_sheet"] )
            else :
                obj_f["df_fund"].to_excel( obj_f["excel_path"] +obj_f["excel_file"] )

        

        ##########################################
        ### save to api count
        self.api_count["wss"] = count_col  
        result = self.save_api_count()   
                

        return obj_f


    ######################################################################################################
    def get_wss_fund_perf(self,obj_f):
        ###########################################################################
        ### 给定基金代码、区间、获取基金和基金经理绩效指标
        ### notes：wss一次提取多个指标时，所有指标对应的时间区间必须一样
        ### INPUT
        temp_fund_code = obj_f[ "fund_code"] 
        ### 偏股混合型基金指数 885001.WI ; benchmark_code = obj_f[ "benchmark_code"] 
        ### 获取区间开始和结束日期
        date_begin = obj_f[ "date_begin"] 
        date_end = obj_f[ "date_end"]  
        #################################################### 
        # w.wss("720001.OF", "NAV_adj_return,return","startDate=20220114;endDate=20220214;annualized=0;tradeDate=20220213;fundType=1") 
        ### 今年以来回报排名periodreturnranking ytd，近1月回报排名periodreturnranking_1m ;近3月回报排名periodreturnranking_3m ;近6月回报排名periodreturnranking_6m
        # w.wss("720001.OF", "periodreturnranking_ytd,periodreturnranking_1m,periodreturnranking_3m,periodreturnranking_6m","tradeDate=20220213;fundType=1")
        ### 最大回撤risk_maxdownside 基金经理（现任）fund_fundmanager；任职日期fund_manager_startdate ; 任职基金总规模fund_manager totalnetasset ;
        # 现任基金最佳回报fund manager bestperformance；任期最大回撤|fund manager maxdrawdown ; 
        # w.wss("720001.OF", "risk_maxdownside,fund_fundmanager,fund_manager_startdate,fund_manager_totalnetasset,fund_manager_bestperformance,fund_manager_maxdrawdown,fund_managerindex_return","startDate=20211214;endDate=20220214;order=1;unit=1;topN=1;index=1")
        # 720001.0F  7.058352金样才,2014/11/19,6978687519.42,41064466462,-54.064% ,1.542803
        # 基金经理指数区间回报（算术平均）fund_managerindex_return：1.542803 表示 720001.OF 从 20211214-to-20220214 收益率 1.54% 
        count_col = 0 
        ### 
        from WindPy import w
        w.start()
        #################################################### 
        ### 区间类涨跌幅（没有排名）|| 这2个指标的收益率数据是一样的：区间复权单位净值增长率NAV_adj_return ;区间回报return ;
        # 区间\复权单位净值增长率NAV_adj_return；区间\最大回撤
        col_list_perf = ["NAV_adj_return","risk_maxdownside"]
        col_list_str_perf = "NAV_adj_return,risk_maxdownside"
        count_col = count_col + 2 
        # "720001.OF", "NAV_adj_return,return","startDate=20220114;endDate=20220214;annualized=0;tradeDate=20220213;fundType=1"
                
        #################################################### 
        ### 非区间类涨跌幅及排名 
        # return_ytd,今年以来回报； return_1m，近1个月回报
        col_list_perf = col_list_perf +  ["return_ytd","return_1m","return_3m","return_6m"]
        col_list_str_perf = col_list_str_perf + "," + "return_ytd,return_1m,return_3m,return_6m"
        # 今年以来回报排名,periodreturnranking_ytd; 近1月回报排名periodreturnranking_1m ;近3月回报排名periodreturnranking_3m ;近6月回报排名periodreturnranking_6m
        col_list_perf = col_list_perf + ["periodreturnranking_ytd","periodreturnranking_1m","periodreturnranking_3m","periodreturnranking_6m"]
        col_list_str_perf = col_list_str_perf +"," + "periodreturnranking_ytd,periodreturnranking_1m,periodreturnranking_3m,periodreturnranking_6m"
        count_col = count_col + 8

        para_str = "startDate=" + date_begin + ";endDate=" + date_end +";annualized=0;tradeDate=" + date_end + ";fundType=1"
        print("Debug=", temp_fund_code, col_list_str_perf, para_str )
        obj_w = w.wss(temp_fund_code, col_list_str_perf, para_str )           
        ### notes:返回的指标都是大写！ 
        # NAV_ADJ_RETURN,RISK_MAXDOWNSIDE,RETURN_YTD,RETURN_1M,RETURN_3M,RETURN_6M,PERIODRETURNRANKING_YTD,PERIODRETURNRANKING_1M,PERIODRETURNRANKING_3M,PERIODRETURNRANKING_6M]
        ### 
        print("obj_w \n", obj_w)
        if obj_w.ErrorCode == 0 :
            ### notes: 这里index不能用 obj_w.Fields
            df1=pd.DataFrame(obj_w.Data, index= col_list_perf )

            #################################################### 
            ### 基金简称；基金经理（现任）fund_fundmanager；任职日期fund_manager_startdate ;几何平均年化回报率,fund_manager_geometricannualizedyield 
            # 任职基金总规模fund_manager_totalnetasset ;；任期最大回撤|fund manager maxdrawdown ; #这个不要：基金经理指数区间回报（算术平均）fund_managerindex_return
            # notes：有了任职日期和几何年化回报，就可以计算出任职以来的累计回报率。 
            col_list_perf_2 = ["name_official","fund_fundmanager","fund_manager_startdate","fund_manager_geometricannualizedyield"]
            col_list_perf_2 = col_list_perf_2 +  ["fund_manager_totalnetasset","fund_manager_maxdrawdown"]
            col_list_str_perf_2 = "name_official,fund_fundmanager,fund_manager_startdate,fund_manager_geometricannualizedyield"
            col_list_str_perf_2 = col_list_str_perf_2 + "," +"fund_manager_totalnetasset,fund_manager_maxdrawdown"
            count_col = count_col + 6
            para_str = "order=1;returnType=1" 
            ### 
            print("Debug=", temp_fund_code, col_list_str_perf_2 , para_str )
            obj_w = w.wss(temp_fund_code, col_list_str_perf_2 , para_str )  
            print("obj_w \n", obj_w)
            if obj_w.ErrorCode == 0 :
                ### notes: 这里index不能用 obj_w.Fields
                df2=pd.DataFrame(obj_w.Data, index= col_list_perf_2)
                #################################################### 
                ### 部分数据转换： 日期 
                # notes :"fund_manager_startdate" 返回的原始日期类型是list, [datetime.datetime(2014, 11, 19, 0, 0)]
                # 转化成df后的类型是Timestamp，数值如  dt.datetime.strftime
                import datetime as dt 
                df2.loc[ "fund_manager_startdate",0] = dt.datetime.strftime(  df2.loc[ "fund_manager_startdate",0] , "%Y%m%d")
                ### 亿元转换
                df2.loc[ "fund_manager_totalnetasset",0] = df2.loc[ "fund_manager_totalnetasset",0]/100000000
                
                ### notes: index 是指标，不能ignore
                df2= df2.append(df1 , ignore_index=False)

                ### transpose
                obj_f["df_perf"] = df2.T
            else :
                ### transpose
                obj_f["df_perf"] = df1.T
        
        #################################################### 
        ### 简历,fund_manager_resume
        if obj_f[ "if_jjjl_resume"] in [1,"1" ] :
            para_str = "order=1;returnType=1" 
            count_col = count_col + 5
            obj_w = w.wss(temp_fund_code, "fund_manager_resume", para_str )  
            obj_f["fund_manager_resume"] = obj_w.Data[0][0]
        
        ### notes:返回的指标都是大写！ 
        # NAV_ADJ_RETURN,RISK_MAXDOWNSIDE,RETURN_YTD,RETURN_1M,RETURN_3M,RETURN_6M,PERIODRETURNRANKING_YTD,PERIODRETURNRANKING_1M,PERIODRETURNRANKING_3M,PERIODRETURNRANKING_6M]
        # NAME_OFFICIAL,FUND_FUNDMANAGER,FUND_MANAGER_STARTDATE,FUND_MANAGER_GEOMETRICANNUALIZEDYIELD,FUND_MANAGER_TOTALNETASSET,FUND_MANAGER_MAXDRAWDOWN

        #################################################### 
        

        #################################################### 
        ### 计算wind-api用量,并保存到excel
        # count_col  
        count_port = 1 
        self.api_count["wss"] =  count_col * count_port 
        result = self.save_api_count() 

        return obj_f

    
    def get_wsd_fund_unit(self,obj_f):
        ###########################################################################
        ###  给定基金代码、区间、获取基金净值
        ### notes:默认按交易日下载，若选择按月下载，则每个月所有基金保存在一个文件里。
        ### INPUT
        temp_fund_code = obj_f[ "fund_code"] 
        ### 偏股混合型基金指数 885001.WI
        # benchmark_code = obj_f[ "benchmark_code"] 
        ### 获取区间开始和结束日期
        date_begin = obj_f[ "date_begin"] 
        date_end = obj_f[ "date_end"]  
        #################################################### 
        ### 日收益率：w.wsd("720001.OF", "NAV_adj", "2022-01-01", "2022-02-13", "PriceAdj=F")
        ### 周收益率：w.wsd("720001.OF", "NAV_adj", "2022-01-01", "2022-02-13", "Period=W;PriceAdj=F")
        count_col = 0 
        ### 除了给定交易日，其他都按照week 周度频率提取数据
        if obj_f["unit_type"] == "day" :
            # "PriceAdj=F" 
            para_end = "Period=D;PriceAdj=F"
        # elif obj_f["unit_type"] == "week" :
        elif obj_f["unit_type"] in ["m","M","month"] :
            # "PriceAdj=F" 
            para_end = "Period=M;PriceAdj=F"
        else : 
            ### "unit_type" =="week"
            para_end = "Period=W;PriceAdj=F"
        
        #################################################### 
        from WindPy import w
        w.start()
        #################################################### 
        ### 提取的指标，默认只有 "NAV_adj"
        if "col_list_wind_api" in obj_f.keys() :
            col_list_wind_api = obj_f["col_list_wind_api"]
            ### col_list_wind_api 应该类似于 "NAV_adj,return_1m"
            ### notes:若指标不只是NAV_adj，有可能需要设置其他参数，否则会报错。如return_1m 指标需要设置参数 annualized=0 ,用于判断是否换算年化收益率
            obj_w = w.wsd( temp_fund_code , col_list_wind_api, date_begin, date_end, para_end )
        else :
            col_list_wind_api = "NAV_adj" 
            obj_w = w.wsd( temp_fund_code , "NAV_adj", date_begin, date_end, para_end )

        #################################################### 
        ### Debug判断返回数据是否出错
        if obj_w.ErrorCode == 0 :
            ### notes: 这里index不能用 obj_w.Fields
            df1 = pd.DataFrame(obj_w.Data, index= ["NAV_adj"] ,columns=obj_w.Times )
            df1 =df1.T
            df1["unit_fund"] = df1["NAV_adj"]/ df1["NAV_adj"].values[0]
            df_unit= df1
            ##########################################
            ### 计算涨跌幅 | 需要df里时间序列数据升序排列
            df_unit["pct_chg_fund"] = df_unit["unit_fund"].pct_change(1)
            df_unit["pct_chg"] = df_unit["pct_chg_fund"]

            #################################################### 
            ### 计算wind-api用量,并保存到excel
            # count_col = 1 
            count_col = len( col_list_wind_api.split(",") ) 
            ### 计算交易日数量
            obj_date= w.tdayscount( date_begin, date_end, "")
            count_date = obj_date.Data[0][0] 
            self.api_count["wsd"] =  count_col * count_date 
            result = self.save_api_count()  
                
        else : 
            print("Error,wind_api: check input var: wsd()", temp_fund_code , "NAV_adj", date_begin, date_end, para_end )
            print( obj_w )
        
        ##############################################################################
        ### 下载基准指数
        print("keys=" , obj_f.keys()  )
        if "benchmark_code" in obj_f.keys() or "bench_code" in obj_f.keys() :
            ### 获取基准地区间净值or收益率
            ### 偏股混合型基金指数 885001.WI
            if "benchmark_code" in obj_f.keys()  :
                benchmark_code = obj_f[ "benchmark_code"] 
            else :
                benchmark_code = obj_f[ "bench_code"] 
            
            #################################################### 
            ### 可能地代码类型：885001.WI、 000300.SH、159915.SZ、000333.SZ、519979.OF
            # 收盘价：w.wsd("885001.WI", "close", "2022-01-01", "2022-02-13", "Period=W;PriceAdj=F")
            # 涨跌幅：w.wsd("885001.WI", "pct_chg", "2022-01-01", "2022-02-13", "Period=W;PriceAdj=F")
            ### notes:涨跌幅pct_chg、close和定点复权"close2",适用于股票和指数"000300.SH,000333.SZ,159915.SZ； 不适用于 基金 519979.OF
            # w.wsd("000300.SH,000333.SZ,159915.SZ", "close2", "2021-12-31", "2022-02-13", "adjDate=20220214;Period=W;PriceAdj=F")
            if benchmark_code[-3:] == ".OF" :
                obj_w = w.wsd( benchmark_code , "NAV_adj", date_begin, date_end, para_end )
                if obj_w.ErrorCode == 0 :
                    ### notes: 这里index不能用 obj_w.Fields
                    df2 = pd.DataFrame(obj_w.Data, index= ["NAV_adj"] ,columns=obj_w.Times )
                    df2 = df2.T
                    df2["unit_bench"] = df2["NAV_adj"]/ df2["NAV_adj"].values[0]
                    df_unit= df_unit.append( df2.loc[:, "NAV_adj" ] )
            else :
                ### 涨跌幅pct_chg、close和定点复权"close2" 都可以;但是 885001.WI 不能用 "close2"
                temp_col = "close"
                obj_w = w.wsd( benchmark_code , temp_col, date_begin, date_end, para_end )
                if obj_w.ErrorCode == 0 : 
                    ### notes: 这里index不能用 obj_w.Fields
                    df2 = pd.DataFrame(obj_w.Data, index= [temp_col] ,columns=obj_w.Times )
                    df2 = df2.T
                    df2["unit_bench"] = df2[ temp_col ]/ df2[ temp_col].values[0]
                    df_unit["unit_bench"] = df2["unit_bench"]

            ##########################################
            ### 计算涨跌幅 | 需要df里时间序列数据升序排列
            df_unit["pct_chg_bench"] = df_unit["unit_bench"].pct_change(1) 

            

            print("Debug== df-unit \n", df_unit )
            #################################################### 
            ### 计算wind-api用量,并保存到excel
            count_col = 1 
            self.api_count["wps"] = count_col * count_date 
            result = self.save_api_count()
        

        #################################################### 
        ### output: 
        df_unit["date"] = df_unit.index
        ### 判断返回数据是否出错 if obj_w.ErrorCode == 0
        obj_f["ErrorCode"] = obj_w.ErrorCode

        obj_f["df_unit"] = df_unit

        return obj_f
    

    def get_index_indi_data (self,obj_index):
        ###########################################################################
        ###  给定基金基准指数代码、日期，获取月度收益率 
        ### INPUT
        date_start = obj_index["date_start"] 
        date_end = obj_index["date_end"]  
        
        ### 导入配置文件中的指数列表 
        temp_file = "fund_indi_manage.xlsx"
        path_file = self.obj_config["dict"]["path_data_pms"]
        df_benchmark = pd.read_excel( path_file + temp_file ,sheet_name="benchmark")
        df_benchmark = df_benchmark[ df_benchmark["if_use"] ==1 ] 
        list_index_code = list( df_benchmark["code"] )
        
        from WindPy import w
        w.start() 
        ###  
        # w.wss("CBA02501.CS,000832.CSI", "pct_chg_per","startDate=20230228;endDate=20230331")
        str_para = "startDate="+ str(date_start) +";endDate="+ str(date_end ) 
        obj1 = w.wss(list_index_code,"pct_chg_per", str_para )    
        time.sleep(0.3)

        ### 判断是否报错 
        if obj1.ErrorCode == 0 :
            ### 合并数据
            code_list_all = obj1.Codes
            data_list = obj1.Data[0]  
            ### 这时index是指数代码，需要转置
            obj_index["df_index"] = pd.DataFrame( obj1.Data[0] ,index=obj1.Codes ,columns=[ str(date_end)] )  
            obj_index["df_index"] =obj_index["df_index"].T
            ### save date as column
            obj_index["df_index"]["date"] = str(date_end)

        else :
            # 报错的情况
            print("Error:obj1 \n ", obj1 )
            print("code_list" ,list_index_code )  
            print("Debug=================== \n", obj1 )


        #################################################### 
        ### 计算wind-api用量,并保存到excel 
        count_col = len( list_index_code ) 
        ### 计算交易日数量 obj_date= w.tdayscount( date_begin, date_end, "") || count_date = obj_date.Data[0][0] 
        self.api_count["wss"] =  count_col 
        result = self.save_api_count()   

        return obj_index 


    ######################################################################################################
    ### tdays
    def get_tdays(self,obj_dt  ):
        ### 获取日期
        
        from WindPy import w
        w.start()
        # obj_w = w.tdays("2019-07-17", "2019-08-14", "")
        
        obj_w = w.tdays( obj_dt["date_begin"], obj_dt["date_end"], "")
        
        '''obj_w
        .ErrorCode=0
        .Codes=[]
        .Fields=[]
        .Times=[20220905,20220906,20220907,20220908,20220909,20220913,20220914,20220915,20220916,20220919,...]
        .Data=[[2022-09-05 00:00:00,2022-09-06 00:00:00,2022-09-07 00:00:00,2022-09-08 00:00:00,2022-09-09 00:00:00,2022-09-13 00:00:00,2022-09-14 00:00:00,2022-09-15 00:00:00,2022-09-16 00:00:00,2022-09-19 00:00:00,...]]

        '''
        print("Debug tdays: \n", obj_w )

        ### notes: 数据是倒置的，所以日期要先放在columns
        df_dt = pd.DataFrame(obj_w.Data, index= ["date"] ,columns=obj_w.Times )
        df_dt=df_dt.T

        ### notes: column=date 格式是datetime, 还需要保存成 str 和 int 格式
        # int是外部excel文件导入df时的默认格式
        import datetime as dt  
        df_dt["date_str"] = df_dt["date"].apply( lambda x :dt.datetime.strftime(x,"%Y%m%d" ))
        df_dt["date_int"] = df_dt["date_str"].apply( lambda x : int(x) )

        obj_dt["df_dt"] = df_dt



        
        return obj_dt