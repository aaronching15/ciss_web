# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
功能：用Choice-api模块获取API数据;
    1,class choice_api : 获取 等数据
    2,class choice_api_pms :组合管理
notes:
    1,Choice-API安装报错：Win 87；分析：安装时设置安装的文件目录为api脚本的访问目录，把文件夹整个放在【C:\ProgramData\Anaconda3\Lib\site-packages\】里就行

    
数据来源： Wind-API 万得量化数据接口
last update 220902 | since  160121
derived from  rC_Data_Initial.py with get-Wind.py gradually. | 190718file_name
===============================================
'''
import pandas as pd
import numpy as np
import json
import time
import datetime as dt
import os  

# if not "c" in  locals().keys() :
#     from EmQuantAPI import c
#     loginResult = c.start("ForceLogin=1", '', "")



class choice_api():
    ### 获取Wind的PMS相关数据:wpf,wps,wpd,wupf
    def __init__(self):
        ### 获取wpf相关数据
        ##########################################
        ### 
        self.nan = np.nan 
        self.path_ciss_web = os.getcwd().split("ciss_web")[0]+"ciss_web\\"
        self.path_ciss_rc = self.path_ciss_web +"CISS_rc\\"
        self.path_dt = self.path_ciss_rc + "db\\db_times\\"
        ###  
        self.path_data_choice = os.getcwd().split("ciss_web")[0]+"\\data_choice\\" 
        self.path_wpf = self.path_data_choice + "wpf\\"
        self.path_wpd = self.path_data_choice + "wpd\\"
        self.path_wsd = self.path_data_choice + "wsd\\"

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
        ###
        import dateutil.relativedelta as rd 
        self.time_str_pre_1m = dt.datetime.strftime(self.time_now - rd.relativedelta(months=1) , "%Y%m%d")
        self.time_str_pre_3m = dt.datetime.strftime(self.time_now - rd.relativedelta(months=3) , "%Y%m%d")
        self.time_str_pre_6m = dt.datetime.strftime(self.time_now - rd.relativedelta(months=6) , "%Y%m%d")
        self.time_str_pre_1y = dt.datetime.strftime(self.time_now - rd.relativedelta(years=1) , "%Y%m%d")
        self.time_str_pre_3y = dt.datetime.strftime(self.time_now - rd.relativedelta(years=3) , "%Y%m%d")

        ##########################################
        ### 每次调用API，要保存数据提取数量，保存到外部文件 
        self.api_count ={} 

    def print_info(self):
        ### print all modules for current class
        ###################################################
        ### csd | 给定交易区间，获取股票、债券、基金、指数的 基本资料、行情、估值、盈利
        

        ###########################################################################
        ### css | 给定交易日，获取股票、债券、基金、指数的 基本资料、行情、估值、盈利
        print("get_css_stock_month | 给定日期和代码，获取股票月行情数据:前收盘价、最高最低、收盘价、成交金额 ")
        print("get_css_ma_n | 均线：给定日期、均线参数和代码列表，获取股票和指数的均线数据 ")
        print("get_css_mv_pe | 市值和PE估值类指标：给定日期和代码列表 ")
        print("get_css_estimate | 预测类类指标：给定日期和代码列表 ")
        ### 基金指标
        print("get_css_fund_nav_rank | 基金指标：给定日期、和代码列表，获取基金净值、区间收益率、区间排名 ")


        ###########################################################################
        ### ctr |专题报表,df:1，指数成分股；2，企业财务报表；3，机构持股；4，大股东增减持



        ###########################################################################
        ### sector,获取板块的成分股，df，"001004"对应全行业？？



        ###########################################################################
        ### time  
        print("get_tdays | 获取日期 todo ")

        ###########################################################################
        ###
        print("save_api_count | 保存api_count 指标到excel ")


        ###########################################################################
        ###########################################################################
        ### BEFORE ||之前wind的模式
        print("get_wss_ma_n | 均线：给定日期、均线参数和代码列表，获取股票和指数的均线数据 ")
        print("get_wss_close_pctchg_amt | 行情和估值：给定日期、周期参数和代码列表，获取股票和指数的收盘价、涨跌幅、成交额、市值和PE_ttm")
        print("get_wss_estimate | 预测指标：给定日期、和代码列表，获取股票的FY1,FY2的一致预测指标 ")
        

        ###################################################
        ### portfolio
        print("get_wpf | 获取PMS组合持仓数据。")
        print("get_wps | 获取PMS组合区间涨跌幅、回撤、Alpha、Sharpe等绩效指标。")
        print("get_wpd | 获取PMS组合日期序列的总资产和盈亏等 ")
        print("get_wupf |  ")
        ###################################################
        ### quote and indicators
        print("---------------------------------------------------------------------- ")
        print("get_wss_ma_amt_mv | 给定含代码的df，获取特定价量数据 ")
        print("get_wss_pct_chg_period | 给定含代码的df，获取多个区间涨跌幅 ")
        print("get_wss_fund_1date | 给定基金代码及日期，获取多个不同基金指标 ")
        print("---------------------------------------------------------------------- ")
        print("get_wsd_period | 给代码和收盘价等指标，获取区间内每个交易日的指标数据,并合并保存到xlsx文件 ")

        ###################################################
        ### fund performance 
        print("---------------------------------------------------------------------- ")
        print("get_wss_fund_perf | 给定基金代码、区间、获取基金和基金经理绩效指标 ")
        print("get_wsd_fund_unit | 给定基金代码、区间、获取基金净值 ")
        print("---------------------------------------------------------------------- ")

    
    ######################################################################################################
    ### 计算指标用量
    def save_api_count(self):
        ### 保存api_count 指标到excel 

        ##########################################
        ### 每次调用API，要保存数据提取数量，保存到外部文件 
        file_name = "api_count_choice.xlsx"
        sheet = "api_count" 
        df_api = pd.read_excel( self.path_data_choice + file_name,sheet_name= sheet ) 
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

        df_api.to_excel( self.path_data_choice + file_name,sheet_name= sheet ,index=False)


        return 1
    
    def mainCallback(self, quantdata):
        """
        mainCallback 是主回调函数，可捕捉如下错误
        在start函数第三个参数位传入，该函数只有一个为c.EmQuantData类型的参数quantdata
        :param quantdata:c.EmQuantData
        :return:
        """
        print("mainCallback",str(quantdata)) 
        #登录掉线或者 登陆数达到上线（即登录被踢下线） 这时所有的服务都会停止
        if str(quantdata.ErrorCode) == "10001011" or str(quantdata.ErrorCode) == "10001009":
            print ("Your account is disconnect. You can force login automatically here if you need.")
        #行情登录验证失败（每次连接行情服务器时需要登录验证）或者行情流量验证失败时，会取消所有订阅，用户需根据具体情况处理
        elif str(quantdata.ErrorCode) == "10001021" or str(quantdata.ErrorCode) == "10001022":
            print ("Your all csq subscribe have stopped.")
        #行情服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
        elif str(quantdata.ErrorCode) == "10002009":
            print ("Your all csq subscribe have stopped, reconnect 6 times fail.")
        # 行情订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_QUOTE_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
        elif str(quantdata.ErrorCode) == "10002012":
            print ("csq subscribe break on some error, reconnect and request automatically.")
        # 资讯服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
        elif str(quantdata.ErrorCode) == "10002014":
            print ("Your all cnq subscribe have stopped, reconnect 6 times fail.")
        # 资讯订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_INFO_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
        elif str(quantdata.ErrorCode) == "10002013":
            print ("cnq subscribe break on some error, reconnect and request automatically.")
        # 资讯登录验证失败（每次连接资讯服务器时需要登录验证）或者资讯流量验证失败时，会取消所有订阅，用户需根据具体情况处理
        elif str(quantdata.ErrorCode) == "10001024" or str(quantdata.ErrorCode) == "10001025":
            print("Your all cnq subscribe have stopped.")
        else:
            pass

        return 1


    ######################################################################################################
    ### 股票等月行情用量
    def get_css_stock_month(self, obj_data) :
        ### 给定日期和代码，获取股票月行情数据：前收盘价、最高最低、收盘价、成交金额
        ### 给定日期、均线参数和代码列表，获取股票和指数的均线数据
        ### trade_date是给定的交易日期、para_ma是均线的参数、df_data是包含名为"code"代码列的df
        ### col_name是保存获取的指标值的列名称，如ma100，ma40-pre等。
        trade_date = obj_data["trade_date"]
        # para_ma = obj_data["para_ma"]
        df_data = obj_data["df_data"]  
        ### 要把index都设置为code
        df_data.index = df_data["code"]
        ###
        if "col_list_str" in obj_data.keys():
            col_list_str = obj_data["col_list_str"] 
        elif "col_list" in obj_data.keys():
            col_list_str = ",".join( obj_data["col_list"] ) 

        ########################################################################## 
        ### 每次下载100个 | 测试的时候把单次数量改为2个
        para_num_code =  100
        num_100 = len( df_data.index )//para_num_code +1

        # ### TODO 测试用
        # para_num_code = 3
        # num_100 = 2  

        ###########################################################################
        ### 调用登录函数（激活后使用，不需要用户名密码）
        # Qs报错： SyntaxError: import * only allowed at module level
        # Ana: EmQuantAPI.py 里有一个class c()
        if not "c" in  locals().keys() :
            from EmQuantAPI import c
            loginResult = c.start("ForceLogin=1", '', "")
        ### 没什么用  
        # loginResult = c.start("ForceLogin=1", '', self.mainCallback)
        # if(loginResult.ErrorCode != 0):
        #     print("login in fail")

        ###########################################################################
        ### 
        count_api = 0 
        count_df = 0 
        for temp_i in range( num_100 ) :
            print("Monthly quote data,Working on codes: ",trade_date,temp_i*para_num_code ,(temp_i+1)*para_num_code  )
            ### 0,1,...,46
            sub_index = df_data.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
            code_list = list( df_data.loc[ sub_index , "code"   ] )

            ### notes:代码可以是str也可以是list格式，
            # 指标列必须是str：转换 col_list_str = ",".join( col_list )
            # col_list_str = "PreCloseM,OpenM,HighM,LowM,CloseM,AvgPriceM,DifferRangeM,AmountM"
            num_indi = len( col_list_str.split(",") )
            ### Ispandas=1 means return is dataframe,但是如果数据有问题需要自己在df里识别识别
            para_str = "TradeDate=" + trade_date+ ", Ispandas=1"
            print("str of c.css() : " ,code_list , col_list_str, para_str )
            ### Ispandas 方式返回时会带有列 "DATES"
            df_temp = c.css( code_list , col_list_str, para_str)

            ###    DATES PRECLOSEM  OPENM  HIGHM   LOWM CLOSEM AVGPRICEM DIFFERRANGEM      AMOUNTM
            # CODES
            # 000002.SZ  2022-09-29      20.5  20.49  20.59  17.11  17.21   18.5066     -16.0488  2.87953e+10
            # 300059.SZ  2022-09-29      25.4  25.44  25.86  22.21  22.25   23.5022     -12.4016  7.82423e+10

            ###################################################
            ### 判断是否报错 | case1:判断是否为None，df_temp.loc[0,"000002.SZ"] == None 
            if type(df_temp ) == pd.core.frame.DataFrame :
                ### print('all_data占据内存约: {:.2f} GB'.format(df.memory_usage().sum()/ (1024**3))) || GB
                if not df_temp.memory_usage().sum() > 5 : 
                    ### 可能存在error
                    df_temp.to_excel("D:\\df_temp.xlsx")
                    print("Error check: \n ", df_temp.iloc[-1,-1] ,  df_temp )
                    
                else :
                    ### append to full df 
                    if count_df == 0 :
                        df_all = df_temp
                        count_df = 1
                    else :
                        df_all = df_all.append( df_temp )  
                    ### 
                    count_api = count_api + para_num_code 
            ###
            else :
                ### 可能有错误
                # df_temp= <class 'EmQuantAPI.c.EmQuantData'> ErrorCode=10003007, ErrorMsg=date format is not correct, Data={}
                print("Error check: \n ", type(df_temp ) , df_temp )
            ### 
            time.sleep(0.3) 
        
        ###################################################
        ### 退出登录
        if "c" in  locals().keys() :
            loginResult = c.stop()

        ###########################################################################
        ### 把df_all里的指标列存入 df_data | 这个和wind是不一样的
        ### notes:返回数据的列名col_name都是大写字符，如“PRECLOSEM  OPENM ”
        # df_all.index 就是codes ; 例如： dict_col_indi["close"] = "CloseM"
        dict_col = obj_data["dict_col"] 
        for temp_col in dict_col.keys() :  
            ### from PreCloseM to PRECLOSEM, by str1.upper()
            str_upper =  dict_col[temp_col].upper()
            df_data.loc[ df_all.index , temp_col  ] = df_all.loc[ df_all.index, str_upper ]


        ###########################################################################
        ### TEST 
        # trade_date = "20220731"
        # code_list_str = "300059.SZ,000002.SZ"
        # # col_list=
        # col_list_str = "PreCloseM,OpenM,HighM,LowM,CloseM,AvgPriceM,DifferRangeM,AmountM"
        # ### Ispandas=1 means return is dataframe
        # para_str = "TradeDate=" + trade_date+ ", Ispandas=1"
        # obj_data = c.css(code_list_str,col_list_str, para_str)
        ''' 
        DATES PRECLOSEM  OPENM  HIGHM   LOWM CLOSEM AVGPRICEM DIFFERRANGEM      AMOUNTM
        CODES
        000002.SZ  2022-09-29      20.5  20.49  20.59  17.11  17.21   18.5066     -16.0488  2.87953e+10
        300059.SZ  2022-09-29      25.4  25.44  25.86  22.21  22.25   23.5022     -12.4016  7.82423e+10
        '''
        
        ###################################################
        ### 计算wind-api用量,并保存到excel
        self.api_count["wss"] = count_api * num_indi
        result = self.save_api_count()       
        ###########################################################################
        ### output
        ### save to excel 
        df_all.to_excel("D:\\df_all.xlsx")
        obj_data["df_data"] = df_data         

        return obj_data
    

    def get_css_ma_n(self, obj_data ):
        ### 均线：给定日期、均线参数和代码列表，获取股票和指数的均线数据
        ### ref：get_wind_api.py\\get_wss_ma_n()
        ### 给定日期、均线参数和代码列表，获取股票和指数的均线数据
        ### trade_date是给定的交易日期、para_ma是均线的参数、df_data是包含名为"code"代码列的df
        ### col_ma 是报错ma数据的列名称，如 ma16，ma16_pre
        col_ma = obj_data["col_ma"]
        trade_date = obj_data["trade_date"]
        para_ma = obj_data["para_ma"]
        ### notes:不需要col_name,因为指标只有1个 "MA"
        ### 
        df_data = obj_data["df_data"]  
        ### 要把index都设置为code
        df_data.index = df_data["code"]
        

        ########################################################################## 
        ### 每次下载100个 | 测试的时候把单次数量改为2个
        para_num_code =  100
        num_100 = len( df_data.index )//para_num_code +1
        
        ### TODO 测试用
        # para_num_code = 3
        # num_100 = 2  

        ###########################################################################
        ### 调用登录函数（激活后使用，不需要用户名密码）
        if not "c" in  locals().keys() :
            from EmQuantAPI import c
            loginResult = c.start("ForceLogin=1", '', "")

        ###########################################################################
        ### 
        count_api = 0 
        count_df = 0 
        num_indi = 1
        for temp_i in range( num_100 ) :
            print("Monthly quote data,Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
            ### 0,1,...,46
            sub_index = df_data.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
            code_list = list( df_data.loc[ sub_index , "code"   ] )

            ### notes:代码可以是str也可以是list格式，
            # 指标列必须是str：转换 col_list_str = ",".join( col_list )
            # col_list_str = "PreCloseM,OpenM,HighM,LowM,CloseM,AvgPriceM,DifferRangeM,AmountM" 
            ### Ispandas=1 means return is dataframe,但是如果数据有问题需要自己在df里识别识别
            # Period =计算周期,取值范围:1 日, 2 周 ,3 月,4 季,5 半年,6 年
            # AdjustFlag=复权方式 取值范围：1 不复权, 后复权,3 前复权 
            para_str = "TradeDate=" + trade_date+ ",N="+ str(para_ma)  + ",Period=1,AdjustFlag=3,"+", Ispandas=1"
            ### Ispandas 方式返回时会带有列 "DATES"
            # CODES          DATES       MA 
            # 000002.SZ  2022-09-30  16.4088
            # 300059.SZ  2022-09-30    22.61
            print("str of c.css() : " ,code_list ,"MA", para_str )
            df_temp = c.css( code_list , "MA", para_str) 

            ### 'EmQuantData' object has no attribute 'iloc'
            ###################################################
            ### 判断是否报错 | 判断是否为None，df_temp.loc[0,"000002.SZ"] == None 
            if type(df_temp ) == pd.core.frame.DataFrame :
                if not df_temp.memory_usage().sum() > 5 : 
                    ### 可能存在error
                    df_temp.to_excel("D:\\df_temp.xlsx")
                    print("Error check: \n ", df_temp.iloc[-1,-1] ,  df_temp )
                    
                else :
                    ### append to full df 
                    if count_df == 0 :
                        df_all = df_temp
                        count_df = 1
                    else :
                        df_all = df_all.append( df_temp )  
                    ### 
                    count_api = count_api + para_num_code 
            else :
                ### 可能有错误
                # df_temp= <class 'EmQuantAPI.c.EmQuantData'> ErrorCode=10003007, ErrorMsg=date format is not correct, Data={}
                print("Error check: \n ", type(df_temp ) , df_temp )
            ### 
            time.sleep(0.3)  
        
        ###################################################
        ### 退出登录
        if "c" in  locals().keys() :
            loginResult = c.stop()
        
        ###########################################################################
        ### 把df_all里的指标列存入 df_data | 这个和wind是不一样的
        df_data.loc[ df_all.index , col_ma  ] = df_all.loc[ df_all.index, "MA" ] 
        
        ###################################################
        ### 计算wind-api用量,并保存到excel
        self.api_count["wss"] = count_api * num_indi
        result = self.save_api_count()       

        ###########################################################################
        ### output
        ### save to excel 
        df_all.to_excel("D:\\df_all.xlsx")
        obj_data["df_data"] = df_data      
        

        return obj_data
    
    def get_css_mv_pe(self, obj_data ):
        ### 市值和PE估值类指标：给定日期和代码列表
        ### ref：choice手册，1.1.1.6.1 ； get_wind_api.py\\get_wss_close_pctchg_amt()
        ### 给定日期、均线参数和代码列表，获取股票和指数的均线数据
        ### trade_date是给定的交易日期、para_ma是均线的参数、df_data是包含名为"code"代码列的df
        ### col_name是保存获取的指标值的列名称，如ma100，ma40-pre等。
        trade_date = obj_data["trade_date"] 
        dict_col = obj_data["dict_col"] 
        col_list = []
        for temp_col in dict_col.keys() : 
            ### from PreCloseM to PRECLOSEM, by str1.upper()
            str_upper =  dict_col[temp_col].upper()
            col_list = col_list + [ str_upper ]
        col_list_str = ",".join( col_list )
        ### 
        df_data = obj_data["df_data"]  
        ### 要把index都设置为code
        df_data.index = df_data["code"]

        ########################################################################## 
        ### 每次下载100个 | 测试的时候把单次数量改为2个  
        para_num_code =  100
        num_100 = len( df_data.index )//para_num_code +1
        
        ### TODO 测试用
        para_num_code = 3
        num_100 = 2  

        ###########################################################################
        ### 调用登录函数（激活后使用，不需要用户名密码）
        if not "c" in  locals().keys() :
            from EmQuantAPI import c
            loginResult = c.start("ForceLogin=1", '', "")

        ###########################################################################
        ### 
        count_api = 0 
        count_df = 0 
        num_indi = 1
        for temp_i in range( num_100 ) :
            print("Monthly quote data,Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
            ### 0,1,...,46
            sub_index = df_data.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
            code_list = list( df_data.loc[ sub_index , "code"   ] )

            ### notes:代码可以是str也可以是list格式，
            # 指标列必须是str：转换 col_list_str = ",".join( col_list ) 
            ### Ispandas=1 means return is dataframe,但是如果数据有问题需要自己在df里识别识别
            # Period =计算周期,取值范围:1 日, 2 周 ,3 月,4 季,5 半年,6 年
            # AdjustFlag=复权方式 取值范围：1 不复权, 后复权,3 前复权 
            para_str = "TradeDate=" + trade_date+ ",Period=1,AdjustFlag=3"+", Ispandas=1"
            ### Ispandas 方式返回时会带有列 "DATES"
            # CODES  DATES    PETTM    PBMRQN PCFCFOTTM     MVBYCSRC 
            # 000002.SZ  2022-09-30  8.16121  0.825747   34.4185  1.87421e+11
            # 300059.SZ  2022-09-30  31.6466   4.79913   11.9342  2.93354e+11
            print("str of c.css() : " ,code_list , col_list_str, para_str )
            df_temp = c.css( code_list , col_list_str , para_str) 

            ###################################################
            ### 判断是否报错 | 判断是否为None，df_temp.loc[0,"000002.SZ"] == None
            if type(df_temp ) == pd.core.frame.DataFrame :
                if not df_temp.memory_usage().sum() > 5 : 
                    ### 可能存在error
                    df_temp.to_excel("D:\\df_temp.xlsx")
                    print("Error check: \n ", df_temp.iloc[-1,-1] ,  df_temp )
                    
                else :
                    ### append to full df 
                    if count_df == 0 :
                        df_all = df_temp
                        count_df = 1
                    else :
                        df_all = df_all.append( df_temp )  
                    ### 
                    count_api = count_api + para_num_code 
            else :
                ### 可能有错误
                # df_temp= <class 'EmQuantAPI.c.EmQuantData'> ErrorCode=10003007, ErrorMsg=date format is not correct, Data={}
                print("Error check: \n ", type(df_temp ) , df_temp )
            ### 
            time.sleep(0.3)  

        ###################################################
        ### 退出登录
        if "c" in  locals().keys() :
            loginResult = c.stop()

        ###########################################################################
        ### 把df_all里的指标列存入 df_data | 这个和wind是不一样的
        # df_all.index 就是codes ; 例如： dict_col_indi["close"] = "CloseM"
        dict_col = obj_data["dict_col"] 
        for temp_col in dict_col.keys() :  
            ### from PreCloseM to PRECLOSEM, by str1.upper()
            str_upper =  dict_col[temp_col].upper()
            df_data.loc[ df_all.index , temp_col  ] = df_all.loc[ df_all.index, str_upper ]
            
        ###################################################
        ### 计算wind-api用量,并保存到excel
        self.api_count["wss"] = count_api * num_indi
        result = self.save_api_count()       

        ###########################################################################
        ### output
        ### save to excel 
        df_all.to_excel("D:\\df_all.xlsx")
        obj_data["df_data"] = df_data              

        return obj_data


    def get_css_estimate(self, obj_data ):
        ### 预测类类指标：给定日期和代码列表
        ### ref：choice手册，1.1.1.7.1 ； get_wind_api.py\\get_wss_close_pctchg_amt()
        ### 给定日期、均线参数和代码列表，获取股票和指数的均线数据
        ### trade_date是给定的交易日期、para_ma是均线的参数、df_data是包含名为"code"代码列的df
        ### col_name是保存获取的指标值的列名称，如ma100，ma40-pre等。
        trade_date = obj_data["trade_date"] 
        dict_col = obj_data["dict_col"] 
        col_list = []
        for temp_col in dict_col.keys() : 
            ### from PreCloseM to PRECLOSEM, by str1.upper()
            str_upper =  dict_col[temp_col].upper()
            col_list = col_list + [ str_upper ]
        col_list_str = ",".join( col_list )
        ### 
        df_data = obj_data["df_data"]  
        ### 要把index都设置为code
        df_data.index = df_data["code"]

        ########################################################################## 
        ### 每次下载100个 | 测试的时候把单次数量改为2个
        para_num_code =  100
        num_100 = len( df_data.index )//para_num_code +1
        
        ### TODO 测试用
        # para_num_code = 3
        # num_100 = 2  

        ###########################################################################
        ### 调用登录函数（激活后使用，不需要用户名密码）
        if not "c" in  locals().keys() :
            from EmQuantAPI import c
            loginResult = c.start("ForceLogin=1", '', "")

        ###########################################################################
        ### 
        count_api = 0 
        count_df = 0 
        num_indi = 1
        for temp_i in range( num_100 ) :
            print("Monthly quote data,Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
            ### 0,1,...,46
            sub_index = df_data.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
            code_list = list( df_data.loc[ sub_index , "code"   ] )

            ### notes:代码可以是str也可以是list格式，
            # 指标列必须是str：转换 col_list_str = ",".join( col_list ) 
            ### Ispandas=1 means return is dataframe,但是如果数据有问题需要自己在df里识别识别
            # PredictYear 预测年度 正整数，例：2016 
            para_str = "EndDate=" + trade_date+ ",Ispandas=1"
            ### Ispandas 方式返回时会带有列 "DATES"
            # CODES         DATES    SESTNIFY1    SESTNIFY2    SESTNIFY3 SESTNIYOY SESTNICGR2 SESTGRYOY SESTGRCOMPOUNDGROWTHRATE2 
            # 000002.SZ  2022-09-30  2.52273e+10  2.76735e+10  3.01431e+10   12.0019    10.8432   9.02621  8.28281
            # 300059.SZ  2022-09-30  9.90575e+09  1.22759e+10  1.49689e+10   15.8171    19.8032   12.2534  17.0314
            print("str of c.css() : " ,code_list , col_list_str, para_str )
            df_temp = c.css( code_list , col_list_str , para_str) 

            ###################################################
            ### 判断是否报错 | 判断是否为None，df_temp.loc[0,"000002.SZ"] == None
            if type(df_temp ) == pd.core.frame.DataFrame :
                if not df_temp.memory_usage().sum() > 5 : 
                    ### 可能存在error
                    df_temp.to_excel("D:\\df_temp.xlsx")
                    print("Error check: \n ", df_temp.iloc[-1,-1] ,  df_temp )
                    
                else :
                    ### append to full df 
                    if count_df == 0 :
                        df_all = df_temp
                        count_df = 1
                    else :
                        df_all = df_all.append( df_temp )  
                    ### 
                    count_api = count_api + para_num_code 
            else :
                ### 可能有错误
                # df_temp= <class 'EmQuantAPI.c.EmQuantData'> ErrorCode=10003007, ErrorMsg=date format is not correct, Data={}
                print("Error check: \n ", type(df_temp ) , df_temp )
            ### 
            time.sleep(0.3)  

        ###################################################
        ### 退出登录
        if "c" in  locals().keys() :
            loginResult = c.stop()

        ###########################################################################
        ### 把df_all里的指标列存入 df_data | 这个和wind是不一样的
        # df_all.index 就是codes ; 例如： dict_col_indi["close"] = "CloseM"
        dict_col = obj_data["dict_col"] 
        for temp_col in dict_col.keys() :  
            ### from PreCloseM to PRECLOSEM, by str1.upper()
            str_upper =  dict_col[temp_col].upper()
            df_data.loc[ df_all.index , temp_col  ] = df_all.loc[ df_all.index, str_upper ]
        
        ###################################################
        ### 计算wind-api用量,并保存到excel
        self.api_count["wss"] = count_api * num_indi
        result = self.save_api_count()       

        ###########################################################################
        ### output
        ### save to excel 
        df_all.to_excel("D:\\df_all.xlsx")
        obj_data["df_data"] = df_data              

        return obj_data


    def get_css_fund_nav_rank(self, obj_data ):
        ### 基金指标：给定日期、和代码列表，获取基金净值、区间收益率、区间排名
        ### trade_date是给定的交易日期、para_ma是均线的参数、df_data是包含名为"code"代码列的df
        ### col_name是保存获取的指标值的列名称，如ma100，ma40-pre等。
        trade_date = obj_data["trade_date"] 
        start_date = obj_data["start_date"]
        end_date = obj_data["end_date"]  
        ###
        dict_col = obj_data["dict_col"] 
        col_list = []
        for temp_col in dict_col.keys() : 
            ### from PreCloseM to PRECLOSEM, by str1.upper()
            str_upper =  dict_col[temp_col].upper()
            col_list = col_list + [ str_upper ]
        col_list_str = ",".join( col_list )
        ### 
        df_data = obj_data["df_data"]  
        ### 要把index都设置为code
        df_data.index = df_data["code"]

        ########################################################################## 
        ### 每次下载100个 | 测试的时候把单次数量改为2个
        para_num_code =  100
        num_100 = len( df_data.index )//para_num_code +1
        
        ### TODO 测试用
        # para_num_code = 3
        # num_100 = 2  

        ###########################################################################
        ### 调用登录函数（激活后使用，不需要用户名密码）
        if not "c" in  locals().keys() :
            from EmQuantAPI import c
            loginResult = c.start("ForceLogin=1", '', "")

        ###########################################################################
        ### 
        count_api = 0 
        count_df = 0 
        num_indi = 1
        for temp_i in range( num_100 ) :
            print("Monthly quote data,Working on funds: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
            ### 0,1,...,46
            sub_index = df_data.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
            code_list = list( df_data.loc[ sub_index , "code"   ] )

            ###################################################
            ### notes:代码可以是str也可以是list格式，
            # 指标列必须是str：转换 col_list_str = ",".join( col_list ) 
            ### Ispandas=1 means return is dataframe,但是如果数据有问题需要自己在df里识别识别
            ###################################################
            ### notes:,参数IsNaau=2 对应 今年以来回报,YTDRETURN, 近1月回报,MONTHLYRETURN
            para_str = "TradeDate=" + trade_date+ ",StartDate="+ start_date +",EndDate="+ end_date +",FundType=2, IsNaau=2,Ispandas=1"
            print("para_str=", para_str )
            # col_list_str = "NAVUNIT,NAVADJ,MRETURN,MONTHLYRETURN,FUNDSCALE,NAVRETURNRANKINGP,NAVRETURNRANKINGPCTP,YTDRETURN" 
            ### Ispandas 方式返回时会带有列 "DATES"
            # CODES                   000001.OF    000011.OF
            # DATES                  2022-10-11   2022-10-11
            # NAVUNIT                     1.073       18.869
            # NAVADJ                    6.93138      39.0407
            # MRETURN                    9.2668      11.8627
            # MONTHLYRETURN              9.2668      11.8627
            # FUNDSCALE             3.34856e+09  4.86639e+09
            # NAVRETURNRANKINGP       2852/6798    1517/6798
            # NAVRETURNRANKINGPCTP      41.9535      22.3154
            # YTDRETURN                 -11.249      -7.3004

            print("str of c.css() : " ,code_list , col_list_str, para_str )
            df_temp = c.css( code_list , col_list_str , para_str) 

            ###################################################
            ### 判断是否报错 | 判断是否为None，df_temp.loc[0,"000002.SZ"] == None
            if type(df_temp ) == pd.core.frame.DataFrame :
                ### 判断是否是空的df, 空的 df1.memory_usage().sum() = 0 
                # if df_temp.iloc[-1,-1] == None :
                if not df_temp.memory_usage().sum() > 5 :
                    ### 可能存在error
                    df_temp.to_excel("D:\\df_temp.xlsx")
                    print("Error check: \n ", df_temp.iloc[-1,-1] ,  df_temp )
                    
                else :
                    ### append to full df 
                    if count_df == 0 :
                        df_all = df_temp
                        count_df = 1
                    else :
                        df_all = df_all.append( df_temp )  
                    ### 
                    count_api = count_api + para_num_code 
            else :
                ### 可能有错误
                # df_temp= <class 'EmQuantAPI.c.EmQuantData'> ErrorCode=10003007, ErrorMsg=date format is not correct, Data={}
                print("Error check: \n ", type(df_temp ) , df_temp )
            ### 
            time.sleep(0.3)  

        ###################################################
        ### 退出登录
        if "c" in  locals().keys() :
            loginResult = c.stop()

        ###########################################################################
        ### 把df_all里的指标列存入 df_data | 这个和wind是不一样的
        # df_all.index 就是codes ; 例如： dict_col_indi["close"] = "CloseM"
        dict_col = obj_data["dict_col"] 
        for temp_col in dict_col.keys() :  
            ### from PreCloseM to PRECLOSEM, by str1.upper()
            str_upper =  dict_col[temp_col].upper()
            df_data.loc[ df_all.index , temp_col  ] = df_all.loc[ df_all.index, str_upper ]
        
        ###################################################
        ### 计算wind-api用量,并保存到excel
        self.api_count["wss"] = count_api * num_indi
        result = self.save_api_count()       

        ###########################################################################
        ### output
        ### save to excel 
        df_all.to_excel("D:\\df_all.xlsx")
        obj_data["df_data"] = df_data              

        return obj_data