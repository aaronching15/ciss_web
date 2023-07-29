# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
功能：
1,class portfolios():定义组合对象
2，class portfolio_return():计算组合收益率和区间绩效指标
3，class gen_portfolios():生成组合对象
4，class manage_portfolios():管理组合对象
    1，组合管理对象：ports.py\\class admin_portfolios():
    2，组合管理引擎：bin\\engine_portfolio.py
5，class manage_pms():管理Wind-PMS组合管理模块的建仓和调仓等功能

last update 230320 | since  181102 
    
分析：
0，main function
    0.1, profit and loss analysis/monitoring
    0.2, performance attributes
    0.3, periodic statistics
    0.4, asset allocation ? | data can be merged into grouped view in accounts,funds or groups
    0.5, risks 
    0.6, rebalance 组合调整
    0.7， 

1，输入:
    1.1,证券池子,如StockPool：
    
2，配置文件 | config\config_port.py

3，股票池和组合当前是 1：N，下一步考虑 N：1，N:N的情况
    例如：如果同时有｛A股，港股，美股｝多个市场情况下，是把所有个股放1个stockpool里，
    还是每个市场单独一个stockpool ? 
Notes: 
refernce: rC_Portfolio_17Q1.py 
===============================================
'''
import sys,os
# from tracemalloc import stop # 占用内存统计
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
sys.path.append("..") 

###################################################
class portfolios():
    def __init__(self, config={},port_name=''):
        import gen_portfolios
        # generate portfolio object
        self.portfolio = gen_portfolios(config,port_name)
    
    def print_info(self):        
        print("get_port_list:获取组合列表 ")
        print("") 

################################################### 
class portfolio_return():
    def __init__(self ): 
        ################################################
        ### config_data
        from config_data import config_data
        config_data1 = config_data()
        self.path_dict = config_data1.obj_config["dict"] 
        self.path_ciss_web = self.path_dict["path_ciss_web"]
        self.path_ciss_rc = self.path_dict["path_ciss_rc"]
        self.path_db = self.path_dict["path_db"] 
        self.path_db_times = self.path_dict["path_db_times"]
        self.path_db_assets = self.path_dict["path_db_assets"]
        
        ################################################
        ### 导入数据地址          
        self.path_data_pms = self.path_dict["path_data_pms"]
        self.path_data_adj = self.path_dict["path_data_adj"]
        self.path_fundpool = self.path_dict["path_fundpool"]
        self.path_wind_terminal = self.path_dict["path_wind_terminal"]  
        self.path_wsd = self.path_dict["path_wsd"] 
        self.path_wss = self.path_dict["path_wss"] 
        self.path_wpf = self.path_dict["path_wpf"] 
        self.path_wpd = self.path_dict["path_wpd"] 


        print("")
        self.nan = np.nan 
        import datetime as dt  
        self.time_now = dt.datetime.now()
        self.time_pre =  self.time_now - dt.timedelta(days=1) 
        self.time_pre10 =  self.time_now - dt.timedelta(days=10) 
        ### 获取近1w、1m、3m、6m、1year日期
        self.time_now_str = dt.datetime.strftime(self.time_now , "%Y%m%d")
        self.time_pre_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=1) , "%Y%m%d")
        self.time_pre10_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=10) , "%Y%m%d")

    def print_info(self):   
        ##########################################
        ### 提取数据 
        print("get_port_list | 获取组合列表和和策略列表")  
        print("get_port_perf | 获取组合收益率和区间绩效指标")     
        print("get_port_unit | 获取组合和给定基准的历史净值 ")
        print("get_bench_unit | 获取给定基准的历史净值 ")
        print("get_port_holding | 获取组合持仓列表")     
        ##########################################
        ### 计算指标
        print("cal_port_ret_notrade | 给定组合初始权重和区间日股票收益率，计算区间组合日收益率，期间无调仓")
        print("cal_port_performance | 给定区间组合收益率，计算区间累计收益、最大回撤、超额收益、波动率、夏普比例等")
        print("")
    
    def get_port_list(self,obj_p): 
        ###  获取组合列表和策略列表
        port_type = obj_p["port_type"] 

        ##########################################
        ### 导入配置文件
        from config_data import config_data
        config_data_1 = config_data() 
        
        ###
        sheet= "组合列表"
        file_name ="pms_manage.xlsx"
        df_p = pd.read_excel( self.path_data_pms+file_name,sheet_name=sheet )
        if port_type == "all" :
            obj_p["df_p"] = df_p
        else : 
            obj_p["df_p"] = df_p[ df_p["asset_type"] == port_type ]

        ### 获取策略列表
        ### 导入组合的策略配置比例
        sheet = "组合策略配置" 
        df_psw = pd.read_excel( config_data_1.obj_config["dict"]["path_data_pms"] +file_name,sheet_name=sheet )
        obj_p["df_port_stra_weight"] = df_psw

        return obj_p

    def get_port_perf(self,obj_p): 
        ###  获取组合收益率和区间绩效指标
        
        sheet= "组合列表"
        file_name ="pms_manage.xlsx"
        df_p = pd.read_excel( self.path_data_pms+file_name,sheet_name=sheet )
        
        if "port_type" in obj_p.keys() :
            port_type = obj_p["port_type"] 
            if port_type == "all" :
                    obj_p["df_p"] = df_p
            else : 
                obj_p["df_p"] = df_p[ df_p["asset_type"] == port_type ]

        elif "port_name" in obj_p.keys() :
            port_name = obj_p["port_name"] 
            obj_p["df_p"] = df_p[ df_p["port_name"] == port_name ]
        
        ##########################################
        ### 2，用Wind-api获取区间收益率、最大回撤、月胜率、Alpha、Sharpe

        ### w.wps("行业成长价值精选", "Return_w,Return_m,Return_y,Return_1m,Return_3m,Return_6m,Alpha_std,Sharpe_std,MaxDrawdown_std,MaxDrawdown_6m,MaxDrawdown_y,Winning_Rate_m","view=PMS;startDate=20220211;endDate=20220211;Currency=BSY;returntype=1;fee=1")
        ### WindCodes	Return_w	Return_m	Return_y	Return_1m	Return_3m	Return_6m	Alpha_std	Sharpe_std	MaxDrawdown_std	MaxDrawdown_6m	MaxDrawdown_y	Winning_Rate_m
        # 	本周收益率	本月收益率	本年收益率	近1个月收益率	近3个月收益率	近6个月收益率	Alpha成立至今	Sharpe成立至今	最大回撤：成立至今	最大回撤：近6个月	最大回撤：本年	月胜率
        # 行业成长价值精选	0.003032		-0.001744	-0.001744	-0.001744	-0.001744	0.047268	-1.769173	-0.007046	-0.007046	-0.007046	0
        if "get_wps" in obj_p.keys() :    
            ##########################################
            ### wind_api获取数据
            from get_wind_api import wind_api
            class_wind_api = wind_api()
            df_perf = obj_p["df_p"]
            df_perf = class_wind_api.get_wps( df_perf )
            ### output: 
            obj_p["df_perf"] = df_perf
        

        return obj_p

    def get_port_unit(self,obj_p): 
        ###  获取组合历史净值;默认week,或day.如果是交易日，需要保存。
        ####################################################################################
        date_begin = obj_p["date_begin"]
        date_end = obj_p["date_end"]
        import datetime as dt
        date_begin_dt= dt.datetime.strptime( str( int(date_begin )) , "%Y%m%d") 
        date_end_dt= dt.datetime.strptime( str( int(date_end )) , "%Y%m%d") 
        ### 净值类型：week
        ### notes:如果是周week，只会返回结束日期；如果是交易日day，会正常返回序列
        unit_type = obj_p["unit_type"]  

        ##########################################
        ### 获取组合基准代码和名称；同时提取基准指数的收益率
        df_port_list = pd.read_excel( self.path_data_pms+ "pms_manage.xlsx", sheet_name="组合列表" )
        df_temp = df_port_list[ df_port_list["port_name"] == obj_p["port_name"]  ]
        if len( df_temp.index ) > 0 :
            benchmark_code = df_temp["benchmark_code"].values[0]
            # value="885001.WI"
        else :
            print("No port name "+ obj_p["port_name"] +"in excel file=pms_port_unit.xlsx, sheet=组合列表 ")

        ####################################################################################
        ### notes：如果是 day，则需要维护历史数据 
        from get_wind_api import wind_api
        class_wind_api = wind_api()
        
        if unit_type == "week" :
            ### week 直接取数据，不考虑基准
            ##########################################
            ### wind_api获取数据
            ### w.wpd("行业成长价值精选", "NetAsset,TotalPL", "20220113", "20220213","view=PMS;Currency=BSY;period=D;Fill=Blank")
            obj_p = class_wind_api.get_wpd( obj_p )
            
            #################################################### 
            ### 计算净值 |  [""NetAsset,TotalPL""]
            ### notes: 可能一开始部分日期组合净资产为0                     
            df_port = obj_p["df_port"]
            df_port = df_port[ df_port["NetAsset"] > 0   ]
            
            obj_w = obj_p["obj_w"]    
            if  obj_w.ErrorCode == 0 :
                ### 返回NetAsset，TotalPL 两列，T日盈亏除以T-1日净资产，可以得到收益率百分比
                ### 期初净资产
                df_port["asset_init"] = df_port["NetAsset"] - df_port["TotalPL"]
                ### 单位净值 
                df_port["unit_port"] = df_port["NetAsset"] / df_port["asset_init"].values[0]
                ### 日涨跌幅
                df_port["pct_chg"] = df_port["TotalPL"] / df_port["asset_init"]


        elif unit_type == "day" :
            ####################################################################################
            ### Part 1 计算基准的区间净值和维护历史数据
            obj_b = self.get_bench_unit( obj_p)
            df_bench = obj_b["df_bench"]

            ####################################################################################
            ### Part 2 计算组合的区间净值和维护组合历史净值数据:交易日
            port_name = obj_p["port_name"] 

            ###########################################
            ### 对查询的组合，检查历史净值文件，并下载缺失的部分。
            file_pms = "pms_port_unit.xlsx"
            df_unit_manage = pd.read_excel( self.path_data_pms + file_pms, sheet_name="port_unit")        
            ### step 1 获取组合最新净值日期 date_latest
            df_sub = df_unit_manage[ df_unit_manage["port_name"] == port_name ]
            ### 判断组合名称是否在组合净值列表文件内。
            if len( df_sub.index ) > 0 :                
                date_latest = df_sub["date_latest"].values[0]    
                  
                ##########################################
                ### 判断历史开始日期是否比输入的开始日期晚
                date_init = df_sub["date_init"].values[0 ] 
                date_init_dt = dt.datetime.strptime( str( int(date_init )) , "%Y%m%d") 
                
                if date_begin_dt  < date_init_dt : 
                    ### 采用给定的开始日期作为开始日期
                    obj_p["date_begin"] = date_begin
                else :
                    ### 大多数情况，采用之前下载的最新日期作为开始日期
                    obj_p["date_begin"] = date_latest 
                              
                ##########################################
                ### 判断上一次更新日期date_latest 是否比最新日期差了超过3个工作日                
                date_latest_dt = dt.datetime.strptime( str( int(date_latest)) , "%Y%m%d") 
                if date_latest_dt + dt.timedelta(days=1 ) < self.time_pre :                    
                    ##########################################
                    ### 重新定义开始和结束日期 date_begin
                    ### 采用之前下载的最新日期作为开始日期
                    obj_p["date_begin"] = date_latest 
                    ### 采用最新日期作为结束日期
                    obj_p["date_end"] = self.time_pre_str 
                    print("Debug date:" ,date_latest, obj_p["date_end"]  )  

                    ####################################################################################
                    ### wind_api获取数据: 获取组合区间净值和盈亏数据
                    ### w.wpd("行业成长价值精选", "NetAsset,TotalPL", "20220113", "20220213","view=PMS;Currency=BSY;period=D;Fill=Blank")
                    from get_wind_api import wind_api
                    class_wind_api = wind_api()

                    ####################################################################################
                    ### 如果有基准代码也会同时获取基准的区间收益率、净值
                    obj_p = class_wind_api.get_wpd( obj_p )
                    ### notes:如果有基准代码，也会提取相关的数据。if "benchmark_code" in obj_p.keys() :
                    
                    #################################################### 
                    ### 计算净值 |  [""NetAsset,TotalPL""]
                    ### notes: 可能一开始部分日期组合净资产为0                     
                    df_port = obj_p["df_port"]
                    df_port = df_port[ df_port["NetAsset"] > 0   ] 

                    obj_w = obj_p["obj_w"]     
                    #################################################### 
                    ### df_port 是最新下载的净值数据
                    df_port["date"] = df_port.index

                    ### exhi，将百分比数值控制百分比,2位小数点
                    # df_port[ "exhi_"+ "unit_port"] = df_port["unit_port"].round(decimals=2) 
                    
                    #################################################### 
                    ### 导入历史净值数据，跟新后保存
                    file_name  = port_name + ".xlsx"
                    if os.path.exists( self.path_wpd + file_name ) :
                        ### 如果存在文件，则导入
                        df_old = pd.read_excel( self.path_wpd + file_name )
                        df_old.index = df_old["date"]
                        ### 删除重复项,根据"date"列去重；因为出现2个一样日期列，导致报错
                        df_old = df_old.drop_duplicates(["date"],keep="last" )
                        
                        ### 历史净值 df_old 
                        # df_port_all = df_old
                        # date_last_old = df_port_all.index[-1]
                        ### append ：向dataframe对象中添加新的行，如果添加的列名不在dataframe对象中，将会被当作新的列进行添加
                        df_port_all = df_port.append(df_old   ) 

                    else :
                        df_port_all = df_port      

                    #################################################### 
                    ### 调整"date"
                    df_port_all["date"] = df_port_all.index   
                    ### 按升序排列,用于计算净值
                    df_port_all = df_port_all.sort_values(by="date" , ascending="True" )
                    
                    #################################################### 
                    ### 计算其他列：根据NetAsset，TotalPL 两列，T日盈亏除以T-1日净资产，可以得到收益率百分比！！！
                    ### 期初净资产 asset_init
                    df_port_all["asset_init"] = df_port_all["NetAsset"] - df_port_all["TotalPL"]
                    ### 单位净值 unit_port —— 注意，这里的期初可能不是全部的期初
                    df_port_all["unit_port"] = df_port_all["NetAsset"] / df_port_all["asset_init"].values[0]
                    ### 日涨跌幅 pct_chg
                    df_port_all["pct_chg"] = df_port_all["TotalPL"] / df_port_all["asset_init"]    
                    #################################################### 
                    ### 保存到外部文件
                    ### log 文件
                    df_unit_manage.loc[ df_sub.index[0], "date_latest" ] = self.time_pre_str 
                    file_pms = "pms_port_unit.xlsx"
                    df_unit_manage.to_excel(self.path_data_pms + file_pms, sheet_name="port_unit" ,index=False)     
                    print("Debug--df_unit_manage")
                    print( df_unit_manage.T ) 
                    ### PMS组合净值文件
                    df_port_all.to_excel( self.path_wpd + file_name , sheet_name="port_unit" ,index=False)   
                    #################################################### 
                    ### 为了后续日期的比较不出错，再次导入
                    df_port_all = pd.read_excel( self.path_wpd + file_name )
                    df_port_all.index = df_port_all["date"]

                else :
                    ### 直接导入历史净值数据，跟新后保存
                    file_name  = port_name + ".xlsx"
                    df_port_all = pd.read_excel( self.path_wpd + file_name )
            else :
                print("No port name "+ port_name +"in excel file=pms_port_unit.xlsx, sheet=port_unit ")
                     
        ########################################################################################################
        ### Notes:df_port下载的日期区间是之前没下载过的，df_port_all 是全历史的数据；展示的是输入的日期区间
        ### 三个区间都是不一样的。 
        df_port_exhi = df_port_all[ df_port_all["date"] > date_begin ]
        df_port_exhi = df_port_exhi[ df_port_exhi["date"] < date_end ]

        ### 裁切成相同的日期区间 
        # notes:如果是导入数据，可能出现  <class 'numpy.datetime64'> 2021-12-31T00:00:00.000000000 
        df_bench = df_bench[ df_bench["date"] > date_begin ]
        df_bench = df_bench[ df_bench["date"] < date_end ]
        ########################################################################################################
        ### 合并组合净值和基准净值 || notes:index不一样，不能直接用append
        ### 删除重复项
        df_port_exhi = df_port_exhi.drop_duplicates( ["date"] )
        df_port_exhi.index= df_port_exhi["date"]
        df_bench.index = df_bench["date"]
        df_port_exhi.to_excel("D:\\df_port_exhi.xlsx")
        for temp_i in df_bench.index :
            df_port_exhi.loc[temp_i,"pct_chg_bench"] = df_bench.loc[temp_i,"pct_chg_bench"] 
            df_port_exhi.loc[temp_i,"unit_bench"] = df_bench.loc[temp_i,"unit_bench"] 
        
        ### 直接整列赋值会出错，奇怪。
        # df_port_exhi["pct_chg_bench"] = df_bench["pct_chg_bench"] 
        # df_port_exhi["unit_bench"] = df_bench["unit_bench"]   
             
        ########################################################################################################
        ### exhi，
        df_port = df_port_exhi
        ### 将百分比数值控制百分比,2位小数点
        df_port[ "exhi_"+ "unit_port"] = df_port["unit_port"] 
        ### 由于区间变了，需要让初始日期净值归一
        df_port[ "exhi_"+ "unit_port"] = df_port[ "exhi_"+ "unit_port"]/df_port[ "exhi_"+ "unit_port"].values[0]
        df_port[ "exhi_"+ "unit_port"] =df_port[ "exhi_"+ "unit_port"].round(decimals= 4)         
        df_port[ "exhi_"+ "pct_chg"] = df_port["pct_chg"]*100
        df_port[ "exhi_"+ "pct_chg"] =df_port[ "exhi_"+ "pct_chg"].round(decimals= 2) 
        ####################################################  
        ### bench
        df_port[ "exhi_"+ "unit_bench"] = df_port["unit_bench"]
        df_port[ "exhi_"+ "unit_bench"] = df_port[ "exhi_"+ "unit_bench"]/df_port[ "exhi_"+ "unit_bench"].values[0]
        df_port[ "exhi_"+ "unit_bench"] = df_port[ "exhi_"+ "unit_bench"].round(decimals=4)  
        # 
        df_port[ "exhi_"+ "pct_chg_bench"] = df_port["pct_chg_bench"]*100
        df_port[ "exhi_"+ "pct_chg_bench"] =df_port[ "exhi_"+ "pct_chg_bench"].round(decimals= 2) 
        ####################################################  
        ### PNL 总盈亏
        df_port[ "exhi_"+ "NetAsset"] = (df_port["NetAsset"]/10000).round(decimals= 2)
        df_port[ "exhi_"+ "TotalPL"] = (df_port["TotalPL"]/10000).round(decimals= 2)
        

        ### debug 
        #################################################### 
        ### 更新组合净值
        df_port.index = df_port[ "date" ]
        obj_p["df_port"] = df_port

        return obj_p
        
    def get_bench_unit(self, obj_p) :
        ### 获取给定基准的历史净值 
        ####################################################################################
        ### 对查询的基准指数、股票、基金等，下载复权净值和涨跌幅、检查历史净值文件并下载缺失的部分。
        date_begin = obj_p["date_begin"]
        date_end = obj_p["date_end"]
        benchmark_code =  obj_p["benchmark_code"] 
        obj_b = {}
        obj_b["code"] =  benchmark_code         

        ####################################################################################
        ### 判断指数是否存在、类型; 获取组合最新净值日期 date_latest            
        ### wsd_bench.xlsx || asset_type	code	date_init	date_latest	code_name_used 
        df_bench_manage = pd.read_excel( self.path_data_pms +  "wsd_bench.xlsx" )                    
        df_sub = df_bench_manage[ df_bench_manage["code"] == benchmark_code ]        
        ### date_begin_bench，基准指数提取的开始时间 |   date_begin_bench = date_begin

        #######################################
        ### 如果不存在，新建该指数的记录
        if not len( df_sub.index ) > 0 :
            ### 需要讲代码加入列表
            print("No code "+ obj_p["port_name"] +"in excel file=wds_bench.xlsx,  ")
            temp_i = df_bench_manage.index.max() +1 
            df_bench_manage.loc[temp_i, "code"] = benchmark_code 
            # value="885001.WI"
            df_bench_manage.loc[temp_i, "date_init"] = date_begin
            df_bench_manage.loc[temp_i, "date_latest"] = date_end
            df_bench_manage.loc[temp_i, "count"] = 0 
            ### 采用输入的开始日期,作为数据提取的开始时间
            ##########################################
            ### 基准指数提取的开始时间
            obj_b["date_begin"] = date_begin 
            ##########################################
            ### 上一次更新时间
            date_latest = "20100101"
        else :
            ##########################################
            ### 已经有历史记录了
            temp_i = df_sub.index[0 ]
            ##########################################
            ### 上一次更新时间
            date_latest = df_bench_manage.loc[temp_i, "date_latest"] 
            ##########################################
            ### 判断初始更新日期;有可能之前没这么早的数据
            import datetime as dt  
            date_init = df_bench_manage.loc[temp_i, "date_init"] 
            date_init_dt = dt.datetime.strptime( str( int(date_init)) , "%Y%m%d") 
            date_begin_dt = dt.datetime.strptime( str( int(date_begin)) , "%Y%m%d") 
            if date_begin_dt < date_init_dt :
                df_bench_manage.loc[temp_i, "date_init"] = date_begin
                ### 采用输入的开始日期,作为数据提取的开始时间
                ### 基准指数提取的开始时间
                obj_b["date_begin"] = date_begin 
            else :
                ### 采用上次更新日期作为数据提取的开始时间 
                obj_b["date_begin"] = df_bench_manage.loc[temp_i, "date_latest"]   
            ########################################## 
        

        ####################################################################################
        ### 判断上一次更新日期date_latest 是否比最新日期差了超过2个工作日
        import datetime as dt  
        date_latest_dt = dt.datetime.strptime( str( int(date_latest)) , "%Y%m%d") 
        if date_latest_dt + dt.timedelta(days=1 ) < self.time_pre :  
            #################################################################################### 
            ### 给代码和收盘价等指标，获取区间内每个交易日的指标数据,并合并保存到xlsx文件 
            from get_wind_api import wind_api
            class_wind_api = wind_api()   
            ### 采用最新日期作为结束日期           
            obj_b["date_end"] = self.time_pre_str   
            print("Debug  obj_b: \n" ,obj_b  )

            obj_b = class_wind_api.get_wsd_period( obj_b  )
            ### output：全历史 obj_b["df_pct_chg"], 给定区间的 obj_b["df_period"] 
            ### df_pct_chg.columns: "pct_chg","date","unit" 
            df_bench = obj_b["df_period"] 

            ########################################
            ### 要把 unit,pct_chg 变成 unit_bench, pct_chg_bench ，TODO
            df_bench["unit_bench"] = df_bench["unit"] 
            df_bench["pct_chg_bench"] = df_bench["pct_chg"] 
            ### notes:净值数据需要是连续的，需要重新计算
            df_bench["unit_bench"] = df_bench["unit_bench"]/df_bench["unit_bench"].values[0]

            ####################################################
            ### 更新wsd时间区间
            df_bench_manage.loc[temp_i, "date_latest"] = date_end
            df_bench_manage.loc[temp_i, "count"] =df_bench_manage.loc[temp_i, "count"] +1
            df_bench_manage.to_excel( self.path_data_pms +  "wsd_bench.xlsx",index=False  )      
            
        else :
            ####################################################
            ### 直接导入历史数据
            file_name = "wsd_" + benchmark_code + ".xlsx"
            df_bench = pd.read_excel( self.path_wsd + file_name    )
            df_bench["unit_bench"] = df_bench["unit"] 
            df_bench["pct_chg_bench"] = df_bench["pct_chg"] 
            ### notes:净值数据需要是连续的，需要重新计算
            df_bench["unit_bench"] = df_bench["unit_bench"]/df_bench["unit_bench"].values[0]
        
        ### 删除重复项
        df_bench = df_bench.drop_duplicates( ["date"] )

        ####################################################################################
        ### output ：obj_b["df_bench"] = df_bench
        # output：全历史 obj_b["df_pct_chg"], 给定区间的 obj_b["df_period"] 
        obj_b["df_bench"] = df_bench 

        return obj_b

    def get_port_holding(self, obj_p) :
        ### 获取组合持仓列表
        #########################################
        ### 先判断是否已经下载过对应日期的持仓文件 | wpf_FOF偏债混合_20220121.xlsx
        # path= C:\rc_202X\rc_202X\data_pms\wpf
        file_name = "wpf_" + obj_p["port_name"]  + "_" + obj_p["date_end"]  + ".xlsx"
        if os.path.exists( self.path_wpf + file_name ) :
            print("requested pms holding file has exists.")
            ### index_col=0,将第一列变为index
            obj_p["df_pms_holding"] = pd.read_excel(self.path_wpf + file_name) 

        else : 
            dict_in= {}
            dict_in["pms_name"] = obj_p["port_name"] 
            ### notes:开始和结束日期是月末交易日，周末假期会报错
            dict_in["date_start"] = obj_p["date_begin"] 
            dict_in["date_end"] = obj_p["date_end"] 
            dict_in["col_type"] = "108"
            dict_in["if_excel"] = 1 
            ##########################################
            ### wind_api获取数据
            from get_wind_api import wind_api
            class_wind_api = wind_api() 
            df_data = class_wind_api.get_wpf( dict_in )        
            obj_p["df_pms_holding"] = df_data

        ##########################################
        ### 调整列值
        df_h = obj_p["df_pms_holding"]
        df_h["weight"] = df_h["NetHoldingValue"]/df_h["NetHoldingValue"].sum()
        df_h["weight_init"] = df_h["TotalCost"]/df_h["TotalCost"].sum()
        df_h["pnl_pct"] = df_h["EUnrealizedPL"]/df_h["TotalCost"]

        obj_p["df_pms_holding"] = df_h 

        return obj_p

    def cal_port_ret_notrade(self, obj_port) :
        ###  给定组合初始权重和区间日股票收益率，计算区间组合日收益率，期间无调仓
        # notes:不应该用初始权重乘以每日的涨跌幅，会导致数据失真
        #########################################
        ### PARA 参数设置
        ### Notes:为了快速计算，本算法忽略【总资产、股票市值、现金、债券市值、股票数量、股票成本价】等精细的计算        
        para_fee_trade = 0.003
        para_asset_init = 100000000.0
        # 逆回购日收益率
        para_ret_cash_1d = 0.02/365
        para_max_stock_pct = 0.95

        #######################################################################
        ### INPUT: date_list 排序过的交易日，作为 df_ashare_adjclose，df_ashare_pctchg的columns
        date_list = obj_port["dict"]["date_list"]  
        # 日期升序排列,以防万一
        date_list.sort()
        
        ### df_ashare_ana的index是股票代码，columns是基础信息和各个组合名称
        df_ashare_ana = obj_port["df_ashare_ana"]  
        try :
            df_ashare_ana.index = df_ashare_ana["S_INFO_WINDCODE"]
        except :
            df_ashare_ana["S_INFO_WINDCODE"] = df_ashare_ana.index 
        ### Notes:df_ashare_ana可能又很多NaN值
        df_ashare_ana = df_ashare_ana.fillna( 0.0 )

        ###  df_ashare_adjclose，df_ashare_pctchg 行是股票代码，列是日期        
        df_ashare_pctchg = obj_port["df_ashare_pctchg"] 
        df_ashare_adjclose = obj_port["df_ashare_adjclose"] 
        df_ashare_pctchg.index = df_ashare_pctchg["S_INFO_WINDCODE"]
        df_ashare_adjclose.index = df_ashare_adjclose["S_INFO_WINDCODE"]

        ### ！数据整理，df_ashare_ana 和 df_ashare_adjclose 两个index的股票代码顺序可能不一样
        # df_ashare_ana的股票代码可能少于 df_ashare_adjclose
        # 这样整理后，index顺序也会一致
        ### notes: df_ashare_pctchg,df_ashare_adjclose 的index是数字需要调整
        ### notes:df_ashare_ana.index长度可能比df_ashare_pctchg长,需要互相剔除不是共有的代码
        df_ashare_ana = df_ashare_ana[ df_ashare_ana["S_INFO_WINDCODE"].isin( df_ashare_pctchg["S_INFO_WINDCODE"]  )  ]
        df_ashare_pctchg = df_ashare_pctchg[ df_ashare_pctchg["S_INFO_WINDCODE"].isin( df_ashare_ana["S_INFO_WINDCODE"]  )  ]
        df_ashare_adjclose = df_ashare_adjclose[ df_ashare_adjclose["S_INFO_WINDCODE"].isin( df_ashare_ana["S_INFO_WINDCODE"]  )  ]

        df_ashare_pctchg = df_ashare_pctchg.loc[ df_ashare_ana.index ,  : ]
        df_ashare_adjclose = df_ashare_adjclose.loc[ df_ashare_ana.index ,  : ]
                
        # col_list_port组合名称列表， 是df_ashare_ana的部分columns
        col_list_port = obj_port["dict"]["col_list_port"] 
        
        #######################################################################
        ### P1 新建组合总资产 df_port_all, index是组合名称，columns是日期
        # notes:不应该用初始权重乘以每日的涨跌幅，会导致数据失真
        # 总资产、股票市值、现金、债券市值 | 行是组合名称、列是日期
        df_port_all = pd.DataFrame( index=col_list_port, columns= date_list )
        df_port_cash = pd.DataFrame( index=col_list_port, columns= date_list )
        df_port_stockvalue = pd.DataFrame( index=col_list_port, columns= date_list )
        df_port_bondvalue = pd.DataFrame( index=col_list_port, columns= date_list )
        # 股票数量、股票成本价 | 行是股票代码、列是组合名称 | 股票数量和成本不变，因此index是股票，columns是组合名称
        df_port_stock_num = pd.DataFrame( index=df_ashare_adjclose.index , columns= col_list_port )
        df_port_stock_cost = pd.DataFrame( index=df_ashare_adjclose.index , columns= col_list_port )

        count_days = 0 
        for temp_date in date_list :
            # print("Date ",temp_date )
            if count_days == 0 :
                #########################################
                ### Step 1 初始化总资产df_port_all，现金余额 df_port_cash
                df_port_all.loc[:, temp_date ] = para_asset_init
                df_port_cash.loc[:, temp_date ] = para_asset_init
                df_port_stockvalue.loc[:, temp_date ] = 0.0
                df_port_bondvalue.loc[:, temp_date ] = 0.0
                
                #########################################
                ### Step 2 根据权重，计算组合内买入股票的数量，由于部分股票复权价巨大，不要限制100股。例如贵州茅台复权价13800+
                # Notes:df1的每一列分别乘以df2的某一列，得一列一列如：df1.loc[:,0]*df2.loc[:,1 ]
                # df1 *df2.loc[:,1 ]，会导致df1每一列的全部值，乘以df2某一列里按顺序的单一数字
                for temp_port in col_list_port: 
                    print("Date and port ",temp_date , temp_port)
                    # 股票成本
                    df_port_stock_cost.loc[:, temp_port] = df_ashare_adjclose.loc[:, temp_date ]
                    # 组合权重乘以账户总资产，理论市值！
                    temp_list_mv = df_ashare_ana.loc[:, temp_port] * df_port_all.loc[temp_port, temp_date ] *para_max_stock_pct  
                    
                    # 股票数量,向下取整数
                    df_port_stock_num.loc[:, temp_port] = temp_list_mv  / df_port_stock_cost.loc[:, temp_port].astype(float )
                    ### 
                    try :
                        df_port_stock_num.loc[:, temp_port] = df_port_stock_num.loc[:, temp_port].astype(int )
                    except : 
                        print("Debug=== df_ashare_ana.loc[:, temp_port] ", df_ashare_ana.loc[:, temp_port]  )
                        print("Debug===df_port_all.loc[temp_port, temp_date ] ", df_port_all.loc[temp_port, temp_date ]  )
                        print("Debug=== df_port_stock_cost.loc[:, temp_port] ", df_port_stock_cost.loc[:, temp_port] )
                        df_port_stock_num.to_excel("D:\\df_port_stock_num0.xlsx")
                        asd 

                    # 股票市值 = 数量 乘以平均成本价
                    df_port_stockvalue.loc[ temp_port,temp_date  ] = ( df_port_stock_num.loc[:, temp_port] * df_port_stock_cost.loc[:, temp_port] ).sum()
                    # 现金变动：计算 交易成本 
                    df_port_cash.loc[temp_port,temp_date] = df_port_cash.loc[temp_port,temp_date] - (para_fee_trade + 1 ) *df_port_stockvalue.loc[ temp_port,temp_date  ]
                    # 总资产= 股票市值 + 现金
                    df_port_all.loc[temp_port,temp_date] =  df_port_stockvalue.loc[ temp_port,temp_date  ] + df_port_cash.loc[temp_port,temp_date]

                    # print("Debug;",df_port_all.loc[temp_port,temp_date], df_port_stockvalue.loc[ temp_port,temp_date  ]  , df_port_cash.loc[temp_port,temp_date] )
                ### 
                temp_date_pre =temp_date
                #
            if count_days > 0 :
                for temp_port in col_list_port: 
                    print("Date and port ",temp_date , temp_port)
                    # 股票成本不变    df_port_stock_cost.loc[:, temp_port]； 股票数量不变   df_port_stock_num.loc[:, temp_port]    
                    # 更新股票市值 = 数量 乘以 市场价
                    df_port_stockvalue.loc[ temp_port,temp_date  ] = ( df_port_stock_num.loc[:, temp_port] * df_ashare_adjclose.loc[:, temp_date ] ).sum()
                    # 现金变动：算利息
                    df_port_cash.loc[temp_port,temp_date] = df_port_cash.loc[temp_port,temp_date_pre] * (para_ret_cash_1d + 1 ) 
                    # 总资产= 股票市值 + 现金
                    df_port_all.loc[temp_port,temp_date] = df_port_stockvalue.loc[ temp_port,temp_date  ] + df_port_cash.loc[temp_port,temp_date]
                    # print("Debug;",df_port_all.loc[temp_port,temp_date], df_port_stockvalue.loc[ temp_port,temp_date  ]  , df_port_cash.loc[temp_port,temp_date] )
            
                ### 
                temp_date_pre =temp_date
            
            ### Debug
            # df_port_all.to_excel("D:\\df_port_all.xlsx")
            count_days = count_days + 1 

        #######################################################################
        ### Save to output object
        
        obj_port["dict"]["para_fee_trade"] = para_fee_trade
        obj_port["dict"]["para_asset_init"] = para_asset_init 
        obj_port["dict"]["para_ret_cash_1d"] = para_ret_cash_1d 
        obj_port["dict"]["para_max_stock_pct"] = para_max_stock_pct 
        ### 
        obj_port["dict"]["date_list"] = date_list 
        obj_port["df_port_all"] = df_port_all 
        obj_port["df_port_cash"] = df_port_cash
        obj_port["df_port_stockvalue"] = df_port_stockvalue 
        obj_port["df_port_bondvalue"] = df_port_bondvalue
        obj_port["df_port_stock_num"] = df_port_stock_num 
        obj_port["df_port_stock_cost"] = df_port_stock_cost 
        ### 
        obj_port["df_port_unit"] = df_port_all/ para_asset_init
        

        return obj_port 

    def cal_port_performance(self, obj_port) :
        ### 给定区间组合收益率，计算区间累计收益、最大回撤、超额收益、波动率、夏普比例等 
        #######################################################################
        ###  
        
        obj_date={}
        obj_date["date"]= obj_data["dict"]["date_start"]  

        return obj_port 






###################################################
class gen_portfolios():
    def __init__(self,config={},port_name=''):
        portfolio_head = self.gen_port_head(config,port_name)
        self.port_name = portfolio_head["portfolio_name"] 
        self.port_id = portfolio_head["portfolio_id"] 
        self.port_head = portfolio_head
        self.port_df = pd.DataFrame()
        # self.sp_df =sp_df
        

    def gen_port_head(self, config={},port_name='' ) :
        '''
        Generate portfolio head 
        refernce: rC_Portfolio_17Q1.py 
        previous object : log of portfolio 
        '''
        
        portfolio_head ={}
        ## get stockpool id using  time stamp
        import sys
        sys.path.append("..")
        from db.basics import time_admin
        time_admin1 = time_admin()
        time_stamp = time_admin1.get_time_stamp()

        # if config == {} :
        ## Basic info
        # initial date of generate portfolio 
        portfolio_head["InitialDate"] = "" # previous 
        portfolio_head["Index_Name"] = ""
        if port_name =='':
            portfolio_head["portfolio_name"] = str(time_stamp )
            portfolio_head["portfolio_id"] =  "id_time_" + str(time_stamp )
            portfolio_head["portfolio_id_time"] = str(time_stamp )
        else :
            portfolio_head["portfolio_name"] = port_name
            portfolio_head["portfolio_id"] =  "id_time_" + str(time_stamp)+"_name_"+port_name
            portfolio_head["portfolio_id_time"] = str(time_stamp )

        portfolio_head["MaxN"] = ""
        portfolio_head["Leverage"] = ""
        portfolio_head["date_Start"] = ""
        portfolio_head["date_LastUpdate"] = ""
        portfolio_head["path_SP"] = ""   # previous name = path_Symbol
        portfolio_head["w_equity_max"] = 0.0
        portfolio_head["w_equity_min"] = 0.0
        portfolio_head["w_bond_max"] = 0.0
        portfolio_head["w_bond_min"] = 0.0
        portfolio_head["w_cash_min"] = 0.0 # min level of cash as weight in portfolio
        portfolio_head["info"] = ""
        ## Values 
        portfolio_head["Total_Cost"] = 0.0
        portfolio_head["Cash"] = 0.0
        portfolio_head["Stock"] = 0.0
        portfolio_head["Total"] = 0.0
        portfolio_head["Unit"] = 0.0
        portfolio_head["MDD"] = 0.0
        ## profit,loss, returns,risks and other statistics 
        portfolio_head["PnL"] = 0.0
        portfolio_head["PnL_pct"] = 0.0
        portfolio_head["r_annual"] = 0.0
        portfolio_head["PnL_total"] = 0.0
        portfolio_head["PnL_Pct"] = 0.0
        portfolio_head["total_ProfitReal"] = 0.0
        portfolio_head["total_Profit_R"] = 0.0
        # statistics: max,min,mean,median
        portfolio_head["W_max"] = 0.0
        portfolio_head["W_max_code"] = 0.0
        portfolio_head["profit_max"] = 0.0
        portfolio_head["profit_max_code"] = ""
        portfolio_head["loss_max"] = 0.0
        portfolio_head["loss_max_code"] = ""
        portfolio_head["PnL_Pct_max"] = 0.0
        portfolio_head["PnL_Pct_max_code"] = ""
        portfolio_head["PnL_Pct_min"] = 0.0
        portfolio_head["PnL_Pct_min_code"] = ""
        portfolio_head["num_Trade_Profit"] = 0.0
        portfolio_head["ave_Trade_Profit"] = 0.0
        portfolio_head["total_Loss_R"] = 0.0
        portfolio_head["num_Trade_Loss"] = 0.0
        portfolio_head["total_Fees"] = 0.0
        portfolio_head["W_Ideal_max"] = 0.0
        portfolio_head["W_Ideal_max_code"] = ""
        portfolio_head["ave_Trade_Loss"] = 0.0
        # unit, cash,stocks
        portfolio_head["Unit-5D"] = {}
        portfolio_head["Cash-5D"] = {}
        portfolio_head["Stock-5D"]= {}

        return portfolio_head

    def gen_port_stat(self, sp_df,config={},port_name='' ) :
        '''output_port_suites(
        Generate portfolio using symbol list as input 
        last 181102 | since 181102     
        
        '''
        port_head = self.gen_port_head({},'') 
        import pandas as pd 
        class port():
            def __init__(self, port_name,port_head,sp_df):
                self.port_name = port_head["portfolio_name"]
                self.port_df = pd.DataFrame()
                self.portfolio_head = port_head
                self.sp_df = sp_df
        portfolio = port(port_name,port_head,sp_df)

        return portfolio


    def gen_port_suites(self,port_head,config_apps,df_sp,sp_name0,port_name) :
        ### using stockpool as input to generate portfolio 
        # 应该是不包括apps对象信息的
        # Ana:portfolio中，cash deposit/withdraw 是被动接受。
        # Ana:account 中，由于要算净值，出入金应该放account里，
        # funds是主动管理cash I/O的主体，涉及安排资金流转的功能
        ### 根据带权重的symbol list，用gen_portfolio 模块，建立初始组合，采用不复权价格。
        # 190711： var name: df_sp = temp_df_growth
        
        id_time_stamp = port_head["portfolio_id_time"]  


        ###########################################################
        ### Generate and output SP, Port., Accounts, Trades

        from db.stockpools import gen_stockpools
        # config= {}
        # sp_name0= 'growth_' + str(int_ind3)  
        config ={}
        stockpool_0 = gen_stockpools(id_time_stamp,df_sp,config,sp_name0)
        print("type of sp_df",  type( stockpool_0 )  ) 

        ###########################################################
        ### Initialize supportting configuration 
        ### 生成组合的配置文件 config_IO_0
        from config.config_IO import config_IO
        ## Import config module 
        config_IO_0 = config_IO('config_name').gen_config_IO_port('',port_name)

        import json
        file_json = stockpool_0.sp_head["id_sp"] +'.json'
        with open( config_IO_0['path_stockpools']+ file_json ,'w') as f:
            json.dump( stockpool_0.sp_head ,f) 
        file_csv = stockpool_0.sp_head["id_sp"] +'.csv'
        ### encoding with "gbk" if contain CN 
        stockpool_0.sp_df.to_csv(config_IO_0['path_stockpools']+file_csv, encoding="gbk")

        print("Stockpool has been generated ")
        print( stockpool_0.sp_head )
        print("==============================================")

        ### save to portfolio
        print("Portfolio has been generated ")
        print( self.port_head )
        print("==============================================")

        # config_IO_0['path_accounts'] == 'D:\\CISS_db\\rc001\\ports\\' 
        ###########################################################
        ### IO| Output stockpool into output file
        file_json = self.port_head["portfolio_id"] +'.json'
        with open( config_IO_0['path_ports']+ file_json ,'w') as f:
            json.dump( self.port_head ,f) 
        file_csv =  self.port_head["portfolio_id"] +'.csv'
        self.port_df.to_csv(config_IO_0['path_ports']+file_csv )

        ### generate account 
        port_df_0 = self.port_df
        from db.accounts import gen_accounts
        
        init_cash= config_apps.config['init_cash']

        # todo dates? comes from where ?
        init_date=config_apps.config['date_start'].replace("-",'') # "2014-05-31"
        account_name= config_apps.config["account_name"] # 'rc001'

        account_0 = gen_accounts(id_time_stamp,port_df_0,init_date,config,init_cash,account_name)
        print("Accounts have been generated ")
        print( account_0.account_sum.info() )
        print( account_0.account_stock.info() )
        # print( account_0.account_bond.info() )
        print("==============================================")
        ## IO| Output account into file
        file_json = account_0.account_head["account_id"] +'.json'
        with open( config_IO_0['path_accounts']+ file_json ,'w') as f:
            json.dump( account_0.account_head ,f) 
        file_csv_as = account_0.account_head["account_id"]+ '_AS' +'.csv'
        file_csv_ab = account_0.account_head["account_id"]+ '_AB' +'.csv'
        file_csv_asum = account_0.account_head["account_id"]+ '_Asum' +'.csv'
        account_0.account_sum.to_csv(config_IO_0['path_accounts']+file_csv_asum )
        account_0.account_stock.to_csv(config_IO_0['path_accounts']+file_csv_as )
        account_0.account_bond.to_csv(config_IO_0['path_accounts']+file_csv_ab )

        ###########################################################
        ### generate trade book 
        from db.trades import gen_trades 
        trades_0 = gen_trades(id_time_stamp,config_apps.config["trade_name"])  # 'rc001'
        trades_id = trades_0.tradebook_head['trades_id']
        print('Name of trading object :', trades_id )
        ## IO| Output trade book into file
        # trade head
        file_json = trades_0.tradebook_head["trades_id"] +'.json'
        with open( config_IO_0['path_trades']+ file_json ,'w') as f:
            json.dump( trades_0.tradebook_head ,f) 
        # trade book 
        file_csv_tb = trades_id+ '_TB' +'.csv'
        trades_0.tradebook.to_csv(config_IO_0['path_trades']+file_csv_tb )
        # trade statistics 
        file_csv_tb_stat = trades_id+ '_TB_stat' +'.csv'
        trades_0.tradebook_stats.to_csv(config_IO_0['path_trades']+file_csv_tb_stat )
        # trade plan
        file_csv_tp = trades_id+ '_TP' +'.csv'
        trades_0.tradeplan.to_csv(config_IO_0['path_trades']+file_csv_tp )
        # trade analyzing data/report 
        file_csv_ta = trades_id+ '_TA' +'.csv'
        trades_0.trade_ana.to_csv(config_IO_0['path_trades']+file_csv_ta )

        ### generate signal
        from db.signals import signals 
        signals_0 = signals(id_time_stamp,config_apps.config["signal_name"])  # 'rc001'
        ## IO| Output signals into output file
        file_json = signals_0.signals_head["signals_id"] +'.json'
        with open( config_IO_0['path_signals']+ file_json ,'w') as f:
            json.dump( signals_0.signals_head ,f) 
        file_csv =  signals_0.signals_head["signals_id"] +'.csv'
        signals_0.signals_df.to_csv(config_IO_0['path_signals']+file_csv )

        class portfolio_suites():
            def __init__(self,stockpool_0,account_0,trades_0, signals_0 ):
                self.stockpool = stockpool_0
                self.account = account_0
                self.trades = trades_0
                self.signals = signals_0
        portfolio_suites = portfolio_suites(stockpool_0,account_0,trades_0, signals_0)

        return portfolio_suites


###################################################
class manage_portfolios():
    ### admin: load/dump,update portfolio using given periods 

    ## import portfolio information 
    # dir: D:\CISS_db\rc001\ports
    def __init__(self,path_base, config={},port_name='',port_id=''):
        self.port_name = port_name
        # port_id can help us to locate portfolio head file 
        self.port_id = port_id
        (port_head,port_df,config_IO_0 ) = self.load_portfolio(port_id,path_base,port_name)
        self.port_head = port_head
        self.port_df = port_df
        self.config_IO_0 = config_IO_0

    def load_portfolio(self,port_id='',path_base= '',port_name='') :
        '''
        notes:current engine_portfilio does not use this module | 190109 

        load portfolio information 
        # similar with gen_port_suites(config_apps,temp_df_growth,sp_name0)
        Input： Dates, Path_TradeSys , start_Date, path_Input,
        
        Output： Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana
        reference: def Import_Portfolio_Data_Live(self, Path_TradeSys , start_Date):
        logic: there should be only one head json file in directory of "..\\ports\\",if not

        '''

        ### load configuration  
        ### "from ..config" 表示从上一级文件夹内的config文件夹读取模块 | 190109
        ### Old method to import from adjacent directory
        # sys.path.append("..") 
        # from ..config.config_IO import config_IO
        # New method to include absolute path | since 190412
        sys.path.append("C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\")
        from config.config_IO import config_IO
                
        ## Import config module 
        # 主要是组合目录和子目录信息
        config_IO_0 = config_IO('config_name').load_config_IO_port(port_id,path_base,port_name)
        ## load portfolio head and df 

        ### Get id_time_1544021284_name_port_rc181205_market_value_999.json.
        # Version concise | port_id = 1544021284
        # file_json = "id_time_"+port_id_time+"_name_"+ port_name +'.json' 
        # file_csv =  "id_time_"+port_id+"_name_"+ port_name  +'.csv'
        # Version complete 
        # port_id = id_time_1544021284_name_port_rc181205_market_value_999
        # but port_id_time  = 1544021284
        file_json = port_id +'.json' 
        file_csv =  port_id + '.csv'

        # TypeError: the JSON object must be str, bytes or bytearray, not 'TextIOWrapper'
        # with open( config_IO_0['path_ports']+ file_json ,'w') as f:
        #     port_head = json.load( f) 
        
        # Qs pd.read_json() will import empty port_head
        # port_head = pd.read_json( config_IO_0['path_ports']+ file_json ) 
        with open(config_IO_0['path_ports']+ file_json, 'r') as f: 
            port_head = json.loads(f.read())
        # type of port_head is dict  

        ### Get id_time_1544021284_name_port_rc181205_market_value_999.json
        
        port_df = pd.read_csv(config_IO_0['path_ports']+file_csv )

        return port_head,port_df,config_IO_0 

    def load_portfolio_suites(self,date_LastUpdate ,config_IO_0,port_head,port_df,port_name,sp_name0) :
        # load portfolio suites using 
        # last 181115 
        ###########################################################
        ### IO| Input stockpool into output file

        #######################################################################        
        ### IO stockpool   
        id_time_stamp = port_head["portfolio_id_time"]
        str_date = date_LastUpdate.replace("-","")
        id_sp =  "id_sp_"+id_time_stamp+"_"+sp_name0
        print("str_date ", str_date  )
        print("id_sp " , id_sp )

        class stockpool:
            def __init__(self,id_time_stamp,config_IO_0,id_sp, str_date,sp_name0) :
                # load sp_head frpm outside file 
                # sp_name0=  str(int_ind3) 
                
                # Qs :TypeError: the JSON object must be str, bytes or bytearray, not 'TextIOWrapper'
                # Ana:error json file with date is empty for lates version
                # Ans: open(~,'w' )中 "w"表示打开文件写入，但是 这里我们需要的是read，所以应该用'r'
                file_json_date = id_sp +'.json'
                # file_json_date = id_sp +'_'+str_date  +'.json'
                print( config_IO_0['path_stockpools']+ file_json_date )
                with open( config_IO_0['path_stockpools']+ file_json_date ,'r') as f:
                    # sp_head = json.loads( f )  will bring error 
                    sp_head = json.loads( f.read() ) 
                self.sp_head = sp_head
                # print("sp_head 666")
                # print( sp_head )

                file_csv_date = id_sp +'_'+str_date +'.csv' 
                sp_df = pd.read_csv( config_IO_0['path_stockpools']+file_csv_date)
                self.sp_df = sp_df
         
        # print( file_csv_date )
        # asd # 后缀加 最新日期！！！~ 
        stockpool_0 = stockpool(id_time_stamp,config_IO_0,id_sp,str_date,sp_name0)
        print("Stockpool has been import ")
        print( stockpool_0.sp_head )

        #######################################################################        
        ### IO stockpool account 
        account_name= port_name
        class accounts():
            def __init__(self,id_time_stamp,config_IO_0 ,str_date):
                id_account = "id_account_" + id_time_stamp+"_"+ account_name
                file_json_date = id_account +'_'+str_date  +'.json'
                with open( config_IO_0['path_accounts']+ file_json_date ,'r') as f:
                    account_head = json.loads( f.read() ) 
                self.account_head = account_head

                file_csv_as   = id_account + '_AS'  +'_'+str_date +'.csv'
                file_csv_ab   = id_account + '_AB'  +'_'+str_date +'.csv'
                file_csv_asum = id_account + '_Asum'+'_'+str_date +'.csv'
                account_sum   = pd.read_csv(config_IO_0['path_accounts']+file_csv_asum )
                print( account_sum.head() )
                if "Unnamed: 0" in account_sum.columns :
                    account_sum.index = account_sum["Unnamed: 0"]
                    account_sum= account_sum.drop(["Unnamed: 0"],axis =1) 
                
                account_stock = pd.read_csv(config_IO_0['path_accounts']+file_csv_as )
                # assign original index to account stock 
                if "Unnamed: 0" in account_stock.columns :
                    account_stock.index = account_stock["Unnamed: 0"]
                    account_stock= account_stock.drop(["Unnamed: 0"],axis =1) 
                account_bond  = pd.read_csv(config_IO_0['path_accounts']+file_csv_ab )
                if "Unnamed: 0" in account_bond.columns :                    
                    account_bond.index = account_bond["Unnamed: 0"]
                    account_bond= account_bond.drop(["Unnamed: 0"],axis =1) 

                account_sum.index= account_sum['date']
                self.account_sum  = account_sum
                # index should be date but 0,1,2 if imported 
                self.account_sum.index  = account_sum['date']
                self.account_stock= account_stock
                self.account_bond = account_bond


        account_0 = accounts(id_time_stamp,config_IO_0 ,str_date)

        #######################################################################        
        ### IO trade book
        trades_name = port_name 
        class trades():
            def __init__(self,trades_name, str_date): 
                # trades_id = id_time_stamp +"_"+ trades_name
                # trade head
                file_json_date = "trades_id_"+ id_time_stamp +"_"+ trades_name+"_"+ str_date +'.json'
                with open( config_IO_0['path_trades']+ file_json_date ,'r') as f:
                    tradebook_head = json.loads( f.read() ) 
                self.tradebook_head = tradebook_head

                # trade book 
                file_csv_tb = "trades_id_"+ id_time_stamp +"_"+trades_name+ '_TB'+"_"+ str_date +'.csv'
                tradebook = pd.read_csv(config_IO_0['path_trades']+file_csv_tb )
                if "Unnamed: 0" in tradebook.columns :
                    tradebook.index = tradebook["Unnamed: 0"]
                    tradebook= tradebook.drop(["Unnamed: 0"],axis =1) 

                self.tradebook = tradebook 
                # trade statistics 
                file_csv_tb_stat = "trades_id_"+ id_time_stamp +"_"+trades_name+ '_TB_stat'+"_"+ str_date +'.csv'
                tradebook_stats = pd.read_csv(config_IO_0['path_trades']+file_csv_tb_stat )
                if "Unnamed: 0" in tradebook_stats.columns :
                    tradebook_stats.index = tradebook_stats["Unnamed: 0"]
                    tradebook_stats= tradebook_stats.drop(["Unnamed: 0"],axis =1)
                self.tradebook_stats = tradebook_stats

                # trade plan
                file_csv_tp = "trades_id_"+ id_time_stamp +"_"+trades_name+ '_TP'+"_"+ str_date +'.csv'
                tradeplan = pd.read_csv(config_IO_0['path_trades']+file_csv_tp )
                if "Unnamed: 0" in tradeplan.columns :
                    tradeplan.index = tradeplan["Unnamed: 0"]
                    tradeplan= tradeplan.drop(["Unnamed: 0"],axis =1) 

                self.tradeplan = tradeplan 
                # trade analyzing data/report 
                file_csv_ta = "trades_id_"+ id_time_stamp +"_"+trades_name+ '_TA'+"_"+ str_date +'.csv'
                trade_ana = pd.read_csv(config_IO_0['path_trades']+file_csv_ta )
                if "Unnamed: 0" in trade_ana.columns :
                    trade_ana.index = trade_ana["Unnamed: 0"]
                    trade_ana= trade_ana.drop(["Unnamed: 0"],axis =1) 
                self.trade_ana = trade_ana

        trades_0 = trades(trades_name, str_date)

        #######################################################################        
        ### IO generate signal
        signals_name = port_name
        # ????要把 id_time_stamp, 放进signals.py
        class signals():
            def __init__(self,id_time_stamp,signals_name,str_date,config_IO_0) :
                # signal head 
                file_json_date = "id_signals_" + id_time_stamp +"_"+signals_name +"_"+ str_date +'.json'
                with open( config_IO_0['path_signals']+ file_json_date ,'r') as f:
                    self.signals_head = json.loads( f.read() ) 
                
                # signal df 
                file_csv_date ="id_signals_" + id_time_stamp +"_"+signals_name +"_"+ str_date +'.csv'
                signals_df = pd.read_csv(config_IO_0['path_signals']+file_csv_date )
                self.signals_df = signals_df

        signals_0 = signals(id_time_stamp,signals_name,str_date,config_IO_0)

        class portfolio_suites():
            def __init__(self,stockpool_0,account_0,trades_0, signals_0 ):
                self.stockpool = stockpool_0
                self.account = account_0
                self.trades = trades_0
                self.signals = signals_0
        portfolio_suites = portfolio_suites(stockpool_0,account_0,trades_0, signals_0)


        return portfolio_suites

    def output_port_suites(self,temp_date,portfolio_suites,config_IO_0,port_head,port_df ) :
        # sp_name0 = str(int_ind3)
        # save contents of portfolio to output csv file
        # derived from def gen_port_suites(self)
              
        stockpool_0 = portfolio_suites.stockpool
        account_0 = portfolio_suites.account 
        trades_0 = portfolio_suites.trades
        signals_0 = portfolio_suites.signals  

        # datetime to string 
        # import datetime as dt 
        # str_date = dt.datetime.strftime(temp_date,"%Y%m%d")
        str_date = temp_date.replace("-","") # 20140603
        print('str_date',str_date)
    
        #######################################################################        
        ### save stockpool 
        file_json = stockpool_0.sp_head["id_sp"] +'.json'
        with open( config_IO_0['path_stockpools']+ file_json ,'w') as f:
            json.dump( stockpool_0.sp_head ,f) 
        file_json_date = stockpool_0.sp_head["id_sp"]+'_'+str_date  +'.json'
        with open( config_IO_0['path_stockpools']+ file_json_date ,'w') as f:
            json.dump( stockpool_0.sp_head ,f) 
        
        file_csv = stockpool_0.sp_head["id_sp"] +'.csv'
        stockpool_0.sp_df.to_csv(config_IO_0['path_stockpools']+file_csv)
        file_csv_date = stockpool_0.sp_head["id_sp"]+'_'+str_date +'.csv'
        stockpool_0.sp_df.to_csv(config_IO_0['path_stockpools']+file_csv_date)
        # print( file_csv_date )
        # asd # 后缀加 最新日期！！！~ 
        print("Stockpool has been updated ")
        print( stockpool_0.sp_head )

        #######################################################################
        ### save portfolio
        ## todo at least update date of portfolio head 
        port_head["date_LastUpdate"] = str_date

        file_json = port_head["portfolio_id"] +'.json'
        with open( config_IO_0['path_ports']+ file_json ,'w') as f:
            json.dump( port_head ,f) 
        file_json_date = port_head["portfolio_id"]+'_'+str_date  +'.json'
        with open( config_IO_0['path_ports']+ file_json_date ,'w') as f:
            json.dump( port_head ,f) 

        file_csv =  port_head["portfolio_id"] +'.csv'
        port_df.to_csv(config_IO_0['path_ports']+file_csv )
        file_csv_date =  port_head["portfolio_id"]+'_'+str_date  +'.csv'
        port_df.to_csv(config_IO_0['path_ports']+file_csv_date )

        print("Portfolio has been updated ")

        #######################################################################
        ### save account into file     
        file_json = account_0.account_head["account_id"] +'.json'
        with open( config_IO_0['path_accounts']+ file_json ,'w') as f:
            json.dump( account_0.account_head ,f) 
        file_json_date = account_0.account_head["account_id"] +'_'+str_date +'.json'
        with open( config_IO_0['path_accounts']+ file_json_date ,'w') as f:
            json.dump( account_0.account_head ,f) 

        file_csv_as = account_0.account_head["account_id"]+ '_AS' +'.csv'
        file_csv_ab = account_0.account_head["account_id"]+ '_AB' +'.csv'
        file_csv_asum = account_0.account_head["account_id"]+ '_Asum' +'.csv'
        account_0.account_sum.to_csv(config_IO_0['path_accounts']+file_csv_asum )
        account_0.account_stock.to_csv(config_IO_0['path_accounts']+file_csv_as )
        account_0.account_bond.to_csv(config_IO_0['path_accounts']+file_csv_ab )

        file_csv_as_date = account_0.account_head["account_id"]    + '_AS' +'_'+str_date +'.csv'
        file_csv_ab_date = account_0.account_head["account_id"]    + '_AB' +'_'+str_date +'.csv'
        file_csv_asum_date = account_0.account_head["account_id"]+ '_Asum' +'_'+str_date +'.csv'
        account_0.account_sum.to_csv(config_IO_0['path_accounts']+file_csv_asum_date )
        account_0.account_stock.to_csv(config_IO_0['path_accounts']+file_csv_as_date )
        account_0.account_bond.to_csv(config_IO_0['path_accounts']+file_csv_ab_date )
        print("Accouts has been updated ")

        #######################################################################
        ### save trades into file
        # trade head
        print( "trades_0.tradebook_head " )
        print( trades_0.tradebook_head  ) 
        file_json = trades_0.tradebook_head["trades_id"] +'.json'
        with open( config_IO_0['path_trades']+ file_json ,'w') as f:
            json.dump( trades_0.tradebook_head ,f) 

        file_json_date = trades_0.tradebook_head["trades_id"] +'_'+str_date +'.json'
        with open( config_IO_0['path_trades']+ file_json_date ,'w') as f:
            json.dump( trades_0.tradebook_head ,f) 

        # trade book 
        trades_id = trades_0.tradebook_head['trades_id']
        file_csv_tb = trades_id+ '_TB' +'.csv'
        trades_0.tradebook.to_csv(config_IO_0['path_trades']+file_csv_tb )

        file_csv_tb_date = trades_id+ '_TB' +'_'+str_date +'.csv'
        trades_0.tradebook.to_csv(config_IO_0['path_trades']+file_csv_tb_date )
        # trade statistics 
        file_csv_tb_stat = trades_id+ '_TB_stat' +'.csv'
        trades_0.tradebook_stats.to_csv(config_IO_0['path_trades']+file_csv_tb_stat )

        file_csv_tb_stat_date = trades_id+ '_TB_stat'+'_'+str_date  +'.csv'
        trades_0.tradebook_stats.to_csv(config_IO_0['path_trades']+file_csv_tb_stat_date )
        # trade plan
        file_csv_tp = trades_id+ '_TP' +'.csv'
        trades_0.tradeplan.to_csv(config_IO_0['path_trades']+file_csv_tp )

        file_csv_tp_date = trades_id+ '_TP' +'_'+str_date +'.csv'
        trades_0.tradeplan.to_csv(config_IO_0['path_trades']+file_csv_tp_date )
        # trade analyzing data/report 
        file_csv_ta = trades_id+ '_TA' +'.csv'
        trades_0.trade_ana.to_csv(config_IO_0['path_trades']+file_csv_ta )

        file_csv_ta_date = trades_id+ '_TA'  +'_'+str_date+'.csv'
        trades_0.trade_ana.to_csv(config_IO_0['path_trades']+file_csv_ta_date )

        print("Trades has been updated ")

        # Output signals into output file
        file_json = signals_0.signals_head["signals_id"] +'.json'
        with open( config_IO_0['path_signals']+ file_json ,'w') as f:
            json.dump( signals_0.signals_head ,f) 

        file_json_date = signals_0.signals_head["signals_id"]+'_'+str_date +'.json'
        with open( config_IO_0['path_signals']+ file_json_date ,'w') as f:
            json.dump( signals_0.signals_head ,f) 

        file_csv =  signals_0.signals_head["signals_id"] +'.csv'
        signals_0.signals_df.to_csv(config_IO_0['path_signals']+file_csv )

        file_csv_date =  signals_0.signals_head["signals_id"]+'_'+str_date  +'.csv'
        signals_0.signals_df.to_csv(config_IO_0['path_signals']+file_csv_date ) 

        return portfolio_suites 



##################################################################
class manage_pms():
    ### 管理Wind-PMS组合管理模块的建仓和调仓等功能
    def __init__(self  ): 
        ### 继承父类indicators的定义，等价于 
        ################################################
        ### config_data
        from config_data import config_data
        config_data1 = config_data()
        self.path_dict = config_data1.obj_config["dict"] 
        self.path_ciss_web = self.path_dict["path_ciss_web"]
        self.path_ciss_rc = self.path_dict["path_ciss_rc"]
        self.path_db = self.path_dict["path_db"] 
        self.path_db_times = self.path_dict["path_db_times"]
        self.path_db_assets = self.path_dict["path_db_assets"]
        
        ################################################
        ### 导入数据地址          
        self.path_data_pms = self.path_dict["path_data_pms"]
        self.path_data_adj = self.path_dict["path_data_adj"]
        self.path_fundpool = self.path_dict["path_fundpool"]
        self.path_wind_terminal = self.path_dict["path_wind_terminal"]  
        self.path_wsd = self.path_dict["path_wsd"] 
        self.path_wss = self.path_dict["path_wss"] 
        self.path_wpf = self.path_dict["path_wpf"] 
        self.path_wpd = self.path_dict["path_wpd"] 
        ### 
        self.nan = np.nan 
        ###########################################
        ### 导入组合更新表格，记录每一次组合调整
        file_log = "pms_port_upload_log.xlsx"
        self.df_log= pd.read_excel( self.path_data_pms +file_log )

    def print_info(self):
        print("pms_upload | 给定df_port，生成PMS组合调整模板,权重百分比方式 ")
        print("pms_upload_trade | 给定买卖代码，生成PMS组合调整模板,交易流水方式 ")
        print(" |  ")
        
        ### 标准化工具
        print(" |  ")
        return 1 

    def pms_upload(self, obj_port):
        ### 给定df_port，生成PMS组合调整模板,权重百分比方式
        ### 根据Navigate，有4种调整方式：组合持仓、权重、流水、重置
        asset_type = obj_port["asset_type"]
        df_port = obj_port["df_port"] 
        date_latest = obj_port["date_latest"]
        port_name = obj_port["port_name"]
        ### 所有资产权重不超过95%
        df_port["weight"] = df_port["weight"]/df_port["weight"].sum() *0.95 
        ### 权益配置比例，weight_equity        
        if asset_type == "stock" :
            weight_equity = obj_port["weight_equity"] # = 0.8
            ### 只有纯股票时需要计算权益比例
            df_port["weight"] = df_port["weight"] * weight_equity
        
        
        ################################################################################################
        ### input para
        ### 例子：w.wupf("FOF偏债混合", "20220127", "600030.SH,600036.SH,10003538.SH", "0.050000,0.070000,0.010000",
        #  "0,0,0","Direction=Long,Long,Long;CreditTrading=No,No;type=weight")
        # port_name = "量化成长选股"
        if "代码" in df_port.columns :
            code_list = list( df_port["代码"])
        elif "code" in df_port.columns :
            code_list = list( df_port["code"])
        
        print("code_list:",code_list)
        weight_list = list( df_port["weight"])
        price_list_str= ""
        list_str1 = "Direction="
        list_str2 = "CreditTrading="
        list_str3 = "type=weight"
        ### 
        code_list_str = ""
        weight_list_str = ""
        for i in range( len( code_list ) ) :
            code_list_str =  code_list_str + str( code_list[i] ) +","
            ### 最多保留6位小数点；默认保留5位
            temp_w = round( weight_list[i], 4)
            weight_list_str =  weight_list_str + str( temp_w ) +","
            price_list_str = price_list_str + "0,"
            list_str1 = list_str1 + "Long,"
            list_str2 = list_str2 + "No,"
        # 去掉最后的逗号
        code_list_str =  code_list_str[:-1]
        weight_list_str =  weight_list_str[:-1]
        price_list_str = price_list_str[:-1]
        list_str1 = list_str1[:-1]
        list_str2 = list_str2[:-1]

        ### 合并尾部str
        end_str = list_str1 + ";" + list_str2 + ";" + list_str3

        # print( port_name )
        # print(  date_latest )
        # print(  code_list_str )
        # print(  weight_list_str )
        # print(  end_str )
        ###########################################
        from WindPy import w  
        w.start()
        obj_w = w.wupf( port_name, date_latest, code_list_str, weight_list_str,price_list_str, end_str)
        # print( obj_w )
        ###########################################
        ### Debug=====
        print("Return msg:",port_name, obj_w.Data )
        df1 = pd.DataFrame( obj_w.Data )
        df1.to_excel("D:\\temp.xlsx")
        ###########################################
        ### save to log file 
        temp_index = self.df_log.index.max() +1 
        self.df_log.loc[ temp_index, "port_adj_type" ] = "port_weight"
        self.df_log.loc[ temp_index, "port_name" ] = port_name
        self.df_log.loc[ temp_index, "asset_type" ] = 	asset_type		
        self.df_log.loc[ temp_index, "date_latest" ] = date_latest
        self.df_log.loc[ temp_index, "weight_sum" ] =  df_port["weight"].sum()
        if asset_type == "stock" : 
            self.df_log.loc[ temp_index, "weight_equity" ] =   obj_port["weight_equity"]

        file_log = "pms_port_upload_log.xlsx"
        self.df_log.to_excel( self.path_data_pms +file_log,index=False ) 

        return obj_port 
    
    def pms_upload_trade(self, obj_port):
        ### 给定买卖代码，生成PMS组合调整模板,交易流水方式 ")
        ###########################################
        ### 导入组合持仓
        date_latest = obj_port["date_latest"] 
        trade_type = obj_port["trade_type"] 
        port_name = obj_port["port_name"]  
        trade_type = obj_port["trade_type"]  
        code_sell = obj_port["code_sell"]  
        code_buy = obj_port["code_buy"]  

        ##########################################        
        if len( port_name ) > 2 :
            ### 单一组合
            ##########################################
            ### 只卖不买
            if trade_type == "sell_only" :
                file_port = "wpf_" + port_name + ".xlsx"
                df_port = pd.read_excel( self.path_wpf + file_port )
                ###columns= Windcode	AssetName	BeginHoldingValue	NetHoldingValue	BeginTotalCost	TotalCost	BeginPosition	Position	EUnrealizedPL
                ### 获取总资产；NetHoldingValue=市值；Position=数量
                total_mv = df_port[ "NetHoldingValue" ].sum()
                ### 正常情况下长度是1
                df_port_sub = df_port[ df_port["Windcode"]== code_sell ]
                if len( df_port_sub.index )  == 1 :
                    temp_num = df_port_sub["Position" ].values[0]
                    temp_mv = df_port_sub["NetHoldingValue" ].values[0]
                    if temp_num > 0 :
                        from WindPy import w  
                        w.start()
                        end_str =  "Direction=Long;Method=BuySell;CreditTrading=No;type=flow"
                        obj_w = w.wupf( port_name, date_latest, code_sell, str( -1*int(temp_num) ) ,"0",end_str )
                        ### Debug=====
                        print("Return msg:",port_name, obj_w.Data )
                        print("Sell value=",temp_mv, total_mv )
                        df1 = pd.DataFrame( obj_w.Data )
                        df1.to_excel("D:\\temp.xlsx")

        else :
            ##########################################
            ### 判断和调整所有组合
            ### 导入所有组合
            file_port = "wpf_" + "all" + ".xlsx"
            df_port_all = pd.read_excel( self.path_wpf + file_port )
            port_list = list( df_port_all["port_name"].drop_duplicates() )
            print("port_list:",len(port_list), port_list ) 

            from WindPy import w  
            w.start()
            ##########################################
            ### 只卖不买
            if trade_type == "sell_only" :                
                for port_name in port_list :
                    df_port = df_port_all[ df_port_all["port_name"] == port_name ]
                    
                    ### 正常情况下长度是1
                    df_port_sub = df_port[ df_port["Windcode"]== code_sell ]
                    if len( df_port_sub.index )  == 1 :
                        total_mv = df_port[ "NetHoldingValue" ].sum()
                        ###columns= Windcode	AssetName	BeginHoldingValue	NetHoldingValue	BeginTotalCost	TotalCost	BeginPosition	Position	EUnrealizedPL
                        ### 获取总资产；NetHoldingValue=市值；Position=数量
                        total_mv = df_port[ "NetHoldingValue" ].sum()
                        ### 正常情况下长度是1
                        df_port_sub = df_port[ df_port["Windcode"]== code_sell ]
                        if len( df_port_sub.index )  == 1 :
                            temp_num = df_port_sub["Position" ].values[0]
                            temp_mv = df_port_sub["NetHoldingValue" ].values[0]
                            if temp_num > 0 :
                                end_str =  "Direction=Long;Method=BuySell;CreditTrading=No;type=flow"
                                print( port_name, date_latest, code_sell, str( -1* int(temp_num) ) ,"0",end_str )
                                obj_w = w.wupf( port_name, date_latest, code_sell, str( -1* int(temp_num) ) ,"0",end_str )
                                ### Debug=====
                                print("Return msg:",port_name, obj_w.Data )
                                print("Sell value=",temp_mv, total_mv )
                                df1 = pd.DataFrame( obj_w.Data )
                                df1.to_excel("D:\\temp.xlsx")

            ##########################################
            ### 只买不卖
            if trade_type == "buy_only" :
                ### 买入权重
                weight_buy = obj_port["weight_buy"] 
                ##########################################
                ### 获取给定日期收盘价 | w.wsq("013280.OF", "rt_latest,rt_last")；现价，最新成交价
                obj1= w.wsq( code_buy , "rt_latest")
                price_latest = obj1.Data[0][0] # = 1.4614 
                for port_name in port_list :
                    df_port = df_port_all[ df_port_all["port_name"] == port_name ] 
                    ### 正常情况下长度是1
                    if len( df_port.index ) > 0 :
                        ### 获取可用现金
                        df_temp = df_port[ df_port[ "NetHoldingValue" ]== "CNY" ]
                        cash = df_temp["NetHoldingValue"].values()[0] 
                        total_mv = df_port[ "NetHoldingValue" ].sum()
                        w_max = cash*0.95/ total_mv
                        if weight_buy > w_max :
                            weight_buy = w_max
                        ### 计算买入的数量：
                        num_buy = round( total_mv*weight_buy/ price_latest,0 )
                        end_str =  "Direction=Long;Method=BuySell;CreditTrading=No;type=flow"
                        print( port_name, date_latest, code_buy, str( int(num_buy) ) ,"0",end_str )
                        obj_w = w.wupf( port_name, date_latest, code_buy, str( int(num_buy) ) ,"0",end_str )
                        ### Debug=====
                        print("Return msg:",port_name, obj_w.Data )
                        print("Sell value=",temp_mv, total_mv )
                        df1 = pd.DataFrame( obj_w.Data )
                        df1.to_excel("D:\\temp.xlsx")
            
            ##########################################
            ### 默认有卖有买
            if trade_type == "sell_buy" :
                ### 买入权重, weight_buy==""不需要
                # weight_buy = obj_port["weight_buy"]  
                ##########################################
                ### 获取给定日期收盘价 | w.wsq("013280.OF", "rt_latest,rt_last")；现价，最新成交价
                obj1= w.wsq( code_buy , "rt_latest")
                price_latest = obj1.Data[0][0] # = 1.4614 
                ### 
                port_list = [ "固收加FOF20","FOF期权9901","FOF隐形冠军","躺赢股基","躺赢FOF股基","躺赢债基","躺赢货基","躺赢FOF债基","量化成长选股"]
                
                for port_name in port_list :
                    df_port = df_port_all[ df_port_all["port_name"] == port_name ]  
                    df_port_sub = df_port[ df_port["Windcode"]== code_sell ]
                    if len( df_port_sub.index )  == 1 :
                        ##########################################
                        ### Part 1:卖出部分
                        total_mv = df_port[ "NetHoldingValue" ].sum()
                        ###columns= Windcode	AssetName	BeginHoldingValue	NetHoldingValue	BeginTotalCost	TotalCost	BeginPosition	Position	EUnrealizedPL
                        ### 获取总资产；NetHoldingValue=市值；Position=数量
                        total_mv = df_port[ "NetHoldingValue" ].sum()
                        ### 正常情况下长度是1
                        df_port_sub = df_port[ df_port["Windcode"]== code_sell ]
                        if len( df_port_sub.index )  == 1 :
                            temp_num = df_port_sub["Position" ].values[0]
                            temp_mv = df_port_sub["NetHoldingValue" ].values[0]
                            if temp_num > 0 :                                
                                ### notes:round(num_buy,0)数值还会有.0, 需要 int(x)=12
                                end_str =  "Direction=Long;Method=BuySell;CreditTrading=No;type=flow"
                                print( port_name, date_latest, code_sell, str( -1* int(temp_num)  ) ,"0",end_str )
                                obj_w = w.wupf( port_name, date_latest, code_sell, str( -1* int(temp_num)  ) ,"0",end_str )
                                ### Debug=====
                                print("Return msg:",port_name, obj_w.Data )
                                print("Sell value=",temp_mv, total_mv )
                                df1 = pd.DataFrame( obj_w.Data )
                                df1.to_excel("D:\\temp.xlsx")

                                ##########################################
                                ### Part 2:卖出后的买入部分
                                ### 卖出的现金 temp_mv
                                ### 计算买入的数量：
                                print(" temp_mv / price_latest", temp_mv , price_latest  )
                                num_buy = round( temp_mv / price_latest,0 )
                                end_str =  "Direction=Long;Method=BuySell;CreditTrading=No;type=flow"
                                print( port_name, date_latest, code_buy, str( int(num_buy) ) ,"0",end_str )
                                obj_w = w.wupf( port_name, date_latest, code_buy, str( int(num_buy) ) ,"0",end_str )
                                ### Debug=====
                                print("Return msg:",port_name, obj_w.Data )
                                print("Buy value=",temp_mv, total_mv )
                                df1 = pd.DataFrame( obj_w.Data )
                                df1.to_excel("D:\\temp.xlsx")
        
        ###########################################
        ### save to log file 
        temp_index = self.df_log.index.max() +1 
        self.df_log.loc[ temp_index, "port_adj_type" ] = "trade"
        self.df_log.loc[ temp_index, "port_name" ] = port_name 		
        self.df_log.loc[ temp_index, "date_latest" ] = date_latest  
        file_log = "pms_port_upload_log.xlsx"
        self.df_log.to_excel( self.path_data_pms +file_log,index=False ) 


        ###########################################
        ### Example：w.wupf("FOF偏债混合", "20220208", "012238.OF,011970.OF", "10000,-9000", "0","Direction=Long,Long;Method=BuySell,BuySell;CreditTrading=No,No;type=flow")
        ### w.wupf("FOF顺势轮动", "20220208", "012238.OF", "-1076770", "0","Direction=Long;Method=BuySell;CreditTrading=No;type=flow")
        return obj_port 
