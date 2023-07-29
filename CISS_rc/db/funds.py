# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
1，分析全市场基金
2，分析自建的、内部的基金
################################################
last update 220221 | since  181102
Menu :
1,class fund():自建基金对象的相关管理
2,class fund_ana():基金分析相关模块
3,class fund_exhi():基金展示相关模块
4,class fundpool():基金池和基金筛选相关模块

################################################
数据分析步骤：
    1，时间：对于更新时间t，确定对应季末时间T和上一季度末时间T-1
        1.1，时间：确定每年基金数据披露日t_report in [1,2,...,T]，给定t日，定位最近的t_report日;
        基金数据的发布时间分析：
        对于每一年，对于0131、0331、0430、0731、0830、1030六个基金数据披露截止时间，要根据披露的基金持仓
        信息补全。企业股东数据的披露截至时间是0430、0830、1030，基本上可以和上述6个对应起来。
        区间[0101,0131],[0101,0331],[0331,0430],[0630,0731],[0630,0830],[0930,1031],
            [1231,0430],[0331,0430],[0630,0830],[0930,1030],
        1.2，导入基金基础信息：导入该期披露的所有基金基础信息：代码，基金公司、基金经理、类型；
        1.3，调仓频率：基于数据披露日：季度；基于股票价格变动：月度；

    2，数据获取——基金,个股：...见“指标分析”，file=0基金持仓仿真.xlsx
    2.1，个股类数据：
        2.1.2，20050830可以得到2季度所有持仓，与之前的top10重合。
    2.2，基金类数据：
    2.3，行业类数据：28个中信一级行业组合，中信二级行业成长和价值龙头锚109*2=218个

    3，数据分析：例如：Brinson模型：行业和个股收益率的拆分；个股特征：如龙头股、权重变动；基金统计：如基金持仓抱团
        notes:：剔除新股。
    3.1，持仓个股指标：
    3.2，基金业绩和排名：例如，按照一定规则选出来的“绩优”基金；
    3.3，行业组合构建：按照市值、净利润、growth/PE构建行业组合；每个月末/季度末，计算按上月末行业分类下的当月行业分布，并统计上述组合的收益情况；

    4，指标：
    4.1，持仓指标：
        4.1.1，Brinson模型：行业和个股收益率的拆分：对于基金F1、全部基金持仓的股票
    4.2，基金收益率指标：
    4.3，行业类指标：财务、个股收益率、风格等
    4.4，市场类指标：根据市场变动，测算当前季度基金的仓位变动比例；

    5，指标和模型最优化：
    5.1，预测最新仓位，目标方程为最小化收益率误差、或股票组合的加权收益率。
    5.2，限制条件：组合调仓频率、行业配置偏离、个股配置偏离{成长、价值锚}。例如，组合调仓变动季度不超过40%，月度不超过15%；
    5.3，拟合：
        5.3.1，个股拟合
        5.3.2，行业拟合：用持仓占比较高的10个行业组合(或细分成长、价值)对基金组合进行拟合。

    6，统计分析：
    6.1，个股偏离率：估计的仓位和实际仓位的偏离情况；
    6.2，基金业绩和排名偏离率：
    6.3，交易行为：历史组合变动行为，是否低买高卖等。
################################################
Notes: 
1,200612首次实际应用该模块，为了汇总、统计和分析市场不同基金的排名和持仓数据等。
2，研究过程中我发现，定义标准，例如什么是业绩好的基金，对于分析和策略组合结果有非常重要的影响，可能类似于工艺而不是技术。
3，pd.read_csv时，可以用use_cols读取部分columns
===============================================
'''

# from re import S
import sys,os 
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

import pandas as pd
import numpy as np

#######################################################################
### 导入配置文件对象，例如path_db_wind等
from config_data import config_data_fund_ana
config_data_fund_ana_1 = config_data_fund_ana()
from data_io import data_io 
data_io_1 = data_io()
from times import times
times_1 = times()

from data_io_fund_ana import data_io_fund_ana
data_io_fund_ana_1 =  data_io_fund_ana()

###################################################
class fund():
    def __init__(self ):


        ################################################
        self.obj_config = config_data_fund_ana_1.obj_config
        self.data_io_1 = data_io()
        #######################################################################
        ### 转换后的基金wds数据表
        path_wds_fund = self.obj_config["dict"]["path_wds_fund"]
        
        ### 基金分析数据输出目录
        path_ciss_db_fund = self.obj_config["dict"]["path_ciss_db_fund"]

    def print_info(self):        
        print("TODO   ")
        ### 股票基金 
        print("  |  ")        

    def gen_fund(self, obj_fund ) :
        '''
        Generate portfolio using symbol list as input 
        last 181102 | since 181102
        Ana：
        1，the symplest method is directly using information from a single portfolio.
        2, 
        ''' 
        # generate account type 
        # 1, Open end/closed: cash withdraw/deposit 
        # 2, 出资方：社保或政府事业单位，公募基金，保险基金，券商集合，银行理财，私募，散户
        # 3,         
        # todo define specific limitation for several holders.
        '''
        fund type:
        devided by asset:
        1,equity,bond, derivatives,money market
        2,open-ended,close-ended,ETF,
        4,China non-public:
            brokerage asset managemetn
            private hedge fund 

        '''
        return obj_fund


    def gen_fund_ind_X(self,obj_fund ) :
        ### todo 
        # 根据给定的1~3级行业，抓取行业组合，根据每半年指数基本面调整后的权重分配，计算出需要调整的权重，对不同的行业组合进行申购和赎回操作。
        


        return obj_fund

    def update_fund_ind_X(self,obj_fund) :
        ### 



        return obj_fund

#########################################################################################
### 3,class fund_exhi():基金展示相关模块
class fund_exhi():
    def __init__(self ):
        ################################################
        ### Config with path 
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

        self.path_stra = self.path_dict["path_stra"] 
        
        ################################################
        self.nan = np.nan
        import datetime as dt  
        self.time_now = dt.datetime.now()
        ### 获取近1w、1m、3m、6m、1year日期
        self.time_now_str = dt.datetime.strftime(self.time_now , "%Y%m%d")
        
        #######################################################################
        ### 转换后的基金wds数据表
        self.obj_config = config_data_fund_ana_1.obj_config
        self.data_io_1 = data_io()
        self.path_wds_fund = self.obj_config["dict"]["path_wds_fund"]        
        ### 基金分析数据输出目录
        self.path_ciss_db_fund = self.obj_config["dict"]["path_ciss_db_fund"]
        
        import datetime as dt  
        self.time_now = dt.datetime.now()
        ### 获取近1w、1m、3m、6m、1year日期
        self.time_now_str = dt.datetime.strftime(self.time_now , "%Y%m%d")


    def print_info(self):        
        print(" ")
        ### 股票基金 
        print("get_fund_exhi | windapi-给定基金代码、基准、日期区间，计算区间业绩指标和基金近3年内净值,以及对应基准指数相对收益 ")    
        print("stat_fund_chg_pct | 统计不同类型基金近一周、近一个月等收益率指标前N名 ")        
        print("  |  ")   
    
    def get_fund_exhi(self, obj_f):
        ##########################################
        ### windapi-给定基金代码、基准、日期区间，计算区间业绩指标和基金近3年内净值,以及对应基准指数相对收益
        # obj_f[ "fund_code"], obj_f[ "exhi_type"] 
        # ### 获取区间开始和结束日期
        # date_begin = obj_f[ "date_begin"] 
        # date_end = obj_f[ "date_end"]  
        # obj_f["col_list_wind_api"]
        # 偏股混合型基金指数 885001.WI; bench_code = obj_f[ "bench_code"] or         
        ##########################################
        ### wind_api获取数据
        from get_wind_api import wind_api
        class_wind_api = wind_api()
        
        ####################################################################################
        ### 展示用的数据提取
        if obj_f[ "exhi_type"] == "perf" :
        ##########################################
        ### step 1 计算区间业绩指标 
            # 1,区间涨跌幅，风险 # 2,相对收益和风险
            obj_f = class_wind_api.get_wss_fund_perf( obj_f ) 
            df_perf =obj_f["df_perf"]  

            print("Debug:", df_perf )
            ### 为了方便写报告，将常用的基金指标自动连成一句话
            str_perf =df_perf.loc[0, "name_official"] +"、"
            str_perf =str_perf + "基金经理" + df_perf.loc[0, "fund_fundmanager"] +"、"
            # notes df_perf.loc[0, "fund_manager_startdate"] 已经转换成str ,'20141119'
            str_perf =str_perf + "" + df_perf.loc[0, "fund_manager_startdate"] +"任职以来、"
            str_perf =str_perf + "几何年化回报率" + str(round(df_perf.loc[0, "fund_manager_geometricannualizedyield"],2) ) +"%、"
            str_perf =str_perf + "任职以来最大回撤" + str(round(df_perf.loc[0, "fund_manager_maxdrawdown"],2) ) +"%、"
            str_perf =str_perf + "管理总资产" + str(round(df_perf.loc[0, "fund_manager_geometricannualizedyield"],2)) +"亿元。"
            obj_f["str_perf"] = str_perf
            
            ##########################################
            ### EXHI：展示数据的格式改造
            col_list_exhi = ["NAV_adj_return","risk_maxdownside"]
            col_list_exhi = col_list_exhi +["return_ytd","return_1m","return_3m","return_6m"]
            col_list_exhi = col_list_exhi + [ "fund_manager_geometricannualizedyield","fund_manager_totalnetasset","fund_manager_maxdrawdown" ]
            for temp_col in col_list_exhi :
                df_perf.loc[0, "exhi_" + temp_col ] =round( df_perf.loc[0,  temp_col ], 2)
        
        ####################################################################################
        ### 只获取基金净值
        if obj_f[ "exhi_type"] ==  "unit" :
            ##########################################
            ### step 1 判断本地文件是否存在，以及数据是否涵盖目标区间
            # 4种可能：1，本地文件不存在；2，本地结束日期<开始日期；3，本地结束日期>开始日期 且本地结束日期<结束日期；4，本地结束日期>=结束日期,已有数据不用下载
            temp_file = "wsd_" + obj_f[ "fund_code"] + ".xlsx"
            # C:\rc_2023\rc_202X\data_pms\wsd 
            print("Debug 判断本地文件是否存在=" , self.path_wsd +  temp_file)
            if os.path.exists(self.path_wsd + temp_file ) :
                ### 导入本地日期数据
                df_hist = pd.read_excel( self.path_wsd +  temp_file ) 
                
                if "Unnamed: 0" in df_hist.columns :
                    df_hist = df_hist.drop("Unnamed: 0",axis=1)
                df_hist = df_hist.sort_values(by="date",ascending=True)
                ### save to ouput obj
                obj_f["date_hist_start"] = df_hist["date"].values[0]
                obj_f["date_hist_end"] = df_hist["date"].values[-1]
                ###  pd.to_datetime() :  numpy.datetime64 转 datetime

                date_hist_start = pd.to_datetime( df_hist["date"].values[0] )
                date_hist_end = pd.to_datetime( df_hist["date"].values[-1] )
                ### 记录历史文件里的复权净值
                temp_unit_hist = df_hist["NAV_adj"].values[-1]

                ### type , class 'numpy.datetime64'>
                import datetime as dt 
                date_begin = dt.datetime.strptime( obj_f[ "date_begin"] , "%Y%m%d")
                date_end = dt.datetime.strptime( obj_f[ "date_end"] , "%Y%m%d")
                # date_hist_start  2023-01-03 00:00:00 <class 'pandas._libs.tslibs.timestamps.Timestamp'> 
                print("date_hist_start ",date_hist_start, "date_hist_end ", date_hist_end )
                print( date_begin,type(date_begin),date_end  )
                # diff= 17 days 00:00:00
                print("timedelta=" , date_hist_end - date_begin )
                ### str to datetime
                ##########################################
                ### 若 本地结束日期<结束日期, 下载【本地结束日期，结束日期 】区间数据，并合并历史数据
                if date_hist_end < date_end + dt.timedelta(days=0)  :
                    ### 下载【本地结束日期，结束日期 】区间数据
                    date_begin = obj_f[ "date_begin"]
                    obj_f[ "date_begin"] = dt.datetime.strftime( date_hist_end , "%Y%m%d") 
                    # obj_f[ "date_end"] 不变
                    obj_f["unit_type"] = "day"
                    obj_f = class_wind_api.get_wsd_fund_unit( obj_f ) 
                    ### 时间戳转日期  unsupported operand type(s) for -: 'datetime.date' and 'Timestamp'
                    obj_f["df_unit"]["date"] = obj_f["df_unit"]["date"].astype('datetime64[ns]')                     
                    
                    ##########################################
                    ### task：这时合并的df里会有2个日期为date_hist_end 的数据，而且NAV_adj列对应的净值是不同的
                    # 例如：1                     1   2022-12-21  5.724741；2   2022-12-21 00:00:00          NaT  5.399351
                    ### 合并历史数据
                    if obj_f["ErrorCode"] == 0 :
                        ### 记录下载文件里的复权净值，如果2个净值差异超过0.0001，需要根据系数调整
                        temp_unit = obj_f["df_unit"][ "NAV_adj" ][0]
                        if not abs( temp_unit_hist -  temp_unit) < 0.0001 :
                            temp_para = temp_unit/ temp_unit_hist 
                            df_hist["NAV_adj"] = df_hist["NAV_adj"] * temp_para
                        
                        ##########################################
                        ### 合并
                        df_hist = df_hist.append( obj_f["df_unit"] )                        
                                                
                        ##########################################
                        ### 创建date_shift 计算和前一行的日期的差异
                        df_hist["date_shift"] = df_hist["date"].shift(1)
                        df_hist["date_shift"] = df_hist["date"] - df_hist["date_shift"]
                        # 例如 ：   NaT 1 days 0 days 1 days || 如果报错，可能是列数据不是datetime格式。
                        # 不能剔除 NaT，因为这是第一行，需要剔除可能存在的 0 days
                        df_hist = df_hist[ df_hist["date_shift"] != dt.timedelta(days=0) ] 

                        ##########################################
                        ### 重新计算全历史净值和中断的涨跌幅
                        df_hist["NAV_adj"] = df_hist["NAV_adj"]/ df_hist["NAV_adj"].values[0]
                        df_hist["unit_fund"] = df_hist["NAV_adj"] 
                        df_hist["pct_chg_fund"] = df_hist["unit_fund"].pct_change(1)
                        df_hist["pct_chg"] = df_hist["pct_chg_fund"]

                        ##########################################
                        ### 保存到 历史数据文件
                        df_hist.to_excel("D:\\df_hist.xlsx",index=False)
                        df_hist.to_excel( self.path_wsd +  temp_file,index=False ) 

                    else :
                        print( obj_f["ErrorCode"]  )
                ##########################################
                ### 本地结束日期>=结束日期, 已有数据不用下载
                else :
                    print( "本地结束日期>=结束日期, 已有数据不用下载" )
                    df_hist.to_excel("D:\\df_hist.xlsx",index=False)
                    obj_f["df_unit"] = df_hist

            ##########################################
            ### 不存在本地文件数据，需要下载【开始日期，结束日期 】
            else : 
                # obj_f[ "date_end"] 不变
                obj_f["unit_type"] = "day"
                obj_f = class_wind_api.get_wsd_fund_unit( obj_f ) 
                ### 时间戳转日期  unsupported operand type(s) for -: 'datetime.date' and 'Timestamp'
                obj_f["df_unit"]["date"] = obj_f["df_unit"]["date"].astype('datetime64[ns]')                     
               
                if obj_f["ErrorCode"] == 0 :
                    ##########################################
                    ### 保存到 历史数据文件
                    obj_f["df_unit"].to_excel("D:\\df_hist.xlsx",index=False)
                    obj_f["df_unit"].to_excel( self.path_wsd +  temp_file,index=False ) 
                else :
                    print( obj_f["ErrorCode"]  )

            ##########################################
            ###


        return obj_f 

    def stat_fund_chg_pct(self,obj_f):
        ###  统计不同类型基金近一周、近一个月等收益率指标前N名
        # 根据日期，导入wind终端导出的、已经梳理好的基金列表
        date_fund_stat = obj_f["date_fund_stat"] 
        fund_type = obj_f["fund_type"] 
        # 需要导入2个文件：全部基金(只含主代码)-220121.xlsx ； FOF基金-220121.xlsx
        # 如果date_fund_stat只有6位数，[-6:]也可以得到 220220
        date_short = date_fund_stat[-6:]

        file_fund_all = "全部基金(只含主代码)-" + date_short + ".xlsx"
         
        df_fund = pd.read_excel( self.path_wind_terminal + file_fund_all )

        fundtype_wind =list(  df_fund["投资类型"].drop_duplicates())
        ''' fundtype_wind  ['国际(QDII)股票型基金', '商品型基金', '灵活配置型基金', '增强指数型基金',
         '国际(QDII)另类投资基金', '偏股混合型基金', '被动指数型基金', '国际(QDII)混合型基金',
          '普通股票型基金', 'REITs', '偏债混合型基金', '平衡混合型基金', '中长期纯债型基 金', 
          '短期纯债型基金', '混合债券型二级基金', '混合债券型一级基金', '被动指数型债券基金', 
          '增强指数型债券基金', '股票多空', '类REITs', '货币市场型基金', '混合型FOF基金', 
          '国际(QDII)债券型基金', '债券型FOF基金', '股票型FOF基金', nan]
        
        基金分类的对应规则：
        fundtype_s = ["主动股票","指数","偏债混合","纯债"]
        

        '''
        ################################################### 
        ### 
        if fund_type == "主动股票" :
            fund_list_sub = ["普通股票型基金","偏股混合型基金" ,"平衡混合型基金" ,"灵活配置型基金"]
            df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]

        ################################################### 
        ### 
        if fund_type == "股票指数" :
            fund_list_sub = ["被动指数型基金","增强指数型基金" ]
            df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]


        ################################################### 
        ### 偏债混合：难点：从灵活配置里边提取20%以内权益配置比例的品种。
        if fund_type == "偏债混合" :
            fund_list_sub = ["偏债混合型基金","混合债券型二级基金" ]
            df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]


        ################################################### 
        ### 
        if fund_type == "纯债" :
            fund_list_sub = ["中长期纯债型基 金","中长期纯债型基金","短期纯债型基金", "混合债券型一级基金",'被动指数型债券基金','增强指数型债券基金' ]
            df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]

        ################################################### 
        ### 
        if fund_type == "美股港股" :
            fund_list_sub = ["国际(QDII)股票型基金","国际(QDII)混合型基金" ]
            df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]

        ################################################### 
        ### 
        if fund_type == "FOF" :
            fund_list_sub = ['混合型FOF基金', '债券型FOF基金', '股票型FOF基金' ]
            df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]

        ################################################### 
        ### 
        if fund_type == "对冲reits" :
            fund_list_sub = ['REITs','股票多空', '类REITs' ]
            df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]

        ################################################### 
        ### 
        if fund_type == "商品" :
            fund_list_sub = ["商品型基金" ]
            df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]

        

        ################################################### 
        ### EXHI:部分收益率指标需要round
        df_fund_exhi = df_fund_sub
        for temp_col in ["日回报","年初至今","近一周","近一月","近一季","近一年","近半年","近一年","近两年","近三年","成立以来","年化回报" ] :
            df_fund_exhi["exhi_"+ temp_col] = df_fund_exhi[temp_col] *100
            df_fund_exhi["exhi_"+ temp_col] =df_fund_exhi["exhi_"+ temp_col].round(decimals=2 )
        
        ###
        df_fund_exhi["exhi_"+ "最新规模"] = df_fund_exhi["最新规模(亿元)"].round(decimals=2 )  
        print("Debug==", df_fund_exhi.head() )
        ################################################### 
        ### sort values
        df_fund_exhi = df_fund_exhi.sort_values(by= "近一月", ascending=False )
        df_fund_exhi = df_fund_exhi.iloc[:100, : ]
        obj_f["df_fund"] = df_fund
        obj_f["df_fund_exhi"] = df_fund_exhi

        return obj_f

#########################################################################################
### 4,class fundpool():基金池和基金筛选
class fundpool():
    def __init__(self ):
        self.obj_config = config_data_fund_ana_1.obj_config
        self.data_io_1 = data_io()
        #######################################################################
        ### Wind导出的分类基金数据
        self.path_wind_fund = self.obj_config["dict"]["path_wind_fund"]         
        
        ### 转换后的基金wds数据表
        self.path_wds_fund = self.obj_config["dict"]["path_wds_fund"]        
        ### 基金分析数据输出目录
        self.path_ciss_db_fund = self.obj_config["dict"]["path_ciss_db_fund"]


        ################################################
        ### Config with path 
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

        self.path_stra = self.path_dict["path_stra"] 

        ################################################
        self.nan = np.nan
        import datetime as dt  
        self.time_now = dt.datetime.now()
        ### 获取近1w、1m、3m、6m、1year日期
        self.time_now_str = dt.datetime.strftime(self.time_now , "%Y%m%d")

        import datetime as dt  
        self.time_now = dt.datetime.now()
        ### 获取近1w、1m、3m、6m、1year日期
        self.time_now_str = dt.datetime.strftime(self.time_now , "%Y%m%d")

    def print_info(self):        
        print(" ")
        ### 股票基金 
        print("cal_fundpool | 计算基金池：主动股票、指数、混合、纯债、对冲等 ")        
        print("fof_selection | 对FOF基金持仓的股票基金、偏股、偏债基金、债券基金进行筛选")   
        print("fund_stock_selection | 筛选不同类型基金重仓的股票")        

    def cal_fundpool(self, obj_f ) :
        ######################################################################################################
        ### 计算基金池：主动股票、偏股混合、偏债混合、纯债、FOF、QDII、对冲和商品等
        ### TODO 未来可以先用Wind--FF-"业绩表现"，分季度、半年区间初步筛选基金，然后再计算细分指标。
        ### 功能：按 业绩初步筛选--指标细筛 的步骤，筛选基金池。
        ### log：230425：Wind--FF基金索引，默认只能导出sheet=概况，而不是全部，需要改变。先用初步筛选，file=FF-基金研究-主动股票-开放申购-业绩表现-230424.xlsx
        ### notes：FF-基金索引里要用到的：概况、业绩表现、风险收益、资产配置、投资风格、交易信息。
        ### derived from test_fof_fund_pool.py
        date_fundpool = obj_f["date_fundpool"]  
        type_fundpool = obj_f["type_fundpool"] 
        ### if_purchase 取值 0 or 1
        if_purchase = obj_f["if_purchase"] 
        ########################################################


        ### notes:type_fundpool="all"时，计算所有的
        # date_short = date_fundpool[-6:]
        # print("Debug-date_fundpool-2= ",date_fundpool ) 
        
        path_output = self.path_fundpool
        ###################################################################################################### 
        if type_fundpool == "all" : 
            ### 计算4种基金池
            fund_type_list = ["主动股票","偏股混合","偏债混合","纯债","美股港股" , "FOF" , "股票指数"  ] 
        else :
            fund_type_list = [ type_fundpool ]
        ######################################################################################################
        for fund_type in fund_type_list : 
            ################################################### 
            if fund_type == "主动股票" :
                # fund_list_sub = ["普通股票型基金",]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                ### FF-基金研究-主动股票-220218.xlsx
                # dict_w ={"收益率": 0.3,"最大回撤" :0.25,"投资风格":0.00, "超额收益" :0.25,"投研团队实力": 0.15,"交易便利": 0.05} 
                dict_w ={"收益率": 0.3,"最大回撤" :0.25,  "超额收益" :0.25,"投研团队实力": 0.15,"交易便利": 0.05} 
                fundpool_type = "1"
                        
            ################################################### 
            ### 偏股混合： 
            if fund_type == "偏股混合" :
                # fund_list_sub = ["偏股混合型基金" ,"平衡混合型基金" ,"灵活配置型基金" ]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                dict_w ={"收益率": 0.45,"最大回撤" :0.25,  "超额收益" :0.1,"投研团队实力": 0.05,"交易便利": 0.05} 
                fundpool_type = "3"

                        
            ################################################### 
            ### 偏债混合：难点：从灵活配置里边提取20%以内权益配置比例的品种。
            if fund_type == "偏债混合" :
                # fund_list_sub = ["偏债混合型基金","混合债券型二级基金" ]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                dict_w ={"收益率": 0.45,"最大回撤" :0.25, "超额收益" :0.1,"投研团队实力": 0.05,"交易便利": 0.05} 
                fundpool_type = "3"


            ################################################### 
            ### 
            if fund_type == "纯债" :
                # fund_list_sub = ["中长期纯债型基 金","中长期纯债型基金","短期纯债型基金", "混合债券型一级基金",'被动指数型债券基金','增强指数型债券基金' ]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                dict_w ={"收益率": 0.45,"最大回撤" :0.25,  "超额收益" :0.1,"投研团队实力": 0.05,"交易便利": 0.05} 
                fundpool_type = "3"

            ################################################### 
            ### 
            if fund_type == "美股港股" :
                # fund_list_sub = ["国际(QDII)股票型基金","国际(QDII)混合型基金" ]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                dict_w ={"收益率": 0.3,"最大回撤" :0.25, "超额收益" :0.25,"投研团队实力": 0.15,"交易便利": 0.05} 
                fundpool_type = "1"

            ################################################### 
            ### 
            if fund_type == "FOF" :
                # fund_list_sub = ['混合型FOF基金', '债券型FOF基金', '股票型FOF基金' ]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                dict_w ={"收益率": 0.3,"最大回撤" :0.25,  "超额收益" :0.25,"投研团队实力": 0.15,"交易便利": 0.05} 
                fundpool_type = "3"

            ################################################### 
            ### 
            if fund_type == "股票指数" :
                # fund_list_sub = ["被动指数型基金","增强指数型基金" ]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                dict_w ={"收益率": 0.05,"最大回撤" :0.05,  "超额收益" :0.30,"投研团队实力": 0.25,"交易便利": 0.35} 
                fundpool_type = "2"

            ################################################### 
            ### 
            if fund_type == "对冲reits" :
                # fund_list_sub = ['REITs','股票多空', '类REITs' ]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                dict_w ={"收益率": 0.3,"最大回撤" :0.25,  "超额收益" :0.25,"投研团队实力": 0.15,"交易便利": 0.05} 
                fundpool_type = "3"

            ################################################### 
            ### 
            if fund_type == "商品" :
                # fund_list_sub = ["商品型基金" ]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                dict_w ={"收益率": 0.3,"最大回撤" :0.25, "超额收益" :0.25,"投研团队实力": 0.15,"交易便利": 0.05} 
                fundpool_type = "1"
            
            ################################################### 
            ### 货币基金没有回撤
            if fund_type == "货币" :
                # fund_list_sub = ["中长期纯债型基 金","中长期纯债型基金","短期纯债型基金", "混合债券型一级基金",'被动指数型债券基金','增强指数型债券基金' ]
                # df_fund_sub = df_fund[ df_fund["投资类型"].isin( fund_list_sub ) ]
                dict_w ={"收益率": 0.6, "超额收益" :0.1,"投研团队实力": 0.15,"交易便利": 0.15} 
                fundpool_type = "4"

            ###################################################
            ### 判断基金是否可申购，如果是要剔除含持有期的品种 | if_purchase=0 对应无限制，1对应可申购
            if if_purchase in [1,"1"] :
                ### 1对应可申购
                fund_type = fund_type + "-开放申购" 

            ################################################################################
            ### Import 导入给定类型基金的数据
            file_input = "FF-基金研究-"+fund_type + "-"+ date_fundpool[-6:] + ".xlsx"
            file_output =  "基金池rc_"+ fund_type +"_" + date_fundpool + ".xlsx"
            file_output2 =  "基金池rc_"+ fund_type  + ".xlsx"

            print("Check:" ,date_fundpool, file_input  , file_output )

            ###################################################
            ### dict_fund_data是字典，对应多张表
            ### notes:230425开始，Wind终端-FF基金索引导出的excel表格不再是包括全部表，而是每个sheet单独文件
            if int( date_fundpool ) < 20230400 :
                dict_fund_data = pd.read_excel( self.path_wind_terminal + file_input ,sheet_name=None)        
            else :
                dict_fund_data ={}
                dict_fund_data["概况"] = pd.read_excel( self.path_wind_fund + file_input ,sheet_name="概况")   
                ### notes:"获奖情况"本质上已经包含在业绩表现里，不用再导入了
                ### notes:202307 开始，wind-FF里没有 "投资风格" 了
                for temp_sheet in [ "业绩表现","风险收益","资产配置","交易信息" ] : 
                    file_input_sub =  "FF-基金研究-"+fund_type +"-"+ temp_sheet + "-"+ date_fundpool[-6:] + ".xlsx"
                    # C:\rc_2023\rc_202X\data_pms\wind_terminal\FF-基金研究
                    dict_fund_data[temp_sheet ] = pd.read_excel( self.path_wind_fund + file_input_sub ,sheet_name= temp_sheet )   
            
            
            ################################################################################
            ### 部分列改名,  dict_fund_data是字典，对应多张表 
            df_temp = dict_fund_data["概况"]

            ##############################################################################
            ### 剔除小规模基金 || 数值改造：异常值替代"--"、string，过小值等 
            df_drop = df_temp [ df_temp["规模(亿)"] == "--" ]
            df_temp = df_temp.drop( df_drop.index , axis = 0   )
            ### 230424开始，最后2行可能是空的，标注“数据来源”
            df_drop = df_temp [ df_temp["规模(亿)"] == "" ]
            df_temp = df_temp.drop( df_drop.index , axis = 0   )
            
            df_temp["规模(亿)"] = df_temp["规模(亿)"].astype(float) 
            # 规模大于 3500万
            df_temp = df_temp[ df_temp["规模(亿)"] > 0.35 ]  

            df_drop = df_temp [ df_temp["收益率（%）"] == "--" ]
            df_temp = df_temp.drop( df_drop.index , axis = 0   )
            df_temp["收益率（%）"] = df_temp["收益率（%）"].astype(float) 
            df_temp["成立年限"] = df_temp["成立年限"].astype(float) 
            ######################################
            ### notes:历史数据有可能出现 年化收益率(%)(2016年) 
            for year in [ 2016,2017,2018,2019,2020,2021 ] :
                year_str = str(year)
                temp_col = "年化收益率(%)("+ year_str + "年)"
                if temp_col in df_temp.columns: 
                    df_temp["年化收益率(%)(年初至今)"] = df_temp[ temp_col ].astype(float)  
            if "年化收益率(%)(年初至今)" in df_temp.columns: 
                df_temp["年化收益率(%)(年初至今)"] = df_temp["年化收益率(%)(年初至今)"].astype(float)  
            # 
            df_fund = df_temp  

            ################################################################################
            ### 判断是否需要用wind-API抓取指数指标

            ########################################
            ### 定义fundction，用wind-API抓取指数指标
            def get_index_track_value():
                import WindPy as wp 
                wp.w.start()
                ### 需要先获取基金对应的指数基准，再取同样基准品种抓取跟踪误差
                ### notes:不能直接获取所有基金的跟踪误差指标，那样所有基金会使用同一个默认的“000001.SH”指数作为基准，
                col_list = list( df_fund["基金代码"].values )
                obj_w1 = wp.w.wss( col_list[:3] , "fund_trackindexcode")
                col_list_bench = obj_w1.Data[0]
                # 构建一一对应的dict
                dict_code = dict( zip(col_list, col_list_bench   )  )
                
                ### Parameters
                str_start = input("输入区间开始日期，such as 20210101：")
                str_end = input("输入区间结束日期，such as 20211103：")
                indi_list = ["fund_trackerror_threshold","risk_navoverbenchannualreturn","risk_avgtrackdeviation_trackindex","risk_avgtrackdeviation_benchmark","risk_trackerror","risk_annuinforatio","risk_timeranking","risk_stockranking","risk_inforatioranking"]
                for temp_indi in indi_list :
                    df_fund[temp_indi ] = "nan"
                
                str_indi_list = "fund_trackerror_threshold,risk_navoverbenchannualreturn,risk_avgtrackdeviation_trackindex,risk_avgtrackdeviation_benchmark,risk_trackerror,risk_annuinforatio,risk_timeranking,risk_stockranking,risk_inforatioranking"
                str1 = "startDate="+str_start +";" +"endDate="+str_end +";"+"returnType=1;period=1;index="
                ### 
                for temp_i in df_fund.index : 
                    temp_code = df_fund.loc[temp_i, "基金代码" ] 
                    # "startDate=20210101;endDate=20211103;returnType=1;period=1;index=000001.SH;riskFreeRate=1;fundType=1")
                    str_all = str1 + dict_code[ temp_code ] +  ";riskFreeRate=1;fundType=1"
                    print("str_all:", str_all ) 
                    obj_w = wp.w.wss( temp_code ,str_indi_list , str_all)

                    ### save value to df 
                    for temp_j in range(0,len( indi_list ) ) :

                        df_fund.loc[temp_i, indi_list[temp_j ] ] = obj_w.Data[ temp_j  ]

                return df_fund



            ################################################################################
            ### Step 1，统计基金公司、基金经理规模|股票型基金  
            ### 计算规模加权收益率
            df_temp["aum_ret"] = df_temp["规模(亿)" ] * df_temp["收益率（%）" ] 
            df_comp0 = df_temp.groupby("管理人" )["规模(亿)","aum_ret" ].sum()
            ### 取平均值
            df_comp = df_temp.groupby("管理人" )["收益率（%）","成立年限" ,"年化收益率(%)(年初至今)" ].mean()
            # pd.merge(df1,df2)
            df_comp["规模(亿)"] = df_comp0["规模(亿)"]
            df_comp["规模加权收益率"] = df_comp0["aum_ret"] / df_comp0["规模(亿)"]

            ###########################################
            ### 基金经理汇总 
            df_manager0 = df_temp.groupby("基金经理" )["规模(亿)","aum_ret" ].sum()
            ### 取平均值
            df_manager = df_temp.groupby("基金经理" )["收益率（%）","成立年限" ,"年化收益率(%)(年初至今)" ].mean()
            # pd.merge(df1,df2)
            df_manager["规模(亿)"] = df_manager0["规模(亿)"]
            df_manager["规模加权收益率"] = df_manager0["aum_ret"]/ df_manager0["规模(亿)"]


            ###########################################
            ### 对于每一只基金，将基金公司数据赋值 
            # notes: rank() 数值最小的是1，最大的是2876
            num_fund = len( df_fund.index  )
            df_fund["基金公司规模"] = df_fund["管理人"].apply( lambda x : df_comp.loc[x ,"规模(亿)"  ]  )
            df_fund["pct_基金公司规模"] =  df_fund[ "基金公司规模" ].rank()  / num_fund

            ## 业绩的几种计算方式：规模加权收益率、平均收益率、前25%分位收益率
            df_fund["基金公司业绩"] = df_fund["管理人"].apply( lambda x : df_comp.loc[x ,"规模加权收益率"  ]  )
            df_fund["pct_基金公司业绩"] =  df_fund[ "基金公司业绩" ].rank()  / num_fund

            # df_fund["基金公司业绩"] = df_fund["管理人"].apply( lambda x : df_comp.loc[x ,"收益率（%）"  ]  )
            # df_fund["pct_基金公司业绩"] = df_fund[ "基金公司业绩" ].rank()  / num_fund

            #######################################
            ### 剔除C类基金重复项, 因为C类的规模需要加到基金公司规模中
            df_fund["end_char"] = df_fund["基金名称"].str[-1]
            df_fund["begin_char"] = df_fund["基金名称"].str[:-1]
            df_fund_C = df_fund[ df_fund["end_char"] == "C" ]  
            df_fund = df_fund.drop( df_fund_C.index , axis=0 )

            ### 将C类和A类对应起来
            for temp_i in df_fund_C.index: 
                temp_name = df_fund_C.loc[temp_i, "begin_char" ]
                ### try to find 
                df_s = df_fund[ df_fund["begin_char"] == temp_name  ]
                if len( df_s.index ) >0  : 
                    df_fund_C.loc[temp_i, "code_others" ] = ",".join( df_s["基金代码"].values  )
                    df_fund.loc[ df_s.index , "code_C" ] = df_fund_C.loc[temp_i, "基金代码" ]


            ################################################################################
            ### Step 2，ranking - 收益率   近3月	40%；近6月	40%；近1年	20%
            # pd.merge(df1, df2, left_on='column_name', right_on='column_name') # 文件合并，left_on=左侧DataFrame中的列；right_on=右侧DataFrame中的列

            # notes:"近3月"对应收益率,"Unnamed: 7"对应近3个月排名；"近6月"对应收益率,"Unnamed: 9"对应近6个月排名
            df_perf = dict_fund_data["业绩表现"].loc[:,["基金代码","近3月","近6月","近1年" ] ]
            # notes: left_on="基金代码", right_on="基金代码" 会导致合并后的df有2个columns:"基金代码_x",""基金代码"_y"
            df_fund = pd.merge( df_fund, df_perf  , on="基金代码" )

            ################################################################################
            ### Step 3，ranking - 风险 和 超额收益 | 都在一张表里
            # 风险: 历史最大回撤	100%； 波动率（%）	30%； 下行风险（%）	70%
            # 最大回撤已经在 df_fund 里了
            # 超额收益 ：Alpha（%）	30%；Sharpe	20%；Sortino	30%；Calmar	20%
            df_risk = dict_fund_data["风险收益"] 
            df_fund = pd.merge( df_fund, df_risk  , on="基金代码" )

            ################################################################################
            ### Step 4，ranking - 投资风格	rank-roe	50%；	rank-换手率	50%
            # df_style = dict_fund_data["投资风格"].loc[:,["基金代码","重仓ROE（%）","换手率（%）" ] ] 
            # notes:202307 开始，wind-FF里没有 sheet="投资风格" 了

            # df_style = dict_fund_data["投资风格"].loc[:,["基金代码","换手率（%）" ] ] 
            # df_fund = pd.merge( df_fund, df_style  , on="基金代码" )
            # # notes: "换手率（%）" 在季报期可能没有数值； "重仓ROE（%）"wind说暂时不提供
            # df_fund["换手率（%）"]=df_fund["换手率（%）"].replace("--",-1)

            ################################################################################
            ### Step 5 ranking 投研团队实力	：基金公司收益率排名	60%,pct_基金公司规模	30%；	成立年限	10%
            # df_award = dict_fund_data["获奖情况"].loc[:,["基金代码","获奖次数" ] ]
            # df_fund = pd.merge( df_fund, df_award , on="基金代码" )

            ################################################################################
            ### Step 6 ranking 交易便利	： 基金规模	50%；	管理费率	50%
            df_trade = dict_fund_data["交易信息"].loc[:,["基金代码","管理费率(%)","托管费率(%)" ] ]
            ### notes:220630股票基金880006.OF出现过管理费"--",需要进行替代
            df_trade["管理费率(%)"] = df_trade["管理费率(%)"].replace("--", 2.0 )
            df_trade["托管费率(%)"] = df_trade["托管费率(%)"].replace("--", 0.3 )
            df_trade["fee"] = df_trade["管理费率(%)"] + df_trade["托管费率(%)"] 
            df_fund = pd.merge( df_fund, df_trade , on="基金代码" )

            ################################################################################
            ### Step 7 其他备用指标：资产配置：| 和基金池计算无关
            col_list= ["股票占净值比（%）","债券占净值比（%）","重仓股","重仓债" ]
            df_holding = dict_fund_data["资产配置"].loc[:,["基金代码"]+ col_list ]
            for temp in col_list :
                df_holding[temp] = df_holding[temp].replace("--", "" )
            
            df_fund = pd.merge( df_fund, df_holding , on="基金代码" )


            ###################################################
            ### 判断基金是否可申购，如果是要剔除含持有期的品种 | if_purchase=1
            
            if if_purchase in [1,"1"] :
                ### "申购限额(万)" 剔除 "--",对应限制大额申购
                temp_index = df_fund[ df_fund["申购限额(万)"] == "--"].index
                df_fund=df_fund.drop(temp_index ,axis=0) 

                ### "申购限额(万)" 将 "无限制" 替换为 1000000 
                index1 = df_fund[ df_fund["申购限额(万)"] == "无限制"].index                
                df_fund.loc[index1,"申购限额(万)"] = 1000000
                
                ### 筛选 申购金额在 500万以上的品种
                df_fund = df_fund[ df_fund["申购限额(万)"] >= 500 ]
                
                ### 对于 df_fund，剔除含持有期的品种
                str_list=["滚动","持有","定开","个月","年"]
                address="|".join(str_list)
                df_fund =df_fund[~ df_fund["基金名称_x"].str.contains(address) ]
                               
                ### debug
                df_fund.to_excel("D:\\debug.xlsx") 


            ################################################################################
            ### Part 2，数据分析
            ################################################################################
            ### Step 0，对每个指标进行排序、标准化打分、加权合并
            ### 数值改造：异常值替代"--"、string，过小值等；对于部分列，需要把非数值部分改为数值
            # 一定要有的数据： 近3月 近6月   近1年；Sharpe Sortino；  | notes:Sharpe Sortino基本一定会有；重仓ROE（%）  换手率（%）暂时没有
            # 数值可以为空的：Alpha（%）   Calmar     波动率（%）   下行风险（%） | notes:Alpha（%）   Calmar,基金业绩不到1年没有
            # notes: "换手率（%）" 在季报期可能没有数值； "重仓ROE（%）"wind说暂时不提供
            for temp_str in ["近3月","近6月","近1年","Sharpe","Sortino","成立年限","规模(亿)" ] :
                df_drop = df_fund [ df_fund[ temp_str ] == "--" ]
                df_fund = df_fund.drop( df_drop.index , axis = 0   )    
                df_fund[ temp_str ] = df_fund[ temp_str ].astype(float)  
            # 211103：近1年2282只基金业绩大于1年， 594只基金小于1年。

            # for temp_str in ["Alpha（%）","Calmar","获奖次数"] :
            for temp_str in ["Alpha（%）","Calmar"] :
                # 数值越大越好， 缺失值用最小值代替 
                try : 
                    v_min = df_fund[ temp_str ].min()  
                except : 
                    v_min = -1 
                # notes: to_replace 是被替代的数据
                df_fund[ temp_str ]  =df_fund[ temp_str ].replace( to_replace="--"  , value=v_min )
                

            for temp_str in [ "波动率（%）","下行风险（%）","fee"] :
                # 数值越小越好， 缺失值用最大值代替；因为数值没有负号
                df_fund[ temp_str ]  =df_fund[ temp_str ].replace( to_replace= "--", value=df_fund[ temp_str ].max() ) 

            ###########################################
            ### 替代极端值，
            def df_replace_pct(df_fund,col_list, para_head,para_tail ):
                ### 替换1%极端值
                # df_fund.quantile(0.99,axis=0) 类型是series；转df格式后 0.99 是columns
                # 参数para_head对应前1%百分位数值，0.01对应后99%数值，默认是数值越大越好
                df_head = pd.DataFrame( df_fund.quantile( para_head ,axis=0) ) 
                # print(  df_head.loc["近6月" , 0.99] )
                df_tail = pd.DataFrame( df_fund.quantile( para_tail ,axis=0) )

                ### 函数 替换百分位
                # def replace_pct( list_value , v_head, v_tail ): 
                #     return list_value
                
                for temp_col in col_list : 
                    # print("Debug temp_col\n" ,temp_col ,  df_head  , df_tail )
                    # Apply函数：df.apply()时，axis = 1，一行数据作为Series，用于计算比如求和sum； 对一列操作axis=0 
                    v_head =  df_head.loc[temp_col , para_head]
                    v_tail =  df_tail.loc[temp_col , para_tail]
                    df_fund[ temp_col ] = df_fund[ temp_col ].apply( lambda x : v_head if x>= v_head else (v_tail if x <= v_tail else x )  )


                return df_fund 

            ### col_list_upward:col越大越好， col_list_downward 越小越好 || ,"获奖次数" 不再使用
            col_list_upward = ["近3月","近6月","近1年","Sharpe","Sortino","Alpha（%）","Calmar","成立年限","最大回撤"] 
            if fundpool_type == "4" :
                ### 货币3 |notes:货币基金没有最大回撤
                col_list_upward = ["近3月","近6月","近1年","Sharpe","Sortino","Alpha（%）","Calmar","成立年限" ] 

            # col_list_downward =["换手率（%）","波动率（%）","下行风险（%）","fee","规模(亿)"]
            col_list_downward =["波动率（%）","下行风险（%）","fee","规模(亿)"]
            col_list = col_list_upward + col_list_downward
            para_head = 0.99
            para_tail = 0.01
            # df_fund.to_excel(  "D:\\test.xlsx" ,sheet_name="基础池计算",encoding="gbk"  )    
            df_fund = df_replace_pct(df_fund,col_list, para_head,para_tail) 

            ###########################################
            ### 去排名；标准化打分
            num_fund = len( df_fund.index )
            for temp_col in col_list_upward :
                # notes: rank() 数值最小的是1，最大的是2876；对应最小的值是最大的排序数字，因此不需要处理 
                df_fund["pct_"+ temp_col ] =  df_fund[temp_col ].rank()   / num_fund 

            for temp_col in col_list_downward  :
                # notes: rank() 数值最大的是1，最小的是2876；col_list_downward需要反过来处理一下
                df_fund["pct_"+ temp_col ] =  ( num_fund + 1 - df_fund[temp_col ].rank() ) / num_fund


            
            ################################################################################
            ### Part 3，加权打分
            ###########################################
            ### 加权合并 
            # notes： 之前已经计算过了的 ：	pct_基金公司业绩 ,50%,pct_基金公司规模	30% 
            col_list_upward = ["近3月","近6月","近1年","Sharpe","Sortino","Alpha（%）","Calmar","成立年限","基金公司业绩","基金公司规模"] 
            # col_list_downward =["换手率（%）","波动率（%）","下行风险（%）","fee","规模(亿)"]
            col_list_downward =["波动率（%）","下行风险（%）","fee","规模(亿)"]

            '''收益率	近3月	40%	近6月	40%	近1年	20%
            最大回撤	历史最大回撤 60%	波动率（%）	10%	下行风险（%）	30%
            超额收益	Alpha（%）	30%	Sharpe	20%	Sortino	30%	Calmar	20%
            投资风格	rank-roe	50%	rank-换手率	50%
            投研团队实力	基金公司收益率排名	60%	基金公司规模排名	30%	成立年限	10% || 获奖次数 因与长期业绩重复、数据麻烦不用。
            交易便利	基金规模	50%	管理费率	50%
            '''
            ### 
            df_fund["score_"+ "收益率" ] = df_fund["pct_" +"近3月" ]*0.4 +  df_fund["pct_" +"近6月" ]*0.4 +  df_fund["pct_" +"近1年" ]*0.2

            if fundpool_type == "4" :
                ### 货币4 |notes:货币基金没有最大回撤
                df_fund["score_"+ "最大回撤" ] =   df_fund["pct_" + "波动率（%）" ]*0.3 +  df_fund["pct_" +"下行风险（%）" ]*0.7
            else :
                df_fund["score_"+ "最大回撤" ] = df_fund["pct_" + "最大回撤" ]*0.6 +  df_fund["pct_" + "波动率（%）" ]*0.1 +  df_fund["pct_" +"下行风险（%）" ]*0.3

            df_fund["score_"+ "超额收益" ] = df_fund["pct_" +"Sharpe" ]*0.2 +  df_fund["pct_" +"Sortino"]*0.3 +  df_fund["pct_" +"Alpha（%）" ]*0.3 +  df_fund["pct_" +"Calmar"  ]*0.2
            # notes: roe 暂时没有数据；"换手率（%）"季度没有数据
            # df_fund["score_"+ "投资风格" ] = df_fund["pct_" +"换手率（%）" ]
            df_fund["score_"+ "投资风格" ] = 0.0

            df_fund["score_"+ "投研团队实力" ] = +  df_fund["pct_" +"成立年限" ]*0.1 +  df_fund["pct_" +"基金公司业绩"]*0.6 +  df_fund["pct_" + "基金公司规模" ]*0.3
            df_fund["score_"+ "交易便利" ] = df_fund["pct_" + "规模(亿)"]*0.5 +  df_fund["pct_" +"fee" ]*0.5

            ########################################
            ### dict_w 是各项基金分析各项指标的权重
            # dict_w ={} 
            # dict_w["收益率"] = 0.3
            # dict_w["最大回撤"  ] = 0.25
            # # dict_w["投资风格" ] = 0.05
            # dict_w[ "超额收益" ] = 0.25
            # dict_w["投研团队实力"  ] = 0.15
            # dict_w["交易便利"] = 0.05

            df_fund["score" ] = 0.0 

            if fundpool_type == "4" :
                ### 货币4 |notes:货币基金没有最大回撤
                col_list_score = ["收益率", "超额收益",  "投研团队实力" ,"交易便利"]
            else :
                col_list_score = ["收益率","最大回撤", "超额收益",  "投研团队实力" ,"交易便利"]
            # for temp_col in :
            for temp_col in col_list_score :
                df_fund["score" ] =df_fund["score" ] + df_fund["score_"+ temp_col ] * dict_w[temp_col]

            df_fund["pct_score" ] =  df_fund[ "score" ].rank()   / num_fund 

            df_fund=df_fund.sort_values(by="score",ascending=False )
            print("前十名基金 \n ", df_fund.head(10).T )


            ################################################################################
            ### Part 4，EXHI

            ### step 0，基础池和核心池数量
            if fundpool_type in ["1","2","3" ] :
                # 货币和FOF数量少
                len_025 = int(  max(100, round( num_fund*0.25 ,0 ) ) )
                print("基础池基金数量：",len_025 )
                df_fundpool = df_fund.head( len_025  )

                len_005 = int( max(100 , round( num_fund*0.05 ,0 ) ) )
                print("核心池基金数量：",len_005 )
            if fundpool_type in ["4","5","6"  ] :
                # 货币和FOF数量少
                len_025 = int(  max(100, round( num_fund*0.25 ,0 ) ) )
                print("基础池基金数量：",len_025 )
                df_fundpool = df_fund.head( len_025  )

                len_005 = int( min(100 , round( num_fund*0.05 ,0 ) ) )
                print("核心池基金数量：",len_005 )


            
            #########################################
            ### step 1，保留部分columns，并更名
            # "基金代码","基金名称_x","类型","收益率（%）","同类排名","年化收益率(%)(年初至今)","七日年化(%)","申购限额(万)","最大回撤",
            # "规模(亿)","成立年限","基金经理","管理人","综合评级","基金公司规模","pct_基金公司规模","基金公司业绩","pct_基金公司业绩",
            # "近3月","近6月","近1年","基金名称_y","Alpha（%）","Sharpe","Sortino","Calmar","波动率（%）","下行风险（%）","重仓ROE（%）",
            # "换手率（%）","管理费率(%)","托管费率(%)","fee",
            # "pct_近3月","pct_近6月","pct_近1年","pct_Sharpe","pct_Sortino","pct_Alpha（%）","pct_Calmar","pct_成立年限",
            # "pct_最大回撤","pct_换手率（%）","pct_波动率（%）","pct_下行风险（%）","pct_fee","pct_规模(亿)",
            # "score_收益率","score_最大回撤","score_超额收益","score_投资风格","score_投研团队实力","score_交易便利","score","pct_score"
            col_list_keep = [ "基金代码","基金名称_x","类型","近3月","近6月","近1年", "score_收益率","score_最大回撤","score_超额收益","score_投资风格","score_投研团队实力","score_交易便利","score","pct_score","code_C" ]
            col_list_replace = [ "基金代码","基金名称","类型","近3月收益率","近6月收益率","近1年收益率", "收益率得分","最大回撤得分","超额收益得分","投资风格得分","投研团队实力得分","交易便利得分","总分","总分百分位","C类基金代码" ]

            ### 仅保留部分列
            df_fundpool = df_fundpool.loc[:, col_list_keep ]
            # notes: zip(list1,list2)本身不是dict需要转换 。list1会变成keys，list2变成values
            dict_column = dict( zip(col_list_keep, col_list_replace)  )
            ### 部分列改名
            df_fundpool = df_fundpool.rename( columns=dict_column )

            ### step 2，小数点
            # todo： 百分位乘以100 
            for temp_col in ["收益率得分","最大回撤得分","超额收益得分","投资风格得分","投研团队实力得分","交易便利得分","总分","总分百分位"  ] :
                df_fundpool[temp_col] = df_fundpool[temp_col] *100

            ### 保留1位小数的
            col_list_1 = [ "收益率得分","最大回撤得分","超额收益得分","投资风格得分","投研团队实力得分","交易便利得分","总分","总分百分位"  ]
            df_fundpool.loc[:, col_list_1 ] = df_fundpool.loc[:, col_list_1 ].round(1)

            ### 保留2位小数的
            col_list_2 = [ "近3月收益率","近6月收益率","近1年收益率"]
            df_fundpool.loc[:, col_list_2 ] = df_fundpool.loc[:, col_list_2 ].round(2)

            ### 基金公司 df_comp | 
            df_comp = df_comp.round(2)
            col_list_keep = ["规模加权收益率", "收益率（%）","成立年限","年化收益率(%)(年初至今)","规模(亿)" ]
            col_list_replace = [ "规模加权收益率","平均收益率","平均成立年限","平均年化收益率(%)(年初至今)","总规模(亿)" ]
            # notes: zip(list1,list2)本身不是dict需要转换 。list1会变成keys，list2变成values
            dict_column2 = dict( zip(col_list_keep, col_list_replace)  )
            ### 部分列改名
            df_comp = df_comp.rename( columns=dict_column2 )

            ########################################################################################
            ### 保存多个df到excel-sheet文件
            for temp_file in [file_output,file_output2] :
                writer_excel = pd.ExcelWriter( path_output + temp_file )

                ### 基金产品
                df_fundpool.head( len_005  ).to_excel( writer_excel ,sheet_name="核心池",encoding="gbk"  ) 
                df_fundpool.to_excel( writer_excel ,sheet_name="基础池",encoding="gbk"  ) 

                ### 基金公司
                df_comp.to_excel( writer_excel ,sheet_name="基金公司",encoding="gbk"  )#  

                ### 基金经理 | 乘以股票比例？
                df_manager.to_excel( writer_excel ,sheet_name="基金经理",encoding="gbk"  )#  

                ### 统计，原始数据
                df_fundpool.head( len_005  ).describe().to_excel( writer_excel ,sheet_name="核心池统计",encoding="gbk"  ) 
                df_fundpool.describe().to_excel( writer_excel ,sheet_name="基础池统计",encoding="gbk"  ) 
                df_comp.describe().to_excel( writer_excel ,sheet_name="基金公司统计",encoding="gbk"  )
                df_manager.describe().to_excel( writer_excel ,sheet_name="基金经理统计",encoding="gbk"  )
                ### 原始数据和C类基金
                df_fund.to_excel( writer_excel ,sheet_name="raw_data",encoding="gbk"  ) 
                df_fund_C.to_excel( writer_excel ,sheet_name="C类基金",encoding="gbk"  ) 

                writer_excel.save()
                writer_excel.close()



        return obj_f


    def fof_selection(self,obj_f):
        ####################################################################################
        ### 对FOF基金持仓的股票基金、债券基金进行筛选
        ### derived from FOF持仓选基,file=test_pms_manage.py 
        ### Input:date_latest;quarter_end
        date_latest = obj_f["date_latest"]
        # 20211231
        quarter_end = obj_f["quarter_end"]
        ### 绝大部分FOF类型是混合型FOF基金 ； 股票FOF、债券FOF
        # fof_type = obj_f["fof_type"]
        ### stockfund,mixfund,bondfund
        selection_type = obj_f["selection_type"] 
        ### values: fund_stock,主动股票;fund_mixed_stock,偏股混合;fund_mixed_bond,偏债混合;
        # fund_bond,纯债;fund_hkus,QDII港股美股
        

        ####################################################################################
        ### step 1，根据FOF类型，筛选FOF业绩和回撤排名前10-20%的FOF基金
        # file= 基金池rc_FOF_20220308.xlsx or 基金池rc_FOF-开放申购_20230424.xlsx
        if "str_purchase" in obj_f.keys() :
            file_name = "基金池rc_FOF" + obj_f["str_purchase"]  + "_"+ date_latest + ".xlsx"
            
        else :
            file_name = "基金池rc_FOF_"+ date_latest + ".xlsx"
        df_fof = pd.read_excel( self.path_fundpool+file_name, sheet_name="raw_data" )
        
        ### 已经是YTD收益率降序排列
        ##############################
        ### 对收益率和回撤进行排序打分
        df_fof = df_fof.sort_values(by="score",ascending=False )
        df_fof_top = df_fof.iloc[:20, :]

        print("df_fof_top, ", df_fof_top.head().T ) 
        
        
        ##############################################################
        ### step 2，获取FOF季度末重仓基金   
        ##########################################
        ### 只保留特定类型基金
        ### values: fund_stock,主动股票;fund_mixed_stock,偏股混合;fund_mixed_bond,偏债混合;
        # fund_bond,纯债;fund_hkus,QDII港股美股
        if selection_type == "fund_stock" :
            list_fund_type = ["普通股票型基金"]
        elif selection_type == "fund_bond" :
            list_fund_type = ["短期纯债型基金","中长期纯债型基金","混合债券型一级基金" ]
        elif selection_type == "fund_mixed_stock" :
            list_fund_type = ["偏股混合型基金","平衡混合型基金","灵活配置型基金","平衡混合型基金" ]
        elif selection_type == "fund_mixed_bond" :
            list_fund_type = ["混合债券型二级基金","偏债混合型基金"]
        ### 港股美股还比较少基金持有
        # elif selection_type == "fund_hkus" :
        #     list_fund_type = ["国际(QDII)股票型基金","国际(QDII)混合型基金" ]
        # elif selection_type == "fof" :
        #     list_fund_type = ["股票型FOF","债券型FOF","混合型FOF" ]
        
        ###  
        file_name = "重仓基金(明细)-"+ quarter_end + ".xlsx"
        df_fundlist_top10 = pd.read_excel( self.path_wind_terminal+file_name  )
        
        df_fundlist_fof = df_fundlist_top10[ df_fundlist_top10["投资类型"].isin( list_fund_type  )  ]

        print("df_fundlist_fof ")
        print( df_fundlist_fof.head() )
        ##########################################
        ### 寻找每个fof对应的持仓基金
        count_code = 0 
        for temp_code in  df_fof_top["基金代码"] : 
            ### find holding funds of temp_code 
            df_sub = df_fundlist_fof[ df_fundlist_fof["代码"]==temp_code ]
            if len( df_sub.index ) >0 :
                if count_code == 0  :
                    df_holding_funds = df_sub
                    count_code = 1
                else :
                    df_holding_funds = df_holding_funds.append(df_sub, ignore_index= True)

        df_holding_funds.to_excel( "D:\\df_holding_fund_s.xlsx" )

        ##########################################
        ### 去除重复项
        df_holding_funds["change_pct"] = df_holding_funds["季度持仓变动(万份)"] / df_holding_funds["基金总规模(亿元)"]  
        # "持仓份额(万份)","季度持仓变动(万份)","持仓市值(万元)","占基金净值比(%)","占基金市值比(%)","基金总规模(亿元)","占基金总规模比(%)"
        df_funds = df_holding_funds.groupby( "基金代码" )["持仓份额(万份)","change_pct","持仓市值(万元)","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"].sum()
        # "change_pct","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"
        for temp_col in ["change_pct","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"] :
            df_funds["s_"+ temp_col ] = df_funds[temp_col] / df_funds[temp_col].sum()
        ### 分配权重
        df_funds["s_sum" ] = df_funds["s_"+ "change_pct"] *0.7
        for temp_col in ["占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"] :
            df_funds["s_sum" ] = df_funds["s_sum" ] + df_funds["s_"+ temp_col ] *0.1
        
        ### 取前20%基金
        df_funds = df_funds.sort_values(by="s_sum",ascending=False  ) 
        num_top20pct = max(20, int( len(df_funds.index )*0.2 )  ) 
        df_funds = df_funds.iloc[:num_top20pct,: ]

        ### 剔除负值
        df_funds = df_funds[ df_funds["s_sum"] > 0  ] 

        df_funds["code"] = df_funds.index
        ##########################################
        ### 计算权重
        ### 控制单只基金权重不超过10% 
        df_funds[ "weight" ] = df_funds[ "s_sum" ]/df_funds[ "s_sum" ].sum()
        # 
        df_funds["w_diff"] = df_funds["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_funds["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_funds["temp"] = df_funds["weight"].apply(lambda x : x if x <0.085 else 0.0 )
        temp_sum = df_funds["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_funds[ df_funds["w_diff"] <0.085 ]
        df_funds.loc[ df_sub.index, "weight" ] = df_funds.loc[ df_sub.index, "weight" ] * para_sum
        df_funds["weight"] = df_funds["weight"].apply(lambda x : 0.095 if x >0.095 else x )
               
        ##########################################
        ### 获取持仓基金的名称
        for temp_code in df_funds.index: 
            ### find name in df_holding_funds
            df_sub = df_holding_funds[ df_holding_funds["基金代码"] == temp_code ]
            df_funds.loc[temp_code,"名称"  ] = df_sub["基金名称"].values[0]
        ### notes:FOF名称 = "名称"  ； FOF持仓基金的名称="基金名称"

        ##########################################
        ### EXHI
        ### 持仓份额(万份)	change_pct	持仓市值(万元)	占基金净值比(%)	占基金市值比(%)	占基金总规模比(%)
        df_funds["exhi_weight"  ] = df_funds["weight"]*100
        df_funds["exhi_weight"  ] = df_funds["exhi_weight"].round(decimals=2)
        df_funds["exhi_持仓份额"  ] = df_funds["持仓份额(万份)"]
        df_funds["exhi_持仓份额"  ] = df_funds["exhi_持仓份额"].round(decimals=1)
        df_funds["exhi_持仓变动百分比"  ] = df_funds["change_pct"]
        df_funds["exhi_持仓变动百分比"  ] = df_funds["exhi_持仓变动百分比"].round(decimals=1 )
        # 持仓市值(万元)
        df_funds["exhi_持仓市值"  ] = df_funds["持仓市值(万元)"]
        df_funds["exhi_持仓市值"  ] = df_funds["exhi_持仓市值"].round(decimals=1 )
        # 占基金净值比(%) 
        df_funds["exhi_占基金净值比"  ] = df_funds["占基金净值比(%)"].round(decimals=2)

        ##########################################
        ### 保存到csv ; fund_stock,主动股票;fund_mixed_stock,偏股混合;fund_mixed_bond,偏债混合;
        # fund_bond,纯债;fund_hkus,QDII港股美股
        file_name = "stra_fof_" + selection_type + "_" + date_latest +".xlsx" 
        df_funds.to_excel(self.path_stra + file_name ,index=False ) 
        file_name = "stra_fof_" + selection_type + ".xlsx" 
        df_funds.to_excel(self.path_stra + file_name ,index=False ) 

        ###
        obj_f["df_funds"] = df_funds

        return obj_f



    def fund_stock_selection(self,obj_f):
        ####################################################################################
        ### 筛选不同类型基金重仓的股票
        ### notes:从2022Q2开始，重仓股票数据导出时，会按基金类型分类逐个导出。重仓基金还是全部数据，file=重仓基金(明细)-20220630.xlsx
        ### derived from fof_selection()
        ### Input:date_latest;quarter_end

        date_latest = obj_f["date_latest"]
        # 20211231
        quarter_end = obj_f["quarter_end"] 

        selection_type = obj_f["selection_type"] 
        ### values: fund_stock,主动股票;fund_mixed_stock,偏股混合;fund_mixed_bond,偏债混合;fund_hkus,QDII港股美股
        if "str_purchase" in obj_f.keys() :
            str_purchase = obj_f["str_purchase"] 
        else :
            str_purchase = ""

        ####################################################################################
        ### step 1，根据基金类型，筛选基金业绩和回撤排名前10-20%的基金
        # file=基金池rc_主动股票_20220308.xlsx
        if selection_type == "fund_stock" :
            file_name = "基金池rc_" + "主动股票" + str_purchase +"_"+ date_latest + ".xlsx"
        elif selection_type == "fund_mixed_stock" :
            file_name = "基金池rc_" + "偏股混合" + str_purchase +"_"+ date_latest + ".xlsx"
        elif selection_type == "fund_mixed_bond" :
            file_name = "基金池rc_" + "偏债混合" + str_purchase +"_"+ date_latest + ".xlsx"
        elif selection_type == "fund_hkus" :
            file_name = "基金池rc_" + "美股港股" + str_purchase + "_"+ date_latest + ".xlsx"
        ### 
        df_fund_list = pd.read_excel( self.path_fundpool+file_name, sheet_name="raw_data" )
        
        ### 已经是YTD收益率降序排列
        ##############################
        ### 对收益率和回撤进行排序打分
        df_fund_list = df_fund_list.sort_values(by="score",ascending=False )
        num_top20pct = max(20, int( len( df_fund_list.index )*0.2 )  )  
        df_fund_list_top = df_fund_list.iloc[:num_top20pct, :]

        print("df_fund_list_top, ", df_fund_list_top.head().T ) 
        
        
        ##############################################################
        ### step 2，获取给定分类基金的季度末重仓股票   
        ### notes:Wind终端有时候需要按分类导出，有时候可以选择 全部 基金类型
        ##########################################
        ### 只保留特定类型基金
        ### values: fund_stock,主动股票;fund_mixed_stock,偏股混合;fund_mixed_bond,偏债混合;
        # fund_bond,纯债;fund_hkus,QDII港股美股
        if selection_type == "fund_stock" :
            list_fund_type = ["普通股票型基金"]
        elif selection_type == "fund_bond" :
            list_fund_type = ["短期纯债型基金","中长期纯债型基金","混合债券型一级基金" ]
        elif selection_type == "fund_mixed_stock" :
            list_fund_type = ["偏股混合型基金","平衡混合型基金","灵活配置型基金","平衡混合型基金" ]
        elif selection_type == "fund_mixed_bond" :
            list_fund_type = ["混合债券型二级基金","偏债混合型基金"]
        ### 港股美股还比较少基金持有
        # elif selection_type == "fund_hkus" :
        #     list_fund_type = ["国际(QDII)股票型基金","国际(QDII)混合型基金" ]
        # elif selection_type == "fof" :
        #     list_fund_type = ["股票型FOF","债券型FOF","混合型FOF" ]

        ############################################################## 
        ### df_fundlist_all 是符合基金类型的所有基金持股记录 
        ### notes:Wind终端有时候需要按分类导出，有时候可以选择 全部 基金类型
        file_name_all = "重仓持股(明细)-" + quarter_end + ".xlsx"
        if os.path.exists( self.path_wind_terminal+file_name_all ) :
            ### 如果存在，表示所有基金类型都在这一个文件里了
            df_fundlist_all = pd.read_excel( self.path_wind_terminal+file_name_all  )
            # "投资类型"
            df_fundlist_all = df_fundlist_all[ df_fundlist_all["投资类型"].isin( list_fund_type ) ]
                    
        else :
            ### 因为导出时分不同文件名，所以要逐个导入，再合并
            count_f = 0 
            for fund_type in list_fund_type :
                file_name = "重仓持股(明细)-" + fund_type + "-" + quarter_end + ".xlsx"
                if count_f == 0 :
                    df_fundlist_all = pd.read_excel( self.path_wind_terminal+file_name  )
                    count_f = 1 
                else :
                    df_temp = pd.read_excel( self.path_wind_terminal+file_name  )
                    df_fundlist_all =df_fundlist_all.append(df_temp  )
                    count_f = 1 
        
        print("df_fundlist_all")
        print( df_fundlist_all.head() )

        ##########################################
        ### 寻找每个fof对应的持仓基金 
        count_code = 0 
        for temp_code in  df_fund_list_top["基金代码"] : 
            ### find holding funds of temp_code 
            df_sub = df_fundlist_all[ df_fundlist_all["代码"]==temp_code ]
            if len( df_sub.index ) >0 :
                if count_code == 0  :
                    df_holding_funds = df_sub
                    count_code = 1
                else :
                    df_holding_funds = df_holding_funds.append(df_sub, ignore_index= True)

        df_holding_funds.to_excel( "D:\\df_holding_stocks.xlsx" )

        ##########################################
        ### 去除重复项 
        df_holding_funds["change_pct"] = df_holding_funds["季度持仓变动(万股)"] / df_holding_funds["持股数量(万股)"]  
        # "持仓份额(万份)","季度持仓变动(万份)","持仓市值(万元)","占基金净值比(%)","占基金市值比(%)","基金总规模(亿元)","占基金总规模比(%)"
        df_stocks = df_holding_funds.groupby( "股票代码" )["持股数量(万股)","change_pct","持股总市值(万元)","持股市值占基金净值比(%)","持股市值占基金股票投资市值比(%)"].sum()
        # "change_pct","占基金净值比(%)","占基金市值比(%)","占基金总规模比(%)"
        for temp_col in ["change_pct","持股总市值(万元)","持股市值占基金净值比(%)","持股市值占基金股票投资市值比(%)"] :
            df_stocks["s_"+ temp_col ] = df_stocks[temp_col] / df_stocks[temp_col].sum()
        ### 分配权重
        df_stocks["s_sum" ] = df_stocks["s_"+ "change_pct"] *0.7
        for temp_col in ["持股总市值(万元)","持股市值占基金净值比(%)","持股市值占基金股票投资市值比(%)"] :
            df_stocks["s_sum" ] = df_stocks["s_sum" ] + df_stocks["s_"+ temp_col ] *0.1
        
        ### 取前20名基金
        df_stocks = df_stocks.sort_values(by="s_sum",ascending=False  )  
        df_stocks = df_stocks.iloc[:50,: ]

        ### 剔除负值
        df_stocks = df_stocks[ df_stocks["s_sum"] > 0  ] 

        df_stocks["code"] = df_stocks.index
        ##########################################
        ### 计算权重
        ### 控制单只基金权重不超过10% 
        df_stocks[ "weight" ] = df_stocks[ "s_sum" ]/df_stocks[ "s_sum" ].sum()
        # 
        df_stocks["w_diff"] = df_stocks["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_stocks["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_stocks["temp"] = df_stocks["weight"].apply(lambda x : x if x <0.085 else 0.0 )
        temp_sum = df_stocks["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_stocks[ df_stocks["w_diff"] <0.085 ]
        df_stocks.loc[ df_sub.index, "weight" ] = df_stocks.loc[ df_sub.index, "weight" ] * para_sum
        df_stocks["weight"] = df_stocks["weight"].apply(lambda x : 0.095 if x >0.095 else x )
               
        ##########################################
        ### 获取持仓基金的名称
        for temp_code in df_stocks.index: 
            ### find name in df_holding_funds
            df_sub = df_holding_funds[ df_holding_funds["股票代码"] == temp_code ]
            df_stocks.loc[temp_code,"名称"  ] = df_sub["股票简称"].values[0]
        ### notes:FOF名称 = "名称"  ； FOF持仓基金的名称="基金名称"

        ##########################################
        ### EXHI
        ### 持仓份额(万份)	change_pct	持仓市值(万元)	占基金净值比(%)	占基金市值比(%)	占基金总规模比(%)
        df_stocks["exhi_weight"  ] = df_stocks["weight"]*100
        df_stocks["exhi_weight"  ] = df_stocks["exhi_weight"].round(decimals=2)
        df_stocks["exhi_持股数量_万股"  ] = df_stocks["持股数量(万股)"]
        df_stocks["exhi_持股数量_万股"  ] = df_stocks["exhi_持股数量_万股"].round(decimals=1)
        df_stocks["exhi_持仓变动百分比"  ] = df_stocks["change_pct"]
        df_stocks["exhi_持仓变动百分比"  ] = df_stocks["exhi_持仓变动百分比"].round(decimals=1 )
        # 持仓市值(万元)
        df_stocks["exhi_持股总市值_万元"  ] = df_stocks["持股总市值(万元)"]
        df_stocks["exhi_持股总市值_万元"  ] = df_stocks["exhi_持股总市值_万元"].round(decimals=1 )
        # 占基金净值比(%) 
        df_stocks["exhi_持股市值占基金股票投资市值比"  ] = df_stocks["持股市值占基金股票投资市值比(%)"].round(decimals=2)

        ##########################################
        ### 保存到csv ; fund_stock,主动股票;fund_mixed_stock,偏股混合;fund_mixed_bond,偏债混合;
        # fund_bond,纯债;fund_hkus,QDII港股美股
        file_name = "stra_fund_stock_" + selection_type + "_" + date_latest +".xlsx" 
        df_stocks.to_excel(self.path_stra + file_name ,index=False ) 
        file_name = "stra_fund_stock_" + selection_type + ".xlsx" 
        df_stocks.to_excel(self.path_stra + file_name ,index=False ) 

        ###
        obj_f["df_stocks"] = df_stocks

        return obj_f

#########################################################################################
### 3，基金分析相关模块
class fund_ana():
    def __init__(self ):
        self.obj_config = config_data_fund_ana_1.obj_config
        self.data_io_1 = data_io()
        #######################################################################
        ### 转换后的基金wds数据表
        path_wds_fund = self.obj_config["dict"]["path_wds_fund"]
        
        ### 基金分析数据输出目录
        path_ciss_db_fund = self.obj_config["dict"]["path_ciss_db_fund"]

    def print_info(self):        
        print(" ")
        ### 股票基金 
        print("fund_filter_group_fund  |基金规模、业绩、公司、重仓行业(中信1、3级)等角度的筛选和分组  ")        
        print("fund_filter_group_stock  |基金持仓股票权重、区间涨跌幅的筛选和分组  ")        
        print("fund_ana_stock_change_ind  |基金持仓股票变动的分析:仓位变动、买卖时机、行业分布等  ")   
        print("fund_ana_nav_rank  |基金净值和排名分析  ") 
        print("fund_esti_port_stock_adjust  |基金持仓股票和调仓行为的仿真和预测  ") 
        print("fund_stra_port_stock_alpha  |基金股票组合收益增强策略 ") 
        print("fund_port_weighting_adj |基金收益增强模拟组合加权和调仓频率、交易成本 ") 
        print("fund_manage_perf_eval_group_esti |基金仿真和调仓行为描述、全市场基金 ") 
        
        ### 通用标准功能
        print("df_group_column_pct_level | 对df内给定column和百分比参数计算分组 ") 
        print("df_dfA_match_col_from_dfB | 将df_B内的部分列col_list匹配至最新df_A内，匹配依据是 基金和股票代码")

    #####################################################################
    ### 基金行为回顾、分析、预测 |新建 df_fund_sp_ana from df_fund_stock_port
    '''系统性地数据分析：目录={统计分析、指标、信号、优化}
    ref:file=file:///D:/TOUYAN/基金_能力评价和投资交易行为仿真/20191127-东方证券-东方证券《因子选股系列研究之六十二》：来自优秀基金经理的超额收益.pdf
    1，基金筛选和分组,fund_filter_group_fund ：
    1.1，基金规模维度：过小和过大都不好；因素：1，流动性约束和交易限制；2，投资权重和规模限制：“双十规定”
        3，打新收益稀释。规模下限阈值1,3,5；上限：30,50,100。notes：2亿是基金成立要求，大部分规模在2.5亿以内的基金很可能存在支持规模的部分。
    1.2，基金业绩角度：业绩考核主要看季度和年，因此选最近1季度、1年、3年；基金业绩排名：按5挡评级分5组。
    1.3，基金公司角度：基金公司越大，非公募产品可能也持有许多公募产品持仓的股票，主要原因是共享投研平台。
    1.4，基金基准和名称识别：许多基金会包括行业名称、主题名称如消费、新能源等；有的基金会对应大小市值、有的会划分成长和价值
    1.5，持股集中度角度：前十大持仓股占股票市值是否超过50%，25%。

    2，股票权重维度和分组,fund_filter_group_stocks：
    2.1，上限：由于10%限制，市值超过10%必须卖出，同时也说明是基金经理最看好的股票；重仓认定：单票超5%算作重仓；
    2.2，绩优基金股票池：业绩好的基金前十大重仓股合并成股票池；对于单只基金持仓股看有几个属于该股票池。
    2.3，绩差基金股票池：业绩差的基金前十大重仓股合并成股票池；
    2.4，区间股票上涨幅度：观察前一季度和当季度内股票上涨幅度，观察是否买入或加仓上涨股票，以及选股能力。
    2.5，是否属于同期沪深300、中证500、创业板指数成分；
    
    3，基金持仓股票变动的分析,fund_ana_stock_change_ind ：
    3.0，需要的数据：最近3期持仓股票数据；、资产负债-股票比例；
    3.1，是否长期持有；
    3.2，交易频率：seattrading-基金换手率
    3.3，买入时机：成本价格对应的买入时机、买入时所处的百分位、买入时财务指标{pe,roe,growth}
    3.4，卖出时机：实现收益对应的卖出时机,所处的百分位、财务指标{pe,roe,growth}
    3.5，股票仓位变动：要剔除股票本身涨跌因素；
    3.6，基金持仓股票的行业分布；从行业个股模拟行业收益，有可能的话用行业指数
    3.7，股票的市值、财务的成长和价值

    4，基金净值和排名分析,fund_ana_nav_rank：
    4.1，基金区间收益率：最近1季度、1年、3年区间每日净值增长率；
    4.2，区间每日净值增长率和不同指数的相关性：分析净值变动和不同基准组合{市值、行业、成长价值 }的相关性；
    4.3，统计基金公司整体收益和前20%、33%、50%收益。
    
    5，基金持仓股票和调仓行为的仿真和预测, fund_esti_port_stock_adjust ：
    5.1，根据持仓变动、历史持仓，滞后组合收益和净值的偏离，判断基金偏好的股票分组，例如行业、行业成长、小市值、基金重仓等{参考abcd3d的分组}；
    5.2，根据持仓变动、交易换手率、滞后组合收益和净值的偏离程度，判断基金区间调仓时机、股票品种和买卖权重。

    6，基金股票组合收益增强策略,fund_stra_port_stock_alpha：
    6.1，精选基金：绩优基金组内超配组合；业绩差基金的持仓股的负面剔除组合。
    6.2，精选行业：绩优基金精选行业；
    6.3，逆向策略：选择只被少数绩优基金持有的股票。

    7，基金收益增强模拟组合加权和调仓频率、交易成本,fund_port_weighting_adj：
    7.1，等权重加权
    7.2，相对于基准超配权重加权
    7.3，考虑价量和财务因子加权
    7.4，组合调整：1，1年三次披露日期；2，每个月按最新打分

    8，基金仿真和调仓行为描述、全市场基金, fund_manage_perf_eval_group_esti：
    8.1，单只基金动态的持仓描述和调仓行为预测；
    8.2，不同分组，如绩优基金、行业、小市值等基金的持仓描述和调仓行为预测；
    8.3，基金调仓行为和股票价格的均值回归分析；        
    '''

    def fund_filter_group_fund(self,obj_fund):
        ### 基金规模、业绩、公司、重仓行业(中信1、3级)等角度的筛选和分组  
        '''系统性地数据分析：目录={统计分析、指标、信号、优化}
        ref:file=file:///D:/TOUYAN/基金_能力评价和投资交易行为仿真/20191127-东方证券-东方证券《因子选股系列研究之六十二》：来自优秀基金经理的超额收益.pdf
        1，基金筛选和分组,fund_filter_group_fund ：
        1.1，基金规模维度：过小和过大都不好；因素：1，流动性约束和交易限制；2，投资权重和规模限制：“双十规定”
            3，打新收益稀释。规模下限阈值1,3,5；上限：30,50,100。notes：2亿是基金成立要求，大部分规模在2.5亿以内的基金很可能存在支持规模的部分。
        1.2，基金业绩角度：业绩考核主要看季度和年，因此选最近1季度、1年、3年；基金业绩排名：按5挡评级分5组。
        1.3，基金公司角度：基金公司越大，非公募产品可能也持有许多公募产品持仓的股票，主要原因是共享投研平台。
        1.4，基金基准和名称识别：许多基金会包括行业名称、主题名称如消费、新能源等；有的基金会对应大小市值、有的会划分成长和价值
        1.5，持股集中度角度：前十大持仓股占股票市值是否超过50%，25%。
        '''
        ### 
        df_fund = obj_fund["df_fund"] 
        df_fund_stock_port = obj_fund["df_fund_stock_port"]

        ### 剔除指数基金，理论上已经剔除过了。
        df_fund = df_fund[ df_fund["IS_INDEXFUND"]== 0 ] 

        ### 分组方程，根据某一列值从大到小的百分比分5或N档，并将0~N-1的档位level保存成新的一列
        para_list_quantile_pct_default = [0.2,0.4,0.6,0.8,1.0]
        para_list_quantile_pct_fund_stock_mv =  [0.05,0.2,0.3,0.5,1.0]

        #################################################################################
        ### 1.1，基金规模维度 |资产净值(元),F_PRT_NETASSET;持有股票市值(元),F_PRT_STOCKVALUE;
        '''
        截止日期	F_PRT_ENDDATE
        资产净值(元)	F_PRT_NETASSET
        1，持有股票市值(元)	F_PRT_STOCKVALUE
        2，持有股票市值占资产净值比例(%)	F_PRT_STOCKTONAV
        3，持有国债及现金占资产净值比例(%)	F_PRT_GOVCASHTONAV
        4，持有债券市值(不含国债)占资产净值比例(%)	F_PRT_BDTONAV_NOGOV；这个20q1最高有188%的，加了杠杆。
        5，持有基金市值占资产净值比例(%)	F_PRT_FUNDTONAV
        新建指标：nonstock_tonav = 3+4+5
        '''
        # 计算非股票部分持仓，反推基金股票投资比例；按基金净值分组，持有股票市值分组；若股票市值/基金净值
        # 去重复项和指数基金后：2020Q1，全部=4833，
        # 资产净值(元)，F_PRT_NETASSET：股票市值大于等于：3kw,4556;5kw,4398;1e,3900;2e,3257;5e,2228；
        # 持有股票市值(元)，F_PRT_STOCKVALUE：股票市值大于等于：3kw,2584;5kw,2408;1e,1711;2e,1202;5e,755。
        # notes:import_data_fund_holdings()已经剔除股票市值小于4千万的
        # 股票市值对应百分比：>30e,122，5%;>10e,459，20%；>5e,757,30%；2e,1204,48%;1e,1715,69% ；股票市值大于4kw的约2500个。
        # 百分比分5组：0%~5%，5%~20%，20%~30%，30%~50%，50%~100%
        # 规模分组
        
        ### 股票市值，资产净值(元)的百分比分5组：0%~5%，5%~20%，20%~30%，30%~50%，50%~100% 
        col_list_mv = ["F_PRT_STOCKVALUE","F_PRT_NETASSET","F_PRT_STOCKTONAV","F_PRT_GOVCASHTONAV","F_PRT_BDTONAV_NOGOV","F_PRT_FUNDTONAV" ]
        col_list_mv_level = []
        
        for col_name in col_list_mv :
            col_list_mv_level = col_list_mv_level + [ col_name + "_level"]
            '''input:df_fund,col_name,para_list_quantile_pct
            output:df_fund            '''
            obj_df ={}
            obj_df["df_input"] = df_fund
            obj_df["col_name"] = col_name
            obj_df["para_list_quantile_pct"]= para_list_quantile_pct_fund_stock_mv
            
            obj_df = self.df_group_column_pct_level(obj_df)
        
        ### 赋值给df_fund
        df_fund = obj_df["df_input"]

        #################################################################################
        ### 1.2，基金业绩角度：业绩考核主要看季度和年，因此选最近1季度、1年、3年；基金业绩排名：按5挡评级分5组。
        '''notes:即便剔除后，基金类型"F_FUNDTYPE"还有 8种，也就是“同类排名”项目下，会有多个第一名1。
        [普通股票型基金(封闭式),平衡混合型基金(封闭式),灵活配置型基金,平衡混合型基金,平衡混合型基金,偏股混合型基金,混合债券型二级基金,偏股混合型基金]
        1，需要用收益率数据而不是排名数据去进行自定义的排名。|对于股票30%以下的基金理论上应该用股票部分的收益。
        2，本基金收益率：
            收益率(本季以来)	F_AVGRETURN_THISQUARTER;收益率(本年以来)	F_AVGRETURN_THISYEAR
            收益率(一个月)	F_AVGRETURN_MONTH;收益率(三个月)	F_AVGRETURN_QUARTER
            收益率(六个月)	F_AVGRETURN_HALFYEAR;收益率(一年)	F_AVGRETURN_YEAR
            收益率(两年)	F_AVGRETURN_TWOYEAR;收益率(三年)	F_AVGRETURN_THREEYEAR
            收益率(五年)	F_AVGRETURN_FIVEYEAR;收益率(成立以来)	F_AVGRETURN_SINCEFOUND;年化收益率	F_ANNUALYEILD;

        3，同类基金收益率
        "F_SFRETURN_THISYEAR","F_SFRANK_THISYEAR","F_SFRETURN_RECENTQUARTER","F_SFRANK_RECENTQUARTER","F_SFRETURN_RECENTHALFYEAR",
        "F_SFRANK_RECENTHALFYEAR","F_SFRETURN_RECENTYEAR","F_SFRANK_RECENTYEAR","F_SFRETURN_RECENTTWOYEAR","F_SFRANK_RECENTTWOYEAR",
        "F_SFRETURN_RECENTTHREEYEAR","F_SFRANK_RECENTTHREEYEAR","F_SFRETURN_RECENTFIVEYEAR","F_SFRANK_RECENTFIVEYEAR","F_SFRETURN_SINCEFOUND"
        '''
        ### 暂时简化为短期、中期、长期3个视角共6个收益率指标，再加上本季和本年两个监控指标
        col_list_perf = ["F_AVGRETURN_QUARTER","F_AVGRETURN_HALFYEAR","F_AVGRETURN_YEAR","F_AVGRETURN_TWOYEAR","F_AVGRETURN_THREEYEAR","F_AVGRETURN_FIVEYEAR"]
        col_list_perf =col_list_perf +["F_AVGRETURN_THISQUARTER","F_AVGRETURN_THISYEAR"]
        
        ### 构建短期、中期、长期、年初至今四个基金排名指标|notes：两指标相加方式可以使得缺乏较长排名的基金排名下降。
        #短期，最近1季度和半年："F_AVGRETURN_QUARTER","F_AVGRETURN_HALFYEAR"|
        df_fund["fund_rank_short"] = df_fund["F_AVGRETURN_QUARTER"]*0.5 + df_fund["F_AVGRETURN_HALFYEAR"]*0.5
        #中期，"F_AVGRETURN_YEAR","F_AVGRETURN_TWOYEAR" 
        df_fund["fund_rank_mid"] = df_fund["F_AVGRETURN_QUARTER"]*0.5 + df_fund["F_AVGRETURN_HALFYEAR"] *0.5
        #长期，,"F_AVGRETURN_THREEYEAR","F_AVGRETURN_FIVEYEAR"
        df_fund["fund_rank_long"] = df_fund["F_AVGRETURN_THREEYEAR"]*0.5 + df_fund["F_AVGRETURN_FIVEYEAR"] *0.5
        #实时，逐日，"F_AVGRETURN_THISQUARTER","F_AVGRETURN_THISYEAR"
        df_fund["fund_rank_ytd"] = df_fund["F_AVGRETURN_THISQUARTER"]*0.5 + df_fund["F_AVGRETURN_THISYEAR"]*0.5

        col_list_perf_rank=["fund_rank_short","fund_rank_mid","fund_rank_long","fund_rank_ytd"]

        for col_name in col_list_perf_rank :
            obj_df ={}
            obj_df["df_input"] = df_fund
            obj_df["col_name"] = col_name
            # 基金业绩按五档，各20%取值
            obj_df["para_list_quantile_pct"]= para_list_quantile_pct_default
            obj_df = self.df_group_column_pct_level(obj_df)
        
        ### 赋值给df_fund
        df_fund = obj_df["df_input"]


        #################################################################################
        ### 1.3，基金公司角度：基金公司越大，非公募产品可能也持有许多公募产品持仓的股票，主要原因是共享投研平台。
        # 暂时不在这里统计； 对每个基金公司，选取不同时期业绩前30%的基金构成组合："F_INFO_CORP_FUNDMANAGEMENTCOMP"
        

        #################################################################################
        ### 1.4，基金基准和名称识别：许多基金会包括行业名称、主题名称如消费、新能源等；有的基金会对应大小市值、有的会划分成长和价值
        # 暂时不在这里统计；"F_INFO_FULLNAME"基金全名会比简称好，例如162006.SZ，全名里有成长，但简称里没有。
        # example:160211.SZ	国泰中小盘成长混合型证券投资基金(LOF)	国泰小盘
        # notes:基金名称可能和实际持仓不同，例如易方达中小盘，持仓大部分是大中市值股票。
        
        #################################################################################
        ### 1.5，股票仓位、持股集中度、行业集中度：前十大持仓股占股票市值是否超过50%，25%。
        
        ### 十大持仓股票合计占全部股票市值比例
        # 占股票市值比， STOCK_PER
        for temp_i in df_fund.index :
            temp_fund_code = df_fund.loc[temp_i, "fund_code" ]
            stockmv_to_nav = df_fund.loc[temp_i, "F_PRT_STOCKTONAV" ]
            ### 获取前十大重仓股合计权重,持有股票市值占基金净值比例(%),"F_PRT_STKVALUETONAV",注意这个不是占股票市值的比例
            # find stock records in df_fund_stock_port
            df_fund_stock_port_sub = df_fund_stock_port[df_fund_stock_port["fund_code"]==temp_fund_code ]
            temp_len = len( df_fund_stock_port_sub.index )
            if temp_len > 0 :
                ### 判断是否股票数量小于10个，若多于10个可能是全部持仓或前十大+全部持仓
                if temp_len > 10 :
                    ### 去重复项
                    df_fund_stock_port_sub=df_fund_stock_port_sub.drop_duplicates("S_INFO_STOCKWINDCODE",keep="last")
                    ### 股票市值降序排列，并取前十。占股票市值比 STOCK_PER，或 持有股票市值占基金净值比例(%)，F_PRT_STKVALUETONAV
                    df_fund_stock_port_sub = df_fund_stock_port_sub.sort_values(by="STOCK_PER",ascending=False )
                    # df_fund_stock_port_sub = df_fund_stock_port_sub.sort_values(by="F_PRT_STKVALUE",ascending=False )

                    df_fund_stock_port_sub = df_fund_stock_port_sub.iloc[:10,:]

                ### 计算前十大持仓股票合计占净值比例
                top10_to_nav = df_fund_stock_port_sub["F_PRT_STKVALUETONAV"].sum()
                top10_to_stockmv = df_fund_stock_port_sub["STOCK_PER"].sum()
                top5_to_stockmv = df_fund_stock_port_sub.iloc[:5,:]["STOCK_PER"].sum()
            else :
                top10_to_nav = 0.0
                top10_to_stockmv = 0.0
                top5_to_stockmv = 0.0
            
            ### 十大持仓股票合计占全部股票市值比例，top10_to_stockmv | 持有股票市值占资产净值比例(%)，F_PRT_STOCKTONAV
            #notes:"F_PRT_STOCKTONAV"的值是百分位，例如58.02对应 58%
            # top10_to_stockmv = top10_to_nav/stockmv_to_nav
            # save to df_fund
            df_fund.loc[temp_i, "top10_to_nav" ] = top10_to_nav
            df_fund.loc[temp_i, "top10_to_stockmv" ] = top10_to_stockmv
            df_fund.loc[temp_i, "top5_to_stockmv" ] = top5_to_stockmv

        ### 分档：股票仓位、持股集中度top5,top10        
        obj_df ={}
        obj_df["df_input"] = df_fund
        obj_df["col_name"] = "top10_to_stockmv"
        # 五档各20%取值
        obj_df["para_list_quantile_pct"]= para_list_quantile_pct_default
        obj_df = self.df_group_column_pct_level(obj_df)        
        
        obj_df["col_name"] = "top10_to_stockmv"
        obj_df = self.df_group_column_pct_level(obj_df)        
        # 赋值给df_fund
        df_fund = obj_df["df_input"]

        #################################################################################
        ### 统计前十大重仓股的中信一级行业前3
        code_list_s = df_fund_stock_port["S_INFO_STOCKWINDCODE"].drop_duplicates().to_list()
        obj_ind={}
        obj_ind["code_list"] = code_list_s
        obj_ind["date_end"] = obj_fund["dict"]["date_adj_port"]
        obj_ind["if_column_ind"] = "citics"
        obj_ind["if_all_codes"] = 0         
        obj_ind = data_io_1.get_ind_date(obj_ind)
        df_s_ind =  obj_ind["df_s_ind"]
        ### 将行业分类赋值给 df_fund_stock_port
        for temp_i in df_s_ind.index :
            temp_code = df_s_ind.loc[temp_i, "wind_code" ]
            ### find index list of temp_code in df_s_ind
            df_fsp_sub = df_fund_stock_port[ df_fund_stock_port["S_INFO_STOCKWINDCODE"] == temp_code ]
            if len( df_fsp_sub.index ) >0 :
                df_fund_stock_port.loc[ df_fsp_sub.index, "ind_code"  ] = df_s_ind.loc[temp_i, "ind_code" ]
                for temp_col in obj_ind["col_list_ind"] :
                    df_fund_stock_port.loc[ df_fsp_sub.index, temp_col] = df_s_ind.loc[temp_i,temp_col ]
        
        ### 根据前十大持仓股，统计行业分布；第一大行业名称、行业权重。 
        # 缩小df，加快速度:股票代码，股票占净值，股票占股票总值，股票市值，占股票市值比，占流通股本比例(%)
        col_list_sub = ["fund_code","S_INFO_STOCKWINDCODE","F_PRT_STKVALUETONAV","STOCK_PER","F_PRT_STKVALUE","FLOAT_SHR_PER"]
        col_list_sub = col_list_sub +["ind_code","citics_ind_code_s_1","citics_ind_code_s_2","citics_ind_code_s_3"  ]

        df_fsp = df_fund_stock_port.loc[:, col_list_sub ]
        for temp_i in df_fund.index :
            temp_fund_code = df_fund.loc[temp_i, "fund_code" ]
            ### 获取前十大重仓股合计权重,持有股票市值占基金净值比例(%),"F_PRT_STKVALUETONAV",注意这个不是占股票市值的比例
            # find stock records in df_fsp
            df_fsp_sub = df_fsp[df_fsp["fund_code"]==temp_fund_code ]
            temp_len = len( df_fsp_sub.index )
            if temp_len > 0 :
                ### 判断是否股票数量小于10个，若多于10个可能是全部持仓或前十大+全部持仓
                if temp_len > 10 :
                    ### 去重复项
                    df_fsp_sub=df_fsp_sub.drop_duplicates("S_INFO_STOCKWINDCODE",keep="last")
                    ### 股票市值降序排列，并取前十。
                    df_fsp_sub = df_fsp_sub.sort_values(by="F_PRT_STKVALUE",ascending=False )
                    df_fsp_sub = df_fsp_sub.iloc[:10,:]

                ### 计算前十大持仓股票的行业权重 
                # index 是 10.0,20.0， columns不变
                df_ind = df_fsp_sub.loc[:,["ind_code","STOCK_PER"] ].groupby("ind_code").sum()
                df_ind = df_ind.sort_values(by="STOCK_PER",ascending=False ) 
                ### 赋值给 df_fund
                temp_count = 0
                df_fund.loc[temp_i, "allo_ind_weight_top3"] = 0.0
                for temp_ind in df_ind.index :
                    ### 取值 10.0,20.0,21.0 等
                    # temp_count对应第n大行业配置，code对应行业代码，weight对应股票内行业配置比例
                    df_fund.loc[temp_i, "allo_ind_code_"+ str(temp_count) ] = int( temp_ind )
                    df_fund.loc[temp_i, "allo_ind_weight_"+ str(temp_count) ] = df_ind.loc[temp_ind, "STOCK_PER" ]
                    
                    ### 计算前3大行业配置合计权重：
                    if temp_count < 3:
                        df_fund.loc[temp_i, "allo_ind_weight_top3"] =df_fund.loc[temp_i, "allo_ind_weight_top3"]+  df_ind.loc[temp_ind, "STOCK_PER" ]
                    ###
                    temp_count =temp_count +1 
        
        ###计算分档：前三大行业配置集中度"allo_ind_weight_top3" ；第一大行业配置集中度"allo_ind_weight_0"
        ### "allo_ind_weight_top3"
        obj_df ={}
        obj_df["df_input"] = df_fund
        obj_df["col_name"] = "allo_ind_weight_top3"
        # 五档各20%取值
        obj_df["para_list_quantile_pct"]= para_list_quantile_pct_default
        obj_df = self.df_group_column_pct_level(obj_df)        
        ### "allo_ind_weight_0"
        obj_df["col_name"] = "allo_ind_weight_0"
        obj_df = self.df_group_column_pct_level(obj_df)        
        

        ### save to obj_fund
        obj_fund["df_fund"] = df_fund
        obj_fund["df_fund_stock_port"] = df_fund_stock_port

        obj_fund["col_list_mv"] = col_list_mv
        obj_fund["col_list_mv_level"] = col_list_mv_level
        obj_fund["para_list_quantile_pct_fund_stock_mv"] = para_list_quantile_pct_fund_stock_mv
        obj_fund["para_list_quantile_pct_default"] = para_list_quantile_pct_default
        obj_fund["col_list_perf"] = col_list_perf
        obj_fund["dict"]["col_list_perf_rank"] = col_list_perf_rank

        return obj_fund


    def fund_filter_group_stock(self,obj_fund):
        ### 基金持仓股票权重、区间涨跌幅的筛选和分组  
        '''
        2，股票权重维度和分组,fund_filter_group_stocks：
        2.1，上限：由于10%限制，市值超过10%必须卖出，同时也说明是基金经理最看好的股票；重仓认定：单票超5%算作重仓；
        2.2，绩优基金股票池：业绩好的基金前十大重仓股合并成股票池；对于单只基金持仓股看有几个属于该股票池。
        2.3，绩差基金股票池：业绩差的基金前十大重仓股合并成股票池；
        2.4，区间股票上涨幅度：观察前一季度和当季度内股票上涨幅度，观察是否买入或加仓上涨股票，以及选股能力。
        2.5，是否属于同期沪深300、中证500、创业板指数成分；
        
        '''
        ### Import obj_fund
        df_fund = obj_fund["df_fund"] 
        df_fund_stock_port = obj_fund["df_fund_stock_port"]
        col_list_perf_rank = obj_fund["dict"]["col_list_perf_rank"]

        # 基金重仓股的股票代码
        stock_list_fund = obj_fund["stock_list_fund"]

        ### 构建股票池 stockpool ; 
        # obj_fund["stock_list_fund"] 是全部基金代码列表
        df_stockpool_fund = pd.DataFrame( index=obj_fund["stock_list_fund"] )
        df_stockpool_fund["wind_code"] = df_stockpool_fund.index

        #################################################################################
        ### 2，股票权重维度和分组,fund_filter_group_stocks：

        #################################################################################
        ### 2.1，上限：由于10%限制，市值超过10%必须卖出，同时也说明是基金经理最看好的股票；重仓认定：单票超5%算作重仓；
        # 对 df_fund_stock_port 进行操作。|notes:主要目的是用于基金重仓个券的统计分析
        # 维度1：股票占净值，"F_PRT_STKVALUETONAV",超过9%、5%算重仓
        df_fund_stock_port["F_PRT_STKVALUETONAV_5pct"] =df_fund_stock_port["F_PRT_STKVALUETONAV"].apply(lambda x : 1 if x >=4.99 else 0 )
        df_fund_stock_port["F_PRT_STKVALUETONAV_9pct"] =df_fund_stock_port["F_PRT_STKVALUETONAV"].apply(lambda x : 1 if x >=8.99 else 0 )
        
        # 维度2：股票占股票总值，"STOCK_PER"，取9%，5%两档
        df_fund_stock_port["STOCK_PER_5pct"] =df_fund_stock_port["STOCK_PER"].apply(lambda x : 1 if x >=4.99 else 0 )
        df_fund_stock_port["STOCK_PER_9pct"] =df_fund_stock_port["STOCK_PER"].apply(lambda x : 1 if x >=8.99 else 0 )
        
        #################################################################################
        ### 2.2，绩优基金股票池：业绩好的基金前十大重仓股合并成股票池；对于单只基金持仓股看有几个属于该股票池。
        '''业绩好的定义：
        长期、中期、短期、年初至今的基金收益：
        col_list_perf_rank=["fund_rank_short","fund_rank_mid","fund_rank_long","fund_rank_ytd"]
        对应的分组是col_name +"_level"
        notes:一个股票，有可能同时在绩优基金和绩差基金里。
        '''
        col_list_perf_rank_level = []
        for temp_col in col_list_perf_rank :
            col_list_perf_rank_level = col_list_perf_rank_level + [ temp_col +"_level" ]

        fund_list = df_fund_stock_port["fund_code"].drop_duplicates().to_list()
        for temp_fund in fund_list :
            df_fsp = df_fund_stock_port[ df_fund_stock_port["fund_code"]==temp_fund ]
            ### find fund code in df_fund
            df_fund_sub = df_fund[ df_fund["fund_code"]==temp_fund ]
            if len( df_fund_sub.index )>0 :
                for temp_col in col_list_perf_rank :
                    temp_col_perf = temp_col +"_level"
                    df_fund_stock_port.loc[df_fsp.index, temp_col_perf] = df_fund_sub[temp_col_perf].values[0]
                    
        
        # 通过判断单只基金的单一持仓股票x，对应的长中短期业绩排名，可以判断该股票是否被绩优秀或绩差基金持有的程度。
        # 可以计算加权后的单只股票持有基金的平均排名，也可以计算被绩优(value=0)、绩差(value=4)基金持有的数量。
        #################################################################################
        ### 2.3，绩差基金股票池：业绩差的基金前十大重仓股合并成股票池；
        # 同上，通过判断单只基金的单一持仓股票x，对应的长中短期业绩排名，可以判断该股票是否被绩优秀或绩差基金持有的程度。
        # 可以计算加权后的单只股票持有基金的平均排名，也可以计算被绩优(value=0)、绩差(value=4)基金持有的数量。
        para_quantile = 0.5 # 50% 对应median
        df_stockpool_perf_level = df_fund_stock_port.loc[:, ["S_INFO_STOCKWINDCODE"]+col_list_perf_rank_level ].groupby("S_INFO_STOCKWINDCODE").quantile(para_quantile)
        
        
        # df_stockpool_temp的index是股票代码，columns是4个不同时间区间的对应基金业绩中位数，越小越好，取值0~4之间
        # 对4个column，将股票池进一步划分为5个档位，取值最小的20%区间是绩优股票池，取值最大的20%区间是绩差基金股票池
        ### 将4个column保存至基金重仓股票池 df_stockpool_fund
        # pd.concat: axis=1 是按index横向扩展，如果有相同column值会都各自保存1列；axis=0 是按column向下扩展，如果有相同index值会都各自保存1行
        # join="outer"是对2个df的index取交集，非共有的index/column会保留 ； join="inner"是去并集，非共有的index/column会被删除。
        df_stockpool_fund = pd.concat([df_stockpool_fund, df_stockpool_perf_level ],axis=1, join="outer")
         
        #################################################################################
        ### 2.4，区间股票上涨幅度：观察前一季度和当季度内股票上涨幅度，观察是否买入或加仓上涨股票，以及选股能力。
        # 导入区间股票涨跌幅，1个是季度区间，1个是披露区间，1个是混合区间:从季末到披露日期
        # 1,季末区间 date_ann：obj_fund["dict"]["date_ann_pre"] ; obj_fund["dict"]["date_ann"]
        # 2,基金信息披露区间 date_report： obj_fund["dict"]["date_report_pre"] ;obj_fund["dict"]["date_report"] 
        # 3,季末到信息披露区间 date_gap：obj_fund["dict"]["date_report"]；obj_fund["dict"]["date_ann"]
        df_stockpool_fund["S_INFO_WINDCODE"] = df_stockpool_fund.index
        df_stockpool_fund["wind_code"] = df_stockpool_fund.index 
        
        obj_data={}
        obj_data["dict"] = {}
        #########################################
        ### 1,季末区间 date_ann
        obj_data["dict"]["date_start"] = obj_fund["dict"]["date_ann_pre"]
        obj_data["dict"]["date_end"] = obj_fund["dict"]["date_ann"]
        obj_data["df_ashare_ana"] = df_stockpool_fund
        # obj_data["df_ashare_ana"] 至少要包括 "S_INFO_WINDCODE"
        
        obj_data = data_io_1.get_period_pct_chg_codelist( obj_data )
        ### 返回df
        df_stockpool_fund = obj_data["df_ashare_ana"] 

        ### 对列值改名,删除无用的
        df_stockpool_fund = df_stockpool_fund.rename( columns={"period_pct_chg":"period_pct_chg_date_ann"} )
        df_stockpool_fund = df_stockpool_fund.drop(["adjclose_end","adjclose_start"], axis=1  )

        #########################################
        ### 2,基金信息披露区间 date_report
        obj_data["dict"]["date_start"] = obj_fund["dict"]["date_report_pre"]
        obj_data["dict"]["date_end"] = obj_fund["dict"]["date_report"] 
        obj_data["df_ashare_ana"] = df_stockpool_fund
        # obj_data["df_ashare_ana"] 至少要包括 "S_INFO_WINDCODE"


        obj_data = data_io_1.get_period_pct_chg_codelist( obj_data )
        ### 返回df
        df_stockpool_fund = obj_data["df_ashare_ana"]
        ### 对列值改名,删除无用的
        df_stockpool_fund = df_stockpool_fund.rename( columns={"period_pct_chg":"period_pct_chg_date_report"} )
        df_stockpool_fund = df_stockpool_fund.drop(["adjclose_end","adjclose_start"], axis=1  )

        #########################################
        ### 3,季末到信息披露区间 date_gap：obj_fund["dict"]["date_report"]；obj_fund["dict"]["date_ann"]
        obj_data["dict"]["date_start"] = obj_fund["dict"]["date_report"]
        obj_data["dict"]["date_end"] = obj_fund["dict"]["date_ann"] 
        obj_data["df_ashare_ana"] =  df_stockpool_fund
        # obj_data["df_ashare_ana"] 至少要包括 "S_INFO_WINDCODE"
        obj_data = data_io_1.get_period_pct_chg_codelist( obj_data )
        ### 返回df
        df_stockpool_fund = obj_data["df_ashare_ana"]
        ### 对列值改名,删除无用的
        df_stockpool_fund = df_stockpool_fund.rename( columns={"period_pct_chg":"period_pct_chg_date_gap"} )
        df_stockpool_fund = df_stockpool_fund.drop(["adjclose_end","adjclose_start"], axis=1  )
        
        #########################################
        ### 4，对上述3个日期区间的股票涨跌幅分别分档
        para_list_quantile_pct_default = [0.2,0.4,0.6,0.8,1.0]
        obj_df ={}
        # 五档各20%取值
        obj_df["para_list_quantile_pct"]= para_list_quantile_pct_default

        obj_df["df_input"] = df_stockpool_fund
        ### "period_pct_chg_date_ann"
        obj_df["col_name"] = "period_pct_chg_date_ann"
        obj_df = self.df_group_column_pct_level(obj_df)
        ### "period_pct_chg_date_report"
        obj_df["col_name"] = "period_pct_chg_date_report"
        obj_df = self.df_group_column_pct_level(obj_df)
        ### "period_pct_chg_date_gap"
        obj_df["col_name"] = "period_pct_chg_date_gap"
        obj_df = self.df_group_column_pct_level(obj_df)

        df_stockpool_fund = obj_df["df_input"]        

        #################################################################################
        ### 2.5，是否属于同期沪深300、中证500、创业板指数成分；以持仓数据发生的季末日期为基准=obj_fund["dict"]["date_ann"]
        ### 获取季末日期前最近的1个交易日 obj_fund["dict"]["date_ann"]
        obj_date = {}
        obj_date["date"] = obj_fund["dict"]["date_ann"]
        obj_date = data_io_1.get_trading_days( obj_date )
        temp_trading_day = max( obj_date["date_list_pre"] )

        ### 导入指数成分;
        '''notes:必须是季末之前第一个交易日才有指数成分的数据！例如20200529
        1."AIndexHS300FreeWeight"的数据在20120104-20141031区间是每个月底才有数据！！
        为此需要对交易日计算月末日期。
        '''
        # 获取月末的交易日
        print("Debug===temp_trading_day ",type(temp_trading_day), temp_trading_day )
        if temp_trading_day >= 20120104.0 and temp_trading_day<20141031 :
            obj_date_monthend = {}
            obj_date_monthend["date"] = temp_trading_day 
            obj_date_monthend["date_frequency"] = "month" 
            obj_date_monthend = data_io_1.get_trading_days( obj_date_monthend )
            temp_trading_day = max( obj_date_monthend["date_list_pre"] )
            print("Debug===temp_trading_day month ",type(temp_trading_day), temp_trading_day )

        table_name = "AIndexHS300FreeWeight"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name + "\\"
        file_name = "WDS_TRADE_DT_" + str( int(temp_trading_day) ) + "_ALL.csv"
        try :
            df_index_consti = pd.read_csv(path_table + file_name , encoding="gbk")
        except :
            df_index_consti = pd.read_csv(path_table + file_name )

        index_list = df_index_consti["S_INFO_WINDCODE"].drop_duplicates().to_list()

        ### 判断沪深300、中证500、中证1000、创业板指数 是否在指数列表里
        code_index_list = ["000300.SH", "000905.SH","000852.SH","399006.SZ"]
        for code_index in code_index_list :
            if code_index in index_list :
                df_temp = df_index_consti[ df_index_consti["S_INFO_WINDCODE"]== code_index ]
                list_consti = df_temp["S_CON_WINDCODE"].to_list()
                df_stockpool_fund["if_"+code_index ] = df_stockpool_fund["S_INFO_WINDCODE"].apply(lambda x :1 if x in list_consti else 0 )

        #################################################################################
        ### Save to obj_fund 
        obj_fund["df_stockpool_fund"] = df_stockpool_fund
        obj_fund["df_fund_stock_port"] = df_fund_stock_port

        return obj_fund

    def fund_ana_stock_change_ind(self,obj_fund):
        ### 基金持仓股票变动的分析:仓位变动、买卖时机、行业分布等  
        ''' 
        3，基金持仓股票变动的分析,fund_ana_stock_change_ind ：
        3.0，需要的数据：最近1季和2季度，前1年和2年持仓股票数据；、资产负债-股票比例；
        3.1，是否长期持有；
        3.2，交易频率：seattrading-基金换手率
        3.3，买入时机：成本价格对应的买入时机、买入时所处的百分位、买入时财务指标{pe,roe,growth}
        3.4，卖出时机：实现收益对应的卖出时机,所处的百分位、财务指标{pe,roe,growth}
        3.5，股票仓位变动：要剔除股票本身涨跌因素；
        3.6，基金持仓股票的行业分布、行业配置变动；从行业个股模拟行业收益，有可能的话用行业指数
        3.7，股票的市值、财务的成长和价值
        '''
        ### Import obj_fund
        df_fund = obj_fund["df_fund"] 
        df_fund_stock_port = obj_fund["df_fund_stock_port"]
        df_stockpool_fund = obj_fund["df_stockpool_fund"] 
        
        col_list_perf_rank = obj_fund["dict"]["col_list_perf_rank"]
        col_list_perf_rank=["fund_rank_short","fund_rank_mid","fund_rank_long","fund_rank_ytd"]
        df_stockpool_fund = obj_fund["df_stockpool_fund"] 

        # 基金重仓股的股票代码 || "stock_list_fund" 应该是基金持仓的股票代码list
        stock_list_fund = obj_fund["stock_list_fund"]


        #################################################################################
        ### 3，基金持仓股票变动的分析,fund_ana_stock_change_ind ：
        #################################################################################
        ### 3.0，需要的数据：最近1季和2季度，前1年和2年持仓股票数据；、资产负债-股票比例；
        ### 若当前季末是3,9月，则取之前的12,6月的全部持仓股票；匹配 
        '''
        分析：1，过去1、2、4、8个季度仓位变动类型：买入0-1，加仓1-2，持有1--1，减仓2--1，清仓1--0。
        2，重点关注前十大持仓披露时间，因为信息价值较高。
        3，判断是主动增持还是被动股票上涨导致的进入当期前10大；
            Q1,4月底披露top10，和Q4_pre全部持仓比较；notes：基金Q4持仓最晚在0331披露
            Q2,7月底披露top10，和Q1、Q4_pre全部持仓比较； 
            Q3,10月底披露top10，和Q2全部持仓比较； 
            Q4,1月底披露top10，和Q3，Q2全部持仓。
        步骤：1,获取最近1、3、4、8个季度日期，导入基金持仓股票；
        2，看和前2个季度比，持仓的变动。持仓变动分类：
        '''
        date_q = obj_fund["dict"]["date_report"] 
        date_q_pre = obj_fund["dict"]["date_report_pre"]
        date_q_pre2 = obj_fund["dict"]["date_report_pre2"]
        # notes：date_q_pre_1y和date_q_pre_2y 只能是6月或12月，方便获取全部持仓。
        date_q_pre_1y = obj_fund["dict"]["date_report_pre_1y"] 
        date_q_pre_2y = obj_fund["dict"]["date_report_pre_2y"] 
        # 最近一个披露全部持仓的季度
        date_q_6or12m = obj_fund["dict"]["date_q_6or12m"]
        # date_quarter_pastN = [1,4,3,2 ]
        date_quarter_pastN = obj_fund["dict"]["date_quarter_pastN"]
        
        ###########################################################
        ### 导入基金持仓数据，前1季度 obj_fund["dict"]["date_report_pre"]
        ''' 所需要的columns：持有股票市值(元),F_PRT_STKVALUE，持有股票市值占基金净值比例(%),F_PRT_STKVALUETONAV
        占股票市值比,STOCK_PER,占流通股本比例(%);FLOAT_SHR_PER,公告日期 ANN_DATE 
        col_list_stock_port = ["ANN_DATE","F_PRT_STKVALUE","F_PRT_STKVALUETONAV","STOCK_PER","FLOAT_SHR_PER"]
        output:例子："STOCK_PER_suffix","STOCK_PER_suffix_diffpct"
        '''
        obj_fund_pre={}
        obj_fund_pre["df_fund"]  = obj_fund["df_fund"] 
        obj_fund_pre["dict"]={}
        obj_fund_pre["fund_list"] = obj_fund["fund_list"]
        obj_fund_pre["dict"]["date_report"] = date_q_pre
        obj_fund_pre = data_io_fund_ana_1.import_data_fund_holdings(obj_fund_pre ) 
        ### 导入行业信息
        obj_fund_pre["dict"]["date_adj_port"] = date_q_pre # 这个赋值不是很确定。
        obj_fund_pre= data_io_fund_ana_1.import_data_fund_stock_name_listday_ind(obj_fund_pre )
        
        obj_match = {}
        obj_match["if_diff"] =1         
        obj_match["col_1"] = "fund_code"
        obj_match["col_2"] = "S_INFO_STOCKWINDCODE"
        obj_match["fund_list"] = obj_fund["fund_list"]
        obj_match["col_list_match"] = obj_fund_pre["col_list_stock_port"]
        # "match_suffix",词根、前缀和后缀: root, prefix and suffix
        
        #判断是否有导入数据： obj_fund["if_missing_file"] = 1 
        if not obj_fund_pre["if_missing_sp"] == 1 :
        # 将df_pre内的部分列col_list匹配至最新df内，匹配依据是 基金和股票代码 
            obj_match["df_A"] = df_fund_stock_port
            obj_match["df_B"] = obj_fund_pre["df_fund_stock_port"]
            obj_match["match_suffix"] = "_pre"
            obj_match = self.df_dfA_match_col_from_dfB( obj_match)
            # 将结果赋值给 df_fund_stock_port
            df_fund_stock_port = obj_match["df_A"]
        
        df_fund_pre_1q = obj_fund_pre["df_fund"]
        df_fund_stock_port_pre_1q = obj_fund_pre["df_fund_stock_port"]
        ###########################################################        
        ### 导入基金持仓数据，前2季度 obj_fund["dict"]["date_report_pre"]
        obj_fund_pre["dict"]["date_report"] = date_q_pre2
        obj_fund_pre = data_io_fund_ana_1.import_data_fund_holdings(obj_fund_pre ) 
        #判断是否有导入数据： obj_fund["if_missing_file"] = 1 
        if not obj_fund_pre["if_missing_sp"] == 1 :
            # notes:第二次取值只有df_A,df_B两项有变动。
            obj_match["df_A"] = df_fund_stock_port
            obj_match["df_B"] = obj_fund_pre["df_fund_stock_port"]  
            obj_match["match_suffix"] = "_pre2"  
            obj_match = self.df_dfA_match_col_from_dfB( obj_match)
            # 将结果赋值给 df_fund_stock_port
            df_fund_stock_port = obj_match["df_A"]

        ###########################################################        
        ### 导入基金持仓数据，前1年 obj_fund["dict"]["date_report_pre_1y"] 
        obj_fund_pre["dict"]["date_report"] = date_q_pre_1y
        obj_fund_pre = data_io_fund_ana_1.import_data_fund_holdings(obj_fund_pre ) 
        ### 导入行业信息
        obj_fund_pre["dict"]["date_adj_port"] = date_q_pre # 这个赋值不是很确定。
        obj_fund_pre= data_io_fund_ana_1.import_data_fund_stock_name_listday_ind(obj_fund_pre )
        
        #判断是否有导入数据： obj_fund["if_missing_file"] = 1 
        if not obj_fund_pre["if_missing_sp"] == 1 :
            # notes:第二次取值只有df_A,df_B两项有变动。
            obj_match["df_A"] = df_fund_stock_port
            obj_match["df_B"] = obj_fund_pre["df_fund_stock_port"]   
            obj_match["match_suffix"] = "_pre_1y"   
            obj_match = self.df_dfA_match_col_from_dfB( obj_match)
            # 将结果赋值给 df_fund_stock_port
            df_fund_stock_port = obj_match["df_A"]
        
        ### 计算单边股票交易换手率：1年前;还需要导入基金的股票还手率数据 from ChinaMutualFundSeatTrading]
        obj_fund_pre = data_io_fund_ana_1.import_data_fund_profit_turnover(obj_fund_pre)
        
        obj_fund_pre["col_list_asset_allo"] = obj_fund_pre["col_list_asset_allo"] +["F_TRADE_STOCKAM","F_TRADE_STOCKPRO"]
        if not obj_fund_pre["if_missing_ap"] == 1 :
            df_fund_pre = obj_fund_pre["df_fund"]
            for temp_i in df_fund.index :
                temp_fund_code = df_fund.loc[temp_i, "fund_code" ]
                # find fund_code in obj_fund_6or12m["df_fund"]
                df_fund_pre_sub = df_fund_pre[ df_fund_pre["fund_code"]==temp_fund_code ]
                if len( df_fund_pre_sub.index ) > 0 :
                    # 剔除第一个日期 "ANN_DATE"                    
                    for temp_col in obj_fund_pre["col_list_asset_allo"]: 
                        df_fund.loc[temp_i, temp_col+"_pre_1y" ] = df_fund_pre_sub[temp_col].values[0]
            #
            df_fund["stock_turnover_stockvalue"+"_pre_1y"] = df_fund["F_TRADE_STOCKAM"+"_pre_1y"]*0.5 / df_fund["F_PRT_STOCKVALUE"+"_pre_1y"]
            df_fund["stock_turnover_fundasset"+ "_pre_1y" ] = df_fund["F_TRADE_STOCKAM"+"_pre_1y"]*0.5 / df_fund["F_PRT_NETASSET"+ "_pre_1y"] 
        
        ### save for df_fund_ind_weigt
        df_fund_pre_1y = obj_fund_pre["df_fund"]
        df_fund_stock_port_pre_1y = obj_fund_pre["df_fund_stock_port"]

        ###########################################################        
        ### 导入基金持仓数据，前2年 obj_fund["dict"]["date_report_pre_2y"] 
        obj_fund_pre["dict"]["date_report"] = date_q_pre_2y
        obj_fund_pre = data_io_fund_ana_1.import_data_fund_holdings(obj_fund_pre ) 
        ### 导入行业信息
        obj_fund_pre["dict"]["date_adj_port"] = date_q_pre # 这个赋值不是很确定。
        obj_fund_pre= data_io_fund_ana_1.import_data_fund_stock_name_listday_ind(obj_fund_pre )

        #判断是否有导入数据： obj_fund["if_missing_file"] = 1 
        if not obj_fund_pre["if_missing_sp"] == 1 :
            # notes:第二次取值只有df_A,df_B两项有变动。
            obj_match["df_A"] = df_fund_stock_port
            obj_match["df_B"] = obj_fund_pre["df_fund_stock_port"]  
            obj_match["match_suffix"] = "_pre_2y"     
            obj_match = self.df_dfA_match_col_from_dfB( obj_match)
            # 将结果赋值给 df_fund_stock_port
            df_fund_stock_port = obj_match["df_A"]

        ### 计算单边股票交易换手率：2年前;还需要导入基金的股票换手率数据 from ChinaMutualFundSeatTrading]
        obj_fund_pre = data_io_fund_ana_1.import_data_fund_profit_turnover(obj_fund_pre)
        obj_fund_pre["col_list_asset_allo"] = obj_fund_pre["col_list_asset_allo"] +["F_TRADE_STOCKAM","F_TRADE_STOCKPRO"]
        if not obj_fund_pre["if_missing_ap"] == 1 :
            df_fund_pre = obj_fund_pre["df_fund"]
            for temp_i in df_fund.index :
                temp_fund_code = df_fund.loc[temp_i, "fund_code" ]
                # find fund_code in obj_fund_6or12m["df_fund"]
                df_fund_pre_sub = df_fund_pre[ df_fund_pre["fund_code"]==temp_fund_code ]
                if len( df_fund_pre_sub.index ) > 0 :
                    # 剔除第一个日期 "ANN_DATE"
                    for temp_col in obj_fund_pre["col_list_asset_allo"]:
                        df_fund.loc[temp_i, temp_col+"_pre_2y" ] = df_fund_pre_sub[temp_col].values[0]
            # 
            df_fund["stock_turnover_stockvalue"+"_pre_2y"] = df_fund["F_TRADE_STOCKAM"+"_pre_2y"]*0.5 / df_fund["F_PRT_STOCKVALUE"+"_pre_2y"]
            df_fund["stock_turnover_fundasset"+ "_pre_2y" ] = df_fund["F_TRADE_STOCKAM"+"_pre_2y"]*0.5 / df_fund["F_PRT_NETASSET"+ "_pre_2y"] 

        ### save for df_fund_ind_weigt
        df_fund_pre_2y = obj_fund_pre["df_fund"]
        df_fund_stock_port_pre_2y = obj_fund_pre["df_fund_stock_port"]

        #################################################################################
        ### 3.1，是否长期持有；只要过去1、2年都持有过，就判定长期持有
        #看 "STOCK_PER_pre","STOCK_PER_pre_1y","STOCK_PER_pre_2y"
        def holding_2y(series_x) :
            result = 0 
            # notes:只要在df_fund_stock_port里的，"STOCK_PER"肯定为正。
            if "STOCK_PER_pre_2y" in series_x and "STOCK_PER_pre_1y" in series_x:
                if "STOCK_PER_pre" in series_x :
                    if series_x["STOCK_PER_pre"]>0 and (series_x["STOCK_PER_pre_1y"]>0 and series_x["STOCK_PER_pre_2y"]>0) :
                        result = 1 
            #         else :
            #             result = 2
            #     else :
            #         result = 3
            # else :
            #     result = 4

            return result 
        # 1 if x["STOCK_PER_pre"]>0 and (x["STOCK_PER_pre_1y"]>0 and x["STOCK_PER_pre_2y"]>0) else 0
        df_fund_stock_port["if_holding_2y"] = df_fund_stock_port.apply(lambda x : holding_2y(x) ,axis=1) 

        #################################################################################
        ### 3.2，交易频率：seattrading-基金换手率
        # notes:date_q_6or12m,date_q_pre_y1,date_q_pre_y2,3个日期都是6或12月，披露最近半年股票交易金额
        ### 判断是否最近季度是6或12月
        if not "F_TRADE_STOCKAM" in df_fund_stock_port.columns :                
            ### 1,导入最近一次6或12月半年度数据,并将相关数据赋值给df_fund
            obj_fund_6or12m={}
            obj_fund_6or12m["df_fund"]  = obj_fund["df_fund"] 
            obj_fund_6or12m["dict"]={}
            obj_fund_6or12m["fund_list"] = obj_fund["fund_list"]
            obj_fund_6or12m["dict"]["date_report"] = date_q_6or12m
            obj_fund_6or12m["dict"]["date_adj_port"] = date_q_6or12m
            obj_fund_6or12m = data_io_fund_ana_1.import_data_fund_holdings(obj_fund_6or12m ) 
            ### 导入行业信息：
            obj_fund_6or12m = data_io_fund_ana_1.import_data_fund_stock_name_listday_ind( obj_fund_6or12m)

            df_fund_6or12m = obj_fund_6or12m["df_fund"]
            df_fund_stock_port_6or12m = obj_fund_6or12m["df_fund_stock_port"]

            for temp_i in df_fund.index :
                temp_fund_code = df_fund.loc[temp_i, "fund_code" ]
                # find fund_code in obj_fund_6or12m["df_fund"]
                df_fund_6or12m_sub = df_fund_6or12m[ df_fund_6or12m["fund_code"]==temp_fund_code ]
                if len( df_fund_6or12m_sub.index ) > 0 :
                    # 剔除第一个日期 "ANN_DATE"
                    for temp_col in obj_fund_6or12m["col_list_asset_allo"]:
                        df_fund.loc[temp_i, temp_col ] = df_fund_6or12m_sub[temp_col].values[0]
        # else :
            ### 最近季度是6或12月，可以直接计算
        
        
        # 1,股票交易金额除以当期股票市值，2,股票交易金额除以当期净资产;计算单边
        if "F_TRADE_STOCKAM" in df_fund.columns :
            df_fund["stock_turnover_stockvalue"] =df_fund["F_TRADE_STOCKAM"]*0.5 / df_fund["F_PRT_STOCKVALUE"]
            df_fund["stock_turnover_fundasset"] =df_fund["F_TRADE_STOCKAM"]*0.5 / df_fund["F_PRT_NETASSET"]
                
        #################################################################################
        ### 3.3，期初，买入时机：成本价格对应的买入时机、买入时所处的百分位、买入时财务指标{pe,roe,growth}
        '''1,计算每只持仓股票x的期末市值变动金额stockvalue_change_report和百分比stockvalue_change_pct_report，根据：期初股票市值、最新市值、股价复权涨跌幅，
        Qs:如何判断区间内股票的平均成交价格？基金重仓股的仓位相对较重，减持一般需要一个过程。
        idea：可以考虑对区间内取高、中、低三个价格供后续分析
        而减持规模达到一定水平时才会在重仓基金数量变化中有所反应，从这个角度看，增减持可能比重仓的基金数量变化来的更加及时，因而对异常收益的区分也更加明显
        '''        
        
        ### 3.4，期末，卖出时机：实现收益对应的卖出时机,所处的百分位、财务指标{pe,roe,growth}
        '''notes:obj_fund["dict"]["col_list_stock_indicators"] 中的每一个指标，加上后缀"_pre"就是前一个季度的数据
        其中无后缀对应了披露日股价所处百分位和最近季度末财务指标，后缀"_pre"对应前一披露日和上一季度末财务指标
        1,股价所处百分位："close_pct_s_100"  ??还不确定是否有该列，from line213， data_io_pricevol_financial.py
        2，股价所处趋势："abcd3d" - "abcd3d_pre"
        3，市值、PE,PEG,ROE,净利润，收入和毛利，例如："NET_PROFIT_FY1" - "NET_PROFIT_FY1_pre" 等

        todo，需要在后续分析时再进行计算
        '''
        ### 有些obj_fund["dict"]["col_list_stock_indicators"]是不需要做比较的，例如 行业分布ind_code,是否交易状态等
        ### 以下是可以用于计算加减的列
        col_list_stock_indicators = ["S_DQ_ADJCLOSE","S_DQ_MV","S_VAL_MV","S_VAL_PE_TTM","S_VAL_PB_NEW","EST_OPER_REVENUE_FY1"]
        col_list_stock_indicators = col_list_stock_indicators+["NET_PROFIT_FY1","EST_PE_FY1","EST_PEG_FY1","EST_PB_FY1","EST_OPER_PROFIT_FY0","EST_OPER_REVENUE_FY0","EST_PB_FY0" ]
        col_list_stock_indicators = col_list_stock_indicators+["EST_PE_FY0","EST_PEG_FY0","EST_ROE_FY0","EST_TOTAL_PROFIT_FY0","NET_PROFIT_FY0","NET_PROFIT_YOY"]
        # col_list_stock_indicators = col_list_stock_indicators+["ma_s_16","ma_s_40","ma_s_100","abcd3d"]

        obj_fund["dict"]["col_list_stock_indicators"] = col_list_stock_indicators

        for temp_col in obj_fund["dict"]["col_list_stock_indicators"] :
            if temp_col+"_pre" in df_fund_stock_port.columns :
                df_fund_stock_port[temp_col+"_change"] = df_fund_stock_port[temp_col ]- df_fund_stock_port[temp_col+"_pre"]  

        #################################################################################
        ### 3.5，股票仓位变动：要剔除股票本身涨跌因素；
        ### 股票区间涨跌幅:_report,2个季末区间; _ann,2次披露日期区间; _gap,季末至披露区间，约3周或2个月不等。
        # df_stockpool_fund包括： ["period_pct_chg_date_report","period_pct_chg_date_ann","period_pct_chg_date_gap"]
        for temp_i in df_fund_stock_port.index :
            if df_fund_stock_port.loc[temp_i,"F_PRT_STKVALUE_pre"] > 0 :
                # find stock change pct from df_stockpool_fund
                df_sf = df_stockpool_fund[df_stockpool_fund["wind_code"] == temp_i ]
                if len(df_sf.index ) > 0 :
                    stockvalue_change_start = df_fund_stock_port.loc[temp_i,"F_PRT_STKVALUE_pre"]*( 1 + df_sf["period_pct_chg_date_ann"].values[0] )
                    df_fund_stock_port.loc[temp_i,"stockvalue_change_report" ] = df_fund_stock_port.loc[temp_i,"F_PRT_STKVALUE"] - stockvalue_change_start
                    df_fund_stock_port.loc[temp_i,"stockvalue_changepct_report" ] = df_fund_stock_port.loc[temp_i,"stockvalue_change_report" ]/ stockvalue_change_start
                else :
                    df_fund_stock_port.loc[temp_i,"stockvalue_change_report" ] = df_fund_stock_port.loc[temp_i,"F_PRT_STKVALUE"] 
                    df_fund_stock_port.loc[temp_i,"stockvalue_changepct_report" ] = 1.0    
            else :
                df_fund_stock_port.loc[temp_i,"stockvalue_change_report" ] = df_fund_stock_port.loc[temp_i,"F_PRT_STKVALUE"] 
                df_fund_stock_port.loc[temp_i,"stockvalue_changepct_report" ] = 1.0
        
        #################################################################################
        ### 3.6，基金持仓股票的行业分布、行业配置变动；从行业个股模拟行业收益，有可能的话用行业指数
        '''ana:行业分析的重点是：1，本基金不同时期行业配置变动；2，和几个基准指数行业配置的偏离；3，和不同分组（如公司整体，
        绩优基金、全市场）基金配置的偏离。
        4，分析周期看，将短中长期设置为最近6or12月底，1年前，2年前。
        基础指标：
            columns=["ind_code","citics_ind_code_s_1","citics_ind_code_s_2","citics_ind_code_s_3","citics_ind_name_1","citics_ind_name_2","citics_ind_name_3"]
            方案1：对df_fund建立基于中信1、3级行业的列，会导致df_fund越来越大，影响后续速度
            方案2：新建df_fund_ind_weigt，专门分析各个行业和细分行业配置，因为1个时期中信1、3级行业合起来就有100列，若4期就有400列。
            notes: df_fund里已经计算了最近一期基于中信一级行业的 ["allo_ind_code_"+ str(temp_count),"allo_ind_name_"+ str(temp_count)]
            在def fund_filter_group_fund(self,obj_fund)里。
        分析指标df：
            1，df_fund_ind_weigt:组合季末配置比例和短中长期前季末的配置变动，和不同基准指数配置比例差异；
            2，df_fund_ind_ret:组合持仓股票在当季度内收益，根据上一季末持仓还是当季度持仓计算？idea：可以假设季度内快速、匀速、慢速调仓；
            例如快速调仓指的是季度内第一个月调整50%,后2月分别25%，25%；匀速是季度内3个月分别33%，33%，34%。
            调整的内容是前后2个季度持仓股票的差值。
        notes:计算差值时要识别该季度对应的是披露top10还是全部持仓，top10必须和top10比，如果和全部持仓的行业分布比较容易出错。
        '''
        ##################################################
        ### 新建df_fund_ind_weigt,保存季度末股票组合内行业配置比例，df_fund_ind_ret是持仓股票的区间收益？如何计算
        def get_ind_weight(df_fund,df_fund_stock_port) :
            #####################################################
            ### 根据df_fund，df_fund_stock_port新建df_fund_ind_weigt
            # notes:df_fund_stock_port_6or12m 中可能没有行业代码信息
            col_list_ind = ["fund_code","F_INFO_CORP_FUNDMANAGEMENTCOMP"]
            df_fund_ind_weigt = df_fund.loc[:, col_list_ind ]

            #####################################################
            ### 导入中信1级和3级代码, 例如 40.0  
            col_list_ind1 = df_fund_stock_port["citics_ind_code_s_1"].drop_duplicates().to_list()
            col_list_ind3 = df_fund_stock_port["citics_ind_code_s_3"].drop_duplicates().to_list()       
            for temp_col in col_list_ind1 :
                col_list_ind = col_list_ind +[ temp_col]
                ### 新增columns： 行业权重列
                df_fund_ind_weigt[temp_col ] = 0 

            for temp_col in col_list_ind3 :
                col_list_ind = col_list_ind +[ temp_col]
                ### 新增columns： 行业权重列
                df_fund_ind_weigt[temp_col ] = 0 

            #####################################################
            ### 调整index            
            df_fund_ind_weigt.index = df_fund_ind_weigt[ "fund_code" ]

            #####################################################
            ### 将基金持仓股票对应的中信1、3级行业配置比例赋值给df_fund_ind_weigt ，from df_fund_stock_port
            for temp_f in df_fund_ind_weigt.index :
                temp_fund_code = df_fund_ind_weigt.loc[temp_f, "fund_code"]
                # fing fund holding stocks in df_fund_stock_port
                print("Debug== ",temp_f ,temp_fund_code  )
                if len(temp_fund_code)==9 and temp_fund_code[-3:]==".OF"  :
                    # 奇怪：temp_fund_code在末尾的取值竟然有 "fund_code"; len("000584.OF")=9,len("fund_code")=9
                    df_fsp = df_fund_stock_port[df_fund_stock_port["fund_code"]== temp_fund_code ]
                    if len( df_fsp.index ) > 0 :
                        for temp_i in df_fsp.index :
                            # 先对citics_ind_code_s_1 进行 groupby().sum()
                            for temp_col in ["citics_ind_code_s_1","citics_ind_code_s_2","citics_ind_code_s_3"] :
                                temp_df_sum = df_fsp.loc[:,[temp_col,"STOCK_PER"] ].groupby(temp_col).sum()
                                for temp_ind in temp_df_sum.index :
                                    df_fund_ind_weigt.loc[temp_fund_code, temp_ind ] = temp_df_sum.loc[temp_ind, "STOCK_PER" ]

            return df_fund_ind_weigt

        ### 1，最近一次半年度全部持仓的行业配置权重：df_fund_ind_weigt，专门分析各个行业和细分行业配置，因为1个时期中信1、3级行业合起来就有100列，若4期就有400列
        df_fund_ind_weigt = get_ind_weight(df_fund,df_fund_stock_port)
        # notes:df_fund_stock_port_6or12m 中可能没有行业代码信息！！ 
        df_fund_ind_weigt_6or12m = get_ind_weight(df_fund_6or12m,df_fund_stock_port_6or12m)

        ### 导入短、中、长期行业配置：1季度、1年、2年以前
        ### 短期：最近一个季度，有可能只有top10重仓股 || notes：如果用最近6or12m，有可能就是当季度：
        df_fund_ind_weigt_pre_1q = get_ind_weight(df_fund_pre_1q, df_fund_stock_port_pre_1q )
        ### 中期：
        df_fund_ind_weigt_pre_1y = get_ind_weight(df_fund_pre_1y, df_fund_stock_port_pre_1y )
        ### 长期：
        df_fund_ind_weigt_pre_2y = get_ind_weight(df_fund_pre_2y, df_fund_stock_port_pre_2y )
        
        #################################################################################
        ### 3.7，基金持仓股票风格配置比例：大小市值、财务角度的成长和价值 |分别基于季末日期和披露日期
        '''1,通过对持仓股票的分析，计算基金x在大、中、小+小微四档股票市值的配置比例，选期初股票市值；
        2,，历史财务角度的成长和价值；roe_pre_1y，profit_growth_1y；
        3，未来财务角度的成长和价值roe_fy1，profit_growth_fy1；
        逻辑：
        notes：参考factor_model的相关模块，多纳入一下多因子的指标。
        '''
        ######################################
        ### 总市值和流通市值
        ### 对上一季度末日期的所有股票进行市值分组并计算配置的比例，1~300,301~800,801~1800，
        # 从df_fund_stock_port 获取总市值和流通市值,"S_DQ_MV","S_VAL_MV"
        
        ### 导入季度初期全市场股票，并计算3个市值档位：300th,800th,1800th.
        table_name = "AShareEODDerivativeIndicator"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name + "\\"
        file_name = "WDS_TRADE_DT_" + str( int( obj_fund["dict"]["date_ann"] ) ) + "_ALL.csv"
        try :
            df_eod_allA = pd.read_csv(path_table + file_name , encoding="gbk")
        except :
            df_eod_allA = pd.read_csv(path_table + file_name ) 
        
        # 分别对流通市值、总市值降序排列分档 ； 
        list_rank=[ [1,300],[301,800],[801,1800],[1801,10000] ]
        df_rank= pd.DataFrame(list_rank, columns=["lb","ub"] )

        col_list = ["S_DQ_MV","S_VAL_MV"]
        df_temp = df_eod_allA.loc[:, col_list ]
        for col_name in col_list :
            # 按col_name 列的值降序排列并重设index从0,1,...
            df_temp = df_temp.sort_values(by=col_name ,axis=0, ascending=False ).reset_index()
            ### notes:在20060331时，quanA仅有1348只股票
            number_stocks = len( df_temp.index  )

            for temp_i in df_rank.index :
                temp_ub = df_rank.loc[temp_i, "ub"]
                if number_stocks > temp_ub : 
                    temp_value = df_temp.loc[temp_ub-1, col_name ]
                    # 赋值给df_rank
                    df_rank.loc[temp_i, col_name ] = temp_value

        ### Function：根据col_name,df_rank,返回col_value所属列区间对应的分组    
        def cal_rank(col_name,df_rank,col_value):
            ### 根据col_name,df_rank,返回col_value所属列区间对应的分组
            rank_level = 3
            rank_ub = 10000
            # index值中 1~300,301~800,801~1800,1800~10000 分别对应大、中、小、小微
            for temp_i in df_rank.index :                
                # 数值默认从大到小排列：
                if col_value >= df_rank.loc[temp_i, col_name ] :
                    rank_ub = min(rank_ub, df_rank.loc[temp_i, "ub"] )
                    # notes:如果只是temp_i,取值有可能从0 --> 1--> 2--> 3
                    rank_level = min(rank_level,temp_i)
            return rank_level,rank_ub

        def cal_weight_col_rank_level(col_list,stock_list_fund,df_fund_stock_port,df_fund) :
            ### 对持仓股票和基金计算所处的不同列值col_list(如市值)的分档
            for col_name in col_list :
                for temp_s in stock_list_fund:
                    # find stock holdings in df_fund_stock_port 
                    df_fsp = df_fund_stock_port [df_fund_stock_port["S_INFO_STOCKWINDCODE"]==temp_s ]
                    if len(df_fsp.index) > 0 :
                        # 计算持仓股票不同市值分档 | S_DQ_MV	S_VAL_MV
                        col_value = df_fsp[ col_name ].values[0]
                        (rank_level,rank_ub) = cal_rank(col_name,df_rank,col_value)
                        df_fund_stock_port.loc[df_fsp.index, col_name+"_rank" ] = rank_level
                
                for temp_i in df_fund.index :
                    temp_fund = df_fund.loc[temp_i, "fund_code"] 
                    # find stock holdings in df_fund_stock_port 
                    df_fsp = df_fund_stock_port [df_fund_stock_port["fund_code"]==temp_fund ]
                    if len(df_fsp.index) > 0 :
                        # 计算持仓股票不同市值分档
                        temp_df_sum = df_fsp.loc[:,[col_name+"_rank","STOCK_PER"] ].groupby(col_name+"_rank").sum()
                        for temp_rank in temp_df_sum.index :
                            # temp_df_sum.index 是 col_name+"_rank" 的值，column是
                            # 赋值给 df_fund
                            # notes: temp_rank 数值可能是 0.0，1.0，2.0
                            df_fund.loc[temp_i,"weight_"+ col_name+"_rank_"+str( int(temp_rank) )] = temp_df_sum.loc[temp_rank, "STOCK_PER" ]
            return df_fund

        df_fund = cal_weight_col_rank_level(col_list,stock_list_fund,df_fund_stock_port,df_fund)
        ############################################################################
        ### 成长性 vs 价值 | 数据来源表格：df_fund_stock_port，"AShareConsensusRollingData",但后缀"_FY0"和"_FY1"等需要自行计算
        ### 2，历史财务角度的成长和价值；"EST_PE_FY0"/"EST_PEG_FY0"和价值 "EST_PE_FY0" 
        # notes:在 这一期，会出现"EST_PEG_FY0","EST_PE_FY0"两个指标为空的情况
        for temp_i in df_fund_stock_port.index :
            df_fund_stock_port.loc[temp_i,"growth_fy0"] = 0.0
            df_fund_stock_port.loc[temp_i,"value_fy0"] = 0.0
            if not  pd.isnull( df_fund_stock_port.loc[temp_i, "EST_PE_FY0"] )==True :
                ###
                if not  pd.isnull( df_fund_stock_port.loc[temp_i, "EST_PEG_FY0"] )==True :
                    df_fund_stock_port.loc[temp_i,"growth_fy0"] = df_fund_stock_port.loc[temp_i, "EST_PE_FY0"]/ df_fund_stock_port.loc[temp_i, "EST_PEG_FY0"]
                ### 取值越大越好，因此需要PE倒数 1/PE
                df_fund_stock_port.loc[temp_i,"value_fy0"] = 1/df_fund_stock_port.loc[temp_i,"EST_PE_FY0"]

        # used method 
        # df_fund_stock_port["growth_fy0"] = df_fund_stock_port.apply(lambda x : df_fund_stock_port["EST_PE_FY0"]/df_fund_stock_port["EST_PEG_FY0"] if ( df_fund_stock_port["EST_PEG_FY0"]>0 and df_fund_stock_port["EST_PE_FY0"]>0 ))
        # df_fund_stock_port["value_fy0"] = df_fund_stock_port.apply(lambda x : 1/df_fund_stock_port["EST_PE_FY0"] if df_fund_stock_port["EST_PE_FY0"]>0 else 0.0   )     
        
        col_list = ["growth_fy0", "value_fy0"] 
        df_temp = df_fund_stock_port.loc[:, col_list ]
        for col_name in col_list :
            # 按col_name 列的值降序排列并重设index从0,1,...
            df_temp = df_temp.sort_values(by=col_name ,axis=0, ascending=False ).reset_index()
            ### notes:在20060331时，quanA仅有1348只股票
            number_stocks = len( df_temp.index  )
            
            for temp_i in df_rank.index :
                temp_ub = df_rank.loc[temp_i, "ub"]
                if number_stocks > temp_ub : 
                    temp_value = df_temp.loc[temp_ub-1, col_name ]
                    # 赋值给df_rank
                    df_rank.loc[temp_i, col_name ] = temp_value
        
        df_fund = cal_weight_col_rank_level(col_list,stock_list_fund,df_fund_stock_port,df_fund)
        
        ############################################################################
        ### 3，未来财务角度的成长 "EST_PE_FY1"/"EST_PEG_FY1"和价值 "EST_PE_FY1" 
        # notes:这一期，可能会出现"EST_PEG_FY1","EST_PE_FY1"两个指标为空的情况
        for temp_i in df_fund_stock_port.index :
            df_fund_stock_port.loc[temp_i,"growth_fy1"] = 0.0
            df_fund_stock_port.loc[temp_i,"value_fy1"] = 0.0
            if not  pd.isnull( df_fund_stock_port.loc[temp_i, "EST_PE_FY1"] )==True :
                ###
                if not  pd.isnull( df_fund_stock_port.loc[temp_i, "EST_PEG_FY1"] )==True :
                    df_fund_stock_port.loc[temp_i,"growth_fy1"] = df_fund_stock_port.loc[temp_i, "EST_PE_FY1"]/ df_fund_stock_port.loc[temp_i, "EST_PEG_FY1"]
                ### 取值越大越好，因此需要PE倒数 1/PE
                df_fund_stock_port.loc[temp_i,"value_fy1"] = 1/df_fund_stock_port.loc[temp_i,"EST_PE_FY1"]
        # used method
        # df_fund_stock_port["growth_fy1"] = df_fund_stock_port.apply(lambda x : df_fund_stock_port["EST_PE_FY1"]/df_fund_stock_port["EST_PEG_FY1"] if ( df_fund_stock_port["EST_PEG_FY1"]>0 and df_fund_stock_port["EST_PE_FY1"]>0 ) else 0.0 )
        # df_fund_stock_port["value_fy1"] = df_fund_stock_port.apply(lambda x : 1/df_fund_stock_port["EST_PE_FY1"] if df_fund_stock_port["EST_PE_FY1"]>0  else 0.0 )
        
        col_list = ["growth_fy1", "value_fy1"] 
        
        df_temp = df_fund_stock_port.loc[:, col_list ]
        for col_name in col_list :
            # 按col_name 列的值降序排列并重设index从0,1,...
            df_temp = df_temp.sort_values(by=col_name ,axis=0, ascending=False ).reset_index()
            ### notes:在20060331时，quanA仅有1348只股票
            number_stocks = len( df_temp.index  )

            for temp_i in df_rank.index :
                temp_ub = df_rank.loc[temp_i, "ub"]
                if number_stocks > temp_ub : 
                    temp_value = df_temp.loc[temp_ub-1, col_name ]
                    # 赋值给df_rank
                    df_rank.loc[temp_i, col_name ] = temp_value
        
        df_fund = cal_weight_col_rank_level(col_list,stock_list_fund,df_fund_stock_port,df_fund)
        
        ### save to obj_fund
        obj_fund["df_fund_stock_port"] = df_fund_stock_port
        obj_fund["df_fund"] = df_fund
        obj_fund["df_fund_ind_weigt"] = df_fund_ind_weigt
        obj_fund["df_fund_ind_weigt_6or12m"] = df_fund_ind_weigt_6or12m
        obj_fund["df_fund_ind_weigt_pre_1q"] = df_fund_ind_weigt_pre_1q
        obj_fund["df_fund_ind_weigt_pre_1y"] = df_fund_ind_weigt_pre_1y
        obj_fund["df_fund_ind_weigt_pre_2y"] = df_fund_ind_weigt_pre_2y
        
        return obj_fund   
        

    def fund_ana_nav_rank(self,obj_fund):
        ### 基金净值和排名分析  
        '''
        4，基金净值和排名分析,fund_ana_nav_rank：
        4.1，基金区间收益率：最近1季度、1年、3年区间每日净值增长率；
        4.2，区间每日净值增长率和不同指数的相关性：分析净值变动和不同基准组合{市值、行业、成长价值 }的相关性；
        4.3，统计基金公司整体收益和前20%、33%、50%收益。
        
        '''
        ###################################################################################
        ### 4，基金净值和排名分析,fund_ana_nav_rank：
        ### 4.1，基金区间收益率：最近1季度、1年、3年区间每日净值增长率；短期、中期、长期、年初至今四个基金排名指标
        col_list_perf_rank = obj_fund["dict"]["col_list_perf_rank"]         # 
        # col_list_perf_rank=["fund_rank_short","fund_rank_mid","fund_rank_long","fund_rank_ytd"]
        
        ### TODO，设计一个参数输入，可以对基金按排名筛选
        '''
        
        
        '''

        
        ### TODO

        return obj_fund 
    def fund_esti_port_stock_adjust(self,obj_fund):
        ### 基金持仓股票和调仓行为的仿真和预测  
        '''5，基金持仓股票和调仓行为的仿真和预测, fund_esti_port_stock_adjust ：
            5.1，根据持仓变动、历史持仓，滞后组合收益和净值的偏离，判断基金偏好的股票分组，例如行业、行业成长、小市值、基金重仓等{参考abcd3d的分组}；
            5.2，根据持仓变动、交易换手率、滞后组合收益和净值的偏离程度，判断基金区间调仓时机、股票品种和买卖权重。
        '''


        
        return obj_fund 
    def fund_stra_port_stock_alpha(self,obj_fund):
        ### 基金股票组合收益增强策略 
        '''
    6，基金股票组合收益增强策略,fund_stra_port_stock_alpha：
    6.1，精选基金：绩优基金组内超配组合；业绩差基金的持仓股的负面剔除组合。
    6.2，精选行业：绩优基金精选行业；
    6.3，逆向策略：选择只被少数绩优基金持有的股票。
        '''
        
        return obj_fund 
    def fund_port_weighting_adj(self,obj_fund):
        ### 基金收益增强模拟组合加权和调仓频率、交易成本 
        '''
        7，基金收益增强模拟组合加权和调仓频率、交易成本,fund_port_weighting_adj：
        7.1，等权重加权
        7.2，相对于基准超配权重加权
        7.3，考虑价量和财务因子加权
        7.4，组合调整：1，1年三次披露日期；2，每个月按最新打分
        '''
        
        
        return obj_fund 
    def fund_manage_perf_eval_group_esti(self,obj_fund):
        ### 基金仿真和调仓行为描述、全市场基金 
        '''
        8，基金仿真和调仓行为描述、全市场基金, fund_manage_perf_eval_group_esti：
        8.1，单只基金动态的持仓描述和调仓行为预测；
        8.2，不同分组，如绩优基金、行业、小市值等基金的持仓描述和调仓行为预测；
        8.3，基金调仓行为和股票价格的均值回归分析； 
            '''
        
        
        return obj_fund 

####################################################################################
### 通用标准功能
    def df_group_column_pct_level(self,obj_df):
        ### 对df内给定column和百分比参数计算分组
        '''input:df_fund,col_name,para_list_quantile_pct
        output:df_fund
        例子：para_list_quantile_pct = [0.05,0.2,0.3,0.5,1.0]
        '''
        df_fund = obj_df["df_input"]
        col_name = obj_df["col_name"]
        para_list_quantile_pct = obj_df["para_list_quantile_pct"]
        if not "para_list_quantile_pct" in obj_df.keys() :
            para_list_quantile_pct = [0.05,0.2,0.3,0.5,1.0]

        # new column
        df_fund[ col_name + "_level"] = -1
        ### 0.05对应是前95%的数值，0.7对应前30%的数值
        para_ub = 0.0
        
        for para_lb in para_list_quantile_pct :
            ### 确定百分比上下限
            if para_lb  == para_list_quantile_pct[0] :
                ### 0.05;取值前 0%~5%
                quantile_value_lb = df_fund[col_name ].quantile( 1-para_lb )
                # print("quantile_value_lb ",quantile_value_lb)
                df_fund_sub = df_fund[ df_fund[ col_name ]>= quantile_value_lb ]
            elif para_lb  == para_list_quantile_pct[-1] :
                ### 0.5~1.0, 取后50%
                para_ub = para_list_quantile_pct [ para_list_quantile_pct.index(para_lb ) -1  ]
                quantile_value_ub = df_fund[col_name ].quantile( 1-para_ub )
                df_fund_sub = df_fund[ df_fund[ col_name ] < quantile_value_ub ]
                # print("quantile_value_ub ",quantile_value_ub)
            else :
                para_ub = para_list_quantile_pct [ para_list_quantile_pct.index(para_lb ) -1  ]
                quantile_value_ub = df_fund[col_name ].quantile( 1-para_ub )
                quantile_value_lb = df_fund[col_name ].quantile( 1-para_lb )
                df_fund_sub = df_fund[ df_fund[ col_name ] < quantile_value_ub ]
                df_fund_sub = df_fund_sub[ df_fund_sub[ col_name ] >= quantile_value_lb ]
                # print("quantile_value_ub ",quantile_value_ub)
                # print("quantile_value_lb ",quantile_value_lb)

            if len( df_fund_sub.index ) > 0 :
                df_fund.loc[df_fund_sub.index, col_name+ "_level"] = para_list_quantile_pct.index(para_lb )

        obj_df["df_input"] = df_fund
        return obj_df
    
    def df_dfA_match_col_from_dfB(self,obj_match):
        ### 将df_B内的部分列col_list匹配至最新df_A内，匹配依据是 基金和股票代码
        # df_A = df_fund_stock_port,df_B =df_fund_stock_port_pre
        df_A = obj_match["df_A"]
        df_B = obj_match["df_B"]
        col_list_match = obj_match["col_list_match"]
        # col_1 ="fund_code" ;col_2 = "S_INFO_STOCKWINDCODE"
        col_1 = obj_match["col_1"] 
        col_2 = obj_match["col_2"] 
        # match_suffix ="_pre"
        match_suffix = obj_match["match_suffix"]
        if_diff = obj_match["if_diff"]

        # notes：需要对每只基金内的股票进行匹配，不能批量操作。
        for temp_fund_code in obj_match["fund_list"] :
            # 在当季 df_A = df_fund_stock_port中定位股票
            df_fsp_sub = df_A [df_A [ col_1 ]== temp_fund_code]
            if len(df_fsp_sub.index ) > 0 :
                for temp_i in df_fsp_sub.index :
                    temp_stock_code = df_fsp_sub.loc[temp_i, col_2 ]
                    ### locate fund_stock at df_B = df_fund_stock_port_pre 
                    df_fsp_sub_pre = df_B[df_B[ col_1 ]==temp_fund_code]
                    if len(df_fsp_sub_pre.index ) > 0 :
                        df_fsp_sub_pre = df_fsp_sub_pre[ df_fsp_sub_pre[col_2 ]==temp_stock_code]
                        if len(df_fsp_sub_pre.index ) > 0 :
                            for temp_col in col_list_match :
                                #match_suffix="_pre"
                                df_A.loc[temp_i, temp_col+ match_suffix ] = df_fsp_sub_pre[temp_col].values[0]
                                # obj_match["if_diff"] =1 意味着计算和之前的差值
                            if if_diff ==1 :
                                # 第一个是日期，不计算
                                for temp_col in col_list_match[1:] :
                                    if df_fsp_sub_pre[temp_col].values[0] > 0 :
                                        df_A.loc[temp_i, temp_col+ match_suffix+"_diffpct" ] =df_A.loc[temp_i, temp_col ]/df_A.loc[temp_i, temp_col+ match_suffix ]-1
                                    else :
                                        df_A.loc[temp_i, temp_col+ match_suffix+"_diffpct" ] =1.0

        obj_match["df_A"] = df_A
        return obj_match
