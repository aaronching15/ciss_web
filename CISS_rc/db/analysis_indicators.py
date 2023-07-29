# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
TODO：简化class indicator_ashares(),把数据导入导出的部分转到 data_io_financial_indicator.py

功能：1，导入外部数据，计算分析过程并生成指标和因子等。
Function:serve as estimated input for strategy calculation 
last 200327 | since 181109

Class列表：
父类：class indicators():
子类1：class indicator_ashares():A股指标和因子 
    1，市场、行业和主题指数：价格和成交量；ashares_index_price_vol
    2，市场、行业和主题指数：行业分类、主题分类、行业基本面数据等；ashares_index_funda
    3，个股：价格和成交量；ashares_stock_price_vol
    4，个股：财务和财务预测指标；ashares_stock_funda
    5，个股：股东、机构投资者、收购兼并等事件；ashares_stock_holder_events
    6，基金、机构指标和因子；ashares_fund_nav_port 
子类2：class analysis_factor():因子数据分析     
        print("指标和因子数据处理") 
        print("indicator_data_adjust_zscore  |指标数据清洗调整：去异常值和缺失值；标准化") 
        print("indicator_indicator_orthogonal  |因子指标正交处理") 
        print("indicator_indicator_icir  |因子指标IC和ICIR计算 ") 
子类3： 动量指标 class indicator_momentum():


Notes: 
refernce: rC_Stra_MAX.py 
===============================================
'''
import sys,os
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

# 当前目录 C:\rc_2023\rc_202X\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

# sys.path.append("..")
### Import config
from config_data import config_data
config_data_1 = config_data()
from config_indicator import config_indi_financial
config_indi_financial_1 = config_indi_financial()

from data_io_financial_indicator import data_io_financial_indicator
data_io_financial_indicator_1 = data_io_financial_indicator()
# 交易日list： data_io_financial_indicator_1.obj_data_io["dict"]["tradingday"]
# 交易周list： data_io_financial_indicator_1.obj_data_io["dict"]["date_list_tradingday"]

#########################################################
class indicators():
    def __init__(self ):
        #########################################################
        ### 早期对象地址
        self.config_data_1 = config_data_1
        ######################################################################
        ### level 1 目录； ['C:\\zd_zxjtzq\\ciss_web', 
        #  设置数据文件位置
        self.path_ciss_web = config_data_1.obj_config["dict"]["path_ciss_web"]
        self.file_path_admin = self.path_ciss_web + "apps\\rc_data\\"

        ######################################################################
        ### level 1 目录:设置db_wind 数据文件位置，只读取数据
        self.path0 = config_data_1.obj_config["dict"]["path_db_wind"]
        ### level 2 目录
        self.path_wind_adj =  config_data_1.obj_config["dict"]["path_wind_adj"]
        self.path_wind_wds =  config_data_1.obj_config["dict"]["path_wind_wds"]

        ### level 3 目录
        # 导入Wind全历史行业分类数据 || df_600151.SH
        self.path_wind_adj_ind =  config_data_1.obj_config["dict"]["path_rc_ind"] 

        ######################################################################


#########################################################
class indicator_ashares(): 
    # 类的初始化操作
    def __init__(self):
        ### 继承父类indicators的定义，等价于 
        indicators.__init__(self)
        #################################################################################
        ### Initialization 

        ### 导入date_list, 
        # 交易日list： data_io_financial_indicator_1.obj_data_io["dict"]["tradingday"]
        # 交易周list： data_io_financial_indicator_1.obj_data_io["dict"]["date_list_tradingday"]
        # 匹配date_start前最近的交易日期 date_0，匹配date_end前最近的交易日期 date_1
        # notes: "data_check_anndates.csv"中的日期包括了节假日等非A股交易日，比如0501五一节
        # file_name = "date_list_tradingdate_ashares.csv"
        # df_dates = pd.read_csv(self.file_path_admin + file_name  )
        # type of date_list is numpy.int64
        self.date_list = data_io_financial_indicator_1.obj_data_io["dict"]["date_list_tradingday"]

        #################################################################################

    def print_info(self):
        ### print all modules for current script
        print("多资产-A股，class indicator_ashares")
        print("1，市场、行业和主题指数：导入指数成分和权重；ashares_index_constituents")
        print("1，市场、行业和主题指数：价格和成交量；ashares_index_price_vol")
        print("2，市场、行业和主题指数：行业分类、主题分类、行业基本面数据等；ashares_index_funda")
        print("3，个股：价格和成交量；ashares_stock_price_vol")
        print("3.1，个股：价格和成交量子集：累计收益率,平均收益率，最大回撤，波动率；ashares_stock_price_vol_sub " )
        print("3.2，个股：区间价格变动和涨跌幅：ashares_stock_price_vol_change " )  
        
        print("4，个股：市值，财务和财务预测指标；ashares_stock_funda")
        print("5，个股：股东、机构投资者、收购兼并等事件；ashares_stock_holder_events")
        print("6，基金、机构指标和因子；ashares_fund_nav_port")

        return 1 

    #################################################################################
    ### 1，市场、行业和主题指数：导入指数成分和权重；
    def ashares_index_constituents(self,obj_in) :
        ### 根据日期和指数代码，获取指数成分股
        '''
        obj_in["date_start"] = "20050501"
        obj_in["code_index"] = "000300.SH"
        obj_in["table_name"] = "AIndexHS300FreeWeight"
        '''
        date_start = obj_in["date_start"]
        date_list_new = [date for date in self.date_list if date<= int(date_start) ]
        
        date_0 = date_list_new[-1]
        file_name = "WDS_TRADE_DT_"+ str(date_0) +"_ALL.csv"
        print("file_name ",file_name )
        df0 = pd.read_csv(self.path_wind_wds + obj_in["table_name"]+"\\"+ file_name  )
        # print("df0" ,df0.head().T )
        df1= df0[ df0["S_INFO_WINDCODE"] == obj_in["code_index"]  ]
        # print("df1" ,df1.head().T )

        obj1={}

        obj_in["df_ashares_index_consti"] = df1

        return obj_in

    #################################################################################
    ### 1，市场、行业和主题指数：价格和成交量
    def ashares_index_price_vol(self) :
        # 1，市场、行业和主题指数：价格和成交量
        


        return 1

    def ashares_index_price_vol_sub(self,obj_in ) :
        # 1.1，几种类型：时点数据，区间数据
        ''' obj_in={}
        obj_in["type_date"] = 1 means 1date ;2 means period
        obj_in["date_start"] = "20050501"
        obj_in["date_end"] = "20050701"
        obj_in["type_indi"] = 1 means price ;2 means vol
        obj_in["code"] = "000300.SH"
        obj_in["table_name"] = "AIndexEODPrices"
        
        notes:
        1,沪深300指数的全收益指数H000300.SH推出日期是 20060405
        指数默认是不考虑分红的那部分钱的。我们平时看到的上证50、沪深300、恒生、H股指数等等，都是把每年的分红排除在外的点数。
        不考虑分红和把分红再投入，这两者收益会有很大差别。指数公司也考虑了这一点，所以也设计了全收益指数。全收益指数会默认把分红再投入考虑进来。
        2,股票通常是按列表读取数据，指数通常只读取1个
        '''
        type_date = obj_in["type_date"]
        date_start = obj_in["date_start"] 
        date_end = obj_in["date_end"] 
        type_indi = obj_in["type_indi"] 
        wind_code = obj_in["code"] 
        table_name = obj_in["table_name"] 
        info = 'wind_code: {wind_code},date_start: {date_start} ,date_end: {date_end}'.format(wind_code=wind_code,date_start=date_start,date_end=date_end) 
        print(info)
        ### 1date 时点情况：


        ### period区间情况：
        ### 1，导入相关数据，生成[T,T+1]区间每个交易日数据df 

        # date_list_new = [date for date in date_list if date<= int(date_start)  ]
        # date_0 = date_list_new[-1]
        date_list_new = [date for date in self.date_list if date>= int(date_start) and date<= int(date_end) ]
        
        
        ### 读取date_0 ~ date_1 期间每个交易日的指数数据
        count_date = 0 
        for temp_date in date_list_new :
            #type(temp_date ) = int
            
            # WDS_TRADE_DT_20050104_ALL.csv
            file_name = "WDS_TRADE_DT_"+ str( temp_date ) +"_ALL.csv"
            df0 = pd.read_csv( self.path_wind_wds +table_name+"\\"+ file_name  )
            df1= df0[ df0["S_INFO_WINDCODE"]== wind_code ]
            if count_date == 0 :
                df_index_price = df1 
                count_date = 1 
            else :
                df_index_price = df_index_price.append(df1,ignore_index=True  ) 

        df_index_price =df_index_price.sort_values(by="TRADE_DT")
        ### 2，计算区间日均涨跌幅，累计涨跌幅，日均波动率；周
        # notes：用S_DQ_CHANGE	S_DQ_PCTCHANGE 两个计算的每日收益率是一样的
        # 这里的百分比变动需要除以100， "S_DQ_PCTCHANGE"
        preclose_0 = df_index_price.loc[df_index_price.index[0],"S_DQ_PRECLOSE"  ]
        # 方法一，计算绝对点位累计变动后，加上期初值，再统一除以首日前收盘价
        # df_index_price["S_DQ_CHANGE_cumsum"] = df_index_price["S_DQ_CHANGE"].cumsum()
        # df_index_price["pct_chg_accu"] = df_index_price["S_DQ_CHANGE_cumsum"]/preclose_0
        # 方法二：对每日涨跌幅 除以100加上1后累乘，最后再减1
        df_index_price["S_DQ_PCTCHANGE_cumprod"] = df_index_price["S_DQ_PCTCHANGE"]/100 +1 
        df_index_price["ret_pct_accu"] =df_index_price["S_DQ_PCTCHANGE_cumprod"].cumprod() -1

        ### TODO 如何根据日期频率date_freq，计算周/月/季度的平均收益率和波动率？？？
        ### 累计收益率,平均收益率，最大回撤，波动率 || 
        ret_pct_accu = df_index_price.loc[ df_index_price.index[-1] ,"ret_pct_accu"]
        ret_pct_ave = df_index_price["S_DQ_PCTCHANGE"].mean() /100
        ret_pct_std = (df_index_price["S_DQ_PCTCHANGE"]/100).std() 
        ### 最大回撤 mdd计算 url https://blog.csdn.net/weixin_38997425/article/details/82915386
        mdd= 0.0 
        for temp_i in df_index_price.index :
            temp_mdd = (1+df_index_price.loc[temp_i, "ret_pct_accu"]) / (1+ df_index_price.loc[:temp_i, "ret_pct_accu"].max())-1
            mdd = min( mdd,temp_mdd )
        print("mdd", mdd)
        print("ret_pct_accu ret_pct_ave ,ret_pct_std,mdd",ret_pct_accu,ret_pct_ave ,ret_pct_std,mdd)

        ### to csv
        # df_index_price.to_csv("D:\\df_index_price.csv")
        
        obj_out ={}
        obj_out["df_index_price"] = df_index_price
        obj_out["df_index_price"] = df_index_price
        obj_out["ret_pct_accu"] =ret_pct_accu
        obj_out["ret_pct_ave"] =ret_pct_ave
        obj_out["ret_pct_std"] =ret_pct_std
        obj_out["mdd"] =mdd

        return obj_out 

    #################################################################################
    def ashares_index_funda(self) :
        # 2，市场、行业和主题指数：行业分类、主题分类、行业基本面数据等；



 









        return 1

    #################################################################################
    def ashares_stock_price_vol(self,obj_in) :
        ### 3，个股：价格和成交量等指标数据
 








        obj_out = obj_in
        return obj_out

    def ashares_stock_price_vol_sub(self,obj_in ) :
        ### 3.1，个股：价格和成交量子集：累计收益率,平均收益率，最大回撤，波动率
        # 1.1，几种类型：时点数据，区间数据
        ''' obj_in={}
        obj_in["date_start"] = "20050501"
        obj_in["table_name"] = "AShareEODPrices" 
        obj_in["df_factor"] = df_factor  ;"wind_code"  in columns

        Input:
        Inidicators:
        近1个月日均成交额比近6个月日均成交额均值 amt_ave_1m_6m = amt_ave_1m/amt_ave_6m
        近1个月日均换手率比近6个月日均换手率均值 turnover_ave_1m_6m= turnover_ave_1m/turnover_ave_6m
        近1个月日均波动率比近6个月日均波动率标准差 volatility_std_1m_6m= volatility_std_1m/volatility_std_6m
        20天移动平均价格比120天移动平均价格 ma_20d_120d = ma_20d/ma_120d
        20天平均涨跌幅比120天平均涨跌幅 ret_averet_ave_20d_120d = ret_averet_ave_20d/ret_averet_ave_120d
        20天累计涨跌幅比120天累计涨跌幅 ret_accumu_20d_120d = ret_accumu_20d/ret_accumu_120d
        20天内最大回撤比120天内最大回撤 ret_mdd_20d_120d = (1+ret_mdd_20d)/((1+ret_mdd_120d)
        收盘价在过去52周内百分比 close_pct_52w = 52周最高价(复权)，S_PQ_ADJHIGH_52W/52周最低价(复权)，S_PQ_ADJLOW_52W
        收盘价在过去52周内百分比 close_pct_52w = (close_52w_last -close_52w_low)/(close_52w_high -close_52w_low)
        Output:

        notes:
        1，股票通常是按列表读取数据，指数通常只读取1个
        2，"AShareEODPrices" 重要指标：
            S_DQ_CLOSE 当日收盘价
            S_DQ_ADJPRECLOSE 后复权前一日收盘价
            S_DQ_ADJCLOSE 后复权当日收盘价
            S_DQ_ADJFACTOR：复权因子= S_DQ_ADJCLOSE/ S_DQ_CLOSE
        ''' 
        date_start = obj_in["date_start"] 
        table_name = obj_in["table_name"] 
        df_factor = obj_in["df_factor"] 
        table_name_derivative = "AShareEODDerivativeIndicator" 
        table_name_index_price = "AIndexEODPrices"

        info = 'table_name: {table_name},date_start: {date_start}  '.format(table_name=table_name,date_start=date_start ) 
        print(info)
        '''TODO 根据日行情数据，计算前推N日的区间收益率、波动率、最大回撤等指标
        
        '''
        ###########################################################################
        ### 1，导入相关数据，生成[T-N,T]区间N个交易日数据df,
        # notes:对于code_list 种的部分新上市股票，上市日期可能低于N日
        N = 120
        date_list_pre = [date for date in self.date_list if date<= int(date_start)  ]
        # 例：现有数据起始日为20050104，在20050501前推5个月仅有77个交易日
        N = min( N,  len(date_list_pre) )
        date_list_pre = date_list_pre[ -1*N:  ]
        date_last = date_list_pre[-1]

        print("Previous N days:", N, len(date_list_pre),date_list_pre )
        
        ###########################################################################
        ### 对每只股票新建df_dates,所有df_dates保存在obj_stock_dates[temp_code] 。
        # dates as index and indicator as columns   
        # 以下是计算过程和结果所有指标，因子指标只有一部分。    
        col_list = ["close_52w_last","close_52w_low", "close_52w_high" ,"close_pct_52w"]
        col_list = col_list +["amt_ave_1m","amt_ave_6m","amt_ave_1m_6m","turnover_ave_1m","turnover_ave_6m","turnover_ave_1m_6m" ]
        col_list = col_list +["volatility_std_1m","volatility_std_6m","volatility_std_1m_6m"]
        col_list = col_list +["ret_averet_ave_20d","ret_averet_ave_120d","ret_averet_ave_20d_120d"]
        col_list = col_list +["ma_20d","ma_120d","ma_20d_120d","ret_accumu_20d","ret_accumu_120d","ret_accumu_20d_120d" ]
        col_list = col_list +["ret_mdd_20d","ret_mdd_120d","ret_mdd_20d_120d"]
        col_list = col_list +["ret_alpha_ind_citic_1_20d","ret_alpha_ind_citic_1_120d"]
        col_list = col_list +["ret_alpha_stockpool_mv_20d","ret_alpha_stockpool_mv_120d"]
        col_list = col_list +["ret_alpha_index_bm_20d","ret_alpha_index_bm_120d"]
        
        # obj_stock_dates = {}
        df_dates_stocks = pd.DataFrame( index=date_list_pre, columns= ["0"] )
        df_dates_index = pd.DataFrame( index=date_list_pre, columns= ["0"] )

        for temp_i in df_factor.index :
            temp_code = df_factor.loc[temp_i, "wind_code"  ]
            for temp_col in col_list :
                df_dates_stocks [ temp_code+"_"+temp_col ] = 0.0

        ###########################################################################
        ### 读取[T-N,T]区间期间每个交易日的股票日行情数据
        # 导入wds表格的columns.csv
        df_cols = pd.read_csv(self.path_wind_wds + obj_in["table_name"]+"\\"+ "columns.csv"  )
        col_list_1d = df_cols["0"].values
        code_index = df_factor["code_index"].values[0]
        for temp_date in date_list_pre :
            print("temp_date",temp_date )
            ### 1,读取个股交易日行情数据   WDS_TRADE_DT_20050104_ALL.csv
            file_name = "WDS_TRADE_DT_"+ str( temp_date ) +"_ALL.csv"
            try :
                df0 = pd.read_csv( self.path_wind_wds +table_name+"\\"+ file_name  )
            except :
                df0 = pd.read_csv( self.path_wind_wds +table_name+"\\"+ file_name,encoding="gbk"  )

            for temp_i in df_factor.index :
                temp_code = df_factor.loc[temp_i, "wind_code"  ]
                df1= df0[ df0["S_INFO_WINDCODE"]== temp_code ]
                
                if len( df1.index ) > 0 :
                    temp_j = df1.index[0]
                    for temp_col in col_list_1d :
                        df_dates_stocks.loc[temp_date, temp_code+"_"+temp_col ]= df1.loc[ temp_j , temp_col ]
                else :
                    for temp_col in col_list_1d :
                        df_dates_stocks.loc[temp_date, temp_code+"_"+temp_col ]= np.nan
            
            ### 2,读取个股日衍生行情里的换手率指标            
            # 换手率，S_DQ_TURN，NUMBER(20,4)，%，换手率(基准.自由流通股本)，S_DQ_FREETURNOVER
            # notes:001872.SZ在200501-200505之间没有日衍生行情，但股票1993年就上市了。由于发生了收购兼并，日衍生行情在20171225之后才有数据。

            temp_col ="S_DQ_FREETURNOVER"
            file_name = "WDS_TRADE_DT_"+ str( temp_date ) +"_ALL.csv"
            df0 = pd.read_csv( self.path_wind_wds +table_name_derivative  +"\\"+ file_name  )
            for temp_i in df_factor.index :
                temp_code = df_factor.loc[temp_i, "wind_code"  ]
                df1= df0[ df0["S_INFO_WINDCODE"]== temp_code ]
                
                if len( df1.index ) > 0 :                    
                    temp_j = df1.index[0]
                    df_dates_stocks.loc[temp_date, temp_code+"_"+temp_col ]= df1.loc[ temp_j , temp_col ]
                else :
                    df_dates_stocks.loc[temp_date, temp_code+"_"+temp_col ]= np.nan
            
            ### 3 读取指数交易日行情数据   WDS_TRADE_DT_20050104_ALL.csv
            # table_name_index_price = "AIndexEODPrices"
            file_name = "WDS_TRADE_DT_"+ str( temp_date ) +"_ALL.csv"
            df0 = pd.read_csv( self.path_wind_wds +table_name_index_price +"\\"+ file_name  )
            
            # code_index = df_factor["code_index"].values[0] 
            df1= df0[ df0["S_INFO_WINDCODE"]== code_index ] 
            if len( df1.index ) > 0 :
                temp_j = df1.index[0]
                ## 对于指数，只需要三个：收盘价(点)，S_DQ_CLOSE；涨跌幅(%)，S_DQ_PCTCHANGE；成交金额(千元)，S_DQ_AMOUNT
                for temp_col in ["S_DQ_CLOSE","S_DQ_PCTCHANGE","S_DQ_AMOUNT"  ] :
                    df_dates_stocks.loc[temp_date, code_index+"_"+temp_col ]= df1.loc[ temp_j , temp_col ]
            else :
                for temp_col in col_list_1d :
                    df_dates_stocks.loc[temp_date, code_index+"_"+temp_col ]= np.nan
        
        ###########################################################################

        ### 指标计算，对每只个股的历史日行情序列               
        for temp_i in df_factor.index :
            temp_code = df_factor.loc[temp_i, "wind_code"  ] 
            print("temp_code",temp_code )
            #################################################################################
            ### 收盘价在过去52周内百分比 close_pct_52w = (close_52w_last -close_52w_low)/(close_52w_high -close_52w_low)
            # 这是最简单的，不需要历史时间序列 | "S_PQ_ADJHIGH_52W"== "close_52w_high";"S_PQ_ADJLOW_52W"=="close_52w_low";
            # notes:"S_PQ_ADJHIGH_52W"s是在df_factor之前已经导入了的指标
            temp_col_high = "S_PQ_ADJHIGH_52W"
            temp_col_low = "S_PQ_ADJLOW_52W"
            temp_col = "S_DQ_CLOSE_TODAY"
            df_factor.loc[temp_i, "close_52w_high"] = df_factor.loc[temp_i, "S_PQ_ADJHIGH_52W"]
            df_factor.loc[temp_i, "close_52w_low"] = df_factor.loc[temp_i, "S_PQ_ADJLOW_52W"]
            df_factor.loc[temp_i, "close_52w_last"] =  df_factor.loc[temp_i, "S_DQ_CLOSE_TODAY"]
            df_factor.loc[temp_i, "close_pct_52w"] = (df_factor.loc[temp_i, "close_52w_last"]- df_factor.loc[temp_i, "close_52w_low"] )/(df_factor.loc[temp_i, "close_52w_high"]  -df_factor.loc[temp_i, "close_52w_low"] )
            
            # notes:若一列中有np.nan,计算平均值时会自动忽略。
            #################################################################################
            ### 近1个月日均成交额比近6个月日均成交额均值 amt_ave_1m_6m = amt_ave_1m/amt_ave_6m
            # 成交金额(千元)，S_DQ_AMOUNT
            temp_col = "S_DQ_AMOUNT"
            # if "amt_ave_1m" in df_factor.columns and "amt_ave_6m" in df_factor.columns :
            df_factor.loc[temp_i, "amt_ave_1m"] = df_dates_stocks.loc[ date_list_pre[-20:]  ,temp_code+"_"+temp_col].mean()
            df_factor.loc[temp_i, "amt_ave_6m"] = df_dates_stocks.loc[date_list_pre[-1*N:] ,temp_code+"_"+temp_col].mean()
            df_factor.loc[temp_i, "amt_ave_1m_6m"] = df_factor.loc[temp_i, "amt_ave_1m"]/df_factor.loc[temp_i, "amt_ave_6m"]
            #################################################################################
            ### 近1个月日均换手率比近6个月日均换手率均值 turnover_ave_1m_6m= turnover_ave_1m/turnover_ave_6m
            # 换手率(基准.自由流通股本)，S_DQ_FREETURNOVER from 中国A股日行情估值指标[AShareEODDerivativeIndicator]
            temp_col = "S_DQ_FREETURNOVER"
            df_factor.loc[temp_i, "turnover_ave_1m"] = df_dates_stocks.loc[ date_list_pre[-20:]  ,temp_code+"_"+temp_col].mean()
            df_factor.loc[temp_i, "turnover_ave_6m"] = df_dates_stocks.loc[date_list_pre[-1*N:] ,temp_code+"_"+temp_col].mean()
            df_factor.loc[temp_i, "turnover_ave_1m_6m"] = df_factor.loc[temp_i, "turnover_ave_1m"]/df_factor.loc[temp_i, "turnover_ave_6m"]
            #################################################################################
            ### 近1个月日均波动率比近6个月日均波动率标准差 volatility_std_1m_6m= volatility_std_1m/volatility_std_6m
            # 波动率 ：涨跌幅(%)，S_DQ_PCTCHANGE
            temp_col = "S_DQ_PCTCHANGE"
            df_factor.loc[temp_i, "volatility_std_1m"] = df_dates_stocks.loc[date_list_pre[-20:] ,temp_code+"_"+temp_col].std()
            df_factor.loc[temp_i, "volatility_std_6m"] = df_dates_stocks.loc[date_list_pre[-1*N:] ,temp_code+"_"+temp_col].std()
            df_factor.loc[temp_i, "volatility_std_1m_6m"] =df_factor.loc[temp_i, "volatility_std_1m"] /df_factor.loc[temp_i, "volatility_std_6m"] 

            ### 20天平均涨跌幅比120天平均涨跌幅 ret_averet_ave_20d_120d = ret_averet_ave_20d/ret_averet_ave_120d
            df_factor.loc[temp_i, "ret_averet_ave_20d"] = df_dates_stocks.loc[date_list_pre[-20:] ,temp_code+"_"+temp_col].mean()
            df_factor.loc[temp_i, "ret_averet_ave_120d"] = df_dates_stocks.loc[date_list_pre[-1*N:] ,temp_code+"_"+temp_col].mean()
            df_factor.loc[temp_i, "ret_averet_ave_20d_120d"] =df_factor.loc[temp_i, "ret_averet_ave_20d"] /df_factor.loc[temp_i, "ret_averet_ave_120d"] 
            #################################################################################
            ### 20天移动平均价格比120天移动平均价格 ma_20d_120d = ma_20d/ma_120d
            # 复权收盘价(元),S_DQ_ADJCLOSE
            temp_col = "S_DQ_ADJCLOSE"
            df_factor.loc[temp_i, "ma_20d"] = df_dates_stocks.loc[date_list_pre[-20:] ,temp_code+"_"+temp_col].mean()
            df_factor.loc[temp_i, "ma_120d"] = df_dates_stocks.loc[date_list_pre[-1*N:] ,temp_code+"_"+temp_col].mean()
            df_factor.loc[temp_i, "ma_20d_120d"] =df_factor.loc[temp_i, "ma_20d"] /df_factor.loc[temp_i, "ma_120d"] 
            #################################################################################
            ### 20天累计涨跌幅比120天累计涨跌幅 ret_accumu_20d/ret_accumu_120d
            # 如果期初日无数值，则日涨跌幅或日收盘价都算不出来累计收益 |  "S_DQ_PCTCHANGE",S_DQ_ADJCLOSE
            temp_col = "S_DQ_ADJCLOSE"
            
            # temp_n个交易日，替代N，删除NaN所在行
            temp_df_close_adj = df_dates_stocks.loc[ : , temp_code+"_"+temp_col]
            temp_df_close_adj = temp_df_close_adj.dropna( axis=0, how="all" ).to_frame()
            date_list_sub = temp_df_close_adj.index

            # 若总记录数量小于N=120,如77，则提取前77个记录
            temp_n = min(N,len(temp_df_close_adj.index ))
            # 若总记录数量小于N=20,如19，则提取前19个记录
            temp_n_20 = min(20 ,len(temp_df_close_adj.index ))

            temp_close_pre_1  =  temp_df_close_adj.loc[date_list_sub[-1] ,temp_code+"_"+temp_col]
            temp_close_pre_20 =  temp_df_close_adj.loc[date_list_sub[-1*temp_n_20 ] ,temp_code+"_"+temp_col] 
            temp_close_pre_N  =  temp_df_close_adj.loc[date_list_sub[0] ,temp_code+"_"+temp_col]  

            print("type temp_df_close_adj", temp_close_pre_1/ temp_close_pre_20 ,temp_close_pre_1/ temp_close_pre_N  )

            df_factor.loc[temp_i, "ret_accumu_20d"] = temp_close_pre_1/ temp_close_pre_20 -1
            df_factor.loc[temp_i, "ret_accumu_120d"] = temp_close_pre_1/ temp_close_pre_N -1 
            # (temp_close_pre_1/ temp_close_pre_20 )/( temp_close_pre_1/ temp_close_pre_N ) = temp_close_pre_N / temp_close_pre_20
            df_factor.loc[temp_i, "ret_accumu_20d_120d"] = temp_close_pre_N / temp_close_pre_20 -1 
            #################################################################################
            ### 20天内最大回撤比120天内最大回撤 ret_mdd_20d_120d = ret_mdd_20d/ret_mdd_120d
            df_factor.loc[temp_i, "ret_mdd_20d"] = 0.0
            df_factor.loc[temp_i, "ret_mdd_120d"] = 0.0
            # for past 20 days ,少数情况下120天内有交易日小于20天
            temp_close_max= 0.0
            temp_mdd= 0.0 
            for j in range( min(temp_n_20,temp_n ) ) :
                # temp_j= 0,1,...19; -20+temp_j=-20,-19,...-1
                temp_j = date_list_sub[-1*temp_n_20+j]
                temp_close = temp_df_close_adj.loc[ temp_j, temp_code+"_"+temp_col ]
                temp_close_max = max(temp_close_max,temp_close )
                temp_mdd = min(temp_mdd, temp_close/temp_close_max-1 )
            df_factor.loc[temp_i, "ret_mdd_20d"] = temp_mdd
            # for past 120 days | temp_n =min(N,len(temp_df_close_adj.index )
            temp_close_max= 0.0
            temp_mdd= 0.0 
            for j in range( temp_n )  :
                # temp_j= 0,1,...N-1; -N+temp_j=-N,-N+1,...-1
                temp_j = date_list_sub[-1*temp_n+j]
                temp_close = temp_df_close_adj.loc[ temp_j, temp_code+"_"+temp_col ]
                temp_close_max = max(temp_close_max,temp_close )
                temp_mdd = min(temp_mdd, temp_close/temp_close_max-1 )   

            df_factor.loc[temp_i, "ret_mdd_120d"] = temp_mdd
            # notes:20天内的最大回撤mdd肯定比120天内少，相对回撤看的是20天内的下跌幅度达到了过去120天最大跌幅的百分比
            # 例如: (1-0.1)/(1-0.2)=0.9/0.8=1.125,值越大说明短期跌的越少，1.0意味着短期跌幅覆盖了120天内的全部跌幅
            df_factor.loc[temp_i, "ret_mdd_20d_120d"] = (1+ df_factor.loc[temp_i, "ret_mdd_20d"]) /(1+ df_factor.loc[temp_i, "ret_mdd_120d"] )

            #################################################################################
            ### 20天和120天个股相对于中信一级的行业市值加权超额收益率 ret_alpha_ind_citic_1_20d,ret_alpha_ind_citic_1_120d
            # 中信一级行业代码 "citics_ind_code_s_1"的list = df_ret_accumu.index
            ind_list = list( df_factor["citics_ind_code_s_1"].drop_duplicates() ) 
            # 1,分行业计算市值加权收益，20天和120天
            # df_ret_accumu 的index就是中信一级行业分类数值 10.0，11.0，...,70.0
            for temp_citic_1 in ind_list :
                print("temp_citic_1",temp_citic_1 )
                # 分别计算个股20天和120天的相对收益率
                df_factor_sub = df_factor[ df_factor["citics_ind_code_s_1"]== temp_citic_1 ]
                # 计算行业内的市值加权收益率
                ret_citic_1_20d = (df_factor_sub["ret_accumu_20d"]*df_factor_sub["S_DQ_MV"]).sum()/df_factor_sub["S_DQ_MV"].sum()
                ret_citic_1_120d = (df_factor_sub["ret_accumu_120d"]*df_factor_sub["S_DQ_MV"]).sum()/df_factor_sub["S_DQ_MV"].sum()
                print("temp_citic_1",temp_citic_1, round(ret_citic_1_20d,2),round(ret_citic_1_120d,2) )
                # 计算个股相对行业的收益率
                df_factor.loc[df_factor_sub.index, "ret_alpha_ind_citic_1_20d" ] = df_factor.loc[df_factor_sub.index,"ret_accumu_20d"] -ret_citic_1_20d 
                df_factor.loc[df_factor_sub.index, "ret_alpha_ind_citic_1_120d" ] = df_factor.loc[df_factor_sub.index,"ret_accumu_120d"] -ret_citic_1_120d 
            
            #################################################################################
            ### 20天和120天个股相对于全样本空间市值加权的超额收益率 ret_alpha_stockpool_mv_20d,ret_alpha_stockpool_mv_120d
            # 计算全样本空间市值加权的收益率
            ret_stockpool_mv_20d = (df_factor["ret_accumu_20d"]*df_factor["S_DQ_MV"]).sum()/df_factor["S_DQ_MV"].sum()
            ret_stockpool_mv_120d = (df_factor["ret_accumu_120d"]*df_factor["S_DQ_MV"]).sum()/df_factor["S_DQ_MV"].sum()
            print("temp_citic_1",temp_citic_1, round(ret_stockpool_mv_20d ,2),round(ret_stockpool_mv_120d ,2) )
            # 计算个股相对全样本空间市值加权的收益率
            df_factor.loc[df_factor_sub.index, "ret_alpha_stockpool_mv_20d" ] = df_factor.loc[df_factor_sub.index,"ret_accumu_20d"] -ret_stockpool_mv_20d 
            df_factor.loc[df_factor_sub.index, "ret_alpha_stockpool_mv_120d" ] = df_factor.loc[df_factor_sub.index,"ret_accumu_120d"] -ret_stockpool_mv_120d

            #################################################################################
            ### 20天和120天个股相对于基准指数的超额收益率 ret_alpha_index_bm_20d,ret_alpha_index_bm_120d
            # 计算指数20，120天累计收益率 ；对于指数日行情：收盘价(点)，S_DQ_CLOSE；涨跌幅(%)，S_DQ_PCTCHANGE
            # temp_n个交易日，替代N，删除NaN所在行
            temp_col = "S_DQ_CLOSE" 
            temp_df_close_adj = df_dates_stocks.loc[ :, code_index+"_"+temp_col]
            temp_df_close_adj = temp_df_close_adj.dropna( axis=0, how="all" ).to_frame()
            date_list_sub = temp_df_close_adj.index
            
            temp_close_pre_1  =  temp_df_close_adj.loc[date_list_sub[-1] ,code_index+"_"+temp_col]
            temp_close_pre_20 =  temp_df_close_adj.loc[date_list_sub[-20] ,code_index+"_"+temp_col] 
            temp_close_pre_N  =  temp_df_close_adj.loc[date_list_sub[0] ,code_index+"_"+temp_col]  
            
            index_ret_accumu_20d = temp_close_pre_1/ temp_close_pre_20 -1
            index_ret_accumu_120d = temp_close_pre_1/ temp_close_pre_N -1 
            print("Index retrun for 20days and 120days", index_ret_accumu_20d ,index_ret_accumu_120d  )

            df_factor[ "ret_alpha_index_bm_20d" ] =  df_factor["ret_accumu_20d"]  -index_ret_accumu_20d
            df_factor[ "ret_alpha_index_bm_120d" ] = df_factor["ret_accumu_120d"] -index_ret_accumu_120d

        obj_out ={}
        # 输出col_list 
        obj_out["col_list_price_vol"] = col_list 
        obj_out["df_factor"] = df_factor

        return obj_out 

    def ashares_stock_price_vol_change(self,obj_in ):
        ### 3.2，个股：区间价格变动和涨跌幅：
        '''
        Function：
        1，计算股票区间涨跌幅
        2，计算股票区间成交额、成交量
        3，计算股票区间权息变动

        Input:obj_in至少包括的变量：
        1,obj_index_consti["df_change"]：dataframe,cols至少包括["wind_code"]
        2,obj_index_consti["date_pre"]:区间变动的起始日
        3,obj_index_consti["date"]：区间变动的结束日
        
        output：
        df_index_consti["df_change"] 

        notes:
        1，"AShareEODPrices" 重要指标：
        S_DQ_CLOSE 当日收盘价
        S_DQ_ADJPRECLOSE 后复权前一日收盘价
        S_DQ_ADJCLOSE 后复权当日收盘价
        S_DQ_ADJFACTOR：复权因子= S_DQ_ADJCLOSE/ S_DQ_CLOSE
        '''
        #
        temp_date_pre = obj_in["date_pre"]
        temp_date = obj_in["date"] 
        df_change = obj_in["df_change"]
        
        table_name = "AShareEODPrices" 
        
        col_list = ["CRNCY_CODE","S_DQ_CLOSE" ,"S_DQ_ADJCLOSE" ,"S_DQ_PCTCHANGE","S_DQ_AMOUNT","S_DQ_ADJFACTOR" ]
        obj_in["col_list"] = col_list

        for temp_col in col_list :
            df_change[temp_col ] = np.nan
        
        ### 获取初始日行情数据：
        # 注意：部分股票可能在月初未上市！
        file_name = "WDS_TRADE_DT_"+ str( temp_date_pre ) +"_ALL.csv"
        try :
            df0 = pd.read_csv( self.path_wind_wds +table_name+"\\"+ file_name  )
        except :
            df0 = pd.read_csv( self.path_wind_wds +table_name+"\\"+ file_name,encoding="gbk"  )
        # print(df0.columns )
        
        for temp_i in df_change.index :
            temp_code = df_change.loc[temp_i,  "wind_code" ]
            # find wind_code in df0
            df1 = df0[df0["S_INFO_WINDCODE"]==temp_code  ]
            if len( df1.index ) > 0 :
                temp_j = df1.index[0]
                for temp_col in col_list :
                    ###         
                    df_change.loc[temp_i,"pre_"+ temp_col ] = df0.loc[ temp_j,temp_col  ]
            else :
                print("No record for code ",temp_code  ) 
        
        ### 获取最近日行情数据：
        # 注意：部分股票可能在月初未上市！
        file_name = "WDS_TRADE_DT_"+ str( temp_date ) +"_ALL.csv"
        try :
            df0 = pd.read_csv( self.path_wind_wds +table_name+"\\"+ file_name  )
        except :
            df0 = pd.read_csv( self.path_wind_wds +table_name+"\\"+ file_name,encoding="gbk"  )
        # print(df0.head().T )        
        # notes:有可能出现股票退市的情况，例如20060831要取日行情，遇到000406.SZ于20060406退市了。
        for temp_i in df_change.index :
            temp_code = df_change.loc[temp_i,  "wind_code" ]
            # find wind_code in df0 
            df1 = df0[df0["S_INFO_WINDCODE"]==temp_code  ]
            
            if len( df1.index ) > 0 :
                temp_j = df1.index[0]
                for temp_col in col_list :
                    df_change.loc[temp_i,"last_"+ temp_col ] = df0.loc[ temp_j,temp_col  ]
            else :
                #通常是由于中途上市或者退市导致的，需要特定模块引入上市或退市日期；退市日期当日无交易，但前3周就知道。
                from data_io import data_io
                data_io_1 = data_io()
                obj_date = {}
                obj_date["wind_code"] = temp_code
                obj_date = data_io_1.get_list_delist_day(obj_date)
                # notes:退市日有可能是nan， float类型；obj_date["delist_date"];obj_date["list_date"]
                # 例子：20060421.0,对于000406.SZ:退市日期当日060421无交易{060406是最后一个交易日}，但前3周就知道。
                
                # 选择提前15个交易日
                ### notes:对于601988.SH在20050531，obj_date["delist_date"] == np.nan 没用
                if not np.isnan(obj_date["delist_date"] ) :
                    obj_date["date"] = obj_date["delist_date"]
                    obj_date = data_io_1.get_trading_days(obj_date)
                    delist_date =  str(int( obj_date["date_list_pre"][-15] ))
                    print("delist_date ",delist_date )
                    file_name_1 = "WDS_TRADE_DT_"+ str( delist_date) +"_ALL.csv"
                    try :
                        df_1 = pd.read_csv( self.path_wind_wds +table_name+"\\"+ file_name_1  )
                    except :
                        df_1 = pd.read_csv( self.path_wind_wds +table_name+"\\"+ file_name_1,encoding="gbk"  )

                    df_2 = df_1[df_1["S_INFO_WINDCODE"]==temp_code  ]

                    temp_j = df_2.index[0]
                    for temp_col in col_list :
                        df_change.loc[temp_i,"last_"+ temp_col ] = df_1.loc[ temp_j,temp_col  ] 

        ##########################################################################
        ### 计算区间变动：
        # 区间涨跌幅 s_change_adjclose 
        df_change["s_change_adjclose"] = df_change["last_"+"S_DQ_ADJCLOSE"]/df_change["pre_"+"S_DQ_ADJCLOSE"]-1
        # 判断区间内是否有权息变动，看复权因子是否变动 S_DQ_ADJFACTOR, close to 1.0 means nearly no change or quite small
        df_change["s_change_adjfacor"] = df_change["last_"+"S_DQ_ADJFACTOR"]/df_change["pre_"+"S_DQ_ADJFACTOR"] 
        
        obj_in["df_change"] = df_change
        

        return obj_in

    #################################################################################
    def ashares_stock_funda(self,obj_in) :
        # 4，个股：市值，财务和财务预测指标；
        '''
        obj_in["date_start"] = "20050501" 
        obj_in["table_name"] = "AShareEODDerivativeIndicator" 
        obj_in_stock["df_factor"] : df类型,包括了"wind_code","code_index","date",  

        ###
        notes:1,若"code_list"存在，则对股票列表取值，若不存在，对单个代码"code_stock"取值
            "code_list" in obj_in.keys()
        df_factor是df类型，至少包括"wind_code"这一列
        '''
        date_start = obj_in["date_start"]
        date_list_new = [date for date in self.date_list if date<= int(date_start) ]        
        date_0 = date_list_new[-1]
        ### 获取市值和市盈率，PE，PB等基本指标
        file_name = "WDS_TRADE_DT_"+ str(date_0) +"_ALL.csv"
        print("file_name ",file_name )
        df0 = pd.read_csv(self.path_wind_wds + obj_in["table_name"]+"\\"+ file_name  )
        df_cols = pd.read_csv(self.path_wind_wds + obj_in["table_name"]+"\\"+ "columns.csv"  )
        col_list = df_cols["0"].values
        # print("df0" ,df0.head().T )

        ### 把日衍生行情所有指标都放入df_factor 
        for temp_i in obj_in["df_factor"].index :
            temp_code = obj_in["df_factor"].loc[temp_i, "wind_code"] 

            df1= df0[ df0["S_INFO_WINDCODE"] == temp_code  ]
            
            if len( df1.index ) > 0 :
                temp_j = df1.index[0]
                for temp_col in col_list : 
                    obj_in["df_factor"].loc[temp_i, temp_col ] = df0.loc[temp_j, temp_col ]
        
        # obj_out = obj_in

        return  obj_in

    #################################################################################
    def ashares_stock_holder_events(self) :
        # 5，个股：股东、机构投资者、收购兼并等事件；
        
 









        return 1

    #################################################################################
    def ashares_fund_nav_port(self) :
        #6，基金、机构指标和因子；



 









        return 1


#########################################################
### 因子数据分析
class analysis_factor():
    # 类的初始化操作
    def __init__(self):
        ### 继承父类indicators的定义，等价于
        indicators.__init__(self)
        #################################################################################
        ### Initialization 

        sys.path.append( self.file_path_admin + "config\\")
        from config_data import config_data
        config_data_1 = config_data()
        self.obj_config = config_data_1.obj_config
        ### factor_model 目录位置：
        # self.obj_config["dict"]["path_factor_model"] = dict_config["path_ciss_db"] +"factor_model\\"
        
        ### 导入date_list, 导入A股历史交易日期 
        df_dates = pd.read_csv(self.path_wind_adj + self.obj_config["dict"]["file_date_tradingday"]  )
        # type of date_list is numpy.int64
        self.date_list = list( df_dates["date"].values )
        self.date_list.sort()
        # month
        # df_dates = pd.read_csv(self.path_wind_adj + self.obj_config["dict"]["file_date_month"] )
        # # type of date_list is numpy.int64
        # self.date_list_m = list( df_dates["date"].values )
        # self.date_list_m.sort()
        # # quarter
        # df_dates = pd.read_csv(self.path_wind_adj + self.obj_config["dict"]["file_date_quarter"]   )
        # # type of date_list is numpy.int64
        # self.date_list_q = list( df_dates["date"].values )
        # self.date_list_q.sort()

        ### 
        #################################################################################

    def print_info(self):
        ### print all modules for current script
        print("指标和因子数据处理")
        print("cal_replace_extreme_value_mad |对单个指标计算均值和MAD，并代替极端值  ")
        print("indicator_data_adjust_zscore  |指标数据清洗调整：去异常值和缺失值；标准化") 
        print("indicator_indicator_orthogonal  |因子指标正交处理") 
        print("indicator_indicator_icir  |因子指标IC和ICIR计算 ") 
        print("indicator_factor_weight |根据IC_IR的值，计算股票i在因子k{1，2,...,K}上的因子权重 ") 
        print("  ")
        print("group_mean_abcd3d_ana |### 个股abcd3d指标和其他指标的标准化分析  ")
        print("market_status_abcd3d_ana |### 全市场、行业内个股的动量状态分析，基于已有的abcd3d指标  ")
        
        return 1 

    def cal_replace_extreme_value_mad(self,df_factor,col_name,level=3 ):
        ### fanction:对单个指标计算均值和MAD，并代替极端值
        #对 df_factor[col_name] 用MAD方法替代异常值
        # temp_median= np.median( code_list )
        # df_factor.to_csv("D:\\temp_df_factor.csv")
        
        temp_median = df_factor[col_name].median()
        temp_mad = np.median(  np.abs( df_factor[col_name] -temp_median  ) )

        #########################################################
        ### 计算上限和下限并替代极端值：upper_limit,lower_limit
        # 感觉大概率不会超过极端值
        upper_limit = temp_median+ level *1.4826*temp_mad
        lower_limit = temp_median- level *1.4826*temp_mad 

        ### 先为所有值取下限值 
        df_factor[col_name+"_mad"] = 0.0

        # 若最大最小值没有超过就不需要调整
        list_adj = []

        for temp_i in df_factor.index : 
            temp_value = df_factor.loc[temp_i, col_name]
            if temp_value > upper_limit :
                df_factor.loc[temp_i, col_name+"_mad"] = upper_limit
            elif temp_value < lower_limit :
                df_factor.loc[temp_i, col_name+"_mad"] = lower_limit
            else :
                df_factor.loc[temp_i, col_name+"_mad"] =df_factor.loc[temp_i, col_name]  

        return df_factor

    def indicator_data_adjust_zscore(self,df_factor,col_list_to_zscore):
        ### 指标数据清洗调整：去异常值和缺失值；标准化
        '''
        1,MAD（Median Absolute Deviation绝对中位数法）:
        我们将大于𝑀𝑒𝑑𝑖𝑎𝑛𝑓+3∗1.4826∗𝑀𝐴𝐷的值或小于𝑀𝑒𝑑𝑖𝑎𝑛𝑓−3∗1.4826∗𝑀𝐴𝐷的值定义为异常值。
        在对异常值做处理时，需要根据因子的具体情况来决定.缺失率小于20%的因子数据
        用中信一级行业的中位数代替，当缺失率大于20%时则做剔除处理。
        notes:数值分布在（μ-2σ,μ+2σ)中的概率为0.9544；数值分布在（μ-3σ,μ+3σ)中的概率为0.9974；

        2,因子标准化
        for factor k: miu_k = sum_1_n[ w_mv * X_ik_rawdata ];X_ik = (X_ik_rawdata-miu_k )/std_k
        定义：回归中需要对单个因子在横截面上进行标准化，从而得到均值为0、标准差为1的标准化因子。
        为保证全市场基准指数对每个风格因子的暴露程度均为0，我们需要对每个因子减去其市值加权均值，再除以其标准差。
        方法比较：Rank标准化后的数据会丢失原始样本的一些重要信息，这里我们仍然选择Z值标准化。
        2.1，引入每个因子对应股票的市值，计算加权均值miu_k
        2.2，计算(x_ik -miu_k)/ std_k
        由于不同因子在数量级上存在差别，例如规模因子在取对数之后仍然是BP因子的数十倍甚至百倍，因此在实际回归中需要对单个因子在横截面上进行标准化，从而得到均值为0、标准差为1的标准化因子。为保证全市场基准指数对每个风格因子的暴露程度均为0，我们需要对每个因子减去其市值加权均值，再除以其标准差，计算方法如下：
        
        INPUT:
            1,df_factor：df,包括股票代码、行业分类、市值；
            2,col_list_to_zscore:list,包括了需要计算标准分值zscore的指标，
        
        notes:
        1，df_factor里默认包含行业分类和市值指标；例如中信一级行业和流通市值数据；
        例如"citics_ind_code_s_1"，S_DQ_MV
        2，市值加权规则：价量类因子选择流通市值加权、基本面类因子选择总市值加权

        参考 光大证券-20170410-多因子系列报告之一：因子测试框架.pdf
        '''
        #########################################################
        ### 1，变量设置
        code_list = df_factor["wind_code"]

        #########################################################
        ### fanction:对单个指标计算均值和MAD，并代替极端值
        def cal_replace_extreme_value_mad(df_factor,col_name ):
            #对 df_factor[col_name] 用MAD方法替代异常值
            # temp_median= np.median( code_list )
            df_factor.to_csv("D:\\temp_df_factor.csv")
            temp_median = df_factor[col_name].median()
            temp_mad = np.median(  np.abs( df_factor[col_name] -temp_median  ) )

            #########################################################
            ### 计算上限和下限并替代极端值：upper_limit,lower_limit
            # 感觉大概率不会超过极端值
            upper_limit = temp_median+3*1.4826*temp_mad
            lower_limit = temp_median-3*1.4826*temp_mad 

            ### 先为所有值取下限值 
            df_factor[col_name+"_mad"] = 0.0

            # 若最大最小值没有超过就不需要调整
            list_adj = []

            for temp_i in df_factor.index : 
                temp_value = df_factor.loc[temp_i, col_name]
                if temp_value > upper_limit :
                    df_factor.loc[temp_i, col_name+"_mad"] = upper_limit
                elif temp_value < lower_limit :
                    df_factor.loc[temp_i, col_name+"_mad"] = lower_limit
                else :
                    df_factor.loc[temp_i, col_name+"_mad"] =df_factor.loc[temp_i, col_name]  

            return df_factor
        
        #########################################################
        ### fanction:对单个指标（市值以外）计算市值加权后的因子标准差
        def cal_zscore_mv_weighted(df_factor,col_name ,col_name_mv ):
            '''不同因子在数量级上存在差别，例如规模因子在取对数之后仍然是BP因子的数十倍甚至百倍，因此在
            实际回归中需要对单个因子在横截面上进行标准化，从而得到均值为0、标准差为1的标准化因子。
            为保证全市场基准指数对每个风格因子的暴露程度均为0，我们需要对每个因子减去其市值加权均值，再除以其标准差。
            for factor k: miu_k = sum_1_n[ w_mv * X_ik_rawdata ];X_ik = (X_ik_rawdata-miu_k )/std_k
            
            notes:
            1,col_name是要计算标准分的因子指标，col_name_mv是市值加权指标
            2,zscore有的指标是越大越好，比如净利润；有的指标越小越好，比如PE
            3,用于计算市值加权均值和miu,std的都应该是 col_name +"_mad" 的值
            '''
            ### 1，miu_k = sum_1_n[ w_mv * X_ik_rawdata ]
            temp_miu = (df_factor[col_name +"_mad"]* df_factor[ "weight_"+col_name_mv ]).sum()

            ### 2,std_k,如果简单计算波动率是假设每个公司都一样，用市值加权可能更能反映波动率对应的
            # 流动性总体（资金资金规模）和基本面总体情况。
            # 通常做法是直接用每个股票的指标值计算std，我们这里相当于统一用市值加权后的值计算std
            # temp_std= df_factor[col_name +"_mad" ].std()
            temp_std= df_factor[ col_name +"_mad" ].std()

            ### 2,X_ik = (X_ik_rawdata-miu_k )/std_k
            df_factor["zscore_"+col_name ] =(df_factor[ col_name +"_mad" ] -temp_miu )/temp_std


            return df_factor

        #########################################################
        ### 计算市值标准分（=因子）和市值加权权重 | 例如流通市值标准分"zscore_S_DQ_MV" 和"weight_S_DQ_MV"
        # 市值加权规则：价量类因子选择流通市值加权、基本面类因子选择总市值加权
        col_name_mv = "S_DQ_MV"
        df_factor = self.cal_replace_extreme_value_mad(df_factor,col_name_mv )
        
        list_zscore = ["zscore_"+col_name_mv  ]
        temp_miu = df_factor[col_name_mv+"_mad"].mean()
        temp_std = df_factor[col_name_mv+"_mad"].std()
        df_factor["zscore_"+col_name_mv ] = ( df_factor[col_name_mv+"_mad"] - temp_miu )/temp_std
        # 所有个股的市值加权权重
        df_factor["weight_"+col_name_mv ] = df_factor[col_name_mv+"_mad"]/df_factor[col_name_mv+"_mad"].sum()

        #########################################################
        ### 计算各个指标的标准分值zscore， 例如市盈率(PE,TTM) "S_VAL_PE_TTM"
        # col_name = "S_VAL_PE_TTM"
        col_list_zscore = []
        for col_name in col_list_to_zscore :
            col_list_zscore = col_list_zscore + [ "zscore_"+col_name  ]
            list_zscore = list_zscore = ["zscore_"+col_name  ]
            # calculate mad value
            print("col_name ",col_name)
            df_factor = self.cal_replace_extreme_value_mad(df_factor,col_name )
            # calculate zscore value
            df_factor = cal_zscore_mv_weighted(df_factor,col_name ,col_name_mv)
            # notes：zscore有的指标是越大越好，比如净利润；有的指标越小越好，比如PE

        obj_out = {}
        obj_out["df_factor"] =df_factor
        # 包括所有zscore列名的column list
        obj_out["col_list_zscore"] = col_list_zscore

        return obj_out

    def indicator_indicator_orthogonal(self,obj_in ) :
        ### 2.4,因子做对称正交处理
        '''
        INPUT:obj_in 包括：
        1,df_factor：df,包括股票代码、行业分类、市值；
        2,col_list_zscore:list,包括所有zscore列名的column list  

        notes:
        1，df_factor的列有很多非因子项，需要对其取因子指标的部分 col_list_to_zscore

        因子正交 factor SymmetricOrthogonalization的步骤：
        file=天风证券-专题报告：因子正交全攻略——理论、框架与实践.pdf;
        path=.\TOUYAN\天风证券金工合集\多因子选股系列报告\
        step1，求t时间矩阵df1[股票数量N，因子数量K]的协方差矩阵Sigma= df1.cov(),重叠矩阵M=np.matrix( (N-1)*Sigma );
        step2,M_inv: 求解S*S_t= inv(M) :矩阵逆的2种方法：1，M_inv=np.matrix(M).I ;2,np.linalg.inv(M) 3,伪逆-不可逆的情况，np.linalg.pinv(M) ;
        step3：M_inv=U*D_inv*U', (A,B)=np.linalg.eig(M_inv), M_inv*A=B*A，B是特征向量矩阵，A是特征值vector；
        D = np.dot( np.dot(np.linalg.inv(B),M_inv ),B ) ;矩阵中的每个数字都保留两位有效数字 D2 = np.round( D,decimals=2,out=None )
        D_inv = np.linalg.inv( D )
        求 D_inv_sqrt,对对角线上的每个值求平方根的倒数，即 1/sqrt( rambda1 )
        过度矩阵 S = U* D_inv_sqrt* U' *C , 需要求得U 和 C；
        U是M的特征向量矩阵，即(V,U) =np.linalg.eig(M) 可以求得 U；
        M_inv_sqrt = U * D_inv_sqrt * U'
        规范正交方法：S=U* D_inv_sqrt* U' *C, { C=U} = U* D_inv_sqrt
        规范正交后的因子没有稳定的对应关系。规范正交和PCA一样，在每个截面上以方差最大的方向来确定第一主成分，但是不同截面上第一主成分的方向可能会差别很大，这样就导致不同截面上主成分序列上的因子没有稳定的对应关系[Qian 2007]。
        比较分析：施密特正交由于在过去若干个截面上都取同样的因子正交顺序，因此正交后的因子和原始因子有显式的对应关系，而规范正交在每个截面上选取的主成分方向可能不一致，导致正交前后的因子没有稳定的对应关系。由此可见，正交后组合的效果，很大一部分取决于正交前后因子是否有稳定的对应关系。 br
        对称正交（SymmetricOrthogonalization，[Löwdin1970, Schweinler1970]）是一种特殊的正交方法，它的过渡矩阵是取CK×K=𝐼𝐾×𝐾，即
        对称正交 S = U* D_inv_sqrt* U'
        对称正交有几个重要的性质[Klein 2013]：1.相对于施密特正交法，对称正交不需要提供正交次序，对每个因子是平等看待的；
        2.在所有正交过渡矩阵中，对称正交后的矩阵和原始矩阵的相似性最大，即正交前后矩阵的距离最小。我们用变化前后的矩阵的距离（Frobenius 范数）𝜙来衡量因子正交前后变化的大小
        '''
        ### get specific df with N rows and K columns
        df_factor_ortho = obj_in["df_factor"].loc[:, obj_in["col_list_zscore"] ]
        df_factor_ortho.to_csv("D:\\df_factor_ortho_1.csv")
        index_list = df_factor_ortho.index
        col_list = columns=df_factor_ortho.columns

        N = len( df_factor_ortho.index  )
        K= len( obj_in["col_list_zscore"] )
        print("N,K ",N ,K )
        ### step1，求t时间矩阵df1[股票数量N，因子数量K]的协方差矩阵Sigma= df1.cov(),重叠矩阵M=np.matrix( (N-1)*Sigma );
        df_factor_ortho_sigma = df_factor_ortho.cov()
        print( "df_factor_ortho_sigma ",df_factor_ortho_sigma  )

        matrix_m = np.matrix( (N-1)*df_factor_ortho_sigma )
        print("matrix_m",matrix_m )
        
        ### step2,M_inv
        # 20200404,34*34的情况下，出现matrix_m_inv全是nan的情况
        # notes:即便求伪逆矩阵pinv也会出现numpy.linalg.LinAlgError: SVD did not converge 报错的情况
        # 只能考虑减少因子梳理，34个确实没必要。
        # matrix_m_inv = np.linalg.inv( matrix_m )
        matrix_m_inv = np.linalg.pinv( matrix_m )
        print("matrix_m_inv",matrix_m_inv )

        ### step3：(A,B)=np.linalg.eig(M_inv)
        (A,B)=np.linalg.eig( matrix_m_inv )
        D = np.dot( np.dot(np.linalg.inv(B),matrix_m_inv ),B )
        print("Matrix D \n", D)
        D_inv = np.linalg.inv( D )
        # 求 D_inv_sqrt,对对角线上的每个值求平方根的倒数，即 1/sqrt( rambda1 )
        D_inv_sqrt = D_inv
        len_D = len( D_inv[0] ) # D_inv is n*n matrix 
        for i in range( len_D ) :
            D_inv_sqrt[i][i] = np.sqrt( D_inv_sqrt[i][i]  )

        # 为了避免后续出现nan值，对D_inv_sqrt求对角矩阵，diag(M)会返回vector形式的对角线值，diag(diag(M))会返回对角线矩阵
        D_inv_sqrt = np.diag(np.diag(D_inv_sqrt))
        print("D_inv_sqrt diag \n", D_inv_sqrt )

        # U是M的特征向量矩阵
        (V,U) =np.linalg.eig( matrix_m )       

        # matrix to array
        # pd.DataFrame(U.A).to_csv("D:\\matrix_U.csv")
        # pd.DataFrame(D_inv_sqrt).to_csv("D:\\matrix_D_inv_sqrt.csv")
        U_trans = U.T
        # pd.DataFrame(U_trans.A).to_csv("D:\\matrix_U_trans.csv")
        # print("Debug== 1\n",  U * D_inv_sqrt )
        # print("Debug== 2\n",  D_inv_sqrt* U.T )
        # 对称正交 S = U* D_inv_sqrt* U'
        S = U * D_inv_sqrt  * U.T

        print("S \n",S  )
        # matrix S to array = S.A
        ### Calculate orthogonal factor array from df_factor * matrix S
        array_factor_ortho = np.dot( np.array(df_factor_ortho),S.A )

        ### Calculate new factor matrix    
        # notes:注意！dataframe*dataframe是基于各自位置df2(x,y)乘df2(x,y)一一对应，和矩阵相乘不一样
        # 要先把df转成matrix，再转回df
        matrix_factor_ortho = np.matrix(array_factor_ortho) * S 
        df_factor_ortho=pd.DataFrame( matrix_factor_ortho.A , index =index_list,columns= col_list )

        df_factor_ortho.to_csv("D:\\df_factor_ortho_2.csv")  

        # Save to object      
        obj_in["df_factor_ortho"] = df_factor_ortho
        obj_in["matrix_S"] = S 

        return obj_in

    def indicator_indicator_icir(self,obj_factor):
        ###  2.6,计算各因子6~12个月ICIR，作为各因子权重  
        # notes:只有当累计月份count_month大于3时才计算当月的IC_adj值，IC_adj值大于3，也就是累计月份大于6时才计算ICIR;count_month >=6
        '''
        如何，6计算因子ICIR？参考P7/23，东方金工——动态情景多因子Alpha模型——《因子选股系列研究之八》.pdf
        对于过去T(=12)期计算得到的个股因子暴露X_i_k和个股期间超额收益率r，计算风险调整IC:IC_adj=corr(X_i_k,r_alpha )
        Qs:IC_IR如何计算？IR=ret_mean/ret_std，IC_IR=IC_miu/IC_std
        TODO:1,计算过去12个期（月）末的X_i_k、个股收益率、一级行业内加权收益率,对每一期计算个股的IC_adj_s_t和行业的IC_adj_ind_t；
        2，对12期的个股的IC_adj_s_t和行业的IC_adj_ind_t值,分别计算IC_miu，IC_std，获得IC_IR值
        分析：对于滚动时期的计算，这个值应该分别存储，避免重复计算；未来应该可以选择周或任意时间区间，只需要给出date_list
        步骤梳理：
        input：
        df_date_factor_return包括了所有个股的20天和120天收益率、以及各个因子指标值
        
        notes:相对行业的超额收益率有 "ret_alpha_ind_citic_1_20d" ,"ret_alpha_ind_citic_1_120d" 
        相对于基准指数的超额收益率 ret_alpha_index_bm_20d,ret_alpha_index_bm_120d ;
        20天和120天个股相对于全样本空间市值加权的超额收益率 ret_alpha_stockpool_mv_20d,ret_alpha_stockpool_mv_120d
        '''
        df_factor_ortho = obj_factor["df_factor_ortho"] 
        df_factor = obj_factor["df_factor"]
        
        count_month = obj_factor["count_month"] 
        # 20191129出现过报错，300142.SZ 的最后一期值不是int，而是str || df3.astype('int64')
        temp_date = int( obj_factor["temp_date"] )
        df_date_factor_return = obj_factor["df_date_factor_return"] 
        
        # 判断df_ic_ir 是否存在
        if "df_ic_ir" in obj_factor.keys() :
            df_ic_ir  = obj_factor["df_ic_ir"]
        # else :
        #     df_ic_ir = pd.DataFrame( index= df_factor.index ,columns=["wind_code","date","ic_adj"] )

        #################################################
        ### 2.6.1,对每只个股，计算IC单期的IC值；信息系数（Information Coefficient，简称 IC
        # Normal IC，即某时点某因子在全部股票的暴露值与其下期回报的截面相关系数；RankIC，即某时点某因子在全部股票暴露值排名与其下期回报排名的截面相关系数。
        # 因子 IC 衰退，是通过观察随着滞后时间的延长，因子有效性降低的速度；通过观察半衰期的长短判断该因子的稳定情况。
        if count_month >= 3 :
            df_ic_ir_temp = pd.DataFrame( index= df_factor.index ,columns=["wind_code","date","ic_adj"] )
            df_ic_ir_temp["date"] = temp_date
            for temp_f in obj_factor["col_list_zscore"] : 
                df_ic_ir_temp["ic_adj_"+ temp_f ] = np.nan

            for temp_i in df_factor.index :
                temp_code = df_factor.loc[temp_i,"wind_code"]
                df_ic_ir_temp.loc[temp_i,"wind_code"] = temp_code 
                # for every factor ,calculate
                for temp_f in obj_factor["col_list_zscore"] :
                    print("temp_f" ,temp_f ,temp_code+"_"+temp_f, temp_code+"_ret_alpha_ind_citic_1_120d" )
                    # notes:df_date_factor_return的index是日期序列，columns是每个股票的因子指标值和相对行业超额收益值
                    # 取最近的count_month个月和12个月的较小值求相关系数
                    temp_n = min( count_month, 12 )
                    index_list_sub =  df_date_factor_return.index[ -1*temp_n: ]
                    df_ic_ir_temp.loc[temp_i,"ic_adj_"+ temp_f ] = df_date_factor_return.loc[ index_list_sub ,[ temp_code+"_"+temp_f, temp_code+"_ret_alpha_ind_citic_1_120d"  ] ].corr().loc[ temp_code+"_"+temp_f, temp_code+"_ret_alpha_ind_citic_1_120d" ]
            
            if count_month == 3 :
                df_ic_ir = df_ic_ir_temp
                # 预先增加ic_mean,ic_std,ic_ir相关的列
                for temp_f in obj_factor["col_list_zscore"] :
                    # temp_col = "ic_adj_mean_"+ temp_f
                    df_ic_ir[ "ic_adj_mean_"+ temp_f ] =np.nan
                    df_ic_ir[ "ic_adj_std_"+ temp_f ] =np.nan
                    df_ic_ir[ "ic_ir_"+ temp_f ] =np.nan

            else :
                df_ic_ir = df_ic_ir.append(df_ic_ir_temp,ignore_index=True)

        if  count_month >= 6 :
            temp_n = min( count_month, 12 )
            ### 2.6.2, 对每只个股，计算多期IC计算ICIR值，ic_ir= ic_miu/ic_std; Grinold的算法是IR=ic*sqrt(N)
            for temp_i in df_factor.index :
                temp_code = df_factor.loc[temp_i,"wind_code"]
                df_ic_ir_code = df_ic_ir[ df_ic_ir["wind_code"] == temp_code  ]
                # 默认升序ascending排列
                
                df_ic_ir_code =df_ic_ir_code.sort_values(by="date")
                # 取最新一期的index位置，为其赋值
                temp_index = df_ic_ir_code.index[-1]

                ### 对于个股的历史数据，只取最近12期的值
                for temp_f in obj_factor["col_list_zscore"] :
                    print("temp_f" ,temp_f ,"ic_adj_"+ temp_f  )
                    df_ic_ir.loc[temp_index,  "ic_adj_mean_"+ temp_f ] = df_ic_ir_code.loc[-1*temp_n:, "ic_adj_"+ temp_f ].mean()
                    df_ic_ir.loc[temp_index,  "ic_adj_std_"+ temp_f ] = df_ic_ir_code.loc[-1*temp_n:, "ic_adj_"+ temp_f ].std()
                    df_ic_ir.loc[temp_index,  "ic_ir_"+ temp_f ] =df_ic_ir.loc[temp_index,  "ic_adj_mean_"+ temp_f ]/df_ic_ir.loc[temp_index,  "ic_adj_std_"+ temp_f ] 

        
        ### save df_ic_ir,count_month
        obj_factor["df_ic_ir"] = df_ic_ir
        
        return obj_factor 

    def indicator_factor_weight(self,obj_factor):
        ### 根据IC_IR的值，计算股票i在因子k{1，2,...,K}上的因子权重
        ''' 
        数据保存在 df_factor_weight,列包括wind_code和date，

        因子收益和个股收益的投影： sum{i,1,N}(W_i_k*r_i)=f_k, k=1,2,...,K
        1,对IC_ir的极端值进行处理，求zscore：
        1.1,有大有小，当前导出的ic_ir数值，有"inf","-inf"两种是excel无法识别的，也有极大值和极小值需要剔除
        由于有极大值和极小值，因子指标的中位数基本是0.0，但均值、最大值、最小值、标准差的数值都非常大无法使用。
        1.2,关于数值：绝大部分还是处于+1/-1，尾部超过+5/-5的基本在20个以内/100个值
        1.3，IC_IR值越接近1，表示因子值和相对行业的超额收益越正相关，或者和超额收益的波动率越负相关。

        2,对于个股i，在因子1~K上的暴露之和为1，因此可以用历史IC均值或IC_IR均值算出个股在每个因子上的权重w_s_i_k;
            w_s_i_k = IC_IR_i_k_miu / sum(k,1,K)( IC_IR_i_k_miu ) ,for i=1,2,...,N
        3，对于市场组合，在因子k上的暴露为 IC_IR_k_miu / sum(k,1,K)( abs(IC_IR_k_miu ) )

        Input:df_ic_ir,code_index,temp_date
        Output:df_factor_weight
        '''
        # generate df =df_factor_weight
        index_i = 0 
        df_factor_weight= pd.DataFrame(index=[index_i], columns=["wind_code","date"] )
        
        # Import df_ic_ir 
        df_ic_ir = obj_factor["df_ic_ir"]
        temp_date = int( obj_factor["temp_date"] )

        date_list = list( df_ic_ir["date"].drop_duplicates() )
        date_list.sort() 

        # notes:难点：每一期的股票代码list都不一样
        ### date_list_sub是要匹配之前有6~12个月记录的月份，也就是count_month>=6
        
        ### 取最近的6~12期作为date_pre_temp_date
        date_pre_temp_date = [ date for date in date_list if date<=temp_date]
        date_pre_temp_date.sort()
        date_pre_temp_date= date_pre_temp_date[-12:]

        df_ic_ir_date = df_ic_ir[ df_ic_ir["date"] ==temp_date  ]
        code_list = list( df_ic_ir["wind_code"].drop_duplicates() )

        for temp_code in code_list : 
            ### 只取最近的12期
            df_ic_ir_sub_s = df_ic_ir [ df_ic_ir["wind_code"] == temp_code  ]
            #
            df_ic_ir_sub_s= df_ic_ir_sub_s[ df_ic_ir_sub_s["date"].isin(date_pre_temp_date) ]
            
            ### 2，对于单只个股i，计算个股i单个指标ic_ir均值/个股i所有指标ic_ir均值之和
            # temp_ic_ir = "ic_ir_ret_mdd_20d_120d"
            # 求所有股票在过去T(6~12)期的平均值的绝对值之和
            
            df_factor_weight.loc[index_i,"wind_code"] = temp_code
            df_factor_weight.loc[index_i,"date"] = temp_date
            
            ic_ir_list= []
            sum_ic_ir_median = 0 
            # df_factor_weight
            for temp_ic_ir in df_ic_ir_sub_s.columns:
                if temp_ic_ir[:5] =="ic_ir" :
                    ic_ir_list= ic_ir_list + [ temp_ic_ir ]
                    df_ic_ir_sub_s = self.cal_replace_extreme_value_mad(df_ic_ir_sub_s,temp_ic_ir )
                    # 用np.nan无法识别，用fillna的方式
                    temp_median = df_ic_ir_sub_s[temp_ic_ir+"_mad"].fillna(0.0).median()
                    print("temp_median:" ,temp_median,type(temp_median) )
                    if not temp_median == np.nan :
                        df_factor_weight.loc[index_i, "f_w_"+temp_ic_ir ] = temp_median
                        sum_ic_ir_median = sum_ic_ir_median + abs(temp_median)
                    else :
                        df_factor_weight.loc[index_i, "f_w_"+temp_ic_ir ] = 0.0

            # 最后统一除以均值的绝对值之和
            for temp_ic_ir in ic_ir_list:
                df_factor_weight.loc[index_i,"f_w_"+temp_ic_ir]= df_factor_weight.loc[index_i,"f_w_"+temp_ic_ir]/sum_ic_ir_median

            index_i = index_i + 1  

        ### save to ouput object 
        obj_factor["df_factor_weight"] = df_factor_weight

        return obj_factor

    def group_mean_abcd3d_ana(self,temp_i,df_abcd3d_ana, temp_df_ana) :
        ### 个股abcd3d指标和其他指标的标准化分析 
        '''分别对分组本身、分组内细分成长、价值组计算各个指标        
        '''
        ##############################################################################
        ### 定义标准化的分组指标计算function  
        def group_mean_indicators(temp_i, temp_df_ana,df_abcd3d_ana ):
            ''' temp_i 是df_abcd3d_ana对应的行,temp_df_ana是特定分组
            '''
            ### 计算流通市值加权指标
            print("temp_df_ana[ abcd3d ]",temp_df_ana  )
            df_abcd3d_ana.loc[temp_i,"abcd3d_ave_num" ] = temp_df_ana["abcd3d"].mean()
            temp_value = (temp_df_ana["abcd3d"]*temp_df_ana["S_DQ_MV"] ).sum()
            temp_value = temp_value / temp_df_ana["S_DQ_MV"].sum()
            # 流通市值：temp_df_ana["abcd3d"]*temp_df_ana["S_DQ_MV"]
            df_abcd3d_ana.loc[temp_i,"abcd3d_ave_mvfloat" ] = temp_value
            temp_df_ana_sub = temp_df_ana[ temp_df_ana["abcd3d"]<=-4 ]
            df_abcd3d_ana.loc[temp_i,"abcd3d_pct_down" ] = temp_df_ana_sub["abcd3d"].count()/temp_df_ana["abcd3d"].count() 
            temp_df_ana_sub = temp_df_ana[ temp_df_ana["abcd3d"]>= 4 ]
            df_abcd3d_ana.loc[temp_i,"abcd3d_pct_up" ] = temp_df_ana_sub["abcd3d"].count()/temp_df_ana["abcd3d"].count() 
            # 保存当日涨幅最大和成交金额最大的股票
            if len( temp_df_ana.index ) > 1 :
                df_abcd3d_ana.loc[temp_i,"stock_max_pct" ] = temp_df_ana.loc[ temp_df_ana["S_DQ_CHANGE"].idxmax(),"S_INFO_WINDCODE" ]
                df_abcd3d_ana.loc[temp_i,"stock_max_amt" ] = temp_df_ana.loc[ temp_df_ana["S_DQ_AMOUNT"].idxmax(),"S_INFO_WINDCODE" ]

            ### 计算流通市值加权的，过去40天收盘价所处最高最低价的百分比 | "close_pct_s_16"，"close_pct_s_40"，"close_pct_s_100"
            # temp_df_ana["temp"] = (temp_df_ana["close_pct_s_40"]*temp_df_ana["S_DQ_MV"] ).sum()
            # temp_df_ana["temp"] = temp_df_ana["temp"] / temp_df_ana["temp"].sum()
            # df_abcd3d_ana.loc[temp_i,"close_pct" ] = temp_df_ana["temp"].mean()
            # 简单平均
            df_abcd3d_ana.loc[temp_i,"close_pct" ] = temp_df_ana["close_pct_s_40"].mean()

            ### 计算涨停+跌停数量、历史新高新低| UP_DOWN_LIMIT_STATUS={-1,0,1};LOWEST_HIGHEST_STATUS={-1,0,1}
                        
            temp_df_ana[ "temp"] = temp_df_ana["UP_DOWN_LIMIT_STATUS"].apply(lambda x : x if x >0 else 0 )
            df_abcd3d_ana.loc[temp_i,"num_up_limit" ] = temp_df_ana[ "temp"].sum()
            temp_df_ana[ "temp"] = temp_df_ana["UP_DOWN_LIMIT_STATUS"].apply(lambda x : x if x <0 else 0 )
            df_abcd3d_ana.loc[temp_i,"num_down_limit" ] = temp_df_ana[ "temp"].sum()
            temp_df_ana[ "temp"] = temp_df_ana["LOWEST_HIGHEST_STATUS"].apply(lambda x : x if x >0 else 0 )
            df_abcd3d_ana.loc[temp_i,"num_new_high" ] = temp_df_ana[ "temp"].sum()
            temp_df_ana[ "temp"] = temp_df_ana["LOWEST_HIGHEST_STATUS"].apply(lambda x : x if x <0 else 0 )
            df_abcd3d_ana.loc[temp_i,"num_new_low" ] = temp_df_ana[ "temp"].sum()

            ### 计算流通市值加权的市盈率 S_VAL_PE_TTM，未来1年预测市盈率 EST_PE_FY1；EST_PEG_FY1
            # notes:1,部分指标只覆盖部分股票 ;2,总市值方法会导致银行里的工商银行非流通部分被计入可投资空间。
            temp_df = temp_df_ana[ temp_df_ana["S_VAL_PE_TTM"]>0 ]
            temp_df[ "temp"] = temp_df["S_VAL_PE_TTM"]*temp_df["S_DQ_MV"]
            temp_value = temp_df["S_DQ_MV"].sum()
            df_abcd3d_ana.loc[temp_i ,"PE_ttm"] = temp_df[ "temp"].sum()/temp_value
            
            temp_df = temp_df_ana[ temp_df_ana["EST_PE_FY1"]>0 ]
            temp_df[ "temp"] = temp_df["EST_PE_FY1"]*temp_df["S_DQ_MV"]
            temp_value = temp_df["S_DQ_MV"].sum()
            df_abcd3d_ana.loc[temp_i ,"PE_fy1"] = temp_df[ "temp"].sum()/temp_value

            temp_df = temp_df_ana[ temp_df_ana["EST_PEG_FY1"]>0 ]
            temp_df[ "temp"] = temp_df["EST_PEG_FY1"]*temp_df["S_DQ_MV"]
            temp_value = temp_df["S_DQ_MV"].sum()
            df_abcd3d_ana.loc[temp_i ,"PEG_FY1"] = temp_df[ "temp"].sum()/temp_value

            return df_abcd3d_ana
        
        ##############################################################################
        ### 分组本身 
        df_abcd3d_ana = group_mean_indicators(temp_i, temp_df_ana,df_abcd3d_ana )

        ############################################################################## 
        ### 计算成长和价值分组：价值==PE==EST_PE_FY1;成长==ROE==NET_PROFIT_YOY;EST_PE_FY1	EST_PEG_FY1
        # notes:因为没有roe指标，先用PE/PEG代替，对应的是G，即净利润增长率；数据方面200513当日3800只股票中有1710只有数据
        # 1710只股票PE/PEG平均G值42%，中位数25.8%；NET_PROFIT_YOY指标有1890个值，但1690为正，且平均值90.0，中位数29.66%
        num_stock = len( temp_df_ana.index )
        ##############################################################################
        ### 计算价值组
        temp_i_value = str(temp_i) + "_value"
        df_abcd3d_ana.loc[temp_i_value,"group_type" ] = "value" # "value_growth"
        df_abcd3d_ana.loc[temp_i_value,"group_name" ] = "价值_" +str( df_abcd3d_ana.loc[temp_i,"group_name" ] )
        # temp_df_ana["EST_PE_FY1"] 取有正值的、最小的前50%,但要注意符合要求股票数量是否太少
        temp_df_ana_value = temp_df_ana[ temp_df_ana["EST_PE_FY1"] >=0.0 ]
        temp_df_ana_value = temp_df_ana_value.sort_values(by="EST_PE_FY1",ascending=True )
        
        if len( temp_df_ana_value.index) >= 2  :
            # 超过2只才可以取前50%
            temp_len = round( float(len( temp_df_ana_value.index)) /2 )
            temp_df_ana_value= temp_df_ana_value.iloc[ :temp_len , : ]
            df_abcd3d_ana = group_mean_indicators(temp_i_value, temp_df_ana_value, df_abcd3d_ana )

        ##############################################################################
        ### 计算成长组
        temp_i_growth = str(temp_i) + "_growth"
        df_abcd3d_ana.loc[temp_i_growth,"group_type" ] = "growth" # "value_growth"
        df_abcd3d_ana.loc[temp_i_growth,"group_name" ] = "成长_" + str(df_abcd3d_ana.loc[temp_i,"group_name" ])
        # temp_df_ana["EST_PE_FY1"] 取有正值的、最小的前50%
        temp_df_ana_growth  = temp_df_ana[ temp_df_ana["EST_PE_FY1"] >=0.0 ]
        temp_df_ana_growth  = temp_df_ana_growth[ temp_df_ana_growth["EST_PEG_FY1"] >=0.0 ]
        temp_df_ana_growth["growth_fy1"] = temp_df_ana_growth["EST_PE_FY1"]/temp_df_ana_growth["EST_PEG_FY1"]
        
        # 降序排列取前50%
        temp_df_ana_growth= temp_df_ana_growth.sort_values(by="growth_fy1",ascending=False )
        if len(  temp_df_ana_growth.index)  >= 2  :
            # 超过2只才可以取前50%
            temp_len = round( float(len( temp_df_ana_growth.index)) /2 )
            temp_df_ana_growth= temp_df_ana_growth.iloc[ :temp_len , : ]
            
            df_abcd3d_ana = group_mean_indicators(temp_i_growth, temp_df_ana_growth, df_abcd3d_ana ) 
        return df_abcd3d_ana 

    def market_status_abcd3d_ana(self,obj_ana) :
        ### 全市场、行业内个股的动量状态分析，基于已有的abcd3d指标
        '''
        todo
        1,对几个流通市值组内进一步细分：成长、价值；
        2,创业板和科创板内划分成长、价值；
        notes:成长和价值若分别用roe和pe，容易造成股票的重合；
        3，对每个行业内分大小市值-50%、成长\价值-50%
        200602： 增加区间收盘价所处价格百分比统计： "close_pct_s_"+str(x) 
        tree:
        1,新建df_abcd3d_ana,index是不同的分组例如沪深300、医疗行业等，columns是分析指标
        1.1，成交金额：amt_1_300 :301_800,801_1800,1801_end
        1.2，流通市值：mvfloat_1_300:流通市值前300、500、1000；
            AShareEODDerivativeIndicator{当日流通市值,S_DQ_MV;当日总市值,S_VAL_MV};
        1.2.1，mvfloat_1_300等内分行业选股
        1.2.2，mvfloat_1_300等内成长指标选股
        1.3，行业：ind_citics_1_20 :中信一级行业
        1.4, 常用指标：pe,pb，pcf, dps;
            AShareEODDerivativeIndicator{S_VAL_PE_TTM,市盈率(PE,TTM){若净利润<=0,则返回空},
            市净率(PB),S_VAL_PB_NEW;
            市现率(PCF,经营现金流TTM)S_VAL_PCF_OCFTTM;股价/每股派息,S_PRICE_DIV_DPS }
        1.5,滚动预期类指标：
            Wind一致预测个股滚动指标，AShareConsensusRollingData{
            1，NET_PROFIT
            2，市盈率,EST_PE，FY0,FY1,FTTM,YOY,YOY2
            3，PEG,EST_PEG
            4,市净率,EST_PB
            5,每股现金流,EST_CFPS
            6,利润总额,EST_TOTAL_PROFIT;营业利润,EST_OPER_PROFIT;基准年度,BENCHMARK_YR   }
            中国A股投资评级汇总,AShareStockRatingConsus{
            1,
            2，
            }
        1.6,其他指标：涨停家数、创新高等
            AShareEODDerivativeIndicator{
            涨跌停状态,UP_DOWN_LIMIT_STATUS,1表示涨停;0表示非涨停或跌停;-1表示跌停。
            最高最低价状态,LOWEST_HIGHEST_STATUS,1表示是历史最高收盘价;0表示非历史最高价或最低价;-1表示是历史最低收盘价。    }
        notes: 
        '''
        ########################################################################
        ### Initialization 
        date_list = obj_ana["date_list"] 

        # from data_io import data_timing_abcd3d
        # data_timing_abcd3d_1 = data_timing_abcd3d()
        from data_io_pricevol_financial import data_pricevol_financial
        data_pricevol_financial_1 = data_pricevol_financial()

        ### 定义标准化的汇总分析 | def group_mean_abcd3d_ana(self,temp_i,df_abcd3d_ana, temp_df_ana) :
            
        ########################################################################
        ### abcd3d全市场个股分组和分行业统计
        ### 导入行业分类及对应的中文
        path_ind_names = self.obj_config["dict"]["path_wind_adj"]
        df_ind_names = pd.read_csv(path_ind_names+ "ind_code_name.csv"  ,encoding="gbk" )
        path_output = self.obj_config["dict"]["path_ciss_db"]+"timing_abcd3d\\market_status_group\\"

        obj_data={}
        obj_data["dict"] ={}
        # for temp_date in date_list_post :
        for temp_date in date_list :
            # print("temp_date")

            obj_data["dict"]["date_start"] =  temp_date
            # obj_data["dict"]["date_start"] = input("Type in year start :from 20060104:") 
            
            ### 导入历史行情和abcd3d数据
            obj_data = data_pricevol_financial_1.import_data_ashare_change_amt( obj_data)
            df_mom_eod_prices = obj_data["df_mom_eod_prices"]
            num_stocks = len( df_mom_eod_prices.index )
            
            ########################################################################
            ### 获取中信一级行业列表
            # [0.0, 10.0, 11.0, 12.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 30.0, 
            # 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 40.0, 41.0, 42.0, 50.0, 60.0, 61.0, 62.0, 63.0, 70.0]
            list_ind_code = obj_data["df_mom_eod_prices"]["ind_code"].drop_duplicates().to_list()
            list_ind_code.sort()
            list_ind_code_str = [ str(int(x)) for x in list_ind_code  ]

            # ########################################################################
            # ### 导入市值、财务指标ttm 、预期数据
            # obj_data = data_pricevol_financial_1.import_data_ashare_mv_fi_esti( obj_data)

            ########################################################################
            ### 1,分析统计分组：df_abcd3d_ana,index是不同的分组例如沪深300、医疗行业等，columns是分析指标
            '''
            核心分组：流通市值大中小；
            大类板块：金融地产40+41+42+43、必须消费30+31+32+33+34、食品饮料农林牧渔36+37、
                医疗35、电子60、通讯61、计算机62、传媒63、电力设备新能源27
            '''
            list_rank=[ [1,300],[301,800],[801,1800],[1801,10000] ]
            ### 1.1，成交金额全市场分组：amt_1_300 :301_800,801_1800,1801_end
            list_index0 = ["all"]
            for word in ["amt","mvfloat","mvtotal"   ] :
                for temp_rank in list_rank :
                    word_para = word + "_1day_"+ str(temp_rank[0]) +"_"+ str(temp_rank[1])
                    list_index0 = list_index0 + [ word_para ]
            print( list_index0 )

            '''1,df_abcd3d_ana.columns:
            组内算术平均值:abcd3d_ave_num
            流通市值加权平均值、abcd3d_ave_mvfloat
            数值为-6~-4,-3~3,4~6的加权占比 abcd3d_pct_down,abcd3d_pct_up
            "close_pct"：收盘价所处过去40天百分比
            '''
            col_list=["abcd3d_ave_num","abcd3d_ave_mvfloat","abcd3d_pct_down","abcd3d_pct_up","group_type","close_pct"  ]
            ### 增加 涨跌停、新高新低数量、基本财务
            col_list= col_list +["num_up_limit","num_down_limit","num_new_high","num_new_low" ]
            col_list= col_list +["PE_ttm","PE_fy1","PEG_FY1" ]

            df_abcd3d_ana=pd.DataFrame(index= list_index0, columns=col_list )
            
            ########################################################################
            ### 1.2，统计全市场指标："abcd3d",以及依据的短期和中期指标：indi_short	indi_mid

            temp_i = "all"
            # "group_type"分组类型，市场 market、行业 industry;"group_name"分组名称
            df_abcd3d_ana.loc[temp_i,"group_type" ] = "market"
            df_abcd3d_ana.loc[temp_i,"group_name" ] = "全部A股"
            temp_df_ana = df_mom_eod_prices
            
            df_abcd3d_ana = self.group_mean_abcd3d_ana(temp_i,df_abcd3d_ana, temp_df_ana )
 
            ### 创业板股票 "300" 开头
            str_filter = "300"
            temp_df_ana = df_mom_eod_prices
            temp_df_ana["temp"] = temp_df_ana[ "S_INFO_WINDCODE"].apply(lambda code: 1 if code[:3]==str_filter else 0 )
            temp_df_ana = temp_df_ana[ temp_df_ana["temp"]==1 ]

            temp_i = "chinext"
            # "group_type"分组类型，市场 market、行业 industry;"group_name"分组名称
            df_abcd3d_ana.loc[temp_i,"group_type" ] = "market"
            df_abcd3d_ana.loc[temp_i,"group_name" ] = "创业板"

            df_abcd3d_ana = self.group_mean_abcd3d_ana(temp_i,df_abcd3d_ana, temp_df_ana )
            
            ### 科创板股票 "688" 开头
            str_filter = "688"
            temp_df_ana = df_mom_eod_prices
            temp_df_ana["temp"] = temp_df_ana[ "S_INFO_WINDCODE"].apply(lambda code: 1 if code[:3]==str_filter else 0 )
            temp_df_ana = temp_df_ana[ temp_df_ana["temp"]==1 ]

            temp_i = "star"
            # "group_type"分组类型，市场 market、行业 industry;"group_name"分组名称
            df_abcd3d_ana.loc[temp_i,"group_type" ] = "market"
            df_abcd3d_ana.loc[temp_i,"group_name" ] = "科创板"

            df_abcd3d_ana = self.group_mean_abcd3d_ana(temp_i,df_abcd3d_ana, temp_df_ana )

            ########################################################################
            ### 1.3，统计分组指标："abcd3d" 
            # notes:2006年时A股数量约1300个，2020约3900个。默认1~300,301~800两组肯定有
            list_rank=[ [1,300],[301,800],[801,1800],[1801,10000] ]

            ### 当日成交金额，"amt","S_DQ_AMOUNT"
            word ="amt" 

            df_mom_eod_prices = df_mom_eod_prices.sort_values(by="S_DQ_AMOUNT",ascending=False)
            for temp_rank in list_rank :
                # 判断股票数量是否足够大
                if num_stocks > temp_rank[0] :
                    # print( temp_rank[0],temp_rank[1] )
                    # for para in ["_1day_1_300","_1day_301_800","_1day_801_1800","_1day_1801_end"] :
                    temp_i = word + "_1day_"+ str(temp_rank[0]) +"_"+ str(temp_rank[1])
                    # "group_type"分组类型，市场 market、行业 industry;"group_name"分组名称
                    df_abcd3d_ana.loc[temp_i,"group_type" ] = "market"
                    df_abcd3d_ana.loc[temp_i,"group_name" ] = "成交金额_"+str(temp_rank[0]) +"_"+ str(temp_rank[1])
                    # notes:第N~M个值在index里对应的是 N-1~M-1个。
                    temp_df_ana = df_mom_eod_prices.iloc[ temp_rank[0]-1:temp_rank[1] ,:]
                    df_abcd3d_ana = self.group_mean_abcd3d_ana(temp_i,df_abcd3d_ana, temp_df_ana )

            ### 当日流通市值，mvfloat，'S_DQ_MV'
            word ="mvfloat"  
            df_mom_eod_prices = df_mom_eod_prices.sort_values(by="S_DQ_MV",ascending=False) 
            for temp_rank in list_rank :
                # 判断股票数量是否足够大
                if num_stocks > temp_rank[0] :
                    # print( temp_rank[0],temp_rank[1] )
                    # for para in ["_1day_1_300","_1day_301_800","_1day_801_1800","_1day_1801_end"] :
                    temp_i = word + "_1day_"+ str(temp_rank[0]) +"_"+ str(temp_rank[1])
                    # "group_type"分组类型，市场 market、行业 industry;"group_name"分组名称
                    df_abcd3d_ana.loc[temp_i,"group_type" ] = "market"
                    df_abcd3d_ana.loc[temp_i,"group_name" ] = "流通市值_"+str(temp_rank[0]) +"_"+ str(temp_rank[1])

                    temp_df_ana = df_mom_eod_prices.iloc[ temp_rank[0]-1:temp_rank[1] ,:]

                    df_abcd3d_ana = self.group_mean_abcd3d_ana(temp_i,df_abcd3d_ana, temp_df_ana )

            ### 当日总市值，mvtotal,"S_VAL_MV"
            word ="mvtotal"  
            df_mom_eod_prices = df_mom_eod_prices.sort_values(by="S_VAL_MV",ascending=False) 
            for temp_rank in list_rank :
                # 判断股票数量是否足够大
                if num_stocks > temp_rank[0] :
                    # print( temp_rank[0],temp_rank[1] )
                    # for para in ["_1day_1_300","_1day_301_800","_1day_801_1800","_1day_1801_end"] :
                    temp_i = word + "_1day_"+ str(temp_rank[0]) +"_"+ str(temp_rank[1])
                    # "group_type"分组类型，市场 market、行业 industry;"group_name"分组名称
                    df_abcd3d_ana.loc[temp_i,"group_type" ] = "market"
                    df_abcd3d_ana.loc[temp_i,"group_name" ] = "总市值_"+str(temp_rank[0]) +"_"+ str(temp_rank[1])

                    # notes:第N~M个值在index里对应的是 N-1~M-1个。
                    temp_df_ana = df_mom_eod_prices.iloc[ temp_rank[0]-1:temp_rank[1] ,:]
                    df_abcd3d_ana = self.group_mean_abcd3d_ana(temp_i,df_abcd3d_ana, temp_df_ana )

            ### 计算行业分组：list_ind_code以及排序过
            '''
            行业分组：流通市值大中小；大类板块：金融地产40+41+42+43、必须消费30+31+32+33+34、食品饮料农林牧渔36+37、
                医疗35、电子60、通讯61、计算机62、传媒63、电力设备新能源27
            list_ind_code_str = [ str(int(x)) for x in list_ind_code  ]
            '''
            for ind_code in list_ind_code :
                # code_str = list_ind_code_str( list_ind_code.index(ind_code) )
                code_str = str(int( ind_code ))
                # find code name in df_ind_names
                print( ind_code )
                # print(  df_ind_names["ind_code"].values )
                df_ind_names_sub = df_ind_names[ df_ind_names["ind_code"]== int(ind_code) ]
                if len(df_ind_names_sub.index ) > 0 :
                    ind_name = df_ind_names_sub["ind_name"].values[0]
                    df_mom_eod_prices_ind = df_mom_eod_prices[ df_mom_eod_prices["ind_code"]==ind_code ]

                    temp_i = ind_code
                    # "group_type"分组类型，市场 market、行业 industry;"group_name"分组名称
                    df_abcd3d_ana.loc[temp_i,"group_type" ] = "industry"
                    df_abcd3d_ana.loc[temp_i,"group_name" ] = ind_name
                    
                    df_abcd3d_ana = self.group_mean_abcd3d_ana(temp_i,df_abcd3d_ana, df_mom_eod_prices_ind )
            
            ### Sort for exhibition 为展示阅读进行排序
            # group_list = ["market","industry","value_growth"] 
            group_list = ["market","industry","value","growth"]
            count_group = 0 
            for temp_group in group_list : 
                df_abcd3d_ana_sub = df_abcd3d_ana [df_abcd3d_ana["group_type"]==temp_group ]
                df_abcd3d_ana_sub=df_abcd3d_ana_sub.sort_values(by="abcd3d_ave_mvfloat",ascending=False)
                if count_group == 0 :
                    df_out = df_abcd3d_ana_sub
                    count_group = 1 
                else :
                    df_out = df_out.append(df_abcd3d_ana_sub)
            df_abcd3d_ana = df_out

            ### save to csv file 
            # D:\CISS_db\timing_abcd3d\market_status_group
            file_name = "abcd3d_market_ana_trade_dt_" + str(temp_date) + ".csv"
            print( df_abcd3d_ana)
            df_abcd3d_ana.to_csv(path_output + file_name ,encoding="gbk")

        ### 
        obj_ana["path_output"] = path_output
        obj_ana["file_name"] = file_name
        obj_ana["list_group_type"] = df_abcd3d_ana["group_type" ].to_list()
        obj_ana["list_group_name"] = df_abcd3d_ana["group_name" ].to_list()

        return obj_ana

#########################################################
### 动量指标
class indicator_momentum():
    def __init__(self, indicator_name ):
        self.indicator_name = indicator_name


    # def load_quotes(self,quote_type='CN_day',sp_df,  config_IO,symbol_list,date_start,date_end) :


    def indi_mom_ma_all(self,code_head,code_df ):
        # Calculate for whole time series 

        # todo 需要至少之前100天的quotation 数据。
        # 注意indexnumber =400的时间处，用windows=40去计算，平均值对应的是｛400-40+1~400｝
        # 该时间应该使用 index_num=399处的平均值。
        # index_num={1:39},数值是 NaN

        # 升序 Ascending，in case we do not have sorted data from csv file
        code_df= code_df.sort_values(['date'],ascending=True ) 

        # reference rC_Stra_MAX.py\AnalyticaData
        from config.config_indicator import config_indi_mom
        technical_ma = config_indi_mom('').technical_ma()
        # period of moving averagee
        ma_x = technical_ma['ma_x'] # [3,8,16,40,100]
        # relative value of price over moving average price 
        p_ma = technical_ma['p_ma'] # [0,0,0,0,0]
        # status of moving average 
        ma_up = technical_ma['ma_up'] # =[1,1,1,1,1] 
        # generate analitical parameters
        code_df_ana=code_df 
        code_df_ana['close_pre']  = code_df_ana['close' ].shift(1)
        code_df_ana['high_pre']  = code_df_ana['high' ].shift(1)
        code_df_ana['low_pre']  = code_df_ana['low' ].shift(1)
        code_df_ana['amt_pre']  = code_df_ana['amt' ].shift(1)
        code_df_ana['turn_pre']  = code_df_ana['turn' ].shift(1)
        columns_mom_ma = []
        for ma_x_i in ma_x :
            # Moving Average  , close_ma(x)
            temp_str= 'ma' + str( ma_x_i )
            
            code_df_ana[temp_str ] = pd.rolling_mean(code_df_ana['close' ],window= ma_x_i )
            # 要避免看穿未来，对T日的预测只能用T-1日作为最新数据
            # df..shift(1) means shift downward 向下平移1位，第一位数值会变成NaN
            # df..shift(-1) means shift upnward 向上平移
            code_df_ana[temp_str+'_pre' ] = code_df_ana[temp_str ].shift(1)
            # 因为要做分母，replace 0 or negative value to be large number or local max。
            def avoid_zero(x):
                if x <= 0 :
                    x= NaN
                return x
            code_df_ana[temp_str+'_pre' ] =code_df_ana[temp_str+'_pre'].map(lambda x: avoid_zero(x),na_action=None)
            # replace NaN with nearest values 
            code_df_ana[temp_str+'_pre' ] =code_df_ana[temp_str+'_pre'].fillna(method='ffill')
            columns_mom_ma = columns_mom_ma +[temp_str+'_pre']

            # 'P/MA8', pre close over close_ma(x)
            temp_str2= 'dif_P_MA' + str( ma_x_i )
            columns_mom_ma = columns_mom_ma +[temp_str2]
            # code_df_ana[temp_str2] = code_df_ana['close']/code_df_ana[temp_str+'_pre' ] 
            code_df_ana[temp_str2] = code_df_ana.apply(lambda x: x['close_pre']/x[temp_str+'_pre'],axis=1) 
            
            # 'MA3_up' close_ma(x)_T over close_ma(x)_T-1
            temp_str3= 'ma' + str( ma_x_i ) + '_up'
            columns_mom_ma = columns_mom_ma +[temp_str3]
            code_df_ana[temp_str3] =  code_df_ana[temp_str+'_pre' ].diff(1) 

            # 'P/H100' pre close over pre high value of past 100 days
            temp_str4= 'dif_P_H' + str( ma_x_i ) 
            code_df_ana[ temp_str4 ] = pd.rolling_mean(code_df_ana['high' ],window= ma_x_i )
            code_df_ana[temp_str4+'_pre' ] = code_df_ana[temp_str4 ].shift(1)
            columns_mom_ma = columns_mom_ma +[temp_str4+'_pre']

            # 'P/L100'
            temp_str5= 'dif_P_L' + str( ma_x_i ) 
            code_df_ana[ temp_str5 ] = pd.rolling_mean(code_df_ana['low' ],window= ma_x_i )
            code_df_ana[temp_str5 +'_pre' ] = code_df_ana[temp_str5 ].shift(1)
            columns_mom_ma = columns_mom_ma +[temp_str5+'_pre']
            
            # amt_ma(x)
            temp_str6= 'amt_ma_pre' + str( ma_x_i ) 
            columns_mom_ma = columns_mom_ma +[temp_str6 ]
            code_df_ana[temp_str6 ] = pd.rolling_mean(code_df_ana['amt_pre' ],window= ma_x_i )
            # amt/amt_ma(x)
            # temp_str7= 'amt_amt_ma_pre' + str( ma_x_i ) 
            # columns_mom_ma = columns_mom_ma +[temp_str7 ]
            # code_df_ana[temp_str7 ] = code_df_ana.apply(lambda x: x['amt_pre']/x[temp_str6+'_pre'],axis=1) 

            # turn_ma(x)
            # temp_str8= 'turn_ma_pre' + str( ma_x_i ) 
            # columns_mom_ma = columns_mom_ma +[temp_str8 ]
            # code_df_ana[temp_str8 ] = pd.rolling_mean(code_df_ana['turn_pre' ],window= ma_x_i )
            # turn/turn_ma(x)
            # temp_str9= 'turn_turn_ma_pre' + str( ma_x_i ) 
            # columns_mom_ma = columns_mom_ma +[temp_str9 ]
            # code_df_ana[temp_str9 ] = code_df_ana.apply(lambda x: x['turn_pre']/x[temp_str8+'_pre'],axis=1) 

 
        return code_df_ana

    def indi_mom_ma_1D(self,code_head,code_df ):
        # Calculate for given date 


        code_df_ana = 1 



        return code_df_ana






 















