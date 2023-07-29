# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
last update 190712 | since  181106


功能：Function:
1,class signals_ashare():
    1.1,get_signal_filter_level_para |根据给定指标列表和筛选标准生成买卖信号列

2,class signals():

分析：
1，策略信号管理 
1.1, 经典策略信号单点包含{-1,0.1} 三个值，分别代表 卖出，持有，买入三种交易信号
1.2，新信号具备更丰富的意义：
    1，信号是一个时间序列，能充分代表策略输出的投资预测的所有核心要素
    2，信号和预测的区别：预测时传统意义上的定性分析，信号是遵循一定数据变量规范，直接并入
    策略系统计算的标准化数据。
    3，定义：频率， ||ref 采样频率为Fs，信号频率F，采样点数为N,采样时间越长，频率分辨率越高；采样率越大，频谱宽度越长


2，配置文件 | config\config_signals.py
 
4,output file 
    1,head json file of signals 
    2,supportting information of signals

Notes: 
refernce: rC_Portfolio_17Q1.py 
===============================================
'''
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
import pandas as pd
import numpy as np

###################################################
class signals_ashare():
    def __init__(self ):
        ### 导入配置文件
        #######################################################################
        ### 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")
        from config_data import config_data_factor_model
        config_data_1 = config_data_factor_model()
        self.obj_config = config_data_1.obj_config

        #######################################################################
    
    def print_info(self):
        ### print(  )
        print("get_signal_filter_level_para |根据给定指标列表和筛选标准生成买卖信号列")
        print("  ")

        return 1

    def get_signal_filter_level_para(self,obj_signal):
        ### 根据给定指标列表和筛选标准生成买卖信号列
        '''
        现在是把具体指标都罗列出来，未来改进成统一的规范
        todo:
        1,df_ashare_ana:df表
        2,col_list:需要计算信号的列
        3,direction_list:需要计算信号的列的比较方向，如<,>=,或其他def function()
        4,para_list：需要计算信号的列的数值比较参数
        
        '''
        
        leverage_para = obj_signal["dict"]["leverage_para"] #=1 
        df_ashare_ana = obj_signal["df_ashare_ana"]

        print("Initial number of stocks ", len(df_ashare_ana.index ) )
        ### 1,季度平均roe不低于 2.5%;季度roe同比改善1个点
        # df_ashare_ana["S_FA_ROE" +"_signal"] = df_ashare_ana[ df_ashare_ana [ "S_FA_ROE" +"_q_ave" ]>= 0.025*leverage_para ]
        # df_ashare_ana = df_ashare_ana[ df_ashare_ana [ "S_FA_ROE" +"_diff" ]>= 0.003*leverage_para ]
        df_ashare_ana["S_FA_ROE" +"_q_ave" +"_signal"] = df_ashare_ana[ "S_FA_ROE" +"_q_ave" ].apply(lambda x : 1 if x >= 0.025*leverage_para else 0  )  
        df_ashare_ana["S_FA_ROE" +"_diff" +"_signal"] = df_ashare_ana[ "S_FA_ROE" +"_diff" ].apply(lambda x : 1 if x >= 0.003*leverage_para else 0  )  
        df_ashare_ana["all" +"_signal"] = df_ashare_ana["S_FA_ROE" +"_q_ave" +"_signal"] *df_ashare_ana["S_FA_ROE" +"_diff" +"_signal"]
        print("Update:filter=roe ", df_ashare_ana["all" +"_signal"].sum() )

        ### 2,单季度.营业总收入环比增长率(%)>0.05;单季度.营业总收入同比增长率(%) >0.05
        df_ashare_ana["S_QFA_CGRGR" +"_diff"  +"_signal"] = df_ashare_ana[ "S_QFA_CGRGR" +"_diff" ].apply(lambda x : 1 if x >= 0.03*leverage_para else 0  )  
        df_ashare_ana["S_QFA_CGRPROFIT" +"_diff"  +"_signal"] = df_ashare_ana[ "S_QFA_CGRPROFIT" +"_diff"  ].apply(lambda x : 1 if x >= 0.03*leverage_para else 0  )  
        df_ashare_ana["all" +"_signal"] = df_ashare_ana["all" +"_signal"] *df_ashare_ana["S_QFA_CGRGR" +"_diff"  +"_signal"] 
        df_ashare_ana["all" +"_signal"] = df_ashare_ana["all" +"_signal"] *df_ashare_ana["S_QFA_CGRPROFIT" +"_diff"  +"_signal"] 
        print("Update:filter=revenue_growth ", df_ashare_ana["all" +"_signal"].sum() )


        ### 3,单季度.净利润同比增长率(%) >0.05;单季度.净利润环比增长率(%)  >0.05
        # df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_QFA_YOYPROFIT" ] >= 0.05*leverage_para   ]
        # df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_QFA_CGRPROFIT" ]>= 0.05*leverage_para ]
        # df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_QFA_CGRPROFIT" +"_q_pre" ] >= 0.05*leverage_para  ]
        # print("Update:filter=earning_growth ", len(df_ashare_ana.index ) )

        ### 4，盈利持续性: 季度平均ROIC >=0.018, 季度平均总资产净利率ROA >=0.015
        df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_FA_ROIC" +"_q_ave" ] >= 0.01*leverage_para   ]
        df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_FA_ROIC" +"_diff" ] >= 0.003*leverage_para   ]
        df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_FA_ROA" +"_q_ave" ] >= 0.01*leverage_para   ]
        df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_FA_ROA" +"_diff" ] >= 0.003*leverage_para   ]

        df_ashare_ana["S_FA_ROIC" +"_q_ave"+"_signal"] = df_ashare_ana["S_FA_ROIC" +"_q_ave" ].apply(lambda x : 1 if x >= 0.01*leverage_para else 0  )  
        df_ashare_ana["S_FA_ROIC" +"_diff" +"_signal"] = df_ashare_ana["S_FA_ROIC" +"_diff"].apply(lambda x : 1 if x >= 0.003*leverage_para else 0  )  
        df_ashare_ana["all" +"_signal"] = df_ashare_ana["all" +"_signal"] *df_ashare_ana["S_FA_ROIC" +"_q_ave"+"_signal"] 
        df_ashare_ana["all" +"_signal"] = df_ashare_ana["all" +"_signal"] *df_ashare_ana["S_FA_ROIC" +"_diff" +"_signal"]

        df_ashare_ana[ "S_FA_ROA" +"_q_ave"+"_signal"] = df_ashare_ana[ "S_FA_ROA" +"_q_ave"].apply(lambda x : 1 if x >= 0.01*leverage_para else 0  )  
        df_ashare_ana[ "S_FA_ROA" +"_diff"  +"_signal"] = df_ashare_ana[ "S_FA_ROA" +"_diff" ].apply(lambda x : 1 if x >= 0.003*leverage_para else 0  )  
        df_ashare_ana["all" +"_signal"] = df_ashare_ana["all" +"_signal"] *df_ashare_ana["S_FA_ROA" +"_q_ave"+"_signal"] 
        df_ashare_ana["all" +"_signal"] = df_ashare_ana["all" +"_signal"] *df_ashare_ana["S_FA_ROA" +"_diff" +"_signal"]

        print("Update:filter=earning_consistant ", df_ashare_ana["all" +"_signal"].sum() )

        ### 5，季度资产质量：经营活动产生的现金流量净额/营业收入,S_FA_OCFTOOR >= 0.05,数值主要参考某一期观察的平均值
            # 经营活动产生的现金流量净额/经营活动净收益,S_FA_OCFTOOPERATEINCOME >= 0.35 
        # df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_FA_OCFTOOR" ] >= 0.05*leverage_para   ]
        # df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_FA_OCFTOOPERATEINCOME" ] >= 0.35*leverage_para  ]
        # df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_FA_OCFTOOR" +"_diff"] >= 0.01*leverage_para   ]
        # df_ashare_ana = df_ashare_ana[df_ashare_ana[ "S_FA_OCFTOOPERATEINCOME" +"_diff"] >= 0.03*leverage_para  ]

        df_ashare_ana[ "S_FA_OCFTOOR"  +"_diff"  +"_signal"] = df_ashare_ana[ "S_FA_OCFTOOR" +"_diff" ].apply(lambda x : 1 if x >= 0.01*leverage_para else 0  )  
        df_ashare_ana["all" +"_signal"] = df_ashare_ana["all" +"_signal"] *df_ashare_ana["S_FA_OCFTOOR" +"_diff"+"_signal"] 

        print("Update:filter=earning_quality ",  df_ashare_ana["all" +"_signal"].sum() )

        ###Save to output
        obj_signal["df_ashare_ana"] = df_ashare_ana
        
        return obj_signal

###################################################
class signals():
    def __init__(self,id_time_stamp,signals_name=''):
        self.signals_name =signals_name
        # define columns of signals_df and signal model
        signals_head = self.gen_signals_head(id_time_stamp,{},signals_name) 
        self.signals_head = signals_head
        signals_df = self.gen_signals_stock()
        self.signals_df = signals_df

    def print_info(self):
        ### print(  )

        print("All columns for signals_df==weight_list ")
        print("代码，买卖，按账户总值百分比，持仓百分比，现金可用百分比，市场流动性百分比， \n  'code','bsh','pct_account','pct_holding','pct_cash','pct_liquid', 'number','amt' ")
        print("资产类别，市场，货币,'asset','market','currency'")
        print("委托方向、委托类型{数量、金额、组合净值比例，股票持仓比例}，价格-预估或限制，是否限价,\n 'entrust_dir','entrust_type'-'num','amt','pct_port','pct_stock'--,'price_limit','if_limit' ")
        print("num","amt","pct_port","pct_stock")

        print("Function:update_signals_stock_weight | get precise signal list using weight list as input")

        return 1

    def gen_signals_head(self, id_time_stamp,config={},signals_name='') :
        # generate head file of signals
        import sys
        sys.path.append("..")
        # from db.basics import time_admin
        # time_admin1 = time_admin()
        # time_stamp = time_admin1.get_time_stamp()

        signals_head ={}
        if config == {} :
            ## Basic info
            # initial date of generate signals 
            signals_head["InitialDate"] = "" # previous 
            signals_head["Index_Name"] = ""
            if signals_name =='':
                signals_head["signals_name"] = "name_signals_" + id_time_stamp
                # str(time_stamp ) == id_time_stamp
                signals_head["signals_id"] =  "id_signals_" + id_time_stamp 
                signals_head["sp_id_time"]= id_time_stamp
            else :
                signals_head["signals_name"] = "name_signals_" +signals_name
                signals_head["signals_id"] =  "id_signals_" + id_time_stamp +"_"+signals_name
                signals_head["sp_id_time"]= id_time_stamp
            # frequency of signal as a time series 
            signals_head["time_frequency"]='D' # Daily,Weekly,Monthly,Quarterly...
            # 1 means signal can do short estimation and 0 means no short selling positions
            signals_head["estimate_short"] = 0 
            signals_head["estimate_type"] = 0 
            # whcih analyzing method is used to produce signals
            signals_head["analyzing_method"] = 0 
            # whcih analyzing measure is used to produce signals:absolute values,percentage,log normal。。。
            signals_head["analyzing_measure"] = 0 


        return signals_head

    def gen_signals_stock(self) :
        # generate dataframe signals # format: DataFrame 
        # befo1907:old version:  ['Signal', 'temp_Ana', 'Order', 'Symbol'])
        # last 190712
        
        ### 按账户总值百分比，持仓百分比，现金可用百分比，市场流动性百分比 
        ### 'pct_account','pct_holding','pct_cash','pct_liquid'
        columns_signals= ['code','bsh','pct_account','pct_holding','pct_cash','pct_liquid',
        'number','amt']
        ### 资产类别，市场，货币,'asset','market','currency'
        columns_signals= columns_signals + ['asset','market','currency']
        ### 增加 委托方向、委托类型{数量、金额、组合净值比例，股票持仓比例}，价格{预估或限制}，是否限价。
        ### "entrust_dir","entrust_type"{"num","amt","pct_port","pct_stock"},"price_limit","if_limit"
        columns_signals= columns_signals + ["entrust_dir","entrust_type","price_limit","if_limit"]
        ### "num","amt","pct_port","pct_stock"
        columns_signals= columns_signals + ["num","amt","pct_port","pct_stock"]

        signals_df =  pd.DataFrame(columns=columns_signals)
        
        ### pure signal： 1，0，-1 ;多头，无、空头
        signals_df['signal_pure'] = 1 

        return signals_df

    def update_signals_stock_weight(self,optimizer_weight_list,portfolio_suites):
        ### Function:get precise signal list using weight list 
        ### NOTES: signals_df 实质上就是 weight_list
        # last 190712 
        # minimum output columns: code,entrust direction委托方向，entrust method委托方式
        # columns:["code","entrust_dir","entrust_type","weight","num","price"]
        # derived from def gen_signals_stock(self)

        portfolio_suites.signals.signals_df = optimizer_weight_list
        # signals_list = optimizer_weight_list
        ## using cash 
 

        return portfolio_suites




