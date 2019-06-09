# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 181106 | since  181106
Menu :
Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }
    
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
import pandas as pd 
import sys
sys.path.append("..")
###################################################
class signals():
    def __init__(self,id_time_stamp,signals_name=''):
        self.signals_name =signals_name
        # define columns of signals_df and signal model
        signals_head = self.gen_signals_head(id_time_stamp,{},signals_name) 
        self.signals_head = signals_head
        signals_df = self.gen_signals_stock()
        self.signals_df = signals_df

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
        # generate dataframe signals
        # old version:  ['Signal', 'temp_Ana', 'Order', 'Symbol'])
        # 按账户总值百分比，持仓百分比，现金可用百分比，市场流动性百分比 'pct_account','pct_holding','pct_cash'
        columns_signals= ['code','bsh','pct_account','pct_holding','pct_cash','pct_liquid',
        'number','amt','asset','market','currency']
        # 资产类别，市场，货币,'asset','market','currency'

        signals_df =  pd.DataFrame(columns=columns_signals)
        # pure signal： 1，0，-1
        signals_df['signal_pure'] = 1 
        return signals_df


    def update_signals_stock_weight(self,optimizer_weight_list,portfolio_suites):
        # using weight list to get precise signal list 

        ## load trading days to get latest trading dates 


        ## step load quotes using given dates 


        portfolio_suites.signals.signals_df = optimizer_weight_list
        # signals_list = optimizer_weight_list
        ## using cash 
 

        return portfolio_suites




