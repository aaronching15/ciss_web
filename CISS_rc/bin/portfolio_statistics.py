# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 190110 | since  190110
Menu :
分析：
1，portfolio statistics 组合相关数据的统计分析
    1，要素：
    2，过程：
    

2，配置文件 | config\config_port.py

Notes: 
refernce: apps\test_stats.py 
===============================================
'''
import pandas as pd 

class port_stats():
    def __init__(self,stra_id='CN001',stra_founder='rc',stra_supervisor="Du"  ):
        self.stra_id = stra_id
        self.stra_founder = stra_founder
        self.stra_supervisor = stra_supervisor
        path_ind = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\static\\"
        file_ind_CN = "ind_wind_CN.csv"
        self.dict_ind = pd.read_csv(path_ind + file_ind_CN,encoding="gbk")

    def account_ret_month(self,df_asum):
        ### Calculate monthly return 
        # last 190111 | since 190111
        import numpy as np 
        ### import and  get all unit and mddd at  end of months
        path_stat = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\db\\db_times\\"
        file_time = "times_CN_day_20120101_20181102.csv"
        time_raw = pd.read_csv(path_stat + file_time)
        time_raw['date'] = pd.to_datetime( time_raw['SSE'] )

        years = [2014,2015,2016,2017,2018]
        # float to string 
        months = np.linspace(1,12,12).astype(np.int16)
        import datetime as dt 
        end_mon_dates = []
        for i_year in years:
            for i_month in months:
                # notes: i_month might be single digit 
                temp_date = str(i_year)+'-'+str(i_month)+'-'+'01'
                temp_date = dt.datetime.strptime(temp_date,"%Y-%m-%d")
                time_end_mon = time_raw[ time_raw['date']< temp_date ]
                # print("time_end_mon")
                # print( time_end_mon.tail() )
                end_mon_dates = end_mon_dates + [ time_end_mon.loc[time_end_mon.index[-1],'SSE' ] ] 

        # print(end_mon_dates)

        df_end = pd.DataFrame(end_mon_dates,columns=["end_dates_mon"]  )


        df_asum2 = df_asum[ df_asum["date"].isin(end_mon_dates ) ]
        # df_asum2.reset_index(drop=True) 
        df_asum2.index = df_asum2["date"]
        # print( "df_asum2" )
        # print( df_asum2 )

        df_asum2['mdd_mon'] =0.0
        df_asum2['ret'] =0.0
        i=0 
        mdd_mon = 0
        for temp_i in df_asum2.index :
            # skip the first month
            if temp_i != df_asum2.index[0] :
                # we want to find monthly return and monthly mdd 
                unit_mon_pre = df_asum2.loc[index_pre ,'unit']
                
                temp_mdd = df_asum2.loc[temp_i,'unit']/df_asum2.loc[:index_pre,'unit'].max()-1
                df_asum2.loc[temp_i,'mdd_mon'] = min(df_asum2.loc[index_pre,'mdd_mon'] , temp_mdd )
                df_asum2.loc[temp_i,'ret_mon'] = df_asum2.loc[temp_i,'unit']/df_asum2.loc[index_pre,'unit']-1
                i = i+1
            ###
            index_pre = temp_i 

        return df_asum2

    def trade_tp_monthly_sum(self,path0 ,file_name) :
        # for trade plan 
        # path0  = path_base + port_name +'\\accounts\\'
        # file_name = trades_id_1543136329_port_rc181123_w_allo_value_45_TP_20181105.csv
        # code  date_plan   date_trade_1st  method  period  quote_index_start   quote_index_end signal_pure weight_dif  total_amount    num ave_price

        df_TP = pd.read_csv(path0 + file_name)
        

        # ind1="15", 有145个"weight_dif"是空值，有122个"signal_pure"是空值，需要去掉
        # print( df_TP["weight_dif"].isnull().value_counts() )
        # print( df_TP["signal_pure"].isnull().value_counts() )
      # axis：0-行操作（默认），1-列操作 
      # how：any-只要有空值就删除（默认），all-全部为空值才删除 
      # inplace：False-返回新的数据集（默认），True-在愿数据集上操作
        # df_TP = df_TP.dropna(axis=0, how='any', inplace=False)

        # notes: "weight_dif" 有正有负，"weight_dif2"都是正数
        df_TP["weight_dif2"] =df_TP["weight_dif"]*df_TP["signal_pure"]
        
        # print(df_TP.describe() )
        # "signal_pure" "weight_dif" 
        ##############################################################################
        ### 设想，table columns=[ ind1, style, date_list{ "2014-05-31",...} ]
        ### 1，新建2列，分别是买入金额和卖出金额
        ### 2，"date_trade_1st" 变成 datetime, 然后按月份汇总，得到每年5,11月的交易数据
        ### 3，存到df，index是总的B，S weight，columns是组合名称
        df_TP["weight_dif_add"] =  df_TP["weight_dif"].apply(lambda x: max(0.0, x) )
        df_TP["weight_dif_minus"] =df_TP["weight_dif"].apply(lambda x: min(0.0, x) )
        df_TP["date"]= pd.to_datetime( df_TP["date_trade_1st"],format="%Y-%m-%d" )
        
        ### df summarize by month or year 
        ### method 1 using datetime as index and then df["2014-06"]
        # df_TP = df_TP.set_index('date')
        # print( df_TP["2014-06"].describe() )
        # print( df_TP["2014-06"].head() )

        ### show by month 
        df_TP = df_TP.set_index('date')
        # print("df_summary__________")
        
        df_summ = df_TP.resample("M").sum()
        df_summ = df_summ[ df_summ["total_amount"]>1 ]
        # print( df_summ )

        return df_summ



    def trade_tb_stat(self,path0 ,file_name) :
        df_TB = pd.read_csv(path0 + file_name)

        ### 时间角度： 买卖笔数统计，平均成交金额，平均实现收益，交易费用。
        # df_tb_time 
        df_TB["date2"] = pd.to_datetime( df_TB["date"] ) 
        df_TB = df_TB.set_index('date2')
        # 
        # print("df_TB")
        # print(df_TB.info() )
        amt_sum = df_TB["amount"].sum() 
        if amt_sum > 0 :
            df_TB["amt_pct"] = df_TB["amount"]/amt_sum
        else :
            df_TB["amt_pct"] = df_TB["amount"]*-1/amt_sum
        df_TB["amt_buy"] = df_TB["amount"]* df_TB["BSH"].apply(lambda x : max(x,0.0)  )
        df_TB["amt_sell"] = df_TB["amount"]* df_TB["BSH"].apply(lambda x : min(x,0.0)*-1  )
        df_TB["num_buy"] =  df_TB["BSH"].apply(lambda x : max(x,0.0)  )
        df_TB["num_sell"] = df_TB["BSH"].apply(lambda x : min(x,0.0)*-1  )
        # 有成交的每个月的 买入笔数，卖出笔数，买入总金额，卖出总金额，总盈亏，总费用
        #   买入平均金额，卖出平均金额，平均盈亏，平均费用
        df_mean = df_TB.resample("M").mean()
        df_mean =df_mean[df_mean["amount"]>0  ]
        df_sum = df_TB.resample("M").sum()
        df_sum =df_sum[df_sum["amount"]>0  ]
        # print("df_mean________________________")
        # # df_mean.describe().loc["count","amt_sell"]
        # print( df_mean.info()  )
        # print("df_sum________________________")
        # print( df_mean.sum()  )

        df_stat = df_sum
        df_stat["pct_fees_profit"] = df_stat["fees"]/df_stat["profit_real"] 
        df_stat["ave_amt_buy"] = df_mean["amt_buy"]
        df_stat["ave_amt_sell"] = df_mean["amt_sell"]
        df_stat["ave_profit"] = df_mean["profit_real"]
        df_stat["ave_fees"] = df_mean["fees"]

        ### 股票角度，总交易金额占比，总利润占比
        # df_tb_stock
        # 对于每个股票，统计：有交易的月份，每个月交易金额，总盈亏，总费用，取top5~20
        # vipvip 我们想要观察anchor stock的表现，多大程度上能代表主动基准组合的收益和风险
        df_stat_s = df_TB.groupby("symbol").sum()
        # 47 stocks 
        df_stat_s["amt_pct"] =df_stat_s["amount"]/df_stat_s["amount"].sum()
        profit_sum = df_stat_s["profit_real"].sum()
        if profit_sum >0 :
            df_stat_s["profit_pct"] =df_stat_s["profit_real"]/profit_sum
        else :
            df_stat_s["profit_pct"] =df_stat_s["profit_real"]*-1/profit_sum
        df_stat_s = df_stat_s.sort_values(["amt_pct"] ,ascending= False)

        ### get CN name of securities
        # dict_ind = pd.read_csv(path_ind + file_ind_CN,encoding="gbk")
        print( "columns of df_stat_s ", df_stat_s.head() )
        for temp_i in df_stat_s.index :
            temp_code = temp_i
            print( "temp code ",temp_code )
            # get CN name
            temp_df = self.dict_ind[ self.dict_ind["symbol"]==temp_code ]
            df_stat_s.loc[temp_i,"name"] = temp_df["name" ].iloc[0] 

        return df_stat,df_stat_s

    def signals_info(self,df_sig):
        ### Calculate current signals
        # last 190111 | since 190111

        ### import industry name 
        # wind_code sec_name    ind4_index_code ind3_index_code ind2_index_code ind1_index_code ind4_code   ind3_code   ind2_code   ind1_code

        # path_ind = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\static\\"
        # file_ind_CN = "ind_wind_CN.csv"
        # dict_ind = pd.read_csv(path_ind + file_ind_CN,encoding="gbk")

        # symbol    name    ind_4_code  ind_3_code  ind_2_code  ind_1_code  ind_1_name  ind_2_name  ind_3_name  ind_4_name
        # col_dict_ind || ['symbol', 'name', 'ind_4_code', 'ind_3_code', 'ind_2_code',
        #   'ind_1_code', 'ind_1_name', 'ind_2_name', 'ind_3_name', 'ind_4_name']
        # col_df_sig = ["code","ind1_code","ind2_code","ind3_code","w_allo_value_ind1","w_allo_growth_ind1","signal_pure"]
        
        for temp_i in df_sig.index :
            temp_code = df_sig.loc[temp_i,"code"]
            temp_signal = df_sig.loc[temp_i,"signal_pure"]
            # get CN name
            temp_df = self.dict_ind[ self.dict_ind["symbol"]==temp_code ]
            df_sig.loc[temp_i,"name"] = temp_df[ "name" ].iloc[0] 
            # get CN signal
            temp_df = self.dict_ind[ self.dict_ind["symbol"]==temp_code ]
            if temp_signal  == 1 :
                 df_sig.loc[temp_i,"signal_CN"] = "买入"
                 df_sig.loc[temp_i,"signal_EN"] = "Buy"
            elif temp_signal  == -1 :
                 df_sig.loc[temp_i,"signal_CN"] = "卖出"
                 df_sig.loc[temp_i,"signal_EN"] = "Sell"
            else :
                 df_sig.loc[temp_i,"signal_CN"] = ""
                 df_sig.loc[temp_i,"signal_EN"] = ""
            
            for temp_ind in ['ind_1_name', 'ind_2_name', 'ind_3_name', 'ind_4_name'] :
                temp_df = self.dict_ind[ self.dict_ind["symbol"]==temp_code ]
                # >>> rr2.ind_2_name.iloc[0] | '银行' <class 'str'>
                df_sig.loc[temp_i,temp_ind] = temp_df[temp_ind ].iloc[0] 



        return df_sig

    def stra_info_ind(self,df_stra):
        ### Get CN names for given indstry 
        # last 190118 | since 190118

        ### import industry name 
        # wind_code sec_name    ind4_index_code ind3_index_code ind2_index_code ind1_index_code ind4_code   ind3_code   ind2_code   ind1_code

        # path_ind = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\static\\"
        # file_ind_CN = "ind_wind_CN.csv"
        # dict_ind = pd.read_csv(path_ind + file_ind_CN,encoding="gbk")

        for temp_i in df_stra.index :
            # name for code
            temp_code = df_stra.loc[temp_i,"code"]
            temp_df = self.dict_ind[ self.dict_ind["symbol"]==temp_code ]
            df_stra.loc[temp_i,"name"] = temp_df[ "name" ].iloc[0] 
            for temp_ind in ['ind_1_name', 'ind_2_name', 'ind_3_name', 'ind_4_name'] :
                temp_df = self.dict_ind[ self.dict_ind["symbol"]==temp_code ]
                # >>> rr2.ind_2_name.iloc[0] | '银行' <class 'str'>
                df_stra.loc[temp_i,temp_ind] = temp_df[temp_ind ].iloc[0] 

            # name for anchor code
            if "code_anchor_value" in df_stra.columns :
                temp_code_anchor = df_stra.loc[temp_i,"code_anchor_value"]
            elif  "code_anchor_growth" in df_stra.columns :
                temp_code_anchor = df_stra.loc[temp_i,"code_anchor_growth"]
            else :
                temp_code_anchor = temp_code
            temp_df = self.dict_ind[ self.dict_ind["symbol"]== temp_code_anchor ]
            df_stra.loc[temp_i,"name_anchor"] = temp_df[ "name" ].iloc[0] 
            
            
            


            



        return df_stra


























