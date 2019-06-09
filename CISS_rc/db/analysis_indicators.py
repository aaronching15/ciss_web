# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
Function:
serve as estimated input for strategy calculation 
last 181109 | since 181109
功能： 

Menu :
分析：
0，main function
    

1，输入:
   
2，配置文件 |   

THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }


Notes: 
refernce: rC_Stra_MAX.py 
===============================================
'''
import pandas as pd 
import sys 
sys.path.append("..")

class indicators():
    def __init__(self, indicator_name ):
        self.indicator_name = indicator_name



#########################################################
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



#########################################################
class indicator_fundamental():
    def __init__(self, indicator_name ):
        self.indicator_name = indicator_name







 















