# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
程序说明部分

161228_0916
发现一个现有 问题，002510.SZ 在



Stra_MA16x_5D(self, Ana_Data)
# get latest 5 BSH_signals from Ana_Data

Stra_Low2_Signals_vol(self, Ana_Data, data):
        # 160819 Aeon_rC
        # 选股策略，选出：
        # 过去6个月内，从高点开始，先出现一轮较大的下跌，盘整后，再出现第二轮较大的下跌后不再创新低时，买入

===============================================
'''
# 20151213 by
# Help tushare: http://tushare.org/trading.html#id2
# Input : open high low close volume ; MA(3,8,16,40,100); P>MA_X;MA_X_Up ; MA_X_UpDays ;
# parameters: threshold for MA-X ;
# Buy/Sell/Hold signals, status :
# Number of stocks can hold, Cash ,Stock, Account, Unit price, Max_U MDD=maximum draw down, Price, MDD_Price
# Performance: Sharp, sortino, chg_Unit, chg_Price, chg_UnitNegative, ...
import timeit
import pandas as pd
import os
# file_path = os.getcwd()
# file_path0='C:\\rC_Py3_Output\\'
import numpy as np
import time

# t0=timeit.timeit(里边放程序)
# print(t0)
import math

class rC_TradeSys() :
    # 类的初始化操作
    def __init__(self, code ,start ,end,file_path0  ):
        self.code=code
        self.start=start
        self.end=end
        self.file_path0 = file_path0
        # Preparation : Get current path

        ''' TODO： Build a class for account: Cash, Stocks, Futures,Options , etc.  '''

    ''' Part 1 Get Price data '''
    def ImportFiData_csv_WindPy(self,code,file_path0):

        # to tell isnan
        # Read csv daily price data from csv, which already downloaded from Wind by Matlab
        # C:\rC_Matlab_Output
        '''注意：这里文件地址命名需要用 \\, 全部\\可以识别，因为单个string里的\ 会被补成\\  '''
        # C:\rC_Py3_Output
        # file_path0 = 'D:\\rC_Py3_Output'
        file_path=file_path0+ '\\' + 'Wind_' + code + '_updated.csv'
        # skiprows=1 不读取第一行
        data = pd.read_csv( file_path, header=None,skiprows=1, sep=',')
        if len( data.columns ) == 9 :
            data = data.drop([0], axis=1 )
        # len= 8 is fine.
        # Lets change the name of the column
        data.columns = ['date' ,'open' ,'high', 'low' ,'close', 'volume', 'amt' ,'pct_chg']
        # data['close'][800:].plot()
        # ['date' ,'open' ,'high' ,'close', 'low', 'volume']
        # print( 'data.shape is :', data.shape)

        data2=data.dropna( axis=0)
        [r,c]=data2.shape
        # 160512          print( data2['date'][0:3])
        # 160822 下边这个可以打印日期：
        # print( data2['date'].head(1).values + '   ,   ' + data2['date'].tail(1).values)

        return data2

    def ImportFiData_csv_WindPy_min(self, code='600030.SH', file_path0= 'F:\\rC_Py3_Output_min' ):

        # C:\rC_Py3_Output

        file_path = file_path0 + '\Wind_' + code + '.csv'
        # skiprows=1 不读取第一行
        data = pd.read_csv(file_path, header=None, skiprows=1, sep=',')
        # Lets change the name of the column
        data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        # data['close'][800:].plot()
        # ['date' ,'open' ,'high' ,'close', 'low', 'volume']
        print('data.shape is :', data.shape)

        data2 = data.dropna(axis=0)
        [r, c] = data2.shape

        return data2

    def ImportFiData_csv_WindPy_US(self, code='AAPL.O'):
        # 160430 注意，对于美国股票，'amt' 这个返回的都是nan 数据
        # to tell isnan
        # Read csv daily price data from csv, which already downloaded from Wind by Matlab
        # C:\rC_Matlab_Output
        '''注意：这里文件地址命名需要用 \\, 全部\\可以识别，因为单个string里的\ 会被补成\\  '''
        # C:\rC_Py3_Output
        file_path0 = 'D:\\data_Input_Wind'
        file_path = file_path0 + '\Wind_' + code + '.csv'
        # skiprows=1 不读取第一行
        data = pd.read_csv(file_path, header=None, skiprows=1, sep=',')
        # Lets change the name of the column
        data.columns = ['date', 'open', 'high', 'low', 'close', 'volume','amt', 'pct_chg']
        # data['close'][800:].plot()
        # ['date' ,'open' ,'high' ,'close', 'low', 'volume']
        print('data.shape is :', data.shape)
        # Change nan value to 0
        data['amt']=0
        data2 = data.dropna(axis=0)
        [r, c] = data2.shape

        return data2


        '''
        0      1      2      3      4          5
        0     36724    NaN    NaN    NaN    NaN        NaN
        1     36725    NaN    NaN    NaN    NaN        NaN
        2     36726    NaN    NaN    NaN    NaN        NaN
        3     36727    NaN    NaN    NaN    NaN        NaN

        3752  42380  16.30  16.48  15.51  15.68  243460000
        3753  42381  15.92  16.34  15.70  15.89  188830000
        [3754 rows x 6 columns]
        '''

    # def ImportFinancialData(code='600030',start='2014-01-01',end='2015-12-14') : # , time1, time2, items, para_ma
    def ImportFinancialData_tushare(self,code='600030', start='2014-01-01', end='2015-12-14') : # , time1, time2, items, para_ma
        # Task：抓取股票数据并输出
        # We use this function to get stock/index/future/ETF data
        import tushare as ts
            # date:date open   high  close    low   volume      amount ...
        ''' Notice that we just temporarily declare data as Global var.  '''
        global data
        data=ts.get_h_data(code,start,end) # <class 'pandas.core.frame.DataFrame'> (code,start,end)
        # print( data.describe()) # shows obs,mean, std, high ,low ,...
        # print( type(data) )

        # print( data['2015-12-10'] ) # data[:3]  first 3 lines ; # data['2015-12-10']  get result from specific day
        # print( data.iloc[0:5,0:4] ) #  0:5 选取first 5 rows and 0:4,means first 4 columns/Index
        # print( np.mean(data.iloc[0:5,0:4]) )

        # del df[0] : delete a line ; df.drop(0): delete a column
        # Output to csv : dataframe可以使用to_csv方法方便地导出到csv文件中，如果数据中含有中文，一般encoding指定为”utf-8″,否则导出时程序会因为不能识别相应的字符串而抛出异常，index指定为False表示不用导出dataframe的index数据。

        # file_path=file_path0+'Output.csv'
        # print( 'Data has been saved to:  ' +file_path )
        # data.to_csv(  file_path  , encoding='utf-8', index=False)
        return data

        # print( data.head() )
        # print( data.describe()  )
        # 数据类型： data.dtypes ； 查看数据框的索引 data.index
        # print(data.index)
        # print(data.columns)
        # DatetimeIndex(['2015-12-14', '2015-12-11', ...
        # Index(['open', 'high', 'close', 'low', 'volume', 'amount'], dtype='object')
        # 查看数据值   print(data.values)
        # 行列转换 data.T
        # 指定根据哪一列数据进行排序

    # 510300 return "None" ????
    # ts.get_h_data(code), code=600030 only return past 1 year price

    ''' Part 2 Calculate analytic values '''
    ''' tushare
    date      open   high  close    low   volume      amount ...
    2015-12-08  17.81  18.11  17.64  17.63
    2015-12-07  18.01  18.11  17.90  17.75
    2015-12-04  18.52  18.62  18.24  18.16
    2015-12-03  18.70  19.14  18.74  18.47
    2015-12-02  17.74  19.15  18.82  17.65
    2015-12-01  17.63  18.10  17.79  17.40
    '''
    def AnalyticFiData(self,data0, MA_x=[3,8,16,40,100], P_MA=[0,0,0,0,0], MA_up=[1,1,1,1,1]  ) :
        #　get shape of data
        data=data0.values
        [x,y]=data0.shape # x is # of days and y=6
        # The first row of WindPy data is string ,so we need x=x-1
        x=x-1

        '''After data.dropna, data2的index代号还是没有变，原来从0开始，现在变成从594开始  '''
        '''Here, x=3160, y=6 but index of data started from 594   '''
        # print('x and y are ') # e.g. x=520, y=6

        # Short term MA-x
        # Creat a Matrix storing analytic results :
        # index means row in Python
        Columns=['MA3','MA8','MA16','MA40','MA100','P/MA3','P/MA8','P/MA16','P/MA40','P/MA100','MA3_up','MA8_up','MA16_up','MA40_up','MA100_up','MA2','MA7','MA15','MA39','MA99']
        '''
        # Items to be added to Columns: MA3_upDays ```, MA3_8,MA8_16 ```, MA3_High40Days Close_High40Days MA3_High16Days

        '''
        # print( Columns )
        # 这里的 index 表示的是row的代号， 但是我们其实是想定义column的代号
        # Ana_Data= pd.DataFrame( np.zeros([x,len(MA_x)+len(P_MA)+len(MA_up) ]),columns=Columns )  # 520*15
        len0=len(MA_x)
        temp=np.zeros([x,len0*4 ])

        # close=data['close']  # When data is dataframe type
        close=data[1:,4 ] # get the 5-th column of data[:,4]
        # Under WindPy. we change string to float
        temp_c= []
        for i in range(len(close)) :
            temp_c=temp_c+[float(close[i]) ]

        close=temp_c

        temp1=np.max(MA_x)
        for i in range(temp1-1, x ):
            # Wind: Started from the 2nd row
            ''' Calculate MA price '''
            # Moving Average and Returns
            # close_px = df['Adj Close']
            #　mavg = pd.rolling_mean(close_px, 40)
            ''' Quick  method by write to every row of Ana_Data '''
            # print( type(MA_x[0]) )
            # print( close[0] )
            # Notice that under WindPy file, close[0]='CLOSE'

            for j in range( len0 ) :
                # print( close[i-MA_x[j]-1:i ] )
                # print( i-MA_x[j]-1)
                # print( i )
                # temp[i,j] = np.mean(close.iloc[i-MA_x[j]-1:i ] )
                ''' Calculate MA(x) price '''
                temp[i,j] = np.mean(close[i-MA_x[j]-1:i ] )
                ''' Calculate P/MA-1 price '''

                temp[i,j+len0] = close[i]/temp[i,j ] -1
                ''' Calculate MA_Up price '''
                # todo 161231 这里的代码对应了input的数据必须超过 MA_max+10的长度才行，之前是MA_max+3,就导致MA_up都是0
                if i>np.max(MA_x)+1  :
                    temp[i,j+len0*2] = temp[i,j]/temp[i-1,j]-1
                ''' Calculate MA(x-1) price '''
                temp[i,j+len0*3 ] = np.mean(close[i-MA_x[j]-1+1:i ] )

        Ana_Data=pd.DataFrame(temp,columns=Columns)
            # ''' Slow method by write to every row of Ana_Data '''
            # for j in range(len(MA_x)) :
            #     Ana_Data.iloc[i,j] = np.mean(close.iloc[i-MA_x[j]-1:i ] )
            # ''' Calculate P/MA-1 price '''
            # for j in range(len(MA_x),len(MA_x)+len(P_MA) ) :
            #     Ana_Data.iloc[i,j] = close.iloc[i]/Ana_Data.iloc[i,j-len(MA_x)] -1
            # ''' Calculate change of MA price '''
            # for j in range(len(MA_x)+len(P_MA),len(MA_x)+len(P_MA)+len(MA_up)) :
            #     ''' Here we face the problem of i have to be >= 1  '''
            #     if i>np.max(MA_x)+1  :
            #         Ana_Data.iloc[i,j] = Ana_Data.iloc[i,j-len(MA_x)-len(P_MA)]/Ana_Data.iloc[i-1,j-len(MA_x)-len(P_MA)]-1
            #     # if i==499 :
            #     #     print('========'  )
            #     #     print( Ana_Data.iloc[i  ,j-len(MA_x)-len(P_MA)] )
            #     #     print( Ana_Data.iloc[i-1,j-len(MA_x)-len(P_MA)] )
            #     #     print('========'  )

        # file_path=file_path0+'Output2.csv'
        # Ana_Data.to_csv(  file_path  , encoding='utf-8', index=False)
        # print( 'Data has been saved to:  ' +file_path )

        return Ana_Data

    def AnalyticFiData_2 (self,data0, MA_x=[3,8,16,40,100], P_MA=[0,0,0,0,0], MA_up=[1,1,1,1,1]  ) :
        '''
        # todo last update 170403 | since 160314
        # todo 以前的设计是让3~100天的数据都可以分析，但是目前来看，可能40天左右的数据已经足够需要。
        # From temp1=np.max(MA_x) to temp1= MA_x[-2]
        update
        171130 发现对于 1：N 日的数据，之前只能计算到 N-1日的ma_x
        '''

        # 160314 We want to add the highest and lowest price for past x days
        #　get shape of data
        data=data0.values
        [x,y]=data0.shape # x is # of days and y=6
        # The first row of WindPy data is string ,so we need x=x-1
        x=x-1
        '''After data.dropna, data2的index代号还是没有变，原来从0开始，现在变成从594开始  '''
        '''Here, x=3160, y=6 but index of data started from 594   '''
        # print('x and y are ') # e.g. x=520, y=6

        # Short term MA-x
        # Creat a Matrix storing analytic results :
        # index means row in Python
        Columns=['MA3','MA8','MA16','MA40','MA100','P/MA3','P/MA8','P/MA16','P/MA40','P/MA100','MA3_up','MA8_up','MA16_up',
                 'MA40_up','MA100_up','MA2','MA7','MA15','MA39','MA99',
                 'P/H3','P/H8','P/H16','P/H40','P/H100','P/L3','P/L8','P/L16','P/L40','P/L100']
        '''
        # Items to be added to Columns: MA3_upDays ```, MA3_8,MA8_16 ```, MA3_High40Days Close_High40Days MA3_High16Days

        '''
        # print( Columns )
        # 这里的 index 表示的是row的代号， 但是我们其实是想定义column的代号
        # Ana_Data= pd.DataFrame( np.zeros([x,len(MA_x)+len(P_MA)+len(MA_up) ]),columns=Columns )  # 520*15
        len0=len(MA_x)
        # print(x)
        temp=np.zeros([x,len0*6 ])

        # close=data['close']  # When data is dataframe type
        close=data[1:,4 ] # get the 5-th column of data[:,4]
        # todo x =len(close)

        high=data[1:,2 ] # get the 3-th column of data[:,4]
        low=data[1:,3 ] # get the 4-th column of data[:,4]
        # print('171130  ==close -5 ', close[-5:] )
        # Under WindPy. we change string to float
        temp_c= []
        temp_h= []
        temp_l= []
        for i in range(len(close)) :
            temp_c=temp_c+[float(close[i]) ]
            temp_h=temp_h+[float(high[i]) ]
            temp_l=temp_l+[float(low[i]) ]

        close=temp_c
        high=temp_h
        low=temp_l
        # todo 170403
        # temp1=np.max(MA_x)
        temp1 = MA_x[-2]
        for i in range(temp1-1, x ):
            # todo 171130: i_max = x-1, which is the last index of close
            # Wind: Started from the 2nd row
            ''' Calculate MA price '''
            # Moving Average and Returns
            # close_px = df['Adj Close']
            #　mavg = pd.rolling_mean(close_px, 40)
            ''' Quick  method by write to every row of Ana_Data '''
            # print( type(MA_x[0]) )
            # print( close[0] )
            # Notice that under WindPy file, close[0]='CLOSE'

            for j in range( len0 ) :
                # print( close[i-MA_x[j]-1:i ] )
                # print( i-MA_x[j]-1)
                # print( i )
                # temp[i,j] = np.mean(close.iloc[i-MA_x[j]-1:i ] )
                # todo  比如 i=i_max=x-1=3136, MA_x[j]=16时， 我们需要 close[i- 16:i ],这样会有16个值
                ''' Calculate MA(x) price '''
                # before 171130 :temp[i,j] = np.mean(close[i-MA_x[j]-1:i ] )
                temp[i,j] = np.mean(close[i-MA_x[j]:i ] )
                ''' Calculate P/MA-1 price '''

                temp[i,j+len0] = close[i]/temp[i,j ] -1
                ''' Calculate MA_Up price '''
                if i>np.max(MA_x)+1  :
                    temp[i,j+len0*2] = temp[i,j]/temp[i-1,j]-1
                ''' Calculate MA(x-1) price '''
                temp[i,j+len0*3 ] = np.mean(close[i-MA_x[j]-1+1:i ] )
                ''' Calculate P/High(x) price '''
                if i>np.max(MA_x)+1  :
                    # before 171130 这样会多出来一个 |  close[i]/np.max( high[i-MA_x[j]-1:i ]) -1
                    temp[i,j+len0*4 ] =close[i]/np.max( high[i-MA_x[j]:i ]) -1
                ''' Calculate P/Low(x) price '''
                if i>np.max(MA_x)+1  :
                    # before 171130 这样会多出来一个 | close[i]/np.min( low[i-MA_x[j]-1:i ]) -1
                    temp[i,j+len0*5 ] =close[i]/np.min( low[i-MA_x[j]:i ]) -1
            # if i == x:
            #     print('171130 2008')
            #     print( close[i-MA_x[j]-1:i ]  )
            #     print( close[i-MA_x[j]-1 ] , close[i-MA_x[j] ], close[ i ] )
            #     asd

        Ana_Data=pd.DataFrame(temp,columns=Columns)
            # ''' Slow method by write to every row of Ana_Data '''
            # for j in range(len(MA_x)) :
            #     Ana_Data.iloc[i,j] = np.mean(close.iloc[i-MA_x[j]-1:i ] )
            # ''' Calculate P/MA-1 price '''
            # for j in range(len(MA_x),len(MA_x)+len(P_MA) ) :
            #     Ana_Data.iloc[i,j] = close.iloc[i]/Ana_Data.iloc[i,j-len(MA_x)] -1
            # ''' Calculate change of MA price '''
            # for j in range(len(MA_x)+len(P_MA),len(MA_x)+len(P_MA)+len(MA_up)) :
            #     ''' Here we face the problem of i have to be >= 1  '''
            #     if i>np.max(MA_x)+1  :
            #         Ana_Data.iloc[i,j] = Ana_Data.iloc[i,j-len(MA_x)-len(P_MA)]/Ana_Data.iloc[i-1,j-len(MA_x)-len(P_MA)]-1
            #     # if i==499 :
            #     #     print('========'  )
            #     #     print( Ana_Data.iloc[i  ,j-len(MA_x)-len(P_MA)] )
            #     #     print( Ana_Data.iloc[i-1,j-len(MA_x)-len(P_MA)] )
            #     #     print('========'  )

        # file_path=file_path0+'Output2.csv'
        # Ana_Data.to_csv(  file_path  , encoding='utf-8', index=False)
        # print( 'Data has been saved to:  ' +file_path )

        return Ana_Data

    ''' Calculate the strategy signals/states   '''
    def Stra_MA_Signals(self, Ana_Data) :

        # Condition1: P>MA40D and MA8D is Up.
        # ？用哪个信号呢？要不先从P>MA40开始吧。
        [x,y]=Ana_Data.shape # x is # of days and y=6
        ''' 注意：这里的Signals每一列代表一个策略，如果是多列的话我们可以做成 '''
        Signals_temp= np.zeros([x,1 ])    # 520*15
        #Signals= pd.DataFrame( np.zeros([x,1 ])  )  # 520*15
        print('=======Stra_MA=======')
        #　Columns=['MA3','MA8','MA16','MA40','MA100','P/MA3','P/MA8','P/MA16','P/MA40','P/MA100','MA3_up','MA8_up','MA16_up','MA40_up','MA100_up']

        P_MA40=Ana_Data['P/MA40']
        P_MA16=Ana_Data['P/MA16']
        MA8_up=Ana_Data['MA8_up']
        MA16_up=Ana_Data['MA16_up']
        MA40_up=Ana_Data['MA40_up']
        MA100_up=Ana_Data['MA100_up']
        for i in range(100+1, x )  :
            # and Ana_Data.iloc[i,11] >=0  and Ana_Data.iloc[i,8]>0  : # 11th item means MA8_Up and 8th means P/MA40d
            # if P_MA16.iloc[i] >0 and P_MA40.iloc[i] >0 and MA16_up.iloc[i]>0 and MA40_up.iloc[i]>0 :
            #160125: if  MA8_up.iloc[i]>=0 and MA8_up.iloc[i-1]>=-0.005 and P_MA40.iloc[i] >=-0.005 and MA40_up.iloc[i]>=-0.01 :
            #    if  MA8_up.iloc[i]>=0 and MA8_up.iloc[i-1]>=-0.005 and P_MA16.iloc[i] >=-0.005 and MA16_up.iloc[i]>=-0.01 and MA40_up.iloc[i] >=-0.005 and P_MA40.iloc[i] >=-0.01:
            # Fixed Items： Means these items can provide solid effect either: shift return or lower MDD, avoid frequent trading
            # before 160428 : if MA40_up.iloc[i-1]>=0 and P_MA40.iloc[i]>=-0.01 : #  MA100_up.iloc[i-1]>=-0.001 and
                # if MA16_up.iloc[i]>=0  and MA16_up.iloc[i-1]>=-0.005  :
            if P_MA40[i]>=-0.005 and  MA16_up[i]>=0  and  MA40_up[i]>=0 : #  MA100_up.iloc[i-1]>=-0.001 and
                Signals_temp[i,0]=1

            # if i<x-20 and Signals.iloc[i+1,0]==0 :
            #     if Ana_Data.iloc[i,8]>0 and
                # todo  P>40 and MA8D,MA16D both up and P/max(MA8,MA16,MA40)<1.05 and MA40D<1.05*max(MA40D(last 40days) ) 160113

        Signals= pd.DataFrame( Signals_temp )
        # file_path=file_path0+'Output_Signals.csv'
        # Signals.to_csv(  file_path  , encoding='utf-8', index=False)
        # print( 'Data has been saved to:  ' +file_path )
        return Signals

    def Stra_MA_Signals_a(self, Ana_Data) :
        # 160428
        [x,y]=Ana_Data.shape # x is # of days and y=6
        Signals_temp= np.zeros([x,1 ])    # 520*15
        P_MA40=Ana_Data['P/MA40']
        P_MA16=Ana_Data['P/MA16']
        MA8_up=Ana_Data['MA8_up']
        MA16_up=Ana_Data['MA16_up']
        MA40_up=Ana_Data['MA40_up']
        MA100_up=Ana_Data['MA100_up']
        for i in range(100+1, x )  :
            # and  MA40_up[i]>=0 : #  MA100_up.iloc[i-1]>=-0.001 and
            # P_MA40[i]>=-0.01 and  MA16_up[i]>=0 :
            if P_MA40[i]>=-0.01 and  MA16_up[i]>=0 :
                Signals_temp[i,0]=1

        Signals= pd.DataFrame( Signals_temp )
        return Signals

    def Stra_MA_Signals_a_vol(self, Ana_Data, data ):
        # todo [i+1] 说明主要用于回测，因为有未来函数
        # todo amt[i+1] ==0 161228 有可能持有期停牌，或其他无成交量的情况
        # 161006 Stra_MA_Signals_a_vol calculate the B/S/H signal after current trading day closed, any
            # transactions will be excuted at next trading days
        # 160507 观察剔除成交金额为0和开盘涨停后的收益
        # derived from Stra_MA_Signals_a , which is derived from Stra_MA_Signals
        # data ：DATE	OPEN	HIGH	LOW	CLOSE	VOLUME	AMT	PCT_CHG
        # type of data is pandas , need to be transferred to
        amt=data['amt'].values
        close=data['close'].values
        open=data['open'].values
        [x, y] = Ana_Data.shape  # x is # of days and y=6
        Signals_temp = np.zeros([x, 1])  # 520*15
        P_MA40 = Ana_Data['P/MA40']
        P_MA16 = Ana_Data['P/MA16']
        MA8_up = Ana_Data['MA8_up']
        MA16_up = Ana_Data['MA16_up']
        MA40_up = Ana_Data['MA40_up']
        MA100_up = Ana_Data['MA100_up']
        # 161006, we change for i in range(100+1 , x-1 ): to for i in range(100 , x-1 ):
        for i in range(100 , x ):
            # Case 1: Signal: 0 to 1
            if Signals_temp[i-1, 0] == 0 :
                #　Buy signal:
                if P_MA40[i] >= -0.01 and MA16_up[i] >= 0:
                    # We can buy tomorrow morning
                    #
                    if amt[i + 1] > 5000000 and open[i + 1] <= close[i] * 1.0975:
                        Signals_temp[i, 0] = 1
                    # We cannot buy tomorrow morning
                    else:
                        Signals_temp[i, 0] = 0

            # Case 2: Signal: 1 to 1
            if Signals_temp[i - 1, 0] == 1:
                # 　Hold signal:
                if P_MA40[i] >= -0.01 and MA16_up[i] >= 0:
                    # print('171130=== ', P_MA40[i], MA16_up[i])
                    Signals_temp[i, 0] = 1
                elif amt[i + 1] ==0  :
                    Signals_temp[i, 0] = 1
                # Case 3: Signal: 1 to 0
                else :
                    # We can sell tomorrow morning
                    if amt[i + 1] >= 5000000 and open[i + 1] >= close[i] * 0.9025:
                        Signals_temp[i, 0] = 0
                    # We cannot buy tomorrow morning
                    else :
                        Signals_temp[i, 0] = 1

        Signals = pd.DataFrame(Signals_temp)
        return Signals


    def Stra_MA_Signals_a_vol_4Port(self, Ana_Data, data ):
        # todo last modify 171216
        # todo Last modify 161225_1507 add condition ( MA16_up[i] >= 0 and MA40_up >=0 ) from MA16_up[i] >= 0
        # todo Last check 161225_1322 : 感觉有问题，test_Singal 用这个是没有超额收益的。
        # todo 和 Stra_MA_Signals_a_vol 的区别是，我们这里不需要下一个交易日的价量数据，就可以得到信号
        # 161006 Stra_MA_Signals_a_vol calculate the B/S/H signal after current trading day closed, any
            # transactions will be excuted at next trading days
        # 160507 观察剔除成交金额为0和开盘涨停后的收益
        # derived from Stra_MA_Signals_a , which is derived from Stra_MA_Signals
        # data ：DATE	OPEN	HIGH	LOW	CLOSE	VOLUME	AMT	PCT_CHG
        # type of data is pandas , need to be transferred to
        # amt=data['amt'].values
        # close=data['close'].values
        # open=data['open'].values
        [x, y] = Ana_Data.shape  # x is # of days and y=6
        Signals_temp = np.zeros([x, 1])  # 520*15
        P_MA40 = Ana_Data['P/MA40']
        # P_MA16 = Ana_Data['P/MA16']
        # MA8_up = Ana_Data['MA8_up']
        MA16_up = Ana_Data['MA16_up']
        MA40_up = Ana_Data['MA40_up']
        # MA100_up = Ana_Data['MA100_up']
        # 161006, we change for i in range(100+1 , x-1 ): to for i in range(100 , x-1 ):
        # 161006, we change for i in range(100   , x-1 ): to for i in range(100 , x ):
        for i in range(40, x):
        # for i in range(100 , x ):
            # Case 1: Signal: 0 to 1
            if Signals_temp[i-1, 0] == 0 :
                #　Buy signal:
                # todo Last modify 161225 add condition ( MA16_up[i] >= 0 and MA40_up >=0 ) from MA16_up[i] >= 0
                # if P_MA40[i] >= -0.01 and ( MA16_up[i] >= 0 and ( MA40_up[i] >=0 and MA100_up[i] >=0 ) ):
                if P_MA40[i] > -0.01 and MA16_up[i] > 0  :
                    # We can buy tomorrow morning
                    # if amt[i + 1] > 2000000 and open[i + 1] <= close[i] * 1.0975:
                    Signals_temp[i, 0] = 1

                    # We cannot buy tomorrow morning
                else:
                    Signals_temp[i, 0] = 0

            # Case 2: Signal: 1 to 1
            if Signals_temp[i - 1, 0] == 1:
                # 　Hold signal:
                if P_MA40[i] >= -0.01 and MA16_up[i] >=0  :
                    # print( '171130=== ', P_MA40[i] , MA16_up[i])
                    Signals_temp[i, 0] = 1
                # elif amt[i+1] ==0  :
                #     # todo amt[i+1] ==0 161228 有可能持有期停牌，或其他无成交量的情况
                #     Signals_temp[i, 0] = 1
                # Case 3: Signal: 1 to 0
                else :
                    # We can sell tomorrow morning
                    # if amt[i + 1] > 2000000 and open[i + 1] >= close[i] * 0.9025:
                    Signals_temp[i, 0] = 0
                    # We cannot buy tomorrow morning
            # else :
            #     # todo Last modify 161225 之前下边Signal 赋值都是 1 ，会不会导致无信号的时候也都是买入股票
            #     Signals_temp[i, 0] = 0

        Signals = pd.DataFrame(Signals_temp)
        return Signals

    def Stra_MA_Signals_a_vol_pMA16(self, Ana_Data, data ):
        # 160921 logic: 股价在ma16下方，在短期股价较低处买入 | P_MA40[i]<0.03
        # 4p_ma40: price<= 1.03*ma16
        # derived from Stra_MA_Signals_a , which is derived from Stra_MA_Signals
        # data ：DATE	OPEN	HIGH	LOW	CLOSE	VOLUME	AMT	PCT_CHG
        # type of data is pandas , need to be transferred to
        amt=data['amt'].values
        close=data['close'].values
        open=data['open'].values
        [x, y] = Ana_Data.shape  # x is # of days and y=6
        Signals_temp = np.zeros([x, 1])  # 520*15
        P_MA40 = Ana_Data['P/MA40']
        P_MA16 = Ana_Data['P/MA16']
        MA8_up = Ana_Data['MA8_up']
        MA16_up = Ana_Data['MA16_up']
        MA40_up = Ana_Data['MA40_up']
        MA100_up = Ana_Data['MA100_up']
        for i in range(100 + 1, x-1 ):
            # Case 1: Signal: 0 to 1
            if Signals_temp[i-1, 0] == 0 :
                #　Buy signal:
                if (P_MA40[i] >= -0.01 and MA16_up[i] >= 0) and P_MA40[i]<0.03   :
                    # We can buy tomorrow morning
                    if amt[i + 1] > 10000000 and open[i + 1] <= close[i] * 1.0975:
                        Signals_temp[i, 0] = 1
                    # We cannot buy tomorrow morning
                    else:
                        Signals_temp[i, 0] = 0

            # Case 2: Signal: 1 to 1
            if Signals_temp[i - 1, 0] == 1:
                # 　Hold signal:
                if P_MA40[i] >= -0.01 and MA16_up[i] >= 0:
                    Signals_temp[i, 0] = 1
                # Case 3: Signal: 1 to 0
                else :
                    # We can sell tomorrow morning
                    if amt[i + 1] > 2000000 and open[i + 1] >= close[i] * 0.9025:
                        Signals_temp[i, 0] = 0
                    # We cannot buy tomorrow morning
                    else :
                        Signals_temp[i, 0] = 1

        Signals = pd.DataFrame(Signals_temp)
        return Signals

    def Stra_Low2_Signals_vol(self,  data, Last_N):
        # 160819 Aeon_rC
        # 选股策略，选出： 就一个交易日
        # 过去6个月内，从高点开始，先出现一轮较大的下跌，盘整后，再出现第二轮较大的下跌后不再创新低时，买入
        # Last_N :最近N个交易日
        amt=data['amt'].values
        close=data['close'].values
        open=data['open'].values
        high = data['high'].values
        low = data['low'].values
        [x, y] = data.shape  # x is # of days and y=6

        #
        # Signals_temp = np.zeros([x, 1])  # 520*15
        # P_MA40 = Ana_Data['P/MA40']
        # P_MA16 = Ana_Data['P/MA16']
        # MA8_up = Ana_Data['MA8_up']
        # MA16_up = Ana_Data['MA16_up']
        # MA40_up = Ana_Data['MA40_up']
        # MA100_up = Ana_Data['MA100_up']

        # 过去x个交易日（6个月）内，从高点开始，先出现一轮较大的下跌，盘整后，再出现第二轮较大的下跌后不再创新低时，买入
        # 条件： 总天数 x=120天左右
            # Last 130 trading days=last 6 months
        # Last_N :最近N个交易日
        x = min(x, Last_N)
        N0=-1*x
        # 取close的最后 x个交易日数值
        close0=close[N0:]
        open0 =open[N0:]
        dif_len= len(close) - len(close0)
        # dif= len(close) - len(close2)
        # 1, x长度内，最初的0~40天内，定位最高收盘价（最高最高价）对应日期k-th day，0<=k<=40
        N1=40
        # print(close0)
        # print( max( close0[:N1]) )
        # print( type(open0) )
        if max( close0[:N1] ) >= max( open0[:N1]) :
        #感觉用最高价太不靠谱 max(high(-1*N1:) )
            H1_N1 = close0[:N1].max()
            # 数值上： length(close)>x>N1*-1>   close.index(max(close(N1:) ) )
            # 总时间长度减过去x个交易日，加上-N1个交易日内中的到最高收盘价的位置：
            # len(close)+N1+ close[N1:].index( max(close[N1:])  )
            # Numpy中找出array中最大值所对应的行和列
                # http://blog.csdn.net/theonegis/article/details/50819871
                # p.where( close0[:N1]== H1_N1 )[0] 这整个才是 array
            H1_N1_place = np.where( close0[:N1]== H1_N1 )[0][0]
            # H1_N1_place = close0.index( H1_N1 ) # dif_len + close0.index( H1_N1 )
        else :
            # H1_N1 = max(open0[:N1] )
            H1_N1 = open0[:N1].max()
            # print(H1_N1)
            # print(np.where( open0[:N1]== H1_N1 )[0])
            H1_N1_place = np.where( open0[:N1]== H1_N1 )[0][0]

        # 2，从最高收盘价开始，定位第一波下跌达到的最低价 m1 =min[k, x-10 ]，计算k~m1 的跌幅dd1(drawdown)
            # 假设，一轮连续的下跌时间不超过40个交易日
        N2=H1_N1_place+40*1.1
        if min( close0[H1_N1_place :N2] ) <= min( open0[H1_N1_place :N2] ) :
            L2_N2= close0[H1_N1_place :N2].max()
            L2_N2_place = np.where( close0[H1_N1_place :N2] == L2_N2 )[0][0]
        else :
            L2_N2 = open0[H1_N1_place:N2].min()
            L2_N2_place = np.where( open0[H1_N1_place :N2] == L2_N2 )[0][0]

        DD1=L2_N2/H1_N1-1 # 注意 DD1<0 是必要条件，否则这段时间在上涨！
        # 3 定位 L2 开始到倒数第10个交易日区间内的最高点H2, 看[L2--H2]区间内价格波动情况miu， vol
            #  计算[m2,x]区间的股价日均波动情况miu2, std2，当前价格/最高价h2,低点以来最高涨幅r2=h2/m2-1
        N3=-10
        if max( close0[L2_N2_place:N3] ) >= max( open0[L2_N2_place:N3] ) :
            H2_N2= close0[L2_N2_place:N3].max()
            H2_N2_place= np.where( close0[L2_N2_place:N3] == H2_N2 )[0][0]

        else :
            H2_N2 = open0[L2_N2_place:N3].max()
            H2_N2_place = np.where( open0[L2_N2_place:N3] == H2_N2 )[0][0]

        r_2=[]
        for i in [L2_N2_place+1 , H2_N2_place]  :
            r_2=r_2+[ close0[i]/open0[i]-1 ]
        r2_np = np.array(r_2)
        # todo: 只统计上涨日/下跌日 的平均幅度和波动
        miu2= r2_np.sum()/len(r_2) # abs(miu2)<1.5%, or miu2**2< 0.000225
        r2_2_np= r2_np*r2_np
        std2= r2_2_np.sum()/len(r_2)-miu2**2 # std2< 3% or 2.5%

        # 4，定位H2开始至最新交易日的最低价，或最后20个交易日内的最低价，并分析最低价开始的波幅
            # 第三段区间下跌的最低价 L3=min(close0[-10:]); 计算2波低价中最高价 h1=max[m1~m2]，跌幅dd3(drawdown)
            # H2_N2_place <  len(close0)+(-10)
        if H2_N2_place <  len(close0)+N3 :
            if min( close0[H2_N2_place :] ) <= min( open0[ H2_N2_place :] ) :
                L3_N3 = close0[ H2_N2_place :].min()
                L3_N3_place = np.where( close0[H2_N2_place :] == L3_N3 )[0][0]
                # L3_N3_place = close0.index( L3_N3 )
            else :
                L3_N3 = open0[ H2_N2_place :].min()

                L3_N3_place = np.where( open0[H2_N2_place :] == L3_N3 )[0][0]
        else :
            if min(close0[N3: ] ) <= min(open0[ N3:] ):
                L3_N3 = close0[ N3:].min()
                L3_N3_place = np.where( close0[N3:] == L3_N3 )[0][0]
                    # close0.index( L3_N3 )
            else:
                L3_N3 = min(open0[ N3:] )
                L3_N3_place = np.where( open0[N3:] == L3_N3 )[0][0]

        DD3=L3_N3/H2_N2-1
        # 5，判定条件：1 dd1<=-25% ,2 dd3<=-10%, miu2<2%, std2<2%, h2<=m2*1.10 或1.15
        Signal = 0
        if DD1<=-0.20 and DD3<-0.09 :
            if ( miu2< 1.5/100 and miu2> -1.5/100 ) and std2<= 3/100 :
                if  H2_N2<L2_N2*1.2 and L3_N3<=L2_N2*1.03:
                    Signal=1

        return Signal
        # 160822

    def Stra_MA_Signals_a_vol_min(self, Ana_Data, data):
        # 160507 观察剔除成交金额为0和开盘涨停后的收益
        # derived from Stra_MA_Signals_a , which is derived from Stra_MA_Signals
        # data ：DATE	OPEN	HIGH	LOW	CLOSE	VOLUME	AMT	PCT_CHG
        # type of data is pandas , need to be transferred to
        amt = data['amt'].values
        close = data['close'].values
        open = data['open'].values
        [x, y] = Ana_Data.shape  # x is # of days and y=6
        Signals_temp = np.zeros([x, 1])  # 520*15
        P_MA40 = Ana_Data['P/MA40']
        P_MA100 = Ana_Data['P/MA100']
        P_MA16 = Ana_Data['P/MA16']
        MA8_up = Ana_Data['MA8_up']
        MA16_up = Ana_Data['MA16_up']
        MA40_up = Ana_Data['MA40_up']
        MA100_up = Ana_Data['MA100_up']
        for i in range(100 + 1, x - 1):
            # Case 1: Signal: 0 to 1
            if Signals_temp[i - 1, 0] == 0:
                # 　Buy signal:
                if P_MA100[i] >= -0.01 and MA100_up[i] >= 0:
                    # We can buy tomorrow morning
                    if amt[i + 1] > 2000000 and open[i + 1] <= close[i] * 1.0975:
                        Signals_temp[i, 0] = 1
                    # We cannot buy tomorrow morning
                    else:
                        Signals_temp[i, 0] = 0

            # Case 2: Signal: 1 to 1
            if Signals_temp[i - 1, 0] == 1:
                # 　Hold signal:
                if P_MA100[i] >= -0.01 and MA100_up[i] >= 0:
                    Signals_temp[i, 0] = 1
                # Case 3: Signal: 1 to 0
                else:
                    # We can sell tomorrow morning
                    if amt[i + 1] > 2000000 and open[i + 1] >= close[i] * 0.9025:
                        Signals_temp[i, 0] = 0
                    # We cannot buy tomorrow morning
                    else:
                        Signals_temp[i, 0] = 1

        Signals = pd.DataFrame(Signals_temp)
        return Signals

    def Stra_MA_Signals_b(self, Ana_Data) :
        # 160428
        [x,y]=Ana_Data.shape # x is # of days and y=6
        Signals_temp= np.zeros([x,1 ])    # 520*15
        P_MA40=Ana_Data['P/MA40']
        P_MA16=Ana_Data['P/MA16']
        MA8_up=Ana_Data['MA8_up']
        MA16_up=Ana_Data['MA16_up']
        MA40_up=Ana_Data['MA40_up']
        MA100_up=Ana_Data['MA100_up']
        for i in range(100+1, x )  :
            if P_MA40[i]>=0 and  MA16_up[i]>=0 and  MA40_up[i]>=0 and  MA100_up[i]>=0 :# and  MA40_up[i]>=0 : #  MA100_up.iloc[i-1]>=-0.001 and
                Signals_temp[i,0]=1

        Signals= pd.DataFrame( Signals_temp )
        return Signals

    def Stra_MA_Signals_2(self, Ana_Data) :

        # Condition1: P>MA40D and MA8D is Up.
        # ？用哪个信号呢？要不先从P>MA40开始吧。
        [x,y]=Ana_Data.shape # x is # of days and y=6
        ''' 注意：这里的Signals每一列代表一个策略，如果是多列的话我们可以做成 '''
        Signals_temp= np.zeros([x,1 ])    # 520*15
        #Signals= pd.DataFrame( np.zeros([x,1 ])  )  # 520*15
        print('=======Stra_MA=======')
        #　Columns=['MA3','MA8','MA16','MA40','MA100','P/MA3','P/MA8','P/MA16','P/MA40','P/MA100','MA3_up','MA8_up','MA16_up','MA40_up','MA100_up']

        P_MA40=Ana_Data['P/MA40']
        P_MA16=Ana_Data['P/MA16']
        MA8_up=Ana_Data['MA8_up']
        MA16_up=Ana_Data['MA16_up']
        MA40_up=Ana_Data['MA40_up']
        MA100_up=Ana_Data['MA100_up']
        for i in range(100+1, x )  :
            # and Ana_Data.iloc[i,11] >=0  and Ana_Data.iloc[i,8]>0  : # 11th item means MA8_Up and 8th means P/MA40d
            # if P_MA16.iloc[i] >0 and P_MA40.iloc[i] >0 and MA16_up.iloc[i]>0 and MA40_up.iloc[i]>0 :
            #160125: if  MA8_up.iloc[i]>=0 and MA8_up.iloc[i-1]>=-0.005 and P_MA40.iloc[i] >=-0.005 and MA40_up.iloc[i]>=-0.01 :
            #    if  MA8_up.iloc[i]>=0 and MA8_up.iloc[i-1]>=-0.005 and P_MA16.iloc[i] >=-0.005 and MA16_up.iloc[i]>=-0.01 and MA40_up.iloc[i] >=-0.005 and P_MA40.iloc[i] >=-0.01:
            # Fixed Items： Means these items can provide solid effect either: shift return or lower MDD, avoid frequent trading
            if MA100_up.iloc[i]>=-0.001 and MA16_up.iloc[i]>=-0.001 :
                 Signals_temp[i,0]=1

            # if i<x-20 and Signals.iloc[i+1,0]==0 :
            #     if Ana_Data.iloc[i,8]>0 and
                # todo  P>40 and MA8D,MA16D both up and P/max(MA8,MA16,MA40)<1.05 and MA40D<1.05*max(MA40D(last 40days) ) 160113

        Signals= pd.DataFrame( Signals_temp )
        # file_path=file_path0+'Output_Signals.csv'
        # Signals.to_csv(  file_path  , encoding='utf-8', index=False)
        # print( 'Data has been saved to:  ' +file_path )
        return Signals

    def Stra_MA_Signals_short(self, Ana_Data):
        # We introduce short signals
        # Condition1: P>MA40D and MA8D is Up.
        # ？用哪个信号呢？要不先从P>MA40开始吧。
        [x, y] = Ana_Data.shape  # x is # of days and y=6
        ''' 注意：这里的Signals每一列代表一个策略，如果是多列的话我们可以做成 '''
        Signals_temp = np.zeros([x, 1])  # 520*15
        # Signals= pd.DataFrame( np.zeros([x,1 ])  )  # 520*15
        print('=======Stra_MA=======')
        # 　Columns=['MA3','MA8','MA16','MA40','MA100','P/MA3','P/MA8','P/MA16','P/MA40','P/MA100','MA3_up','MA8_up','MA16_up','MA40_up','MA100_up']

        P_MA40 = Ana_Data['P/MA40']
        P_MA16 = Ana_Data['P/MA16']
        MA8_up = Ana_Data['MA8_up']
        MA16_up = Ana_Data['MA16_up']
        MA40_up = Ana_Data['MA40_up']
        thres0 = 0
        thres1 = 0.002
        for i in range(100 + 1, x):
            # MA16_up.iloc[i]>0 and MA16_up.iloc[i-1]>0 and P_MA40.iloc[i] >0.03  :
            if P_MA40.iloc[i] > thres0 and MA16_up.iloc[i] > thres1 * -1:
                # 'MA8' is Up and P>MA40D
                Signals_temp[i, 0] = 1
            elif P_MA40.iloc[i] < thres0 * -1 and MA16_up.iloc[i] < thres1:
                Signals_temp[i, 0] = -1


                # if i<x-20 and Signals.iloc[i+1,0]==0 :
                #     if Ana_Data.iloc[i,8]>0 and
                # todo  P>40 and MA8D,MA16D both up and P/max(MA8,MA16,MA40)<1.05 and MA40D<1.05*max(MA40D(last 40days) ) 160113

        Signals = pd.DataFrame(Signals_temp)
        # file_path=file_path0+'Output_Signals.csv'
        # Signals.to_csv(  file_path  , encoding='utf-8', index=False)
        # print( 'Data has been saved to:  ' +file_path )
        return Signals

    def Stra_MA8_H2sL2b(self, Ana_Data) :
        # Condition1: P>MA40D and MA8D is Up.
        # ？用哪个信号呢？要不先从P>MA40开始吧。
        [x,y]=Ana_Data.shape # x is # of days and y=6
        ''' 注意：这里的Signals每一列代表一个策略，如果是多列的话我们可以做成 '''
        Signals_temp=   np.zeros([x,1 ])   # 520*15

        # print('======= Stra_MA8_H2sL2b ======')
        #　Columns=['MA3','MA8','MA16','MA40','MA100','P/MA3','P/MA8','P/MA16','P/MA40','P/MA100','MA3_up','MA8_up','MA16_up','MA40_up','MA100_up']

        for i in (100-1, x ):

            '''MA3D price is the min in pas 8 days, 这里用MA3D代替close  '''
            if i<x-20-8 and Signals.iloc[i+1,0]==0  and Ana_Data.iloc[i+1,12] <0 and Ana_Data.iloc[i+1,1]==min(Ana_Data.iloc[i+1:i+8,1])  and data['low'][i] >data['low'][i+1]*1.005   : # 11th item means MA8_Up and 8th means P/MA40d
                # 'MA8' is Up and P>MA40D
                Signals_temp[i,0]=1

            elif i<x-20-8 and Signals.iloc[i+1,0]==1 : # Hold stocks
                if data['low'][i] <=data['low'][i+1]*0.99 or data['high'][i] <=data['high'][i+1]*0.99 :
                    Signals_temp[i,0]=0
                else:
                    Signals_temp[i,0]=1
            else :
                    Signals_temp[i,0]=0
        Signals= pd.DataFrame( Signals_temp  )
        file_path=file_path0+'Output_Signals_H2s.csv'
        Signals.to_csv(  file_path  , encoding='utf-8', index=False)
        print( 'Data has been saved to:  ' +file_path )
        return Signals

    def Stra_MA16x_5D(self, Ana_Data) : # In 5D, signals is a list
        # 计算策略在过去5个交易日的
        # Condition1: P>MA40D and MA8D is Up.
        # ？用哪个信号呢？要不先从P>MA40开始吧。
        [x,y]=Ana_Data.shape # x is # of days and y=6
        ''' 注意：这里的Signals每一列代表一个策略，如果是多列的话我们可以做成 '''

        # Signals= pd.DataFrame( np.zeros([x,1 ])  )  # 520*15
        Signals=[0,0,0,0,0]
        # print('======= Stra_MA8_H2sL2b ======')
        #　Columns=['MA3','MA8','MA16','MA40','MA100','P/MA3','P/MA8','P/MA16','P/MA40','P/MA100','MA3_up','MA8_up','MA16_up','MA40_up','MA100_up']
        tempS=len(Signals)
        for i in range( tempS ):
            '''MA3D price is the min in pas 8 days, 这里用MA3D代替close  '''
            if Ana_Data.iloc[i,12] >0 and Ana_Data.iloc[i,8]>0  :
                # 11th : MA8_Up and 12th : MA16_Up and 8th means P/MA40d
                # 'MA8' is Up and P>MA40D
                # Signals.iloc[i,0]=1
                Signals[i]=1
            elif Signals[i-1]==1 : # Hold stocks
                if data['low'][i] <=data['low'][i-1]*0.99 or data['high'][i] <=data['high'][i-1]*0.99 :
                    Signals[i]=0
                else:
                    Signals[i]=1
            else :

                Signals[i]=0
        # file_path=file_path0+'Output_Signals_H2s.csv'
        # Signals.to_csv(  file_path  , encoding='utf-8', index=False)
        # print( 'Data has been saved to:  ' +file_path )
        return Signals

    def Stra_MA8_H2sL2b_5D(self, Ana_Data) : # In 5D, signals is a list

        # Condition1: P>MA40D and MA8D is Up.
        # ？用哪个信号呢？要不先从P>MA40开始吧。
        [x,y]=Ana_Data.shape # x is # of days and y=6
        ''' 注意：这里的Signals每一列代表一个策略，如果是多列的话我们可以做成 '''

        # Signals= pd.DataFrame( np.zeros([x,1 ])  )  # 520*15
        Signals=[0,0,0,0,0]
        # print('======= Stra_MA8_H2sL2b ======')
        #　Columns=['MA3','MA8','MA16','MA40','MA100','P/MA3','P/MA8','P/MA16','P/MA40','P/MA100','MA3_up','MA8_up','MA16_up','MA40_up','MA100_up']
        tempS=len(Signals)
        for i in range( tempS ):
            '''MA3D price is the min in pas 8 days, 这里用MA3D代替close  '''
            if Ana_Data.iloc[i-1,12] <0 and Ana_Data.iloc[i-1,1]==min(Ana_Data.iloc[i-1:i-8,1])  and data['low'][i] >data['low'][i-1]*1.005   :
                # 11th item means MA8_Up and 8th means P/MA40d
                # 'MA8' is Up and P>MA40D
                # Signals.iloc[i,0]=1
                Signals[i]=1
            elif  Signals[i-1]==1 : # Hold stocks
                if data['low'][i] <=data['low'][i-1]*0.99 or data['high'][i] <=data['high'][i-1]*0.99 :
                    Signals[i]=0
                else:
                    Signals[i]=1
            else :

                Signals[i]=0
        # file_path=file_path0+'Output_Signals_H2s.csv'
        # Signals.to_csv(  file_path  , encoding='utf-8', index=False)
        # print( 'Data has been saved to:  ' +file_path )
        return Signals

    def Stra_MA8_H2sL2b_tushare(self, time0='2015-07-01', time1='2015-12-29' ) :
        # Get latest signals using Stra_MA8_H2sL2b strategies :
        import tushare as ts
        CodeList=ts.get_industry_classified()
        print( CodeList.shape )

        #　Columns=['MA3','MA8','MA16','MA40','MA100','P/MA3','P/MA8','P/MA16','P/MA40','P/MA100','MA3_up','MA8_up','MA16_up','MA40_up','MA100_up']
        MA_x=[3, 8, 16,40,100]
        P_MA=[0 , 0,0,0,0] # 1 if Price>MA(x)
        # Creat Signals matrix

        try:
            # Signals=pd.DataFrame( np.zeros([6, len(CodeList)  ])  )
            Signals_temp=  np.zeros([6, len(CodeList)  ])
            tempCL=len(CodeList)
            for i2 in range( tempCL):  # len(CodeList)
                code=CodeList.iloc[i2,0]   # '600030'
                Results=rC_TradeSys(code, time0 , time1)

                data2=Results.ImportFinancialData(code, time0 , time1)
                try :
                    Ana_Data2=Results.AnalyticFiData(data2,MA_x, P_MA, [1,1,1,1,1]  )
                    Signals_temp2=Results.Stra_MA8_H2sL2b_5D(Ana_Data2)
                    Signals_temp[i2]=Signals_temp2  #选取第0行到第4行，第0列到第1列区域内的元素,赋值给Signals的第i2列
                    if sum( Signals_temp2)>0 :
                        print([i2,code,Signals_temp2]  )
                except:
                     print( 'Error occur' )
                     print([i2,code ]  )
                     print(Signals_temp2 )
                # print('--- Signals_Stra_MA8_H2sL2b---OK')
                # print( Signals_temp.iloc[:5, :2].T )
                # Signals[i2]=Signals_temp.iloc[:5, :2].T  #选取第0行到第4行，第0列到第1列区域内的元素,赋值给Signals的第i2列
                    # len(Signals_temp) =123 and len(Signals_temp.T)=1
                # print( Signals_temp )

        except :
            print(code)
            print(i2)
            print( Signals_temp.iloc[:5, :2].T )
            print('--- Signals_Stra_MA8_H2sL2b---')
            print( Signals[0:i2] )
            file_path=file_path0+'Output_Signals_H2sL2b_160105.csv'
            Signals.to_csv(  file_path  , encoding='utf-8', index=False)
            file_path2=file_path0+'Output_Signals_H2sL2b_160105_codelist.csv'
            CodeList.to_csv(  file_path2  , encoding='utf-8', index=False)

        Signals=pd.DataFrame( Signals_temp  )
        print('--- Signals_Stra_MA8_H2sL2b---OK2')
        print(  i2 )
        print(  Signals[i2] )
        file_path=file_path0+'Output_Signals_H2sL2b.csv'
        Signals.to_csv(  file_path  , encoding='utf-8', index=False)
        print( 'Data has been saved to:  ' +file_path )
        file_path2=file_path0+'Output_Signals_H2sL2b_160105_codelist.csv'
        CodeList.to_csv(  file_path2  , encoding='utf-8', index=False)
        return Signals

    def Stra_MA_dif_M(self, Ana_Data, data ):
        # 170930 ：ma40-ma_16
        # todo 170723 derived from ABCD idea:
        # todo amt[i+1] ==0 161228 有可能持有期停牌，或其他无成交量的情况
        # 161006 Stra_MA_Signals_a_vol calculate the B/S/H signal after current trading day closed, any
            # transactions will be excuted at next trading days
        # 160507 观察剔除成交金额为0和开盘涨停后的收益
        # derived from Stra_MA_Signals_a , which is derived from Stra_MA_Signals
        # data ：DATE	OPEN	HIGH	LOW	CLOSE	VOLUME	AMT	PCT_CHG
        # type of data is pandas , need to be transferred to
        amt=data['amt'].values
        close=data['close'].values
        open=data['open'].values
        [x, y] = Ana_Data.shape  # x is # of days and y=6
        Signals_temp = np.zeros([x-1, 1])  # 520*15
        P_MA8 = Ana_Data['P/MA8']
        P_MA40 = Ana_Data['P/MA40']
        P_MA100 = Ana_Data['P/MA100']
        P_MA16 = Ana_Data['P/MA16']
        MA8_up = Ana_Data['MA8_up']
        MA16_up = Ana_Data['MA16_up']
        MA40_up = Ana_Data['MA40_up']
        MA100_up = Ana_Data['MA100_up']
        # 161006, we change for i in range(100+1 , x-1 ): to for i in range(100 , x-1 ):
        # 170723 1629
        # dif_S = P_MA8  - P_MA16
        # P_MA8  - P_MA16 = (P-ma8) - (P- ma16 ) = ma16 - ma8
        dif_M = P_MA16 - P_MA40
        # todo Stra 170930 ：等价于 ma40-ma_16 这个值到达最大值开始缩小后，买入； 到达最小值开始扩大后，卖出。
        # todo Ana[i] 对应的是截止 i-1 日收盘的数据，也就是说 i-Day时，我们有 Ana[:i]的数据
        j= 0
        for i in range(100 , x-1 ):
            if j>0 :
                # Case 1: Signal: 0 to 1
                if Signals_temp[i-1, 0] == 0 :
                    #　Buy signal:
                    if dif_M[i] >= 0 and dif_M[i] >= dif_M[j]   :
                    # if dif_M[i] >= 0 and P_MA40[i]>=0   :
                        # We can buy tomorrow morning
                        if open[i ] <= close[i-1] * 1.0975 :
                            Signals_temp[i, 0] = 1
                        # We cannot buy tomorrow morning
                        else:
                            Signals_temp[i, 0] =0

                # Case 2: Signal: 1 to 1
                if Signals_temp[i - 1, 0] == 1:
                    # 170723_1639 Hold signal:
                    if dif_M[i] >= 0 and dif_M[i] >= dif_M[j] :
                    # if dif_M[i] >= 0 and P_MA40[i]>=0 :
                        Signals_temp[i, 0] = 1
                    # Case 3: Signal: 1 to 0
                    else :
                        # We can sell tomorrow morning
                        if open[i +1] >= close[i] * 0.9025 and amt[i + 1] >5000000 :
                            Signals_temp[i, 0] = 0
                        # We cannot buy tomorrow morning
                        else :
                            Signals_temp[i, 0] = 1
            j = i

        Signals = pd.DataFrame(Signals_temp)
        return Signals

    def Stra_GoodStock(self, Ana_Data, data ):
        # 170930 ：为好股票（总是会创新高）建一个策略

        amt = data['amt'].values
        close = data['close'].values
        open = data['open'].values
        [x, y] = Ana_Data.shape  # x is # of days and y=6
        Signals_temp = np.zeros([x, 1])  # 520*15
        P_MA40 = Ana_Data['P/MA40']
        P_MA16 = Ana_Data['P/MA16']
        MA8_up = Ana_Data['MA8_up']
        MA16_up = Ana_Data['MA16_up']
        MA40_up = Ana_Data['MA40_up']
        MA100_up = Ana_Data['MA100_up']
        # 161006, we change for i in range(100+1 , x-1 ): to for i in range(100 , x-1 ):
        for i in range(100, x - 1):
            # Case 1: Signal: 0 to 1
            if Signals_temp[i - 1, 0] == 0:
                # 　Buy signal:
                if P_MA40[i] >= -0.01 and MA16_up[i] >= 0:
                    # We can buy tomorrow morning
                    #
                    if amt[i + 1] > 5000000 and open[i + 1] <= close[i] * 1.0975:
                        temp_Cost = open[i + 1]
                        Signals_temp[i, 0] = 1
                    # We cannot buy tomorrow morning
                    else:
                        Signals_temp[i, 0] = 0

            # Case 2: Signal: 1 to 1
            if Signals_temp[i - 1, 0] == 1:
                # 　Hold signal:
                if P_MA40[i] >= -0.01 and MA40_up[i] >= 0:
                    Signals_temp[i, 0] = 1
                elif amt[i + 1] == 0:
                    Signals_temp[i, 0] = 1
                # Case 3: Signal: 1 to 0
                else:
                    # We can sell tomorrow morning
                    if amt[i + 1] >= 5000000 and open[i + 1] >= close[i] * 0.9025:
                        # Signals_temp[i, 0] = 0
                        if temp_Cost *1.01 <  open[i + 1]:
                            Signals_temp[i, 0] = 0
                        else :
                            Signals_temp[i, 0] = 1
                    # We cannot buy tomorrow morning
                    else:
                        Signals_temp[i, 0] = 1

        Signals = pd.DataFrame(Signals_temp)
        return Signals


        Signals = pd.DataFrame(Signals_temp)
        return Signals

    # =========================170930 ===============================================================

    def Account_one(self,data,  Ana_Data, Signals ) :
        # We use this account to adjust singal stock performance
        # 160312 改进一个重大错误
        Columns=['Num_Max', 'Cash', 'Stock','Total','Unit', 'MDD', 'Index','MDD_I' ] # 8 items
        [x,y]=Ana_Data.shape
        Account_one_temp=np.zeros([x,8 ])
        Account_one= pd.DataFrame( np.zeros([x,8 ]),columns=Columns )  # 520*8

        ''' Length of Signal, Data, Ana_Data,Account_one are the same!  '''
        # print('Length of Data. is: ', data.shape) # 843,6
        # print('Length of Signal. is: ', len( Signals )) # 843
        # [x2,y2]=Account_one.shape # 843, 8
        # print('Length of Account_one. is: ', Account_one.shape ) # 843,8
        fee1=0.0025
        N1=100
        Initial=10000000
        # Notice that length of Account_one is x but last row of Account_one is as Account_one[x-1,:]

        # # print('Length of Account_one. is: ', Account_one.iloc[x-1,:] )
        # Account_one.iloc[x-1,:]=[0,Initial,0,Initial,1,0,1,0]
        # Account_one.iloc[x-2,:]=[0,Initial,0,Initial,1,0,1,0]
        for i2 in range( 100 ):
            #  from 0 to np.max(MA_x)-1=99
            Account_one_temp[ i2,:]=[0,Initial,0,Initial,1,0,1,0]
        # try:
        close=data['close']
        open=data['open']

        for j in range( 100-2, x  ):
            # From 100-2+1=99 to x-1
            # i2= 0,1,..., x-3

            # print('Length of Account_one. is: ',  j, len(Signals ) )
            # Signals[0][j+1] : Signal 的column "0" 的第j+1个值
            # print('XXXXX: ',  j,Signals[0][j+1], Signals[0][j+2] )
            if Signals[0][j-1]==1 and Signals[0][j-2]==0 :
            # Buy stocks signal in last day
                # No stock case
                if  Account_one_temp[j-1,0]==0 :
                    # Number of stocks :Calculate maximum number of stocks we can buy.
                    ''' 注意：以后仓位的动态调整可以从这里进一步设置  '''
                    # print( data['open'][j] )
                    # print( Account_one.iloc[j+1,2] )
                    # Account_one.iloc[j,1]= round( Account_one.iloc[j+1,2]/100/data['open'][j] ）
                    Account_one_temp[j,0]=  round(  Account_one_temp[j-1,1]/N1/open.iloc[j]*0.95  )   # data['open'][j]
                    # print( Account_one.iloc[j,0] )
                    # Stock Value : Calculate total value of stocks :
                    Account_one_temp[j,2]= Account_one_temp[j,0]*N1*close.iloc[j]  # data['open'][j]

                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]= Account_one_temp[j-1 ,1] -  Account_one_temp[j,0]*N1*open.iloc[j] *(1+fee1)
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]= min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6] # data['close'][j]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  '1st day Buy signal but already have positions ' )
            elif Signals[0][j-1]==1 and Signals[0][j-2]==1 :
            # Hold stocks
                # Number of stocks
                if  Account_one_temp[j-1,0]>0 :
                    Account_one_temp[j,0]=Account_one_temp[j-1,0]
                    # Stock Value : Calculate total value of stocks :?
                    Account_one_temp[j,2]= Account_one_temp[j,0]*N1*close.iloc[j]
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]=Account_one_temp[j-1,1]
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]= min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  'Hold signals but no positions ; ' )

            elif Signals[0][j-1]==0 and Signals[0][j-2]==1  :
            # Sell stocks
                # Number of stocks
                if  Account_one_temp[j-1,0]>0 :
                    Account_one_temp[j,0]=Account_one_temp[j-1,0]
                    # Stock Value : Calculate total value of stocks :
                    Account_one_temp[j,2]= 0
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]=Account_one_temp[j-1,1] + Account_one_temp[j,0]*N1*open.iloc[j]*(1-fee1)
                    Account_one_temp[j,0]=0
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]=  min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  'Sell signals but no positions ' )
            elif Signals[0][j-1]==0 and Signals[0][j-2]==0 :
            # No stocks
                if Account_one_temp[j-1,0]==0 :
                    Account_one_temp[j,0]=0
                    # Stock Value : Calculate total value of stocks :? todo : Open*Max_number
                    Account_one_temp[j,2]= 0
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]=Account_one_temp[j-1,1]
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]=  min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1 ,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1 ,6]) -1)
                else :
                    print(  'No signals but have positions ' )
                    print(  Account_one.iloc[j-1,0] )

        # except :
        #     print('--- Error for account---')
        #     print(i2,j,)
        #     print('--- Error for account---')

        Account_one= pd.DataFrame( Account_one_temp,columns=Columns )  # 520*8
        # file_path=file_path0+'Account_one.csv'
        # Account_one.to_csv(  file_path  , encoding='utf-8', index=False)

        # print( 'Account_one results have been saved to:  ' +file_path )
        return Account_one

    def Account_one_L(self, data, Ana_Data, Signals,Leverage ):

        # todo 171216 解决了在 signal第一天 1 to 0 无法卖出时，第二天0 to 0 也能正常卖出的问题
        #   解决办法是　在0 to 0 case, 如果昨日持仓数量大于0，执行卖出
        # todo Last Update 171216 | since  161228
        # 160428 加入Leverage 指的是杠杆率，Leverage=50%表示只花50%的资金买股票
        amt = data['amt'].values
        Columns = ['Num_Max', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']  # 8 items
        x=len(Signals[0])
        # [x, y] = Ana_Data.shape
        Account_one_temp = np.zeros([x, 8])
        # Account_one_L = pd.DataFrame(np.zeros([x, 8]), columns=Columns)  # 520*8

        ''' Length of Signal, Data, Ana_Data,Account_one are the same!  '''
        # print('Length of Data. is: ', data.shape) # 843,6
        # print('Length of Signal. is: ', len( Signals )) # 843
        # [x2,y2]=Account_one.shape # 843, 8
        # print('Length of Account_one. is: ', Account_one.shape ) # 843,8
        fee1 = 0.0025
        N1 = 100
        Initial = 10000000
        # Notice that length of Account_one is x but last row of Account_one is as Account_one[x-1,:]

        # # print('Length of Account_one. is: ', Account_one.iloc[x-1,:] )
        # Account_one.iloc[x-1,:]=[0,Initial,0,Initial,1,0,1,0]
        # Account_one.iloc[x-2,:]=[0,Initial,0,Initial,1,0,1,0]
        for i2 in range(100):
            #  from 0 to np.max(MA_x)-1=99
            Account_one_temp[i2, :] = [0, Initial, 0, Initial, 1, 0, 1, 0]
        # try:
        close = data['close']
        open = data['open']

        for j in range(100 - 2, x):
            # todo beginning of the day
            # todo  Num Cash Stock MV
            Account_one_temp[j, 0] = Account_one_temp[j - 1, 0]
            Account_one_temp[j, 2] = Account_one_temp[j-1 , 0] * N1 * close.iloc[j-1]
            Account_one_temp[j, 1] = Account_one_temp[j - 1, 1]
            Account_one_temp[j, 3] = Account_one_temp[j, 2] + Account_one_temp[j, 1]
            # Unit : MDD
            Account_one_temp[j, 4] = Account_one_temp[j-1 , 4]
            Account_one_temp[j, 5] =Account_one_temp[j-1 , 5]
            # Index
            Account_one_temp[j, 6] = close.iloc[j] / close.iloc[j - 1] * Account_one_temp[j - 1, 6]
            # MDD_I
            Account_one_temp[j, 7] = min(Account_one_temp[j - 1, 7],   Account_one_temp[j, 6] / max(Account_one_temp[0:j - 1, 6]) - 1)
            # From 100-2+1=99 to x-1
            # i2= 0,1,..., x-3

            if amt[j] >= 5000000 and close.iloc[j]>0 :
                if Signals[0][j - 1] == 1 and Signals[0][j - 2] == 0:
                    # todo Buy stocks signal in the first  day
                    # todo No stock case
                    if Account_one_temp[j - 1, 0] <= 0:
                        # Number of stocks :Calculate maximum number of stocks we can buy.
                        ''' 注意：以后仓位的动态调整可以从这里进一步设置  '''
                        # print( data['open'][j] )
                        # print( Account_one.iloc[j+1,2] )
                        # todo check if open[j] exist or not, might =0
                        if open.iloc[j] > 0 :
                            # Get number to buy
                            Account_one_temp[j, 0] = round(  Account_one_temp[j - 1, 1] / N1 / open.iloc[j] *Leverage * 0.95)  # data['open'][j]
                            # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                            Account_one_temp[j, 1] = Account_one_temp[j - 1, 1] - Account_one_temp[j, 0] * N1 * \
                                                                                  open.iloc[j] * (1 + fee1)
                        else :
                            Account_one_temp[j, 0] = round(  Account_one_temp[j - 1, 1] / N1 / close.iloc[j] * Leverage * 0.95)  # data['open'][j]
                            # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                            Account_one_temp[j, 1] = Account_one_temp[j - 1, 1] - Account_one_temp[j, 0] * N1 * \
                                                                                  close.iloc[j] * (1 + fee1)
                        # print( Account_one.iloc[j,0] )
                        # Stock Value : Calculate total value of stocks :
                        Account_one_temp[j, 2] = Account_one_temp[j, 0] * N1 * close.iloc[j]  # data['open'][j]

                        # Total
                        Account_one_temp[j, 3] = Account_one_temp[j, 2] + Account_one_temp[j, 1]
                        # Unit :
                        Account_one_temp[j, 4] = Account_one_temp[j, 3] / Initial
                        # MDD
                        Account_one_temp[j, 5] = min(Account_one_temp[j - 1, 5],
                                                     Account_one_temp[j, 4] / max(Account_one_temp[0:j - 1, 4]) - 1)
                        # Index
                        Account_one_temp[j, 6] = close.iloc[j] / close.iloc[j - 1] * Account_one_temp[
                            j - 1, 6]  # data['close'][j]
                        # MDD_I
                        Account_one_temp[j, 7] = min(Account_one_temp[j - 1, 7],
                                                     Account_one_temp[j, 6] / max(Account_one_temp[0:j - 1, 6]) - 1)
                    else:
                        print('Warning :1st day Buy signal but already have positions ')


                elif Signals[0][j - 1] == 1 and Signals[0][j - 2] == 1:
                    # Hold stocks
                    # Number of stocks
                    if Account_one_temp[j - 1, 0] >= 0:
                        Account_one_temp[j, 0] = Account_one_temp[j - 1, 0]
                        # Stock Value : Calculate total value of stocks :?
                        Account_one_temp[j, 2] = Account_one_temp[j, 0] * N1 * close.iloc[j]
                        # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                        Account_one_temp[j, 1] = Account_one_temp[j - 1, 1]
                        # Total
                        Account_one_temp[j, 3] = Account_one_temp[j, 2] + Account_one_temp[j, 1]
                        # Unit :
                        Account_one_temp[j, 4] = Account_one_temp[j, 3] / Initial
                        # MDD
                        Account_one_temp[j, 5] = min(Account_one_temp[j - 1, 5],
                                                     Account_one_temp[j, 4] / max(Account_one_temp[0:j - 1, 4]) - 1)
                        # Index
                        Account_one_temp[j, 6] = close.iloc[j] / close.iloc[j - 1] * Account_one_temp[j - 1, 6]
                        # MDD_I
                        Account_one_temp[j, 7] = min(Account_one_temp[j - 1, 7],
                                                     Account_one_temp[j, 6] / max(Account_one_temp[0:j - 1, 6]) - 1)
                    else:
                        print('Hold signals but no positions ')
                        print(  Account_one_temp[j - 1, 0] )
                        # print('Hold signals but no positions ; Account_one ',j )
                        # print(Account_one_temp[j - 25:j + 1, :])
                        # print(close[j - 5:j + 1])
                        # asd
                elif Signals[0][j - 1] == 0 and Signals[0][j - 2] == 1:
                    # Sell stocks
                    # Number of stocks

                    if Account_one_temp[j - 1, 0] > 0:
                        Account_one_temp[j, 0] = Account_one_temp[j - 1, 0]
                        # Stock Value : Calculate total value of stocks :
                        Account_one_temp[j, 2] = 0
                        if open.iloc[j] >0 :
                            # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                            Account_one_temp[j, 1] = Account_one_temp[j - 1, 1] + Account_one_temp[j, 0] * N1 * open.iloc[j] * (  1 - fee1)

                        else :
                            Account_one_temp[j, 1] = Account_one_temp[j - 1, 1] + Account_one_temp[j, 0] * N1 * close.iloc[j] * (1 - fee1)

                        Account_one_temp[j, 0] = 0
                        # print('171216 2301 ', Account_one_temp[j , 0], open.iloc[j], close.iloc[j])
                        # Total
                        Account_one_temp[j, 3] = Account_one_temp[j, 2] + Account_one_temp[j, 1]
                        # Unit :
                        Account_one_temp[j, 4] = Account_one_temp[j, 3] / Initial
                        # MDD
                        Account_one_temp[j, 5] = min(Account_one_temp[j - 1, 5],
                                                     Account_one_temp[j, 4] / max(Account_one_temp[0:j - 1, 4]) - 1)
                        # Index
                        Account_one_temp[j, 6] = close.iloc[j] / close.iloc[j - 1] * Account_one_temp[j - 1, 6]
                        # MDD_I
                        Account_one_temp[j, 7] = min(Account_one_temp[j - 1, 7],
                                                     Account_one_temp[j, 6] / max(Account_one_temp[0:j - 1, 6]) - 1)
                    else:
                        print('Sell signals but no positions ' )
                        # 在停牌，跌停，涨停的情况下是有可能的

                elif Signals[0][j - 1] == 0 and Signals[0][j - 2] == 0:
                    if Account_one_temp[j-1 , 0]  <= 0 :
                        # No stocks
                        Account_one_temp[j, 0] = Account_one_temp[j - 1, 0]
                        # Stock Value : Calculate total value of stocks :?
                        if Account_one_temp[j - 1, 2] > 0:
                            Account_one_temp[j, 2] = Account_one_temp[j, 0] * N1 * close.iloc[j]
                        else:
                            Account_one_temp[j, 2] = Account_one_temp[j - 1, 2]
                        # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                        Account_one_temp[j, 1] = Account_one_temp[j - 1, 1]
                        # Total
                        Account_one_temp[j, 3] = Account_one_temp[j - 1, 3]
                        # Unit :
                        Account_one_temp[j, 4] = Account_one_temp[j, 3] / Initial
                        # MDD
                        Account_one_temp[j, 5] = min(Account_one_temp[j - 1, 5],
                                                     Account_one_temp[j, 4] / max(Account_one_temp[0:j - 1, 4]) - 1)
                        # Index
                        Account_one_temp[j, 6] = close.iloc[j] / close.iloc[j - 1] * Account_one_temp[j - 1, 6]
                        # MDD_I
                        Account_one_temp[j, 7] = min(Account_one_temp[j - 1, 7],
                                                     Account_one_temp[j, 6] / max(Account_one_temp[0:j - 1, 6]) - 1)
                    else :
                        # todo  有可能第一天没法卖出，那今天需要继续卖出
                        Account_one_temp[j, 0] = Account_one_temp[j - 1, 0]
                        # Stock Value : Calculate total value of stocks :
                        Account_one_temp[j, 2] = 0
                        if open.iloc[j] > 0:
                            # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                            Account_one_temp[j, 1] = Account_one_temp[j - 1, 1] + Account_one_temp[j, 0] * N1 * \
                                                                                  open.iloc[j] * (1 - fee1)

                        else:
                            Account_one_temp[j, 1] = Account_one_temp[j - 1, 1] + Account_one_temp[j, 0] * N1 * \
                                                                                  close.iloc[j] * (1 - fee1)

                        Account_one_temp[j, 0] = 0
                        # print('171216 2301 ', Account_one_temp[j , 0], open.iloc[j], close.iloc[j])
                        # Total
                        Account_one_temp[j, 3] = Account_one_temp[j, 2] + Account_one_temp[j, 1]
                        # Unit :
                        Account_one_temp[j, 4] = Account_one_temp[j, 3] / Initial
                        # MDD
                        Account_one_temp[j, 5] = min(Account_one_temp[j - 1, 5],
                                                     Account_one_temp[j, 4] / max(Account_one_temp[0:j - 1, 4]) - 1)
                        # Index
                        Account_one_temp[j, 6] = close.iloc[j] / close.iloc[j - 1] * Account_one_temp[j - 1, 6]
                        # MDD_I
                        Account_one_temp[j, 7] = min(Account_one_temp[j - 1, 7],
                                                     Account_one_temp[j, 6] / max(Account_one_temp[0:j - 1, 6]) - 1)
                else:
                    print('No signals but have positions ')
                    asdasdasd # this case should not happen

            else :
                # Not enough volumne in the market, we can only hold or do nothing
                Account_one_temp[j, 0] = Account_one_temp[j - 1, 0]
                # Stock Value : Calculate total value of stocks :?
                if Account_one_temp[j-1, 2] > 0 :
                    Account_one_temp[j, 2] = Account_one_temp[j, 0] * N1 * close.iloc[j]
                else :
                    Account_one_temp[j, 2] = Account_one_temp[j-1, 2]
                # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                Account_one_temp[j, 1] = Account_one_temp[j - 1, 1]
                # Total
                Account_one_temp[j, 3] =Account_one_temp[j, 2] + Account_one_temp[j, 1]
                # Unit :
                Account_one_temp[j, 4] = Account_one_temp[j, 3] / Initial
                # MDD
                Account_one_temp[j, 5] = min(Account_one_temp[j - 1, 5],
                                             Account_one_temp[j, 4] / max(Account_one_temp[0:j - 1, 4]) - 1)
                # Index
                Account_one_temp[j, 6] = close.iloc[j] / close.iloc[j - 1] * Account_one_temp[j - 1, 6]
                # MDD_I
                Account_one_temp[j, 7] = min(Account_one_temp[j - 1, 7],
                                             Account_one_temp[j, 6] / max(Account_one_temp[0:j - 1, 6]) - 1)

        # except :
        #     print('--- Error for account---')
        #     print(i2,j,)
        #     print('--- Error for account---')

        Account_one_L = pd.DataFrame(Account_one_temp, columns=Columns)  # 520*8
        # file_path=file_path0+'Account_one.csv'
        # Account_one.to_csv(  file_path  , encoding='utf-8', index=False)

        # print( 'Account_one results have been saved to:  ' +file_path )
        return Account_one_L

    def Account_one_short(self,data,  Ana_Data, Signals ) :
        # For Account_one_short, we also allow short positions
        # Signal states of (j-1, j) from (0,1),(1,1), (1,0)  to:
        # add: (-1,0) (-1,-1) (-1,1) (0,-1) (1,-1)


        # We use this account to adjust singal stock performance
        Columns=['Num_Max', 'Cash', 'Stock','Total','Unit', 'MDD', 'Index','MDD_I' ] # 8 items
        [x,y]=Ana_Data.shape
        Account_one_temp=np.zeros([x,8 ])
        Account_one= pd.DataFrame( np.zeros([x,8 ]),columns=Columns )  # 520*8

        ''' Length of Signal, Data, Ana_Data,Account_one are the same!  '''
        # print('Length of Data. is: ', data.shape) # 843,6
        # print('Length of Signal. is: ', len( Signals )) # 843
        # [x2,y2]=Account_one.shape # 843, 8
        # print('Length of Account_one. is: ', Account_one.shape ) # 843,8
        fee1=0.0025
        N1=100
        Initial=10000000
        # Notice that length of Account_one is x but last row of Account_one is as Account_one[x-1,:]

        # # print('Length of Account_one. is: ', Account_one.iloc[x-1,:] )
        # Account_one.iloc[x-1,:]=[0,Initial,0,Initial,1,0,1,0]
        # Account_one.iloc[x-2,:]=[0,Initial,0,Initial,1,0,1,0]
        for i2 in range( 100 ):
            #  from 0 to np.max(MA_x)-1=99
            Account_one_temp[ i2,:]=[0,Initial,0,Initial,1,0,1,0]
        # try:
        close=data['close']
        open=data['open']

        for j in range( 100-2, x  ):
            # From 100-2+1=99 to x-1
            # i2= 0,1,..., x-3

            # print('Length of Account_one. is: ',  j, len(Signals ) )
            # Signals[0][j+1] : Signal 的column "0" 的第j+1个值
            # print('XXXXX: ',  j,Signals[0][j+1], Signals[0][j+2] )
            if Signals[0][j-1]==1 and Signals[0][j-2]==0 :
            # Buy stocks signal in last day
                # No stock case
                if  Account_one_temp[j-1,0]==0 :
                    # Number of stocks :Calculate maximum number of stocks we can buy.
                    ''' 注意：以后仓位的动态调整可以从这里进一步设置  '''
                    # print( data['open'][j] )
                    # print( Account_one.iloc[j+1,2] )
                    # Account_one.iloc[j,1]= round( Account_one.iloc[j+1,2]/100/data['open'][j] ）
                    Account_one_temp[j,0]=  round(  Account_one_temp[j-1,1]/N1/open.iloc[j]*0.95  )   # data['open'][j]
                    # print( Account_one.iloc[j,0] )
                    # Stock Value : Calculate total value of stocks :
                    Account_one_temp[j,2]= Account_one_temp[j,0]*N1*close.iloc[j]  # data['open'][j]
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]= Account_one_temp[j-1 ,1] - Account_one_temp[j,0]*N1*open.iloc[j] *(1+fee1)
                    # Account_one_temp[j,0]*N1*open.iloc[j]
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]= min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6] # data['close'][j]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  '1st day Buy signal but already have positions ' )
            elif Signals[0][j-1]==1 and Signals[0][j-2]==1 :
            # Hold stocks
                # Number of stocks
                if  Account_one_temp[j-1,0]>0 :
                    Account_one_temp[j,0]=Account_one_temp[j-1,0]
                    # Stock Value : Calculate total value of stocks :?
                    Account_one_temp[j,2]= Account_one_temp[j,0]*N1*close.iloc[j]
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]=Account_one_temp[j-1,1]
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]= min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  'Hold signals but no positions ' )

            elif Signals[0][j-1]==0 and Signals[0][j-2]==1  :
            # Sell stocks
                # Number of stocks
                if  Account_one_temp[j-1,0]>0 :
                    Account_one_temp[j,0]=Account_one_temp[j-1,0]
                    # Stock Value : Calculate total value of stocks :
                    Account_one_temp[j,2]= 0
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]=Account_one_temp[j-1,1] + Account_one_temp[j,0]*N1*open.iloc[j]*(1-fee1)
                    Account_one_temp[j,0]=0
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]=  min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  'Sell signals but no positions ' )
            elif Signals[0][j-1]==0 and Signals[0][j-2]==0 :
            # No stocks
                if Account_one_temp[j-1,0]==0 :
                    Account_one_temp[j,0]=0
                    # Stock Value : Calculate total value of stocks :? todo : Open*Max_number
                    Account_one_temp[j,2]= 0
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]=Account_one_temp[j-1,1]
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]=  min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1 ,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1 ,6]) -1)
                else :
                    print(  'No signals but have positions ' )
                    print(  Account_one.iloc[j-1,0] )
                '''===# For short positions: add:(j-1,j)= (-1,0) (-1,-1) (-1,1) (0,-1) (1,-1) ===='''
            elif Signals[0][j-1]==0 and Signals[0][j-2]==-1 :
                # close short position at the open price
            # Sell stocks
                # Number of stocks
                if  Account_one_temp[j-1,0]>0 :
                    Account_one_temp[j,0]=Account_one_temp[j-1,0]
                    # Stock Value : Calculate total value of stocks :
                    Account_one_temp[j,2]= 0 # Account_one_temp[j-1 ,2] + Account_one_temp[j,0]*N1*( close.iloc[j-1]- open.iloc[j] )
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]=Account_one_temp[j-1,1] - Account_one_temp[j,0]*N1*open.iloc[j]*(1+fee1)
                    Account_one_temp[j,0]=0
                    Account_one_temp[j,2]=0
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]=  min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  'Sell signals but no positions ' )

            elif Signals[0][j-1]==-1 and Signals[0][j-2]==-1 :
                # Keep current short positions
                    # Hold stocks
                # Number of stocks
                if  Account_one_temp[j-1,0]>0 :
                    Account_one_temp[j,0]= Account_one_temp[j-1,0]
                    # Stock Value : Calculate total value of stocks :?
                    Account_one_temp[j,2]= Account_one_temp[j,0]*N1*close.iloc[j]*-1
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]= Account_one_temp[j-1,1]
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]= min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  'Hold signals but no positions ' )

            elif Signals[0][j-1]==1 and Signals[0][j-2]==-1 :
                # close short position at the open price and open long positions
                if  Account_one_temp[j-1,0]>0 :
                    Account_one_temp[j,0]=Account_one_temp[j-1,0]
                    # Stock Value : Calculate total value of stocks :
                    Account_one_temp[j,2]= 0
                        #  Account_one_temp[j-1 ,2] + Account_one_temp[j,0]*N1*( close.iloc[j-1]- open.iloc[j] )
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]=Account_one_temp[j-1,1] - Account_one_temp[j,0]*N1*open.iloc[j] *(1+fee1)
                    Account_one_temp[j,0]=0
                    Account_one_temp[j,2]=0
                    # New Number
                    Account_one_temp[j,0]= round( Account_one_temp[j,1]/N1/open.iloc[j]*0.95  )
                    # New Stock Value
                    Account_one_temp[j,2] =Account_one_temp[j,0]*N1*close.iloc[j]  # Account_one_temp[j,0]*N1*open.iloc[j]
                    # New Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]= Account_one_temp[j ,1] - Account_one_temp[j,0]*N1*open.iloc[j]*(1+fee1)
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]=  min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  'Sell signals but no positions ' )

            elif Signals[0][j-1]==-1 and Signals[0][j-2]==0 :
                # open short positions
                if  Account_one_temp[j-1,0]==0 :
                    # Number of stocks :Calculate maximum number of stocks we can buy.
                    ''' 注意：以后仓位的动态调整可以从这里进一步设置  '''
                    Account_one_temp[j,0]=  round(  Account_one_temp[j-1,1]/N1/open.iloc[j]*0.95  )   # data['open'][j]
                    # print( Account_one.iloc[j,0] )
                    # Stock Value : Calculate total value of stocks :
                    Account_one_temp[j,2]= -1*Account_one_temp[j,0]*N1*close.iloc[j]
                        # + Account_one_temp[j,0]*N1*( open.iloc[j]-close.iloc[j] )
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]= Account_one_temp[j-1 ,1]+Account_one_temp[j,0]*N1*( open.iloc[j])*(1-fee1)
                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]= min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6] # data['close'][j]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  '1st day short signal but already have positions ' )

            elif Signals[0][j-1]==-1 and Signals[0][j-2]==1 :
                # close long position at the open price and open short positions
                if  Account_one_temp[j-1,0]>0 :
                    Account_one_temp[j,0]=Account_one_temp[j-1,0]
                    # Stock Value : Calculate total value of stocks :
                    Account_one_temp[j,2]= 0
                    # Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]=Account_one_temp[j-1,1] + Account_one_temp[j,0]*N1*open.iloc[j]*(1-fee1)
                    Account_one_temp[j,0]=0
                    # New Number
                    Account_one_temp[j,0]= round( Account_one_temp[j,1]/N1/open.iloc[j]*0.95  )
                    # New Stock Value
                    Account_one_temp[j,2] =-1*Account_one_temp[j,0]*N1*close.iloc[j]  # Account_one_temp[j,0]*N1*open.iloc[j] #
                    # New Cash : Calculate ending value of cash = Cash_last + Cash-in - Cash-out
                    Account_one_temp[j,1]= Account_one_temp[j ,1] +Account_one_temp[j,0]*N1*open.iloc[j]*(1-fee1)

                    # Total
                    Account_one_temp[j,3]= Account_one_temp[j,2] + Account_one_temp[j,1]
                    # Unit :
                    Account_one_temp[j,4]= Account_one_temp[j,3]/Initial
                    # MDD
                    Account_one_temp[j,5]=  min(Account_one_temp[j-1,5] ,Account_one_temp[j,4]/max( Account_one_temp[0:j-1,4]) -1)
                    # Index
                    Account_one_temp[j,6]=close.iloc[j]/close.iloc[j-1]*Account_one_temp[j-1,6]
                    # MDD_I
                    Account_one_temp[j,7]=min( Account_one_temp[j-1,7], Account_one_temp[j,6]/max( Account_one_temp[0:j-1,6]) -1)
                else :
                    print(  'Sell signals but no positions ' )

        # except :
        #     print('--- Error for account---')
        #     print(i2,j,)
        #     print('--- Error for account---')

        Account_one= pd.DataFrame( Account_one_temp,columns=Columns )  # 520*8
        # file_path=file_path0+'Account_one.csv'
        # Account_one.to_csv(  file_path  , encoding='utf-8', index=False)

        # print( 'Account_one results have been saved to:  ' +file_path )
        return Account_one















