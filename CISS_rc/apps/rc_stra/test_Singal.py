# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
20160312
计算单个代码的策略回报和输出相应结果。
测试 : rC_Stra_MAX.py
之前资料来源 test_backup.py
===============================================
'''

'''
000300.SH | 000016.SH |  000905.SH |  159901.SZ | 159902.SZ | 159915.SZ | 510050.SH | 510300.SH
518880.SH | 511880.SH |511010.SH | 510900.SH | 510180.SH | 511220.SH | 159939.SZ | HSI.HI
HSCEI.HI | N225.GI | JKSE.JK | SPX.GI | IXIC.GI | FTSE.L | FCHI.CI | GDAXI.GI | SX5P.GI
'''
file_path0 =  'D:\\data_Input_Wind\\'
''' Test for single stock  '''
# for i in range( len(SymbolList) ) :
# 884228.WI # 森林浆果，蓝莓，樱桃，柠檬，草莓，树莓
# # some_Symbols : 上海铜，国债期货5y，恒生指数 0,CU.SHF, 1,TF.CFE, 2,HSI.HI,
# 股票 600036.SH 600470.SH 600887.SH
code='000300.SH'  # 159901.SZ todo 测试专用代码 633336.SH
Stra = 'dif-M'
date = '170808'
ma_len = 100
# for code in ['510300.SH','510050.SH','510500.SH','159901.SZ','159915.SZ','600036.SH','600470.SH','600887.SH','300377.SZ' ] :
# for code in [ '002186.SZ'] :
for code in [ 'CBA00203.CS'] :
    # GOOG.O
    # Wind行业或主题指数 882485.WI 医疗保健设备指数
    #  ADSK.O '600638.SH' 159915.SZ # 600519.SH 002797.SZ 002736.SZ 600608.SH 600556.SH
    # 161001 该更新股票列表了， 002797 第一创业这些股票都没在股票池里
    # 0001.HK  3968.HK 3336.HK 0857.HK 2899 1299.HK  1359.HK 0579.HK 2628.HK 0805.HK 0665.HK 1211.HK 0874.HK
    # 0220.HK 0493.HK
    # 中规中矩的股票 601238.SH 2238.HK
    if_CN=1 # 如果是中国股票选1 ，如果是美国股票选
    if_HK = 0
    if code[-2:] == 'HK' :
        if_HK= 1
    if_min=0
    ifPlot= 1  # 这个决定是否要画图输出
    Leverage= 0.95
    # return_FixedIncome=0.00
    # return_FixedIncome_s=0.00
    return_FixedIncome=0.05
    return_FixedIncome_s=0.025
    # code=code[2:11]
    # 注意：Index160114 和 Index160114 里边数据格式不是都是 ['600036.SH'],导致 code=code[2:11] 不能正常识别
    # code=code[2:-2]
    print(code)

    import time
    ''' Set parameters '''
    MA_x=[3, 8, 16,40,100]
    P_MA=[0 , 0,0,0,0] # 1 if Price>MA(x)
    MA_up=[1,1,1,1,1] # # of days we want such MA(x) to be Up

    # ''' Test for all stocks  '''
    # # We first import stock symbols
    # import pandas as pd
    #
    # file_path0 = 'C:\\Users\\ruoyu.cheng\\rC_quant_qq\\rC_Matlab\\'
    # # file_path0 = 'C:\\qq_rC\\rC_Matlab\\'
    # # C:\qq_rC\rC_Matlab
    #
    # # file_path= file_path0 + 'Index160114.csv'
    # file_path= file_path0 + 'ETF160114.csv'
    # # file_path= file_path0 + 'All_StockSymbols.csv'
    # SymbolList = pd.read_csv( file_path, header=None, sep=',')
    # # SymbolList=str(data.values[1])

    ''' Import data from csv by Wind_Matlab '''
    t0=time.time()
    # code='510050.SH' # 159915.SZ
    time0= '2012-07-01'
    time1=  '2015-12-31'
    import rC_Stra_MAX as rC
    Results=rC.rC_TradeSys(code, time0 , time1 , file_path0 )

    if if_CN == 1:
        # Chinese stocks
        data = Results.ImportFiData_csv_WindPy(code,file_path0)
        # 160708 解决日期的序列问题
        # print( data2['date'].head(1).values + '   ,   ' + data2['date'].tail(1).values)
        # ['1996-03-12   ,   2016-07-05']
        if if_HK == 1 :
            # 可能存在部分交易日A股有交易，而港股休市无交易的情况，需要调整数据。
            data = data.dropna( axis=0 )
        else :
            data = data.dropna(axis=0)

    else:
        # ImportFiData_csv_WindPy_US
        data = Results.ImportFiData_csv_WindPy_US(code)
        # 171213 0022 注意，GOOG.O 的 amt都是nan

        # US stocks
        # data = Results.ImportFiData_csv_WindPy_US(code)
    if  if_min==1 :
        file_path0 = 'F:\\rC_Py3_Output_min'
        #F:\rC_Py3_Output_min
        data = Results.ImportFiData_csv_WindPy_min(code, file_path0 )

    # 160709 注意，这里的日期文本已经是对应从该股票上市以来的值
    # print( data['date'].head(1).values + '   ,   ' + data['date'].tail(1).values)
    [x,y]=data.shape
    print(data.shape)
    print(data[-5:])
    t1=time.time()
    print('Time spent:', t1-t0)

    t0=time.time()
    print( '======AnalyticFiData========' )
    # print( data['close']) # index start with 594 but not 1
    Ana_Data=Results.AnalyticFiData_2(data,MA_x, P_MA, [1,1,1,1,1]  )


    # I found rC. can directly use variables in rC_Stra_MAX_160114
    file_path=file_path0+'Output_Ana_Data_' +code + '.csv'
    Ana_Data.to_csv(  file_path  , encoding='utf-8', index=False)
    # ??? the first 2 rows of Ana_Data is all 0, why??
    print( 'File has been saved to', file_path )

    t1=time.time()
    print('Time spent:', '%.2f' %(t1-t0) )

    t0=time.time()
    print( '======Stra_MA_Signals========' )
    # Signals=Results.Stra_MA_Signals_2(Ana_Data)
    # 160507 Stra_MA_Signals_a_vol(self, Ana_Data,data  ) data ：DATE	OPEN	HIGH	LOW	CLOSE	VOLUME	AMT	PCT_CHG

    # todo primary stra
    # todo 171217 改过Account_one_L， Stra_MA_Signals_a_vol_4Port
    # Signals = Results.Stra_MA_Signals_a_vol_4Port(Ana_Data, data)
    Signals=Results.Stra_MA_Signals_a_vol(Ana_Data,data)

    # print( '% of days the portfolio had holdings:  ' + str( sum(Signals[0][:])/len(Signals) ) )

    # Signals=Results.Stra_MA_Signals(Ana_Data)
    print( 'Signals in last 5 days is:\n', Signals[-5:] )
    file_path=file_path0+'\\'+'Signals_' +code +'.csv'
    # Signals  <class 'pandas.core.frame.DataFrame'>
    Signals.to_csv(  file_path  , encoding='utf-8', index=False)
    t1=time.time()
    print('Time spent:',  '%.2f' %(t1-t0) )


    t0=time.time()
    print( '======Account_Built========' )
    # Account_one_short 可以兼容 Account_one，当做Account_one来用
    # todo 171217 改过Account_one_L， 解决了 模拟净值都突然掉到0，但Stra_MA_Signals_a_vol 没问题的情况 。
    Account_one= Results.Account_one_L(data, Ana_Data, Signals, Leverage )

    # Account_one= Results.Account_one_short(data,  Ana_Data, Signals )
    # plot a picture
    # Account_one['Unit','Price'].plot()
    print(code, "Leverage=", Leverage,'code', code , 'date', str(date) ,'ma_len', str(ma_len) )
    # print ( Account_one.iloc[len(Account_one)-1,:] )
    print ("Index:", Account_one.iloc[len(Account_one)-1,6],"|", Account_one.iloc[len(Account_one)-1,7]  )
    total_Unit_Return= Account_one.iloc[len(Account_one)-1,4]
    print ("Unit:", Account_one.iloc[len(Account_one)-1,4] ,"|", Account_one.iloc[len(Account_one)-1,5] ,"|", str( sum(Signals[0][:])/len(Signals) ))
    annual_Unit_Return=total_Unit_Return**(252/x)
    annual_Port_Return=total_Unit_Return**(252/x) +(1-Leverage)*return_FixedIncome + (1-sum(Signals[0][:])/len(Signals))*0.9*return_FixedIncome_s
    print ("Annual return in past " ,  round((x/252)*100)/100 , "  years is : " , round(100*annual_Port_Return)/100  )

    # Stra = 'dif-M'  date = '170808' ma_len = 16
    file_path=file_path0+'\\'+'Account_one_' +code + '_' + str(date) + '_' + str(ma_len) +'.csv'
    # Account_one  <class 'pandas.core.frame.DataFrame'>
    Account_one.to_csv(  file_path  , encoding='utf-8', index=False)
    # import matplotlib.pyplot as plt
    # plt.plot(Account_one['Unit'] )
    # plt.title("test numbers")
    # plt.show()
    print('File has been saved to :' , file_path )

''' todo : MDD has no value'''
t1=time.time()
print('Time spent:',  '%.2f' %(t1-t0))

t0=time.time()
# # todo
# # 备用的测试其他策略Signal的部分
# # Signals=Results.Stra_MA_Signals_2(Ana_Data)
# Signals_a=Results.Stra_MA_Signals_a(Ana_Data)
# Account_one_a= Results.Account_one= Results.Account_one_L(data, Ana_Data, Signals_a, Leverage )
# # print('% of days the portfolio had holdings.  '+sum(Signals_a[0])/len(Signals_a[0])  )
# print ("Unit_a:", Account_one_a.iloc[len(Account_one_a)-1,4],"|", Account_one_a.iloc[len(Account_one_a)-1,5] ,"|", str( sum(Signals_a[0][:])/len(Signals_a) ) )
#
# # Signals=Results.Stra_MA_Signals_2(Ana_Data)
# Signals_b=Results.Stra_MA_Signals_b(Ana_Data)
# Account_one_b= Results.Account_one= Results.Account_one_L(data, Ana_Data, Signals_b, Leverage )
# # print('% of days the portfolio had holdings.  '+sum(Signals_b[0])/len(Signals_b[0])  )
# print ( "Unit_b:", Account_one_b.iloc[len(Account_one_b)-1,4],"|", Account_one_b.iloc[len(Account_one_b)-1,5] ,"|", str( sum(Signals_b[0][:])/len(Signals_b) ) )



''' output a plot'''
if ifPlot==1 :
    import matplotlib.pyplot as plt
    plt.figure(figsize=(16,9), dpi=80 ) # figsize=(18,7), dpi=80
    # 正常版画图
    p1 = plt.subplot(221)
    plt.sca(p1)
    plt.plot( [x for x in range(len(Account_one))] , Account_one.iloc[:,4], 'r')
    plt.xlabel('Time')
    plt.ylabel('Unit')
    plt.title(code + '  Unit_a price')
    plt.legend(loc="upper left") # loc=3 equals to (loc="upper left")
    # plt.show()
    p2 = plt.subplot(222)
    plt.sca(p2)
    plt.plot( [x for x in range(len(Account_one))] , Account_one.iloc[:,5], 'r')
    plt.xlabel('Time')
    plt.ylabel('MDD')
    plt.title('Unit_a MDD')
    plt.legend(loc="upper left")
    p3 = plt.subplot(223)
    p4 = plt.subplot(224)
    plt.sca(p3)
    plt.plot( [x for x in range(len(Account_one))] , Account_one.iloc[:,6], 'r')
    plt.xlabel('Time')
    plt.ylabel('Unit')
    plt.title('Unit_a price')
    plt.legend(loc="upper left")
    plt.sca(p4)
    plt.plot( [x for x in range(len(Account_one))] , Account_one.iloc[:,7], 'r')
    plt.xlabel('Time')
    plt.ylabel('MDD')
    plt.title('Unit_a MDD')
    plt.legend(loc="upper left")
    plt.show()
#







# 简易版画图，解决font manager问题
# p1 = plt.subplot(221)
# plt.sca(p1)
# plt.plot( [x for x in range(len(Account_one_a))] , Account_one_a.iloc[:,4], 'r')
# p2 = plt.subplot(222)
# plt.sca(p2)
# plt.plot( [x for x in range(len(Account_one_a))] , Account_one_a.iloc[:,5], 'r')
# p3 = plt.subplot(223)
# p4 = plt.subplot(224)
# plt.sca(p3)
# plt.plot( [x for x in range(len(Account_one_a))] , Account_one_a.iloc[:,6], 'r')
# plt.sca(p4)
# plt.plot( [x for x in range(len(Account_one_a))] , Account_one_a.iloc[:,7], 'r')
# plt.show()

''' '''
# # Signals=Results.Stra_MA_Signals(Ana_Data)
# file_path0 = 'C:\\rC_Py3_Output'
# file_path=file_path0+'\\'+'Signals_' +code +'.csv'
# Signals.to_csv(  file_path  , encoding='utf-8', index=False)
# t1=time.time()
# print('Time spent:',  '%.2f' %(t1-t0) )
#
#
# t0=time.time()
# print( '======Account_Built========' )
# # Account_one_short 可以兼容 Account_one，当做Account_one来用
# # Account_one= Results.Account_one(data,  Ana_Data, Signals )
# Account_one= Results.Account_one_short(data,  Ana_Data, Signals )
# # plot a picture
# # Account_one['Unit','Price'].plot()
# print ( Account_one.iloc[len(Account_one)-1,:] )
# file_path0 = 'C:\\rC_Py3_Output'
# file_path=file_path0+'\\'+'Account_one_' +code +'.csv'
# Account_one.to_csv(  file_path  , encoding='utf-8', index=False)
# # import matplotlib.pyplot as plt
# # plt.plot(Account_one['Unit'] )
# # plt.title("test numbers")
# # plt.show()
# print('File has been saved to :' , file_path )
#
# ''' todo : MDD has no value'''
# t1=time.time()
# print('Time spent:',  '%.2f' %(t1-t0))









