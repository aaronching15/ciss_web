# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
last update 180131 | since 20160216
180131 2225
我们想要重组数据抓取的结构，每日：
1，传统csv格式，保存个股的全历史日线数据 | 每日更新数据
2，用当日wind数据更新
    1，过去200个交易日日线数据
    2，按A股数据先弄



3，Initialization，利用传统的csv数据，生成过去200天的股价数据 .h5
    step1，读取指数，选定last 200个交易日
    step2，对所有个股取数。

171209 例如 170414,170417这2个交易日 0700.HK 没有 open，high，low，volume，amt数据，是因为这2天分别是港股的耶稣受难节 和复活节假期
    下一步 todo，如果昨日已经有数据，只更新一天的港股数据，也应该做到可以更新
171209 在stockPool 文件夹新建daily文件夹，把每个交易日计算的 signal 数据放进去，避免stockPool内东西太多
170510 对Wind数据下载做一个统一的规划管理。

交易日工作流程：
1，根据 股票，指数+ETF，下载wind-WSQ数据
    # todo 已经获取了当日所有股票/指数代码的交易数据
    # todo for all codes，判断 _updated.csv 是否存在，若不存在， 用 .csv copy 出 _updated.csv
    # todo 因为尽量不要去改变 .csv, 所有的操作如：判断数据问题，数据更新等，都在_updated.csv 层面进行

180130182025

2，根据WSQ数据，更新历史前复权数据，存入code_updated.csv ； 搜集errorCodes

3，根据errorCodes列表，下载wind-WSD历史数据，存入 code.csv

=====================================================================
Derived from
test_Wind2.py 中成功设置每次用wind抓取300条股价当日数据，且增加2个问题分析变量：
    temp_pd_Date ：抓取当日所有股价的数据。
    errorCodes ：将所有更新出问题的代码保存到统一的文件夹内

由于170505 1450开始遇到了Wind不能随心所欲地下载全历史日线级别的数据，因此有必要乘此机会，尽量提高自己
的数据管理水平。

# todo 170505 之前都是下载截止到最新日期的前复权数据，今后可能还要下载不复权数据
            WindData4 = Results2.GetWindData_NoAdj

获取整个股票代码列表的日线数据，csv格式
20160121
这个文件是用来测试如何抓取Wind API接口中的各种数据
测试 : rC_Data_Initial
之前资料来源 test_backup.py
===============================================
===============================================
逐日更新SP
Idea：每日数据下载后，用行业和概念的指数数据抓取强势指数，从强势指数里选出强势股票
    Assumption：建立动态调整机制，不一定要等到当日所有价格都更新后才计算买卖信号，也可以用昨天。
    Logic : 从行业和概念的角度计算强势股票，这样有可能会漏掉一些细分行业牛股，但是好在覆盖面广。
        是否需要对所有股票进行反复的计算呢？——能不能同时计算出明日肯定不会有买入信号的股票，计算出股票明日需要
        上涨多少才有可能出现买入信号？
    文件夹位置 |  C:\zd_zxjtzq\rCtrashes\\rC_Stockpool
        concept_wind , industry_4_wind 行业指数，单个指数的成分股列表
    todo-Future：以行业和概念指数作为基准，发掘有超额收益的个股，形成当日股票池
    Step 1：计算有买入信号的所有行业和概念指数；
    Step 2：计算强势行业指数内的有信号股票，
        Qs ：如果是弱势行业下跌减低，少数龙头股见底反弹的话，这样逐级筛选，可能只会找到已经涨了很多的股票？
        Ana：Wind四级行业分的还是比较细的，保险有2个指数（寿险：国寿，新华），882504,882505（多元保险太保，平安）
            银行有2个指数：882493.WI	多元化银行指数 | 882494.WI	区域性银行指数
            由于细分的真的很细，因此灵活性的角度，还是满足精选个股的需要
        Ana：201704,05两个月市场下跌期间，只有银行和保险股相对强势，
    Step 3: 如何分配权重？ 要考虑分散和集中的均衡。
        Stra: 单个行业或单个概念最多买入1个股票，或者不超过5%，小资金账户不超过10%
        Stra: 单个行业或单个概念最多有2个股票入选SP
    Step 4：如何排序？
        Stra：简单的想，是希望买入后有良好的财务基本面和市场交投，但不能简答用近期的情况来判断。
        Idea：近3个交易日的成交金额？近16,40,100个交易日的日均交易金额？
    Step 5：StockPool 维护更新：加入股票，维持股票，剔除股票。
        现有股票池分析：Hold，Sell
        新增股票列表：将当日要加入的股票加进去 Buy
        todo Idea 多系统，多股票组合情况下，如何避免反复计算 Signal ？

Idea : 新建 rC_

===============================================
'''
import unittest
import time
temp_time1 = time.strftime('%y%m%d%H%M%S',time.localtime(time.time()))
print( temp_time1 )

# todo 参数初始化，内部模块调用
import pandas as pd
import rC_Data_Initial as rC_Data
Results2 =rC_Data.rC_Database( '' )
# todo 外部 API
import WindPy as WP
WP.w.start()
''' Set parameters '''
MA_x = [3, 8, 16, 40, 100]
P_MA = [0 , 0, 0, 0, 0]  # 1 if Price>MA(x)
MA_up = [1, 1, 1, 1, 1]  # # of days we want such MA(x) to be Up
Path_Data = 'D:\\data_Input_Wind\\'

# todo Stock List 选择
file_path1 = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Py3\\Symbols\\'
file_List1 = [ 'All_Index_ETF.csv'  ] # All index and ETF in All_StockSymbols.csv
# file_List = [ 'All_StockSymbols.csv' , 'All_HK.csv','All_Nasdaq.csv']

SP_path = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Stockpool\\'
# file in SP_path : All_Index_ETF; concept_wind.csv ; industry_4_wind.csv ; all_A_Stocks_wind.csv ; 882421.WI etc.
# only HK
# SP_List = [  'HK_HSI_1711.csv','HK_HSCEI_1711.csv'] # 'All_Index_ETF.csv',
# ALL
SP_List = ['All_Index_ETF.csv', 'A-HK_300_Stocks_wind.csv', 'all_A_Stocks_wind.csv', 'industry_4_wind.csv', "industry_321_wind.csv", 'concept_wind.csv']
# SP_List = ["industry_321_wind.csv"]
# only Indexes
# SP_List = ['industry_4_wind.csv',  'concept_wind.csv']
# some_Symbols : 0,CU.SHF, 1,TF.CFE, 2,HSI.HI, ,'some_Symbols.csv'

# SP_List = [ 'all_A_Stocks_wind.csv', 'industry_4_wind.csv',  'concept_wind.csv']

# todo 重要 Input
temp_Date =  input('Please type in Date : ')
temp_LastDay = input('Please type in Pre Day : ')
# temp_Date =  '170523'
# temp_LastDay = '170522'

# todo 1，根据 股票，指数+ETF，下载wind-WSQ数据 ==================================================================
file_List = SP_List

for temp_f in  file_List :
    print('We are calculating Symbol List :', temp_f )
    # todo Get Wind-WSQ single day data
    # step 1 get SymbolList from : SL_path : path of SymbolList
    SL_path = SP_path  + temp_f
    # todo 170511
    temp_pd_Date = Results2.Get_temp_Date_Data( SL_path, temp_Date , Path_Data, temp_f )

# line 141-248
    # # todo items = "rt_date,rt_pre_close,rt_open,rt_high,rt_low,rt_last,rt_vol,rt_amt,rt_pct_chg,rt_mkt_cap,rt_float_mkt_cap"
    # # print( 'temp_pd_Date 170509 ')
    # # print( temp_pd_Date.head(5) )
    # # print(temp_pd_Date.tail(5))
    # #               RT_DATE  RT_PRE_CLOSE  RT_OPEN  RT_HIGH  RT_LOW  RT_LAST  \
    # # 000001.SZ  20170509.0          8.57     8.56     8.64    8.55     8.64
    # # todo 股票的情况 ============================
    # # todo before 171209 | if temp_f == 'all_A_Stocks_wind.csv' or temp_f == 'All_Index_ETF.csv' :
    # if temp_f in  file_List  :
    #     # todo Import  temp_pd_Date
    #     # temp_pd_Date = pd.read_csv( Path_Data + '\Wind_' + temp_f[:-4] + '_' + temp_Date + '_updated' + '.csv')
    #     # temp_pd_Date.index = temp_pd_Date['Unnamed: 0']
    #     # temp_pd_Date = temp_pd_Date.drop( ['Unnamed: 0'], axis =1 )
    #     # print( 'length of temp_pd_Date', len(temp_pd_Date.index )  )

    #     # todo 已经获取了当日所有股票/指数代码的交易数据
    #     # todo for all codes，判断 _updated.csv 是否存在，若不存在， 用 .csv copy 出 _updated.csv
    #     # todo 因为尽量不要去改变 .csv, 所有的操作如：判断数据问题，数据更新等，都在_updated.csv 层面进行
    #     import os
    #     # todo 170510 1131 要先看 _updated 存不存在，是否正常，因为 Get_errorCodes 只抓取 _updated
    #     i = 0

    #     # todo VIP Update csv file and Get errorCodes
    #     # 在有历史日线数据的情况下，更新csv数据
    #     # 注意：Update_WSQ_Get_errorCodes 只更新包括 code_updated 文件中的数据
    #     errorCodes = Results2.Update_WSQ_Get_errorCodes(temp_pd_Date, temp_f, Path_Data, temp_Date,
    #                                                     temp_LastDay)

    #     # for temp_code in temp_pd_Date.index :
    #         # temp_fileName = Path_Data + 'Wind_' + temp_code + '_updated' + '.csv'
    #         # if os.path.isfile(temp_fileName):
    #         #     errorCodes = errorCodes + [temp_code]
    #             # # print('170510 2049')
    #             # temp_fileName_0  = Path_Data + 'Wind_' + temp_code  + '.csv'
    #             #
    #             # import pandas as pd
    #             # if os.path.isfile(temp_fileName_0 ):
    #             #     temp_pd = pd.read_csv( temp_fileName_0 ,  header=None, sep=',', encoding='gbk')
    #             #     temp_pd.columns = temp_pd.loc[0, :]
    #             #     # delete row 0 :  drop([0], axis=0 )
    #             #     temp_pd = temp_pd.drop([0], axis=0 )
    #             #     temp_pd.to_csv( temp_fileName )
    #             # else :
    #             #     # 无历史数据的情况
    #             #     # brandly new code 171121 1826

    #     # todo Import errorCodes
    #     # errorCodes = pd.read_csv( Path_Data + 'Wind_' + temp_f + '_' + 'errorCodes' + '_' + temp_Date + '_updated' + '.csv' )
    #     # errorCodes.index = errorCodes['Unnamed: 0']
    #     # errorCodes = errorCodes.drop(['Unnamed: 0'], axis=1)
    #     # print('length of errorCodes', len( errorCodes.index))
    #     # print( errorCodes )

    #     # print('170510 2045  errorCodes  ')

    #     # todo 根据 errorCodes ，用 wind-WSD 下载 .csv 全历史数据
    #     if len( errorCodes.index ) >0 :
    #         temp_col = errorCodes.columns[0]
    #         for i in errorCodes.index :

    #             code = errorCodes.loc[ i,temp_col ]
    #             try:
    #                 # todo 170505 之前都是下载截止到最新日期的前复权数据，今后可能还要下载不复权数据
    #                 WindData3 = Results2.GetWindData(code, '', '', items='open,high,low,close,volume,amt,pct_chg', output=0)
    #                 # file_path = Results2.Wind2Csv(WindData3, Path_Data  , code)
    #                 # todo to check 170511
    #                 file_path = Results2.Wind2Csv_pd(WindData3, Path_Data, code)
    #                 # todo 170505 之前都是下载截止到最新日期的前复权数据，今后可能还要下载不复权数据
    #                 # WindData4 = Results2.GetWindData_NoAdj(code, '', '', items='open,high,low,close,volume,amt,pct_chg,total_shares', output=0)
    #                 # file_path = Results2.Wind2Csv_NoAdj(WindData4, file_path0, code)

    #                 print('The ', str(i), 'th code is ', code, 'still ', str( errorCodes.index[-1] - i), ' to go.')
    #             # python中date、datetime、string的相互转换  http://my.oschina.net/u/1032854/blog/198179
    #             #  WindData3.Times[1].strftime('%Y-%m-%d') # '2016-01-05'
    #             # time.mktime( WindData3.Times[1].timetuple()) # datetime.datetime(2016, 1, 5, 0, 0, 0, 5000) to 1451923200.0
    #             except:
    #                 print('The code ' + code + ' is not working ')
    # else :
    #     # case 'industry_4_wind.csv',  'concept_wind.csv'
    #     # todo Create temp_pd_Date
    #     temp_pd_Date = Results2.Get_temp_Date_Data( SL_path, temp_Date, Path_Data, temp_f)
    #     # todo Import temp_pd_Date from csv
    #     # temp_pd_Date = pd.read_csv(Path_Data + '\Wind_' + temp_f[:-4] + '_' + temp_Date + '_updated' + '.csv')
    #     # temp_pd_Date.index = temp_pd_Date['Unnamed: 0']
    #     # temp_pd_Date = temp_pd_Date.drop(['Unnamed: 0'], axis=1)
    #     print('length of temp_pd_Date', len(temp_pd_Date.index))
    #     import os
    #     i = 0
    #     for temp_code in temp_pd_Date.index:
    #         code =  temp_code
    #         try:
    #             # todo 170505 之前都是下载截止到最新日期的前复权数据，今后可能还要下载不复权数据
    #             WindData3 = Results2.GetWindData( code, '', '', items='open,high,low,close,volume,amt,pct_chg', output=0)
    #             # todo From WindData3 to DataFrame

    #             file_path = Results2.Wind2Csv_pd( WindData3, Path_Data  , code)

    #             # todo 170505 之前都是下载截止到最新日期的前复权数据，今后可能还要下载不复权数据
    #             # WindData4 = Results2.GetWindData_NoAdj(code, '', '', items='open,high,low,close,volume,amt,pct_chg,total_shares', output=0)
    #             # file_path = Results2.Wind2Csv_NoAdj(WindData4, file_path0, code)

    #             print('The ', str(i), 'th code is ', code, 'still ', len(temp_pd_Date.index) - i , ' to go.')
    #         # python中date、datetime、string的相互转换  http://my.oschina.net/u/1032854/blog/198179
    #         #  WindData3.Times[1].strftime('%Y-%m-%d') # '2016-01-05'
    #         # time.mktime( WindData3.Times[1].timetuple()) # datetime.datetime(2016, 1, 5, 0, 0, 0, 5000) to 1451923200.0
    #         except:
    #             print('The code ' + code + ' is not working ')
    #         i = i+1



# todo 2，根据 Wind 行业和概念指数，计算强势指数，寻找当日有买入信号的股票 ===========
# 170528
# Step 0 import ......
import numpy as np
path_Code = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Stockpool'
file_path0 = 'D:\\data_Input_Wind\\'
Path_Data = 'D:\\data_Input_Wind'
''' Set parameters '''
MA_x = [3, 8, 16, 40, 100]
P_MA = [0, 0, 0, 0, 0]  # 1 if Price>MA(x)
MA_up = [1, 1, 1, 1, 1]  # # of days we want such MA(x) to be Up
import rC_Data_Initial as rC_Data
code = ''
Results2 = rC_Data.rC_Database( code )
import rC_Portfolio_17Q1 as rC_Port
Output = rC_Port.rC_Portfolio( [], '2015-01-01', '2017-01-01', 25)
Columns_StockPool = ['Date', 'code', 'ifHold', 'B/S/H', 'W_Real', 'W_Ideal', 'Size', 'Growth', 'ST_bad',  'PnL_Last']

# temp_Date = '2017-05-26'
# temp_LastDay = '2017-05-25'
# todo      Step 1：计算有买入信号的所有行业和概念指数；
# s1.1 import 行业指数和概念指数 concept_wind , industry_4_wind
file_List = [ 'concept_wind' , 'industry_4_wind' ]
    # [ 'concept_wind' , 'industry_4_wind' ]
Sig_Ana_Sum = pd.DataFrame( columns=[ 'Signal',	'temp_Ana',	'Order', 'Symbol'] )

for f in  file_List :
    # todo Import index list
    sector_Wind3 = pd.read_csv( path_Code + '\\' + f + '.csv', encoding='gbk')
    sector_Wind3 = sector_Wind3.drop(['Unnamed: 0'], axis=1)
    print('170426 0929 sector_Wind  =', )
    print(sector_Wind3.head(5))

    symbolList = sector_Wind3['1']
    # print( 'symbolList' )
    # print( symbolList )
    StockPool = pd.DataFrame(np.zeros([len(symbolList), 10]), columns=Columns_StockPool)
    StockPool['ifHold'] = 0
    # todo Type 0 :Get signal and collect index list
    [temp_Sig_Ana, temp_Sort] = Output.Get_Sig_Ana_NoAmt(symbolList, Path_Data, temp_Date, temp_LastDay, StockPool)
    # symbolList 中代码的index 应该和 StockPool 里一样，应该要用 if StockPool.loc[j2, 'ifHold'] == 0 作为买入的前提条件
    # 输出 temp_Sig_Ana to csv
    temp_Sig_Ana.to_csv( path_Code + '\\' + 'daily' + '\\' + 'temp_Sig_Ana_' + f + '_'+ temp_Date + '.csv')
    print('temp_Sig_Ana has been saved to :', path_Code + '\\' + 'temp_Sig_Ana_' + f + '_'+ temp_Date + '.csv' )

    # todo Type 1 :Import signal and collect index list
    # temp_Sig_Ana = pd.read_csv( path_Code + '\\' + 'temp_Sig_Ana_' + f + '_'+ temp_Date + '.csv')
    # temp_Sig_Ana =  temp_Sig_Ana.drop(['Unnamed: 0'], axis=1)
    # print( temp_Sig_Ana.head(5) )
    # Signal	temp_Ana	Order	Symbol
    temp_Sig_Ana2 = temp_Sig_Ana[ temp_Sig_Ana['Signal'] ==1  ]
    for temp_index in temp_Sig_Ana2.Symbol :
        # todo 170522 注意： 持仓股需要从 'C:\zd_zxjtzq\rCtrashes\rC_Stockpool' 中获取
        # todo get constituent stocks
        print('temp_index  ', temp_index)
        sector_stocks = pd.read_csv( path_Code + '\\' + temp_index + '.csv', encoding='gbk')
        sector_stocks = sector_stocks.drop(['Unnamed: 0'], axis=1)
        code_List = sector_stocks['1']
        code_List.columns = [ 'symbolList' ]
        print( code_List.head(5) )

        StockPool = pd.DataFrame(np.zeros([len(code_List), 10]), columns=Columns_StockPool)
        StockPool['ifHold'] = 0
        #   todo  Step 2：计算强势行业指数内的有信号股票，
        #         Qs ：如果是弱势行业下跌减低，少数龙头股见底反弹的话，这样逐级筛选，可能只会找到已经涨了很多的股票？
        #         Ana：Wind四级行业分的还是比较细的，保险有2个指数（寿险：国寿，新华），882504,882505（多元保险太保，平安）
        #             银行有2个指数：882493.WI	多元化银行指数 | 882494.WI	区域性银行指数
        #             由于细分的真的很细，因此灵活性的角度，还是满足精选个股的需要
        #         Ana：201704,05两个月市场下跌期间，只有银行和保险股相对强势，
        [temp_Sig_Ana_sub, temp_Sort] = Output.Get_Sig_Ana_NoAmt( code_List, Path_Data, temp_Date, temp_LastDay, StockPool)
        print(  temp_Sig_Ana_sub.head(5))

        temp_Sig_Ana_sub2 = temp_Sig_Ana_sub[temp_Sig_Ana_sub['Signal'] == 1]
        # todo 对于单个有买入信号的指数，temp_Sig_Ana_sub2 选出了买入信号是 1 的股票,需要被加入原有的 List
        Sig_Ana_Sum = Sig_Ana_Sum.append( temp_Sig_Ana_sub2, ignore_index=True  )

# todo Sig_Ana_Sum 要删除重复的code ？？？？
Sig_Ana_Sum.to_csv( path_Code + '\\' + 'daily'+ '\\' + 'Sig_Ana_Sum'  + '_'+ temp_Date + '.csv')
# todo Idea: Sig_Ana_Sum 中重复次数越多的股票，说明对应的强势行业和概念越多，买入考虑时可能应给予更高的权重。
# drop_duplicates and duplicated now accept a keep keyword to target first, last, and all duplicates.
# The take_last keyword is deprecated, see here (GH6511, GH8505
Sig_Ana_Sum2 = Sig_Ana_Sum.drop_duplicates(['Symbol'])  # , keep='last'
Sig_Ana_Sum2.to_csv( path_Code + '\\' + 'daily'+ '\\' + 'Sig_Ana_Sum2'  + '_'+ temp_Date + '.csv')

# todo 需要在 SP 中 加入 Sig_Ana_Sum2 的信息。
Sig_Ana_Sum3 = pd.read_csv( path_Code +'\\' + 'daily'+  '\\' + 'Sig_Ana_Sum2'  + '_'+ temp_Date + '.csv')
Sig_Ana_Sum3 = Sig_Ana_Sum3.drop(['Unnamed: 0'], axis=1)
print( Sig_Ana_Sum3.head(5) )












temp_time2 = time.strftime('%y%m%d%H%M%S', time.localtime(time.time()))
print(temp_time1, '\n', temp_time2)

# todo ===================================================================================================

# todo 3，根据errorCodes列表，下载wind-WSD历史数据，存入 code.csv =================================
# file_List  = ??
# for f in  file_List :
#
#     file_path= file_path1 + f
#
#
#     Type='CNstocks' # Type='CNstocks' if we are getting Chinse stocks because the symbol might be not standarded here
#     # todo todo todo todo todo todo todo todo todo todo todo todo todo todo
#     SymbolList = pd.read_csv( file_path, header=None, sep=',' )
#
#
#     # SymbolList=str(data.values[1])
#     '''=================== Wind2Csv  =======================  '''
#     import rC_Data_Initial as rC_Data
#     DateStr0='20100101'
#     DateStr1='20160122'
#     file_path0='D:\\data_Input_Wind\\'
#
#     # 160505
#     for i in range( len(SymbolList) ):
#     # for i in range( 2100, len(SymbolList)  ) :
#         # if Type=='CNstocks':
#         #     code=str(SymbolList.values[i])[2:-2]
#         # else :
#         #     code = str(SymbolList.values[i])
#         code = str(SymbolList.values[i])[2:-2]
#         #　code=['159001.SZ']
#         # print(code)
#         Results2=rC_Data.rC_Database(code )
#         # WindData3=Results2.GetWindData(code, date_0=DateStr0, date_1=DateStr1 , items='open,high,low,close,volume,pct_chg', output=0)
#         # todo 起始时间选多少呢？？？？？？ 20160125
#         try :
#             # todo 170505 之前都是下载截止到最新日期的前复权数据，今后可能还要下载不复权数据
#             WindData3=Results2.GetWindData(code,'', '' , items='open,high,low,close,volume,amt,pct_chg', output=0)
#             file_path=Results2.Wind2Csv(WindData3 ,file_path0,code )
#             # todo 170505 之前都是下载截止到最新日期的前复权数据，今后可能还要下载不复权数据
#             # WindData4 = Results2.GetWindData_NoAdj(code, '', '', items='open,high,low,close,volume,amt,pct_chg,total_shares', output=0)
#             # file_path = Results2.Wind2Csv_NoAdj(WindData4, file_path0, code)
#
#             print('The ',str(i) , 'th code is ' , code,'still ',str(len(SymbolList)-i) , ' to go.' )
#         # python中date、datetime、string的相互转换  http://my.oschina.net/u/1032854/blog/198179
#         #  WindData3.Times[1].strftime('%Y-%m-%d') # '2016-01-05'
#         # time.mktime( WindData3.Times[1].timetuple()) # datetime.datetime(2016, 1, 5, 0, 0, 0, 5000) to 1451923200.0
#         except :
#             print('The code '+code + ' is not working ')

