# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"
'''
===============================================
急迫的核心Bug：
    170226
    将 Account_Stocks and Account_Stocks2 合并
    
    161225
    核心bug ： 只用1只股票回测时，净值表现弱于 test_Signal 方式回测结果
        已确定：rC_Stra_MAX 中的策略是一样的，问题较可能出现在rC_Portfolio_16Q4 中

    161219
    # todo 161219_0128 初步发现，按照从小到大的排，优先买入的都是涨幅较少的股票，结果就是波动贴近指数
    # temp_Sort = np.argsort(temp_Sig_Ana[1].values, axis=0)
    temp_Sort = np.sort(temp_Sig_Ana[1].values, axis=0) # 从大到小

    161219 :
    原来长期以来的问题出在了 .sub  方式会把pd的每一行减去 第temp行！
    L1125: Account_Stocks.ix[temp] = Account_Stocks.ix[temp].sub(  Account_Stocks.ix[temp] , axis= 1 )

    161212 :
    问题 ： 2016-05-09 出现负数的持仓数量
        5-9只卖出了 3个股票，但TradingBook里只有一个股票的数量和5-6是匹配的，其他2个一个少于持仓，一个大于持仓
        这说明：
        1，计算交易时，引用的持仓数量不对
        2，持仓更新时，引用的交易数量也不对

    已解决 ： 2016-5-4成本价格不对的问题得到解决，但是 2016-05-06出现负市值的问题也已经解决，
    用一个很简单的办法解决了匹配日期的问题：
    但是， Get_Date_Index(data2 ,temp_Date) 这个就没有用了 Get_Date_Index_old 也没用了
    直接用  Output.Get_Price_Ret( temp_Symbol, temp_Date,Path_Data  ) 可以匹配数据

    161211 不知道L930中 temp_DF 这些是否对Account有影响

    161126 1054
        发现在 2016-04-21 这一天持仓股出现了负数，由于持仓股都是创业板股票，且创业板指数在 4-20出现了 -5%的大跌，
        因此估计是 4-20的收盘价触发了卖出的信号，但是不知道为什么会有负数


    161112_1825
        每个新temp_Date 对持仓做一个初始化操作

    161106_2220 已经对所有 Account_Stocks['Num'] 有定义的地方做了正值得判断，但结果还是全都是负数，这个太奇怪了，
        说明Update_Account的流程还是有问题，但Trading_Book_1611062222 内的交易数据格式都是对的，Number也都是正数
        本周对 Update_Account 做一次大改

    161102
    1， 改进关于temp|index_Account_S 对 Update Account list 的影响
    2， 改变当前Account_Stock and Account_Sum 有负数的问题。
        Account_Stocks['Num']

    Idea :StockPool 增加行业，概念主题，市值，roe；在近1,5,20天累计收益和风险的聚类分析时，以行业，或者概念主题筛选
        强势，或者满足条件的板块。

初始化：
    1，创建现金和总股票账户：Items： Columns = ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']
    2，创建股票账户：  Columns = ['Stock-1',..., 'Stock-N' ]
    3，每笔交易记录，实现收益:创建交易记录：  Columns = ['Account','Date','Symbol','Buy/Sell','Price','Number','AveCost','Fees','ProfitReal' ]。
    4，股票池：包括持仓股票和非持仓股票 StockPool List
循环：
    对于过去N/605个交易日(有分析数据的交易日500个)的股票池中的20个股票:
        1, 对股票池内股票，先对持仓股分析/计算买卖信号，得到继续持有的股票和N个卖出信号
        2，对于非持仓股票，计算买入信号，根据规则排序，并选出最佳的N个
        3，执行买入交易，交易后计算收盘持仓的收盘市值，现金水平和市值，更新股票账户和总账户
        4, (未来)对于有非持仓股票列表，每个交易日有一个规则可以更新！

0,160721 数据结构
    1，每个交易日应该有一个输出的持仓表格和交易明细，这样就可以持续的更新持仓矩阵和交易矩阵，并且维持较小的数据量。

0，160709 确定  # print( data2['date'].head(1).values + '   ,   ' + data2['date'].tail(1).values)
                # ['1996-03-12   ,   2016-07-05']
    可以获得股票的时间长度

0，160609 默认股票池的股票都是统一更新到最近一个交易日，默认买卖信号也是统一的，如过去500个交易日。
    注意：未来的目标是让每一个股票的每列买卖信号，股价要和一个时间一一对应，因为有的股票是有停牌期的，而且不同股票的股价
        序列是不一样的。
    初始化：
        1，创建现金和总股票账户：Items： Columns = ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']
        2，创建股票账户：  Columns = ['Stock-1',..., 'Stock-N' ]
        3，
    循环-日常维护：
        1, 对于过去605个交易日(有分析数据的交易日500个)的股票池中的20个股票，
        2，对昨日持仓股中有卖出信号的进行卖出操作，完成后更新股票账户和总账户，计算出最多可以买入的股票数量N1和最多可以
            卖出持仓再买入的股票数量N2和清单。
        3，根据当前交易日之前的100个交易日，计算出有买入信号的股票，并进行最优化排序，得到一个暂时的股票排名列表。
        4, 根据可买入的股票清单，依次买入股票，完成后更新股票账户和总账户。

    Variablea：
        从Start date开始，往前倒105个单位，然后直到End date，跑循环。
        len=6208
        sd=Start date
        ed=end date
        变量和参数：
        Account 股票账户：现金，最多N个股票持仓(数量，市值)

程序说明部分:
    功能：历史回测：
        160508 这个程序是为了对一揽子股票进行操作，不只是统计一段时间
        160507 构建区间统计工具，比如 统计各年度，半年，季度的涨幅最大的股票。
        输入： 统计一段时间【120501,140505】对应一个股票列表里的区间统计
    功能·实盘维护：
        根据上一次跟新的持仓，运行计算下一个交易日的最佳买/卖/持有的交易计划
            需要综合考虑频繁调仓的成本：流动性，交易成本。
    输出结果
        最新理想投资组合，最优化股票清单排名（得分排名，是否入选）
    Last update : 16609 160505 160507

    分析：
    1，现在的股票Signal的1/0应该可以直接拿过来用，Signal的数据结构是不等的，有的可能290个，有的可能4000多个。
        比如统一的数据日期是6208的长度，而到了分析数据买卖信号和Account，可能就是290或者1200的长度。
            初步思路：某一时间(假设是最新)，给定一个代码列表和对应的目标权重列表，在组合账户中
            输入：； 逻辑和计算过程；    输出
    2, Always keep&calculate 2 set of holding lists, one is the real world list and the other one is ideal list without considering
        liquidity and transaction cost

160530 2209
modules:
data,Ana_Data, Signals, Account
Idea：
历史回测：统一吧股价的数据：最近1000个交易日导入，然后导入同时间的Signals，用统一的矩阵来计算Account和是否买卖
实盘：每个交易日计算持仓股的交易信号，以及股票池的交易信号
////////////////////////////
代码如下:
>>> def func(y=2, x):
    return x + y
SyntaxError: non-default argument follows default argument

【错误分析】在C++,Python中默认参数从左往右防止，而不是相反。这可能跟参数进栈顺序有关。
复制代码 代码如下:

>>> def func(x, y=2):
    return x + y
>>> func(1)
3
===============================================
'''
# file_path = os.getcwd()
# file_path0='C:\\rC_Py3_Output\\'
# 160921 Path_Data Path_TradeSys  for Portfolio

# Path_Data = 'F:\\rC_Py3_Output'
# Path_TradeSys = 'F:\\rC_Py3_Output\\TradeSys_0'

import numpy as np
import pandas as pd

class rC_Portfolio():
    # todo ==== Level 1 ========================================================================
    # 类的初始化操作 , N is the maximum number of different stocks we can hold in account
    def __init__(self, symbolList, start, end,N ):
        self.symbolList = symbolList
        self.start = start
        self.end = end
        self.N = N

    def Generate_Portfolio(self, Dates,Initial, MaxN,symbolList  ):
        '''
        感觉完全可以同时给 Portfolio_Live 使用
        初始化 Account_Sum ,  Account_Stocks, Trading_Book, StockPool
        Part 1 : Initialize our portfolio
        Last update 170415 1844 | Since 170403 1318
        '''
        # todo P1 Step1 初始化,创建总股票账户：Account_Sum
        Len_time = len(Dates)
        # Notice that Dates has type of nd.array
        # Account_Sum： Columns = ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']
        Columns_Sum = ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']  # 8 items
        Index_Sum = Dates
        Account_Sum = pd.DataFrame(np.zeros([Len_time, 8]), columns=Columns_Sum, index=Index_Sum)
        # The first 100 entries are just empty
        # for i2 in range( 100 ):
        i2 = Index_Sum[0]
        Account_Sum.loc[i2, 'Total_Cost'] = 0.00
        Account_Sum.loc[i2, 'Cash'] = Initial
        Account_Sum.loc[i2, 'Stock'] = 0
        Account_Sum.loc[i2, 'Total'] = Initial

        Account_Sum.loc[i2, 'Unit'] = 1
        Account_Sum.loc[i2, 'MDD'] = 0.00
        Account_Sum.loc[i2, 'Index'] = 1
        Account_Sum.loc[i2, 'MDD_I'] = 0.00
        # todo P1 Step2 初始化,创建股票账户 Account_Stocks
        # 161023
        # todo 170226 1621 Step 1 Put all Account_Stocks2 into Account_Stocks
        Columns_Stocks = ['Num', 'AveCost', 'LastPrice', 'TotalCost', 'MV', 'PnL', 'PnL_Pct', 'W_Real',
                          'W_Ideal','Date', 'code']  # 11 items
        Account_Stocks = pd.DataFrame(np.zeros([MaxN, 11]), columns=Columns_Stocks)
        Account_Stocks['Date'] = Dates[0]
        Account_Stocks['code'] = ''
        # Columns_Stocks = ['Num','AveCost',  'LastPrice','TotalCost', 'MV',  'PnL', 'PnL_Pct', 'W_Real', 'W_Ideal']  # 9items
        # Account_Stocks = pd.DataFrame(np.zeros([MaxN, 9]), columns=Columns_Stocks)
        # Account_Stocks2 = pd.DataFrame(np.zeros([len(symbolList), 2]), columns=Rows_Stocks)
        # todo todo VIP 把 dataframe 的前2列从 int 格式变成string
        # Account_Stocks2[['Date', 'code']] = Account_Stocks2[['Date', 'code']].astype(str)

        # todo 这里我是想对于每一条个股的持仓记录，都有一个交易日期和一个股票代码对应，这样来避免未来每个交易日频繁
        # todo P1 Step3 初始化,创建交易明细记录 Trading_Book
        Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        Trading_Book = pd.DataFrame(  columns=Columns_Trade)
        # Trading_Book = pd.DataFrame(np.zeros([100, 9]), columns=Columns_Trade)

        # Trading_Book 的具体大小我们不知道，因此一开始只有100长度
        index_Trading_Book = 0
        # index_Trading_Book=5 means only 6 records in Trading_Book

        # todo P1 Step3 初始化,创建股票池 StockPool , 根据input: symbolList
        Columns_StockPool = ['Date', 'code', 'ifHold', 'B/S/H', 'W_Real', 'W_Ideal', 'Size', 'Growth', 'ST_bad',  'PnL_Last']
        # Keyword for future: IPO ,
        StockPool = pd.DataFrame(np.zeros([len(symbolList), 10]), columns=Columns_StockPool)
        # todo todo VIP 把 dataframe 的前2列从 int 格式变成string
        # Pandas: change data type of columns
        StockPool[['Date', 'code', 'B/S/H']] = StockPool[['Date', 'code', 'B/S/H']].astype(str)

        # todo Generate temp_Sig_Ana
        temp_Sig_Ana = pd.DataFrame(np.zeros([len(symbolList), 4]), columns=['Signal', 'temp_Ana', 'Order', 'Symbol'])

        # print(Dates[0])
        for i in range(len(symbolList)):
            StockPool.loc[i, 'Date'] = Dates[0]
            # print(StockPool['Date'][i])
            StockPool.loc[i, 'code'] = symbolList[i]
            # print(StockPool['code'][i])
            StockPool.loc[i, 'ifHold'] = 0
            StockPool.loc[i, 'B/S/H'] = 'H'
            StockPool.loc[i, 'W_Real'] = 0
            StockPool.loc[i, 'W_Real'] = 0
            if MaxN > 0 :
                StockPool.loc[i, 'W_Ideal'] = 1 / (MaxN)
            else :
                StockPool.loc[i, 'W_Ideal'] = 1
            StockPool.loc[i, 'Size'] = 0
            StockPool.loc[i, 'Growth'] = 0
            StockPool.loc[i, 'ST_bad'] = 0
            StockPool.loc[i, 'PnL_Last'] = 0

            # Before: StockList=[]   # 持仓股票 ; StockPool =symbolList  # 非持仓股票
            # 相关变量 ： index_StockList ;index_StockPool =0 ;w_SL # 持仓股票持仓比例 ,w_SP ,# 非持仓股票持仓比例
            # Date_Index_SP = np.zeros([1,len(symbolList)]) # 非持仓股票 对应交易日的数据的索引
        print('Portfolio Initialization has done.')

        return Account_Sum ,  Account_Stocks, Trading_Book, StockPool,temp_Sig_Ana

    def Get_Portfolio_info(self,temp_Port ,Log_Sys,path_Sys ,date_LastUpdate_New):
        # todo 171208 Ans: 原来是excel打开HSI.HI时 日期格式错了，2017-12-08 to 2017\08\08| Qs :新增港股HSI指数数据的判断时，temp_List = data2[data2['date'] <= end_Date] 会导致截取的日期序列不对，考虑改成定位index
        # todo 170401 这个module 主要是用来获取单个Portfolio 的信息,以更新Portfolio
        # start_Date ,indexName, symbolList, Dates, path_Input, Path_Port, Leverage, MaxN
        # todo 如果每次都重新导入 rC_Portfolio_17Q1，不知道会不会消除各个组合互相引用持仓数据的问题
        print('The portfolio we want to update is :')
        print(temp_Port)
        temp_Portfolio = temp_Port
        # path_Portfolio = 'D:\data_Output\Sys_rC1703_1703\Port_SZ50_000300.SH_170306_40_20_0.95'
        # path_Portfolio = path_Sys  # + '\\' + temp_dir
        temp_Log_Name = 'Log_' + temp_Portfolio

        # todo Part 1 获取 Log_Portfolio
        Log_Portfolio = pd.read_csv(path_Sys + '\\' + temp_Log_Name + '.csv')
        Log_Portfolio.index = Log_Portfolio[Log_Portfolio.columns[0]]
        Log_Portfolio = Log_Portfolio.drop(Log_Portfolio.columns[0], axis=1)
        print(Log_Portfolio.columns)
        print(Log_Portfolio.index)
        print('Import Log_Portfolio Done . ', Log_Portfolio.loc['Index_Name', 'value'],
              Log_Portfolio.loc['Unit', 'value'])
        # todo 根据Log_portfolio 生成 temp_dir ????
        indexName = Log_Portfolio.loc['Index_Name', 'value']
        date_Start = Log_Portfolio.loc['date_Start', 'value']
        date_Initial = Log_Sys.loc['InitialDate', 'value']
        str_MaxN = Log_Portfolio.loc['MaxN', 'value']
        str_Leverage = Log_Portfolio.loc['Leverage', 'value']
        temp_dir = 'Port_' + temp_Portfolio + '_' + indexName + '_' + date_Initial + '_' + str_MaxN + '_' + str_Leverage

        # print( Log_Portfolio.loc[ 'Index_Name','value' ] )
        # todo 获取我们要更新组合的参数
        # Dates come from Index_Name， start_date=lastUpdate, end_date= CurrentDate
        Index_Name = Log_Portfolio.loc['Index_Name', 'value']
        print('Index_Name ', Index_Name )
        # symbolList comes from Portfolio_Name,path_Symbol
        Portfolio_Name = Log_Portfolio.loc['Portfolio_Name', 'value']
        MaxN = Log_Portfolio.loc['MaxN', 'value']
        MaxN = int(MaxN)
        Leverage = Log_Portfolio.loc['Leverage', 'value']
        Leverage = float(Leverage)
        # Dates come from Index_Name， start_date=lastUpdate, end_date= CurrentDate
        date_Start = Log_Portfolio.loc['date_Start', 'value']
        # todo 170313 如果每次都是对全部组合从同意交易日开始更新， 用 Log_Sys.loc['date_LastUpdate', 'value']
        # todo 如果每个组合最近一次更新时间不同，那么用 date_LastUpdate = Log_Portfolio.loc['date_LastUpdate', 'value']
        # date_LastUpdate = Log_Sys.loc['date_LastUpdate', 'value']
        date_LastUpdate = Log_Portfolio.loc['date_LastUpdate', 'value']

        # symbolList comes from Portfolio_Name,path_Symbol
        path_Symbol = Log_Portfolio.loc['path_Symbol', 'value']

        # todo 用初步参数，计算需要的参数
        # todo Get symbolList |
        SymbolList_raw = pd.read_csv(path_Symbol, header=None, sep=',')
        symbolList = SymbolList_raw[0][:]
        print( SymbolList_raw.info )
        # todo Get Dates | Get benchmark index data
        path_Input = 'D:\\data_Input_Wind'

        # import os
        # fileName_Date = path_Input + '\Wind_' + Index_Name + '_updated' + '.csv'
        # print(fileName_Date)
        # # todo os.path.isfile 判断文件是否存在，os.path.isdir 判断文件夹是否存在
        # # todo '_updated'  导出的csv是有index， 原版的是没有的
        # if not os.path.isfile((fileName_Date)):
        #     print('1705070 2238 ')
        #     # 如果不存在  '_updated' ，则用普通状态的
        #     fileName_Date = path_Input + '\Wind_' + Index_Name + '.csv'
        #     data = pd.read_csv(fileName_Date, header=None, skiprows=1, sep=',')
        # else:
        #     # todo case  '_updated'
        #     data = pd.read_csv(fileName_Date, header=None, skiprows=1, sep=',')
        #     data.index = data[0]
        #     data = data.drop([0], axis=1)
        #     print(data.head(3))
        # todo using _updated since 170511
        fileName_Date = path_Input + '\Wind_' + Index_Name + '_updated' + '.csv'

        data = pd.read_csv(fileName_Date, header=None, skiprows=1, sep=',')
        if len( data.columns ) == 9 :
            # we need to drop first columns
            data = data.drop( [0] ,axis=1 )

        data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        data2 = data.dropna(axis=0)
        # print('171211 0025 ', len(data2))
        # print('fileName_Date  \n', fileName_Date   )
        # print( data2.head(5))
        # print(data2.tail(5))
        # asd
        # # todo Get Dates | len_Days 用来确定 Dates长度
        # todo Get Dates | step 1 为了避免出现初始日期是非交易日，先做一个slicing
        # 选大于，一般数据量会小一些

        end_Date = '20' + date_LastUpdate_New[:2] + '-' + date_LastUpdate_New[2:4] + '-' + date_LastUpdate_New[
                                                                                           -2:]

        date_Start = date_LastUpdate
        # from '170101' to '2017-01-01''
        start_Date = '20' + date_Start[:2] + '-' + date_Start[2:4] + '-' + date_Start[-2:]
        # todo data2[data2['date'] > start_Date] to data2[data2['date'] >= start_Date]
        # print('data2 ', data2.tail(3))

        # 171229 0131
        from datetime import datetime
        start_Date2 = datetime.strptime(start_Date , '%Y-%m-%d')
        # before
        temp_List = data2[ pd.to_datetime( data2['date'] ) >= start_Date2 ]

        print('171229 0138 temp_List \n ', temp_List)

        # Now 171229
        Dates = list( temp_List['date'] )
        # print('====== Dates ======= \n', type( list(Dates) ) , Dates)
        # from ['2017/12/14' to ['2017-12-14'
        i = 0
        for temp_date in Dates :
            if  datetime.strptime( temp_date , '%Y-%m-%d') :
                Dates[i] = temp_date
            elif datetime.strptime( temp_date , '%Y/%m/%d') :
                Dates[i] = datetime.strftime(  datetime.strptime( temp_date , '%Y/%m/%d'), '%Y-%m-%d'  )

            i= i+1
        print('====== Dates ======= \n', type(list(Dates)), Dates)

        #
        #
        # # print('start_Date', start_Date)
        # # todo Get Dates | step 2 Find the starting index in Data2 of index
        # if len(temp_List) > 0 and temp_List.index[0] - 1 > 0:
        #     # todo 1704031249 更新日期多了1天，from start_Date_Index = temp_List.index[0] - 1 to temp_List.index[0]
        #     start_Date_Index = temp_List.index[0]
        #     # todo 170313 < 有问题，可能还是应该 <=
        #     # before | temp_List = data2[data2['date'] <= end_Date]
        #     temp_pd = data2[data2['date'] == end_Date]
        #     # print('temp_pd ', temp_pd )
        #     if len( temp_pd ) == 1 :
        #         temp_pd_index = temp_pd.index[0]
        #         temp_List = temp_List.loc[start_Date_Index:temp_pd_index, 'date']
        #         Date_List =temp_List
        #     else :
        #         print( 'Please type in the right trading date to update ')
        #         temp_List = data2[data2['date'] <= end_Date]
        #         end_Date_Index = temp_List.index[-1] + 1
        #         Date_List = data2.loc[start_Date_Index:end_Date_Index, 'date']
        #     # todo Get Dates |step 3 Find the len_Days
        #     # len_Days = len(temp_List ) + 1
        #     # len_Days = len(Date_List)
        #
        #     # todo Get Dates | step 4  确定 Dates 中日期长度
        #     # before
        #     Dates =  list( Date_List)



            # todo Get indexName, Path_Port
        indexName = Index_Name
        Path_Port = path_Sys + '\\' + temp_dir



        return Log_Portfolio, start_Date ,indexName, symbolList, Dates, path_Input, Path_Port, Leverage, MaxN

    def Import_Portfolio_Data(self, Path_TradeSys , start_Date):
        '''
        =========================================
        Import_Portfolio_Data 导入前一交易日各类变量，新建各类变量，参数

        # Input： Dates, Path_TradeSys , start_Date, path_Input,
        # Output： Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana
        # para : rate_Fees
        last update :170401
        =========================================
        '''

        # todo P1 Step1 导入总股票账户：Account_Sum ，并更新长度？
        # todo 要小心 是否有Unnamed：0 这样的column
        Account_Sum = pd.read_csv(Path_TradeSys + '\\' + 'Account_Sum_' + start_Date + '.csv')
        # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        Account_Sum.index = Account_Sum[Account_Sum.columns[0]]
        Account_Sum = Account_Sum.drop(Account_Sum.columns[0], axis=1)

        Account_Stocks = pd.read_csv(Path_TradeSys + '\\' + 'Account_Stocks_' + start_Date + '.csv')
        # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        Account_Stocks.index = Account_Stocks[Account_Stocks.columns[0]]
        Account_Stocks = Account_Stocks.drop(Account_Stocks.columns[0], axis=1)
        StockPool = pd.read_csv(Path_TradeSys + '\\' + 'StockPool_' + start_Date + '.csv')
        # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        StockPool.index = StockPool[StockPool.columns[0]]
        StockPool = StockPool.drop(StockPool.columns[0], axis=1)

        Trading_Book = pd.read_csv(Path_TradeSys + '\\' + 'Trading_Book_' + start_Date + '.csv')
        # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        Trading_Book.index = Trading_Book[Trading_Book.columns[0]]
        Trading_Book = Trading_Book.drop(Trading_Book.columns[0], axis=1)

        # todo 170403　temp_Sig_Ana　是每日更新，有必要导入数据.因为如果不存在非持仓股票的更新，
        # todo 那么就需要沿用上一交易日的 temp_Sig_Ana
        #　Signal	temp_Ana	Order	Symbol
        temp_Sig_Ana = pd.read_csv(Path_TradeSys + '\\' + 'Sig_Ana' + start_Date + '.csv')
        # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        temp_Sig_Ana.index = temp_Sig_Ana[temp_Sig_Ana.columns[0]]
        temp_Sig_Ana = temp_Sig_Ana.drop(temp_Sig_Ana.columns[0], axis=1)

        ''' Print head or tail infomation of current portfolio'''
        # print('Account_Sum', Account_Sum.index[0])
        # print(Account_Sum.tail(5))
        # print('StockPool')
        # print(StockPool.head(5))
        # print('Trading_Book')
        # print(Trading_Book.tail(5))
        # print('Sig_Ana')
        # print(temp_Sig_Ana.head(5))

        return Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana

    def Import_Portfolio_Data_Live(self, Path_TradeSys , start_Date):
        '''
        =========================================
        Import_Portfolio_Data 导入前一交易日各类变量，新建各类变量，参数
        Import_Portfolio_Data_Live 导入组合实盘数据，并转换成 策略计算格式
        ---------------------------------------
        columns 区别： From
        Account_Stocks_Live: MV	Type	Unnamed: 0.1.1.1	code	cost	costAve	lastPrice	name	number
        To
        Account_Stocks_    : Num	AveCost	LastPrice	TotalCost	MV	PnL	PnL_Pct	W_Real	W_Ideal	Date	code
        Need：
        1，	W_Real ：  Account_Sum
        2，	W_Ideal： StockPool
        3， Date ：TradeBook 先空着吧
        ---------------------------------------
        Path_TradeSys =  D:\data_Output\Sys_rC1703_1704\Port_Live\data_Sum
        # Input： Dates, Path_TradeSys , start_Date, path_Input,
        # Output： Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana
        # para : rate_Fees
        last update :170412 | since 170408 1305
        =========================================
        '''
        # todo start_Date 是portfolio 前一次更新截止的日期
        # todo temp_Date 是 portfolio 是还未更新的日期

        # todo P1 Step1 导入总股票账户：Account_Sum ，并更新长度？
        # todo 要小心 是否有Unnamed：0 这样的column
        Account_Sum_Live = pd.read_csv(Path_TradeSys + '\\' + 'Account_Sum_' + start_Date + '.csv')
        # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        Account_Sum_Live.index = Account_Sum_Live[Account_Sum_Live.columns[0]]
        Account_Sum_Live = Account_Sum_Live.drop(Account_Sum_Live.columns[0], axis=1)

        Account_Stocks_Live = pd.read_csv(Path_TradeSys + '\\' + 'Account_Stocks_' + start_Date + '.csv')
        # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        Account_Stocks_Live.index = Account_Stocks_Live[Account_Stocks_Live.columns[0]]
        Account_Stocks_Live = Account_Stocks_Live.drop(Account_Stocks_Live.columns[0], axis=1)

        # print('170503 0928,Path_TradeSys ',  Path_TradeSys , 'Account_Stocks_' + start_Date + '.csv' )
        # print('170503 0928, Account_Stocks_Live ', Account_Stocks_Live)
        StockPool_Live = pd.read_csv(Path_TradeSys + '\\' + 'StockPool_' + start_Date + '.csv')
        # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        # print( Path_TradeSys + '\\' + 'StockPool_' + start_Date + '.csv' )
        StockPool_Live.index = StockPool_Live[StockPool_Live.columns[0]]
        StockPool_Live = StockPool_Live.drop(StockPool_Live.columns[0], axis=1)

        # Trading_Book_Live = pd.read_csv(Path_TradeSys + '\\' + 'Trading_Book_' + start_Date + '.csv')
        # # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        # Trading_Book_Live.index = Trading_Book_Live[Trading_Book_Live.columns[0]]
        # Trading_Book_Live = Trading_Book_Live.drop(Trading_Book_Live.columns[0], axis=1)
        #
        # # todo 170403　temp_Sig_Ana　是每日更新，有必要导入数据.因为如果不存在非持仓股票的更新，
        # # todo 那么就需要沿用上一交易日的 temp_Sig_Ana
        # #　Signal	temp_Ana	Order	Symbol
        # temp_Sig_Ana_Live = pd.read_csv(Path_TradeSys + '\\' + 'Sig_Ana' + start_Date + '.csv')
        # # todo 导入的数，第一列 是 Unnamed: 0 ：2017-02-27
        # temp_Sig_Ana_Live.index = temp_Sig_Ana_Live[temp_Sig_Ana_Live.columns[0]]
        # temp_Sig_Ana_Live = temp_Sig_Ana_Live.drop(temp_Sig_Ana_Live.columns[0], axis=1)

        ''' from Account_Sum_Live to Account_Sum ,Account_Stocks,StockPool, Trading_Book,temp_Sig_Ana'''
        # [ Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana] = self.Tran_Live2Standard(temp_Date,Account_Sum_Live ,Account_Stocks_Live, StockPool_Live ,Trading_Book_Live, temp_Sig_Ana_Live)
        # todo 170409 2351
        #         asdasd Not finish yet

        ''' Print head or tail infomation of current portfolio'''
        # print('Account_Sum', Account_Sum.index[0])
        # print(Account_Sum.tail(5))
        # print('StockPool')
        # print(StockPool.head(5))
        # print('Trading_Book')
        # print(Trading_Book.tail(5))
        # print('Sig_Ana')
        # print(temp_Sig_Ana.head(5))

        return Account_Sum_Live ,Account_Stocks_Live, StockPool_Live

    def Tran_Live2Standard(self, temp_Date, TradeBook_Last,Fees) :
        # 把券商输出的每日交易数据转换成标准交易数据格式。
        # from Account_Sum_Live to Account_Sum ,Account_Stocks,StockPool, Trading_Book,temp_Sig_Ana
        # last update 170416 1539 | since 170408 1519
        # todo temp_Date 是还未更新的日期 temp_Date = 2017-04-05
        print( 'Tran_Live2Standard temp_Date', temp_Date)

        Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        Trading_Book = pd.DataFrame(columns=Columns_Trade)

        for temp_Index in TradeBook_Last.index :
            Trading_Book.loc[temp_Index, 'Account' ] = TradeBook_Last.loc[temp_Index, 'Account' ]
            Trading_Book.loc[temp_Index, 'Date'] = temp_Date
            [wind_Code, Type] = self.Get_Windcode( TradeBook_Last.loc[temp_Index, '代码'] )
            print(wind_Code)
            Trading_Book.loc[temp_Index, 'Symbol'] = wind_Code
            if TradeBook_Last.loc[temp_Index, 'dif_cash'] < 0 :
                Trading_Book.loc[temp_Index, 'Buy/Sell'] = 1
            elif TradeBook_Last.loc[temp_Index, 'dif_cash'] > 0 :
                Trading_Book.loc[temp_Index, 'Buy/Sell'] = -1
            else :
                Trading_Book.loc[temp_Index, 'Buy/Sell'] = 0
            Trading_Book.loc[temp_Index, 'Price'] = TradeBook_Last.loc[temp_Index, '成交均价']
            Trading_Book.loc[temp_Index, 'Number'] = TradeBook_Last.loc[temp_Index, '数量']

            Trading_Book.loc[temp_Index, 'AveCost'] = TradeBook_Last.loc[temp_Index, 'costAve']
            Trading_Book.loc[temp_Index, 'Fees'] = abs( TradeBook_Last.loc[temp_Index, 'dif_cash']) * Fees
            Trading_Book.loc[temp_Index, 'ProfitReal'] = TradeBook_Last.loc[temp_Index, 'ProfitReal']


        # # todo Account_Stocks
        # # columns Live: MV	Type	Unnamed: 0.1.1.1	code	cost	costAve	lastPrice	name	number
        # # columns standard:  Account_Stocks_    : Num	AveCost	LastPrice	TotalCost	MV	PnL	PnL_Pct	W_Real	W_Ideal	Date	code
        # Account_Stocks_columns = ['Num', 'AveCost', 'LastPrice', 'TotalCost', 'MV', 'PnL', 'PnL_Pct', 'W_Real', 'W_Ideal',
        #                        'Date', 'code']
        # Account_Stocks = pd.DataFrame(columns=Account_Stocks_columns, index=Account_Stocks_Live.index)
        # Account_Stocks['Num'] = Account_Stocks_Live['number']
        # Account_Stocks['AveCost'] = Account_Stocks_Live['costAve']
        # Account_Stocks['LastPrice'] = Account_Stocks_Live['lastPrice']
        # Account_Stocks['TotalCost'] = Account_Stocks_Live['cost']
        # Account_Stocks['MV'] = Account_Stocks_Live['MV']
        # Account_Stocks['PnL'] =  Account_Stocks_Live['MV'] -  Account_Stocks_Live['cost']
        # Account_Stocks['PnL_Pct'] = Account_Stocks['PnL'] / Account_Stocks_Live['cost']
        # Account_Stocks['W_Real'] =  0 # todo ???? 	W_Real ：  Account_Sum
        # Account_Stocks['W_Ideal'] = 0 # todo W_Ideal： StockPool ???
        # Account_Stocks['Date'] = temp_Date # todo ???? TradeBook
        # Account_Stocks['code'] = Account_Stocks_Live['code']

        # # todo Account_Sum
        # # columns Live(注意：一个组合只有一行数据): 币种	余额	可用	参考市值	资产	盈亏	Account
        # # columns standard: # Total_Cost	Cash	Stock	Total	Unit	MDD	Index	MDD_I
        # Account_Sum_columns = [ 'Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I' ]
        # Account_Sum = pd.DataFrame( columns=Account_Sum_columns, index= Account_Sum_Live.index )
        # Account_Sum.iloc[temp_Date, 'Cash'] =  Account_Sum.iloc[ 0, '可用']
        # # todo 分析：和模拟盘不同，实盘账户会遇到资金转入转出，税后现金分红等的问题，对应的是要调整净值，份额数量。
        # #
        # # todo 实盘账户初始化时应该统一新建各类变量，数据表，而不是在update的时候去新建
        #
        # # todo StockPool

        return Trading_Book

    def Check_div_Cash_Shares(self,temp_code, temp_Date, temp_LastDay ):
        # 用来核对持仓在 temp_Date 是否有分红送股的情况
        # last update 170504 2039 | since 170504
        # todo Input : temp_code, temp_Date, temp_LastDay,
        # todo Output : temp_factor, dif_Cash
        # temp_code = "300450.SZ"
        # temp_Date = "2017-04-18"
        # temp_LastDay = "2017-04-19"
        # todo 注意 最后一部分 "" 表示不复权，  "PriceAdj=F" 表示 前复权
        import WindPy as WP
        WP.w.start()
        data = WP.w.wsd(temp_code, "sec_name,pre_close, close, total_shares", temp_LastDay, temp_Date,  "")
        # data= WP.w.wsd("300450.SZ", "sec_name,pre_close, close, total_shares", "2017-05-03", "2017-05-04", "")
        # todo from list to Dataframe
        pd_Data = pd.DataFrame(data.Data)
        pd_Data = pd_Data.T
        print('pd_Data ')
        # print(pd_Data)
        pd_Data.columns = ['sec_name', 'pre_close', 'close', 'total_shares']

        print(len(data.Data[0]))
        rate_Tax = 0.20
        number = 100

        if len(data.Data[0]) == 2:
            # todo 注意：需要确保前后2个交易日是相邻的，否则数据会出错。
            # # data.Data [['先导智能'], [44.99], [6.054786], [44.0], [None], [None], [None]]
            # close_lastDay = data.Data[2][1] # 'close' 第二个 | wind 的除权除息日的前一日close，未考虑分红
            # pre_close_Date = data.Data[1][0] # 'pre_close' 第一个| wind 的除权除息日的pre_close，已考虑送股分红
            # shares_lastDay = data.Data[3][0] #  前一日总股票数量
            # shares_Date = data.Data[3][1]    #  当日总股票数量

            close_lastDay = pd_Data.loc[0, 'close']  # 'close' 第二个 | wind 的除权除息日的前一日close，未考虑分红
            pre_close_Date = pd_Data.loc[1, 'pre_close']  # 'pre_close' 第一个| wind 的除权除息日的pre_close，已考虑送股分红
            shares_lastDay = pd_Data.loc[0, 'total_shares']  # 前一日总股票数量
            shares_Date = pd_Data.loc[1, 'total_shares']  # 当日总股票数量

            # todo Qs 如何考虑 单纯现金分红和， 现金加送股呢？
            # round 四舍五入是围绕着0来计算的 | 例子： 2.200045 to 2.2
            temp_factor = round(shares_Date * 10 / shares_lastDay) / 10
            # todo 是不是最少十送一？那样的话，就按 1.1 为标准吧
            print('temp_factor ', temp_factor)
            number2 = number * temp_factor
            print('number2 ', number2)
            dif_Cash = (close_lastDay * number - pre_close_Date * number2) * (1 - rate_Tax)
            dif_Cash = round(dif_Cash, 2)
            print('dif_Cash  ', dif_Cash)

        else:
            print('Error: wind返回数据长度不等于2个交易日  注意：需要确保前后2个交易日是相邻的，否则数据会出错。')
            print('data.Data[0] ', data.Data[0])

        return temp_factor, dif_Cash

    # todo ====== Level 2 ========================================================================
    def Get_Windcode(self, code):
        # transpose code from broker format to windcode that can be used to get wind data
        # 注意，券商的数据往往是 2053,2466 这样
        # last update 170416 1115  | since 170412 1118
        # derived from rC_DailyOperation.py | def Update_AS_TB(self, path_Out_Sum, PreTradingDay,LastTradingDay,temp_AS ):

        code_str = str( code )  # todo  601179.0  : len(code_str)-2

        if code_str[-3:] == '.SH' or code_str[-3:] == '.SZ':
            if code_str[:2] in ['60', '00', '30']:
                Type = 1
            elif code_str[:2] in ['50', '15', '51']:
                Type = 4

        elif code_str[-2:] == '.0':
            if code_str[:2] in ['60'] and len(code_str) - 2 == 6:
                # 沪股票
                Type = 1
                windcode = code_str[:6] + '.SH'
            elif code_str[:2] in ['00', '30'] and len(code_str) - 2 == 6:
                # 深股票
                Type = 1
                windcode = code_str[:6] + '.SZ'
            elif code_str[:2] in ['15', '51', '50'] and len(code_str) - 2 == 6:
                # 沪深基金
                Type = 4
                if code_str[:2] in ['15']:
                    windcode = code_str[:6] + '.SZ'
                else:
                    windcode = code_str[:6] + '.SH'
            elif code_str[:2] in ['13'] and len(code_str) - 2 == 6:
                # 沪深债券
                Type = 7
                windcode = code_str[:6] + '.SH'
            elif code_str[:2] in ['20'] and len(code_str) - 2 == 6:
                # 沪深回购
                Type = 8
                windcode = code_str[:6] + '.SH'
            elif len(code_str) - 2 <= 5 and len(code_str) - 2 >= 1:
                # 深圳股票
                Type = 1
                len_dif = 6 - (len(code_str) - 2)
                windcode = '0' * len_dif + code_str[:-2] + '.SZ'  # '0'*3 = '000'
            else:
                # 其他
                print('170314 1754 temp_AS:', temp_AS)
                Type = 9
        else:

            if code_str[:2] in ['60'] and len(code_str) == 6:
                # 沪股票
                Type = 1
                windcode = code_str[:6] + '.SH'
            elif code_str[:2] in ['00', '30'] and len(code_str) == 6:
                # 深股票
                Type = 1
                windcode = code_str[:6] + '.SZ'
            elif code_str[:2] in ['15', '51', '50'] and len(code_str) == 6:
                # 沪深基金
                Type = 4
                if code_str[:2] in ['15']:
                    windcode = code_str[:6] + '.SZ'
                else:
                    windcode = code_str[:6] + '.SH'
            elif code_str[:2] in ['13'] and len(code_str) == 6:
                # 沪深债券
                Type = 7
                windcode = code_str[:6] + '.SH'
            elif code_str[:2] in ['20'] and len(code_str) == 6:
                # 沪深回购
                Type = 8
                windcode = code_str[:6] + '.SH'
            elif len(code_str) - 2 <= 5 and len(code_str) >= 1:

                # 深圳股票
                Type = 1
                len_dif = 6 - (len(code_str) )
                windcode = '0' * len_dif + code_str + '.SZ'  # '0'*3 = '000'
                # print('170416 1114, code_str', code_str, code_str[:2])
            else:
                print('170412 Get windcode:', code_str )
                # 其他
                Type = 9


        return windcode,Type

    def Get_Signal_Next( self, temp_Symbol , Path_Data ,temp_Date ) :
        '''
        # todo  last update 170403 1501  |
        # todo 为了在N日获得N+1日买卖信号，我们 需要新建module Get_Signal_Next
        # todo Ana :(data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_LastDay, Path_Data)
        # todo Get_Price_Ret| Input : temp_LastDay | Output: data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N-1日，不包括N 日
        解决办法，用 Get_Price_Ret_Next 代替 Get_Price_Ret

        '''

        ''' Part 1 Get price and return information '''
        print('Get_Signal for Next trading day ')
        (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret_Next(temp_Symbol, temp_Date, Path_Data)
        # todo 171130 Output: Given temp_Date is Day N, data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N-1日，不包括N 日


        ''' Part 2 Get Analytics data '''
        # print('data_1day',len(data_1day) , 'temp_Date_Index is :', temp_Date_Index )
        # data_ALLdays.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        # type of data_ALLdays is Dataframe-
        # todo Part 2 Get Ana_Data if temo_Date_Index is fine and enough length of data_ALLdays
        MA_x = [3, 8, 16, 40, 100]
        P_MA = [0, 0, 0, 0, 0]  # 1 if Price>MA(x)
        MA_up = [1, 1, 1, 1, 1]  # # of days we want such MA(x) to be Up
        temp_Signal = 0
        if not type(data_1day) == int:
            # todo 170403 from len(data_ALLdays) >= MA_x[-1] + 3 to len(data_ALLdays) >= MA_x[-2] + 3
            # todo 170404 注意 MA_x[-1] 还是要保持，否则 MA_x[-2]情况，Signal永远是 0
            if temp_Date_Index < 80000 and len(data_ALLdays) >= MA_x[-1] + 3:
                temp_Index1 = data_ALLdays['date'].index[-1]
                import rC_Stra_MAX as rC
                time0 = '2012-07-01'
                time1 = '2015-12-31'
                # file_path0 =  'D:\\rC_Py3_Output\\'
                Results = rC.rC_TradeSys(temp_Symbol, time0, time1, Path_Data)

                # todo 170403,from data = data_ALLdays.tail(MA_x[-1] + 10) to data_ALLdays.tail(MA_x[-2] + 5)
                data = data_ALLdays.tail(MA_x[-1] + 10)
                # todo 161231 这里的代码对应了input的数据必须超过 MA_max+10的长度才行，之前是MA_max+3,就导致MA_up都是0
                # print(data)

                Ana_Data = Results.AnalyticFiData_2(data, MA_x, P_MA, MA_up)

                # todo Part 3 Get Stra Signal and the latest Signal
                # todo 171130 确定这里的Signal是最新交易日，因为data_ALLdays是最新交易日
                # Signals = Results.Stra_MA_Signals_a_vol_4Port(Ana_Data, data)
                Signals = Results.Stra_MA_Signals_a_vol_4Port(Ana_Data, data)

                # print(Signals[0].iloc[-2:])
                # 161023 type of Signals is pd.DataFrame
                if Signals[0].iloc[-2] == 0 and Signals[0].iloc[-1] == 1:
                    # todo 由于是计算下一交易日信号，因此无法考虑明日开盘价格信息。
                    # if data_1day['amt'] >= 4000000 and data_ALLdays['open'][temp_Index1] <= (1 + 0.0975) * \
                    #         data_ALLdays['close'][temp_Index1 - 1]:
                    temp_Signal = 1

                elif Signals[0].iloc[-2] == 1 and Signals[0].iloc[-1] == 1:
                    temp_Signal = 1  # 170102
                    # print( data_1day['amt'] )
                    # print( data_ALLdays['open'][temp_Index1] )
                elif Signals[0].iloc[-2] == 1 and Signals[0].iloc[-1] == 0:
                    # todo Error 170108 1029
                    temp_Signal = -1

                else:
                    temp_Signal = 0
            else:
                temp_Signal = 0
        else:
            temp_Signal = 0

        print('temp_Symbol', temp_Symbol, 'temp_Date :', temp_Date , '| Signal', temp_Signal)

        return temp_Signal,temp_Date_Index

    def Get_Signal( self, temp_Symbol , Path_Data ,temp_LastDay) :
        # todo 171217 增加在 signal=1 to 0 第一天没能卖出的情况下，次日0to0的情况下要继续卖出。
        # todo last update 170403  | 2016
        # todo 一开始只为 LastDay设计，现在要试试 temp_Date下能否顺利运行。
        # todo Ana: Get_Signal 的信号是回测信号，基于已经收盘的交易日N,根据N-2，N-1日数据获取N日Signal，
        # todo Ana: 并根据N日收盘后的价格和数量，确定买卖信号
        # todo Ans：为了在N日获得N+1日买卖信号，我们 需要新建module Get_Signal_Next
        # todo 171130 这边的signal好像全部慢了1天，需要改进
        # 171210 计算港股signal时，出现了 6366-20161228， 6368-20161230，但没有6367的情况，这样就不能用 temp_Index1-1，应该用
        #    temp_Index0 = data_ALLdays['date'].index[-2]


        # 161023 Here we only need temp_Date to be last trading day so we can get BSH signal for current day!
        # todo todotodo todotodo todotodo todotodo todotodo todotodo todotodo todotodo todotodo todo
        # 160920 买卖原则：当日股票成交金额应该国内大于4000万，香港大于500万。
        # 从股票代码和日期数据，获取最新的买卖信号
           # if temp_Symbol[i-1]==0 and temp_Symbol[i]==1 :

        # todo Part 1 Get raw data
        print('Get_Signal')
        ( data_1day ,data_ALLdays, temp_Date_Index ) = self.Get_Price_Ret(temp_Symbol, temp_LastDay, Path_Data)
        # todo Given temp_LastDay is N-1 day, data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N-1日，不包括N 日
        # print('data_1day',len(data_1day) , 'temp_Date_Index is :', temp_Date_Index )

        # data_ALLdays.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        # type of data_ALLdays is Dataframe-
        # todo Part 2 Get Ana_Data if temo_Date_Index is fine and enough length of data_ALLdays
        MA_x = [3, 8, 16, 40, 100]
        P_MA = [0, 0, 0, 0, 0]  # 1 if Price>MA(x)
        MA_up = [1, 1, 1, 1, 1]  # # of days we want such MA(x) to be Up
        temp_Signal = 0
        if not type(data_1day) == int :
            if temp_Date_Index < 80000 and len(data_ALLdays) >= MA_x[-1]+3  :
                # todo 171215 ;模拟组合出现了 2017/11/20	601313.SH	1	22.81 一字板涨停买入的情况
                # before
                # temp_Index1 = data_ALLdays['date'].index[-1]
                # temp_Index0 = data_ALLdays['date'].index[-2]
                # now
                # print(data_1day['date'],data_1day['open'], )
                temp_Index1 = data_1day['date'].index
                temp_Index0 = data_ALLdays['date'].index[-1]

                import rC_Stra_MAX as rC
                time0 = '2012-07-01'
                time1 = '2015-12-31'
                # file_path0 =  'D:\\rC_Py3_Output\\'
                Results = rC.rC_TradeSys( temp_Symbol, time0, time1, Path_Data)
                # todo last update 161231
                data = data_ALLdays.tail(MA_x[-1]+10)
                # todo 161231 这里的代码对应了input的数据必须超过 MA_max+10的长度才行，之前是MA_max+3,就导致MA_up都是0
                # todo 171130 由于data 对应 :N-1 日，因此 Ana和Signal 都是最新对应到 N-2,N-1日
                print( data.tail(5) )

                Ana_Data = Results.AnalyticFiData_2( data , MA_x, P_MA, MA_up )
                # print('partial debug 171130 ')
                # print(Ana_Data['P/MA40'].iloc[-1] )
                # print(Ana_Data['MA16'].tail())
                # print(Ana_Data['MA16_up'].tail()  )
                # todo Part 3 Get Stra Signal and the latest Signal

                # Signals = Results.Stra_MA_Signals_a_vol_4Port(Ana_Data, data)
                # todo 170824 from Stra_MA_Signals_a_vol_4Port to Stra_MA_dif_M( Ana_Data, data )
                # todo 171130 由于data 对应 :N-1 日，因此 Ana和Signal 都是最新对应到 N-2,N-1日
                Signals = Results.Stra_MA_Signals_a_vol_4Port(Ana_Data, data)
                # Signals = Results.Stra_MA_dif_M(Ana_Data, data)

                # print('Signals tail', Signals[0].sum(), Signals.tail(5 ) )
                # asd
                print( 'Signals for last 2 days: ',Signals[0].iloc[-2] , Signals[0].iloc[-1 ]  )
                # print( type(Signals) )

                # print(Signals[0].iloc[-2:])
                # 161023 type of Signals is pd.DataFrame
                if Signals[0].iloc[-2] == 0 and Signals[0].iloc[-1] == 1:
                    # now 171215
                    if data_1day['amt'] >= 4000000 and data_1day['open'] <= (1+0.0975)*data_ALLdays['close'].iloc[-1] :
                        temp_Signal = 1

                elif Signals[0].iloc[-2] == 1 and Signals[0].iloc[-1] == 1 :
                    # if data_1day['amt'] >= 4000000 and data_1day['open'] <= (1 + 0.0975) * data_ALLdays['close'][  temp_Index0]:
                    temp_Signal = 1 # 170102

                        # print( data_1day['amt'] )
                    # print( data_ALLdays['open'][temp_Index1] )
                elif Signals[0].iloc[-2] == 1 and Signals[0].iloc[-1] == 0 :
                    # todo Error 170108 1029
                    # print('todo 170108 1028 data_1day amt', temp_Index1) # 5395 -1 =5394 todo 5394报错 有可能是停牌了，只有收盘价格。

                    # print(data_ALLdays.loc[temp_Index1, 'close' ])
                    # print('todo 170227 0050data_1day amt')
                    # print( data_ALLdays.tail(5) )
                    # todo 170227 这里到底要做什么呢?
                    # todo 对下一日和今日的收盘价做一个比较和判断。
                    temp_List = data_ALLdays.index
                    if temp_List[-2] >=temp_Index0 :
                        # 这说明时间序列里的最新日期是够用的。
                        if not type( data_ALLdays.loc[temp_Index0,'close' ] ) == str :
                            if data_1day['amt'] >= 4000000 and data_1day['open' ] >= (1-0.0985)*data_ALLdays['close' ].iloc[-1] :
                                temp_Signal = -1
                        else :
                            temp_Signal = 0
                    else :
                        temp_Signal = 0
                else :
                    # todo 171217，case Signals[0].iloc[-2] == 0 and Signals[0].iloc[-1] == 0 要应对 1-to-0 没来得及卖出的情况阿！
                    if not type(data_ALLdays.loc[temp_Index0, 'close']) == str:
                        if data_1day['amt'] >= 4000000 and data_1day['open'] >= (1 - 0.0985) * \
                                data_ALLdays['close'].iloc[-1]:
                            temp_Signal = -1
                    else:
                        temp_Signal = 0
                # if temp_Symbol =='002186.SZ' and temp_LastDay =='2017-01-25' :
                #     print('222temp_Symbol ',temp_Symbol,'temp_Symbol temp_LastDay ==',Signals[0].iloc[-2] ,Signals[0].iloc[-1] ,temp_Signal )

            else :
                temp_Signal =0
        else :
            temp_Signal = 0

        print('temp_Symbol', temp_Symbol, 'temp_LastDay :', temp_LastDay, '| Signal',temp_Signal  )

        return temp_Signal,temp_Date_Index,data_1day ,data_ALLdays


    def Get_Signal_NoAmt( self, temp_Symbol , Path_Data ,temp_LastDay) :
        # todo adjust for index which may has no amt or volume
        # todo last update 170426  | since 170426
        # todo 一开始只为 LastDay设计，现在要试试 temp_Date下能否顺利运行。
        # todo Ana: Get_Signal 的信号是回测信号，基于已经收盘的交易日N,根据N-2，N-1日数据获取N日Signal，
        # todo Ana: 并根据N日收盘后的价格和数量，确定买卖信号
        # todo Ans：为了在N日获得N+1日买卖信号，我们 需要新建module Get_Signal_Next


        # 161023 Here we only need temp_Date to be last trading day so we can get BSH signal for current day!
        # todo todotodo todotodo todotodo todotodo todotodo todotodo todotodo todotodo todotodo todo
        # 160920 买卖原则：当日股票成交金额应该国内大于4000万，香港大于500万。
        # 从股票代码和日期数据，获取最新的买卖信号
           # if temp_Symbol[i-1]==0 and temp_Symbol[i]==1 :

        # todo Part 1 Get raw data
        print('Get_Signal')
        ( data_1day ,data_ALLdays, temp_Date_Index ) = self.Get_Price_Ret(temp_Symbol, temp_LastDay, Path_Data)
        # todo Given temp_LastDay is N-1 day, data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N-1日，不包括N 日
        # print('data_1day',len(data_1day)  )

        # data_ALLdays.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        # type of data_ALLdays is Dataframe-
        # todo Part 2 Get Ana_Data if temo_Date_Index is fine and enough length of data_ALLdays
        MA_x = [3, 8, 16, 40, 100]
        P_MA = [0, 0, 0, 0, 0]  # 1 if Price>MA(x)
        MA_up = [1, 1, 1, 1, 1]  # # of days we want such MA(x) to be Up
        temp_Signal = 0
        if not type(data_1day) == int :
            if temp_Date_Index < 80000 and len(data_ALLdays) >= MA_x[-1]+3  :
                temp_Index1 = data_ALLdays['date'].index[-1]
                temp_Index0 = data_ALLdays['date'].index[-2]
                import rC_Stra_MAX as rC
                time0 = '2012-07-01'
                time1 = '2015-12-31'
                # file_path0 =  'D:\\rC_Py3_Output\\'
                Results = rC.rC_TradeSys( temp_Symbol, time0, time1, Path_Data)
                # todo last update 161231
                data = data_ALLdays.tail(MA_x[-1]+10)
                # todo 161231 这里的代码对应了input的数据必须超过 MA_max+10的长度才行，之前是MA_max+3,就导致MA_up都是0
                # print(data)
                Ana_Data = Results.AnalyticFiData_2( data , MA_x, P_MA, MA_up )
                # print('partial debug 161231 ')
                # print(Ana_Data['P/MA40'].iloc[-1] )
                # print(Ana_Data['MA16_up'].iloc[-1] )
                # todo Part 3 Get Stra Signal and the latest Signal
                # todo 不确定这里的Signal是对应前一交易日还是最新交易日
                # Signals = Results.Stra_MA_Signals_a_vol_4Port(Ana_Data, data)
                Signals = Results.Stra_MA_Signals_a_vol_4Port(Ana_Data, data)
                # Signals = Results.Stra_MA_dif_M(Ana_Data, data)

                # print( 'Signals for last 2 days: ',Signals[0].iloc[-2] , Signals[0].iloc[-1 ]  )

                # print( data_ALLdays.tail(10) )
                # print( type(Signals) )

                # print(Signals[0].iloc[-2:])
                # 161023 type of Signals is pd.DataFrame
                if Signals[0].iloc[-2] == 0 and Signals[0].iloc[-1] == 1:
                    # from if data_1day['amt'] >= 4000000 and data_ALLdays['open'][temp_Index1] <= (1+0.0975)*data_ALLdays['close'][temp_Index0] :
                    if data_ALLdays['open'][temp_Index1] <= (1+0.0975)*data_ALLdays['close'][temp_Index0] :
                        temp_Signal = 1

                elif Signals[0].iloc[-2] == 1 and Signals[0].iloc[-1] == 1 :
                    temp_Signal = 1 # 170102
                    # print( data_1day['amt'] )
                    # print( data_ALLdays['open'][temp_Index1] )
                elif Signals[0].iloc[-2] == 1 and Signals[0].iloc[-1] == 0 :
                    # todo Error 170108 1029
                    print('todo 170108 1028 data_1day amt', temp_Index1) # 5395 -1 =5394 todo 5394报错 有可能是停牌了，只有收盘价格。
                    print(data_ALLdays.loc[temp_Index1, 'close' ])
                    # print('todo 170227 0050data_1day amt')
                    # print( data_ALLdays.tail(5) )
                    # todo 170227 这里到底要做什么呢?
                    # todo 对下一日和今日的收盘价做一个比较和判断。
                    temp_List = data_ALLdays.index
                    if temp_List[-2] >=temp_Index0 :
                        # 这说明时间序列里的最新日期是够用的。
                        if not type( data_ALLdays.loc[temp_Index0,'close' ] ) == str :
                            # todo from data_1day['amt'] >= 4000000 and data_1day['open' ] >= (1-0.0995)*data_ALLdays['close' ][temp_Index0] :
                            if data_1day['open' ] >= (1-0.0995)*data_ALLdays['close' ][temp_Index0] :
                                temp_Signal = -1
                        else :
                            temp_Signal = 0
                    else :
                        temp_Signal = 0
                else :
                    temp_Signal = 0
            else :
                temp_Signal =0
        else :
            temp_Signal = 0

        print('temp_Symbol', temp_Symbol, 'temp_LastDay :', temp_LastDay, '| Signal',temp_Signal  )

        return temp_Signal,temp_Date_Index

    def Get_BSH_Weight_Amount(self,temp_Signal_Index,temp_Signal, temp_Symbol, StockPool, MaxN, Account_Sum, temp_Date,  Leverage) :
        '''
         Get_Signal_Weight_Amount 是根据策略信号，计算出持仓股票的交易信号，目标权重，以及最大可分配资金
        last update 170401 1823 | since 170401 1823
        Signal to real order :  Get price, Get holding, Do accounting

        '''

        if temp_Signal_Index < 1:
            temp_Signal = -1

        # todo P2 S1.2 Get_Weight
        # 获取理想ideal 投资权重( 资产配置 ), how?
        # 权重条件： 初始等权重，动态等权重，按市值/PB/PE/roe/,波动率/ 近期涨跌幅度等分配权重
        # temp_Weight = GetWeight( temp_Symbol, i,weight_Method ,MaxN ,temp_Date_Index)

        # todo find weight of temp_Symbol  in StockPool[]
        temp_123 = StockPool[StockPool['code'].isin([temp_Symbol])]
        # todo temp_123 返回的是 dataFrame，且 temp_123.index 的值应该就是  StockPool 里的值
        if len(temp_123) > 0:
            W_Ideal = temp_123['W_Ideal'].values[0]  # todo 返回 temp_123['W_Ideal']的值得第一个，理论上应该只有一个
            temp_Weight = W_Ideal
        else:
            temp_Weight = 1 / MaxN
        weight_ideal = 1 / MaxN
        # temp_Amuont is the adjusted amount of capital with leverage constrant
        # Account_Sum： Columns = ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I'] Account_Sum['Total'][ i-1 ]
        # todo 170102_2327
        temp_Amount0 = max(0, Account_Sum.loc[temp_Date, 'Cash'] - Account_Sum.loc[temp_Date, 'Total'] * (1 - Leverage))
        temp_Amount = min(temp_Amount0, Account_Sum.loc[temp_Date, 'Total'] * Leverage * temp_Weight)
        print('temp_Amount ', temp_Amount)

        # todo Next Part
        # Check Account: 资金够不够，股票能否加仓减仓
        # 买卖信号 temp_Signal= buy 1 from [0,1]| hold 0  [1,1]| sell -1 [1,0]
        # 161025_1908 Here we need to find symbol from StockPool
        # todo 161106 既然已经在Account_Stock2 里发现了这个code，理论上应该有正常数量的的持仓
        temp_j = -1000

        for j4 in range(len(StockPool['code'])):
            # print('161025_1932')
            # print(temp_Symbol,StockPool['code'].iloc[j4]  )
            if temp_Symbol == StockPool['code'].iloc[j4]:
                temp_j = j4
                # print('Weight for holding stock is fine.')

        if temp_j == -1000:
            print('161025_1923')
            print('Error: we could not find temp_Symbol in Symbol list')

        weight_j1 = StockPool['W_Real'].iloc[temp_j]
        if weight_j1 == 0:
            weight_j1 = 1 / MaxN
        # Check if enough cash or stock level in Account
        # temp_BSH = self.Check_Account(i, temp_Signal, Account_Sum, weight_j1 , Leverage, weight_ideal)
        temp_BSH = temp_Signal


        return temp_BSH , temp_Weight, temp_Amount

    def Get_temp_Trade(self,Account_Sum,temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,preClose,Leverage) :
        '''
        Get_temp_Trade 是根据策略信号，市场价格数量和持仓信息，计算出具体交易指令明细
            # Get trade details : Price,Number,Cost, AveCost, Fees, ProfitRealized
        last update 170401 1915 | since 170401 1915
                # todo 170401 目的：获得 temp_trade ,但现在是在 Trading_Book 里一起算
        # Input :Account_Sum,temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,
        # Input :    Leverage
        # Output :temp_Number, temp_TotalCost, temp_AveCost, temp_Fees, temp_ProfitReal
        '''

        (Stocks_Cost_ij, Stocks_aveCost_ij, Stocks_Num_ij) = self.Get_HoldingStock(temp_Symbol, Account_Stocks)
        print('Holding information is : ', temp_Symbol, Stocks_Cost_ij, Stocks_aveCost_ij, Stocks_Num_ij)

        # todo temp_Number, temp_TotalCost, temp_AveCost , temp_Fees , temp_ProfitReal
        Account_Sum_Cash_i = Account_Sum.loc[temp_Date, 'Cash']
        Account_Sum_Total_i = Account_Sum.loc[temp_Date, 'Total']

        [temp_Number, temp_TotalCost, temp_AveCost, temp_Fees, temp_ProfitReal] = self.Get_Number( rate_Fees, temp_BSH,
            temp_Amount, data_1day,preClose, Stocks_Cost_ij, Stocks_aveCost_ij,   Stocks_Num_ij, Account_Sum_Cash_i, Account_Sum_Total_i, Leverage)
        # todo 161218_1607 算出 负数的卖出数量问题解决。
        print('Number to trade', temp_Number, temp_TotalCost, temp_AveCost, temp_Fees,temp_ProfitReal)

        Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        temp_Trade = pd.DataFrame(np.zeros([1, 9]), columns=Columns_Trade)
        temp_Trade['Date'] = temp_Date
        temp_Trade['Symbol'] = temp_Symbol
        temp_Trade['Buy/Sell'] = temp_BSH
        temp_Trade['Price'] = data_1day['open']
        temp_Trade['Number'] = temp_Number
        temp_Trade['AveCost'] = temp_AveCost
        temp_Trade['Fees'] = temp_Fees
        temp_Trade['ProfitReal'] = temp_ProfitReal

        return temp_Trade

    def Get_temp_Trade_Next(self,Account_Sum,temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,Leverage) :
        '''
        Get_temp_Trade_Next 是根据策略信号，和持仓信息，计算出明日的交易指令计划
        Derived from Get_temp_Trade
        注意：和Get_temp_Trade不同，这里的 data_1day指的不是下一个交易日数据，而是刚结束的交易日数据，以当日收盘价
            作为明天开盘价的成交价。
        last update 170404 2052 | since 170404 2052
        # Input :Account_Sum,temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,
        # Input :    Leverage
        # Output :temp_Number, temp_TotalCost, temp_AveCost, temp_Fees, temp_ProfitReal
        '''

        (Stocks_Cost_ij, Stocks_aveCost_ij, Stocks_Num_ij) = self.Get_HoldingStock(temp_Symbol, Account_Stocks)
        print('Holding information is : ', temp_Symbol, Stocks_Cost_ij, Stocks_aveCost_ij, Stocks_Num_ij)

        # todo temp_Number, temp_TotalCost, temp_AveCost , temp_Fees , temp_ProfitReal
        Account_Sum_Cash_i = Account_Sum.loc[temp_Date, 'Cash']
        Account_Sum_Total_i = Account_Sum.loc[temp_Date, 'Total']

        [temp_Number, temp_TotalCost, temp_AveCost, temp_Fees, temp_ProfitReal] = self.Get_Number_Next( rate_Fees, temp_BSH,
            temp_Amount, data_1day, Stocks_Cost_ij, Stocks_aveCost_ij,   Stocks_Num_ij, Account_Sum_Cash_i, Account_Sum_Total_i, Leverage)

        if temp_Number <=0 :
            # todo 未来情况下，有可能出现算不出买入数量的问题，这时候只能估算了
            # print('170706')
            # print( Account_Stocks['MV'].mean() )
            # print( 'data_1day  ', type(data_1day) )
            if not type(data_1day) == int  :
                temp_Number = int( round( Account_Stocks['MV'].mean()/data_1day['close']/100 )*100 )
            else :
                temp_Number = 0


        print('Number to trade', temp_Number, temp_TotalCost, temp_AveCost, temp_Fees,temp_ProfitReal)

        Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        temp_Trade = pd.DataFrame(np.zeros([1, 9]), columns=Columns_Trade)
        temp_Trade['Date'] = temp_Date
        temp_Trade['Symbol'] = temp_Symbol
        temp_Trade['Buy/Sell'] = temp_BSH
        # 170706
        if not type(data_1day) == int:
            temp_Trade['Price'] = data_1day['close']
        else :
            temp_Trade['Price'] = 0
        temp_Trade['Number'] = temp_Number
        temp_Trade['AveCost'] = temp_AveCost
        temp_Trade['Fees'] = temp_Fees
        temp_Trade['ProfitReal'] = temp_ProfitReal

        return temp_Trade

    def Get_Sig_Ana(self, symbolList, Path_Data,temp_Date, temp_LastDay, StockPool  ):
        # todo 计算 temp_Sig_Ana 和 temp_Sort，将可买入的股票按顺序排列
        # potential Input : self, symbolList, Path_Data,temp_Date, temp_LastDay,temp_Signal_Index,StockPool, MaxN, Account_Sum, Leverage ):
        # todo last update 1704022208 | sicne 1704022208
        temp_Sig_Ana = pd.DataFrame(np.zeros([len(symbolList), 4]), columns=['Signal', 'temp_Ana', 'Order', 'Symbol'])

        # todo Get signals and ana_factors for all non-holding stocks
        print('# todo P2 S2.1 Get signals and ana_factors for all non-holding stocks ')

        for j2 in range(len(symbolList)):
            temp_Sig_Ana.loc[j2, 'Order'] = j2  # todo 在symbolList里的序号
            temp_Symbol = symbolList[j2]
            temp_Sig_Ana.loc[j2, 'Symbol'] = temp_Symbol

            temp_Ana = 1  # temp_Ana = 1 means useless ,as smaller as good
            # print('170703-1900 ','j2=',type(j2)) # str type
            # print( '\nStockPool-type ', type(StockPool.loc[ str(j2) , 'ifHold'])  ) # float
            # 170703 注意，下一行有时候是右边的状态：   StockPool.loc[j2, 'ifHold'] == 0 |  StockPool.loc[ str(j2), 'ifHold'] == 0.0
            if StockPool.loc[j2, 'ifHold'] == 0  :

                # todo P2 S3.2  Get_Signal ideal
                # For future, if we want real-time signal, then we need to use lastest price to replace Last Day price
                (temp_Signal, temp_Date_Index,data_1day ,data_ALLdays) = self.Get_Signal(temp_Symbol, Path_Data, temp_LastDay)

                # todo P2 S3.3 Get_BSH_Weight_Amount : Portfolio角度，获取 买卖信号，权重，金额
                # Find weight, Check Account Get price, Get holding, Do accounting
                # (temp_BSH, temp_Weight, temp_Amount) = self.Get_BSH_Weight_Amount(temp_Signal_Index, temp_Signal,
                #                                                                   temp_Symbol, StockPool, MaxN,
                #                                                                   Account_Sum,temp_Date,
                #                                                                   Leverage)
                # todo Ana 感觉这里还不需要用到 Get_BSH_Weight_Amount 和 指数信号 temp_Signal_Index
                temp_BSH = temp_Signal

                temp_Ana = 1  # temp_Ana = 1 means useless ,as smaller as good
                # todo 只保留买入信号
                if temp_BSH == 1:
                    temp_Sig_Ana.loc[j2, 'Signal'] = 1
                    # todo  Get ana_factors, p_H40
                    MA_x = [3, 8, 16, 40, 100]
                    # 170830 todo 为了测试 Initial_1708, 要暂时改变 temp_Ana 的定义！！
                    temp_Ana = self.Get_AnaFactors(temp_Symbol, Path_Data, temp_Date, MA_x)
                    # temp_Ana = 1

                    # print('temp_Ana 170308 ', temp_Ana)
                    temp_Sig_Ana.loc[j2, 'temp_Ana'] = temp_Ana * -1
                    print(temp_Date, " | ", temp_Symbol, " | Signal: ", temp_Signal, 'temp_Sig_Ana ', temp_Ana)

            # 买入信号数量大于对 最多可以买入的股票数量 ：sum( temp_Sig_Ana[0].values ) > temp_Num_dif
            # 买入信号数量小于对 最多可以买入的股票数量 ：sum( temp_Sig_Ana[0].values ) < temp_Num_dif
            # if temp_Sig_Ana[:,1]=[-0.1, -0.2, -0.3, -0.07, 0, -0.25] :
            # temp_Sort = [ 2, 5, 1, 0, 3, 4         ]
            # temp_Sig_Ana[:,1][temp_Sort] = [-0.3, -0.25, -0.2, -0.1, -0.07, 0]

        # todo P2 S3.4 策略排序 ： 初步发现，按照从小到大的排，优先买入的都是涨幅较少的股票，结果就是波动贴近指数
        # 数值越小越好，从小到大排列， 数值1表示无意义；temp_Sort is an index list  from small to large
        temp_Sort = np.argsort(temp_Sig_Ana['temp_Ana'].values, axis=0)  # 从小到大的值的index
        # temp_Sort = np.sort(temp_Sig_Ana[1].values, axis=0) # 从小到大的值
        # todo 未来也可以按照其他指标排序，比如roe， PCF，vol ，amt等

        return temp_Sig_Ana, temp_Sort

    def Get_Sig_Ana_NoAmt(self, symbolList, Path_Data,temp_Date, temp_LastDay, StockPool  ):
        # todo adjust for index which may has no amt or volume
        # todo last update 170426  | since 170426
        # todo 计算 temp_Sig_Ana 和 temp_Sort，将可买入的股票按顺序排列
        # potential Input : self, symbolList, Path_Data,temp_Date, temp_LastDay,temp_Signal_Index,StockPool, MaxN, Account_Sum, Leverage ):
        # todo last update 1704022208 | sicne 1704022208
        temp_Sig_Ana = pd.DataFrame(np.zeros([len(symbolList), 4]), columns=['Signal', 'temp_Ana', 'Order', 'Symbol'])

        # todo Get signals and ana_factors for all non-holding stocks
        print('# todo P2 S2.1 Get signals and ana_factors for all non-holding stocks ')
        for j2 in range(len(symbolList)):
            temp_Sig_Ana.loc[j2, 'Order'] = j2  # todo 在symbolList里的序号
            temp_Symbol = symbolList[j2]
            temp_Sig_Ana.loc[j2, 'Symbol'] = temp_Symbol

            # todo 884079.WI 指数中有 200055.SZ	方大B， 这种奇怪的 B股，需要剔除 | 900952.SH
            if not temp_Symbol[:2] in ['20', '90','13','12'] :
                temp_Ana = 1  # temp_Ana = 1 means useless ,as smaller as good
                # print('170426 1446, ifHold ', StockPool.loc[j2, 'ifHold'] ,StockPool.loc[j2, 'ifHold'] == 0  )
                if StockPool.loc[j2, 'ifHold'] == 0:

                    # todo P2 S3.2  Get_Signal ideal
                    # For future, if we want real-time signal, then we need to use lastest price to replace Last Day price
                    (temp_Signal, temp_Date_Index) = self.Get_Signal_NoAmt(temp_Symbol, Path_Data, temp_LastDay)

                    # todo P2 S3.3 Get_BSH_Weight_Amount : Portfolio角度，获取 买卖信号，权重，金额
                    # Find weight, Check Account Get price, Get holding, Do accounting
                    # (temp_BSH, temp_Weight, temp_Amount) = self.Get_BSH_Weight_Amount(temp_Signal_Index, temp_Signal,
                    #                                                                   temp_Symbol, StockPool, MaxN,
                    #                                                                   Account_Sum,temp_Date,
                    #                                                                   Leverage)
                    # todo Ana 感觉这里还不需要用到 Get_BSH_Weight_Amount 和 指数信号 temp_Signal_Index
                    temp_BSH = temp_Signal

                    temp_Ana = 1  # temp_Ana = 1 means useless ,as smaller as good
                    # todo 只保留买入信号
                    if temp_BSH == 1:
                        temp_Sig_Ana.loc[j2, 'Signal'] = 1
                        # todo  Get ana_factors, p_H40
                        MA_x = [3, 8, 16, 40, 100]
                        temp_Ana = self.Get_AnaFactors(temp_Symbol, Path_Data, temp_Date, MA_x)
                        # print('temp_Ana 170308 ', temp_Ana)
                        temp_Sig_Ana.loc[j2, 'temp_Ana'] = temp_Ana * -1
                        print(temp_Date, " | ", temp_Symbol, " | Signal: ", temp_Signal, 'temp_Sig_Ana ', temp_Ana)
            else:
                temp_BSH = 0
                temp_Sig_Ana.loc[j2, 'Signal'] = 0
                temp_Sig_Ana.loc[j2, 'temp_Ana'] = 0

                # 买入信号数量大于对 最多可以买入的股票数量 ：sum( temp_Sig_Ana[0].values ) > temp_Num_dif
            # 买入信号数量小于对 最多可以买入的股票数量 ：sum( temp_Sig_Ana[0].values ) < temp_Num_dif
            # if temp_Sig_Ana[:,1]=[-0.1, -0.2, -0.3, -0.07, 0, -0.25] :
            # temp_Sort = [ 2, 5, 1, 0, 3, 4         ]
            # temp_Sig_Ana[:,1][temp_Sort] = [-0.3, -0.25, -0.2, -0.1, -0.07, 0]

        # todo P2 S3.4 策略排序 ： 初步发现，按照从小到大的排，优先买入的都是涨幅较少的股票，结果就是波动贴近指数
        # 数值越小越好，从小到大排列， 数值1表示无意义；temp_Sort is an index list  from small to large
        temp_Sort = np.argsort(temp_Sig_Ana['temp_Ana'].values, axis=0)  # 从小到大的值的index
        # temp_Sort = np.sort(temp_Sig_Ana[1].values, axis=0) # 从小到大的值
        # todo 未来也可以按照其他指标排序，比如roe， PCF，vol ，amt等

        return temp_Sig_Ana, temp_Sort

    def Get_Sig_Ana_Next(self, symbolList, Path_Data,temp_Date, StockPool  ):
        '''
        根据N日 temp_Date的数据，计算 N+1日的买卖信号
        Derived from Get_Sig_Ana
        last update 170404 2153 | since 170404 2153
        '''
        # todo 计算 temp_Sig_Ana 和 temp_Sort，将可买入的股票按顺序排列
        # todo last update 1704022208 | sicne 1704022208
        temp_Sig_Ana_Next = pd.DataFrame(np.zeros([len(symbolList), 4]), columns=['Signal', 'temp_Ana', 'Order', 'Symbol'])

        # todo P3 S3.2  Get signals and ana_factors for all non-holding stocks
        for j2 in range(len(symbolList)):
            temp_Sig_Ana_Next.loc[j2, 'Order'] = j2  # todo 在symbolList里的序号
            temp_Symbol = symbolList[j2]
            temp_Sig_Ana_Next.loc[j2, 'Symbol'] = temp_Symbol

            temp_Ana = 1  # temp_Ana = 1 means useless ,as smaller as good
            if StockPool.loc[j2, 'ifHold'] == 0:

                # todo P3 S3.2.1  Get_Signal ideal
                # For future, if we want real-time signal, then we need to use lastest price to replace Last Day price
                [temp_Signal_Next,temp_Date_Index_Next]= self.Get_Signal_Next( temp_Symbol, Path_Data, temp_Date)

                temp_BSH_Next = temp_Signal_Next

                # todo 只保留买入信号
                if temp_BSH_Next == 1:
                    temp_Sig_Ana_Next.loc[j2, 'Signal'] = 1
                    # todo  Get ana_factors, p_H40
                    MA_x = [3, 8, 16, 40, 100]
                    # todo From Get_AnaFactors to Get_AnaFactors_Next
                    temp_Ana = self.Get_AnaFactors_Next(temp_Symbol, Path_Data, temp_Date, MA_x)

                    # print('temp_Ana 170308 ', temp_Ana)
                    temp_Sig_Ana_Next.loc[j2, 'temp_Ana'] = temp_Ana * -1
                    print('Get_Sig_Ana_Next', temp_Date, " | ", temp_Symbol, " | Signal: ", temp_Signal_Next , 'temp_Sig_Ana ', temp_Ana)


        # todo P3 S3.2  策略排序 ： 初步发现，按照从小到大的排，优先买入的都是涨幅较少的股票，结果就是波动贴近指数
        # 数值越小越好，从小到大排列， 数值1表示无意义；temp_Sort is an index list  from small to large
        temp_Sort_Next = np.argsort(temp_Sig_Ana_Next['temp_Ana'].values, axis=0)  # 从小到大的值的index
        # temp_Sort = np.sort(temp_Sig_Ana[1].values, axis=0) # 从小到大的值
        # todo 未来也可以按照其他指标排序，比如roe， PCF，vol ，amt等

        return temp_Sig_Ana_Next , temp_Sort_Next

    def Get_Weight(self, temp_Symbol , i,weight_Method, MaxN,temp_Date_Index=1 ) :
        # todo 对于“好”，“普通”，“差”的股票，要分配给不同的权重，并且单只股票可以在3种权重中有机会切换。
        # todo 目的是避免0700.HK_vs_HSI.HI ; Nasdaq_VS_Appl. 这样，单只股票上涨几十倍，但指数只上涨小几倍以内的情况。
        # todo 权重分配时还应该考虑个股之间，个股和基准指数之间的相关性。
        # todo 卖出股票/降低权重的3原因：基本面恶化，价格过高，有更好的选择。
        # temp_Symbol='000300.SH', i=1, weight_Method, MaxN,temp_Date_Index=1 ) :
        # 获取理想ideal 投资权重( 资产配置 ), how?
        # 权重条件： 初始等权重，动态等权重，按市值/PB/PE/roe/,波动率/ 近期涨跌幅度等分配权重
        temp_Weight = 0
        if weight_Method == 'equalWeight_ini':
            temp_Weight = 1 / MaxN

        return temp_Weight

    def Get_AnaFactors(self, temp_Symbol, Path_Data, temp_Date, MA_x) :
        '''
        Get_AnaFactors 计算的是N-1日的价量分析数据
        last update 170404 2209 | since 2016
        '''
        print('Get_AnaFactors')
        # 161018
        #  MA_x = [3, 8, 16, 40, 100]
        # p_H40=close/H40-1
        # 过去40天股价未大涨，或者处于回调状态，即便是腾讯，也有许多这样的买入时机，如151006，160301等
        # todo Part 1 Get raw data
        (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_Date, Path_Data )
        # data_ALLdays.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        # type of data_ALLdays is Dataframe
        # 170522
        # print('temp_Date_Index len(data_ALLdays) MA_x[-1] ',temp_Symbol,  temp_Date_Index, data_ALLdays , MA_x[-1] )
        # todo Part 2 Get Ana_Data if temp_Date_Index is fine and enough length of data_ALLdays
        if temp_Date_Index < 80000 and len(data_ALLdays) >= MA_x[-1] + 3:
            # Today is data_ALLdays['date'].index[-1]
            # Lastday is data_ALLdays['date'].index[-2]
            temp_Index1 = data_ALLdays['date'].index[-2]

            # todo 若尾数是N-th日, pd.iloc[-41:-1] 对应的是 N-41 至 N-1，N日之前的40天
            H40 = max(max(data_ALLdays['close'].iloc[-41:-1].values), max(data_ALLdays['open'].iloc[-41:-1].values))
            # todo 170404 下边这个会错过 data_ALLdays[-2]当天的数据，只用了前38天的数据
            #H40= max( max(data_ALLdays['close'].iloc[-40:-2].values),max(data_ALLdays['open'].iloc[-40:-2].values) )

            if H40>0 :
                p_H40 =  data_ALLdays['close'].iloc[-1]/H40-1
        else :
            p_H40 = 100

        return p_H40

    def Get_AnaFactors_Next(self, temp_Symbol, Path_Data, temp_Date, MA_x) :
        '''
        Get_AnaFactors_Next 计算的是 N日/下一个交易日的分析数据，Get_AnaFactors 计算的是N-1日的价量分析数据

        Derived from Get_AnaFactors
        last update 170404 2200 | since 170404 2200
        '''
        print('Get_AnaFactors_Next')
        # p_H40=close/H40-1
        # 过去40天股价未大涨，或者处于回调状态，即便是腾讯，也有许多这样的买入时机，如151006，160301等
        # todo Part 1 Get raw data
        (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret_Next( temp_Symbol, temp_Date ,Path_Data  )
        # todo data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日

        # todo Part 2 Get Ana_Data if temp_Date_Index is fine and enough length of data_ALLdays
        if temp_Date_Index < 80000 and len(data_ALLdays) >= MA_x[-1] + 3:
            # todo 若尾数是N-th日, pd.iloc[-41:-1] 对应的是 N-41 至 N-1，N日之前的40天
            H40= max( max(data_ALLdays['close'].iloc[-41:-1].values),max(data_ALLdays['open'].iloc[-41:-1].values) )
            # print('H40:', H40 )
            if H40>0 :
                p_H40 =  data_ALLdays['close'].iloc[-1]/H40-1
        else :
            p_H40 = 100

        return p_H40

    def Check_Account(self,i, temp_Signal, Account_Sum ,w_SL_j  , Leverage ,weight_ideal ) :
        # todo last check 161218
        # Account_Sum; Columns_Sum = ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']  # 8 items
        # 买卖信号 , buy 1 from [0,1]  |  hold 0 from [1,1]  |  sell -1 from [1,0]
        print( 'Check_Account')
        # print(temp_Signal, Account_Sum[ 'Cash'][i] ,Leverage * Account_Sum[ 'Total'][i-1])

        print('Available cash level ', Account_Sum[ 'Cash'][i]/ (Leverage * Account_Sum[ 'Total'][i]), '| ',(weight_ideal-w_SL_j )+0.005 )
        # print('w_SL_j ', w_SL_j , 'weight_ideal*0.8', weight_ideal*0.8)
        if temp_Signal == 1 :
            # Buy if weight_SL_j < weight_ideal and cash is enough
                # 计算理想持仓比例
            if w_SL_j < weight_ideal*1.2 and Account_Sum[ 'Cash'][i]/ (Leverage * Account_Sum[ 'Total'][i]) >= (weight_ideal-w_SL_j )+0.005 :
                result_BSH = 1
            else :
                result_BSH = 0
        elif temp_Signal == 0 :
                result_BSH = 0
        elif temp_Signal == -1 :
            # temp_Signal == -1 :
                result_BSH = -1

        return result_BSH

    def Update_Trading_Book( self, temp_Trade , Trading_Book ) :
        # todo Last modified :161218_1609
        # Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        # 根据核对账户后的买卖指令Order 进行下单操作。
        # rate_Fees= 0.0025
        # 160914  根据BSH信号，更新交易记录/Trading_Book
        # todo Part 1 判断TradingBook 是否已满，如果满了，我们为它加几页
        print('Update_Trading_Book')
        Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        # last_Trade = pd.DataFrame(np.zeros([1, 9]), columns=Columns_Trade)

        # todo Decode temp_Trade | last update 170401
        temp_Date = temp_Trade.loc[0,'Date']
        temp_Symbol = temp_Trade.loc[0,'Symbol']
        temp_BSH = temp_Trade.loc[0,'Buy/Sell']
        # temp_Trade['Price'] = data_1day['open']
        temp_Number = temp_Trade.loc[0,'Number']
        temp_AveCost = temp_Trade.loc[0,'AveCost']
        temp_Fees = temp_Trade.loc[0,'Fees']
        temp_ProfitReal = temp_Trade.loc[0,'ProfitReal']

        # todo Count number of Trading_Book['Number']>0
        temp_TB_Index = sum(Trading_Book['Number'].iloc[:] > 0)  # todo todo
        # print('170828 2241 Trading_Book', Trading_Book)
        if len(Trading_Book.index ) <1 or Trading_Book['Number'].iloc[-1]>0  :
            # todo The Trading_Book is full, we Append the Trading Book with 100 rows
            # we extend the length of Trading_Book with 100 entries
            Trading_Book=np.concatenate((Trading_Book , np.zeros([100, 9]) ), axis=0)
            Trading_Book = pd.DataFrame(Trading_Book, columns=Columns_Trade)

        # todo Part 2 Trade orders :已经check过账户和市场状态，是可以交易的
        # 买卖信号 , buy 1 from [0,1]  |  hold 0 from [1,1]  |  sell -1 from [1,0]
        # Trading_Book | Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']

        if temp_BSH == 1  :
            # if temp_TB_Index >= 100 :
            #     print( Trading_Book.head(temp_TB_Index) )
            #     Path_TradeSys = 'D:\\rC_Py3_Output\\TradeSys_0'
            #     Trading_Book.to_csv(Path_TradeSys + '\\' + 'Trading_Book' + '_temp' + '.csv')
            print( 'temp_TB_Index', temp_TB_Index )
            print( len(Trading_Book['Date']) )

            Trading_Book.loc[temp_TB_Index, 'Date' ] = temp_Date
            Trading_Book.loc[temp_TB_Index,'Symbol' ] = temp_Symbol
            Trading_Book.loc[temp_TB_Index,'Buy/Sell' ] = 1
            # Get symbol price at specific day
            # (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_Date, Path_Data)
            Trading_Book.loc[temp_TB_Index,'Price' ] = temp_Trade.loc[0,'Price']
            # Number of stocks to buy  todo todo todo
            # 股票总成本，平均成本，股票数量: Stocks_Cost[i][j]   Stocks_aveCost[i][j]    Stocks_Num[i][j]
            Trading_Book.loc[temp_TB_Index,'Number' ] = temp_Number
            # Trading_Book | Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
            # todo Trading_Book['AveCost'][temp_TB_Index] 指的是当前交易的平均成本
            Trading_Book.loc[temp_TB_Index,'AveCost' ] = temp_AveCost
            Trading_Book.loc[temp_TB_Index,'Fees' ] = temp_Fees
            Trading_Book.loc[temp_TB_Index,'ProfitReal' ] = temp_ProfitReal

        elif temp_BSH == -1  :
            Trading_Book.loc[temp_TB_Index,'Date' ] = temp_Date
            Trading_Book.loc[temp_TB_Index,'Symbol' ] = temp_Symbol
            Trading_Book.loc[temp_TB_Index,'Buy/Sell' ] = -1

            Trading_Book.loc[temp_TB_Index,'Price' ] = temp_Trade.loc[0,'Price']
            # 股票总成本，平均成本，股票数量: Stocks_Cost[i][j]   Stocks_aveCost[i][j]    Stocks_Num[i][j]
            Trading_Book.loc[temp_TB_Index,'Number' ] = temp_Number
            # Trading_Book | Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
            Trading_Book.loc[temp_TB_Index,'AveCost' ] = temp_AveCost
            Trading_Book.loc[temp_TB_Index,'Fees'  ] = temp_Fees
            Trading_Book.loc[temp_TB_Index,'ProfitReal' ] = temp_ProfitReal

        return Trading_Book

    def Get_Price_Ret( self, temp_Symbol, temp_LastDay ,Path_Data  ) :
        '''
        # todo Given temp_LastDay is N-1 day, data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N-1日，不包括N 日
        # last update 170413 |　170403 1541
        # todo Last Check 161229 : 测试 300017.SZ 141212-141215时，有发生返回的data2是空集的情况。
        # todo Last Check 我们要拿到截止 temp_Date 的历史价格数据
        # path_Data = path_Input_Wind
        '''
        import os
        # To get price from csv file
        # Path_Data = 'D:\\rC_Py3_Output'
        # Path_TradeSys = 'D:\\rC_Py3_Output\\TradeSys_0'
        # todo 170507 if '_updated' existed , use '_updated', otherwise :
        # todo '_updated'  导出的csv是有index， 原版的是没有的
        file_path = Path_Data + '\Wind_' + temp_Symbol + '_updated' + '.csv'
        if not os.path.isfile(( file_path )):
            # 如果不存在  '_updated' ，则用普通状态的
            file_path = Path_Data + '\Wind_' + temp_Symbol  + '.csv'
            data = pd.read_csv(file_path, header=None, skiprows=1, sep=',')

        else :
            # todo case  '_updated'
            print( temp_Symbol )
            data = pd.read_csv(file_path, header=None, skiprows=1, sep=',')
            # data.index = data[0]
            # data = data.drop([0], axis=1)
            # print( data.tail(3))

        if len( data.columns ) == 9 :
            # we need to drop first columns
            data = data.drop( [0] ,axis=1 )
        # print( data.head(5) )
        data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        data2 = data.dropna(axis=0)
        # print( temp_LastDay )
        # print( data2.tail(5) )
        # print( 'Get_Price_Ret ')
        # print('file_path', file_path)
        # print('Length of data2 is ', len(data2) )
        # print( data2['date'].head(1).values + '   ,   ' + data2['date'].tail(1).values)
        # print('==== Get correct temp_Date_Index ====')
        # print( data2['date'][temp_Date_Index] )  # '20150421'　　'20160921'
        # print( temp_Date   )  #　'20160110''
        # print('161006 Get_Price_Ret  ')
        # print( len(data2) )
        # Local step1 get index-temp_Date_Index in DataFrame

        # todo modified in 161212_0142
        # temp_Date_Index = self.Get_Date_Index(data2 ,temp_Date)
        # print('170426 1538 data2 ',data2.tail(8),temp_LastDay )

        temp_123 = data2[data2['date'].isin([temp_LastDay])]
        # todo 161229 当在 data2 里找不到 temp_LastDay 时， 会出现empty 的 temp_123
        if len(temp_123) >0 :
            temp_Date_Index = temp_123['date'].index[0] - data2['date'].index[0] # 7000 - 6401

            # Local step2 use index number to get values in DataFrame
            temp_Index0 = data2['date'].index[0]
            # print('Latest day', data2['date'].index[-1])
            if temp_Date_Index>= 80000 :
                print('Get_Price_Ret | We cannot find temp_Date in data2[date]')
                # data_1day = data2[temp_Index0 :temp_Index0 ]
                data_1day = 0
                data_ALLdays = 0

                # return empty data_1day and data_ALLdays  if temp_Date_Index=80000
            else :
                print('Get_Price_Ret | Get correct temp_Date_Index ====')
                # todo 161022_1214 Todo task is the empty DataFrame of data_1day and data_ALLday
                #     print(len(data_1day)) == 1
                # data_1day = data2[temp_Index0 + temp_Date_Index-1:temp_Index0 + temp_Date_Index ]
                # print(data_1day) =  print( data_ALLdays.tail(1) )
                # print('Now we want to print data_1day')
                # data_1day = data2[temp_Index0 + temp_Date_Index :temp_Index0 + temp_Date_Index]
                # print( data2.iloc[160:170] )
                # print( temp_Index0 + temp_Date_Index )
                # todo Given temp_LastDay is N-1 day, data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N-1日，不包括N 日
                data_1day = data2.ix[temp_Index0 + temp_Date_Index]
                # print( data_1day )

                data_ALLdays = data2.ix[temp_Index0: temp_Index0 +temp_Date_Index ]
                # print( len(data_ALLdays))
                # print( type(data_ALLdays))
                # data_ALLdays = data2[temp_Index0  :temp_Index0 + temp_Date_Index]
        else :
            data_1day = 0
            data_ALLdays= 0
            temp_Date_Index = 80000
        # data_1day; data_ALLdays .columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        # print('temp_Date | ', temp_Date)
        # print('Current_Day | ', data2['date'][ temp_Index0 + temp_Date_Index-1]  )
        # print('First trading day of this symbol | ', temp_Index0, data2['date'][temp_Index0] )
        # print('All trading day until last entry ', len(data2) )
        # print('Dates:| ', temp_Index0 + temp_Date_Index-1 ,' | ' , temp_Index0 + temp_Date_Index )

        # return empty data_1day and data_ALLdays  if temp_Date_Index=80000
        return data_1day,data_ALLdays , temp_Date_Index

    def Get_Price_Ret_Next( self, temp_Symbol, temp_Date ,Path_Data  ) :
        '''
        todo # 171130 update data_ALLdays from end with N-1 day "temp_Index0 +temp_Date_Index" to N day temp_Index0 +temp_Date_Index+1
        Output : data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日
         Get_Price_Ret_Next :
        # todo Given temp_LastDay is N-1 day, data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日
        derived from  Get_Price_Ret, where: data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N-1日，不包括N 日
        # last update 170403 1541 | since 170403
        # todo Last Check 161229 : 测试 300017.SZ 141212-141215时，有发生返回的data2是空集的情况。
        # todo Last Check 我们要拿到截止 temp_Date 的历史价格数据
        '''
        import os
        file_path = Path_Data + '\Wind_' + temp_Symbol + '_updated' + '.csv'
        if not os.path.isfile((file_path)):
            # 如果不存在  '_updated' ，则用普通状态的
            file_path = Path_Data + '\Wind_' + temp_Symbol + '.csv'
            data = pd.read_csv(file_path, header=None, skiprows=1, sep=',')

        else:
            # todo case  '_updated'
            print(temp_Symbol)
            data = pd.read_csv(file_path, header=None, skiprows=1, sep=',')
            # data.index = data[0]
            # data = data.drop([0], axis=1)
            # print(data.tail(3))

        if len( data.columns ) == 9 :
            # we need to drop first columns
            data = data.drop( [0] ,axis=1 )

        data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        data2 = data.dropna(axis=0)
        # todo ====
        #
        # file_path = Path_Data + '\Wind_' + temp_Symbol + '.csv'
        # data = pd.read_csv(file_path, header=None, skiprows=1, sep=',')
        # data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        #
        # data2 = data.dropna( axis=0)

        temp_123 = data2[data2['date'].isin([temp_Date])]
        # todo 161229 当在 data2 里找不到 temp_Date 时， 会出现empty 的 temp_123
        if len(temp_123) >0 :
            temp_Date_Index = temp_123['date'].index[0] - data2['date'].index[0] # 7000 - 6401

            # Local step2 use index number to get values in DataFrame
            temp_Index0 = data2['date'].index[0]
            # print('Latest day', data2['date'].index[-1])
            if temp_Date_Index>= 80000 :
                print('Get_Price_Ret | We cannot find temp_Date in data2[date]')
                # data_1day = data2[temp_Index0 :temp_Index0 ]
                data_1day = 0
                data_ALLdays = 0

                # return empty data_1day and data_ALLdays  if temp_Date_Index=80000
            else :
                print('Get_Price_Ret | Get correct temp_Date_Index ====')
                # todo Ana 170403 : 因为这里的Input从 temp_LastDay 变成了 temp_Date
                # todo Given temp_Date is day N，temp_LastDay is day N-1, data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日
                # todo data_1day = data2.ix FROM [temp_Index0 + temp_Date_Index ] TO [temp_Index0 + temp_Date_Index -1 ]
                print('170505 0813,temp_Index0 + temp_Date_Index -1 ', temp_Index0+ temp_Date_Index - 1 )
                # print(data2.ix[ 6421])
                # print( data2.ix[temp_Index0 + temp_Date_Index - 1] )

                # print('170505 0813,data2 ', data2.tail(5))
                # todo 170510 注意：如果前一交易日停牌，有可能没有该index
                temp_index = temp_Index0 + temp_Date_Index -1
                if temp_index in data2.index :
                    data_1day = data2.ix[ temp_index ]
                    # print( data_1day )
                    # todo Before: data_ALLdays = data2.ix[temp_Index0: temp_Index0 +temp_Date_Index]
                    # todo Now   : data_ALLdays = data2.ix[temp_Index0: temp_Index0 +temp_Date_Index]
                    # todo Ana 170403 : 因为这里的Input从 temp_LastDay 变成了 temp_Date
                    # 171130 update data_ALLdays from end with N-1 day "temp_Index0 +temp_Date_Index" to N day temp_Index0 +temp_Date_Index+1
                    data_ALLdays = data2.ix[temp_Index0: temp_Index0 +temp_Date_Index+1 ]
                else :
                    data_1day = 0
                    data_ALLdays = 0
                    temp_Date_Index = 80000
        else :
            data_1day = 0
            data_ALLdays= 0
            temp_Date_Index = 80000

        return data_1day,data_ALLdays , temp_Date_Index

    def Get_Date_Index(self , data2 , temp_Date  )  :
        # Last: 161212
        # # todo 要在data['code'] 定位 temp_Date 未完成
        temp123 = pd.DataFrame(np.zeros([0, 8]) )

        temp_123 = data2[data2['date'].isin([temp_Date])]
        # temp_123 = data2[data2['date'].isin([temp_Date])]
        # if it goes well, we should have temp_123   :
        #         date      open      high       low     close     volume   amt   pct_chg
        # 6205  2016-05-04  6.460108  6.488039  6.396265  6.408235  2086234.0   6205  33652324.7 -0.802965
        if len(temp123 ) ==1 :
            temp_Index0 = data2['date'].index[0]
            temp_Date_Index = temp123['date'].index[0] - temp_Index0
        else :
            temp_Date_Index = 80000
            print('We cannot find temp_Date in data2[date]')

        return temp_Date_Index

    def Get_Date_Index_old(self , data2 , temp_Date  )  :
        # Last Modified : 161212
        # 例子：3次循环，可以从1991-12-20 调整到2016-5-24
        # 在个股交易日期中寻找公共日期的位置,根据temp_Date 来调整 temp_Date_Index
        # todo 161004 解决2变量之间日期字符串的文本不匹配问题
        # todo At temp_Date, we find the index of temp_Date in data2['date'] , or
        #   todo we find next available trading day that has record in data2['date']
        # todo Key problem: we need to find available day at or after temp_Date in data2['date']
        # if temp_Date =2012-12-31 but it is a Sunday ,holiday, or stock do not have transactions that day, then we need to find next available day
        print('Get_Date_Index | ', temp_Date)
        temp_Date_Index = round(len(data2['date'])/2)
        # todo Step 1 transfer  date format of temp_Date(i.e. 2016-5-24)
        import time
        # print( 'rC 161006 1130' )
        # print(temp_Date)
        temp_t1 = time.strptime( temp_Date, "%Y-%m-%d")



        # temp_Date is the key trading day for whole portfolio , i.e. 2012-9-14
        # print( len(data2['date']) )
        # todo Notice that temp_Index is the initial index of data2['date'].index[0]
        temp_Index0 = data2['date'].index[0]
        # print( temp_Index )
        # print( temp_Date_Index )
        # print( data2['date'][temp_Date_Index] ) 会出错是因为 data2的Index 是从 6001开始，到6308的
        # So we need :  print( data2['date'][temp_Index0 + temp_Date_Index] )

        temp_t2 = time.strptime( data2['date'][temp_Index0 + temp_Date_Index], "%Y-%m-%d")
         # temp_t2[0]=2009  temp_t2[1]=11  temp_t2[2]= 25      , i.e. 2009-11-2

        # Strategy: iteratively narrow down the difference from year, month and day.
        # todo 161212 发现一个大bug，20次不一定能保证年，月，日时间都调整对
        iter_Num = 20
        global iN
        iN=0
        while ( iN < iter_Num) :
        # for iN in range( iter_Num ) :

            delta_trade_day= 0
            # 现在时间temp_Date 减去 序列中的定位时间
            dif_year=temp_t1[0]-temp_t2[0]
            dif_mon = temp_t1[1] - temp_t2[1]
            dif_day = temp_t1[2] - temp_t2[2]

            # todo Step 1 Adjust year

            if dif_year +dif_mon+dif_day == 0 :
                iN = iN+ iter_Num+10
                print('dif_year', dif_year, 'dif_mon', dif_mon, 'dif_day', dif_day)
            elif dif_year > 0:
                delta_trade_day = -210 * dif_year
                iN = iN +1
            elif dif_year < 0:
                delta_trade_day = 210 * dif_year
                iN = iN +1
            elif dif_mon > 0:
                delta_trade_day = delta_trade_day - 20 * dif_mon
                iN = iN + 1
            elif dif_mon < 0:
                delta_trade_day = delta_trade_day + 20 * dif_mon
                iN = iN  + 1
            elif dif_day > 0:
                # todo !! dif_day的这段时间，如果10天，可能只有6~8个交易日，因为 每周只有5个交易日
                # delta_trade_day = delta_trade_day - round( (dif_day-1)*5/7+1)
                delta_trade_day = delta_trade_day + (round( dif_day/7 )*5 + dif_day%7)
                iN = iN +1
            elif dif_day < 0:
                delta_trade_day = delta_trade_day - (round( -1*dif_day/7 )*5 + (-1*dif_day)%7)
                iN = iN +1
            print('dif_year', dif_year, 'dif_mon', dif_mon, 'dif_day', dif_day)
            temp_Date_Index = temp_Date_Index - delta_trade_day
            # todo 下边这一行非常重要，避免data2 出现数据溢出
            temp_Date_Index = min(temp_Date_Index, data2['date'].index[-1] - temp_Index0)
            temp_t2 = time.strptime(data2['date'][temp_Index0 + temp_Date_Index], "%Y-%m-%d")

        print('temp_Index0 + temp_Date_Index', temp_Index0 + temp_Date_Index, 'temp_Date_Index', temp_Date_Index, )

            # if temp_t1 < temp_t2 :
            #     # add value of temp_Date_Index until temp_t1== temp_t2
            #     # dif_number> len(data2) if we use 252 trading days per year
            #     delta_trade_day = -232 * dif_year * (dif_year< 0) - 20 * dif_mon * (dif_mon <0) + 22 * dif_mon * (dif_mon > 0) - dif_day * (dif_day < 0) + dif_day * (dif_day > 0)
            #     # print( 'temp_t1 < temp_t2' )
            #     # print(delta_trade_day)
            #     temp_Date_Index = temp_Date_Index -delta_trade_day
            #     temp_Date_Index = min(temp_Date_Index, data2['date'].index[-1] - temp_Index0)
            #     # print(data2['date'][temp_Date_Index])
            #     temp_t2 = time.strptime(data2['date'][temp_Index0 +temp_Date_Index], "%Y-%m-%d")
            #     # print(time.strftime("%Y-%m-%d", temp_t1))
            # elif temp_t2 < temp_t1 :
            #     # t2< t1, t2= 1990-12-20 , t1= 2016-5-24
            #     # 6422_dif_number> 6309_len(data2) if we use 252 trading days per year
            #     delta_trade_day = 232 * dif_year * (dif_year > 0) + 20 * dif_mon * (dif_mon > 0) - 22 * dif_mon * (dif_mon < 0) + dif_day * (dif_day > 0) - dif_day * (dif_day < 0)
            #     # print('temp_t1 > temp_t2')
            #     # print( dif_day )
            #     # print(delta_trade_day)
            #     temp_Date_Index = temp_Date_Index + delta_trade_day
            #     # Sometimes we meet : temp_Index0 +temp_Date_Index=6323 while data2['date'].index[-1]= 6308
            #     temp_Date_Index = min( temp_Date_Index ,data2['date'].index[-1]-temp_Index0 )
            #     # print(data2['date'][temp_Date_Index])
            #     # print('Get_Date_Index inside L489:')
            #     # print( data2['date'].index[-1] )
            #     # print(temp_Index0 +temp_Date_Index)
            #     temp_t2 = time.strptime(data2['date'][temp_Index0 +temp_Date_Index], "%Y-%m-%d")
            #     # print(time.strftime("%Y-%m-%d", temp_t1 ))
            # else :
            #     i=20 # Usually 2~5 iterations is enough to match temp_t2 to temp_t1

        # print('temp_t1', time.strftime("%Y-%m-%d", temp_t1 ) )
        # print('temp_t2', time.strftime("%Y-%m-%d", temp_t2 ) )
        print( temp_Date_Index )
        if temp_t1 != temp_t2 :
            temp_Date_Index = 80000
            print('We cannot find temp_Date in data2[date]')
            print(temp_t1)
            print(temp_t2)
        return temp_Date_Index

    def Get_Number( self, rate_Fees, temp_BSH, temp_Amount, data_1day,preClose,Stocks_Cost_ij ,Stocks_aveCost_ij , Stocks_Num_ij,Account_Sum_Cash_i, Account_Sum_Total_i,Leverage )  :
        # todo Last modified :171215, 161218  ;161210 2308
        # todo 171215, 开盘价需要和昨日收盘价比较
        # todo 根据可用资金，持仓数量和行情价量，计算出交易数量，新的总成本，新平均成本，交易费用和卖出盈亏。
        # todo "temp_AveCost" 等不应该在这里提前设置成 0，而是应该引用导入的持仓数据
        # To get price from csv file
        # data_1day.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        temp_Number = 0
        temp_TotalCost = Stocks_Cost_ij
        temp_AveCost = Stocks_aveCost_ij
        temp_Fees= 0
        temp_ProfitReal=0
        print('Get_Number')
        # print( data_1day['open'] )
        if temp_BSH ==1 and (data_1day['amt'] > 5000000 and (data_1day['open']<= preClose*1.0985 and preClose>0 ) ) :
            # todo 161106 发现下面一行的问题，没有和0值比较，会出现负数！
            temp_Amount = min(temp_Amount, Account_Sum_Cash_i - Account_Sum_Total_i * (1 - Leverage))
            temp_Number = round( temp_Amount /100/data_1day['open'] )*100 - Stocks_Num_ij
            # todo 161210 等权重情况下 temp_Amount  是可以投资于当前股票的最大数量，减去现有数量 Stocks_Num_ij,剩余数量就是
            # todo 可以买入的数量。
            if temp_Number > 100 :
                # 我们希望当天成交金额至少是买入金额的3倍,才能满足交易需要;否则需要调整购买数量
                if data_1day['amt'] < temp_Number * data_1day['open'] * 3 :
                    temp_Factor = 0.3
                    temp_Number = round(data_1day['amt'] * temp_Factor / 100 / data_1day['open']) * 100

                temp_TotalCost =   data_1day['open'] * temp_Number
                # temp_AveCost = (Stocks_Cost_ij + data_1day['open'] * temp_Number) / (Stocks_Num_ij + temp_Number)
                # todo temp_TotalCost 只针对当前这一笔交易的总成本 , temp_AveCost   只针对当前这一笔交易的平均成本
                temp_AveCost =  data_1day['open']
                temp_Fees = data_1day['open'] * temp_Number * rate_Fees
            else :
                # No trade then
                temp_Number = 0

        elif temp_BSH == -1 and (data_1day['amt'] > 5000000  and ( data_1day['open']>= preClose*0.905 and preClose>0 )) :
            temp_Number = Stocks_Num_ij
            # 不一定能一次性卖光的情况：
            if data_1day['amt'] < temp_Number * data_1day['open']  * 3 :
                # Here we can sell only part of our holding number
                temp_Factor = 0.3
                temp_Number = round( data_1day['amt']*temp_Factor/100/data_1day['open']  )*100
                # Here we can sell all our holding number

            # Check if we have negative number here
            if temp_Number >0 :
                # todo 161218之前的问题在于下边一行的Stocks_aveCost_ij，初始化时都变成 0 了
                temp_AveCost = Stocks_aveCost_ij
                # todo 只计算可卖出部分temp_Number,而不是 全部数量Stocks_Num_ij 的：
                # todo temp_TotalCost  ; temp_Fees ;  temp_ProfitReal
                temp_TotalCost =  Stocks_aveCost_ij * temp_Number
                temp_Fees = data_1day['open']  * temp_Number * rate_Fees
                temp_ProfitReal = data_1day['open']*temp_Number - temp_TotalCost
                print('temp_Number temp_AveCost temp_Fees temp_ProfitReal ', temp_Number, temp_AveCost, temp_Fees, temp_ProfitReal)
            else:
                temp_Number = 0

        return temp_Number, temp_TotalCost, temp_AveCost , temp_Fees , temp_ProfitReal

    def Get_Number_Next( self, rate_Fees, temp_BSH, temp_Amount, data_1day, Stocks_Cost_ij ,Stocks_aveCost_ij , Stocks_Num_ij,Account_Sum_Cash_i, Account_Sum_Total_i,Leverage )  :
        '''
        根据N日收盘价和持仓信息，计算明日N+1 日交易具体信息；这里 data_1day代表N日
        Derived from Get_Number
        # todo Last modified :170404 2114 | 161218  ;161210 2308
        # todo 根据可用资金，持仓数量和行情价量，计算出交易数量，新的总成本，新平均成本，交易费用和卖出盈亏。
        # todo "temp_AveCost" 等不应该在这里提前设置成 0，而是应该引用导入的持仓数据
        '''

        # To get price from csv file
        # data_1day.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        temp_Number = 0
        temp_TotalCost = Stocks_Cost_ij
        temp_AveCost = Stocks_aveCost_ij
        temp_Fees= 0
        temp_ProfitReal=0
        print('Get_Number')
        # print( data_1day['open'] )
        if temp_BSH ==1 :
            # todo 161106 发现下面一行的问题，没有和0值比较，会出现负数！
            temp_Amount = min(temp_Amount, Account_Sum_Cash_i - Account_Sum_Total_i * (1 - Leverage))
            # todo 170404 和Get_Number的区别是 from data_1day['open'] to data_1day['close']
            print('debug 170831 temp_Amount | data_1day ',temp_Amount , data_1day)

            if not type(data_1day) == int  :
                # 171213 2356 to prevent data_1day = 0
                temp_Number = round( temp_Amount /100/data_1day['close'] )*100 - Stocks_Num_ij
                # todo 161210 等权重情况下 temp_Amount  是可以投资于当前股票的最大数量，减去现有数量 Stocks_Num_ij,剩余数量就是
                # todo 可以买入的数量。
                if temp_Number > 100 :
                    # todo 170404 和Get_Number的区别是 from data_1day['open'] to data_1day['close']
                    temp_TotalCost =   data_1day['close'] * temp_Number
                    # temp_AveCost = (Stocks_Cost_ij + data_1day['open'] * temp_Number) / (Stocks_Num_ij + temp_Number)
                    # todo temp_TotalCost 只针对当前这一笔交易的总成本 , temp_AveCost   只针对当前这一笔交易的平均成本
                    temp_AveCost =  data_1day['close']
                    temp_Fees = data_1day['close'] * temp_Number * rate_Fees
                else :
                    # No trade then
                    temp_Number = 0
            else :
                temp_Number = 0

        elif temp_BSH == -1   :
            temp_Number = Stocks_Num_ij
            # 不一定能一次性卖光的情况： 先不考虑
            # Check if we have negative number here
            if temp_Number >0 :
                temp_AveCost = Stocks_aveCost_ij
                temp_TotalCost =  Stocks_aveCost_ij * temp_Number
                # todo 170404 和Get_Number的区别是 from data_1day['open'] to data_1day['close']
                temp_Fees = data_1day['close']  * temp_Number * rate_Fees
                temp_ProfitReal = data_1day['close']*temp_Number - temp_TotalCost
                # print('temp_Number temp_AveCost temp_Fees temp_ProfitReal ', temp_Number, temp_AveCost, temp_Fees, temp_ProfitReal)
            else:
                temp_Number = 0

        return temp_Number, temp_TotalCost, temp_AveCost , temp_Fees , temp_ProfitReal

    def Update_Account_List_Trade( self,temp_Trade, rate_Fees,  data_1day ,Account_Stocks, Account_Sum ,StockPool ,i, Index_Sum  ) :

        # todo Last Check 170916 | since 161218_1702
        # todo 170916 由于交易股票会用temp_DF2 去StockPool中匹配，Live实盘交易中有可能出现交易的股票不在StockPool中，这是就会报错
        #   为了解决这个问题，我们在temp_DF2 找不到对应股票时，将新股票增加到StockPool
        # todo 170401 由于 Account_Sum 已经在日初更新过，当前的资金变动都根据 当前日/temp_Date 来定

        # 161011   We want to update Account_Sum and Account_Stocks
        # todo Update Account_Stocks, Find Symbol in Account_Stocks :
        # Method 1 :匹配字符串  http://jingyan.baidu.com/article/a17d52853379828098c8f222.html
        # pattern = r'[a-z][0-9]'
        # True/False = Account_Stocks['Symbol'].str.contains(pattern)
        # 只显示输出的结果 ：Account_Stocks[ Account_Stocks['Symbol'].str.contains(pattern) ］
        # Method 2 : 严格匹配字符串
        # True/False = Account_Stocks['Symbol'].str.match(pattern, as_indexer=False )
        # Method 3 :tt= qwe['AAB'].loc[ qwe['AAB']=='wqe'] ,tt is a small pd.dataFrame.
        # 在Account_Stocks2 中寻找 temp_Symbol, 如果找得到则 len( temp_pd ) >0

        # from temp_Trade to
        temp_Date = temp_Trade.loc[0,'Date']
        temp_Symbol = temp_Trade.loc[0,'Symbol']
        temp_BSH = temp_Trade.loc[0,'Buy/Sell']
        # temp_Trade['Price'] = data_1day['open']
        temp_Number = temp_Trade.loc[0,'Number']
        temp_AveCost = temp_Trade.loc[0,'AveCost']
        temp_Fees = temp_Trade.loc[0,'Fees']
        temp_ProfitReal = temp_Trade.loc[0,'ProfitReal']

        # todo 这里想要找出 Stock_Pool 中对应代码的 index
        temp_DF2 = StockPool[ StockPool['code'].isin([temp_Symbol])]
        # todo temp_DF2 =  2016-05-10  300317.SZ     0.0     H     0.0      0.0   0.0     0.0     0.0   4       0.0
        # todo Date       code  ifHold B/S/H  W_Real  W_Ideal  Size  Growth  ST_bad  PnL_Last
        # print('161218_1732 temp_DF2', temp_DF2 )
        if len(temp_DF2) == 1 :
            index_SP = temp_DF2.index[0]
        else :
            # 如果StockPool 找不到对应股票时，将新股票增加到StockPool
            print( 'New/Error record in StockPool.', temp_DF2 )
            Columns_StockPool = ['Date', 'code', 'ifHold', 'B/S/H', 'W_Real', 'W_Ideal', 'Size', 'Growth', 'ST_bad',   'PnL_Last']
            # Keyword for future: IPO
            StockPool_1 = pd.DataFrame(np.zeros([1, 10]), columns=Columns_StockPool )
            StockPool_1.loc[0,'Date'] = temp_Trade.loc[0, 'Date']
            StockPool_1.loc[0, 'code'] = temp_Trade.loc[0, 'Symbol']
            StockPool_1.loc[0, 'ifHold' ] = 0
            StockPool_1.loc[0, 'B/S/H'] = 'H'
            StockPool_1.loc[0, 'W_Real'] = 0
            StockPool_1.loc[0, 'W_Ideal'] = StockPool['W_Ideal'].median()
            StockPool_1.loc[0,'Size'] = 0
            StockPool_1.loc[0,  'Growth'] = 0
            StockPool_1.loc[0,'ST_bad'] = 0
            StockPool_1.loc[0,  'PnL_Last'] = 0
            # append StockPool_1 to StockPool
            StockPool = StockPool .append( StockPool_1 , ignore_index=True)
            temp_DF2 = StockPool[StockPool['code'].isin([temp_Symbol])]
            index_SP = temp_DF2.index[0]


            # todo 这里想要找出Account_Stocks中对应代码的 index
            # todo 返回 'E' 列符合 对应值的 rows     df2[df2['E'].isin(['two', 'four'])]
        # temp_DF 返回df的code一列中是 temp_Symbol的名单
        # todo 170226 1708
        temp_DF = Account_Stocks[Account_Stocks['code'].isin([temp_Symbol])]
        # todo     Date       code
        # todo  0  2016-05-18  300317.SZ
        # todo OR, Empty DataFrame

        if len(temp_DF) == 1 :
            index_Account_S = temp_DF.index[0]
        else :
            index_Account_S = 'Error record in Account_Stocks.'
        # print('Update_Account_List_Trade 170623 ')
        # print( temp_Symbol )
        # print( 'index_SP ', index_SP)
        # print('index_Account_S ', index_Account_S)
        # print( temp_Trade )

        # todo 分状态：买入：买入前无持仓，买入前有持仓； 卖出：卖出后无持仓，卖出后有持仓; 持仓不动。
        # Trading_Book | Columns = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        # Account_Stocks | Columns_Stocks = ['Num','AveCost',  'LastPrice','TotalCost', 'MV',  'PnL', 'PnL_Pct', 'W_Real', 'W_Ideal''Date', 'code']  # 11 items
        # Account_Sum |  ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']  # 8 items

        # temp_Trade | Columns = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        if temp_Trade.loc[0,'Buy/Sell' ]  == 1 and len(temp_DF2) == 1 :
            temp_DF3 = Account_Stocks[Account_Stocks['Num'].isin([0])]
            # print('temp_DF3 ', temp_DF3)
            # todo temp_DF3 =
            # todo      Date       code        ifHold B/S/H    W_Real  W_Ideal  Size  Growth    ST_bad  PnL_Last
            # todo 4  2016-05-10  300317.SZ     1.0     H    0.132885      0.0   0.0     0.0   0.0       0.0
            if index_Account_S == 'Error record in Account_Stocks.' and len(temp_DF3)>0  :
                # todo 买入：买入前无持仓 : 只可能买入股票，不需要更新持仓收盘价
                # todo 161029 这里temp 的目的是找到空闲的一行， 来放入要买入的股票

                temp = temp_DF3.index[0]
                # print( '170829 2215 temp temp_DF2 ', temp ,len(temp_DF2)  , '|| ', Account_Stocks.loc[temp,: ])
                # print('temp ' ,temp )
                # todo Update StockPool
                StockPool['ifHold'].loc[index_SP ] = 1
                # todo Update  ['Date', 'code'] and Initialize
                Account_Stocks.loc[temp,'Date' ] = temp_Date
                Account_Stocks.loc[temp, 'code' ] = temp_Symbol
                # print('170623 2317 ')
                # print( Account_Stocks.loc[temp,:]  )

                #  ['Num','AveCost',  'LastPrice','TotalCost', 'MV',  'PnL', 'PnL_Pct', 'W_Real', 'W_Ideal']
                Account_Stocks.loc[temp, 'Num' ] = 0
                Account_Stocks.loc[temp,'AveCost' ] = 0
                Account_Stocks.loc[temp,'LastPrice' ] = 0
                Account_Stocks.loc[temp,'TotalCost' ] = 0
                Account_Stocks.loc[temp,'MV' ] = 0
                Account_Stocks.loc[temp,'PnL' ] =  0
                Account_Stocks.loc[temp,'PnL_Pct' ] =  0

                # todo Account_Stocks | Columns_Stocks = ['Num','AveCost',  'LastPrice','TotalCost', 'MV',  'PnL', 'PnL_Pct', 'W_Real', 'W_Ideal']  # 9items
                if temp_Trade.loc[0,'Number' ] >0 :
                    Account_Stocks.loc[temp,'Num' ] = temp_Trade.loc[0,'Number' ]
                    Account_Stocks.loc[temp,'AveCost' ] = data_1day['open']
                    Account_Stocks.loc[temp,'LastPrice' ] = data_1day['close']
                    Account_Stocks.loc[temp,'TotalCost' ] = temp_Trade.loc[0,'Number'] * data_1day['open']
                    Account_Stocks.loc[temp,'MV' ] = temp_Trade.loc[0,'Number' ] * data_1day['close']
                    Account_Stocks.loc[temp,'PnL' ] =  Account_Stocks.loc[temp,'MV' ] -  Account_Stocks.loc[temp,'TotalCost' ]
                    Account_Stocks.loc[temp,'PnL_Pct' ] =  Account_Stocks.loc[temp,'PnL' ] / Account_Stocks.loc[temp,'TotalCost' ]
                    # Account_Sum |  ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']  # 8 items
                    Account_Sum.loc[temp_Date,'Total_Cost' ] =  Account_Stocks['TotalCost'].sum()
                    # todo 1704030007 总觉得Account_Sum 只有在这里会出问题！
                    # print('Cash 170403 befo', Account_Sum.loc[temp_Date,'Cash' ] )
                    Account_Sum.loc[temp_Date,'Cash' ] = Account_Sum.loc[ temp_Date ,'Cash' ] -  temp_Trade.loc[0,'Number' ] * data_1day['open']  * (1+ rate_Fees)
                    # print('Cash 170403 after', Account_Sum.loc[temp_Date, 'Cash'])

                    # print('170503 temp_Trade , ', temp_Trade)
                    # print( '170503  Account_Stocks MV \n', Account_Stocks['MV'] )

                    Account_Sum.loc[temp_Date,'Stock' ] = sum(  Account_Stocks['MV'] )
                    Account_Sum.loc[temp_Date,'Total' ] = Account_Sum.loc[temp_Date,'Cash' ] + Account_Sum.loc[temp_Date,'Stock' ]
                    Account_Sum.loc[temp_Date,'Unit' ] = Account_Sum.loc[temp_Date,'Total' ] / Account_Sum['Total'].iloc[0]
                    temp_MDD =  Account_Sum.loc[temp_Date,'Total' ]/max(Account_Sum.loc[Index_Sum[:i],'Total' ] ) -1

                    Account_Sum.loc[temp_Date,'MDD' ] = min(Account_Sum.loc[Index_Sum[i-1],'MDD' ],temp_MDD )
                    #
                    Account_Stocks.loc[temp,'W_Real' ] = Account_Stocks.loc[temp,'MV' ] / Account_Sum.loc[temp_Date,'Total' ]
                    StockPool.loc[index_SP,'ifHold' ] = 1
                    StockPool.loc[index_SP,'W_Real' ] = Account_Stocks.loc[temp,'MV'] / Account_Sum.loc[temp_Date,'Total' ]
            else :
                if len(temp_DF) == 1 :
                    # todo 买入：买入前有持仓， 即 len( temp_pd ) >0 ：: 可能买入股票，也可能需要更新持仓收盘价
                    temp = index_Account_S

                    # 更新日期
                    Account_Stocks.loc[temp,'Date' ] = temp_Date
                    Account_Stocks.loc[temp,'code' ] = temp_Symbol

                    Account_Stocks.loc[temp, 'Num' ] = Account_Stocks.loc[temp, 'Num' ] + temp_Trade.loc[0,'Number' ]
                    print(Account_Stocks.loc[temp, 'Num' ])
                    Account_Stocks.loc[temp,'TotalCost' ] =Account_Stocks.loc[temp,'TotalCost' ] + temp_Trade.loc[0,'Number' ] * data_1day['open']
                    Account_Stocks.loc[temp,'AveCost' ] = Account_Stocks.loc[temp,'TotalCost' ] / Account_Stocks.loc[temp,'Num' ]
                    print( Account_Stocks.loc[temp,'AveCost' ] )
                    Account_Stocks.loc[temp,'LastPrice'] = data_1day['close']
                    Account_Stocks.loc[temp,'MV'] = Account_Stocks.loc[temp,'Num'] * data_1day['close']
                    Account_Stocks.loc[temp,'PnL'] = Account_Stocks.loc[temp,'MV'] - Account_Stocks.loc[temp,'TotalCost']
                    Account_Stocks.loc[temp,'PnL_Pct'] = Account_Stocks.loc[temp,'PnL'] / Account_Stocks.loc[temp,'TotalCost']
                    # Account_Sum |  ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']  # 8 items
                    Account_Sum.loc[temp_Date,'Total_Cost'] = sum( Account_Stocks['TotalCost'])

                    # todo 1704030007 总觉得Account_Sum 只有在这里会出问题！
                    print('Cash 170403 befo', Account_Sum.loc[temp_Date, 'Cash'])
                    Account_Sum.loc[temp_Date,'Cash'] = Account_Sum.loc[temp_Date,'Cash'] - temp_Trade.loc[0,'Number'] * data_1day['open'] * ( 1 + rate_Fees)
                    print('Cash 170403 after', Account_Sum.loc[temp_Date, 'Cash'])
                    # Account_Sum['Cash'][i] = Account_Sum['Cash'][i - 1] - temp_Trade.loc[0,'Number' ] * data_1day['open']  * (1 + rate_Fees)
                    Account_Sum.loc[temp_Date,'Stock'] = sum( Account_Stocks['MV'])
                    Account_Sum.loc[temp_Date, 'Total'] = Account_Sum.loc[temp_Date,'Cash'] + Account_Sum.loc[temp_Date,'Stock']
                    Account_Sum.loc[temp_Date, 'Unit'] = Account_Sum.loc[temp_Date,'Total'] / Account_Sum['Total'].iloc[0]
                    # print('170309 Unit now', temp_Date)

                    temp_MDD = Account_Sum.loc[temp_Date,'Total'] / max(Account_Sum.loc[Index_Sum[:i] ,'Total' ]) - 1
                    Account_Sum.loc[temp_Date, 'MDD'] = min(Account_Sum.loc[Index_Sum[i-1],'MDD'],  temp_MDD )
                    #
                    Account_Stocks.loc[temp,'W_Real'] = Account_Stocks.loc[temp,'MV'] / Account_Sum.loc[temp_Date,'Total']
                    StockPool.loc[index_SP, 'ifHold'] = 1
                    StockPool.loc[index_SP, 'W_Real'] = Account_Stocks.loc[temp,'MV'] / Account_Sum.loc[temp_Date, 'Total']

        elif (temp_Trade.loc[0,'Buy/Sell'] == -1 and len(temp_DF) == 1) and len(temp_DF2) == 1 :
            # print('170317 1912 temp_Date S  | Cash : ', temp_Date, Account_Sum.loc[temp_Date, 'Cash'])
            # We want to sell stocks
            # # todo Update : StockPool['PnL_Last'] 对每一次卖出，记录该股的盈亏比例
            temp = index_Account_S
            if Account_Stocks.loc[temp,'Num'] == temp_Trade.loc[0,'Number'] :
                # todo 卖出：卖出后无持仓,
                # print('161029 1030 temp = index_Account_S', index_Account_S )

                # todo Update StockPool ifHold
                if len(temp_DF2)  == 1:
                    Account_Stocks.loc[temp,'W_Real'] = 0
                    StockPool.loc[index_SP,'ifHold'] = 0
                    StockPool.loc[index_SP,'W_Real' ] = 0
                else:
                    print(index_SP)

                # todo：之间删掉 Account_Stocks 和 Account_Stocks 中对应的记录。
                # 删除pandas DataFrame的某一/几列: 直接del DF['column-name'] ; 采用drop方法，有下面三种等价的表达式：
                    # 1. DF= DF.drop('column_name', 1) ;2. DF.drop('column_name',axis=1, inplace=True)
                    # 3. DF.drop([DF.columns[[0,1, 3]]], axis=1,inplace=True)   # Note: zero indexed
                # todo Account_Stocks.drop(temp) 这个操作会减少 一行
                # todo Account_Stocks.sub(row, axis='columns') 删除每！！！一行并全部留0值

                # todo 161219 注意：以下的办法虽然非常傻逼，但是至少暂时解决了把第temp行的值删掉并且为0，还不改变序列
                # todo 顺序的需要
                Account_Stocks.loc[temp, :] = 0
                # Account_Stocks.iloc[temp, :] = 0

                # 上面没问题，下边有问题，我在想会不会是因为下边的不是数值，是string
                # Account_Stocks2 = Account_Stocks2.sub(  Account_Stocks2.ix[temp] , axis= 1 )
                # ['Date', 'code']
                # my_index = Account_Stocks['code'] == temp_Symbol
                # Account_Stocks.loc[ my_index, 'code'] = np.nan
                # Account_Stocks.loc[ my_index, 'Date'] = np.nan

                Account_Sum.loc[temp_Date, 'Cash'] = Account_Sum.loc[temp_Date, 'Cash'] + temp_Trade.loc[0, 'Number'] *  data_1day['open'] * ( 1 - rate_Fees)
                Account_Sum.loc[temp_Date,'Total_Cost' ] = sum(Account_Stocks['TotalCost'])
                Account_Sum.loc[temp_Date, 'Stock'] = sum(Account_Stocks['MV'])
                Account_Sum.loc[temp_Date, 'Total'] = Account_Sum.loc[temp_Date, 'Cash'] + Account_Sum.loc[temp_Date,'Stock']
                Account_Sum.loc[temp_Date, 'Unit'] = Account_Sum.loc[temp_Date,'Total'] / Account_Sum['Total'].iloc[0]
                temp_MDD = Account_Sum.loc[temp_Date, 'Total'] / max(Account_Sum.loc[Index_Sum[:i], 'Total']) - 1
                Account_Sum.loc[temp_Date,'MDD'] = min(Account_Sum.loc[Index_Sum[i-1],'MDD'], temp_MDD )
                StockPool['W_Ideal'].loc[index_SP] = temp_Trade.loc[0,'Number'] * data_1day['open'] /Account_Sum.loc[temp_Date,'Total']
            else :
                # todo 卖出：卖出后有持仓
                # 更新日期
                Account_Stocks.loc[temp, 'Date'] = temp_Date
                Account_Stocks.loc[temp, 'code'] = temp_Symbol
                print('20161225_1440 Account_Stocks  Num temp  ',Account_Stocks.loc[temp,'Num'], temp_Trade['Number'], 'i ', i)
                if int(Account_Stocks.loc[temp, 'Num']) > int(temp_Trade['Number']) :
                    Account_Stocks.loc[temp,'TotalCost' ] = Account_Stocks.loc[temp,'TotalCost' ] - temp_Trade.loc[0,'Number'] * Account_Stocks.loc[temp,'AveCost' ]

                    Account_Stocks.loc[temp,'Num' ] = Account_Stocks.loc[temp,'Num' ] - temp_Trade.loc[0,'Number']
                    Account_Stocks.loc[temp,'MV'] = Account_Stocks.loc[temp,'Num' ] * data_1day['close']
                    #　The AveCost does not change when we sell
                    # Account_Stocks['AveCost'][temp] = Account_Stocks['AveCost'][temp - 1] |wrong wrong wrong
                    Account_Stocks.loc[temp,'LastPrice'] = data_1day['close']

                    Account_Stocks.loc[temp,'PnL'] = Account_Stocks.loc[temp,'MV'] - Account_Stocks.loc[temp,'TotalCost']
                    Account_Stocks.loc[temp,'PnL_Pct'] = Account_Stocks.loc[temp,'PnL'] / Account_Stocks.loc[temp,'TotalCost']
                    # Account_Sum |  ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']  # 8 items
                    Account_Sum.loc[temp_Date,'Total_Cost' ] = sum(Account_Stocks['TotalCost'])
                    # print('170317 1912 A1 temp_Date | Cash : ', temp_Date, Account_Sum.loc[temp_Date, 'Cash'])
                    Account_Sum.loc[temp_Date,'Cash'] = Account_Sum.loc[temp_Date,'Cash'] + temp_Trade.loc[0,'Number'] * data_1day['open'] * (  1 - rate_Fees)
                    # print('170317 1912 A2 temp_Date | Cash : ', temp_Date, Account_Sum.loc[temp_Date, 'Cash'])
                    # Account_Sum['Cash'][i] = Account_Sum['Cash'][i - 1] + temp_Trade.loc[0,'Number'][0] * data_1day['open'] * (1 - rate_Fees)
                    Account_Sum.loc[temp_Date,'Stock'] = sum(Account_Stocks['MV'])
                    Account_Sum.loc[temp_Date,'Total' ] = Account_Sum.loc[temp_Date,'Cash' ] + Account_Sum.loc[temp_Date,'Stock' ]
                    Account_Sum.loc[temp_Date,'Unit' ] = Account_Sum.loc[temp_Date,'Total' ] /Account_Sum['Total'].iloc[0]

                    # temp_MDD = Account_Sum.loc[temp_Date, 'Total'] / max(Account_Sum.loc[Index_Sum[:i - 1], 'Total']) - 1
                    temp_MDD =  Account_Sum.loc[temp_Date,'Total' ] / max(Account_Sum.loc[:temp_Date ,'Total' ] )-1
                    Account_Sum.loc[temp_Date,'MDD' ] = min(Account_Sum.loc[Index_Sum[i-1],'MDD' ], temp_MDD)
                    #
                    Account_Stocks.loc[temp,'W_Real' ] = Account_Stocks.loc[temp,'MV' ] / Account_Sum.loc[temp_Date,'Total' ]
                    StockPool.loc[ index_SP,'W_Real' ] = Account_Stocks.loc[temp,'MV' ]  / Account_Sum.loc[temp_Date,'Total' ]
                    StockPool.loc[index_SP,'ifHold' ] = 1

        elif (temp_Trade.loc[0,'Buy/Sell' ] == 0 and len(temp_DF) == 1) and len(temp_DF2) == 1 :
            # 161026 No trading case. Here we want to update correct close price to our holdings
            temp = index_Account_S
            # No trade,we just update portfolio with latest price
            # 更新日期
            Account_Stocks.loc[temp,'Date' ] = temp_Date
            Account_Stocks.loc[temp,'code' ] = temp_Symbol

            Account_Stocks.loc[temp,'LastPrice' ] = data_1day['close']
            Account_Stocks.loc[temp,'MV' ] = Account_Stocks.loc[temp,'Num' ] * data_1day['close']
            Account_Stocks.loc[temp,'PnL'] = Account_Stocks.loc[temp,'MV'] - Account_Stocks.loc[temp,'TotalCost' ]
            Account_Stocks.loc[temp,'PnL_Pct' ] = Account_Stocks.loc[temp,'PnL' ] / Account_Stocks.loc[temp,'TotalCost' ]
            # Account_Sum |  ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']  # 8 items
            Account_Sum.loc[temp_Date,'Total_Cost' ] = sum(Account_Stocks['TotalCost'])

            Account_Sum.loc[temp_Date,'Stock' ] = sum(Account_Stocks['MV'])
            Account_Sum.loc[temp_Date,'Total' ] = Account_Sum.loc[temp_Date,'Cash' ] + Account_Sum.loc[temp_Date,'Stock' ]
            Account_Sum.loc[temp_Date,'Unit' ] = Account_Sum.loc[temp_Date,'Total'] / Account_Sum['Total'].iloc[0]
            temp_MDD =   Account_Sum.loc[temp_Date,'Total' ] / max(Account_Sum.loc[Index_Sum[:i-1],'Total' ] ) -1
            Account_Sum.loc[temp_Date,'MDD' ] = min(Account_Sum.loc[Index_Sum[i-1],'MDD' ], temp_MDD )
            #
            Account_Stocks.loc[temp,'W_Real' ] = Account_Stocks.loc[temp,'MV' ] / Account_Sum.loc[temp_Date,'Total' ]
            StockPool.loc[index_SP,'W_Real' ] = Account_Stocks.loc[temp,'MV'] / Account_Sum.loc[temp_Date,'Total' ]

        # print('170317 1912 BB temp_Date | Cash : ', temp_Date, Account_Sum.loc[temp_Date, 'Cash'])
        # print('170313 1019 ---temp_MDD ', min( Account_Sum['MDD'].min() , temp_MDD))
        # todo 已更新完项目 ： Account_Stocks ， Account_Stocks Account_Sum  StockPool
        # todo 在检查下还有什么可以更新的 ？？

        return Account_Sum ,Account_Stocks ,  StockPool

    def Update_Account_List_NoTrade(self, data_1day, Account_Stocks, Account_Sum,  i, temp_Symbol, temp_Date,Index_Sum):
        # todo Last check 170403 1226 | 161218_2354
        print('Update_Account_List_NoTrade')

        temp_NoT = Account_Stocks[Account_Stocks['code'].isin([temp_Symbol])]
        if len( temp_NoT ) ==1 :
            index_Account_S = temp_NoT.index[0]
        else :
            index_Account_S = 'Error record in Account_Stocks '

        # 161026 No trading case. Here we want to update correct close price to our holdings
        temp = index_Account_S
        # No trade,we just update portfolio with latest price
        # 更新日期
        Account_Stocks.loc[temp,'Date' ] = temp_Date
        Account_Stocks.loc[temp,'code' ] = temp_Symbol
        # todo 170717 万科停牌需要处理这种情况
        if type(data_1day) == int  : # and data_1day == '0'
            print('This stock might not have price today.', temp_Symbol )
        else :
            Account_Stocks.loc[temp,'LastPrice' ] = data_1day['close']
            Account_Stocks.loc[temp,'MV' ] = Account_Stocks.loc[temp,'Num'] * data_1day['close']

        Account_Stocks.loc[temp,'PnL' ] = Account_Stocks.loc[temp,'MV' ] - Account_Stocks.loc[temp,'TotalCost' ]
        Account_Stocks.loc[temp,'PnL_Pct' ] = Account_Stocks.loc[temp,'PnL' ] / Account_Stocks.loc[temp,'TotalCost' ]
        # Account_Sum |  ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']  # 8 items
        Account_Sum.loc[temp_Date,'Total_Cost'] = sum(Account_Stocks['TotalCost'])
        # todo !! 发现问题！Update_Account_List_NoTrade 最后一次更新是 161218
        # todo 这里不更新 Account_Sum.loc[temp_Date,'Cash' ]

        # Account_Sum['Cash'][i] = Account_Sum['Cash'][i - 1]
        Account_Sum.loc[temp_Date,'Stock'] = sum(Account_Stocks['MV'])
        Account_Sum.loc[temp_Date, 'Total'] = Account_Sum.loc[temp_Date, 'Cash'] + Account_Sum.loc[temp_Date,'Stock']
        Account_Sum.loc[temp_Date,'Unit'] = Account_Sum.loc[temp_Date,'Total'] / Account_Sum['Total'].iloc[0]
        temp_MDD = Account_Sum['Total'].iloc[-1] / Account_Sum['Total'].max() - 1
        # print('170313 1019 ---temp_MDD ', temp_MDD)
        Account_Sum.loc[temp_Date, 'MDD'] = min(Account_Sum['MDD'].min(), temp_MDD)
        # print('170313 1019 ---temp_MDD ', min( Account_Sum['MDD'].min() , temp_MDD))
        # print('170313 1019 ---temp_MDD ', Account_Sum.loc[Index_Sum[i-1],'MDD'] )
        Account_Stocks.loc[temp,'W_Real' ] = Account_Stocks.loc[temp,'MV'] / Account_Sum.loc[temp_Date,'Total']

        return Account_Sum, Account_Stocks

    def Update_StockPool(self,temp_Symbol , StockPool,Account_Sum_Total_i , temp_Trade) :
        # todo 161229 Update StockPool['W_Ideal'] 注意：只有 实现收益或亏损时，才更新单只股票的持仓比例
        # todo find weight of temp_Trade['Symbol']  in StockPool[]
        temp_123 = StockPool[StockPool['code'].isin([temp_Symbol])]
        # todo temp_123 返回的是 dataFrame，且 temp_123.index 的值应该就是  StockPool 里的值
        if len(temp_123) > 0:
            # todo 要定位该股票的在StockPool 里的位置，才好更新 W_Ideal ？？？？未完成
            index_Weight = temp_123['W_Ideal'].index[0]
            # W_Ideal = temp_123['W_Ideal'].index[0] # 返回的是index的值 而不是weight
            W_Ideal = temp_123['W_Ideal'].values[0] # todo 返回 temp_123['W_Ideal']的值得第一个，理论上应该只有一个
            W_Real = temp_123['W_Ideal'].values[0]

        pct_profit = temp_Trade.loc[0,'Price']/temp_Trade.loc[0,'AveCost'] -1
        W_LatestSold = temp_Trade.loc[0,'Price']*temp_Trade.loc[0,'Number'] / Account_Sum_Total_i

        if W_LatestSold > W_Ideal :
            if pct_profit > 0 :
                # todo 该股票的最近一次持仓占比升高了，且该笔交易时赚钱的，那么可以提高该股票的 W_Ideal ？？？？未完成
                StockPool.loc[index_Weight,'W_Ideal'  ] = 0.75*W_LatestSold + 0.25* W_Ideal
            else :
                StockPool.loc[index_Weight,'W_Ideal' ] = min(W_Ideal, 0.25 * W_LatestSold + 0.75 * W_Ideal)
        else :
            if pct_profit > 0 :
                StockPool.loc[index_Weight,'W_Ideal' ] = 0.75*W_LatestSold + 0.25* W_Ideal
            else :
                # 前期已亏钱情况下又亏钱，再次缩小仓位。
                StockPool.loc[index_Weight, 'W_Ideal' ] = 0.75 * W_LatestSold


        return StockPool

    def Get_HoldingStock( self, temp_Symbol, Account_Stocks) :
        # todo last check 161218
        # return holding information of temp_Symbol from Account_Stocks
        # (Stocks_Cost_ij, Stocks_aveCost_ij, Stocks_Num_ij) = self.Get_HoldingStock(temp_Symbol,Account_Stocks2, Account_Stocks)
        # Account_Stocks | ['Num','AveCost',  'LastPrice','TotalCost', 'MV',  'PnL', 'PnL_Pct', 'W_Real', 'W_Ideal']  # 9items
        # Account_Stocks2 | ['Date', 'code']
        print('161219_0015 Get_HoldingStock: ', )

        temp_Index = -1
        i = len(Account_Stocks)-1
        while temp_Index == -1 and i>= 0 :
            if Account_Stocks['code'].iloc[i]== temp_Symbol :
                temp_Index = i
            else :
                i=i-1
        # todo 现在我们有可能找到，也有可能没找到，则 temp_Index = -1
        if temp_Index >= 0 :
            Stocks_Cost_ij = Account_Stocks['TotalCost'].iloc[temp_Index]
            Stocks_aveCost_ij =Account_Stocks ['AveCost'].iloc[temp_Index]
            Stocks_Num_ij =Account_Stocks['Num'].iloc[temp_Index]
        else :
            print('No holding information for symbol ', temp_Symbol)
            Stocks_Cost_ij=0
            Stocks_aveCost_ij=0
            Stocks_Num_ij=0

        # print('Account_Stocks: ', Account_Stocks.head(5) )

        return Stocks_Cost_ij, Stocks_aveCost_ij, Stocks_Num_ij

    def Update_Log_Portfolio(self, Log_Portfolio, path_Portfolio ):
        '''
        从 path_Portfolio 中抓取 AS A_Sum TB and SP 来更新 Log_Portfolio

        # last update 170416 1004 || 170306
        # 这个模块是用来更新 Log_Portfolio

        '''

        # import numpy as np
        # import pandas as pd
        import rC_Portfolio_17Q1 as rC_Port

        # # todo Input
        # Log_Index = ['InitialDate', 'Index_Name', 'Portfolio_Name', 'MaxN', 'Leverage', 'date_Start', 'date_LastUpdate']
        # Log_Columns = ['value']
        # Log_Portfolio = pd.DataFrame(index=Log_Index, columns=Log_Columns)

        # # todo ============================
        # # port_dir = 'Port_' + temp_Portfolio + '_' + indexName + '_' + InitialDate_Port + '_' + str(len_Days) + '_' + str(MaxN) + '_' + str(Leverage)
        # # path_Portfolio = path_Sys +'\\' + port_dir
        # path_Portfolio = 'D:\data_Output\Sys_rC1703_1703\Port_SZ50_000300.SH_170306_40_20_0.95'

        # todo 更新 Log_Portfolio
        # 要抓取的几个数据：Account_Sum ,Account_Stocks,TradeBook, StockPool
        # todo 1,Account_Sum | Total_Cost	Cash	Stock	Total	Unit	MDD | pnl pnl_pct, r_annual ?

        temp_str = 'Account_Sum' + '.csv'
        temp_Account_Sum = pd.read_csv( path_Portfolio + '\\' + temp_str )
        print('================== 170306 ==========')
        print('Account_Sum __ tail ')
        print( temp_Account_Sum.tail(5) )

        Log_Portfolio.loc['Total_Cost', 'value'] = temp_Account_Sum['Total_Cost'].iloc[-1]
        Log_Portfolio.loc['Cash', 'value'] = temp_Account_Sum['Cash'].iloc[-1]
        Log_Portfolio.loc['Stock', 'value'] = temp_Account_Sum['Stock'].iloc[-1]
        Log_Portfolio.loc['Total', 'value'] = temp_Account_Sum['Total'].iloc[-1]

        Log_Portfolio.loc['Unit', 'value'] = temp_Account_Sum['Unit'].iloc[-1]
        Log_Portfolio.loc['MDD', 'value'] = temp_Account_Sum['MDD'].iloc[-1]
        Log_Portfolio.loc['PnL', 'value'] = temp_Account_Sum['Stock'].iloc[-1] - temp_Account_Sum['Total_Cost'].iloc[-1]
        Log_Portfolio.loc['PnL_pct', 'value'] = (temp_Account_Sum['Stock'].iloc[-1] -
                                                 temp_Account_Sum['Total_Cost'].iloc[-1]) / \
                                                temp_Account_Sum['Total_Cost'].iloc[-1]
        # 年化收益率
        temp_len = len(temp_Account_Sum['Total_Cost'])
        annual_Port_Return = temp_Account_Sum['Unit'].iloc[-1] ** (252 / temp_len)
        Log_Portfolio.loc['r_annual', 'value'] = annual_Port_Return

        print("Annual return in past ", round((temp_len / 252) * 100) / 100, "  years is : ",
              round(100 * annual_Port_Return) / 100)

        # todo 2,Account_Stocks | PnL	PnL_Pct	W_Real	code
        temp_str = 'Account_Stocks' + '.csv'
        temp_Account_Stocks = pd.read_csv(path_Portfolio + '\\' + temp_str)
        # print('================== 170306 ==========')
        # print('temp_Account_Stocks')
        # print(temp_Account_Stocks.head(5))

        Log_Portfolio.loc['PnL_total', 'value'] = temp_Account_Stocks['PnL'].sum()
        if temp_Account_Stocks[ 'TotalCost'].sum() <= 0 :
            Log_Portfolio.loc['PnL_Pct', 'value'] = 1
        else :
            Log_Portfolio.loc['PnL_Pct', 'value'] = temp_Account_Stocks['PnL'].sum() / temp_Account_Stocks['TotalCost'].sum()
        # df1.idxmin(axis=0) 寻找每个columns里最小值的位置
        # temp_Account_Stocks['W_Real'].max()里最小值的位置
        # max weight
        Log_Portfolio.loc['W_max', 'value'] = temp_Account_Stocks['W_Real'].max()

        temp_pd = temp_Account_Stocks.ix[temp_Account_Stocks['W_Real'].idxmax(axis=0)]
        Log_Portfolio.loc['W_max_code', 'value'] = temp_pd['code']
        # max/min, pnl
        Log_Portfolio.loc['profit_max', 'value'] = temp_Account_Stocks['PnL'].max()
        temp_pd = temp_Account_Stocks.ix[temp_Account_Stocks['PnL'].idxmax(axis=0)]
        Log_Portfolio.loc['profit_max_code', 'value'] = temp_pd['code']
        Log_Portfolio.loc['loss_max', 'value'] = temp_Account_Stocks['PnL'].min()
        temp_pd = temp_Account_Stocks.ix[temp_Account_Stocks['PnL'].idxmin(axis=0)]
        Log_Portfolio.loc['loss_max_code', 'value'] = temp_pd['code']
        # max/min, pnl pct
        Log_Portfolio.loc['PnL_Pct_max', 'value'] = temp_Account_Stocks['PnL_Pct'].max()
        temp_pd = temp_Account_Stocks.ix[temp_Account_Stocks['PnL_Pct'].idxmax(axis=0)]
        Log_Portfolio.loc['PnL_Pct_max_code', 'value'] = temp_pd['code']
        Log_Portfolio.loc['PnL_Pct_min', 'value'] = temp_Account_Stocks['PnL_Pct'].min()
        temp_pd = temp_Account_Stocks.ix[temp_Account_Stocks['PnL_Pct'].idxmin(axis=0)]
        Log_Portfolio.loc['PnL_Pct_min_code', 'value'] = temp_pd['code']

        # todo 3,TradeBook | total_PnL, total_Loss, num_Trade_Profit, ave_Trade_Profit  ,num_Trade_Loss ,ave_Trade_Loss,
        temp_str = 'Trading_Book' + '.csv'
        temp_Trading_Book = pd.read_csv(path_Portfolio + '\\' + temp_str)
        print('================== 170306 ==========')
        print('temp_Trading_Book ')
        # 要剔除Trading_Book尾部0值的row
        temp_Trading_Book2 = temp_Trading_Book[ temp_Trading_Book['Number']>0 ]
        print(temp_Trading_Book2.tail(5))

        Log_Portfolio.loc['total_ProfitReal', 'value'] = temp_Trading_Book['ProfitReal'].sum()
        # Profit trade
        temp_pd = temp_Trading_Book[temp_Trading_Book['ProfitReal'] >= 0]
        Log_Portfolio.loc['total_Profit_R', 'value'] = temp_pd['ProfitReal'].sum()
        Log_Portfolio.loc['num_Trade_Profit', 'value'] = len(temp_pd['ProfitReal'])
        Log_Portfolio.loc['ave_Trade_Profit', 'value'] = temp_pd['ProfitReal'].mean()
        # Loss trade

        temp_pd = temp_Trading_Book[temp_Trading_Book['ProfitReal'] < 0]
        Log_Portfolio.loc['total_Loss_R', 'value'] = temp_pd['ProfitReal'].sum()
        Log_Portfolio.loc['num_Trade_Loss', 'value'] = len(temp_pd['ProfitReal'])
        Log_Portfolio.loc['ave_Trade_Loss', 'value'] = temp_pd['ProfitReal'].mean()
        # fees
        Log_Portfolio.loc['total_Fees', 'value'] = temp_Trading_Book['Fees'].sum()

        # todo 4, StockPool| code_weight_max 抓取历史表现得到的最大权重的股票
        temp_str = 'StockPool' + '.csv'
        temp_StockPool = pd.read_csv(path_Portfolio + '\\' + temp_str)
        # print('================== 170306 ==========')
        # print('temp_StockPool')
        # print(temp_StockPool.head(5))

        # df1.idxmin(axis=0) 寻找每个columns里最小值的位置
        # temp_Account_Stocks['W_Real'].max()里最小值的位置

        Log_Portfolio.loc['W_Ideal_max', 'value'] = temp_StockPool['W_Ideal'].max()
        temp_pd = temp_StockPool.ix[temp_StockPool['W_Ideal'].idxmax(axis=0)]
        Log_Portfolio.loc['W_Ideal_max_code', 'value'] = temp_pd['code']

        # todo output to csv 由于Log文件夹不一定在portfolio文件夹内，因此先不output
        # path_Portfolio  = 'D:\data_Output\Sys_rC1703_1703\Port_SZ50_000300.SH_170306_40_20_0.95'

        # Log_Portfolio.to_csv(path_Portfolio + '\\' + 'Log_Portfolio222' + '.csv')
        # todo ======================================================
        # todo 170401
        # 由于暂时需求，需要计算的数据
        if len( temp_Account_Sum['Unit'] ) >= 5 :
            Log_Portfolio.loc['Unit-5D', 'value'] = str( temp_Account_Sum['Unit'].iloc[-5:])
            Log_Portfolio.loc['Cash-5D', 'value'] = str(temp_Account_Sum['Cash'].iloc[-5:])
            Log_Portfolio.loc['Stock-5D', 'value'] = str(temp_Account_Sum['Stock'].iloc[-5:])
        print('Log_Portfolio222 Done. ')

        return Log_Portfolio

    def Update_StockPool_Outside(self, start_Date,  StockPool, path_Input , path_Output  ):
        # last update 170528 | since  170524
        # todo step 1 Delete bad stocks ？？？？
        # todo step 2 Add good stock
        # This module is to update StockPool
        # path_Input = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Stockpool'
        # path_Output = D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329_000300.SH_170405_12_0.95
        import os
        fileName_Date = path_Input + '\Sig_Ana_Sum2_' + start_Date + '.csv'

        if os.path.isfile((fileName_Date)):
            Sig_Ana_Sum3 = pd.read_csv(fileName_Date, header=None, skiprows=0, sep=',')  # skiprows=1,
            # todo 170524 步骤： 删第一列，把columns从数字变成第一行，删除第一行。
            # print( Sig_Ana_Sum3.head(3) )
            Sig_Ana_Sum3 = Sig_Ana_Sum3.drop([0], axis=1)
            Sig_Ana_Sum3.columns = Sig_Ana_Sum3.ix[0, :]
            Sig_Ana_Sum3 = Sig_Ana_Sum3.drop([0], axis=0)
            # print(Sig_Ana_Sum3.head(3))
            # todo add  Sig_Ana_Sum3 to SP ???
            # From | Signal	temp_Ana	Order	Symbol
            # To   | Date	code	ifHold	B/S/H	W_Real	W_Ideal	Size	Growth	ST_bad	PnL_Last
            for temp_index in Sig_Ana_Sum3.index:
                # print( Sig_Ana_Sum3.loc[temp_index, :] )
                temp_code = Sig_Ana_Sum3.loc[temp_index, 'Symbol']
                temp_pd = StockPool[StockPool['code'] == temp_code]
                if len(temp_pd.index) < 1:
                    # print( '170608 0041 StockPool ', StockPool )

                    temp_index2 = max(StockPool.index) + 1
                    StockPool.loc[temp_index2, 'Date'] = start_Date
                    StockPool.loc[temp_index2, 'code'] = temp_code
                    StockPool.loc[temp_index2, 'ifHold'] = 0
                    StockPool.loc[temp_index2, 'B/S/H'] = 'H'
                    StockPool.loc[temp_index2, 'W_Real'] = 0
                    StockPool.loc[temp_index2, 'W_Ideal'] = StockPool['W_Ideal'].median()
                    StockPool.loc[temp_index2, 'Size'] = 0
                    StockPool.loc[temp_index2, 'Growth'] = 0
                    StockPool.loc[temp_index2, 'ST_bad'] = 0
                    StockPool.loc[temp_index2, 'PnL_Last'] = 0

        # SP
        StockPool.to_csv( path_Output + '\StockPool_Outside_' + start_Date + '.csv')

        return StockPool
# todo ========================================================================================

    def rC_Portfolio_L(self,start_Date, indexName,symbolList, Dates,Path_Data ,Path_TradeSys, Leverage=0.95,Initial=10000000, MaxN=30  ,if_change_SP = 0 ):
        # Parameters  fee1 = 0.0015
        '''
        Part 1 : Initialize our portfolio
        Last update 170403 1318 | Since 170403 1318
        '''

        # todo P1 Step1 初始化组合信息 : Account_Sum ,  Account_Stocks, Trading_Book, StockPool
        [Account_Sum ,  Account_Stocks, Trading_Book, StockPool,temp_Sig_Ana ] = self.Generate_Portfolio(Dates,Initial, MaxN,symbolList  )

        temp_Date = Dates[1]
        # print('===temp_Date ', temp_Date ,Account_Sum)
        # todo P3 Save to Database and to backup csv files交易日文件存档
        # todo Trading_Book, index_Trading_Book  ,Stock_MV, Account_Sum, StockList, StockPool
        Account_Sum.to_csv(Path_TradeSys + '\\' + 'Account_Sum_' + temp_Date + '.csv')
        Account_Stocks.to_csv(Path_TradeSys + '\\' + 'Account_Stocks_' + temp_Date + '.csv')
        # Account_Stocks2.to_csv(Path_TradeSys + '\\' + 'Account_Stocks2_'+ temp_Date + '.csv')
        StockPool.to_csv(Path_TradeSys + '\\' + 'StockPool_' + temp_Date + '.csv')
        Trading_Book.to_csv(Path_TradeSys + '\\' + 'Trading_Book_' + temp_Date + '.csv')
        # todo 还是新建一个 temp_Sig_Ana，为了新建System时，利用 rC_Portfolio_L_Update
        # temp_Sig_Ana.to_csv(Path_TradeSys + '\\' + 'Sig_Ana_' + temp_Date + '.csv')
        temp_Sig_Ana.to_csv(Path_TradeSys + '\\' + 'Sig_Ana' + temp_Date + '.csv')
        print('temp_Date ', temp_Date)


        # todo P1 Step2 初始化其他参数
        # Path_TradeSys = 'F:\\rC_Py3_Output\\TradeSys_0'
        weight_Method = 'equalWeight_ini'
        rate_Fees = 0.0025

        Portfolio_L = self.rC_Portfolio_L_Update( start_Date, indexName, symbolList, Dates,Path_Data, Path_TradeSys, Leverage, MaxN  ,if_change_SP)

        return Portfolio_L

    def rC_Portfolio_L_Update(self, start_Date, indexName,symbolList, Dates, path_Input ,Path_TradeSys, Leverage=0.95, MaxN=30 ,if_change_SP = 0):
        import numpy as np
        import pandas as pd
        import os

        '''
        # todo ===============================================================================
        # todo last update 170401 | since 170504

        # todo rC_Portfolio_L_Update 是在一段时间内，对Portfolio的持仓和交易变化进行更新
        # todo 对于每一个交易日结束后，需要操作的功能包括 当日账户回测 和 次日模拟实盘交易单
        # derived from rC_Portfolio_L （会从0开始建立一个Portfolio）

        # Part 1 : Portfolio Initialization
        #   导入前一交易日各类变量，新建各类变量，参数
        # Part 2 交易日回测 Back Testing
        # Part 3 模拟实盘Live

        # todo ===============================================================================
        '''

        ''' Part 1 :  Portfolio Initialization   '''
        # Input： Dates, Path_TradeSys , start_Date, path_Input,
        # Output： Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana
        # todo 导入组合信息
        [Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana] = self.Import_Portfolio_Data(Path_TradeSys , start_Date )
        # todo Update StockPool from start_Date , 170524
        # # Input : SP, start_Date, path_
        # # Output: SP
        path_Input2 = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Stockpool'
        # path_Output = 'D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329_000300.SH_170405_12_0.95' # path_Port =Path_TradeSys
        path_Output = Path_TradeSys
        if if_change_SP == 1:
            # todo
            StockPool = self.Update_StockPool_Outside(start_Date, StockPool, path_Input2 , path_Output  )

        # ================================================================================
        Len_time = len(Dates)
        Index_Sum = Dates
        # print('Len_time ',Len_time  )
        # print('Dates', Dates[1] )
        # todo para : rate_Fees 初始化其他参数
        weight_Method = 'equalWeight_ini'
        rate_Fees = 0.0025
        temp_TB_Index = 0
        Path_Data = path_Input
        Initial = Account_Sum['Total'].iloc[0]
        # todo 新建次日交易指令　pre_Orders　  ID-Event ：0331xx
            # col_TB : Date	Symbol	Buy/Sell	Price	Number	AveCost	Fees	ProfitReal
            # col_sig_Ana :Signal	temp_Ana	Order	Symbol
        # ['Signal', 'temp_Ana', 'Order', 'Symbol', 'Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        columns_trade_NextDay =  Trading_Book.columns # todo  Trading_Book.columns
        temp_Sig_Ana_Next = pd.DataFrame(np.zeros([len(symbolList), 4]),  columns=['Signal', 'temp_Ana', 'Order', 'Symbol'])
        trade_NextDay = pd.DataFrame(columns=columns_trade_NextDay)

        # print( 'Len_time', Len_time )
        # asd
        ''' Part 2 交易日回测 Back Testing'''
        ''' Part 3 模拟实盘Live  '''
        # P2 Dynamic Portfolio Adjustment
        for i in range(1, Len_time):
            ''' Part 2 Step 1 初始化回测Back Testing：当前日期， 资金，持仓，总值   Initialize Account_Sum, Update cash level '''
            # todo Date and LastDay 获取当前日期
            temp_Date = Dates[i]  # ex. temp_Date = "20160104"
            temp_LastDay = Dates[i - 1]
            print('temp_LastDay ', temp_LastDay )

            # Account_Sum： Columns = ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']
            # print('170315 2102 Dates ' ,  Account_Sum.tail(5) )
            Account_Sum.loc[temp_Date, 'Total_Cost'] = Account_Sum.loc[Dates[i - 1], 'Total_Cost']
            Account_Sum.loc[temp_Date, 'Cash'] = Account_Sum.loc[Dates[i - 1], 'Cash'] * (1 + 0.02 / 365)
            Account_Sum.loc[temp_Date, 'Stock'] = Account_Sum.loc[Dates[i - 1], 'Stock']
            Account_Sum.loc[temp_Date, 'Total'] = Account_Sum.loc[temp_Date, 'Cash'] + Account_Sum.loc[
                temp_Date, 'Stock']
            Account_Sum.loc[temp_Date, 'Unit'] = Account_Sum.loc[temp_Date, 'Total'] / Initial
            Account_Sum.loc[temp_Date, 'MDD_I'] = Account_Sum.loc[Dates[i - 1], 'MDD_I']

            # todo 170504 Update Account_Stocks for dividend of Cash or Shares :

            # for temp_index in Account_Stocks.index:
            #     if Account_Stocks.loc[temp_index, 'Num'] > 0 and len(Account_Stocks.loc[temp_index, 'code']) > 1:
            #         temp_code = Account_Stocks.loc[temp_index, 'code']
            #         # temp_Date, temp_LastDay = '2017-04-05'
            #         print('170504 temp_code temp_Date ,temp_LastDay ', temp_code,temp_Date ,temp_LastDay )
            #         [temp_factor, dif_Cash] = self.Check_div_Cash_Shares(temp_code, temp_Date, temp_LastDay)
            #         # todo Now we need to change number, costAve,lastPrice in AS, and cash in A_Sum
            #         # Num	AveCost	LastPrice	TotalCost	MV	PnL	PnL_Pct	W_Real	W_Ideal	Date	code
            #         Account_Stocks.loc[temp_index, 'Num'] = Account_Stocks.loc[temp_index, 'Num'] * temp_factor
            #         Account_Stocks.loc[temp_index, 'AveCost'] = Account_Stocks.loc[temp_index, 'AveCost'] / temp_factor
            #         # Account_Stocks.loc[temp_index, 'LastPrice'] = Account_Stocks.loc[temp_index, 'LastPrice'] / temp_factor
            #         # todo 这里假设分红分到 现金账户里了。
            #         # 0,Total_Cost,Cash,Stock,Total,Unit,MDD,Index,MDD_I
            #         Account_Sum.loc[temp_Date, 'Cash'] = Account_Sum.loc[temp_Date, 'Cash'] + max(0, dif_Cash)
            #         Account_Sum.loc[temp_Date, 'Total'] = Account_Sum.loc[temp_Date, 'Total'] + max(0, dif_Cash)

            ''' Part 2 Step 2 回测Back Testing: 持仓股票的  Portfolio Adjustment  '''
            # Input：indexName, Account_Stocks，Path_Data, temp_LastDay
            # Output0：temp_Symbol_I, temp_Symbol, , temp_Date_Index
            # Output1: temp_Signal, temp_Weight

            # todo 总量控制，基准指数买入或持有趋势时（1），可以BSH，卖出后空仓趋势时（0，-1），只能Sell，
            temp_Symbol_I = indexName  # '000300.SH'
            # todo 171130 before self.Get_Signal(temp_Symbol_I, Path_Data, temp_LastDay) 会导致指数信号用的是前一天
            (temp_Signal_Index, temp_Date_Index_Index,data_1day ,data_ALLdays) = self.Get_Signal(temp_Symbol_I, Path_Data, temp_Date )
            # print('170424 1335 ===temp_Signal_Index, temp_Date_Index_Index ' ,temp_Signal_Index, temp_Date_Index_Index  )

            for j1 in range(len(Account_Stocks['code'])):

                temp_Symbol = Account_Stocks['code'].iloc[j1]
                # print('j1', j1,temp_Symbol )
                # we want to avoid string type temp_Symbol=0.0 ,which has length 3
                if type(temp_Symbol) == str and len(temp_Symbol) > 3:
                    # print('Length of temp_Symbol(to avoid 0 value): ', len(temp_Symbol))
                    # todo Part 2 Step 2.1  Get_Signal ideal
                    # For future, if we want real-time signal, then we need to use lastest price to replace Last Day price
                    (temp_Signal, temp_Date_Index,data_1day ,data_ALLdays) = self.Get_Signal(temp_Symbol, Path_Data, temp_Date)

                    # todo Part 2 Step 2.2 Get_BSH_Weight_Amount : Portfolio角度，获取 买卖信号，权重，金额
                    # Find weight, Check Account Get price, Get holding, Do accounting
                    (temp_BSH,temp_Weight,temp_Amount ) = self.Get_BSH_Weight_Amount(temp_Signal_Index, temp_Signal,temp_Symbol,StockPool, MaxN,Account_Sum , temp_Date,  Leverage )
                    print('temp_BSH,temp_Weight,temp_Amount',temp_BSH,temp_Weight,temp_Amount,temp_Symbol  )

                    
                    # todo Part 2 Step 2.3  Buy/Sell/Hold order to trade :
                    # temp_BSH = buy 1  |  hold 0   |  sell -1
                    # todo 买入信号：
                    if temp_BSH == 1:
                        # todo Part 2 Step 2.3.1 Get trading day market/price information
                        # 获取当日市场价格信息
                        # (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_Date, Path_Data)
                        # todo 17010 0150 注意：港股组合的情况下，有可能 data_1day＝0　值和 int type ！这说明港股在这一天没有交易

                        # todo Part 2 Step 2.3.2 preTrade : Get_temp_Trade
                        # todo 交易准备：获取 持仓信息，市场可交易数量 等
                        # todo 有买入信号时，只有2种情况：1，买入；2，不买入，Notrade
                        if not type(data_ALLdays) == int:
                            temp_index1 = data_ALLdays.index[-1]
                            preClose = data_ALLdays.loc[temp_index1, 'close']
                        else :
                            preClose = 0

                        ifTrade = 0 # ifTrade = 0 means No trade and 1 means trade
                        if not type(data_1day) == int:
                            if float(data_1day['amt']) > 5000000:
                                ifTrade = 1

                        if ifTrade == 1 :
                            # todo 170401 目的：获得 temp_trade ,但现在是在 Trading_Book 里一起算
                            # Input :Account_Sum,temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,  Leverage
                            # Output :temp_Number, temp_TotalCost, temp_AveCost, temp_Fees, temp_ProfitReal
                            temp_Trade = self.Get_temp_Trade(Account_Sum,temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,preClose,Leverage)
                            print('temp_Trade ', temp_Trade )
                            temp_Number = temp_Trade.loc[0,'Number']
                            # print('TTT 170620 A ', Account_Stocks)
                            if temp_Number > 0:
                                if temp_Trade.loc[0, 'Price'] * temp_Trade.loc[0, 'Number'] >1000000:
                                    print('temp_Trade Amount', temp_Trade.loc[0, 'Price'] * temp_Trade.loc[0, 'Number'])

                                # todo P2 S1.4  Update_Trading_Book
                                Trading_Book  = self.Update_Trading_Book(temp_Trade , Trading_Book )

                                # todo P2 S1.5  Update_Accounts and StockPool
                                [Account_Sum, Account_Stocks, StockPool] = self.Update_Account_List_Trade( temp_Trade, rate_Fees,  data_1day ,Account_Stocks, Account_Sum ,StockPool ,i, Index_Sum)
                                # print('TTT 170620 B ', Account_Stocks)
                            else:
                                temp_BSH == 0
                                # Here we update Account with no trade
                                if not type(data_1day) == int:
                                    [Account_Sum, Account_Stocks ] = self.Update_Account_List_NoTrade(data_1day,  Account_Stocks,  Account_Sum,   i,  temp_Symbol,  temp_Date,  Dates)
                        else :
                            # No trade case :
                            temp_BSH == 0
                            if not type(data_1day) == int:
                                [Account_Sum, Account_Stocks] = self.Update_Account_List_NoTrade(data_1day,  Account_Stocks,  Account_Sum, i,  temp_Symbol,   temp_Date,   Dates)


                    # todo 卖出信号
                    elif temp_BSH == -1:

                        # temp_BSH = buy 1  |  hold 0   |  sell -1
                        # Get trading day price
                        # (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_Date, Path_Data)
                        if not type(data_ALLdays) == int:
                            temp_index1 = data_ALLdays.index[-1]
                            preClose = data_ALLdays.loc[temp_index1, 'close']
                        else :
                            preClose = 0

                        ifTrade = 0 # ifTrade = 0 means No trade and 1 means trade
                        if not type(data_1day) == int:
                            if float(data_1day['amt']) > 5000000:
                                ifTrade = 1

                        if ifTrade == 1:
                            print('171217 1352 temp_Signal, temp_BSH  ', temp_Signal, temp_BSH)

                            (temp_Trade) = self.Get_temp_Trade(Account_Sum, temp_Date, temp_Symbol, Account_Stocks,
                                                               rate_Fees, temp_BSH, temp_Amount, data_1day,preClose, Leverage)
                            temp_Number = temp_Trade.loc[0,'Number']

                            if temp_Number > 0:
                                print('temp_Trade', temp_Trade)
                                # todo P2 S1.4  Update_Trading_Book
                                Trading_Book = self.Update_Trading_Book(temp_Trade, Trading_Book)

                                # todo P2 S1.5  Update_Accounts and StockPool
                                print('1704031132 Account_Sum_Cash befo trade: ', temp_Date,
                                      Account_Sum.loc[temp_Date, 'Cash'], Account_Sum.loc[temp_Date, 'Stock'])
                                [Account_Sum, Account_Stocks, StockPool] = self.Update_Account_List_Trade(temp_Trade,   rate_Fees,   data_1day,  Account_Stocks,   Account_Sum,   StockPool, i,  Index_Sum)
                                print('1704031132 Account_Sum_Cash after trade: ', temp_Date,
                                      Account_Sum.loc[temp_Date, 'Cash'], Account_Sum.loc[temp_Date, 'Stock'])
                            else:
                                temp_BSH == 0
                                # Here we update Account with no trade
                                if not type(data_1day) == int:
                                    print('1704031132 Account_Sum_Cash befo trade: ', temp_Date,
                                          Account_Sum.loc[temp_Date, 'Cash'], Account_Sum.loc[temp_Date, 'Stock'])
                                    [Account_Sum, Account_Stocks, ] = self.Update_Account_List_NoTrade(data_1day,  Account_Stocks,  Account_Sum,   i,   temp_Symbol,  temp_Date,  Dates)
                                    print('1704031132 Account_Sum_Cash after trade: ', temp_Date,
                                          Account_Sum.loc[temp_Date, 'Cash'], Account_Sum.loc[temp_Date, 'Stock'])
                        else :
                            # No trade case | Here we update Account with no trade
                            temp_BSH == 0
                            if not type(data_1day) == int:
                                [Account_Sum, Account_Stocks] = self.Update_Account_List_NoTrade(data_1day,  Account_Stocks,  Account_Sum, i,  temp_Symbol, temp_Date, Dates)


                    elif temp_BSH == 0:
                        # todo We just want to keep our holding
                        # (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_Date, Path_Data)
                        if not type(data_1day) == int:
                            [Account_Sum, Account_Stocks] = self.Update_Account_List_NoTrade(data_1day,  Account_Stocks,   Account_Sum, i,  temp_Symbol,  temp_Date, Dates)

            print('Unit now', temp_Date, Account_Sum.loc[temp_Date, 'Unit'])

            ''' Part 2 Step 3 回测Back Testing: 非持仓股票的  Portfolio Adjustment  '''

            # todo P2 S3.1 计算最大可买入股票数量 = 最大持有股票数量 - 当前持有股票数量 , 以下只有买入行为，无卖出或加仓
            # temp_Signal_List = np.zeros([1,len( symbolList) ])
            # temp_Num_dif is number of stocks we can fill our account
            temp = sum(Account_Stocks['Num'] > 0)
            temp_Num_dif = MaxN - temp
            print('170829 2215 temp_Num_dif 最多可加仓数量 ', temp_Num_dif )
            print('170829 2215 after ALL1 : Account_Stocks[ MV].sum() ', Account_Stocks['MV'].sum())
            # todo number of stocks is smaller than MaxN case :
            if temp_Num_dif > 0:
                # todo P2 S3.2 Generate a list of all non-holding stocks with [BuySignal, P_H40 level]
                [temp_Sig_Ana, temp_Sort ] = self.Get_Sig_Ana( symbolList, Path_Data,temp_Date, temp_LastDay, StockPool  )

                # todo P2 S3.3 有买入信号的股票 | Buy order to trade :
                temp_Num = int(sum(temp_Sig_Ana['Signal'].values))
                print('170829 2215 temp_Num   ', temp_Num , min(temp_Num_dif, temp_Num)  )

                for j3 in range(min(temp_Num_dif, temp_Num)):
                    # todo 计算最少数量的股票
                    temp_Symbol_Index = temp_Sig_Ana.loc[temp_Sort[j3], 'Order']
                    # print('type( temp_Symbol_Index) ',  type( temp_Symbol_Index)  )
                    # todo 获取 symbolList里的股票代码
                    temp_Symbol = symbolList[int(temp_Symbol_Index)]

                    # todo P2 S3.3.1 Get Signal ideal
                    # todo Ana 既然已经在 Get_Sig_Ana中算过Signal，这里就不要再算一次，节省时间提高效率
                    temp_Signal = temp_Sig_Ana.loc[temp_Sort[j3], 'Signal']
                    print('170829 2215 temp_Stock ', temp_Symbol, 'Signal= ', temp_Signal  )
                    if temp_Signal_Index < 1:
                        temp_Signal = -1
                    # todo P2 S3.3.2  Get_BSH_Weight_Amount : Portfolio角度，获取 买卖信号，权重，金额
                    temp_BSH = 0
                    if temp_Signal == 1.0 :
                        print('---170829 2215 temp_Stock ', temp_Symbol, 'Signal= ', temp_Signal)
                        (temp_BSH, temp_Weight, temp_Amount) = self.Get_BSH_Weight_Amount(temp_Signal_Index,temp_Signal,temp_Symbol, StockPool, MaxN, Account_Sum, temp_Date, Leverage)
                        print('temp_BSH,temp_Weight,temp_Amount 170829 2215  ', temp_BSH, temp_Weight, temp_Amount)

                    # todo P2 S3.3.3 Buy/Sell/Hold order to trade :
                    # temp_BSH = buy 1  |  hold 0   |  sell -1
                    # todo 买入信号：
                    if temp_BSH == 1:
                        # todo Get trading day market/price information 获取当日市场价格信息
                        (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_Date,  Path_Data)

                        # todo 17010 0150 注意：港股组合的情况下，有可能 data_1day＝0　值和 int type ！这说明港股在这一天没有交易
                        # todo 交易准备：获取 持仓信息，市场可交易数量 等
                        # todo 有买入信号时，只有2种情况：1，买入；2，不买入，Notrade

                        if not type(data_ALLdays) == int :
                            temp_index1 = data_ALLdays.index[-1]
                            preClose = data_ALLdays.loc[temp_index1, 'close']
                        else :
                            preClose = 0

                        ifTrade = 0  # ifTrade = 0 means No trade and 1 means trade
                        if not type(data_1day) == int:
                            if float(data_1day['amt']) > 5000000:
                                ifTrade = 1

                        if ifTrade == 1:
                            # todo 170401 目的：获得 temp_trade ,但现在是在 Trading_Book 里一起算
                            # Input :Account_Sum,temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,  Leverage
                            # Output :temp_Number, temp_TotalCost, temp_AveCost, temp_Fees, temp_ProfitReal
                            temp_Trade = self.Get_temp_Trade(Account_Sum, temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,preClose, Leverage)
                            print('temp_Trade 170829 2215  ', temp_Trade )
                            temp_Number = temp_Trade.loc[0,'Number']
                            if temp_Number > 0:
                                print('temp_Trade', temp_Trade)
                                if temp_Trade.loc[0, 'Price'] * temp_Trade.loc[0, 'Number'] >1000000:
                                    print('temp_Trade Amount', temp_Trade.loc[0, 'Price'] * temp_Trade.loc[0, 'Number'])

                                # todo P2 S3.3.4 Update_Trading_Book
                                Trading_Book = self.Update_Trading_Book(temp_Trade, Trading_Book)
                                # todo P2 S3.3.5 Update_Accounts and StockPool
                                print('170829 2215 before : Account_Stocks[ MV].sum() ', Account_Stocks[ 'MV'].sum() )
                                [Account_Sum, Account_Stocks, StockPool] = self.Update_Account_List_Trade(temp_Trade,  rate_Fees,  data_1day,  Account_Stocks,  Account_Sum,  StockPool, i, Index_Sum)
                                print('170829 2215 after  : Account_Stocks[ MV].sum() ', Account_Stocks['MV'].sum()  )
                                # print('1704031132 Account_Sum_Cash after trade: ', temp_Date, Account_Sum.loc[temp_Date, 'Cash'], Account_Sum.loc[temp_Date, 'Stock'] )

                                # 由于之前无持仓，不需要  Update_Account_List_NoTrade
                    #Singal Stock Done ====================================================================
                # all Stock Done ====================================================================
            # All stocks calculation of current day done
            print('170829 2215 after ALL2 : Account_Stocks[ MV].sum() ', Account_Stocks['MV'].sum())

            ''' Part 3 回测Back Testing: Data Ana, Statistics, I/O and Visualization  '''
            temp_MDD = Account_Sum.loc[ temp_Date, 'Total'] / max( Account_Sum.loc[Dates[:i], 'Total']) - 1
            # print('170313 1019 ---temp_MDD ',temp_MDD )
            Account_Sum.loc[temp_Date, 'MDD'] = min(Account_Sum.loc[Dates[i - 1], 'MDD'], temp_MDD)
            # print('170313 1019 ---temp_MDD ', Account_Sum.loc[Dates[i-1],'MDD'] )

            # todo 170317 下边 Unit 从 上边 'Unit now' 处的 1.03 变成了 0.15
            print('Unit now', temp_Date)
            print(Account_Sum.loc[temp_Date, 'Unit'])

            # todo P3 Save to Database and to backup csv files交易日文件存档
            # todo Trading_Book, index_Trading_Book  ,Stock_MV, Account_Sum, StockList, StockPool
            Account_Sum.to_csv(Path_TradeSys + '\\' + 'Account_Sum_' + temp_Date + '.csv')
            Account_Stocks.to_csv(Path_TradeSys + '\\' + 'Account_Stocks_' + temp_Date + '.csv')
            # Account_Stocks2.to_csv(Path_TradeSys + '\\' + 'Account_Stocks2_'+ temp_Date + '.csv')
            StockPool.to_csv(Path_TradeSys + '\\' + 'StockPool_' + temp_Date + '.csv')
            Trading_Book.to_csv(Path_TradeSys + '\\' + 'Trading_Book_' + temp_Date + '.csv')
            # todo 170109 2305
            temp_Sig_Ana.to_csv(Path_TradeSys + '\\' + 'Sig_Ana' + temp_Date + '.csv')

            # todo  ==============================================================================================

            ''' Part 3 Step 1 初始化模拟实盘Live：下一个交易日交易计划/指令 pre_Orders　 '''
            # todo VIP Part 3 部分所有和上边同名或者新增的变量，都需要加'_Next'后缀，以避免潜在数据问题

            # todo 输出形式　pre_Orders　列出所有有买入信号的股票并排序(sig_Ana\'order')，和卖出指令
            # todo 以防次日在前20-30个股票中遇到无法交易的股票
            #   由于每日的Orders之间是independent，所以每个交易日重新初始化即可
            trade_NextDay = pd.DataFrame(columns=columns_trade_NextDay )
            # todo ID-Event ：0331xx
            # todo Part 3 Step 1.1 获取 temp_Day 当日交易信号，价格等 |  ID-Event ：0331xx
            # todo 决定用 _next 作为所有相关变量的后缀

            ''' Part 3 Step 2 模拟实盘Live： 持仓股票的  Portfolio Adjustment  '''
            # Input：indexName, Account_Stocks，Path_Data, temp_LastDay
            # Output0：temp_Symbol_I, temp_Symbol, , temp_Date_Index
            # Output1: temp_Signal, temp_Weight
            # todo 总量控制，基准指数买入或持有趋势时（1），可以BSH，卖出后空仓趋势时（0，-1），只能Sell，
            temp_Symbol_I = indexName  # '000300.SH'
            # todo 170403 : signal_Index_Next for tomorrow
            (temp_Signal_Index_Next, temp_Date_Index_Index_Next) = self.Get_Signal_Next(temp_Symbol_I, Path_Data, temp_Date)
            # 为 Part 3 Step 3 准备的现金值
            temp_Cash = Account_Sum.loc[Dates[i], 'Cash']
            print('temp_Cash ', temp_Cash )

            for j1 in range(len(Account_Stocks['code'])):
                temp_Symbol = Account_Stocks['code'].iloc[j1]
                print('P3S2 temp_Symbol ', temp_Symbol)
                # print('j1', j1,temp_Symbol )
                # we want to avoid string type temp_Symbol=0.0 ,which has length 3
                if type(temp_Symbol) == str and len(temp_Symbol) > 3:
                    # print('Length of temp_Symbol(to avoid 0 value): ', len(temp_Symbol))
                    # todo Part 3 Step 2.1  Get_Signal ideal
                    # For future, if we want real-time signal, then we need to use lastest price to replace Last Day price
                    [temp_Signal_Next, temp_Date_Index_Next ] = self.Get_Signal_Next(temp_Symbol, Path_Data, temp_Date)

                    # todo Part 3 Step 2.2 Get_BSH_Weight_Amount : Portfolio角度，获取 买卖信号，权重，金额
                    # Find weight, Check Account Get price, Get holding, Do accounting
                    # [temp_BSH_Next , temp_Weight_Next, temp_Amount_Next ] = self.Get_BSH_Weight_Amount(temp_Signal_Index_Next, temp_Signal_Next,
                    #                                                                   temp_Symbol, StockPool, MaxN,  Account_Sum, temp_Date, Leverage)
                    # print('temp_BSH_Next ,temp_Weight_Next ,temp_Amount_Next ',temp_BSH_Next , temp_Weight_Next, temp_Amount_Next)

                    # todo Part 3 Step 2.3  Buy/Sell/Hold order to trade :
                    temp_BSH_Next = temp_Signal_Next
                    # todo 买入信号：
                    if temp_BSH_Next  == 1:
                        # todo Part 3 Step 2.3.1 以temp_Day 收盘价close作为模拟成交价格，而不是明日的开盘价。
                        (data_1day_Next, data_ALLdays_Next, temp_Date_Index_Next) = self.Get_Price_Ret_Next(temp_Symbol, temp_Date,  Path_Data)
                        # todo data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日

                        # todo Part 3 Step 2.3.2 preTrade : Get_temp_Trade
                        # todo 这里不需要 ifTrade  | 交易准备：获取 持仓信息，市场可交易数量 等
                        # todo 有买入信号时，只有2种情况：1，买入；2，不买入，Notrade
                        # todo 170401 目的：获得 temp_trade ,但现在是在 Trading_Book 里一起算
                        # Input :Account_Sum,temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,  Leverage
                        # Output :temp_Number, temp_TotalCost, temp_AveCost, temp_Fees, temp_ProfitReal
                        temp_Amount = Initial*(1-Leverage)/MaxN
                        temp_Trade_Next = self.Get_temp_Trade_Next(Account_Sum, temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day_Next, Leverage)

                        print('temp_Trade_Next ', temp_Trade_Next )
                        # todo Part 3 Step 2.3.2 save temp_Trade_Next to  trade_NextDay
                        trade_NextDay = trade_NextDay.append( temp_Trade_Next , ignore_index=True)
                        print('trade_NextDay ', trade_NextDay)
                        # todo update estimating cash level | cash in if trade_NextDay.loc[0,'Buy/Sell'] ==-1
                        temp_Cash = temp_Cash - trade_NextDay.loc[0,'Buy/Sell']*trade_NextDay.loc[0,'Price']*trade_NextDay.loc[0,'Number']

                    # todo 卖出信号
                    elif temp_BSH_Next == -1:
                        (data_1day_Next, data_ALLdays_Next, temp_Date_Index_Next) = self.Get_Price_Ret_Next(temp_Symbol, temp_Date, Path_Data)
                        # todo data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日
                        # todo Part 3 Step 2.3.2 preTrade : Get_temp_Trade
                        temp_Amount = Initial * (1 - Leverage) / MaxN
                        temp_Trade_Next = self.Get_temp_Trade_Next(Account_Sum, temp_Date, temp_Symbol, Account_Stocks,
                                                                   rate_Fees, temp_BSH, temp_Amount, data_1day_Next,  Leverage)
                        print('temp_BSH_Next == -1temp_Trade_Next ', temp_Trade_Next)
                        # todo Part 3 Step 2.3.2 save temp_Trade_Next to  trade_NextDay
                        trade_NextDay = trade_NextDay.append(temp_Trade_Next, ignore_index=True)
                        print('temp_BSH_Next == -1 trade_NextDay ', trade_NextDay)

                        # todo update estimating cash level | cash in if trade_NextDay.loc[0,'Buy/Sell'] ==-1
                        temp_Cash = temp_Cash - trade_NextDay.loc[0, 'Buy/Sell'] * trade_NextDay.loc[0, 'Price'] * trade_NextDay.loc[0, 'Number']

                    # todo 无卖出信号时，无交易

            ''' Part 3 Step 3 模拟实盘Live： 非持仓股票的  Portfolio Adjustment  '''
            # 原来是计算尽量少的股票交易，现在从不确定性的角度，应该计算最多 MaxN+5只股票的信号。
            # todo P3 S3.1 估计trade_NextDay 交易后账户可用资金，最多可买股票数量
            temp_MaxN = round( MaxN * temp_Cash/Initial )+1
            print('temp_MaxN ', temp_MaxN)
            # todo number of stocks under estimating cash level
            if temp_MaxN >0 :
                # todo P3 S3.2 Generate a list of all non-holding stocks with [BuySignal, P_H40 level]
                [temp_Sig_Ana_Next , temp_Sort_Next] = self.Get_Sig_Ana_Next( symbolList, Path_Data,temp_Date, StockPool  )
                print('temp_Sig_Ana_Next ', temp_Sig_Ana_Next)

                # todo P3 S3.3 有买入信号的股票 | Buy order to trade :
                temp_Num = int(sum(temp_Sig_Ana_Next['Signal'].values))

                # todo 下边的 5 是个估计数，未来可能根据实际情况再做调整。
                temp_N = int( min( temp_MaxN, MaxN )+ 5)
                # print('temp_Num  temp_N ',temp_Num , temp_N)

                for j3 in range(min(temp_N, temp_Num)):
                    # todo 计算最少数量的股票
                    temp_Symbol_Index = temp_Sig_Ana_Next.loc[temp_Sort_Next[j3], 'Order']
                    # print('type( temp_Symbol_Index) ',  type( temp_Symbol_Index)  )
                    # todo 获取 symbolList里的股票代码
                    temp_Symbol = symbolList[int(temp_Symbol_Index)]
                    print('P3S3 temp_Symbol ',temp_Symbol)

                    # todo Ana 既然已经在 Get_Sig_Ana中算过Signal，这里就不要再算一次，节省时间提高效率
                    temp_Signal_Next = temp_Sig_Ana.loc[ temp_Sort_Next[j3], 'Signal']

                    if temp_Signal_Index_Next < 1:
                        temp_Signal_Next = -1

                    # todo 170404 Account_Sum 未更新NextDay的持仓股调整，因此 Get_BSH_Weight_Amount 没有用
                    temp_BSH_Next = temp_Signal_Next
                    if temp_BSH_Next  == 1:
                        # todo 买入信号：
                        # todo Part 3 Step 2.3.1 以temp_Day 收盘价close作为模拟成交价格，而不是明日的开盘价。
                        (data_1day_Next, data_ALLdays_Next, temp_Date_Index_Next) = self.Get_Price_Ret_Next(temp_Symbol, temp_Date,  Path_Data)
                        # todo data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日

                        # todo Part 3 Step 2.3.2 preTrade : Get_temp_Trade
                        # todo 这里不需要 ifTrade  | 交易准备：获取 持仓信息，市场可交易数量 等
                        # todo 有买入信号时，只有2种情况：1，买入；2，不买入，Notrade
                        # todo 170401 目的：获得 temp_trade ,但现在是在 Trading_Book 里一起算
                        temp_Amount = Initial * (1 - Leverage) / MaxN
                        temp_Trade_Next = self.Get_temp_Trade_Next(Account_Sum, temp_Date, temp_Symbol,  Account_Stocks, rate_Fees, temp_BSH, temp_Amount,  data_1day_Next, Leverage)

                        print('temp_Trade_Next ', temp_Trade_Next)
                        # todo Part 3 Step 2.3.2 save temp_Trade_Next to  trade_NextDay
                        trade_NextDay = trade_NextDay.append(temp_Trade_Next, ignore_index=True)
                        print('trade_NextDay ', trade_NextDay)
                        # todo update estimating cash level | cash in if trade_NextDay.loc[0,'Buy/Sell'] ==-1
                        # temp_Cash = temp_Cash - trade_NextDay.loc[0, 'Buy/Sell'] * trade_NextDay.loc[0, 'Price'] * \
                        #                         trade_NextDay.loc[0, 'Number']
            ''' Part 3 Step 4  模拟实盘Live Output result   '''
            # todo Output Sig_Ana_Next for Next Day
            temp_Sig_Ana_Next.to_csv(Path_TradeSys + '\\' + 'Sig_Ana_Next_' + temp_Date + '.csv')
            # todo Output Trade for Next Day
            trade_NextDay.to_csv(Path_TradeSys + '\\' + 'trade_NextDay_' + temp_Date + '.csv')

            # todo 每日交易完成 =============================================================================

        Account_Sum.to_csv(Path_TradeSys + '\\' + 'Account_Sum' + '.csv')
        Account_Stocks.to_csv(Path_TradeSys + '\\' + 'Account_Stocks' + '.csv')
        # Account_Stocks2.to_csv(Path_TradeSys + '\\' + 'Account_Stocks2' + '.csv')
        StockPool.to_csv(Path_TradeSys + '\\' + 'StockPool' + '.csv')
        Trading_Book.to_csv(Path_TradeSys + '\\' + 'Trading_Book' + '.csv')
        # todo Output Signal list
        temp_Sig_Ana.to_csv(Path_TradeSys + '\\' + 'Sig_Ana' + '.csv')

        # todo Output Sig_Ana_Next for Next Day
        temp_Sig_Ana_Next.to_csv(Path_TradeSys + '\\' + 'Sig_Ana_Next' + '.csv')
        # todo Output Trade for Next Day
        trade_NextDay.to_csv(Path_TradeSys + '\\' + 'trade_NextDay' + '.csv')

        # todo P4 Generate Analyzing profile, table and figure
        # ''' 其次，最需要加仓/dif_weight= StockPool['W_Ideal']- StockPool['W_Real']最大的股票 '''
        # ''' 再次，依次计算StockPool['B/S/H']=Buy的股票，'''
        # ''' 最后，如果持仓股还有空间，在剩余股票中寻找值得买入的股票'''
        # ''' 注意，设计一个机制，使得在市场较弱的情况下，和市场走势一致的股票总仓位比例较低'''

        print('File has been saved to :', Path_TradeSys )
        Portfolio_L = 0
        print('The Portfolio_L has been Update..')

        return Portfolio_L

    def Get_Portfolio_info_Live(self,temp_Port ,Log_Sys_Live ,path_Sys ,date_LastUpdate_New):
        # todo 抓取实盘组合信息
        # todo derived from Get_Portfolio_info
        # todo last updaate 170408 1245 | since 170407 1409
        # todo 170401 这个module 主要是用来获取单个Portfolio 的信息,以更新Portfolio
        # start_Date ,indexName, symbolList, Dates, path_Input, Path_Port, Leverage, MaxN
        # todo 如果每次都重新导入 rC_Portfolio_17Q1，不知道会不会消除各个组合互相引用持仓数据的问题
        print('The portfolio live we want to update is :')
        print(temp_Port)
        temp_Portfolio = temp_Port
        # path_Portfolio = 'D:\data_Output\Sys_rC1703_1703\Port_SZ50_000300.SH_170306_40_20_0.95'
        # path_Portfolio = path_Sys  # + '\\' + temp_dir
        # todo 170407 新建 Log_Portfolio_Live
        temp_Log_Name = 'Log_' + temp_Portfolio +'_Live'

        # todo Part 1 获取 Log_Portfolio_Live
        Log_Portfolio_Live = pd.read_csv(path_Sys + '\\' + temp_Log_Name + '.csv')
        Log_Portfolio_Live.index = Log_Portfolio_Live[Log_Portfolio_Live.columns[0]]
        Log_Portfolio_Live = Log_Portfolio_Live.drop(Log_Portfolio_Live.columns[0], axis=1)
        print(Log_Portfolio_Live.columns)
        print(Log_Portfolio_Live.index)
        print('Import Log_Portfolio Done . ', Log_Portfolio_Live.loc['Index_Name', 'value'],
              Log_Portfolio_Live.loc['Unit', 'value'])
        # todo 根据Log_Portfolio_Live 生成 temp_dir ????
        indexName = Log_Portfolio_Live.loc['Index_Name', 'value']
        date_Start = Log_Portfolio_Live.loc['date_Start', 'value']
        date_Initial = Log_Sys_Live.loc['InitialDate', 'value']
        str_MaxN = Log_Portfolio_Live.loc['MaxN', 'value']
        str_Leverage = Log_Portfolio_Live.loc['Leverage', 'value']
        # print( type(date_Initial),type(str_MaxN), type(str_Leverage) ) | 1704081244 :float, str,str
        temp_dir = 'Port_' + temp_Portfolio + '_' + indexName + '_' + str(date_Initial) + '_' + str_MaxN + '_' + str_Leverage

        # print( Log_Portfolio.loc[ 'Index_Name','value' ] )
        # todo 获取我们要更新组合的参数
        # Dates come from Index_Name， start_date=lastUpdate, end_date= CurrentDate
        Index_Name = Log_Portfolio_Live.loc['Index_Name', 'value']
        # symbolList comes from Portfolio_Name,path_Symbol
        Portfolio_Name = Log_Portfolio_Live.loc['Portfolio_Name', 'value']
        MaxN = Log_Portfolio_Live.loc['MaxN', 'value']
        MaxN = int(MaxN)
        Leverage = Log_Portfolio_Live.loc['Leverage', 'value']
        Leverage = float(Leverage)
        # Dates come from Index_Name， start_date=lastUpdate, end_date= CurrentDate
        date_Start = Log_Portfolio_Live.loc['date_Start', 'value']
        # todo 170313 如果每次都是对全部组合从同意交易日开始更新， 用 Log_Sys.loc['date_LastUpdate', 'value']
        # todo 如果每个组合最近一次更新时间不同，那么用 date_LastUpdate = Log_Portfolio_Live.loc['date_LastUpdate', 'value']
        # date_LastUpdate = Log_Sys.loc['date_LastUpdate', 'value']
        date_LastUpdate = Log_Portfolio_Live.loc['date_LastUpdate', 'value']

        # symbolList comes from Portfolio_Name,path_Symbol
        path_Symbol = Log_Portfolio_Live.loc['path_Symbol', 'value']

        # todo 用初步参数，计算需要的参数
        # todo Get symbolList |
        SymbolList_raw = pd.read_csv(path_Symbol, header=None, sep=',')
        symbolList = SymbolList_raw[0][:]
        print(symbolList)
        # todo Get Dates | Get benchmark index data
        path_Input = 'D:\\data_Input_Wind'
        fileName_Date = path_Input + '\Wind_' + Index_Name + '.csv'
        data = pd.read_csv(fileName_Date, header=None, skiprows=1, sep=',')
        data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        data2 = data.dropna(axis=0)

        # todo Get Dates | len_Days 用来确定 Dates长度
        # todo Get Dates | step 1 为了避免出现初始日期是非交易日，先做一个slicing
        # 选大于，一般数据量会小一些

        end_Date = '20' + date_LastUpdate_New[:2] + '-' + date_LastUpdate_New[2:4] + '-' + date_LastUpdate_New[
                                                                                           -2:]
        date_Start = date_LastUpdate
        # from '170101' to '2017-01-01''
        start_Date = '20' + date_Start[:2] + '-' + date_Start[2:4] + '-' + date_Start[-2:]
        print('171103 1856 start_Date ', start_Date)
        # todo data2[data2['date'] > start_Date] to data2[data2['date'] >= start_Date]
        # print('data2 ', data2.tail(3))
        temp_List = data2[data2['date'] >= start_Date]

        # print('start_Date', start_Date)
        # todo Get Dates | step 2 Find the starting index in Data2 of index
        if len(temp_List) > 0 and temp_List.index[0] - 1 > 0:
            # todo 1704031249 更新日期多了1天，from start_Date_Index = temp_List.index[0] - 1 to temp_List.index[0]
            start_Date_Index = temp_List.index[0]
            # todo 170313 < 有问题，可能还是应该 <=
            temp_List = data2[data2['date'] <= end_Date]
            end_Date_Index = temp_List.index[-1] + 1

            Date_List = data2.loc[start_Date_Index:end_Date_Index, 'date']
            # todo Get Dates |step 3 Find the len_Days
            # len_Days = len(temp_List ) + 1
            len_Days = len(Date_List)

            # todo Get Dates | step 4  确定 Dates 中日期长度
            Dates = list(Date_List)
            print('====== Dates =======len_Days ================')
            # 171103 1854
            start_Date = Dates[0]
            print('start_Date', Dates[0])
            print('end_Date', Dates[-1])
            print('len_Days', len_Days )

            # todo Get indexName, Path_Port
            indexName = Index_Name
            Path_Port = path_Sys + '\\' + temp_dir

        return Log_Portfolio_Live, start_Date ,indexName, symbolList, Dates, path_Input, Path_Port, Leverage, MaxN

    def Tran_Portfolio_Data_Live(self,temp_PreDate, temp_Date, path_data_Sum,  Path_TradeSys, Fees ) :
        # 导入券商来源的数据文件，转换成标准格式的文件。 || 更新当日 TradeBook，cash， MV
        # path_data_Sum = 'D:\data_Output\Sys_rC1703_1704\Port_Live\data_Sum'
        # Path_TradeSys = 'D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329_000300.SH_170405_10_0.95'
        # 根据 Port_Live 里的实盘数据，更新 Port_Live_symbol_List_170329 里的文件 AS， A_Sum, TB, SP 等
        # last update 1704161024   | since 170411 1025
        # derived from [Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana] = self.Import_Portfolio_Data_Live(temp_Date, path_data_Sum, path_Port_Live , start_Date )
        # similar to :  def Tran_Live2Standard(self, temp_Date, Account_Sum_Live ,Account_Stocks_Live, StockPool_Live ,Trading_Book_Live, temp_Sig_Ana_Live )
        # path_data_Sum, path_Port_Live =  D:\data_Output\Sys_rC1703_1704\Port_Live\data_Sum
        # todo Step 1 Import Live data AS， A_Sum, TB, SP 等 from Port_Live_symbol_List_170329
        # todo Ana 170411 因为目前实际交易会频繁出现偏离程序计划交易的情况(初期)，但也不能直接导入券商持仓，但券商持仓的成本价是不对的。
        # todo 以170405为基准日，步骤：1，导入交易单，加入到交易明细中。也就是说，要从 Port_Live 中导入的文件主要是
        # todo TradeBook_170405 , Account_Sum的现金
        # temp_Date = 2017-04-07
        temp_Date_str = temp_Date[2:4] + temp_Date[5:7] + temp_Date[8:10]
        temp_PreDate_str = temp_PreDate[2:4] + temp_PreDate[5:7] + temp_PreDate[8:10]
        # , encoding='gbk', header=None, skiprows=0, sep=','
        temp_TB = pd.read_csv( path_data_Sum + '\\' + 'TradeBook_Last_' + temp_Date_str + '.csv', encoding='gbk'  )
        temp_TB.index = temp_TB[temp_TB.columns[0]]
        temp_TB = temp_TB.drop(temp_TB.columns[0], axis=1)

        temp_A_Sum = pd.read_csv( path_data_Sum  + '\\' + 'Account_Sum_' + temp_Date_str + '.csv' , encoding='gbk')
        temp_A_Sum.index = temp_A_Sum[temp_A_Sum.columns[0]]
        temp_A_Sum = temp_A_Sum.drop(temp_A_Sum.columns[0], axis=1)

        # todo Step 2 transpose var: def Tran_Live2Standard(self, temp_Date, Account_Sum_Live ,Account_Stocks_Live, StockPool_Live ,Trading_Book_Live, temp_Sig_Ana_Live )
        # todo 导入Port_Live的 AS A_Sum TB SP 信息
        # todo  导入昨日的持仓信息
        # path_Port_Live = D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329

        # AS_Live = pd.read_csv( path_Port_Live + '\\' + 'Account_Stocks_' + temp_PreDate + '.csv' )
        # AS_Live.index = AS_Live[AS_Live.columns[0]]
        # AS_Live = AS_Live.drop(AS_Live.columns[0], axis=1)

        A_Sum_Live = pd.read_csv( Path_TradeSys  + '\\'  + 'Account_Sum_' + temp_PreDate + '.csv', encoding='gbk')
        # A_Sum_Live.index = A_Sum_Live[ A_Sum_Live.columns[0]]
        # A_Sum_Live = A_Sum_Live.drop( A_Sum_Live.columns[0], axis=1)
        # No need to import TB ?
        TB_Live = pd.read_csv( Path_TradeSys + '\\'   + 'Trading_Book_' + temp_PreDate + '.csv', encoding='gbk')
        # TB_Live.index = TB_Live[ TB_Live.columns[0]]
        # TB_Live = TB_Live.drop( TB_Live.columns[0], axis=1)

        # SP_Live = pd.read_csv(path_Port_Live + '\\' + 'StockPool_' + temp_PreDate + '.csv')
        # SP_Live.index = SP_Live[ SP_Live.columns[0]]
        # SP_Live = SP_Live.drop( SP_Live.columns[0], axis=1)

        # todo 170411 1759 根据导入的交易和现金数据，更新昨日的持仓表至今日
        # todo 每日的 TradeBook 是空值
        # TradeBook_Last to TradeBook : Account	Date	Symbol	Buy/Sell	Price	Number	AveCost	Fees	ProfitReal
        Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        # temp_Trade = pd.DataFrame(np.zeros([1, 9]), columns=Columns_Trade)

        for temp_index in temp_TB.index :
            temp_Trade = pd.DataFrame(np.zeros([1, 9]), columns=Columns_Trade)
            temp_Trade['Date'] = temp_Date
            # windcode,type   Get_Windcode(self, code)
            code = temp_TB.loc[temp_index, '代码']
            [ windcode, type] = self.Get_Windcode( code )
            # print('170416 1114, code windcode', code, windcode)
            temp_Trade['Symbol'] = windcode # todo

            if temp_TB.loc[temp_index, '交易方向'] == '买入' or temp_TB.loc[temp_index, '交易方向'] == '证券买入' :
                temp_Trade['Buy/Sell'] = 1 # todo  1，-1,0
            elif  temp_TB.loc[temp_index, '交易方向'] == '卖出' or temp_TB.loc[temp_index, '交易方向'] == '证券卖出' :
                temp_Trade['Buy/Sell'] = -1  # todo   1，-1,0
            temp_Trade['Price'] =  temp_TB.loc[temp_index, '成交均价'] # todo
            temp_Trade['Number'] = temp_TB.loc[temp_index, '数量'] # todo
            temp_Trade['AveCost'] = temp_TB.loc[temp_index, 'costAve'] # todo
            temp_Trade['Fees'] = temp_TB.loc[temp_index, '总金额'] * Fees
            temp_Trade['ProfitReal'] = temp_TB.loc[temp_index, 'ProfitReal'] # todo
            # todo 170412 1126 append to TB
            TB_Live = TB_Live.append(temp_Trade , ignore_index=True)

        # Get A_Sum info ,from A_Sum_Live
        print('temp_A_Sum')
        print( temp_A_Sum['可用'] )
        if temp_A_Sum.loc[0, '可用']  > 0 :
            cash_Broker = temp_A_Sum.loc[0, '可用']
            MV_Broker = temp_A_Sum.loc[0, '参考市值']
        else :
            cash_Broker = 0
            MV_Broker = 0



        # todo Step 3 Output to csv : TradeBook , cash info
        TB_Live.to_csv( Path_TradeSys + '\\' + 'Trading_Book_' + temp_Date_str + '.csv')


        return TB_Live, cash_Broker , MV_Broker

    def rC_Portfolio_L_Live(self,start_Date, indexName,symbolList, Dates,Path_Data ,Path_TradeSys,path_Port_Live , Leverage=0.95, Initial=10000000, MaxN=30, if_change_SP =0 ):
        # Parameters  fee1 = 0.0015
        '''
        目标 ：
        Path_TradeSys = 'D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329_000300.SH_170405_10_0.95'
        path_Port_Live = 'D:\data_Output\Sys_rC1703_1704\Port_Live'
        derived from rC_Portfolio_L, to initialize AS, A_Sum,TB and SP file for portfolio
        Part 1 : Initialize our portfolio
        Path_TradeSys = 'D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329_000300.SH_170405_10_0.95'
        path_Port_Live = 'D:\data_Output\Sys_rC1703_1704\Port_Live'
        Last update 170416 0938 | Since 170403 1318
        '''

        # todo P1 Step1 初始化组合信息 : Account_Sum ,  Account_Stocks, Trading_Book, StockPool
        [Account_Sum ,  Account_Stocks, Trading_Book, StockPool,temp_Sig_Ana ] = self.Generate_Portfolio(Dates,Initial, MaxN,symbolList )

        temp_Date = Dates[0] # Dates ['2017-03-31', '2017-04-05']
        # todo P3 Save to Database and to backup csv files交易日文件存档
        # todo Trading_Book, index_Trading_Book  ,Stock_MV, Account_Sum, StockList, StockPool
        # Path_TradeSys = D:\data_Output\Sys_rC1703_1704\Port_Live
        # path_Port_Live =
        Account_Sum.to_csv(Path_TradeSys + '\\' + 'Account_Sum_' + temp_Date + '.csv')
        print( path_Port_Live )

        Account_Stocks.to_csv(Path_TradeSys + '\\' + 'Account_Stocks_' + temp_Date + '.csv')
        # Account_Stocks2.to_csv(Path_TradeSys + '\\' + 'Account_Stocks2_'+ temp_Date + '.csv')
        StockPool.to_csv(Path_TradeSys + '\\' + 'StockPool_' + temp_Date + '.csv')
        Trading_Book.to_csv(Path_TradeSys + '\\' + 'Trading_Book_' + temp_Date + '.csv')
        # todo 还是新建一个 temp_Sig_Ana，为了新建System时，利用 rC_Portfolio_L_Update
        # temp_Sig_Ana.to_csv(Path_TradeSys + '\\' + 'Sig_Ana_' + temp_Date + '.csv')
        temp_Sig_Ana.to_csv(Path_TradeSys + '\\' + 'Sig_Ana' + temp_Date + '.csv')
        print('temp_Date ', temp_Date)
        print('Dates',Dates)

        # todo P1 Step2 初始化其他参数
        # Path_TradeSys = 'F:\\rC_Py3_Output\\TradeSys_0'
        weight_Method = 'equalWeight_ini'
        rate_Fees = 0.0025
        start_Date = Dates[0]
        # print( start_Date )

        Portfolio_L = self.rC_Portfolio_L_Live_Update( start_Date, indexName, symbolList, Dates,Path_Data, Path_TradeSys,path_Port_Live, Leverage, MaxN ,Initial ,if_change_SP)

        return Portfolio_L

    def rC_Portfolio_L_Live_Update(self, start_Date, indexName,symbolList, Dates, path_Input ,Path_TradeSys,path_Port_Live, Leverage=0.95, MaxN=30,Initial= 110000,if_change_SP = 0  ):
        import numpy as np
        import pandas as pd
        '''
        # todo ===============================================================================
        # 170802 内置MaxN 的变量，可以临时持股超过 给定MaxN

        # 用来对实盘的持仓股票进行更新
        last update 170414 | since 170406 2334
        # Derived from rC_Portfolio_L_Update
        Path_TradeSys = 'D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329_000300.SH_170405_10_0.95'
        path_Port_Live = 'D:\data_Output\Sys_rC1703_1704\Port_Live'


        # todo rC_Portfolio_L_Update 是在一段时间内，对Portfolio的持仓和交易变化进行更新
        # todo 对于每一个交易日结束后，需要操作的功能包括 当日账户回测 和 次日模拟实盘交易单
        # derived from rC_Portfolio_L （会从0开始建立一个Portfolio）

        # Part 1 : Portfolio Initialization
        #   导入前一交易日各类变量，新建各类变量，参数
        # Part 2 交易日回测 Back Testing
        # Part 3 模拟实盘Live

        # todo ===============================================================================
        '''

        ''' Part 1 :  Portfolio Initialization   '''
        # Input： Dates, Path_TradeSys , start_Date, path_Input,
        # Output： Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana
        # todo 导入组合信息

        # todo Dates[0] 是已经更新过的交易日的最后一天，Dates[1] 是还未更新的交易日的第一天
        temp_LastDay = Dates[0]
        i = 1
        temp_Date = Dates[i]

        # print('Path_TradeSys',Path_TradeSys)
        # print('path_Port_Live', path_Port_Live )
        path_data_Sum = path_Port_Live + '\\' +'data_Sum'
        Fees = 0.0025
        # todo Get tradebook and cash, MV info of current day
        # [Trading_Book, cash_Broker, MV_Broker] = self.Tran_Portfolio_Data_Live( temp_LastDay , temp_Date, path_data_Sum, Path_TradeSys , Fees)

        # todo Get yesterday A_sum ， AS， SP， TB
        # todo 170412 1352  Account_Sum_Live ,Account_Stocks_Live, StockPool_Live
        # Path_TradeSys = D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329 是实盘跟踪系统的文件夹
        [Account_Sum ,Account_Stocks, StockPool ] = self.Import_Portfolio_Data_Live(  Path_TradeSys , start_Date )

        # todo Update StockPool from start_Date , 170524
        # # Input : SP, start_Date, path_
        # # Output: SP
        path_Input2 = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Stockpool'
        # path_Output = 'D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329_000300.SH_170405_12_0.95'
        path_Output = Path_TradeSys   # path_Port =Path_TradeSys
        if if_change_SP == 1:
            # todo
            # print(StockPool.tail(5))
            StockPool = self.Update_StockPool_Outside(start_Date, StockPool, path_Input2 , path_Output  )
            # print('170608 0103 ')
            # print( StockPool.tail(5) )

        if len(Account_Sum.index) < 1:
            # todo it means we are at the first day here, we need to initialize preDate and lastDate .
            Account_Sum.loc[temp_LastDay, :] = [0, Initial, 0, Initial, 1, 0, 1, 0]
            Account_Sum.loc[temp_Date, :] = [0, Initial, 0, Initial, 1, 0, 1, 0]
            # todo 刚初始化时，Account_Sum 里完全没有数据，如果用在下边的代码里，可能会报错。要么初始化是对第一日进行填入数据。
            # Missing :  i 是表示更新的第几天， Index_Sum= Dates，
            i = 1



        ''' Part 1 :  Portfolio Initialization : Check    '''
        print('170503 temp_Date ', temp_Date  )
        print('170503 Account_Stocks ', Account_Stocks)
        # 170504 1601
        # for temp_index in Account_Stocks.index :
        #     if Account_Stocks.loc[temp_index,'Num'] >0 and len(Account_Stocks.loc[temp_index,'code'])>1 :
        #         temp_code = Account_Stocks.loc[temp_index,'code']
        #         [temp_factor, dif_Cash ]= self.Check_div_Cash_Shares( temp_code, temp_Date, temp_LastDay )
        #         # todo Now we need to change number, costAve,lastPrice in AS, and cash in A_Sum
        #         # Num	AveCost	LastPrice	TotalCost	MV	PnL	PnL_Pct	W_Real	W_Ideal	Date	code
        #         Account_Stocks.loc[temp_index, 'Num'] = Account_Stocks.loc[temp_index,'Num'] * temp_factor
        #         Account_Stocks.loc[temp_index, 'AveCost'] = Account_Stocks.loc[temp_index, 'AveCost'] / temp_factor
        #         # Account_Stocks.loc[temp_index, 'LastPrice'] = Account_Stocks.loc[temp_index, 'LastPrice'] / temp_factor
        #         # todo 这里假设分红分到 现金账户里了。
        #         # 0,Total_Cost,Cash,Stock,Total,Unit,MDD,Index,MDD_I
        #         Account_Sum.loc[ temp_Date, 'Cash'] = Account_Sum.loc[ temp_Date, 'Cash'] + max(0,dif_Cash  )
        #         Account_Sum.loc[temp_Date, 'Total'] = Account_Sum.loc[temp_Date, 'Total'] + max(0, dif_Cash )

        ''' Part 2 交易日回测 Back Testing'''
        Len_time = len(Dates)
        Index_Sum = Dates
        # todo para : rate_Fees 初始化其他参数
        weight_Method = 'equalWeight_ini'
        rate_Fees = 0.0025
        temp_TB_Index = 0
        Path_Data = path_Input
        Initial = Account_Sum['Total'].iloc[0]
        # ['Signal', 'temp_Ana', 'Order', 'Symbol', 'Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        Columns_Trade = ['Account', 'Date', 'Symbol', 'Buy/Sell', 'Price', 'Number', 'AveCost', 'Fees', 'ProfitReal']
        columns_trade_NextDay =  Columns_Trade # todo  Trading_Book.columns
        temp_Sig_Ana_Next = pd.DataFrame(np.zeros([len(symbolList), 4]),  columns=['Signal', 'temp_Ana', 'Order', 'Symbol'])
        trade_NextDay = pd.DataFrame(columns=columns_trade_NextDay)

        import math
        ''' Part 2 交易日回测 Back Testing'''
        ''' Part 3 模拟实盘Live  '''
        # P2 Dynamic Portfolio Adjustment
        # print('170418 1318 Dates ', Dates )

        for i in range(1, Len_time):
            ''' Part 2 Step 1 初始化回测Back Testing：当前日期， 资金，持仓，总值   Initialize Account_Sum, Update cash level '''
            # todo Date and LastDay 获取当前日期
            temp_Date = Dates[i]  # ex. temp_Date = "20160104"
            temp_LastDay = Dates[i - 1]

            # todo 170416 注意：Live情况下，需要每日读取 TB！~
            # 171121 如果发生更改 Account_Sum 或者 Account_Stocks 中的数据，那么日期格式有可能会被excel改掉，正确格式 2017-11-21，错误格式2017\11\21
            # Account_Sum： Columns = ['Total_Cost', 'Cash', 'Stock', 'Total', 'Unit', 'MDD', 'Index', 'MDD_I']
            # print('171121 i Dates ' ,i ,  Dates[i - 1]  ,   Account_Sum.tail(5) )
            Account_Sum.loc[temp_Date, 'Total_Cost'] = Account_Sum.loc[Dates[i - 1], 'Total_Cost']
            Account_Sum.loc[temp_Date, 'Cash'] = Account_Sum.loc[Dates[i - 1], 'Cash'] * (1 + 0.02 / 365)
            Account_Sum.loc[temp_Date, 'Stock'] = Account_Sum.loc[Dates[i - 1], 'Stock']
            Account_Sum.loc[temp_Date, 'Total'] = Account_Sum.loc[temp_Date, 'Cash'] + Account_Sum.loc[
                temp_Date, 'Stock']
            Account_Sum.loc[temp_Date, 'Unit'] = Account_Sum.loc[temp_Date, 'Total'] / Initial
            Account_Sum.loc[temp_Date, 'MDD_I'] = Account_Sum.loc[Dates[i - 1], 'MDD_I']

            ''' Part 2 Step 2 根据券商交易记录，更新持仓股票   Portfolio Adjustment  '''
            # Input：indexName, Account_Stocks，Path_Data, temp_LastDay
            # Output0：temp_Symbol_I, temp_Symbol, , temp_Date_Index
            # Output1: temp_Signal, temp_Weight
            # todo 实盘情况下，每个交易日需要导入 实盘交易记录
            # read_csv TradeBook_Last_170406  from  D:\data_Output\Sys_rC1703_1704\Port_Live\data_Sum
            TB = 'Trading_Book_' + temp_LastDay  +'.csv'
            Trading_Book = pd.read_csv( Path_TradeSys + '\\' + TB , encoding='gbk')
            # print('Trading_Book', Trading_Book.columns )
            for temp_col in Trading_Book.columns :
                if temp_col[:7] == 'Unnamed' :
                    Trading_Book= Trading_Book.drop( temp_col , axis=1)
            # print('Trading_Book', Trading_Book.columns )

            # todo temp_Date = 2017-04-05
            TB_Last  = 'TradeBook_Last_' + temp_Date[2:4] + temp_Date[5:7] + temp_Date[8:10] +'.csv'
            # print('TB_Last666 ', path_data_Sum + '\\' + TB_Last )
            TradeBook_Last = pd.read_csv( path_data_Sum + '\\' + TB_Last, encoding='gbk'  )

            # print('TradeBook_Last 666 ', TradeBook_Last )
            Trading_Book_1Day = self.Tran_Live2Standard(temp_Date, TradeBook_Last, Fees)
            print('Trading_Book_1Day',Trading_Book_1Day  )

            # todo 170416 这里可能不能直接append，因为columns的顺序可能不对？
            Trading_Book = Trading_Book.append( Trading_Book_1Day,ignore_index=True)
            print('Trading_Book has been import from broker data',Trading_Book.tail(5)  )
            cash_GC = 0

            # todo 总量控制，基准指数买入或持有趋势时（1），可以BSH，卖出后空仓趋势时（0，-1），只能Sell，
            temp_Symbol_I = indexName  # '000300.SH'
            # 171130 from temp_LastDay to temp_Date
            (temp_Signal_Index, temp_Date_Index_Index,data_1day ,data_ALLdays) = self.Get_Signal(temp_Symbol_I, Path_Data, temp_Date)

            # todo 流程分析 1，根据Trading_Book 记录，逐笔更新持仓，现金，以及StockPool，
            ''' Part  持仓股的买卖交易 '''
            # todo 170623 2327  Qs ：今日持仓有2个买入，2个卖出，在交易数据表里顺序是先2个买入记录，再2个卖出记录
            # todo 由于没有先计算要卖出的股票，导致买入第一个股票后，Account_Stocks表满了，不能再买入股票
            # todo Ana：应该叫交易单分成 卖出部分和买入部分，先卖出，再买入。
            Trading_Book_1Day_Sell = Trading_Book_1Day[ Trading_Book_1Day['Buy/Sell'].isin([-1]) ]
            Trading_Book_1Day_Buy = Trading_Book_1Day[Trading_Book_1Day['Buy/Sell'].isin([1])]
            for temp_index in Trading_Book_1Day_Sell.index :
                # todo 170416 temp_Trade 应该是 pd 而不是np，temp_Trade = Trading_Book.loc[temp_index, :] 会导致 np或格式不对
                temp_Trade = pd.DataFrame( columns =Trading_Book_1Day_Sell.columns )
                temp_Trade.loc[0, :] = Trading_Book_1Day_Sell.loc[temp_index, :]
                temp_Symbol = Trading_Book_1Day_Sell.loc[temp_index, 'Symbol']
                print('170416 1109 temp_Symbol', temp_Symbol )
                if not temp_Symbol[:2] == '20' :
                    # 获取当日市场价格信息 todo  Path_Data = path_Input_Wind
                    (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_Date, path_Input )
                    # todo 17010 0150 注意：港股组合的情况下，有可能 data_1day＝0　值和 int type ！这说明港股在这一天没有交易
                    # todo idea：如果各个变量的格式都符合模拟实盘的标准，那么下边这个应该是能用的。
                    print('170503 temp_trade ', temp_Trade )
                    # print('Account_Sum now 1 ', temp_Date, Account_Sum.loc[temp_Date, :])
                    # print('333333-1')
                    # print( Account_Stocks )
                    [Account_Sum, Account_Stocks, StockPool] = self.Update_Account_List_Trade( temp_Trade, rate_Fees,  data_1day ,Account_Stocks, Account_Sum ,StockPool ,i, Index_Sum)
                    # print('Account_Sum now 1 ', temp_Date, Account_Sum.loc[temp_Date, :])
                    # todo Ana： Index_Sum and i 主要对应的是 Account_Sum 里的 index，因为要用来定位档期日期，前一日期和计算累计最大回撤等数据。
                    # print('333333-2')
                    # print(Account_Stocks)
                else :
                    cash_GC = float(temp_Trade.loc[0, 'Number'])*100
            for temp_index in Trading_Book_1Day_Buy.index:
                # todo 170416 temp_Trade 应该是 pd 而不是np，temp_Trade = Trading_Book.loc[temp_index, :] 会导致 np或格式不对
                temp_Trade = pd.DataFrame(columns=Trading_Book_1Day_Buy.columns)
                temp_Trade.loc[0, :] = Trading_Book_1Day_Buy.loc[temp_index, :]
                temp_Symbol = Trading_Book_1Day_Buy.loc[temp_index, 'Symbol']
                print('170416 1109 temp_Symbol', temp_Symbol)
                if not temp_Symbol[:2] == '20':
                    # 获取当日市场价格信息 todo  Path_Data = path_Input_Wind
                    (data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_Date, path_Input)
                    # todo 17010 0150 注意：港股组合的情况下，有可能 data_1day＝0　值和 int type ！这说明港股在这一天没有交易
                    # todo idea：如果各个变量的格式都符合模拟实盘的标准，那么下边这个应该是能用的。

                    [Account_Sum, Account_Stocks, StockPool] = self.Update_Account_List_Trade(temp_Trade, rate_Fees,
                                                                                              data_1day, Account_Stocks,
                                                                                              Account_Sum, StockPool, i,
                                                                                              Index_Sum)
                    # print('Account_Sum now 1 ', temp_Date, Account_Sum.loc[temp_Date, :])
                    # todo Ana： Index_Sum and i 主要对应的是 Account_Sum 里的 index，因为要用来定位档期日期，前一日期和计算累计最大回撤等数据。

                else:
                    cash_GC = float(temp_Trade.loc[0, 'Number']) * 100

            ''' Part 持仓股票的价格更新 ！！ missing... '''
            for temp_index in Account_Stocks.index :
                temp_Symbol = Account_Stocks.loc[ temp_index , 'code']
                # Get lastPrice at temp_Date and update lastPrice,MV,PnL,PnL_Pct	W_Real, and A_Sum
                print('temp_Symbol',type(temp_Symbol), temp_Symbol)
                # temp_Symbol = 'nan' at 2017-03-31
                if type(temp_Symbol)== str and temp_Symbol != '0'  :
                    # int 0 或者 float 0 时，都不需要更新持仓信息
                    ( data_1day, data_ALLdays, temp_Date_Index) = self.Get_Price_Ret(temp_Symbol, temp_Date, Path_Data)
                    print('data_1day  ', data_1day )
                    print('Account_Sum now 1 ' , temp_Date, Account_Sum.loc[temp_Date, :])
                    [Account_Sum, Account_Stocks, ] = self.Update_Account_List_NoTrade(data_1day, Account_Stocks,  Account_Sum, i, temp_Symbol,   temp_Date, Dates)
                    print('Account_Sum now 1 ', temp_Date, Account_Sum.loc[temp_Date, :])

            print('Account_Sum now', temp_Date, Account_Sum.loc[temp_Date, :])

            ''' Part 2 Step 3 股票池StockPool里 非持仓股票的  Portfolio Adjustment  '''
            # todo 170414 0930
            # todo P2 S3.1 计算最大可买入股票数量 = 最大持有股票数量 - 当前持有股票数量 , 以下只有买入行为，无卖出或加仓
            # temp_Signal_List = np.zeros([1,len( symbolList) ])
            # temp_Num_dif is number of stocks we can fill our account
            temp = sum(Account_Stocks['Num'] > 0)
            temp_Num_dif = MaxN - temp
            # todo number of stocks is smaller than MaxN case :
            # if temp_Num_dif > 0:
            # todo P2 S3.2 Generate a list of all non-holding stocks with [BuySignal, P_H40 level]
            # add the following at 170703 1843 since there are error record in StockPool as :
            # "Error record in StockPool.       NaN   NaN     NaN     NaN       NaN  "
            # StockPool = StockPool.dropna()

            [temp_Sig_Ana, temp_Sort] = self.Get_Sig_Ana(symbolList, Path_Data, temp_Date, temp_LastDay, StockPool)

                # All stocks calculation of current day done

            ''' Part 3 回测Back Testing: Data Ana, Statistics, I/O and Visualization  '''
            [TB_NoUse, cash_Broker, MV_Broker] = self.Tran_Portfolio_Data_Live(temp_LastDay, temp_Date,
                                                                               path_data_Sum, Path_TradeSys, Fees)
            Account_Sum.loc[temp_Date, 'Cash'] = cash_Broker + cash_GC
            Account_Sum.loc[temp_Date, 'Total'] = Account_Sum.loc[temp_Date, 'Stock'] + cash_Broker + cash_GC
            print('170426 1818 Cash cash_Broker + cash_GC', Account_Sum.loc[temp_Date, 'Total'])
            temp_Unit = Account_Sum['Total'].iloc[0]
            Account_Sum.loc[temp_Date, 'Unit'] = Account_Sum.loc[temp_Date, 'Total']/ temp_Unit

            temp_MDD = Account_Sum.loc[temp_Date, 'Total'] / max(Account_Sum.loc[Dates[:i], 'Total']) - 1
            # print('170313 1019 ---temp_MDD ',temp_MDD )
            Account_Sum.loc[temp_Date, 'MDD'] = min(Account_Sum.loc[Dates[i - 1], 'MDD'], temp_MDD)
            # print('170313 1019 ---temp_MDD ', Account_Sum.loc[Dates[i-1],'MDD'] )

            # todo 170317 下边 Unit 从 上边 'Unit now' 处的 1.03 变成了 0.15
            print('Unit now', temp_Date)
            print(Account_Sum.loc[temp_Date, 'Unit'])

            # todo P3 Save to Database and to backup csv files交易日文件存档
            # todo Trading_Book, index_Trading_Book  ,Stock_MV, Account_Sum, StockList, StockPool


            Account_Sum.to_csv( Path_TradeSys + '\\' + 'Account_Sum_' + temp_Date + '.csv')
            Account_Stocks.to_csv(Path_TradeSys + '\\' + 'Account_Stocks_' + temp_Date + '.csv')
            # Account_Stocks2.to_csv(Path_TradeSys + '\\' + 'Account_Stocks2_'+ temp_Date + '.csv')
            StockPool.to_csv(Path_TradeSys + '\\' + 'StockPool_' + temp_Date + '.csv')
            Trading_Book.to_csv(Path_TradeSys + '\\' + 'Trading_Book_' + temp_Date + '.csv')
            # todo 170109 2305
            temp_Sig_Ana.to_csv(Path_TradeSys + '\\' + 'Sig_Ana' + temp_Date + '.csv')

            # todo  ==============================================================================================

            ''' Part 3 Step 1 计算下一个交易日交易计划/指令 pre_Orders　 '''
            ''' Part 3 Step 1 初始化模拟实盘Live：下一个交易日交易计划/指令 pre_Orders　 '''
            # todo VIP Part 3 部分所有和上边同名或者新增的变量，都需要加'_Next'后缀，以避免潜在数据问题

            # todo 输出形式　pre_Orders　列出所有有买入信号的股票并排序(sig_Ana\'order')，和卖出指令
            # todo 以防次日在前20-30个股票中遇到无法交易的股票
            #   由于每日的Orders之间是independent，所以每个交易日重新初始化即可
            trade_NextDay = pd.DataFrame(columns=columns_trade_NextDay)
            # todo ID-Event ：0331xx
            # todo Part 3 Step 1.1 获取 temp_Day 当日交易信号，价格等 |  ID-Event ：0331xx
            # todo 决定用 _next 作为所有相关变量的后缀

            ''' Part 3 Step 2 模拟实盘Live： 持仓股票的  Portfolio Adjustment  '''
            # Input：indexName, Account_Stocks，Path_Data, temp_LastDay
            # Output0：temp_Symbol_I, temp_Symbol, , temp_Date_Index
            # Output1: temp_Signal, temp_Weight
            # todo 总量控制，基准指数买入或持有趋势时（1），可以BSH，卖出后空仓趋势时（0，-1），只能Sell，
            temp_Symbol_I = indexName  # '000300.SH'
            # todo 170403 : signal_Index_Next for tomorrow

            (temp_Signal_Index_Next, temp_Date_Index_Index_Next) = self.Get_Signal_Next(temp_Symbol_I, Path_Data, temp_Date)
            print('170428 1311 temp_Signal_Index_Next = ', temp_Symbol_I, temp_Signal_Index_Next, temp_Date)

            # todo 为 Part 3 Step 3 准备的现金值
            temp_Cash = Account_Sum.loc[Dates[i], 'Cash']
            print('temp_Cash ', temp_Cash)

            for j1 in range(len(Account_Stocks['code'])):
                temp_Symbol = Account_Stocks['code'].iloc[j1]
                print('P3S2 temp_Symbol ', temp_Symbol)
                # print('j1', j1,temp_Symbol )
                # we want to avoid string type temp_Symbol=0.0 ,which has length 3
                if type(temp_Symbol) == str and len(temp_Symbol) > 3:
                    # print('Length of temp_Symbol(to avoid 0 value): ', len(temp_Symbol))
                    # todo Part 3 Step 2.1  Get_Signal ideal
                    # For future, if we want real-time signal, then we need to use lastest price to replace Last Day price
                    [temp_Signal_Next, temp_Date_Index_Next] = self.Get_Signal_Next(temp_Symbol, Path_Data, temp_Date)

                    # todo Part 3 Step 2.2 Get_BSH_Weight_Amount : Portfolio角度，获取 买卖信号，权重，金额
                    # Find weight, Check Account Get price, Get holding, Do accounting
                    # [temp_BSH_Next , temp_Weight_Next, temp_Amount_Next ] = self.Get_BSH_Weight_Amount(temp_Signal_Index_Next, temp_Signal_Next,
                    #                                                                   temp_Symbol, StockPool, MaxN,  Account_Sum, temp_Date, Leverage)
                    # print('temp_BSH_Next ,temp_Weight_Next ,temp_Amount_Next ',temp_BSH_Next , temp_Weight_Next, temp_Amount_Next)

                    # todo Part 3 Step 2.3  Buy/Sell/Hold order to trade :
                    # todo 170424 from temp_BSH_Next = temp_Signal_Next
                    # todo 170424 to temp_BSH_Next = min(temp_Signal_Index_Next,temp_Signal_Next)

                    # print('170428 1311 temp_Symbol temp_BSH_Next , ', temp_Symbol, temp_BSH_Next)
                    temp_BSH_Next = min(temp_Signal_Index_Next,temp_Signal_Next)
                    print('170428 1311 temp_Symbol temp_BSH_Next , ',temp_Symbol,temp_BSH_Next  )
                    # todo 买入信号：
                    # todo 170417 注意：持仓股本身就不要再放置买入单了，会占用空间。
                    if temp_BSH_Next == 1:
                        print('Do nothing if still buy signal for holding stock. ')
                        # # todo Part 3 Step 2.3.1 以temp_Day 收盘价close作为模拟成交价格，而不是明日的开盘价。
                        # (data_1day_Next, data_ALLdays_Next, temp_Date_Index_Next) = self.Get_Price_Ret_Next(temp_Symbol,
                        #                                                                                     temp_Date,
                        #                                                                                     Path_Data)
                        # # todo data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日
                        #
                        # # todo Part 3 Step 2.3.2 preTrade : Get_temp_Trade
                        # # todo 这里不需要 ifTrade  | 交易准备：获取 持仓信息，市场可交易数量 等
                        # # todo 有买入信号时，只有2种情况：1，买入；2，不买入，Notrade
                        # # todo 170401 目的：获得 temp_trade ,但现在是在 Trading_Book 里一起算
                        # # Input :Account_Sum,temp_Date, temp_Symbol, Account_Stocks,  rate_Fees, temp_BSH, temp_Amount, data_1day,  Leverage
                        # # Output :temp_Number, temp_TotalCost, temp_AveCost, temp_Fees, temp_ProfitReal
                        # temp_Amount =  Initial * (1 - Leverage) / MaxN
                        # # temp_Amount = min(temp_Cash, Initial * (1 - Leverage) / MaxN)
                        # temp_Trade_Next = self.Get_temp_Trade_Next(Account_Sum, temp_Date, temp_Symbol, Account_Stocks,
                        #                                            rate_Fees, temp_BSH_Next , temp_Amount, data_1day_Next,
                        #                                            Leverage)
                        #
                        # print('temp_Trade_Next ', temp_Trade_Next)
                        # # todo Part 3 Step 2.3.2 save temp_Trade_Next to  trade_NextDay
                        #
                        # trade_NextDay = trade_NextDay.append(temp_Trade_Next, ignore_index=True)
                        # print('trade_NextDay ', trade_NextDay)
                        # # todo update estimating cash level | cash in if trade_NextDay.loc[0,'Buy/Sell'] ==-1
                        # temp_Cash = temp_Cash - trade_NextDay.loc[0, 'Buy/Sell'] * trade_NextDay.loc[0, 'Price'] * \
                        #                         trade_NextDay.loc[0, 'Number']

                    # todo 卖出信号
                    elif temp_BSH_Next == -1:
                        (data_1day_Next, data_ALLdays_Next, temp_Date_Index_Next) = self.Get_Price_Ret_Next(temp_Symbol,
                                                                                                            temp_Date,
                                                                                                            Path_Data)
                        # todo data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日
                        # todo Part 3 Step 2.3.2 preTrade : Get_temp_Trade
                        temp_Amount = Initial * (1 - Leverage) / MaxN
                        temp_Trade_Next = self.Get_temp_Trade_Next(Account_Sum, temp_Date, temp_Symbol, Account_Stocks,
                                                                   rate_Fees, temp_BSH_Next , temp_Amount, data_1day_Next,
                                                                   Leverage)
                        print('temp_BSH_Next == -1temp_Trade_Next ', temp_Trade_Next)
                        # todo Part 3 Step 2.3.2 save temp_Trade_Next to  trade_NextDay
                        trade_NextDay = trade_NextDay.append(temp_Trade_Next, ignore_index=True)
                        print('temp_BSH_Next == -1 trade_NextDay ', trade_NextDay)

                        # todo update estimating cash level | cash in if trade_NextDay.loc[0,'Buy/Sell'] ==-1
                        temp_Cash = temp_Cash - trade_NextDay.loc[0, 'Buy/Sell'] * trade_NextDay.loc[0, 'Price'] * \
                                                trade_NextDay.loc[0, 'Number']

                        # todo 无卖出信号时，无交易

            ''' Part 3 Step 3 模拟实盘Live： 非持仓股票的  Portfolio Adjustment  '''
            # 原来是计算尽量少的股票交易，现在从不确定性的角度，应该计算最多 MaxN+5只股票的信号。
            # todo P3 S3.1 估计trade_NextDay 交易后账户可用资金，最多可买股票数量
            temp_MaxN = round(MaxN * temp_Cash / Initial) + 1
            print('temp_MaxN ', temp_MaxN)
            # todo number of stocks under estimating cash level
            # todo 170428 需要计算明天的指数信号！！


            if temp_MaxN > 0:
                # todo P3 S3.2 Generate a list of all non-holding stocks with [BuySignal, P_H40 level]
                [temp_Sig_Ana_Next, temp_Sort_Next] = self.Get_Sig_Ana_Next(symbolList, Path_Data, temp_Date, StockPool)
                print('temp_Sig_Ana_Next ', temp_Sig_Ana_Next)

                # todo P3 S3.3 有买入信号的股票 | Buy order to trade :
                temp_Num = int(sum(temp_Sig_Ana_Next['Signal'].values))

                # todo 下边的 5 是个估计数，未来可能根据实际情况再做调整。
                temp_N = MaxN +1   #  int(min(temp_MaxN, MaxN) + 5)
                # print('temp_Num  temp_N ',temp_Num , temp_N)

                for j3 in range(min(temp_N, temp_Num)):
                    # todo 计算最少数量的股票
                    temp_Symbol_Index = temp_Sig_Ana_Next.loc[temp_Sort_Next[j3], 'Order']
                    # print('170428 1311 temp_Symbol_Index = ', temp_Symbol_Index  )
                    # print('type( temp_Symbol_Index) ',  type( temp_Symbol_Index)  )
                    # todo 获取 symbolList里的股票代码
                    temp_Symbol = symbolList[int(temp_Symbol_Index)]
                    print('P3S3 temp_Symbol ', temp_Symbol)

                    # todo Ana 既然已经在 Get_Sig_Ana中算过Signal，这里就不要再算一次，节省时间提高效率
                    temp_Signal_Next = temp_Sig_Ana.loc[temp_Sort[j3], 'Signal']

                    if temp_Signal_Index_Next < 1:
                        temp_Signal_Next = -1

                    # todo 170404 Account_Sum 未更新NextDay的持仓股调整，因此 Get_BSH_Weight_Amount 没有用

                    temp_BSH_Next = min(temp_Signal_Index_Next, temp_Signal_Next)
                    print('170424-1345 temp_Symbol temp_BSH_Next , ', temp_Symbol, temp_BSH_Next)
                    # temp_BSH_Next = temp_Signal_Next
                    if temp_BSH_Next == 1:
                        # todo 买入信号：
                        # todo Part 3 Step 2.3.1 以temp_Day 收盘价close作为模拟成交价格，而不是明日的开盘价。
                        (data_1day_Next, data_ALLdays_Next, temp_Date_Index_Next) = self.Get_Price_Ret_Next(temp_Symbol,
                                                                                                            temp_Date,
                                                                                                            Path_Data)
                        # todo data_1day 对应的是 N 日，data_ALLdays 对应的是 0：N日，包括N 日

                        # todo Part 3 Step 2.3.2 preTrade : Get_temp_Trade
                        # todo 这里不需要 ifTrade  | 交易准备：获取 持仓信息，市场可交易数量 等
                        # todo 有买入信号时，只有2种情况：1，买入；2，不买入，Notrade
                        # todo 170401 目的：获得 temp_trade ,但现在是在 Trading_Book 里一起算
                        temp_Amount = Initial * (1 - Leverage) / MaxN
                        temp_Trade_Next = self.Get_temp_Trade_Next(Account_Sum, temp_Date, temp_Symbol, Account_Stocks,
                                                                   rate_Fees, temp_BSH_Next , temp_Amount, data_1day_Next,
                                                                   Leverage)

                        print('temp_Trade_Next ', temp_Trade_Next)
                        # todo Part 3 Step 2.3.2 save temp_Trade_Next to  trade_NextDay
                        trade_NextDay = trade_NextDay.append(temp_Trade_Next, ignore_index=True)
                        print('trade_NextDay ', trade_NextDay)
                        # todo update estimating cash level | cash in if trade_NextDay.loc[0,'Buy/Sell'] ==-1
                        # temp_Cash = temp_Cash - trade_NextDay.loc[0, 'Buy/Sell'] * trade_NextDay.loc[0, 'Price'] * \
                        #                         trade_NextDay.loc[0, 'Number']
            ''' Part 3 Step 4  模拟实盘Live Output result   '''
            # todo Output Sig_Ana_Next for Next Day
            temp_Sig_Ana_Next.to_csv(Path_TradeSys + '\\' + 'Sig_Ana_Next_' + temp_Date + '.csv')
            # todo Output Trade for Next Day
            trade_NextDay.to_csv(Path_TradeSys + '\\' + 'trade_NextDay_' + temp_Date + '.csv')

            # todo 每日交易完成 =============================================================================
        # todo 所有交易日更新后，再次输出次日交易相关数据
        # todo 170416 如果这里再次对 AS，A_Sum，TB，SP存档，会导致数据问题。
        # todo 因为唯一需要输出的只有 NextDay 交易相关文件，其他的不需要
        print('Account_Sum now 22 ', temp_Date, Account_Sum.loc[temp_Date, :])
        Account_Sum.to_csv(Path_TradeSys + '\\' + 'Account_Sum' + '.csv')
        Account_Stocks.to_csv(Path_TradeSys + '\\' + 'Account_Stocks' + '.csv')
        # Account_Stocks2.to_csv(Path_TradeSys + '\\' + 'Account_Stocks2' + '.csv')
        StockPool.to_csv(Path_TradeSys + '\\' + 'StockPool' + '.csv')
        Trading_Book.to_csv(Path_TradeSys + '\\' + 'Trading_Book' + '.csv')
        # todo Output Signal list
        temp_Sig_Ana.to_csv(Path_TradeSys + '\\' + 'Sig_Ana' + '.csv')

        # todo Output Sig_Ana_Next for Next Day
        temp_Sig_Ana_Next.to_csv(Path_TradeSys + '\\' + 'Sig_Ana_Next' + '.csv')
        # todo Output Trade for Next Day
        trade_NextDay.to_csv(Path_TradeSys + '\\' + 'trade_NextDay' + '.csv')

        # todo P4 Generate Analyzing profile, table and figure
        # ''' 其次，最需要加仓/dif_weight= StockPool['W_Ideal']- StockPool['W_Real']最大的股票 '''
        # ''' 再次，依次计算StockPool['B/S/H']=Buy的股票，'''
        # ''' 最后，如果持仓股还有空间，在剩余股票中寻找值得买入的股票'''
        # ''' 注意，设计一个机制，使得在市场较弱的情况下，和市场走势一致的股票总仓位比例较低'''

        print('File has been saved to :', Path_TradeSys)
        Portfolio_L = 0
        print('The Portfolio_L_Live has been Update..')

        # todo ========================================================================================================
        # todo 170412 ：这里是否对 Account_Sum里的现金做一个比较，如果加上近日交易净现金后和 cash_Broker差异较大，
        # todo 那要分析是否有 “申购/赎回”| 主要还是以 cash_Broker 为准
        # todo =========================================================================================================
        # todo 下边是原有版本的 rC_Portfolio_L_Live_Update
        # todo =========================================================================================================

        return Portfolio_L












