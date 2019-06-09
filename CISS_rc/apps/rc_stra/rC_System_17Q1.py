# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"
'''
===============================================
last update 170308 1020
Begin 170308 1020
Origined from : rC_Portfolio_17Q1.py
raw contents : test_N_Portfolios_17Q1  | test_N_port_temp.py
主要的功能： rC_Portfolio主要是实现对Portfolio级别的新建，计算，维护；
             rC_System主要是实现对多个Portfolios的新建，计算，维护

初始化：
循环：
    循环-日常维护：

Variablea：

程序说明部分:
    功能：历史回测：
    功能·实盘维护：
    输出结果
    分析：

===============================================
'''


class rC_System():
    # 类的初始化操作 , N is the maximum number of different stocks we can hold in account
    def __init__(self, Sys_name, str_ID ,path_Output ,date_LastUpdate ):
        self.Sys_name = Sys_name
        self.str_ID = str_ID
        self.path_Output = path_Output
        self.date_LastUpdate = date_LastUpdate

    def rC_System_Initialization(self, date_Start, date_LastUpdate, Sys_name, str_ID, portfolio_List, index_List , path_Output,MaxN_Sys,Leverage_Sys,Initial_Sys ,if_change_SP = 0 ):
        # last update :170403 1359 | start 170403 1359
        # todo 主要功能： 对一个System文件夹内的多个portfolios进行初始化操作
        # todo（单个portfolio用 rC_Portfolio_L_Update 实现update）
        # todo ================

        import os
        import pandas as pd
        import rC_Portfolio_17Q1 as rC_Port

        # from '170101' to '2017-01-01''
        end_Date = '20' + date_LastUpdate[:2] + '-' + date_LastUpdate[2:4] + '-' + date_LastUpdate[-2:]
        start_Date = '20' + date_Start[:2] + '-' + date_Start[2:4] + '-' + date_Start[-2:]

        temp_FileName = Sys_name + '_' + str_ID  # 留给Log file   date_Start + date_LastUpdate
        path_Sys = path_Output + '\\' + temp_FileName

        # todo 初始化 Log System
        # todo Part0 初始化 Sys : 新建 Log_TradeSys.csv文件, 要包括主要信息
        Log_Index = ['InitialDate', 'Sys_name', 'str_ID', 'path_Output', 'FileName', 'date_Start', 'date_LastUpdate',
                     'portfolio_List', 'index_List', 'MaxN_List', 'Leverage_List', 'Initial_List', 'path_Symbol',
                     'info']
        Log_Columns = ['value']
        Log_Sys = pd.DataFrame(index=Log_Index, columns=Log_Columns)
        # todo TradeSys 初始日期 InitialDate
        InitialDate_Sys = '170329'
        Log_Sys.loc['InitialDate', 'value'] = InitialDate_Sys
        Log_Sys.loc['Sys_name', 'value'] = Sys_name
        Log_Sys.loc['str_ID', 'value'] = str_ID
        Log_Sys.loc['path_Output', 'value'] = path_Output
        Log_Sys.loc['FileName', 'value'] = temp_FileName
        Log_Sys.loc['date_Start', 'value'] = date_Start
        Log_Sys.loc['date_LastUpdate', 'value'] = date_LastUpdate
        # todo create portfolio_List and save it as string

        # for i_Port in len(portfolio_List ) :
        #     temp_Portfolio = portfolio_List[ i_Port]
        #     temp_Index = index_List[i_Port]
        # portfolio_List = portfolio_List + ['symbol_List_170329']
        # index_List = index_List + ['000300.SH']

        # from list to string
        Log_Sys.loc['portfolio_List', 'value'] = str(portfolio_List)
        # from list to string
        Log_Sys.loc['index_List', 'value'] = str(index_List)
        # todo MaxN
        number_Port = len(portfolio_List)
        MaxN_List = [MaxN_Sys] * number_Port
        Log_Sys.loc['MaxN_List', 'value'] = str(MaxN_List)
        # todo Leverage
        Leverage_List = [Leverage_Sys] * number_Port
        Log_Sys.loc['Leverage_List', 'value'] = str(Leverage_List)
        # todo Initial capital
        Initial_List = [Initial_Sys] * number_Port
        Log_Sys.loc['Initial_List', 'value'] = str(Initial_List)
        # 代码的文件夹
        file_path1 = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Py3\\Symbols\\'

        Log_Sys.loc['path_Symbol', 'value'] = file_path1
        # 备注信息
        Log_Sys.loc['info', 'value'] = 'This is the log file of an investment system.'
        # todo Done, 输出 Log_Sys
        Log_Sys_Name = 'Log_' + Sys_name + '_' + str_ID + '.csv'
        Log_Sys.to_csv( path_Sys + '\\' + Log_Sys_Name)

        # todo 新建 Log_before 文件夹
        import os
        # todo Check  path_Sys 是否已经存在，如果不存在，则创建该文件夹。170824
        if not os.path.isdir((path_Sys+ '\\' + 'Log_before' )):
            os.mkdir(os.path.join( path_Sys ,  'Log_before'))

        # todo  初始化各个 Portfolio : 设置 Log_Portfolio.csv文件, 要包括组合的主要信息
        temp_portfolio_List = Log_Sys.loc['portfolio_List', 'value']
        temp_portfolio_List = temp_portfolio_List[2:-2]
        print('temp_portfolio_List')
        temp_Port_List =  temp_portfolio_List.split(sep="', '")
        print( temp_Port_List)

        Output = rC_Port.rC_Portfolio( [], '2015-01-01', '2017-01-01', 25)
        Log_Index = ['InitialDate', 'Index_Name', 'Portfolio_Name', 'MaxN', 'Leverage', 'date_Start',  'date_LastUpdate']
        Log_Columns = ['value']

        i_Index = 0
        for temp_Portfolio in portfolio_List:
            temp_Index = index_List[i_Index]
            Log_Portfolio = pd.DataFrame(index=Log_Index, columns=Log_Columns)
            # date_Start, date_LastUpdate
            Log_Portfolio.loc['InitialDate', 'value'] = date_Start
            # todo 组合基准指数
            # indexName = '000300.SH'
            Log_Portfolio.loc['Index_Name', 'value'] = temp_Index
            # todo 组合名称
            Log_Portfolio.loc['Portfolio_Name', 'value'] = temp_Portfolio
            # todo 最大持股数量
            MaxN = MaxN_List[i_Index]
            Log_Portfolio.loc['MaxN', 'value'] = MaxN_List[i_Index]
            # todo 最大杠杆倍数
            Leverage = Leverage_List[i_Index]
            Log_Portfolio.loc['Leverage', 'value'] = Leverage_List[i_Index]
            # todo 初始资本规模
            Initial = Initial_List[i_Index]
            Log_Portfolio.loc['Initial', 'value'] = Initial_List[i_Index]
            # todo 组合初始日期
            Log_Portfolio.loc['date_Start', 'value'] = date_Start
            # todo 组合最后更新日期
            Log_Portfolio.loc['date_LastUpdate', 'value'] = date_LastUpdate
            # todo 组合初始股票池地址
            # file_path1 = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Py3\\Symbols\\'
            f = temp_Portfolio + '.csv'
            file_path = file_path1 + f
            Log_Portfolio.loc['path_Symbol', 'value'] = file_path
            # todo ===================================================================
            # todo Get 组合初始股票池
            SymbolList_raw = pd.read_csv(file_path, header=None, sep=',')
            symbolList = SymbolList_raw[0][:]
            print('Import symbolList Done.')

            Output = rC_Port.rC_Portfolio(symbolList, date_Start, date_LastUpdate, MaxN)
            # todo Part 2 Get benchmark index data
            path_Input = 'D:\\data_Input_Wind'
            fileName_Date = path_Input + '\Wind_' + temp_Index + '.csv'
            data = pd.read_csv(fileName_Date, header=None, skiprows=1, sep=',')
            data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
            data2 = data.dropna(axis=0)

            # todo len_Days 用来确定 Dates长度
            # todo step 1 为了避免出现初始日期是非交易日，先做一个slicing
            # 选大于，一般数据量会小一些
            temp_List = data2[data2['date'] > start_Date]
            # todo step 2 Find the starting index in Data2 of index
            start_Date_Index = temp_List.index[0] - 1
            temp_List = data2[data2['date'] < end_Date]
            end_Date_Index = temp_List.index[-1] + 1
            # print('====== start_Date_Index ================================')
            # print(start_Date_Index)
            # print('====== end_Date_Index ==================================')
            # print(end_Date_Index)
            Date_List = data2.loc[start_Date_Index:end_Date_Index, 'date']
            # todo step 3 Find the len_Days
            len_Days = len(Date_List)
            # print('====== len_Days ========================================')
            # print(len_Days)
            # todo step 4  确定 Dates 中日期长度
            Dates = list(Date_List)
            print('====== Dates ===========================================')
            print('start_Date, end_Date, length of days ', Dates[0], Dates[-1], len(Dates))
            # todo 组合初始日期和投资系统初始日期一致 InitialDate_Port = InitialDate_Sys
            InitialDate_Port = InitialDate_Sys

            # todo Part 3 Run the Portfolio
            # todo 未完成 170303 ； path_Sys + "" + port_Name
            # todo: 组合名称， 170311 删除交易日长度
            port_dir = 'Port_' + temp_Portfolio + '_' + temp_Index + '_' + InitialDate_Port + '_' + str(
                MaxN) + '_' + str(Leverage)
            path_Portfolio = path_Sys + '\\' + port_dir

            # todo Check  Path_TradeSys  是否已经存在，如果不存在，则创建该文件夹。
            if not os.path.isdir((path_Portfolio)):
                os.mkdir(os.path.join(path_Sys, port_dir))

            # todo 170403 start_Date='170101',由于不是交易日，会出现问题
            start_Date = Dates[1]
            Portfolio_L = Output.rC_Portfolio_L(start_Date, temp_Index, symbolList, Dates, path_Input,
                                                path_Portfolio, Leverage, Initial, MaxN ,if_change_SP )


            print('The Portfolio' + temp_Portfolio + ' has done.  ')

            # todo 更新 Log_Portfolio
            # 要抓取的几个数据：Account_Sum ,Account_Stocks,TradeBook, StockPool
            # todo 1,Account_Sum | Total_Cost	Cash	Stock	Total	Unit	MDD | pnl pnl_pct, r_annual ?
            # import Account_Sum file
            temp_str = 'Account_Sum' + '.csv'

            # todo 2,Account_Stock | PnL	PnL_Pct	W_Real	code
            # todo 3,TradeBook | total_Profit, total_Loss, num_Trade_Profit, ave_Trade_Profit  ,num_Trade_Loss ,ave_Trade_Loss,
            # todo 4, StockPool| code_weight_max

            # todo 输出 Log_Portfolio
            # 备注信息
            Log_Portfolio.loc['info', 'value'] = 'This is the log file of an investment portfolio.'
            # todo Done, 输出 Log_Portfolio
            Log_Portfolio_Name = 'Log_' + temp_Portfolio + '.csv'
            Log_Portfolio = Output.Update_Log_Portfolio( Log_Portfolio, path_Portfolio)
            Log_Portfolio.to_csv(path_Sys + '\\' + Log_Portfolio_Name)
            i_Index = i_Index + 1

        result = 1
        return result

    def rC_System_Update(self, date_LastUpdate_New, Sys_name, str_ID, path_Output ,if_change_SP = 0 ):
        # last update :170308 1029 | start 170308 1029
        # todo 主要功能： 对一个System文件夹内的多个portfolios进行update操作（单个portfolio用 rC_Portfolio_L_Update 实现update）
        # todo 注意：在更新前，只要更改 Log_Portfolio 里的lastUpdate 日期就行了，不用去portfolio里删除错误更新的文件
        # todo ================
        # todo 根据Sys文件夹，读取Log文件，并依此读取Portfolio文件
        # todo ...... 最后要测试刚弄好的 Log_Portfolio = Output.Update_Log_Portfolio( Log_Portfolio, path_Portfolio )
        # todo ================

        # todo 170323 屏幕输出的内容保持到特定 txt文件
        # import sys
        # screenOutput = sys.stdout
        import time
        # temp_time1 = time.strftime('%y%m%d%H%M%S', time.localtime(time.time()))
        # temp_file_name = 'test_N_port_temp_' + temp_time + '.txt'
        # # todo 170323 屏幕输出的内容保持到特定 txt文件
        # path_Sys = path_Output + '\\' + Sys_name + '_' + str_ID+ '\\' + 'Log_before'
        # path_Sys1 = path_Output + '\\' + Sys_name + '_' + str_ID + '\\' + temp_file_name
        # # todo 初始化
        # temp_file = open(path_Sys + '\\' + temp_file_name, 'w')
        # sys.stdout = temp_file
        # todo =====================================


        import pandas as pd
        import rC_Portfolio_17Q1 as rC_Port

        temp_FileName = Sys_name + '_' + str_ID  # 留给Log file   date_Start + date_LastUpdate
        path_Sys = path_Output + '\\' + temp_FileName

        # todo 读取 Log_Sys
        Log_Sys_Name = 'Log_' + Sys_name + '_' + str_ID + '.csv'
        Log_Sys = pd.read_csv(path_Sys + '\\' + Log_Sys_Name)
        Log_Sys.index = Log_Sys[Log_Sys.columns[0]]
        Log_Sys = Log_Sys.drop(Log_Sys.columns[0], axis=1)

        temp_portfolio_List = Log_Sys.loc['portfolio_List', 'value']
        temp_portfolio_List = temp_portfolio_List[2:-2]
        print('temp_portfolio_List')
        print( temp_portfolio_List )
        temp_Port_List =  temp_portfolio_List.split(sep="', '")

        print('temp_Port_List 000')
        print( temp_Port_List)

        # todo Columns:  InitialDate Sys_name str_ID path_Output FileName date_Start date_LastUpdate
        # todo Columns:  portfolio_List index_List MaxN_List Leverage_List Initial_List path_Symbol info

        # todo 先专注于1个portfolio 读取特定组合
        # temp_dir_List = os.listdir( 'D:\\data_Output'+'\\'+  'Sys_' + 'rC1703' +'_' +'1703')
        # temp_dir_List = os.listdir(path_Output + '\\' + temp_FileName)
        # 上面方式的问题：对于'A_Med_befo1209'，由于也有'_',会出现 portfolio名字被拆分的情况
            #  未来 for temp_dir in temp_dir_List: if temp_dir[:4] == 'Port' : 进入Portfolio
            # temp_dir :   'Port_SZ50_000300.SH_170306_40_20_0.95'
            # #   感觉未来这一块容易出问题 temp_dir[:4] == 'Port' ，万一开头不是Port
            # if temp_dir[:4] == 'Port' :
            # temp_dir_info = temp_dir.split(sep='_')  # ['Port', 'SZ50', '000300.SH', '170306', '40', '20', '0.95']
        # todo 170308 ,for all portfolios ???? 170318
        # 170323 没问题 'SZ50','CSI800','chuang280' 'A_Consum_befo1209', 'A_Med_befo1209' 'CSI100n200' || [有问题:  ]

        Output = rC_Port.rC_Portfolio( [], '2015-01-01', '2017-01-01', 25)

        for temp_Port in temp_Port_List:

            [Log_Portfolio, start_Date ,indexName, symbolList, Dates, path_Input, Path_Port, Leverage, MaxN] = Output.Get_Portfolio_info(temp_Port,Log_Sys,path_Sys,date_LastUpdate_New    )
            # todo 170403 在最近一次update为 170327情况下，Dates范围是 ['2017-03-24', '2017-03-27', '2017-03-28', '2017-03-29', '2017-03-30', '2017-03-31']
            # todo 多了'2017-03-24', '2017-03-27' 2个交易日
            print('1704031242 start_Date Dates ',start_Date, Dates)
            # todo 170307 由于rC_Portfolio_L 会从0开始建立一个Portfolio，因此必须新建一个  rC_Portfolio_L_Update 模块
            # todo 171130 |             indexName = '000300.SH'
            Portfolio_L = Output.rC_Portfolio_L_Update(start_Date ,indexName, symbolList, Dates, path_Input, Path_Port, Leverage, MaxN )
            #                                           start_Date, indexName,symbolList, Dates, path_Input ,Path_TradeSys, Leverage=0.95, MaxN=30 ,if_change_SP = 0)
            # todo now we need to update Log_Portfolio
            Log_Portfolio.loc['date_LastUpdate', 'value'] = date_LastUpdate_New
            # todo 更新 最新净值，MDD等数据

            Log_Portfolio = Output.Update_Log_Portfolio( Log_Portfolio, Path_Port)
            # todo Update Portfolio Unit and MDD in Log_Sys
            temp_Unit = 'Unit_'  + Log_Portfolio.loc['Portfolio_Name', 'value']
            Log_Sys.loc[temp_Unit, 'value'] = Log_Portfolio.loc['Unit', 'value']
            temp_MDD =  'MDD_'  + Log_Portfolio.loc['Portfolio_Name', 'value']
            Log_Sys.loc[temp_MDD, 'value'] = Log_Portfolio.loc['MDD', 'value']

            temp_ProfitReal = 'ProfitReal_'  + Log_Portfolio.loc['Portfolio_Name', 'value']
            Log_Sys.loc[ temp_ProfitReal , 'value'] = Log_Portfolio.loc['total_ProfitReal', 'value']
            # total_ProfitReal
            temp_PnL = 'PnL_' + Log_Portfolio.loc['Portfolio_Name', 'value']
            Log_Sys.loc[ temp_PnL, 'value'] = Log_Portfolio.loc[ 'PnL' , 'value']

            Log_Portfolio.to_csv(path_Sys + '\\' + 'Log_' +temp_Port + '.csv')
            temp_time = time.strftime('%y%m%d%H%M', time.localtime(time.time()))
            print(temp_time)
            print('Log_Portfolio', temp_Port ,' has been update. ')


        # todo Update Log_Sys : 'date_LastUpdate'
        # todo Part 1 获取 Log_Portfolio
        Log_Sys.loc['date_LastUpdate', 'value'] = date_LastUpdate_New
        Log_Sys.to_csv( path_Sys + '\\' + Log_Sys_Name )
        Log_Sys.to_csv(path_Sys + '\\'+ 'Log_before'  + '\\' + 'Log_' + Sys_name + '_' + str_ID + '_' + date_LastUpdate_New + '.csv')
        print('Log_Sys ' , Log_Sys_Name , ' has been update. ')

        # todo 170323 屏幕输出的内容保持到特定 txt文件
        # sys.stdout = screenOutput
        # temp_file.close()
        # todo 170323 感觉没什么用，不知道是不是因为没有保存
        # todo ===============================================

        result = 1
        return result


    def rC_System_Live_Initialization(self,Sys_name, str_ID, InitialDate_Live,date_Start, date_LastUpdate, path_Sys,portfolio_List , temp_f_list,Initial_List, index_List, MaxN_List, Leverage_List, path_Symbol, info,if_change_SP = 0 ):
        '''
        # todo 用来初始化 实盘跟踪的投资组合
        # todo 注意：实盘Live体系的初始日日期可能和模拟盘的不一样。
        #
        # potential input : date_Start, date_LastUpdate, Sys_name, str_ID, portfolio_List, index_List , path_Output,MaxN_Sys,Leverage_Sys,Initial_Sys
        # derived from  rC_System_Initialization
        # last update :170403 1359 | start 170403 1359
        # todo 主要功能： 对一个System文件夹内的多个portfolios进行初始化操作
        # todo（单个portfolio用 rC_Portfolio_L_Update 实现update）
        数据：模拟盘以 System - Portfolio 为母子层级，实盘以 Account to Sys_Portfolio 为母子层级
        一个投资系统中可能有多个组合，（每个组合可能会有资产在不同的账户中——中短期不会有）
        一个实盘账户里会存在多个不同组合，各个组合可能从属于不同的 系统，有Quant_System , 也有Aeon_System
        因此，
        Port_Live\data_Raw 等文件夹是基于证券/券商账户，数据搜集后，再根据量化投资系统的账户属性，分门别类地存入
        各个Portfolio 里：
            例：某System有 alpha，beta两组合，各自有持仓在A 和 B 两个券商账户中。这样的话，就要先将A，B账户的导出数据
            存入 Port_Live中，再拆分，存入 Port_Live_alpha 中

        '''
        import pandas as pd

        ''' Part 0 新建Log 文件  Log_Sys， Log_Portfolio  '''
        import rC_Portfolio_17Q1 as rC_Port
        # todo 初始化 Log System
        # todo Part0 初始化 Sys : 新建 Log_TradeSys.csv文件, 要包括主要信息
        Log_Index = ['InitialDate', 'Sys_name', 'str_ID', 'path_Output', 'FileName', 'date_Start', 'date_LastUpdate',
                     'portfolio_List', 'index_List', 'MaxN_List', 'Leverage_List', 'Initial_List', 'path_Symbol',
                     'info']
        Log_Columns = ['value']
        Log_Sys = pd.DataFrame(index=Log_Index, columns=Log_Columns)
        # todo TradeSys 初始日期 InitialDate
        Log_Sys.loc['InitialDate', 'value'] = InitialDate_Live
        Log_Sys.loc['Sys_name', 'value'] = Sys_name
        Log_Sys.loc['str_ID', 'value'] = str_ID
        Log_Sys.loc['path_Output', 'value'] = path_Sys
        Log_Sys.loc['FileName', 'value'] = Sys_name + '_' + str_ID
        Log_Sys.loc['date_Start', 'value'] = date_Start
        Log_Sys.loc['date_LastUpdate', 'value'] = date_LastUpdate
        # todo create portfolio_List and save it as string
        # from list to string
        Log_Sys.loc['portfolio_List', 'value'] = str(portfolio_List)
        # from list to string
        Log_Sys.loc['index_List', 'value'] = str(index_List)
        # todo MaxN
        number_Port = len(portfolio_List)
        # MaxN_List = [MaxN_Sys] * number_Port
        Log_Sys.loc['MaxN_List', 'value'] = str(MaxN_List)
        # todo Leverage
        # Leverage_List = [Leverage_Sys] * number_Port
        Log_Sys.loc['Leverage_List', 'value'] = str(Leverage_List)
        # todo Initial capital
        # Initial_List = [Initial_Sys] * number_Port
        Log_Sys.loc['Initial_List', 'value'] = str(Initial_List)
        # 代码的文件夹
        # file_path1 = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Py3\\Symbols\\'
        Log_Sys.loc['path_Symbol', 'value'] = path_Symbol
        # 备注信息
        Log_Sys.loc['info', 'value'] = info
        # todo Done, 输出 Log_Sys_Live
        Log_Sys_Name = 'Log_' + Sys_name + '_' + str_ID + '_Live.csv'
        import os
        if not os.path.isdir( path_Sys ):
            os.mkdir( path_Sys )
        Log_Sys.to_csv( path_Sys + '\\' + Log_Sys_Name)

        # todo 新建 Log_before 文件夹
        temp_FileName = Sys_name + '_' + str_ID

        # todo Check  path_Sys 是否已经存在，如果不存在，则创建该文件夹。
        if not os.path.isdir((path_Sys + '\\' +  temp_FileName)):
            os.mkdir(os.path.join(path_Sys, temp_FileName))

        # todo  初始化各个 Portfolio : 设置 Log_Portfolio.csv文件, 要包括组合的主要信息
        # temp_portfolio_List = Log_Sys.loc['portfolio_List', 'value']
        # temp_portfolio_List = temp_portfolio_List[2:-2]
        # print('temp_portfolio_List')
        # temp_Port_List = temp_portfolio_List.split(sep="', '")
        # print(temp_Port_List)
        # date_Start, date_LastUpdate
        start_Date = '20' + date_Start[:2] + '-' + date_Start[2:4] + '-' + date_Start[-2:]
        end_Date = '20' + date_LastUpdate[:2] + '-' + date_LastUpdate[2:4] + '-' + date_LastUpdate[-2:]

        Output = rC_Port.rC_Portfolio([], '2015-01-01', '2017-01-01', 25)
        Log_Index = ['InitialDate', 'Index_Name', 'Portfolio_Name', 'MaxN', 'Leverage', 'date_Start',
                     'date_LastUpdate']
        Log_Columns = ['value']

        i_Index = 0
        for temp_Portfolio in portfolio_List:
            temp_Index = index_List[i_Index]
            Log_Portfolio = pd.DataFrame(index=Log_Index, columns=Log_Columns)
            # date_Start, date_LastUpdate
            Log_Portfolio.loc['InitialDate', 'value'] = date_Start
            # todo 组合基准指数
            # indexName = '000300.SH'
            Log_Portfolio.loc['Index_Name', 'value'] = temp_Index
            # todo 组合名称
            Log_Portfolio.loc['Portfolio_Name', 'value'] = temp_Portfolio
            # todo 最大持股数量
            MaxN = MaxN_List[i_Index]
            Log_Portfolio.loc['MaxN', 'value'] = MaxN_List[i_Index]
            # todo 最大杠杆倍数
            Leverage = Leverage_List[i_Index]
            Log_Portfolio.loc['Leverage', 'value'] = Leverage_List[i_Index]
            # todo 初始资本规模
            Initial = Initial_List[i_Index]
            Log_Portfolio.loc['Initial', 'value'] = Initial_List[i_Index]
            # todo 组合初始日期
            Log_Portfolio.loc['date_Start', 'value'] = date_Start
            # todo 组合最后更新日期
            Log_Portfolio.loc['date_LastUpdate', 'value'] = date_LastUpdate
            # todo 组合初始股票池地址
            file_path1 = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Py3\\Symbols\\'
            f = temp_Portfolio + '.csv'
            file_path = file_path1 + f
            Log_Portfolio.loc['path_Symbol', 'value'] = file_path
            # todo ===================================================================
            # todo Get 组合初始股票池
            SymbolList_raw = pd.read_csv(file_path, header=None, sep=',')
            symbolList = SymbolList_raw[0][:]
            print('Import symbolList Done.')

            # todo Part 2 Get benchmark index data
            path_Input = 'D:\\data_Input_Wind'
            fileName_Date = path_Input + '\Wind_' + temp_Index + '.csv'
            data = pd.read_csv(fileName_Date, header=None, skiprows=1, sep=',')
            data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
            data2 = data.dropna(axis=0)

            # todo len_Days 用来确定 Dates长度
            # todo step 1 为了避免出现初始日期是非交易日，先做一个slicing
            # 选大于，一般数据量会小一些
            temp_List = data2[data2['date'] > start_Date]
            # todo step 2 Find the starting index in Data2 of index
            start_Date_Index = temp_List.index[0] - 1
            temp_List = data2[data2['date'] < end_Date]
            end_Date_Index = temp_List.index[-1] + 1
            Date_List = data2.loc[start_Date_Index:end_Date_Index, 'date']
            # todo step 3 Find the len_Days
            len_Days = len(Date_List)
            # print('====== len_Days ========================================')
            # print(len_Days)
            # todo step 4  确定 Dates 中日期长度
            Dates = list(Date_List)
            print('====== Dates ===========================================')
            print('start_Date, end_Date, length of days ', Dates[0], Dates[-1], len(Dates))
            # todo 组合初始日期和投资系统初始日期一致 InitialDate_Port = InitialDate_Sys
            InitialDate_Port = InitialDate_Live

            # todo Part 3 Run the Portfolio
            # todo 未完成 170303 ； path_Sys + "" + port_Name
            # todo: 组合名称， 170311 删除交易日长度
            port_dir = 'Port_Live_' + temp_Portfolio + '_' + temp_Index + '_' + InitialDate_Port + '_' + str(
                MaxN) + '_' + str(Leverage)
            path_Portfolio = path_Sys + '\\' + port_dir

            # todo Check  Path_TradeSys  是否已经存在，如果不存在，则创建该文件夹。
            if not os.path.isdir((path_Portfolio)):
                os.mkdir(os.path.join(path_Sys, port_dir))

            # todo 170403 start_Date='170101',由于不是交易日，会出现问题
            start_Date = Dates[1]
            print('170416 0936 ')
            print('path_Sys  ', path_Sys )
            # Path_TradeSys = 'D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329_000300.SH_170405_10_0.95'
            # path_Port_Live = 'D:\data_Output\Sys_rC1703_1704\Port_Live'
            Path_TradeSys = path_Portfolio
            path_Port_Live = path_Sys + '\\' + 'Port_Live'

            # path_Port_Live = path_Portfolio = path_Sys + '\\' + 'Port_Live'
            Portfolio_L = Output.rC_Portfolio_L_Live(start_Date, temp_Index, symbolList, Dates, path_Input,
                                                     Path_TradeSys ,path_Port_Live , Leverage, Initial, MaxN ,if_change_SP )
            print('The Portfolio Live ' + temp_Portfolio + ' has done.  ')

            # todo 更新 Log_Portfolio
            # 要抓取的几个数据：Account_Sum ,Account_Stocks,TradeBook, StockPool
            # todo 1,Account_Sum | Total_Cost	Cash	Stock	Total	Unit	MDD | pnl pnl_pct, r_annual ?
            # import Account_Sum file
            temp_str = 'Account_Sum' + '.csv'

            # todo 2,Account_Stock | PnL	PnL_Pct	W_Real	code
            # todo 3,TradeBook | total_Profit, total_Loss, num_Trade_Profit, ave_Trade_Profit  ,num_Trade_Loss ,ave_Trade_Loss,
            # todo 4, StockPool| code_weight_max

            # todo 输出 Log_Portfolio
            # 备注信息
            Log_Portfolio.loc['info', 'value'] = 'This is the log file of an live version1 investment portfolio.'
            # todo Done, 输出 Log_Portfolio
            Log_Portfolio_csv = 'Log_' + temp_Portfolio + '_Live.csv'
            Log_Portfolio = Output.Update_Log_Portfolio( Log_Portfolio, path_Portfolio )
            Log_Portfolio.to_csv(path_Sys + '\\' +Log_Portfolio_csv)
            i_Index = i_Index + 1


        ''' Part 1 新建数据文件夹 Port_Live\data_xxx '''
        # todo 新建 Log_before 文件夹
        import os
        # todo Check  path_Sys 是否已经存在，如果不存在，则创建该文件夹。
        if not os.path.isdir((path_Sys+ '\\' + 'Port_Live' )):
            os.mkdir(os.path.join( path_Sys , 'Port_Live' ))
        # todo 新建 Log_before\data_Raw 等文件夹
        path_Port_Live = path_Sys + '\\' + 'Port_Live'
        for temp_f in temp_f_list :
            # 新建 D:\data_Output\Sys_rC1703_1704\Port_Live\ 文件
            if not os.path.isdir(( path_Port_Live + '\\' + temp_f)):
                os.mkdir(os.path.join( path_Port_Live,temp_f))

        result = 1
        return result


    def rC_System_Live_Update(self,Sys_name, str_ID ,InitialDate_Live,date_Start, date_LastUpdate, path_Sys,portfolio_List , temp_f_list,Initial_List, index_List, MaxN_List, Leverage_List, path_Symbol, info,if_change_SP = 0 ):
        '''
        # todo 用来 更新 实盘跟踪的投资组合
        # todo 注意：实盘Live体系的初始日日期可能和模拟盘的不一样。
        #
        # derived from rC_System_Live_Initialization
        # last update :170416 1216| start 170416 1216

        '''

        import pandas as pd
        import os
        ''' Part 0 新建Log 文件  Log_Sys， Log_Portfolio  '''
        import rC_Portfolio_17Q1 as rC_Port
        # todo 导入 Log System ， 抓取主要信息
        Log_Sys_Name = 'Log_' + Sys_name + '_' + str_ID + '_Live.csv'
        Log_Sys = pd.read_csv( path_Sys + '\\' + Log_Sys_Name)

        # todo 初始化 Log System
        # todo Part0 初始化 Sys : 新建 Log_TradeSys.csv文件, 要包括主要信息
        Log_Index = ['InitialDate', 'Sys_name', 'str_ID', 'path_Output', 'FileName', 'date_Start', 'date_LastUpdate',
                     'portfolio_List', 'index_List', 'MaxN_List', 'Leverage_List', 'Initial_List', 'path_Symbol',
                     'info']
        Log_Columns = ['value']

        # todo 导入 Log Portfolio ， 抓取主要信息
        start_Date = '20' + date_Start[:2] + '-' + date_Start[2:4] + '-' + date_Start[-2:]
        end_Date = '20' + date_LastUpdate[:2] + '-' + date_LastUpdate[2:4] + '-' + date_LastUpdate[-2:]

        Output = rC_Port.rC_Portfolio([], '2015-01-01', '2017-01-01', 25)
        Log_Index = ['InitialDate', 'Index_Name', 'Portfolio_Name', 'MaxN', 'Leverage', 'date_Start',
                     'date_LastUpdate']
        Log_Columns = ['value']

        i_Index = 0
        for temp_Portfolio in portfolio_List:
            temp_Index = index_List[i_Index]
            #
            Log_Portfolio_csv = 'Log_' + temp_Portfolio + '_Live.csv'
            Log_Portfolio = pd.read_csv( path_Sys + '\\' + Log_Portfolio_csv)
            indexName = index_List[i_Index]
            MaxN = MaxN_List[i_Index]
            Leverage = Leverage_List[i_Index]
            Initial = Initial_List[i_Index]
            # todo 组合初始股票池地址
            file_path1 = 'C:\\zd_zxjtzq\\rCtrashes\\rC_Py3\\Symbols\\'
            f = temp_Portfolio + '.csv'
            file_path = file_path1 + f
            # todo ===================================================================
            # todo Get 组合初始股票池
            SymbolList_raw = pd.read_csv(file_path, header=None, sep=',')
            symbolList = SymbolList_raw[0][:]
            print('Import symbolList Done.')

            # todo Part 2 Get benchmark index data
            path_Input = 'D:\\data_Input_Wind'

            # fileName_Date = path_Input + '\Wind_' + temp_Index   + '_updated' + '.csv'
            # print( fileName_Date )
            # # todo os.path.isfile 判断文件是否存在，os.path.isdir 判断文件夹是否存在
            # # todo '_updated'  导出的csv是有index， 原版的是没有的
            # if not os.path.isfile(( fileName_Date )):
            #     print('1705070 2238 ')
            #     # 如果不存在  '_updated' ，则用普通状态的
            #     fileName_Date = path_Input + '\Wind_' + temp_Index + '.csv'
            #     data = pd.read_csv(fileName_Date, header=None, skiprows=1, sep=',')
            # else :
            #     # todo case  '_updated'
            #     data = pd.read_csv(fileName_Date, header=None, skiprows=1, sep=',')
            #     data.index = data[0]
            #     data= data.drop( [0], axis=1 )
            #     print(data.head(3))

            fileName_Date = path_Input + '\Wind_' + temp_Index + '_updated' + '.csv'
            data = pd.read_csv(fileName_Date, header=None, skiprows=1, sep=',')

            if len(data.columns) == 9:
                # we need to drop first columns
                data = data.drop([0], axis=1)

            data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
            data2 = data.dropna(axis=0)
            # print('170510 data2 ', data2.tail(6)  )
            # todo len_Days 用来确定 Dates长度
            # todo step 1 为了避免出现初始日期是非交易日，先做一个slicing
            # 选大于，一般数据量会小一些
            temp_List = data2[data2['date'] > start_Date]
            # todo step 2 Find the starting index in Data2 of index
            start_Date_Index = temp_List.index[0] - 1
            temp_List = data2[data2['date'] < end_Date]
            end_Date_Index = temp_List.index[-1] + 1
            Date_List = data2.loc[start_Date_Index:end_Date_Index, 'date']
            # todo step 3 Find the len_Days
            len_Days = len(Date_List)
            # print('====== len_Days ========================================')
            # print(len_Days)
            # todo step 4  确定 Dates 中日期长度
            Dates = list(Date_List)
            print('====== Dates ===========================================')
            print('start_Date, end_Date, length of days ', Dates[0], Dates[-1], len(Dates))
            # todo 组合初始日期和投资系统初始日期一致 InitialDate_Port = InitialDate_Sys
            InitialDate_Port = InitialDate_Live

            # todo Part 3 Run the Portfolio
            # todo 未完成 170303 ； path_Sys + "" + port_Name
            # todo: 组合名称， 170311 删除交易日长度
            port_dir = 'Port_Live_' + temp_Portfolio + '_' + temp_Index + '_' + InitialDate_Port + '_' + str(
                MaxN) + '_' + str(Leverage)
            path_Portfolio = path_Sys + '\\' + port_dir

            # todo Check  Path_TradeSys  是否已经存在，如果不存在，则创建该文件夹。
            if not os.path.isdir((path_Portfolio)):
                os.mkdir(os.path.join(path_Sys, port_dir))

            start_Date = Dates[1]
            # Path_TradeSys = 'D:\data_Output\Sys_rC1703_1704\Port_Live_symbol_List_170329_000300.SH_170405_10_0.95'
            # path_Port_Live = 'D:\data_Output\Sys_rC1703_1704\Port_Live'
            Path_TradeSys = path_Portfolio
            path_Port_Live = path_Sys + '\\' + 'Port_Live'
            # path_Port_Live = path_Portfolio = path_Sys + '\\' + 'Port_Live'
            # todo 170416 要 Update 组合了。
            Portfolio_L = Output.rC_Portfolio_L_Live_Update(  start_Date, indexName, symbolList, Dates, path_Input, Path_TradeSys,   path_Port_Live, Leverage , MaxN , Initial ,if_change_SP  )


            print('The Portfolio Live ' + temp_Portfolio + ' has done.  ')

            # todo 更新 Log_Portfolio
            # 要抓取的几个数据：Account_Sum ,Account_Stocks,TradeBook, StockPool
            # todo 1,Account_Sum | Total_Cost	Cash	Stock	Total	Unit	MDD | pnl pnl_pct, r_annual ?
            # import Account_Sum file
            temp_str = 'Account_Sum' + '.csv'

            # todo 2,Account_Stock | PnL	PnL_Pct	W_Real	code
            # todo 3,TradeBook | total_Profit, total_Loss, num_Trade_Profit, ave_Trade_Profit  ,num_Trade_Loss ,ave_Trade_Loss,
            # todo 4, StockPool| code_weight_max

            # todo 输出 Log_Portfolio

            # todo Done, 输出 Log_Portfolio
            Log_Portfolio_csv = 'Log_' + temp_Portfolio + '_Live.csv'
            Log_Portfolio = Output.Update_Log_Portfolio( Log_Portfolio, path_Portfolio )
            Log_Portfolio.to_csv(path_Sys + '\\' +Log_Portfolio_csv)
            i_Index = i_Index + 1


        ''' Part 1 新建数据文件夹 Port_Live\data_xxx '''
        # todo 新建 Log_before 文件夹
        import os
        # todo Check  path_Sys 是否已经存在，如果不存在，则创建该文件夹。
        if not os.path.isdir((path_Sys+ '\\' + 'Port_Live' )):
            os.mkdir(os.path.join( path_Sys , 'Port_Live' ))
        # todo 新建 Log_before\data_Raw 等文件夹
        path_Port_Live = path_Sys + '\\' + 'Port_Live'
        for temp_f in temp_f_list :
            # 新建 D:\data_Output\Sys_rC1703_1704\Port_Live\ 文件
            if not os.path.isdir(( path_Port_Live + '\\' + temp_f)):
                os.mkdir(os.path.join( path_Port_Live,temp_f))


        result = 1
        return result


































































