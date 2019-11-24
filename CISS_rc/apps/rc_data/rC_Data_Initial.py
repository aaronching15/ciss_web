# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
todo: replace rC_Data_Initial.py with get-Wind.py gradually. | 190718file_name

功能：初始化需要的股票价格，回报等数据
数据来源： Wind
last update 170509 | since  160121
/
class rC_Database( ) :
    def __init__(self, code ,start ,end ):
    def ImportFiData_csv(self,code='600030.SH' ):
        # Read csv daily price data from csv
    def Price2Return(self,code,data,toCSV=0 ):
        # Change price data to return data
    def GetDataInfo(self,data ):
        # Here we try to get first and last day about data
    def GetWindData(self, code='600036.SH', date_0='20151220', date_1='20160118', items='open,high,low,close,volume,pct_chg',output=1):
        # Get Daily data
/
这个文件主要用来获取数据， 调整数据结构，创建一个可以维护的建议数据库。
todu
1，Done : 把csv的价格数据变成涨跌幅度数据;输入日价格数据，输出涨跌幅度数据
3, 检查数据范围！假设原有数据截止到2015-12-31，我们要补上之后的数据，那么：
    3.1 重新下载数据到最新的日收盘价；3.2 如果当日交易还未结束，那么及时下载最新的数据，更新

===============================================
'''
import pandas as pd
import numpy as np

class rC_Database( ) :
    # 类的初始化操作
    def __init__(self, code  ):
        self.code=code
        # self.start=start
        # self.end=end


        ''' TODO： Build a class for account: Cash, Stocks, Futures,Options , etc.  '''

    ''' Part 1 Get Price data '''

    def ImportFiData_csv(self,code='600030.SH' ):

        # to tell isnan
        # Read csv daily price data from csv, which already downloaded from Wind by Matlab
        # C:\rC_Matlab_Output
        '''注意：这里文件地址命名需要用 \\, 全部\\可以识别，因为单个string里的\ 会被补成\\  '''
        file_path0 = 'C:\\rC_Matlab_Output'
        file_path=file_path0+ '\Day_' + code + '.csv'
        data = pd.read_csv( file_path, header=None, sep=',')
        # Lets change the name of the column
        data.columns = ['date' ,'open' ,'high', 'low' ,'close', 'volume','amt' ,'pct_chg']
        # data['close'][800:].plot()
        # ['date' ,'open' ,'high' ,'close', 'low', 'volume']
        print( 'data.shape is :', data.shape)

        data2=data.dropna( axis=0)
        [r,c]=data2.shape

        '''After data.dropna, data2的index代号还是没有变，原来从0开始，现在变成从594开始  '''
        # print( 'data2.shape is :', data2.shape)
        ''' data2=data.dropna( axis=0) 这一行代替了下面的整个for loop，看来关键还是应该依赖于官方的说明书！！ '''
        # for j in range(r) :
        #     i= r-1-j
        #     # We only need to find the last row i with nan , then delete the 0~i rows
        #     '''data.column[5] 和 data['volume'] 是一样的效果   '''
        #     print(data['volume'][i])
        #     # type(np.nan)=float
        #     if math.isnan( data['volume'][i]) :
        #         # delete the i-th row of data ;http://www.dcharm.com/?p=13
        #         # assign [i+1-th,end] rows to a new data
        #         data2=pd.dataf  ( data[i:] )
        #         j=r-1

        print( 'Data has been read from :  ' +file_path )
        return data2

    def Price2Return(self,code,data,toCSV=0 ):
    # Change price data to return data
    #　,toCSV=0　means we do not need to save result to a csv file
        [r,c]=data.shape
        data_value=data.values
        temp=np.zeros([r,c])
        temp[:,0]=data_value[:,0]
            # np.ones( [1,c] ) #  np.zeros( [1,c] )

        # data have : Columns=[  'date', 'open', 'high',  'low'  , 'close', 'volume']
        for i in range (1,r) :
            #　pass the 0-th row of data
            if data_value[i-1,4]>0 :
                temp[i,1] = data_value[i,1]/data_value[i-1,4]
                temp[i,2] = data_value[i,2]/data_value[i-1,4]
                temp[i,3] = data_value[i,3]/data_value[i-1,4]
                temp[i,4] = data_value[i,4]/data_value[i-1,4]
            else :
                temp[i,1] = 1
                temp[i,2] = 1
                temp[i,3] = 1
                temp[i,4] = 1
            if data_value[i-1,5]>0 :
                temp[i,5] = data_value[i,5]/data_value[i-1,5]
            else :
                temp[i,5] = 1
        Ret =temp
        if toCSV==1 :
            Columns=[  'date', 'open', 'high',  'low'  , 'close', 'volume','amt']
            file_path0='C:\\rC_Py3_Output\\'
            file_path=file_path0+'Price2Return_'+ code + '.csv'
            Ret2=pd.DataFrame( Ret ,columns=Columns)
            Ret2.to_csv(  file_path  , encoding='utf-8', index=False)
            # Ret2.tofile(  file_path  , sep="," )
            print( 'Data has been saved to:  ' +file_path )

        return Ret

    # todo  def Return2Price(self, data ,toCSV=0):
    # Change prcie data to return data

    def GetDataInfo(self,data ):
        # Here we try to get informaion about data

        data2=data.dropna( axis=0)
        [r,c]=data2.shape
        StartDate= data2.iloc[0,0]
        EndDate= data2.iloc[r-1 ,0]

        return [StartDate, EndDate]

        # # 获取数据最新的一天日期
        # import datetime
        # # Wind获取的Excel日期数据和Python下的数据呼唤方法：datetime.datetime.utcfromtimestamp( (data['date'].tail(1)-70*365-19)*86400  )
        # dateArray = datetime.datetime.utcfromtimestamp( (data['date'].tail(1)-70*365-19)*86400  )

    def GetWindData_min(self, code='600036.SH', date_0='2013-06-09 09:00:00', date_1='2016-05-09 09:00:00', items='open,high,low,close,volume,amt,pct_chg', output=1):
        # 160509 :w.wsi("600036.SH", "close", "2013-05-09 09:00:00", "2016-05-09 01:27:47", "BarSize=5;Fill=Previous;PriceAdj=F")
        # 181022 | w.wsi("600036.SH", "open,high,low,close,volume,amt,chg,pct_chg,oi", "2018-10-22 14:54:00", "2018-10-22 18:58:10", "Fill=Previous;PriceAdj=F")
        import WindPy as WP
        # Or: from WindPy import w
        WP.w.start()
        WindData = WP.w.wsi(code, items, date_0, date_1,  "BarSize=5;Fill=Previous;PriceAdj=F")

        return WindData

    def GetWindData(self, code='600036.SH', date_0='20151220', date_1='20160118', items='open,high,low,close,volume,amt,pct_chg',output=1):
        # items='open,high,low,close,volume,pct_chg'
        # Notice that pct_chg is with format as 2.45 , we need to divide it with 100
        # Get Daily data
        import WindPy as WP
        # Or: from WindPy import w
        WP.w.start()
        WindData= WP.w.wsd( code,items,date_0, date_1,'Priceadj=F')
        # if output==1 :
            # todo 要解决的主要问题是wind抓取的数据如何把数据按顺序存到csv文件里
            # print('Missing...')
            # file_path0 = 'C:\\rC_Matlab_Output'
            # file_path=file_path0+ '\Day_' + code + '.csv'
            # data.to_csv(  file_path  , encoding='utf-8', index=False)

        return WindData

    def GetWindData_NoAdj(self, code='600036.SH', date_0='20151220', date_1='20160118', items='open,high,low,close,volume,amt,pct_chg,total_shares',output=1):
        # items='open,high,low,close,volume,pct_chg,total_shares'
        # Notice that pct_chg is with format as 2.45 , we need to divide it with 100
        # Get Daily data
        import WindPy as WP
        # Or: from WindPy import w
        WP.w.start()
        # todo 170505 以前都是前复权 'Priceadj=F'
        # WindData= WP.w.wsd( code,items,date_0, date_1,'Priceadj=F')
        # todo 170505 以前都是不复权 ""
        WindData = WP.w.wsd(code, items, date_0, date_1, "")
        # if output==1 :
            # todo 要解决的主要问题是wind抓取的数据如何把数据按顺序存到csv文件里
            # print('Missing...')
            # file_path0 = 'C:\\rC_Matlab_Output'
            # file_path=file_path0+ '\Day_' + code + '.csv'
            # data.to_csv(  file_path  , encoding='utf-8', index=False)

        return WindData

    def Wind2Csv(self, WindData,file_path0,code  ):
        # todo 170426
        # todo 用来存放Wind历史数据的文件 file_path0=  D:\data_Input_Wind
        # We want to save Wind data( list ) into csv
        # Data type are all list : .Codes .Fields .Times .Data
        #   Columns=[  'date', 'open', 'high',  'low'  , 'close', 'volume'] ; file_path0='C:\\rC_Py3_Output\\'
        # file_path0='C:\\rC_Py3_Output\\'
        # type of WindData.Codes is list

        code=  WindData.Codes[0]

        import csv
        file_path=file_path0 +'Wind_'+ code + '.csv'
        file_path2 = file_path0 + 'Wind_' + code + '_updated' + '.csv'
        #  Python中的csv的writer，打开文件的时候，要小心， 要通过binary模式去打开，即带b的，比如wb，ab+等;
        # 而不能通过文本模式，即不带b的方式，w,w+,a+等，否则，会导致使用writerow写内容到csv中时，产生对于的CR，导致多余的空行。
        # open 这个功能会直接新建一个csv的文件，如果它不存在的话
        #  打开csv并写入内容时，避免出现空格，Python文档中有提到：open('eggs.csv', newline='')
        #  也就是说，打开文件的时候多指定一个参数
        #  open( file_path, 'w',newline='') 而不只是 open( file_path, 'w' )
        with open( file_path, 'w',newline='') as csvfile:
            # fieldnames = ['first_name', 'last_name'] ; Columns=[  'date', 'open', 'high',  'low'  , 'close', 'volume']
            fieldnames = WindData.Fields #  Data3.Fields=Columns ？
            # writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer = csv.writer(csvfile ) #　delimiter=' '
            # Write the first row as head
            writer.writerow(['DATE']+ fieldnames )
            len_item=len(WindData.Data) # = len(Columns) =6

            len_dates=len(WindData.Data[1]) # 253
            # print(WindData.Data[1])
            # print(WindData.Data[-1])
            # python中date、datetime、string的相互转换  http://my.oschina.net/u/1032854/blog/198179
            #  WindData3.Times[1].strftime('%Y-%m-%d') # '2016-01-05'
            # time.mktime( WindData3.Times[1].timetuple()) # datetime.datetime(2016, 1, 5, 0, 0, 0, 5000) to 1451923200.0
            for i in range(len_dates ) :
                temp_list=[ WindData.Times[i].strftime('%Y-%m-%d') ]# str  '20161215', we still need to change it to list
                # writer.writerow({ fieldnames[0] :WindData.Times[i] }) # date
                for j in range(len_item) : # without date here
                    temp_list.append( WindData.Data[j][i] )
                writer.writerow( temp_list ) # date

        with open( file_path2 , 'w',newline='') as csvfile:
            # fieldnames = ['first_name', 'last_name'] ; Columns=[  'date', 'open', 'high',  'low'  , 'close', 'volume']
            fieldnames = WindData.Fields #  Data3.Fields=Columns ？
            # writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer = csv.writer(csvfile ) #　delimiter=' '
            # Write the first row as head
            writer.writerow(['DATE']+ fieldnames )
            len_item=len(WindData.Data) # = len(Columns) =6

            len_dates=len(WindData.Data[1]) # 253
            # print(WindData.Data[1])
            # print(WindData.Data[-1])
            # python中date、datetime、string的相互转换  http://my.oschina.net/u/1032854/blog/198179
            #  WindData3.Times[1].strftime('%Y-%m-%d') # '2016-01-05'
            # time.mktime( WindData3.Times[1].timetuple()) # datetime.datetime(2016, 1, 5, 0, 0, 0, 5000) to 1451923200.0
            for i in range(len_dates ) :
                temp_list=[ WindData.Times[i].strftime('%Y-%m-%d') ]# str  '20161215', we still need to change it to list
                # writer.writerow({ fieldnames[0] :WindData.Times[i] }) # date
                for j in range(len_item) : # without date here
                    temp_list.append( WindData.Data[j][i] )
                writer.writerow( temp_list ) # date

        return file_path


    def Wind2Csv_pd(self, WindData,file_path0,code  ):
        # todo 170511
        # todo 用来存放Wind历史数据的文件 file_path0=  D:\data_Input_Wind
        import pandas as pd

        code=  WindData.Codes[0]
        # print('170511 1730 C')
        import csv
        file_path  =   file_path0 +'Wind_'+ code + '.csv'
        file_path2 = file_path0 + 'Wind_' + code + '_updated' + '.csv'

        temp_col = WindData.Fields
        temp_index = [ ]
        # todo time 的定义，只能一个一个来，不能直接整个list 变
        for temp_time in WindData.Times :
            temp_index = temp_index + [ temp_time.strftime('%Y-%m-%d')  ]

        temp_pd =  pd.DataFrame( WindData.Data )
        temp_pd = temp_pd.T
        temp_pd.columns = temp_col
        temp_pd.index = temp_index

        # print( temp_pd.tail(5) )
        temp_pd.to_csv( file_path  )
        temp_pd.to_csv( file_path2 )


        return file_path

    def Wind2Csv_NoAdj(self, WindData,file_path0,code  ):
        # todo 170426
        # todo 用来存放Wind历史数据的文件 file_path0=  D:\data_Input_Wind
        # We want to save Wind data( list ) into csv
        # Data type are all list : .Codes .Fields .Times .Data
        #   Columns=[  'date', 'open', 'high',  'low'  , 'close', 'volume'] ; file_path0='C:\\rC_Py3_Output\\'
        # file_path0='C:\\rC_Py3_Output\\'
        # type of WindData.Codes is list

        code=  WindData.Codes[0]

        import csv
        #  todo 170505
        file_path=file_path0+'Wind_'+ code + '_NoAdj' +'.csv'
        #  Python中的csv的writer，打开文件的时候，要小心， 要通过binary模式去打开，即带b的，比如wb，ab+等;
        # 而不能通过文本模式，即不带b的方式，w,w+,a+等，否则，会导致使用writerow写内容到csv中时，产生对于的CR，导致多余的空行。
        # open 这个功能会直接新建一个csv的文件，如果它不存在的话
        #  打开csv并写入内容时，避免出现空格，Python文档中有提到：open('eggs.csv', newline='')
        #  也就是说，打开文件的时候多指定一个参数
        #  open( file_path, 'w',newline='') 而不只是 open( file_path, 'w' )
        with open( file_path, 'w',newline='') as csvfile:
            # fieldnames = ['first_name', 'last_name'] ; Columns=[  'date', 'open', 'high',  'low'  , 'close', 'volume']
            fieldnames = WindData.Fields #  Data3.Fields=Columns ？
            # writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer = csv.writer(csvfile ) #　delimiter=' '
            # Write the first row as head
            writer.writerow(['DATE']+ fieldnames )
            len_item=len(WindData.Data) # = len(Columns) =6

            len_dates=len(WindData.Data[1]) # 253
            # print(WindData.Data[1])
            # print(WindData.Data[-1])
            # python中date、datetime、string的相互转换  http://my.oschina.net/u/1032854/blog/198179
            #  WindData3.Times[1].strftime('%Y-%m-%d') # '2016-01-05'
            # time.mktime( WindData3.Times[1].timetuple()) # datetime.datetime(2016, 1, 5, 0, 0, 0, 5000) to 1451923200.0
            for i in range(len_dates ) :
                temp_list=[ WindData.Times[i].strftime('%Y-%m-%d') ]# str  '20161215', we still need to change it to list
                # writer.writerow({ fieldnames[0] :WindData.Times[i] }) # date
                for j in range(len_item) : # without date here
                    temp_list.append( WindData.Data[j][i] )
                writer.writerow( temp_list ) # date

        return file_path

# ========================================================================================================
    def Get_temp_Date_Data(self, SL_path, temp_Date , Path_Data, temp_f ) :
        # todo 通过导入 代码清单，抓取当天交易日temp_Date所有代码的数据，并存入csv文件。
        # todo Input :
        # SymbolList ：代码清单 | from SL_path
        # temp_Date 交易日
        # file_path,
        # temp_f
        # todo Output
        # temp_pd_Date  当日股票数据
        # temp_pd_Date.csv saved to ( temp_Date , Path_Data, temp_f )
        # todo last update 170510 | since 170509

        import pandas as pd
        import WindPy as WP
        WP.w.start()

        items = "rt_date,rt_pre_close,rt_open,rt_high,rt_low,rt_last,rt_vol,rt_amt,rt_pct_chg,rt_mkt_cap,rt_float_mkt_cap"

        SymbolList = pd.read_csv( SL_path , skiprows=1, header=None, sep=',', encoding='gbk')
        # SymbolList = SymbolList.drop([0], axis=0)
        # todo columns '1' 对应的是 wind_code， '2' 对应的是 中文简称
        SymbolList = SymbolList["wind_code"]
        print('SymbolList.head', SymbolList.head(5))

        # todo errorCode_List4csv 是要搜集因为 _updated 导致csv数据出问题的代码
        errorCode_List4csv = []
        code_List = []
        # todo 170506 2251 WSQ 一次最多提取 4000个信息，也就是 11* 350,建议一次抓取300个股票
        temp_period = 300
        temp_len = round(len(SymbolList) / temp_period)
        if temp_len > len(SymbolList) / temp_period:
            temp_len = temp_len - 1
        print('temp_len', temp_len)
        temp_mode = len(SymbolList) % temp_period

        temp_pd_Date = pd.DataFrame()

        for j in range(temp_len + 1):
            # todo 把太长的 code_List 切分成几个sub_code_List
            # for j in range(temp_len, temp_len + 1):
            # print( 'aaaa',len(SymbolList) ,temp_period*(j),temp_period*(j+1)  )

            code_List = []
            if j == temp_len:
                for i in range(temp_period * (j), len(SymbolList)):
                    # code = str(SymbolList.values[i])[2:-2]
                    # todo columns '1' 对应的是 wind_code， '2' 对应的是 中文简称
                    # code = SymbolList.loc[i, 1]
                    code = SymbolList.loc[i, "wind_code"]
                    code_List = code_List + [code]
            else:
                for i in range(temp_period * (j), temp_period * (j + 1)):
                    # code = str(SymbolList.values[i])[2:-2]

                    # code = SymbolList.loc[i, 1]
                    code = SymbolList.loc[i, "wind_code"]
                    code_List = code_List + [code]
            print('code_List ', code_List)
            # todo Get 600036 quote data.
            # w.wsq("000001.SZ", "rt_date,rt_time,rt_pre_close,rt_open,rt_high,rt_low,rt_last,rt_last_amt,rt_last_vol,rt_vol,rt_amt,rt_pct_chg,rt_mkt_cap,rt_float_mkt_cap")
            temp_Data = WP.w.wsq(code_List, items)
            # print('temp_Data \n ', temp_Data)
            # .Codes = [600734.SH, 600735.SH, 600773.SH, 600807.SH, 600834.SH, 600855.SH, 600859.SH, 600887.SH, 600888.SH,
            #           600890.SH, ...]
            # .Fields = [RT_DATE, RT_PRE_CLOSE, RT_OPEN, RT_HIGH, RT_LOW, RT_LAST, RT_VOL, RT_AMT, RT_PCT_CHG, RT_MKT_CAP,
            #            ...]
            # .Times = [20170508 16:02: 22]
            # .Data = [[20170508.0, 20170508.0, 20170508.0, 20170508.0, 20170508.0, 20170508.0, 20170508.0, 20170508.0,
            #           20170508.0, 20
            # todo 170508
            temp_pd_set = pd.DataFrame(temp_Data.Data)
            temp_pd_set = temp_pd_set.T
            temp_pd_set.columns = temp_Data.Fields
            temp_pd_set.index = temp_Data.Codes

            temp_pd_Date = temp_pd_Date.append(temp_pd_set)  # todo 注意，这里不能 , ignore_index=1
            # temp_f[:-4] = 'all_A_Stocks_wind'
            temp_pd_Date.to_csv(Path_Data + 'Wind_' + temp_f[:-4] + '_' + temp_Date + '_updated' + '.csv')
            print(Path_Data + '\Wind_' + temp_f[:-4] + '_' + temp_Date + '_updated' + '.csv')
            # print( temp_pd.head(3) )
            #       RT_DATE  RT_PRE_CLOSE  RT_OPEN  RT_HIGH  RT_LOW  RT_LAST  \   RT_VOL        RT_AMT  RT_PCT_CHG    RT_MKT_CAP  \RT_FLOAT_MKT_CAP
            # 600734.SH  20170508.0         12.75     0.00     0.00    0.00     0.00         0.0  0.000000e+00      0.0000  7.949827e+09    4.476632e+09
            # 600735.SH  20170508.0         20.74     0.00     0.00    0.00     0.00
            # 600773.SH  20170508.0         12.32    12.35    12.44   11.49    11.55


        return temp_pd_Date

    def Update_WSQ_Get_errorCodes(self, temp_pd_Date ,temp_f, Path_Data, temp_Date,temp_LastDay ):
        # 加入对HK股票的支持 | 例如 170414,170417这2个交易日 0700.HK 没有 open，high，low，volume，amt数据，是因为这2天分别是港股的耶稣受难节 和复活节假期

        # function：读取 'temp_Symbol_updated'的数据，如果没有，则加入error_codes
        # Input  | temp_pd_Date , Path_Data, temp_Date
        # todo 增加 temp_LastDay， 为了避免 _updated.csv 中有5-8,5-10，但没有 5-9的情况，格式 '170509' to '2017-05-04'
        # temp_f, Path_Data, temp_Date 这三个是为了输出csv 用的
        # Output | errorCodes ,errorCodes.csv
        # last update 171209  | since 170510

        errorCodes = []
        i = 0
        for temp_code in temp_pd_Date.index:
            # todo 注意，如果当日停牌，temp_pd_Date 中的股票 open，high,low,close,vol,amt 都是0
            temp_pd = temp_pd_Date.loc[temp_code, :]
            # print('17121')
            # print( temp_pd )
            # todo 注意, RT_DATE 会返回 股票价格对应的日期和时间 20170505.0 对应17-5-5， 150006对应 15:00:06
            lastest_Date = temp_pd.loc['RT_DATE']  # 20170505.0
            temp_str = str(lastest_Date)
            lastest_Date_2 = temp_str[:4] + '-' + temp_str[4:6] + '-' + temp_str[6:8]
            preDate = '20' + temp_LastDay[:2] + '-' + temp_LastDay[2:4] + '-' + temp_LastDay[4:6]

            # todo 读取前一个交易日 data2
            # todo Part 2 Import csv file of 600036
            temp_Symbol = temp_code
            # todo 这里到底是导入 .csv  还是 _updated.csv ？感觉应该判断2个文件哪个的最新日期近

            file_path = Path_Data + 'Wind_' + temp_Symbol + '_updated' + '.csv'
            import os
            # print( file_path )
            if os.path.isfile( file_path ):
                try:
                    data = pd.read_csv(file_path, header=None, skiprows=1, sep=',')                
                
                    # print(data.tail(3))
                    # print('8888')
                    if len(data.columns) == 8:
                        data.columns = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'AMT', 'PCT_CHG']
                    elif len(data.columns) == 9:
                        data = data.drop([0], axis=1)
                        data.columns = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'AMT', 'PCT_CHG']
                    # 指数 399006.SZ 的columns 是 OPEN	HIGH	LOW	CLOSE	VOLUME	AMT	PCT_CHG

                    # print( data.tail(3) )
                    data2 = data.dropna(axis=0)
                    # print( data2.tail(3) )
                    # todo 测试 _updated 里是否是前一个交易日
                    # print('22222',data2['DATE'].iloc[-2] , preDate )

                    # todo Check type 1
                    # todo ====================================================================================
                    # todo Check type 2
                    # todo 170511 因为000300.SH 在 170511 只有收盘价，不知道为什么。
                    if len( data2['DATE'])>=2 and data2['DATE'].iloc[-1] == preDate:

                        # data2.iloc[-1]  | 返回最后一行
                        csv_LastDay = data2['DATE'].iloc[-1]  # '2017-05-04' 历史数据中最后一个交易日
                        # todo errorCodes 是要搜集因为 _updated 导致csv数据出问题的代码
                        if data2['CLOSE'].iloc[-1] == data2['VOLUME'].iloc[-1] :
                            # 如果都是 空值， NA
                            errorCodes = errorCodes + [temp_Symbol]

                        # todo Step 判断是否有成交
                        # print('temp_Symbol ', temp_Symbol)
                        # print('lastest_Date_2 from WInd-WSQ, ', lastest_Date_2)
                        # print('csv_LastDay from WInd-WSD-csv, ', csv_LastDay)
                        if temp_pd.loc['RT_AMT'] > 0:
                            # todo Step 判断 最新交易日期和 csv文件最新交易日期是否一致
                            if not csv_LastDay == lastest_Date_2:
                                # 不是同一个更新日期的情况：
                                # DATE,OPEN,HIGH,LOW,CLOSE,VOLUME,AMT,PCT_CHG
                                # todo Step 1 check pre close
                                # RT_PRE_CLOSE is the close price for latest trading day
                                RT_PRE_CLOSE = temp_pd.loc['RT_PRE_CLOSE']
                                # todo 注意，如果当日停牌，temp_pd_Date 中的股票 open，high,low,close,vol,amt 都是0
                                temp_dif = abs(RT_PRE_CLOSE - data2.loc[data2.index[-1], 'CLOSE']) / \
                                           max(RT_PRE_CLOSE, data2.loc[ data2.index[ -1], 'CLOSE'])

                                temp_index = data2.index[-1]
                                print('temp_dif ', temp_dif, 'temp_index ', temp_index)
                                if temp_dif < 0.01 :
                                    # print('170511 A ')
                                    data2.loc[temp_index + 1, 'DATE'] = lastest_Date_2
                                    data2.loc[temp_index + 1, 'OPEN'] = temp_pd.loc['RT_OPEN']
                                    data2.loc[temp_index + 1, 'HIGH'] = temp_pd.loc['RT_HIGH']
                                    data2.loc[temp_index + 1, 'LOW'] = temp_pd.loc['RT_LOW']
                                    data2.loc[temp_index + 1, 'CLOSE'] = temp_pd.loc['RT_LAST']
                                    data2.loc[temp_index + 1, 'VOLUME'] = temp_pd.loc['RT_VOL']
                                    data2.loc[temp_index + 1, 'AMT'] = temp_pd.loc['RT_AMT']
                                    data2.loc[temp_index + 1, 'PCT_CHG'] = temp_pd.loc['RT_PCT_CHG'] * 100
                                    print(data2.loc[temp_index + 1, :])
                                    # RT_DATE   RT_TIME  RT_PRE_CLOSE  RT_OPEN  RT_HIGH  RT_LOW  RT_LAST  \
                                    # 0  20170505.0  150006.0         18.75    18.79    18.84   18.54     18.8
                                    #        RT_VOL       RT_AMT  RT_PCT_CHG    RT_MKT_CAP  RT_FLOAT_MKT_CAP
                                    # 0  34318443.0  640625529.0      0.0027  4.685207e+11      3.878242e+11
                                elif temp_dif > 0.99 and temp_dif < 1.01 :
                                    #  170521
                                    # todo 注意，如果当日停牌，temp_pd_Date 中的股票 open，high,low,close,vol,amt 都是0
                                    # 这时 temp_dif = 1
                                    data2.loc[temp_index + 1, 'DATE'] = lastest_Date_2
                                    data2.loc[temp_index + 1, 'OPEN'] = temp_pd.loc['RT_PRE_CLOSE']
                                    data2.loc[temp_index + 1, 'HIGH'] =  temp_pd.loc['RT_PRE_CLOSE']
                                    data2.loc[temp_index + 1, 'LOW'] =  temp_pd.loc['RT_PRE_CLOSE']
                                    data2.loc[temp_index + 1, 'CLOSE'] = temp_pd.loc['RT_PRE_CLOSE']
                                    data2.loc[temp_index + 1, 'VOLUME'] = temp_pd.loc['RT_VOL']
                                    data2.loc[temp_index + 1, 'AMT'] = temp_pd.loc['RT_AMT']
                                    data2.loc[temp_index + 1, 'PCT_CHG'] = temp_pd.loc['RT_PCT_CHG'] * 100
                                    print(data2.loc[temp_index + 1, :])

                                else:
                                    print('170511 A ')
                                    print('RT_PRE_CLOSE ', RT_PRE_CLOSE)
                                    print('CLOSE in csv file', data2.loc[temp_index, 'CLOSE'])
                                    print('temp_pd \n', temp_pd)
                                    print(data2.loc[temp_index, :])
                                    # todo Step 2 可能发生了分红送配 的情况，单纯现金分红和， 现金加送股
                                    # todo 由于是补充历史前复权数据，因此不考虑分红，全部当成送股。
                                    # todo 由于国内都是高股价变低股价，因此需要分析涨跌幅度
                                    pre_Close2 = temp_pd.loc['RT_LAST'] / (1 + temp_pd.loc['RT_PCT_CHG'])
                                    temp_factor = round(data2.loc[temp_index, 'CLOSE'] * 100 / pre_Close2) / 100

                                    # todo change all OPEN,HIGH,LOW,CLOSE,VOLUME,AMT,PCT_CHG
                                    data2['OPEN'] = data2['OPEN'] / temp_factor
                                    data2['HIGH'] = data2['HIGH'] / temp_factor
                                    data2['LOW'] = data2['LOW'] / temp_factor
                                    data2['CLOSE'] = data2['CLOSE'] / temp_factor
                                    # AMT = VOLUME * AVE_PRICE
                                    data2['VOLUME'] = data2['VOLUME'] * temp_factor
                                    # todo then we update the latest day
                                    data2.loc[temp_index + 1, 'DATE'] = lastest_Date_2
                                    data2.loc[temp_index + 1, 'OPEN'] = temp_pd.loc['RT_OPEN']
                                    data2.loc[temp_index + 1, 'HIGH'] = temp_pd.loc['RT_HIGH']
                                    data2.loc[temp_index + 1, 'LOW'] = temp_pd.loc['RT_LOW']
                                    data2.loc[temp_index + 1, 'CLOSE'] = temp_pd.loc['RT_LAST']
                                    data2.loc[temp_index + 1, 'VOLUME'] = temp_pd.loc['RT_VOL']
                                    data2.loc[temp_index + 1, 'AMT'] = temp_pd.loc['RT_AMT']
                                    # todo 注意 wind返回的百分比数据是乘过100的
                                    data2.loc[temp_index + 1, 'PCT_CHG'] = temp_pd.loc['RT_PCT_CHG'] * 100

                                # todo save data2 to csv
                                file_path2 = Path_Data + 'Wind_' + temp_Symbol + '_updated' + '.csv'
                                data2.columns = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'AMT', 'PCT_CHG']
                                data2.to_csv(file_path2)
                                print('csv have been saved to ', Path_Data + '\Wind_' + temp_Symbol + '_updated' + '.csv')
                                print('The code is ', temp_Symbol, 'still ', str(len(temp_pd_Date.index) - i), ' to go.')

                            else:
                                if not data2['DATE'].iloc[-2] == preDate:
                                    print('Already updated for latest date ,', temp_code, temp_Date)
                                else :
                                    errorCodes = errorCodes + [temp_Symbol]

                        elif temp_pd.loc['RT_AMT'] <= 0 and (temp_pd.loc['RT_LAST'] != temp_pd.loc['RT_PRE_CLOSE']):
                            # Case 2 无成交量地涨停或跌停
                            RT_PRE_CLOSE = temp_pd.loc['RT_PRE_CLOSE']

                            temp_index = data2.index[-1]
                            RT_PRE_CLOSE = temp_pd.loc['RT_PRE_CLOSE']
                            # temp_dif = abs(RT_PRE_CLOSE - data2.loc[data2.index[-1], 'CLOSE']) / max(RT_PRE_CLOSE, data2.loc[
                            #     data2.index[-1], 'CLOSE'])
                            # print('temp_dif ', temp_dif, 'temp_index ', temp_index)
                            data2.loc[temp_index + 1, 'DATE'] = lastest_Date_2
                            data2.loc[temp_index + 1, 'OPEN'] = temp_pd.loc['RT_OPEN']
                            data2.loc[temp_index + 1, 'HIGH'] = temp_pd.loc['RT_HIGH']
                            data2.loc[temp_index + 1, 'LOW'] = temp_pd.loc['RT_LOW']
                            data2.loc[temp_index + 1, 'CLOSE'] = temp_pd.loc['RT_LAST']
                            data2.loc[temp_index + 1, 'VOLUME'] = temp_pd.loc['RT_VOL']
                            data2.loc[temp_index + 1, 'AMT'] = temp_pd.loc['RT_AMT']
                            data2.loc[temp_index + 1, 'PCT_CHG'] = temp_pd.loc['RT_PCT_CHG'] * 100
                            print(data2.loc[temp_index + 1, :])

                        elif pd.isnull(temp_pd.loc['RT_AMT'] ) and pd.isnull(temp_pd.loc['RT_VOL'] ) :
                            # 如果当日是停牌或者上海开市而港股休市，则引用前一日的数据
                            #  'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'AMT', 'PCT_CHG']
                            # todo 171209 看看 0700.HK 在 170414这一天的空值是什么格式 | pd.isnull(data1['volume'])
                            # data1=data[data['date']== ] || data1['volume'].isnull().any() :判断当日是否有成交量，如果是 nan，type=float,那么直接引用前一日的收盘价得了。
                            temp_index = data2.index[-1]
                            # print('temp_dif ', temp_dif, 'temp_index ', temp_index)
                            data2.loc[temp_index + 1, 'DATE'] = lastest_Date_2
                            data2.loc[temp_index + 1, 'OPEN'] = data2.loc[temp_index , 'OPEN']
                            data2.loc[temp_index + 1, 'HIGH'] = data2.loc[temp_index , 'HIGH']
                            data2.loc[temp_index + 1, 'LOW'] = data2.loc[temp_index , 'LOW']
                            data2.loc[temp_index + 1, 'CLOSE'] = data2.loc[temp_index , 'CLOSE']
                            data2.loc[temp_index + 1, 'VOLUME'] = data2.loc[temp_index , 'VOLUME']
                            data2.loc[temp_index + 1, 'AMT'] = data2.loc[temp_index , 'AMT']
                            data2.loc[temp_index + 1, 'PCT_CHG'] = data2.loc[temp_index , 'PCT_CHG']
                            print(data2.loc[temp_index + 1, :])

                        else:
                            print('No amount for ', temp_code)
                            # todo Step 判断 最新交易日期和 csv文件最新交易日期是否一致
                            # Case 1 停牌
                            # Case 2 无成交量地涨停或跌停

                            temp_index = data2.index[-1]
                            # todo 无成交情况
                            data2.loc[temp_index + 1, :] = data2.loc[temp_index, :]
                    else :
                        # print('data2 ' )
                        # print(data2 )

                        print('errorCodes  +1 ', temp_code)
                        errorCodes = errorCodes + [temp_code]
                except :
                    print('errorCodes  +1 ', temp_code)
                    errorCodes = errorCodes + [temp_code]

            else :
                errorCodes = errorCodes + [temp_code]
            i = i + 1

        errorCodes = pd.DataFrame(errorCodes)
        print(Path_Data + 'Wind_' + temp_f + '_' + 'errorCodes' + '_' + temp_Date + '_updated' + '.csv')
        errorCodes.to_csv(
            Path_Data + 'Wind_' + temp_f + '_' + 'errorCodes' + '_' + temp_Date + '_updated' + '.csv')


        return errorCodes

    def Update_Synthetic_Index(self, code_Syn, code_Index, path_Data ):
        # 171228 由于 模拟纯利息收入的指数 INDEX.SH 不能自动更新，因此需要这个 def 来每个交易日自动实现
        # todo update synthetic index | 171228
        # steps import syn_Index and latest date, compared with 000300.SH,get the dif_list of dates, update for every dif date
        # Wind_INDEX.SH_updated
        # last update 171228 | since 171228 2330
        import pandas as pd
        print('code_Syn code_Index ',code_Syn, code_Index)
        columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        # path_Data = 'D:\data_Input_Wind'
        file_path = path_Data + '\Wind_' + code_Syn + '_updated.csv'
        data_Syn = pd.read_csv(file_path, header=None, sep=',')
        data_Syn = data_Syn.drop([0],axis= 0  )
        # Lets change the name of the column
        if len(data_Syn.columns) == 9:
            data_Syn = data_Syn.drop(data_Syn.columns[0], axis=1)

        data_Syn.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        date_Syn_end = data_Syn['date'].iloc[-1]
        print('date_Syn  \n ',type( date_Syn_end ), date_Syn_end )
        # print('888   '  , code_Index )
        file_path2 = path_Data + '\Wind_' + code_Index + '_updated.csv'
        data_Index = pd.read_csv(file_path2 , header=None, sep=',' )
        # print(data_Index.tail(3) )
        # data_Index.columns = data_Index.loc[0,:]
        # data_Index = data_Index.drop([0], axis=1)
        data_Index = data_Index.drop([0] , axis=0  )
        if len(data_Index.columns) ==9 :
            data_Index = data_Index.drop( data_Index.columns[0] , axis=1 )
        data_Index = data_Index.iloc[-100:, :]
        # print( data_Index.tail(3) )
        # Wind_000300.SH_updated
        
        data_Index.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
        print(data_Index.tail(3) )

        date_List = data_Index['date']
        from datetime import datetime
        date_Syn_end = datetime.strptime(date_Syn_end, '%Y-%m-%d')
        # datetime.datetime(2017, 12, 22, 0, 0)

        dif_date_List = []
        temp_index = data_Syn.index[-1]
        temp_close = data_Syn.loc[temp_index, 'close']
        temp_close = float(temp_close ) * (1 + 1 / 365)

        temp_pd = pd.DataFrame(columns=columns)
        for temp_date in date_List :
            print(  )
            temp_date =  datetime.strptime( temp_date, '%Y-%m-%d')
            if temp_date > date_Syn_end:
                dif_date_List = dif_date_List +[ datetime.strftime(temp_date, '%Y-%m-%d') ]

                temp_List = []
                temp_List = temp_List + [datetime.strftime(temp_date, '%Y-%m-%d') ]

                temp_List = temp_List + [temp_close ] # open ,high,low,close
                temp_List = temp_List + [temp_close]
                temp_List = temp_List + [temp_close]
                temp_List = temp_List + [temp_close]
                temp_List = temp_List + [ 100000000 ] # volume
                temp_List = temp_List + [100000000]  # amt
                temp_List = temp_List + [ 0.0000822 ]  # pct_chg
                temp_pd2 = pd.DataFrame(temp_List )
                temp_pd2 =temp_pd2.T
                temp_pd2.columns=columns
                temp_pd = temp_pd.append( temp_pd2 )
                temp_close = temp_close* (1 + 1 / 365)
                temp_index =temp_index +1
            # now we need to update INDEX.SH　
        # print('temp_pd \n' , temp_pd )
        if len( temp_pd.index ) >0 :
            data_Syn = data_Syn.append( temp_pd, ignore_index=True )

            data_Syn.to_csv( file_path )
            print('Path to data_Syn.to_csv is \n', file_path )
        else :
            print('No change applies to synthetic idnex.')
        return  1





# ========================================================================================================
    # def Wind2Csv_min(self, WindData, file_path0, code):
    #     # 160509 previous edition :def Wind2Csv
    #     # Difference point: Date type is : '2013-05-10 09-55-00'
    #     # from :WindData.Times[4].strftime('%Y-%m-%d %H-%M-%S')
    #     code = WindData.Codes[0]
    #
    #     import csv
    #     file_path = file_path0 + 'Wind_' + code + '.csv'
    #     with open(file_path, 'w', newline='') as csvfile:
    #         # fieldnames = ['first_name', 'last_name'] ; Columns=[  'date', 'open', 'high',  'low'  , 'close', 'volume']
    #         fieldnames = WindData.Fields  # Data3.Fields=Columns ？
    #         # writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #         writer = csv.writer(csvfile)  # delimiter=' '
    #         # Write the first row as head
    #         writer.writerow(['DATE'] + fieldnames)
    #         len_item = len(WindData.Data)  # = len(Columns) =6
    #
    #         len_dates = len(WindData.Data[1])  # 253
    #         # print(WindData.Data[1])
    #         # print(WindData.Data[-1])
    #         # python中date、datetime、string的相互转换  http://my.oschina.net/u/1032854/blog/198179
    #         #  WindData3.Times[1].strftime('%Y-%m-%d') # '2016-01-05'
    #         # time.mktime( WindData3.Times[1].timetuple()) # datetime.datetime(2016, 1, 5, 0, 0, 0, 5000) to 1451923200.0
    #         for i in range(len_dates):
    #             temp_list = [
    #                 WindData.Times[i].strftime('%Y-%m-%d %H-%M-%S')]  # str  '20161215', we still need to change it to list
    #             # writer.writerow({ fieldnames[0] :WindData.Times[i] }) # date
    #             for j in range(len_item):  # without date here
    #                 temp_list.append(WindData.Data[j][i])
    #             writer.writerow(temp_list)  # date
    #
    #     return file_path

    # todo After 170509 ===========================================================================
