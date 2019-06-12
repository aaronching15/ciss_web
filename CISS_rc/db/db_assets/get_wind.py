# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
功能：初始化需要的股票价格，回报等数据
数据来源： Wind-API 万得量化数据接口
last update 180923 | since  160121
/ 
===============================================
'''
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

class wind_api():
    # 类的初始化操作
    def __init__(self):
        
        # self.start=start
        # self.end=end 
        ''' TODO： Build a class for account: Cash, Stocks, Futures,Options , etc.  '''

    ''' Part 1 Get Price data '''
    def GetWindData(self, code='600036.SH', date_0='20151220', date_1='20160118', items='open,high,low,close,volume,amt,pct_chg', output=1):
        # last ?
        # items='open,high,low,close,volume,pct_chg'
        # Notice that pct_chg is with format as 2.45 , we need to divide it with 100
        # Get Daily data
        import WindPy as WP
        # Or: from WindPy import w
        WP.w.start()
        WindData = WP.w.wsd(code, items, date_0, date_1, 'Priceadj=F')
        # if output==1 :
        # todo 要解决的主要问题是wind抓取的数据如何把数据按顺序存到csv文件里
        # print('Missing...')
        # file_path0 = 'C:\\rC_Matlab_Output'
        # file_path=file_path0+ '\Day_' + code + '.csv'
        # data.to_csv(  file_path  , encoding='utf-8', index=False)

        return WindData

    def GetWind_indexconst(self,date="2018-05-31",windcode='000016.SH') :
        # last 180923 | since 180923 
        # 为了方便在历史回测中获取最新指数成分股，我们采用过去8年沪深300指数成分股调整
        # 日所在月5,11月的最后一个交易日
        # w.wset("indexconstituent","date=2018-09-23;windcode=000300.SH")
        import WindPy as WP
        # Or: from WindPy import w
        WP.w.start() 
        WindData = WP.w.wset("indexconstituent","date="+date+";windcode="+windcode )
        '''
        >>> WindData
        .ErrorCode=0
        .Codes=[1,2,3,4,5,6,7,8,9,10,...]
        .Fields=[date,wind_code,sec_name,i_weight]
        .Times=[20180923]
        .Data=[[2018-05-31 00:00:00,2018-05-31 00:00:00,2018-05-31 00:00:00,2018-05-31 0
        0:00:00,2018-05-31 00:00:00,2018-05-31 00:00:00,2018-05-31 00:00:00,2018-05-31 0
        0:00:00,2018-05-31 00:00:00,2018-05-31 00:00:00,...],[600000.SH,600016.SH,600019
        .SH,600028.SH,600029.SH,600030.SH,600036.SH,600048.SH,600050.SH,600104.SH,...],[
        浦发银行,民生银行,宝钢股份,中国石化,南方航空,中信证券,招商银行,保利地产,中国联通
        ,上汽集团,...],[2.687,3.929,1.615,1.608,0.762,3.183,6.441,1.905,1.069,2.757,...]
        ]
        '''

        return WindData 

    def Wind2Csv(self,WindData,file_path0,code, date,time_stamp  ):
        # 180923 |与其把csv文件做那么长，不如将小规模数据改成json格式，如果文件较大，
        # 则同时存成 json+csv，json可以放核心的描述变量

        # todo 用来存放Wind历史数据的文件 file_path0=  D:\data_Input_Wind

        import csv
        # version before 190612 2115 
        file_path=file_path0 +'Wind_'+ code +'_'+date+'_'+time_stamp+ '.csv'

        

        # file_path2 = file_path0 + 'Wind_' + code+'_'+date+'_'+time_stamp + '_updated' + '.csv'
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
            writer.writerow(['DATE','time_stamp']+ fieldnames )
            len_item=len(WindData.Data) # = len(Columns) =6

            len_contents=len(WindData.Data[1]) #codes here 253 

            for i in range(len_contents ) :
                temp_list  = [date.replace('-','') ,time_stamp]

                # writer.writerow({ fieldnames[0] :WindData.Times[i] }) # date

                # for j in range(len_item) : # without date here
                # 180923
                # WindData.Data[0] | datetime.datetime(2018, 5, 31, 0, 0)
                
                temp_list.append( WindData.Data[0][i].strftime('%Y%m%d') )
                # WindData.Data[1] | '60000.SH'
                temp_list.append( WindData.Data[1][i] )
                # WindData.Data[2] | '浦发银行'
                temp_list.append( WindData.Data[2][i] )
                # WindData.Data[3] | 2.687 
                temp_list.append( WindData.Data[3][i] )
                # from [2018-05-31,20180923,2018-05-31 00:00:00,600000.SH,浦发银行,2.687]
 
                writer.writerow( temp_list ) # date


        return file_path

    def Wind2Csv_test(self,WindData,file_path0,code, date,time_stamp,file_path=''  ):

        if file_path =='' :
            # before: file_path0 = "C:\\zd_zxjtzq\\RC_trashes\\temp\\keti_sys_stra_24h\\p1\\wind_data\\"
            file_path_funda = "D:\\db_wind\\wind_data\\funda\\" 
        else :
            file_path_funda = file_path
        # file_path0 = "C:\\zd_zxjtzq\\RC_trashes\\temp\\keti_sys_stra_24h\\p1\\wind_data\\"
        # date="2018-05-31"
        # windcode='000016.SH'
        time_stamp = dt.datetime.now().strftime('%Y%m%d')
        print('Curretn time :', time_stamp )
        # GET RAW INDEX CONSTITUENTS AND WEIGHTS from Wind API
        WindData= self.GetWind_indexconst(date ,windcode ) 
        file_path = self.Wind2Csv(WindData,file_path0,windcode, date,time_stamp  )
                            
        years = [2011,2012,2013,2014,2015,2016,2017] 
        count = 0
        for year in years :
          for mmdd in ['05-31','11-30'] :
              date = str(year) +'-'+ mmdd
              # '000300.SH','000905.SH',
              for windcode in ['000300.SH','000905.SH'] :
                  WindData= temp_DB.GetWind_indexconst(date ,windcode ) 
                  file_path = temp_DB.Wind2Csv(WindData,file_path0,windcode, date,time_stamp  )
                  count +=1 
                  print('The '+str(count) +' '+ date+' '+windcode +' has been done ')
                  # 直接新建一个 rc_csi1800 指数，用json描述建立方式

              if year >=2015 or (date == '2014-11-30') :
                  windcode = '000852.SH'
                  WindData= temp_DB.GetWind_indexconst(date ,windcode ) 
                  file_path = temp_DB.Wind2Csv(WindData,file_path0,windcode, date,time_stamp  )
                  count +=1 
                  print('The '+str(count) +' '+ date+' '+windcode +' has been done ')
 
        return 1
    
    def GetWind_funda(self,date="20180831",rptDate="20171231",windcode='600036.SH',items='industry_gicscode') :
        # get fundamental data using WindPy 
        # last 180928 | since 180928  
        '''
        Notice：只是A股市场的一个指标，后续标准化算法进行细类加权打分。
        1,Industry :Basic Info\\Company Profile\\Industrial Classification\\Wind Industrial Code\\industry_gicscode
        2,Value :Securities Analysis\\Stock Valuation\\Enterprise Value\\Enterprise Multiple 2\\val_evtoebitda2 
        3,Growth :Securities Analysis\\Estimated Ratings\\WIND Consensus Estimated(Rolling)\\Consensus Estimated Net Profit:2-yr Compound Growth Rate\\west_netprofit_CAGR
        4,Capital Structure :Securities Analysis\\Financial Data\\Financial Analysis\\Capital Structure\\Capital Immobilization Ratio:ncatoequity
        5,Debt-paying :Securities Analysis\\Estimated Ratings\\WIND Consensus Estimated(Rolling)\\Consensus Estimated Operating Profit:2-yr Compound Growth Rate\\west_avgoperatingprofit_CAGR
        6,Operating ：Securities Analysis\\Financial Data\\Financial Analysis\\Operating Capacity\\Turnover Rate of Current Assets\\caturn 流动资产和非流动周转率都没有
        7,HR :Basic Info\\Company Profile\\Staff Composition\\No. of Technicians\\employee_tech
        8,info advantage :Basic Info\\Company Profile\\Staff Composition\\No. of M.S.\\employee_MS

        WindPy代码 多维数据
        w.wss("600036.SH", "industry_gicscode,val_evtoebitda2,west_netprofit_CAGR,ncatoequity,west_avgoperatingprofit_CAGR,caturn,employee_tech,employee_MS","industryType=4;tradeDate=20180927;rptDate=20171231")
        由于第 2,4,6个数据无Wind返回值，需要换financial indicators。(idea：有时候不在于哪个指标最反应真实财务世界的价值，而是哪个指标最被市场投资者under-estimated)
        2,Value :Securities Analysis\\Estimated Ratings\\WIND Consensus Estimated(Rolling)\\Consensus Estimated ROE(FY2):west_avgroe_FY2
        4，Capital Structure :Securities Analysis\\Financial Data\\Financial Analysis\\Capital Structure\\Solvency Ratio of long-term Assets：longcapitaltoinvestment
        6，Operating ：Securities Analysis\\Financial Data\\Financial Analysis\\Operating Capacity\\Total Assets Turnover Ratio(TTM):turnover_ttm 总资产周转率有数据

        企业价值收益比又称企业倍数，是企业价值(剔除货币资金)与企业收益(扣除利息、税金、折旧和摊销前的收益)的比值。企业倍数估值包含债务，从潜在收购方的角度评估公司的价值。

        【算法】 企业价值收益比=EV2／息税折旧摊销前利润|注：本公式中，息税折旧摊销前利润(EBITDA)是根据选定交易日向前最近一期年报(LYR)财务数据计算的，详细算法参见息税折旧摊销前利润、EV2算法说明。
        【释义】 一致预测净利润2年复合增长率
        【算法】一致预测净利润2年复合增长率=(（一致预测净利润_FY2/净利润_FY0)^（1/2）-1）*100 | 其中：净利润均指归属母公司股东净利润。
        【释义】 一致预测营业利润2年复合增长率 【算法】  一致预测营业利润2年复合增长率＝((一致预测营业利润_FY2/营业利润_FY0)^(1/2)-1)*100
            ana: assumption： 营业利润增长率和 偿债能力具有正相关性。
        Baidu:  营业利润 = 营业收入 - 营业成本 - 期间费用 - 主营业务税金及附加。
                利润总额 = 营业利润 + 投资收益 + 营业外收入 - 营业外支出 + 以前年度损益调整
                净利润 = 利润总额 - 所得税费用。
        Held by State Legal Persons | share_rtd_statejur 
        【释义】 国有企业、国有独资公司、事业单位以及第一大股东为国有及国有控股企业且国有股权比例合计超过50%的有限责任公司或股份有限公司持有股份的限售部分。
        todo, prepare a discussion meeting with touyan and IT: technical view.
        '''

        import WindPy as WP
        # Or: from WindPy import w
        WP.w.start()
        # items = ['industry_gicscode','west_avgroe_FY2','west_netprofit_CAGR','longcapitaltoinvestment','west_avgoperatingprofit_CAGR','turnover_ttm','employee_tech','employee_MS']
        # [['40101010'], [17.1056], [15.6381], [78.33321112638977], [23.7731], [0.0361], [1698.0], [12752.0]]
        # para = "industryType=4;tradeDate=20180927;rptDate=20171231"
        if len(date) == 8 :
            date2 = date
        elif len(date) == 10 :
            # date2 = "20171231"
            date2 = date.replace('-','')
        
        type_ind = 4 
        para = "industryType="+ str(type_ind)+ ";tradeDate="+ date2+ ";rptDate="+ rptDate
        # w.wss("600036.SH", "west_avgroe_FY2","tradeDate=20181008")也可以，可见 rptDate不一定是必须的。
        WindData = WP.w.wss(windcode, items,para)
        # case 600036.SH para = "industryType=4;tradeDate=20180831;rptDate=20180831"
        # Data: [['40101010'], [None], [15.6381], [None], [23.7731], [None], [1698.0], [12752.0] ]
        # case 600036.SH para = "industryType=4;tradeDate=20180831;rptDate=20171231"

        return WindData

    def Data_funda_csvJson(self,len_max,len_codes,items,time_stamp,rptDate,wind_code,file_path0,file_path_funda,date,time_stamp_input,country='CN') :
        '''
        last 181012 | since 181012
        Save fundamental result from Wind-API and save to csv&json file.
        input: wind_code,file_path0,file_path_funda,date,time_stamp_input,country
        para: len_max,len_codes,items,rptDate
        algo:
        output:data_json_rc(WindData,| temp_pd_wind)
        case 1:
        len_max = 100 for wind limitation, or len_max = 2 for test
        len_codes = len(code_list) or len_codes = 3 for test
        '''
            
        file_path=file_path0 +'Wind_'+ wind_code +'_'+date+'_'+time_stamp_input+ '.csv'
        data_raw = pd.read_csv(file_path, encoding="GBK")
        '''
                 DATE  time_stamp      date  wind_code sec_name  i_weight
        0    20170531    20180923  20170531  000001.SZ     平安银行     0.812
        1    20170531    20180923  20170531  000002.SZ      万科A     1.488
        2    20170531    20180923  20170531  000008.SZ     神州高铁     0.060
        '''
        # we need column "wind_code"
        code_list = list(data_raw.wind_code) # series to list | 1     000002.SZ 2     000009.SZ

        # len_item =  len(items)
        # 345= n*(345//n) + 345%n = 取整部分 + 余数部分
        # 
        len_codes = len(code_list) 
        print('len_codes ',len_codes )
        div = len_codes//len_max
        rem = len_codes%len_max
        print('div,rem, ',str(div),str(rem) )
        temp_pd_wind = pd.DataFrame(   )
        for item in items : 
                # avoid single time amout limit from wind 
            temp_list0 = []    
            i=0
            for i in range(div) :
                # 0,1,2, ... | 0-99,100-199
                sub_code_list = code_list[i*len_max+0:i*len_max+ len_max ]
                # 根据成分股获取日期，前推获得最近一期基本面数据 |windAPI基本面指标至少有部分无法自动识别
                # staff\Edu Degree 数据只有年末才有，要做好半年数据不全的准备
                # 也许没必要跟着指数的节奏来，可以直接按照6-31， 12-31的财报时间？？
                # print('date,rptDate,sub_code_list,item')
                # print(date,rptDate,sub_code_list,item)
                WindData= self.GetWind_funda(date,rptDate,sub_code_list,item)
                time.sleep(0.4)
                print(str(i),' WindData \n' )

                # [12.2488, 13.096] for .Codes=[000300.SH,601398.SH],.Fields=[WEST_AVGROE_FY2]
                temp_list0 = temp_list0 + WindData.Data[0]  
                # index= different items(wind indicators) ||
                # columns= sub_code_list

                time_getwind = WindData.Times[0] # datetime.datetime(2018, 10, 9, 16, 24, 47)

            if rem >0 :
                # get reminder portion
                sub_code_list = code_list[div*len_max :len_codes]
                # print('rem case: sub_code_list', sub_code_list)
                WindData= self.GetWind_funda(date,rptDate,sub_code_list,item)
                time.sleep(0.4)
                print(str(i+1),' WindData \n'  )
                temp_list0 = temp_list0 + WindData.Data[0]  
                # print('temp_list0  ',temp_list0 )

                time_getwind = WindData.Times[0] # datetime.datetime(2018, 10, 9, 16, 24, 47)
                # time_getwind 以本轮loop 最后一次的时间为准
            
            temp_pd0 = pd.DataFrame( temp_list0  )
            print('temp_pd0 \n', temp_pd0 )
            temp_pd0.columns = [item]
            temp_pd0.index = code_list[0:len_codes]
            
            # NOTES:对于单个item的数据搜集已经搞定，下一步是整合多个items，保存数据。
            # merge temp_pd0 into temp_pd_wind(with multiple items )
            # empty pd can be merged, axis=1 means 2 pd with similar index and different columns
            #  | append() got an unexpected keyword argument 'axis'
            temp_pd_wind= pd.concat([temp_pd_wind,temp_pd0],axis=1,ignore_index=False)
            # print('temp_pd_wind \n', temp_pd_wind )
            temp_pd_wind['time_getwind'] =  dt.datetime.strftime(time_getwind ,"%Y%m%d")  
        
        print('temp_pd_wind \n', temp_pd_wind )
        '''
                    industry_gicscode        time_getwind
        000001.SZ          40101010 2018-10-10 11:17:53
        industry_gicscode 和 industry_gics 一一对应，可以通过表格建立关系。 ''' 
        # output .json and .csv data file 
        #  file_path0 +'Wind_'+ wind_code +'_'+date+'_'+time_stamp_input+ '.csv'
        temp_name_csv = file_path_funda + 'funda_'+ country + '_'+ wind_code +'_'+date+'_'+time_stamp_input+ '.csv'
        temp_pd_wind.to_csv(temp_name_csv)
        temp_name_json =file_path_funda + 'funda_'+ country + '_'+ wind_code +'_'+date+'_'+time_stamp_input+ '.json'
        temp_name_json_head =file_path_funda + 'funda_'+ country + '_'+ wind_code +'_'+date+'_'+time_stamp_input+ '_head.json'
        # write head summary info to json file and readlater：j1 = pd.read_json('d:\\temp_181012.json')
        # dictionary var to json : with open("temp.json","w",encoding="utf-8") as f:
            # f.write(json.dumps(data1) )
        # pd to json : json = si.to_json()
        temp_pd_wind.to_json(temp_name_json,orient="columns")
        # test2 = pd.read_json(temp_name_json )
        # print('test2 \n',test2 ) 

        temp_dict = {}
        temp_dict['data_type'] = 'funda'
        temp_dict['time_stamp'] = time_stamp
        temp_dict['time_stamp_input'] = time_stamp_input
        temp_dict['country'] = country
        temp_dict['year'] = date[0:4]
        temp_dict['mmdd'] = date[4:8] 
        temp_dict['index_code'] =wind_code
        temp_dict['date_index_review'] =date
        temp_dict['path_csv'] =temp_name_csv
        temp_dict['path_json'] =temp_name_json
        # temp_dict['data_csv'] = temp_pd_wind.to_json(orient="columns")
        print('temp_dict \n',temp_dict )
        with open(temp_name_json_head,"w",encoding="utf-8") as f:
            # output dict. to json 
            f.write(json.dumps(temp_dict) )

        # trying read csv and json 
        file_csv = pd.read_csv(temp_name_csv )
        print('file_csv \n',file_csv )
        file_json = pd.read_json(temp_name_json )
        print('file_json \n',file_json )
        # for non table type json file, pandas cannot read 
        # # read json as dict
        # f=open(temp_name_json_head,"r")
        # for line in f:
        #     # 不断把json中的文件加进 decodes
        #     file_json_head=json.loads(line) 
        file_json_head = temp_dict
        print('file_json_head \n',file_json_head )

        # create a data class with 3 objects
        # def __init__(self, name,age):         
        #__init__(self,属性1，属性2....)：self代表类的实例，通过self访问实例对象的变量和函数
        class data_json_rc :
            def __init__(self, file_csv,file_json,file_json_head):
                self.info = "This class has 3 pd. or dict: file_csv,file_json,file_json_head"
                self.file_csv= file_csv
                self.file_json= file_json
                self.file_json_head= file_json_head

        # 创建 data_json_rc 类的一个对象
        data_json_rc = data_json_rc(file_csv,file_json,file_json_head) 
        return data_json_rc

    def Data_funda_csvJson_test(self,years = [2018],mmdd_list =['11-30'] ):
        # a test case for def Data_funda_csvJson
        # untested 
        # from get_wind import rC_DB 
        # temp_DB = rC_DB('')
        # ### Generate and update 
        # years = [2013,2014,2015,2016,2017] 
        # mmdd_list =['05-31','11-30']

        import json
        file_path0 = "D:\\db_wind\\wind_data\\"
        file_path_funda = file_path0 + "funda\\" 
        items = ['industry_gicscode','west_avgroe_FY2','west_netprofit_CAGR','longcapitaltoinvestment','west_avgoperatingprofit_CAGR','turnover_ttm','employee_tech','employee_MS']

        temp_columns = ['date','rptDate','time_getwind'] + items
        temp_pd0 = pd.DataFrame( columns= temp_columns)
        temp_pd = pd.DataFrame( columns= temp_columns)
        # time_stamp = dt.datetime.now().strftime('%Y%m%d')
        time_stamp_input = "20180923" # timestamp is the day we get 
        time_stamp = "20180923"
        country = 'CN'
        # 沪深市场企业员工学历和岗位分类从2013年开始披露，因此2011,2012的数据可能无法使用。


        index_list = ['000300.SH','000905.SH','000852.SH']
        count = 0
        # len_max = 100 for wind limitation, or len_max = 2 for test
        # len_codes = len(code_list) or len_codes = 3 for test
        len_max = 100  # max 100
        len_codes = 5 # 
        for year in years :
            # year >=2015 or (date == '2014-11-30') :  windcode = '000852.SH' 
            for mmdd in mmdd_list :
                if mmdd == '05-31' :
                    rptDate = str(year-1) +'1231'
                elif mmdd == '11-30' :
                    rptDate = str(year) +'0630'
                date = str(year) +'-'+ mmdd # '2018-05-31' or '20180531' 都可以 
                
            # CSI指数编制中 05-31对应的是前1年12-31的财务数据已经披露，11-30对应的是当年6-30半年报的数据
            # 实际工作中5-31应该可以用基于一季报的半年财务数据预测，11-30可以获得基于Q2/Q3的年度财务数据预测
                # import code lists   ['000300.SH','000905.SH'] 
                for wind_code in ['000300.SH','000905.SH' ] :
                    count +=1 
                    print('The '+str(count) +' '+ date+' '+wind_code +' is working ... ')
                    
                    data_json_rc = self.Data_funda_csvJson(len_max,len_codes,items,time_stamp,rptDate,wind_code,file_path0,file_path_funda,date,time_stamp_input,country='CN')
                    print('===============1')
                    print( data_json_rc.info )
                    print( data_json_rc.file_json_head )
                    print( data_json_rc.file_csv )
                    print( data_json_rc.file_json )
                    print('===============2')          

                if year >=2015 or (date == '2014-11-30') :
                    wind_code = '000852.SH'
                    count +=1 
                    print('The '+str(count) +' '+ date+' '+wind_code +' has been done ')
                    data_json_rc = self.Data_funda_csvJson(len_max,len_codes,items,time_stamp,rptDate,wind_code,file_path0,file_path_funda,date,time_stamp_input,country='CN')
                    print('===============1')
                    print( data_json_rc.info )
                    print( data_json_rc.file_json_head )
                    print( data_json_rc.file_csv )
                    print( data_json_rc.file_json )
                    print('===============2')    

        return result 

    def Data_funda_concat_json(self,file_path_funda,time_stamp_input="20180923"):
        '''
        last 181013 | since 181013
        Merge additional data into one pd： create/import database file,update data 
        with imported new info, save to database file.
        Import fundamental data from seperate json/csv files and concate pd into a big table.
        Qs: whether we need to always import all data before analysis? 
        Ana: maybe we can using 2 periods dat at one time.
        input: wind_code,file_path0,file_path_funda,date,time_stamp_input,country
        para:  
        algo:
        output:data_json_rc
        =======         
        '''
        # step 1 import json file 
        file_name = "funda_country_index_date_"+ time_stamp_input+ "_head.json"
        # 由于不是table形式，不适合用pd.read_json()导入,用 json
        import json
        # json.dumps    将 Python 对象编码成 JSON 字符串|json.loads  将已编码的 JSON 字符串解码为 Python 对象
        # json_head = json.loads(file_name) # 直接这样会报错
        with open(file_path_funda +file_name, "r", encoding='utf-8') as f:
            result = json.loads(f.read())
            f.seek(0)
            bb = json.load(f)    # 与 json.loads(f.read())
        print(result)
 
        return result

    def Load_funda_csvJson(self,file_path_funda,date,time_stamp,wind_code,country='CN') :

        '''
        Load fundamental files from csv or json file
        # last 181017 | since 181017
        para: 
        input:
        output:

        '''
        print('file_path_funda ',file_path_funda)
        # Load .json and .csv data file 
        #  file_path0 +'Wind_'+ wind_code +'_'+date+'_'+time_stamp_input+ '.csv'
        temp_name_csv = file_path_funda + 'funda_'+ country + '_'+ wind_code +'_'+date+'_'+time_stamp+ '.csv'
        temp_name_json =file_path_funda + 'funda_'+ country + '_'+ wind_code +'_'+date+'_'+time_stamp+ '.json'
        temp_name_json_head =file_path_funda + 'funda_'+ country + '_'+ wind_code +'_'+date+'_'+time_stamp+ '_head.json'

        # derived from "def Load_funda_csvJson"
        # trying read csv and json 
        file_csv = pd.read_csv(temp_name_csv )
        file_csv['code'] = file_csv['Unnamed: 0']
        file_csv.drop(['Unnamed: 0'],axis=1)
        # print('file_csv \n',file_csv.head(5) )        
        
        file_json = pd.read_json(temp_name_json )
        # print('file_json \n',file_json )
        # for non table type json file, pandas cannot read 
        # # read json as dict | load是从文件里面load,loads是从str里面load
        with open(temp_name_json_head, "r",encoding="utf-8") as temp_f :
            file_json_head = json.load(temp_f) 
        # print('file_json_head \n',file_json_head ) 

        # create a data class with 3 objects
        # def __init__(self, name,age):         
        #__init__(self,属性1，属性2....)：self代表类的实例，通过self访问实例对象的变量和函数
        class data_json_rc() :
            def __init__(self, file_csv,file_json,file_json_head):
                self.info = "This class has 3 pd. or dict: file_csv,file_json,file_json_head"
                self.file_csv= file_csv
                self.file_json= file_json
                self.file_json_head= file_json_head

        # 创建 data_json_rc 类的一个对象
        data_json_rc = data_json_rc(file_csv,file_json,file_json_head) 

        return data_json_rc
 


