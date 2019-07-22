# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function: 
功能：
1, class data_wind:管理来自Wind-API的数据
2，input：
3，OUTPUT:data_head{字典信息},data_df{表格信息}

wind数据管理模块：
1.0，建立本地数据更新日志，判断每个环节是否已经更新。
    【证券/指数前复权日频率数据，个股】
    input:date
    output:log_data_head,log_data_df
1.1，逐日保存wind单日收盘的行情数据，使用w.wss();
1.2，每周更新股票代码、
1.3,定期调整：每年6和12月第二个星期六获取指数成分变动信息，通常是周五剔除，周一加入。
1.4，缺失数据维护：判断是否发生分红送配，并和当日涨跌幅比较。
若无异常，更新本地全复权个股历史数据。若有异常，则打印近期行情数据，确定后重新下载该股历史行情数据。
2,更新 data_wind.load_quotes(),除了最新前复权数据，还需要抓取昨日分红送配事件数据放在输出的code_df 里。



last update 181107 | since 181107
Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }

分析：
1，数据输入输出管理 

2，配置文件 | config\config_data.py
 
4,output file 
    1,head json file of trades
    2,  dataframe of  
    3,  dataframe of 

Notes: 
refernce: rC_Data_Initial.py 
similar with get_wind.py
===============================================
'''
import sys
sys.path.append("..")
import pandas as pd
#######################################################################
class data_io():
    def __init__(self, db_name=''):
        # 
        self.db_name = 'db_name'
        # todo todo :create data directory !!!


#######################################################################

class data_wind():
    def __init__(self, db_name_win='',path0='D:\\db_wind\\'):
        # 2 data types: head and df 
        self.db_name_win = 'db_name_win' 
        self.path0 = path0

    def print_info(self):
        ###
        print("class data_wind() operates data from wind-api.  ")

        print("log_data_wind || 建立本地数据更新日志，判断每个环节是否已经更新。")


        return 1

    def log_data_wind(self,temp_date,if_type="gen",type_freq="daily"):
        ### 建立本地数据更新日志，判断每个环节是否已经更新。可以导出后在json和csv中维护。
        '''
        # input : 
            temp_date:当前日期
            if_type 判断类型
                1，"gen" 生成log文件等
                2，"update" 更新log文件
            type_freq 日期频率
                1，"daily" 每日行为
                2，"month" 每月行为
                3，"quart" 每季度行为
        # output: log_data_head,log_data_df
        目的：覆盖全部数据及位置，不求全，但求精简能找到数据的位置。

        last 190717| since 190716

        '''

        #######################################################################
        ### 1,Generate file for the first time 
        if if_type == "gen" : 

            #######################################################################
            ### Check or generate log directory
            import os
            path_log_data_io= "D:\\CISS_db\\log\\data_io\\"
            if not os.path.isdir( path_log_data_io ) :
                os.mkdir( path_log_data_io )

            ### generate head and dataframe for data log
            import pandas as pd 
            log_data_head = {}
            log_data_head["date_initial"] = temp_date #20190716

            col_list=[]
            ### id, 名称，键{以逗号划分的表名}，值{以逗号划分的值例子}，数据分类{基础信息、行情、指数和etf}
            col_list=col_list + ["id","name","key","value_example","type"]
            ### 数据获取时间、数据起始时间、数据截止时间,是否有错误，是否完整,价格复权
            col_list=col_list + ["date_update","date_start","date_end","if_error","if_complete","price_adj"]
            ### 文件位置，文件名称，文件(关键字)类型，文件日期格式{直接字符串替代即可}
            #例子：Wind_all_A_Stocks_wind_170925_updated.csv
            #：Wind_all_A_Stocks_wind_YYMMDD_updated.csv
            # file_name =file_name.replace("YYMMDD", temp_date ) # temp_date="190716"
            # "date_format" = "YYMMDD"
            col_list=col_list + ["file_dir","file_name","file_type","date_format","basic_format","quote_format" ]
            
            log_data_df=pd.DataFrame(columns=col_list )

            #######################################################################
            ### Assgin specific data file to rows of log_data_df
            # derived from ：
            name_list= []


            #######################################################################
            ### save json and csv file to location path 
            log_data_head["file_json"] = "log_data_head" +'.json'
            
            import json
            with open( log_data_head["file_json"]  ,'w') as f:
                json.dump( log_data_head ,f) 

            log_data_head["file_csv"] = "log_data_head" +'.csv'

            log_data_df.to_csv(log_data_head["file_csv"], encoding="gbk" ) 

        #######################################################################
        ### 2,Update given data files 

        if if_type == "update" : 
            #######################################################################
            ### 2.1,Import log_data_head,log_data_df
            with open( log_data_head["file_json"] ,'r') as f:
                # sp_head = json.loads( f )  will bring error 
                log_data_head = json.loads( f.read() ) 

            log_data_df = pd.read_csv(log_data_head["file_csv"], encoding="gbk" ) 



        '''
        columns=[""]
        数据及参数分类：
        基本数据：代码和简称匹配{曾用代码，曾用简称}，行业分类数据，时间数据
        行情数据：{A股，指数，基金}{个股前复权日行情，单日市场不复权行情}
        指数和etf数据：T日成分股列表，成分股进出
        基本面数据：财务数据，财务指标数据

        检查要点：item/列名是否完整，时间序列是否完整，(i,j)处是否有缺失值

        例子：
        单日市场不复权行情：
        file_name = "Wind_all_A_Stocks_wind_170925_updated.csv"

        notes:中证数据
        1，指数调整日期，
        2，中证行业市盈率，逐日-个股，http://www.csindex.cn/zh-CN/downloads/industry-price-earnings-ratio?type=zz1&date=2019-07-15
            上市公司分为10个一级行业、25个二级行业、67个三级行业和138个四级行业
        3，中证行业分类{和GICS一个意思}：
        3.1，行业分类CICS表： http://www.csindex.cn/uploads/downloads/other/files/zh_CN/other_download10.xls
        3.2，逐日中证行业市盈率：http://203.187.160.133:9011/115.29.210.20/c3pr90ntc0td/syl/csi20190715.zip
        3.3，逐日中证行业分类表,cicslevel2.xls： http://203.187.160.132:9011/www.csindex.cn/c3pr90ntc0td/uploads/downloads/other/files/zh_CN/ZzhyflWz.zip
        3.4，逐日中证行业分类变动{新股}，cicslevel2change20190715.xls:http://203.187.160.132:9011/www.csindex.cn/c3pr90ntc0td/uploads/downloads/other/files/zh_CN/ZzhyflWzDay.zip
        3.5,中证行业分类说明：http://203.187.160.132:9011/www.csindex.cn/c3pr90ntc0td/uploads/downloads/other/files/zh_CN/other_download7.pdf
        数据位置：D:\\CISS_db\\data_csi\\
        idea:由于中证数据是以压缩包方式下载，因此可能需要人工维护。
        '''

        
        
        #######################################################################
        ### 2.2,按照顺序依次下载
        '''
        1,Daily\\Stock\\CN:{csi300，csi500，csi1000 }
           ..\\Stock\\HK:{HK?? }
           ..\\Stock\\US:{US?? }
           ..\\Index\\CN:{csi300，csi500，csi1000,csi_industry,csi_concept }
           ..\\Index\\HK:{hsi }
           ..\\Index\\US:{SP500,nasdaq}
           ..\\ETF\\CN:{csi300，csi500，csi1000,etf_industry,etf_concept,bond,commodities }
           ..\\ETF\\HK:{hsi }
           ..\\ETF\\US:{sp500,iShares }
        2,Monthly\\stock_list\\CN-csi,HK-csi,US-?
            每个月更新一次：{交易日，中港美股票列表。}
        
        3,Quarterly\\basics\\trading_dates{} 
        '''


        #######################################################################
        ### Quarterly adjustment | 目前以手动为主
        from db.db_assets.get_wind import wind_api
        wind_api_1 = wind_api()


        if if_type == "quart" : 
            ### 导入最新交易日文件，获取最新日期，和当年
            path_dt= "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\db\\db_times\\"
            from db.times import times
            times1 = times()
            date_start= "20100101"
            date_end = temp_date 
            # col_name= [SSE,SZSE,INTERBANK_BM,HKEX,NASDAQ,NYSE]
            # wind_para=[SSE SSE, NIB,HKE,NASDAQ,NASDAQ ]
            # note:
            for mkt in ["SSE","HKE","NIB","NASDAQ"]:
                date_list = times1.update_tday_list(date_start,date_end,mkt) 

            ### TODO，将更新信息加入 log_data_wind

        if if_type == "month" :
            ### todo:手工维护可能更合适？
            ### 每个月更新一次：{交易日，中港美股票列表。}
            file_path0 = "D:\\CISS_db\\data_csi\\"
            
            ### CN A股：csi300,500,1000
            for temp_index in ["000300.SH","000905.SH","000852.SH"] :
                ### get constituents from wind-api
                wind_data0 = wind_api_1.GetWind_indexconst(temp_date,temp_index )
                ### save to csv file 
                date_report = input("Type in date to get index constituents:e.g. 20190617") # temp_date # 
                date_update = temp_date
                file_path = wind_api_1.Wind2Csv_indexconst(wind_data0,file_path0,temp_index, date_report,date_update  )

            ### HK H ，csi_HK300
            ### TODO，将更新信息加入 log_data_wind

        if if_type == "daily" :
            #########################################################################
            ### 1,下载当日所有指数、ETF，股票{A,H}收盘数据
            ### todo,参考 test_Wind_19.py

            path_data = 'D:\\data_Input_Wind\\'
            SP_path = 'D:\\CISS_db\\data_csi\\'
            print("Path for symbol list:",SP_path)
            ### A_Index&ETF, A_stocks, csi_HK300
            ### A股指数和ETF，A股全部股票，中证港股300指数成分。
            file_list = ['All_Index_ETF.csv',"cicslevel2_1907.xls" ,'H11164cons.xls']
            list_names =["index-ETF","CN-stocks","HK-stocks"]
            print("File name for symbols :",SP_List)
            # Excel format for code2wind_code, 
            # =REPT("0",(5-len(code) ) )&E2&".HK"
            # =if(left(code,1)="6",code&".SH",'..SZ') || 删除2和9开头的股票

            temp_date =  input('Please type in Date,e.g.190718 : ')
            temp_predate = input('Please type in Pre Day,e.g.190717 : ')

            from db.db_assets.get_wind import wind_api
            wind_api_1 = wind_api()

            j=0
            for file_name in  file_list :
                print('Working on Symbol List :', file_name )
                # Get Wind-WSQ single day data
                # step 1 get SymbolList from : SL_path : path of SymbolList
                path_list = SP_path  + file_name

                list_name = list_names[j]
                quote_list = wind_api_1.Get_wsq(path_list,temp_date,path_data,list_name,file_name ,  '')
                j=j+1 
            #########################################################################
            ### 2，更新历史数据
            '''
            
            quote_list.to_csv(path_data + 'Wind_' + file_name[:-4] + '_' + temp_date+ '_updated' + '.csv')
            Wind_Index-ETF_190718_updated.csv
            '''
            temp_date = "190718"
            temp_f = 'All_Index_ETF.csv'
            j=0
            list_name = list_names[j]

            # Assign wind_code to indx 
            df_quote_list = pd.read_csv(path_data + 'Wind_' + list_name + '_' + temp_date+ '_updated' + '.csv',index_col='Unnamed: 0')

            for temp_code in df_quote_list.index :
                ###
                df_stock = pd.read_csv(path_data + 'Wind_' + temp_code + '_updated' + '.csv')
                # get lastest date 
                df_stock.loc[df_stock.index[-1],'DATE' ]



        ### todo,确定上述的信息保存在本地的表格里。


 







        #######################################################################

        return log_data_head,log_data_df

    def data_wind_wsd(self,symbols,date_start,date_end,type_wsd='week') :
        # get data using wind api\wsd 
        # using items and para from config 
        date_start =date_start.replace('-','')
        date_end =date_end.replace('-','')
        
        ## Check availablility
        if type(symbols)== list and len(symbols) >1 :
            print("multi-codes with multi-indicators is not supported.")

        # w.wsd("600036.SH", "open,high,low,close,volume,amt,turn", "2018-10-08", "2018-11-06", "Period=W;PriceAdj=F")
        import WindPy as WP
        # Or: from WindPy import w
        WP.w.start()

        from config.config_data import config_data
        # type_wsd='week'
        config_0 = config_data('').gen_config_wsd(type_wsd)
        # get 1 year data more 
        date_start2 =  str(int(date_start[:4])-1) + date_start[4:]
        wind_data = WP.w.wsd(symbols, config_0["items"] , date_start2,date_end, config_0["para"] )
        # print(wind_data)  

        wind_head = {}
        # input keys
        # vip:start date of quotation should be at least 100 days earlier
        # so we choose to set 1 year earlier, which method is quite easy
        
        wind_head['id'] = symbols + '_' + type_wsd + '_' +date_start + '_' +date_end
        wind_head['symbols'] = symbols
        wind_head['items'] =  config_0["items"] 
        wind_head['date_start'] = date_start
        wind_head['date_end'] = date_end
        wind_head['para'] =  config_0["para"] 
        # output keys 
        wind_head['error_code'] = wind_data.ErrorCode
        wind_head['fields'] = wind_data.Fields
        # wind_data.Times, wind_data.Data
        # generate path for data 

        # wind_head['path0'] =  self.path0 + "wsd\\"

        class wind_obj() :
            def __init__(self, wind_head,wind_data):
                self.wind_head = wind_head
                # wind_data object to wind dataframe 

                # todo,1,wind2csv;2,backup file;3,head file 
                # save directory 
                len_1 = len( wind_data.Fields)
                import pandas as pd 
                # notes input items might be the same as wind_data.Fields
                # wind_df = pd.dataframe(columns= wind_data.Fields)
                wind_df = pd.DataFrame(columns= wind_head['items']  )
                for i in range( len_1 ):
                    wind_df[wind_head['items'][i]] = wind_data.Data[i]

                # assign dates to wind_df 
                wind_df['date'] = wind_data.Times
                # print( wind_df )
                self.wind_df = wind_df

        wind_obj_0 = wind_obj(wind_head,wind_data)

        return wind_obj_0

    def data_wind_wsd_us(self,symbols,date_start,date_end,type_wsd='day_us') :
        # for US stocks || type_wsd in [ 'day_us','day_hk' 
        # last 181226 | since 181225

        # get data using wind api\wsd 
        # using items and para from config 
        date_start =date_start.replace('-','')
        date_end =date_end.replace('-','')
        
        ## Check availablility
        if type(symbols)== list and len(symbols) >1 :
            print("multi-codes with multi-indicators is not supported.")

        # w.wsd("600036.SH", "open,high,low,close,volume,amt,turn", "2018-10-08", "2018-11-06", "Period=W;PriceAdj=F")
        import WindPy as WP
        # Or: from WindPy import w 
        WP.w.start()

        from config.config_data import config_data
        # type_wsd='week'
        config_0 = config_data('').gen_config_wsd(type_wsd)
        # get 1 year data more 
        date_start2 =  str(int(date_start[:4])-1) + date_start[4:]
        wind_data = WP.w.wsd(symbols, config_0["items"] , date_start2,date_end, config_0["para"] )
        print(wind_data)  

        wind_head = {}
        # input keys
        # vip:start date of quotation should be at least 100 days earlier
        # so we choose to set 1 year earlier, which method is quite easy
        
        wind_head['id'] = symbols + '_' + type_wsd + '_' +date_start + '_' +date_end
        wind_head['symbols'] = symbols
        wind_head['items'] =  config_0["items"] 
        wind_head['date_start'] = date_start
        wind_head['date_end'] = date_end
        wind_head['para'] =  config_0["para"] 
        # output keys 
        wind_head['error_code'] = wind_data.ErrorCode
        wind_head['fields'] = wind_data.Fields
        # wind_data.Times, wind_data.Data
        # generate path for data 

        # wind_head['path0'] =  self.path0 + "wsd\\"

        class wind_obj() :
            def __init__(self, wind_head,wind_data):
                self.wind_head = wind_head
                # wind_data object to wind dataframe 

                # todo,1,wind2csv;2,backup file;3,head file 
                # save directory 
                len_1 = len( wind_data.Fields)
                import pandas as pd 
                # notes input items might be the same as wind_data.Fields
                # wind_df = pd.dataframe(columns= wind_data.Fields)
                wind_df = pd.DataFrame(columns= wind_head['items']  )
                for i in range( len_1 ):
                    wind_df[wind_head['items'][i]] = wind_data.Data[i]

                # assign dates to wind_df 
                wind_df['date'] = wind_data.Times
                # print( wind_df )
                self.wind_df = wind_df

        wind_obj_0 = wind_obj(wind_head,wind_data)
        ########################################################################
        ### for every missing trading day in CN market, there will be only "close" whcih equals to close_pre




        return wind_obj_0



    def data_wind_local(self,symbols,date_start,date_end,path,type_wsd='week') :
        # get data using local wind data  
        # 注意：获得的数据可能是以之后的某个最新日期前复权的。
            
        wind_obj_0 =1 

        return wind_obj_0

    def load_quotes(self,config_IO_0,code,date_start,date_end,quote_type='CN_day') :
        ### Function import stock daily forawrd quotations .
        # last 190414 
        # Qs：000001.SZ在 20070601-20070620之间停牌，好奇这个模块是否能返回正确的quote？
        
        # we assume quotes already in local driectory.
        # first load from local directory 
        class quotation:
            def __init__(self):
                self.indicator_name = indicator_name

        date_start = date_start.replace('-','')
        date_end = date_end.replace('-','')
        import json
        import os 
        # print( type(config_IO_0) )  dict  

        if quote_type == 'CN_day':
            ### Method 1: D:\\db_wind\\Wind_000001.SZ_updated.csv 
            path_wind = self.path0
            file_csv = "Wind_"+ code + "_updated.csv" # code= "000001.SZ"
            # columns=  Unnamed: 0 DATE OPEN      HIGH       LOW     CLOSE  VOLUME  AMT    PCT_CHG
            
            if os.path.exists( path_wind+ file_csv ) :

                code_df =pd.read_csv( path_wind +file_csv )
                # print( "code_df")
                # in case date value is in "Unnamed: 0"
                if 'DATE' in code_df.columns :
                    code_df['DATE'] = code_df['DATE'] 
                else :
                    code_df['DATE'] = code_df['Unnamed: 0'] 
                code_df['date'] = code_df['DATE'] 
                code_df['open'] = code_df['OPEN'] 
                code_df['close'] = code_df['CLOSE']  
                code_df['high'] = code_df['HIGH']  
                code_df['low'] = code_df['LOW']  
                code_df['volume'] = code_df['VOLUME']  
                code_df['amt'] = code_df['AMT']   
                code_df = code_df.drop(['DATE','OPEN','CLOSE','HIGH','LOW','VOLUME','AMT'],axis =1 )
                # head file 
                code_head = {}
                code_head["symbols"] = code
                code_head["id"] = file_csv
                code_head["items"] = ["open", "high", "low", "close", "volume", "amt", "turn"]
                code_head["date_start"] = date_start
                code_head["date_end"] = date_end
                code_head["para"] = "PriceAdj=F", "error_code"
                code_head["fields"] = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "AMT", "TURN"] 

            else :
                ##################################################################
                ### First try to check local file || download from wind api      
                symbols = code 
                file_json = symbols + '_day_' +date_start + '_' +date_end +'.json'
                path0 = config_IO_0['path_data']

                if not os.path.exists( path0 +file_json ) :
                    #  download from wind api   
                    wind_obj_0 = self.data_wind_wsd(symbols,date_start,date_end,type_wsd='day')
                    with open( path0 +file_json, 'w') as f: 
                        json.dump(wind_obj_0.wind_head ,f)   

                    file_csv =  symbols + '_day_' +date_start + '_' +date_end +'.csv'
                    wind_obj_0.wind_df.to_csv( path0 +file_csv)

                    code_head = wind_obj_0.wind_head
                    code_df = wind_obj_0.wind_df 
                else :
                    ##################################################################
                    ### If fail,check if we have local quotation file
                    # for code in sp_df['code'] :
                    # 000001.SZ_day_20140531_20141130
                    
                    # multi-codes with multi-indicators is not supported  
                    #Qs:  TypeError: string indices must be integers
                    # ana:会不会是1个斜杠导致的问题
                    # path_json = (config_IO_0['path_data']+file_json).replace("\\","\\\\")
                    # code_head = pd.read_json( path_json )
                    # symbols = code #  '600036.SH' 
                    with open( path0 +file_json, 'r') as f: 
                        code_head = json.loads(f.read())   
          
                    file_csv =  symbols + '_day_' +date_start + '_' +date_end +'.csv'
                    # path_csv = (config_IO_0['path_data']+file_csv).replace("\\","\\\\")
                    code_df =pd.read_csv( path0 +file_csv )
                    # open  high    low close   volume  amt turn
        elif quote_type == 'US_day':
            ### Method 1: D:\\db_wind\\quotes_us\\ + AAPL.O_day_20140101_20181224.csv
            path_wind = self.path0
            file_csv = code + "_day_20140101_20181224.csv"  

            code_df =pd.read_csv( path_wind +file_csv )
            # print( "code_df")
            # open  close   volume  date
            
            # head file 
            code_head = {}
            code_head["symbols"] = code
            code_head["id"] = file_csv
            code_head["items"] = ["open", "close", "volume",'date']
            code_head["date_start"] = date_start
            code_head["date_end"] = date_end
            code_head["para"] = "PriceAdj=F", "error_code"
            code_head["fields"] = ["OPEN", "CLOSE", "VOLUME"] 





        return code_head,code_df

'''
find Wind2Csv at get_wind.py
'''
