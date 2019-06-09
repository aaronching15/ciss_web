# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
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


# 1，逻辑：数据的获取应该有专属的 data_head，包括数据文件夹内数据的各种参数，方便未来重复使用
    # data_df,数据大表
# step:1,get data;2,save data to csv 
#    3,load data,4,
class data_wind():
    def __init__(self, db_name_win='',path0='D:\\db_wind\\'):
        # 2 data types: head and df 
        self.db_name_win = 'db_name_win' 
        self.path0 = path0

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


    def Wind2Csv(self, WindData,file_path0,code  ):
        # file_path0=  D:\data_Input_Wind
        # last 181117 | since 170505
        # dervied from rC_Data_Initial.py  

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