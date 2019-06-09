# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
功能：对已有的股票基本面数据进行读取或输出，合并拆分，转换等操作。
Menu



数据来源： Wind
last update 181018 | since  181018
derived from   get_wind.py\class data_json_rc_head()
===============================================
'''
import pandas as pd
import numpy as np
import json
import time
import datetime as dt


class funda_wash() :
    # last | since 181013 
    # 建立把基本面数据集合的头文件。
    # generate head file for a group of fundamental time series
    # todo：用head文件包括核心变量和参数，如"years","items"
    # head class for class data_json_rc 
    def __init__(self ):
        self.info = "This class has following objects: \n[countries,country,items,temp_columns,time_stamp_input,years,mmdd,index_list,path,file_name]"
        # country
        self.countries = ['CN']
        self.country = 'CN'
        self.items = ['industry_gicscode','west_avgroe_FY2','west_netprofit_CAGR','longcapitaltoinvestment','west_avgoperatingprofit_CAGR','turnover_ttm','employee_tech','employee_MS']
        self.temp_columns = ['date','rptDate','time_getwind'] + self.items
        self.time_stamp_input = "20180923" # timestamp is the day we get 
        # 沪深市场企业员工学历和岗位分类从2013年开始披露，因此2011,2012的数据可能无法使用。
        # Before 190411
        # self.years = [2013,2014,2015,2016,2017,2018] 
        # Sine 190411 
        self.years = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018] 
        self.mmdd_date =  ['05-31','11-30']
        self.mmdd_report =  ['12-31','06-30']
        self.index_list = ['000300.SH','000905.SH','000852.SH'] 
        # Before 190411
        # self.path = "C:\\zd_zxjtzq\\RC_trashes\\temp\\keti_sys_stra_24h\\p1\\wind_data\\funda\\"
        # Sine 190411 
        self.path = "D:\\db_wind\\wind_data\\"
        # date could be different, but time_stamp_input need to be the same  
        self.file_name = "funda_country_index_date_"+ self.time_stamp_input+ "_head.json"
        # generate json file 
        self.Gen_json()
 
    def Gen_json(self):
        # self代表类的实例，通过self访问实例对象的变量和函数
        # generate json head file include all fundamental time series data.
        import json

        temp_name_json_head =self.path + self.file_name
 
        temp_dict = {}
        temp_dict['info'] = self.info
        temp_dict['countries'] = self.countries
        temp_dict['country'] = self.country
        temp_dict['items'] = self.items
        temp_dict['temp_columns'] = self.temp_columns
        temp_dict['time_stamp_input'] = self.time_stamp_input
        temp_dict['years'] = self.years
        temp_dict['mmdd_date'] = self.mmdd_date
        temp_dict['mmdd_report'] = self.mmdd_report
        temp_dict['index_list'] = self.index_list
        temp_dict['path'] = self.path
        temp_dict['file_name'] = self.file_name

        # temp_dict['data_csv'] = temp_pd_wind.to_json(orient="columns")
        print('temp_dict \n',temp_dict )
        with open(temp_name_json_head,"w",encoding="utf-8") as f:
            # output dict. to json 
            f.write(json.dumps(temp_dict) )
        f.close()

    def Get_json_groupdata(self,list_dates,list_items,file_path_funda,time_stamp_input="20180923",output_type='dict'):
        # last 181017 |since 181017
        # import and merge json files into one dict or pd type
        # except from financial indicator data,also return zscore data for symbol list
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
        if output_type=='dict' :
            pass
        elif output_type=='pandas':
            pass

        # step 1 import json file 
        # Before 20190412_0028, years starts from 2013
        # file_name = "funda_country_index_date_"+ time_stamp_input+ "_head.json"
        # since 190412
        file_name = "funda_country_index_date_1904_"+ time_stamp_input+ "_head.json"

        # 由于不是table形式，不适合用pd.read_json()导入,用 json
        import json
        # json.dumps    将 Python 对象编码成 JSON 字符串|json.loads  将已编码的 JSON 字符串解码为 Python 对象
        # json_head = json.loads(file_name) # 直接这样会报错
        with open(file_path_funda +file_name, "r", encoding='utf-8') as f:
            result = json.loads(f.read())
            f.seek(0)
            bb = json.load(f)    # 与 json.loads(f.read())
        # print(result)

        if list_dates=='' and list_items =='' :
            # we want all times and items from _head.json file
            list_dates== []
            list_items =[]
            print( 'temp_item' )
            # create a new data class based on length of result['index_list']
            class datagroup_rc( ) :
                # create new big tables for multiple json file
                # source data_json_rc = self.Load_funda_csvJson()
                def __init__(self, result):
                    self.items = result['items']
                    self.index_list = result['index_list']
                    self.datagroup = {}
                    self.info = "This class has 5by"+ str(len(self.index_list)) + " data groups. \ntype of [datagroup,info,index_list,date_list,years,mmdd_date] is \n[pd,str,list,list,list,list]\n A typical table form is datagroup[temp_index+'_'+temp_date]  "
                    self.index_list = result['index_list']
                    self.years = result['years']
                    self.mmdd_date = result['mmdd_date']
                    self.date_list = []
                    for temp_index in self.index_list :  
                        for temp_year in result['years'] :
                            for temp_mmdd in result['mmdd_date'] :
                                if temp_mmdd == '05-31' :
                                    temp_rptDate = str(temp_year-1) +'1231'
                                elif temp_mmdd == '11-30' :
                                    temp_rptDate = str(temp_year) +'0630'
                                temp_date = str(temp_year) +'-'+ temp_mmdd # '2018-05-31' or '20180531' 都可以
                                if not temp_date == "2018-11-30" :
                                    # we want to avoid 20181130 before 20181130
                                    self.date_list = self.date_list + [temp_date]
                                    # initialize symbol list of temp_index at given temp_date
                                    self.datagroup[temp_index+'_'+temp_date] = pd.DataFrame()
                    
                def add_zscore(self,temp_index='',temp_date='',result={}) : 
                    # add zscore value for all dataframe in self.datagroup
                    # last 181018 | since 181018
                    '''
                    scikit-learn的模型中都是假设输入的数据是数值型的，并且都是有意义的，如果有缺失数据是通过NAN，或者空值表示的话，就无法识别与计算
                    要弥补缺失值，可以使用均值，中位数，众数等等。Imputer这个类可以实现
                    from sklearn import preprocessing 
                    注意，有的item数值越大越好，有的越小越好。我们不希望用'mean'的原因是数值不全的企业往往在这方面存在缺陷。
                    imp = preprocessing.Imputer(missing_values='NaN', strategy='mean', axis=0) | strategy='mean',median,most_frequent
                    sklearn.preprocessing.scale()函数，可以直接将给定数据进行标准化。
                    Input contains NaN, infinity or a value too large for dtype('float64')
                    ["industry_gicscode", "west_avgroe_FY2", "west_netprofit_CAGR", "longcapitaltoinvestment", "west_avgoperatingprofit_CAGR", "turnover_ttm", "employee_tech", "employee_MS"]
                    "west_avgroe_FY2", 
                    "west_netprofit_CAGR", 
                    "longcapitaltoinvestment", 
                    "west_avgoperatingprofit_CAGR", 
                    "turnover_ttm", 
                    "employee_tech", 
                    "employee_MS"
                    todo assume all indicators are the lager, the better,so we can replace all NaN with min values
                    A_raw.fillna( A_raw.min())[col1,col2] ,与pad相反，bfill表示用后一个数据代替NaN '''
                    items_list = self.items
                    # create datagroup_zscore if it does not exists.
                    self.datagroup_zscore = {}
                    # all items except wind code for industry
                    if result == {} : 
                        # we only need to calculate a single table 
                        df_temp = self.datagroup[temp_index+'_'+temp_date]
                        # print('df_temp \n ', df_temp.head() ) 
                        df_raw = df_temp[ items_list[1:] ]
                        df_raw = df_raw.fillna( df_raw.min() )  # min for every columns
                        
                        # A_raw  = pd.DataFrame([[6,22,3],[6,4,15],[6,7,8]])
                        # 是按照每个column来算的。 |test case pd.DataFrame([[6,22,3],[6,4,15],[6,7,8]])
                        # A1=preprocessing.scale(A_raw) | 和下边等价
                        '''for col in cols:
                        col_zscore = col + '_zscore'
                        df[col_zscore] = (df[col] - df[col].mean())/df[col].std(ddof=0) '''
                        zscore = lambda x: (x-x.mean())/x.std()
                        df_zs= df_raw.transform(zscore)
                        # add  industry_gicscode  and code
                        df_zs[items_list[0 ] ]=df_temp[items_list[0 ] ] 
                        df_zs['code']= df_temp['code'] # ??? missing code, maybe need  unname:0 ??
                        # A1.mean(axis=0)) A1.std(axis=0) )  
                        # save A1 to new pd 
                        self.datagroup_zscore[temp_index+'_'+temp_date] = df_zs  
                    else : 

                        items_list = result['items']
                        for temp_index in self.index_list :  
                            for temp_date in self.date_list :
                                try:
                                    df_temp = self.datagroup[temp_index+'_'+temp_date]
                                    # print(temp_index+'_'+temp_date)
                                    # print('df_temp \n ', df_temp.head() ) 
                                    df_raw = df_temp[ items_list[1:] ]
                                    df_raw = df_raw.fillna( df_raw.min() )  # min for every columns
                                    
                                    # A_raw  = pd.DataFrame([[6,22,3],[6,4,15],[6,7,8]])
                                    # 是按照每个column来算的。 |test case pd.DataFrame([[6,22,3],[6,4,15],[6,7,8]])
                                    # A1=preprocessing.scale(A_raw) | 和下边等价
                                    '''for col in cols:
                                    col_zscore = col + '_zscore'
                                    df[col_zscore] = (df[col] - df[col].mean())/df[col].std(ddof=0) '''
                                    zscore = lambda x: (x-x.mean())/x.std()
                                    df_zs= df_raw.transform(zscore)
                                    # add  industry_gicscode  and code
                                    df_zs[items_list[0 ] ]=df_temp[items_list[0 ] ] 
                                    df_zs['code']= df_temp['code'] # ??? missing code, maybe need  unname:0 ??
                                    # A1.mean(axis=0)) A1.std(axis=0) )  
                                    # save A1 to new pd 
                                    self.datagroup_zscore[temp_index+'_'+temp_date] = df_zs  
                                except:
                                    # 000852.SH have values after 2014-11-30 ? 
                                    print('Warning ...')
                                    print('No data in ', temp_index+'_'+temp_date )
                                    print(temp_index+'_'+temp_date)
                                    print('df_temp \n ', df_temp.head() )
                    # type of datagroup_zscore is dict with key is a '000300.SH_2013-05-31'| value is a pd 
                    if result == {} : 
                        return df_zs
                    else :
                        return self.datagroup_zscore
                    # return df_zs,self.datagroup_zscore

            ### initialize dataG class 
            temp_dataG = datagroup_rc( result )
            # notes: we do not own df in temp_dataG yet ,it is a empty class
            # print(temp_dataG.info ) 
            # temp_dataG.datagroup_zscore = temp_dataG.add_zscore('','',result)
            
            ### we first need to add raw fundamental data 
            count = 0 
            time_stamp = result['time_stamp_input'] # '20180923'

            for temp_year in result['years'] :
                # print("190412=== ",temp_year )


                for temp_mmdd in result['mmdd_date'] :
                    if temp_mmdd == '05-31' :
                        temp_rptDate = str(temp_year-1) +'1231'
                    elif temp_mmdd == '11-30' :
                        # we have get funda data for 2018-11-30
                        if not temp_year == '2018': 
                            temp_rptDate = str(temp_year) +'0630'
                    temp_date = str(temp_year) +'-'+ temp_mmdd # '2018-05-31' or '20180531' 都可以 
                    
                    if not temp_date == '2018-11-30'  : 
                        # we have get funda data for 2018-11-30
                        for wind_code in result['index_list'] :
                            if wind_code in ['000300.SH','000905.SH' ] :
                                print('The '+str(count) +' '+ temp_date+' '+wind_code +' is working ... ')                            
                                data_json_rc = self.Load_funda_csvJson(file_path_funda,temp_date,time_stamp,wind_code,'CN') 
                                # concate csv or dict in data_json_rc   
                                # add column 'code'
                                if "Unnamed: 0" in data_json_rc.file_csv.columns :
                                    data_json_rc.file_csv.rename(columns={"Unnamed: 0":'code'})
                                    data_json_rc.file_csv.drop(["Unnamed: 0"], axis=1)
                               
                                # Generate symbol list of temp_index at given temp_date
                                temp_dataG.datagroup[wind_code+'_'+temp_date] = data_json_rc.file_csv 
                                # print( temp_dataG.datagroup[wind_code+'_'+temp_date].head() )                         
                                count += 1                          

                            if wind_code == '000852.SH' and (temp_year >=2015 or (temp_date == '2014-11-30') ) : 
                                print('The '+str(count) +' '+ temp_date+' '+wind_code +' has been done ')
                                data_json_rc = self.Load_funda_csvJson(file_path_funda,temp_date,time_stamp,wind_code,'CN')
                                # add column 'code'
                                if "Unnamed: 0" in data_json_rc.file_csv.columns :
                                    data_json_rc.file_csv.rename(columns={"Unnamed: 0":'code'})
                                    data_json_rc.file_csv.drop(["Unnamed: 0"], axis=1)

                                # Generate symbol list of temp_index at given temp_date
                                temp_dataG.datagroup[wind_code+'_'+temp_date] = data_json_rc.file_csv 
                                # print( temp_dataG.datagroup[wind_code+'_'+temp_date] )   
                                # concate csv or dict in data_json_rc 
                                count +=1 

            ### now we transform them into zscore df 
            # get zscore data 
            # choice 1: we only return a pd for fixed index and date 
            # df_zs = temp_dataG.add_zscore(wind_code,temp_date,{})
            # choice 2: we return all pd in a dict for all fixed index and date 
            # dataG_zs = temp_dataG.add_zscore('','',result)
            temp_dataG.datagroup_zscore = temp_dataG.add_zscore('','',result)
            # temp_dataG.datagroup_zscore 
            # wind_code = '000300.SH'
            # temp_date = '2014-05-31'
            # print('dataG_zs: \n ',dataG_zs )
            # df_zs = dataG_zs[wind_code+'_'+temp_date]
            # print('df_zs \n',df_zs  )
            
            # print( temp_dataG.datagroup_zscore[wind_code+'_'+temp_date] ) 

        ################################################################

          

        return temp_dataG

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