# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 181105 | since  181105
Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }
    
分析：
1，时间管理
    1.1，时间频率:分钟，日，周
    1.2，exchange交易所：{SSE SZSE    INTERBANK_BM    HKEX    NASDAQ  NYSE}
        SSE：上海交易所 
        SZSE：深圳交易所    
        INTERBANK_BM：中国银行间债券市场    
        HKEX：香港交易所    
        NASDAQ：纳斯达克  
        NYSE：纽约交易所
    1.3，country 国家或地区,未来可能对应多个货币，主要是法律法规，风俗等方面遵从当地规则
        CN: 中国
        CNHK: 中国香港
        US: 美国

2，配置文件 | config\config_times.py

3,input file 
    3.1，weekly times for 6 main financial markets 
    3.2，daily times for 6 main financial markets 
4,downstream user modules:
    4.1, ..db\accounts.py 

Notes: 
refernce: rC_Portfolio_17Q1.py 
===============================================
'''
import numpy as np
import pandas as pd
import datetime as dt 

###################################################
class times():
    def __init__(self,country='CN',exchange='SSE'):
        self.country = country 
        self.exchange = exchange
        ### Setting path for datetime files 
        self.path_dt = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\db\\db_times\\"
        ### datetime file name  | from 20070101 to 20190708
        self.file_name_day = "times_CN_day.csv"
        self.file_name_week = "times_CN_week.csv"

    def print_info(self):
        ### print all modules for current clss

        print("gen_dates_week: 给定起始日，生成交易周数据")
        print("gen_dates_day: 给定起始日，生成交易日数据")
        print("change_datetime_format:按照给定日期格式将datetime转为str,或str转为datetime. derived from:get_time_format")
        print("update_tday_list : 根据最新交易日，更新现有日期文件 ")
        print(" ")
        print(" ")

        return 1 

    def gen_dates_week(self,country='CN',exchange='SSE',init_date='2014-05-31',if_start=0) :
        # if_start = 0 means init_date is the start date and 1 means it is the end date
        # generate data list using initial date or end date 
        # format if time : "2012-01-01"
        # Before 190412
        # path0= "C:\\zd_zxjtzq\\RC_trashes\\temp\\sys_stra_24h\\CISS_rc\\db\\db_times\\"
        # After 190412
        path0= self.path_dt 

        filename = "times_CN_week_20120101_20181102.csv"

        temp_df = pd.read_csv(path0+filename)
        # temp_df1 is a series, not df 
        temp_series = pd.to_datetime( temp_df[exchange ])
        # notice that init_date might not be a valid trading day 
        dates =  temp_series[ temp_series>=init_date ]

        # change string date to datetime and get what we want 

        return dates 
    def gen_dates_day(self,country='CN',exchange='SSE',init_date='2014-05-31',if_start=0) :
        # if_start = 0 means init_date is the start date and 1 means it is the end date
        # generate data list using initial date or end date 
        # format if time : "2012-01-01"
        path0= self.path_dt 
        # before 190412
        # filename = "times_CN_day_20120101_20181102.csv"
        # After 190412
        filename = "times_CN_day_20070101_20190412.csv"

        temp_df = pd.read_csv(path0+filename)
        # temp_df1 is a series, not df 
        temp_series = pd.to_datetime( temp_df[exchange ])
        # notice that init_date might not be a valid trading day 
        dates =  temp_series[ temp_series>=init_date ]

        # change string date to datetime and get what we want 
        return dates 

    def change_datetime_format(self, date_list ,type_input='str', format0='%Y%m%d') :
        ### return string date format using datetime module
        # previous function name: get_time_format
        # last | since 190717
        import datetime as dt 
        if format0 =='' :
            format0='%Y%m%d'

        if len( date_list  ) == 1 :
            if type_input == 'dt' :
                date_list = dt.datetime.strftime(date_list,format0)
            elif type == 'str' :
                date_list = dt.datetime.strptime(date_list,format0)
        elif len( date_list  ) > 1 :
            date_list2 = []
            if type_input == 'dt' :
                for temp_date in date_list :
                    date_list2 = date_list2 +[ dt.datetime.strftime(temp_date,format0) ]

            elif type == 'str' :
                for temp_date in date_list :
                    date_list2 = date_list2 +[ dt.datetime.strptime(temp_date,format0) ]
            date_list = date_list2

        return date_list

    def update_tday_list(self,date_start,date_end,mkt="SSE") :
        ### 根据最新交易日，补全和更新现有日期文件
        ### 默认是上海交易所，即 SSE
        # notes：由于节假日变更，自然灾害如台风等因素，当年内的交易日有可能发生变动，需要每个季度调整
        # note:若导入的日期原始值为"20190701"，则导入值可能为"20190701.0","float". 
        # note:若导入的日期原始值为"2019-07-01"，则需要 str1.replce("-","")
        # | since 190717
        if mkt =="" :
            mkt= "SSE"

        ### step 1 | import date file 
        date_list_raw = pd.read_csv( self.path_dt+ self.file_name_day)
        ### chagne type as string in case float 

        date_list = date_list_raw[mkt].dropna().astype("str")
        ### date_list_raw.loc[len_SSE,'SSE'] 对应最后一个值
        len_SSE = len( date_list )

        date_list = date_list.apply(lambda x : x.replace(".0",""))
        date_list = list( date_list )
        ### step 2 | compare latest date 
        import datetime as dt 
        # 默认是str格式的日期数据
        date_list_end = dt.datetime.strptime(date_list[-1], '%Y%m%d' )
        date_end_dt =   dt.datetime.strptime(date_end, '%Y%m%d' )
        date_list_start = dt.datetime.strptime(date_list[0], '%Y%m%d' )
        date_start_dt=   dt.datetime.strptime(date_start, '%Y%m%d' )

        ### forward 
        if date_end_dt > date_list_end :
            from db.db_assets.get_wind import wind_api
            wind_api_1 = wind_api()
            ### Need to get missing trading dates from wind-api
            # forward | type is datetime 
            date_list_f = wind_api_1.Get_tdays( date_list[-1] ,date_end, mkt )
            
            date_list_f = self.change_datetime_format(date_list_f ,'dt', '')
            
            ### Add forawrd date list to current list
            # s1 get last index 
            len_old = len(date_list)
            # get rid of the first item
            # for temp_i in range( len(date_list_f[1:]) ) :
            #     date_list_raw.loc[len_old+ temp_i ,mkt ] = date_list[temp_i+1]
            print("len_old",len_old)
            print("len_date_list_f",len(date_list_f) )
            print("len_date_list",len(date_list) )

            date_list = date_list + date_list_f[1:]
            len_new= len(date_list) 
            print("len_date_list",len(date_list) )
            
            date_list_raw.loc[0:len_new-1,mkt ] = date_list
            # save to csv with index 
            date_list_raw.to_csv( self.path_dt+ self.file_name_day ,index=None)

        ### backward case 
        if date_start_dt < date_list_start :
            from db.db_assets.get_wind import wind_api
            wind_api_1 = wind_api()
            ### Need to get missing trading dates from wind-api
            # forward | type is datetime 
            date_list_b = wind_api_1.Get_tdays( date_start,date_list[0], mkt )
            ### Add backawrd date list to current list
            date_list = date_list_f[:-1] + date_list 
            len_new= len(date_list) 
            date_list_raw.loc[0:len_new-1,mkt ] = date_list
            # save to csv with index 
            date_list_raw.to_csv( self.path_dt+ self.file_name_day ,index=None)
            


        return date_list 

    ################################################################################
    def get_port_rebalance_dates(self,date_start,date_end,method='stock_index_csi'  ):
        # get rebalance dates of portfolio using given start and end dates 

        date_start=date_start.replace("-",'') # 20171231
        date_end = date_end.replace("-",'') # 20181115

        mmdd_start= date_start[:4]
        mmdd_end = date_end[:4]

        date_start= dt.datetime.strptime( date_start, "%Y%m%d")
        date_end = dt.datetime.strptime( date_end, "%Y%m%d")

        ### get start year and end year 
        date_start0= dt.datetime.strptime( str(date_start.year), "%Y")
        date_end0= dt.datetime.strptime( str(date_end.year), "%Y")
        
        date_list0 =[]
        if date_end.year > date_start.year :
            # without +1, there will be no date_end.year 
            for year_i in range(date_start.year, date_end.year+1 ) :
                if not year_i == date_start.year :
                    for mmdd in ['0531','1130'] :
                        temp_date = str(year_i) + mmdd 
                        temp_datetime = dt.datetime.strptime( temp_date, "%Y%m%d")
                        date_list0 = date_list0 + [temp_datetime]
                else :
                    # the first year 
                    # 3 cases for date_start [x,0531,x,1130,x]
                    
                    temp_1130 = dt.datetime.strptime( str(year_i) + '1130' , "%Y%m%d")
                    temp_0531 = dt.datetime.strptime( str(year_i) + '0531' , "%Y%m%d")
                    if date_start >= temp_1130 :
                        date_list0 = date_list0 + [temp_1130]
                    elif date_start < temp_0531 :
                        temp_0531_pre = dt.datetime.strptime( str(year_i-1) + '1130' , "%Y%m%d")
                        date_list0 = date_list0 +[temp_0531_pre]+ [temp_0531] +[temp_1130]
                    else :
                        date_list0 = date_list0 + [temp_0531] +[temp_1130]
        else :
            # same year or error  
            for mmdd in ['0531','1130'] :
                temp_date = str( date_start.year ) + mmdd 
                temp_datetime = dt.datetime.strptime( temp_date, "%Y%m%d")
                date_list0 = date_list0 + [temp_datetime]

        date_list0 = pd.to_datetime(date_list0  )

        date_list = date_list0[ date_list0 >= date_start ]
        date_list = date_list0[ date_list0 <= date_end ]

        # print("date_list")
        # print( date_list )

        # return date_list

        # if dif year < 1 :
        #     # in the same year or end date earlier than start date
        #     if dif_month < 0 :
        #         print("Error: end date earlier than start date.")
        #     else :
        #         # three place for date_end in [pre_1130, p1,0531,p2, 1130,p3]
        #         if date_end.month >= 12 and mmdd_end !='1130' :
        #             # [pre_1130, 0531, 1130, end]
        #             if date_start.month >= 12 and mmdd_start !='1130':
        #                 # [pre_1130, 0531, 1130,start, end]
        #                 # only 1 period , 05-31
        #                 date_list = [ str( date_end.year)+'1130'   ]
        #             elif date_start.month < 6  and  mmdd_start !='0531':
        #                 # [pre_1130, start,0531, 1130, end]
        #                 # 3 periods : 1130 in previous year
        #                 date_list = [ str( date_end.year-1)+'0531'   ] 
        #                 date_list =date_list + [str( date_end.year)+'0531' ]
        #                 date_list =date_list + [str( date_end.year)+'1130']
        #             else :
        #                 # [pre_1130, start,0531, 1130, end]
        #                 date_list =[str( date_end.year)+'0531' ]
        #                 date_list =date_list + [str( date_end.year)+'1130']
        #         elif date_end.month < 6 and mmdd_end !='0531':
        #             # [pre_1130, end,0531, 1130]
        #             # only one case 
        #             # [pre_1130, start,end,0531, 1130]
        #             date_list = [ str( date_end.year-1)+'0531'   ] 
        #         else :
        #             # [pre_1130, 0531, end,1130] | 2 cases 
        #             if date_start.month < 6  and  mmdd_start !='0531':
        #                 # [pre_1130,start, 0531, end,1130]
        #                 date_list = [ str( date_end.year-1)+'0531'   ] 
        #                 date_list =date_list + [str( date_end.year)+'0531' ]
        #             else :
        #                 # [pre_1130,0531,start,  end,1130]
        #                 date_list = [str( date_end.year)+'0531' ]
        # if dif = 1 :

        # get rebalance dates of portfolio using given start and end dates 
        periods_reference_change = [] # referenc date for fundamental change
        periods_start = []
        periods_end = []

        for temp_i in range( len(date_list) ) :
            # 3 cases: beginning, middle, end period
            
            if temp_i == 0 :
                # first period
                periods_reference_change =periods_reference_change + [ date_list[0] ] 
                periods_start =periods_start + [date_start]
                periods_end =periods_end + [ date_list[1] ] 

            elif temp_i < len(date_list)-1 :
                # middle period 
                periods_reference_change =periods_reference_change + [ date_list[temp_i] ] 
                periods_start =periods_start + [date_list[temp_i]]
                periods_end =periods_end + [ date_list[temp_i+1] ] 

            else :
                # last period  | temp_i == len(date_list)-1
                periods_reference_change =periods_reference_change + [ date_list[temp_i] ] 
                periods_start =periods_start + [ date_list[temp_i] ]
                periods_end =periods_end + [ date_end  ] 

        class date_periods : 
            def __init__(self,periods_reference_change,periods_start,periods_end) :
                self.periods_reference_change = periods_reference_change 
                self.periods_start = periods_start
                self.periods_end = periods_end


        date_periods = date_periods(periods_reference_change,periods_start,periods_end)
        
        return date_periods



