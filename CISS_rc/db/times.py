# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 230413 | since 181105

3,时间数据：file=date_trade.xlsx;保存日、周、月、季度、年末交易日
    path= ..\ciss_web\CISS_rc\db\db_times  
    
分析：
1，时间管理
    1.1，时间频率:分钟，日，周
    1.2，exchange交易所：{SSE SZSE    INTERBANK_BM    HKEX    NASDAQ  NYSE}
        SSE：上海交易所 ; SZSE：深圳交易所 ;  INTERBANK_BM：中国银行间债券市场    
        HKEX：香港交易所;NASDAQ：纳斯达克  ;NYSE：纽约交易所
    1.3，country 国家或地区,未来可能对应多个货币，主要是法律法规，风俗等方面遵从当地规则
        CN: 中国
        CNHK: 中国香港
        US: 美国

2，配置文件 | config\config_times.py
 
Notes: 
refernce:  
===============================================
'''
import sys,os 
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )
import pandas as pd
import numpy as np 
import json 
sys.path.append("..") 

import datetime as dt 

###################################################
class times():
    def __init__(self,country='CN',exchange='SSE'):
        ###################################################
        ### 导入config配置地址等
        from config_data import config_data
        config_data1 = config_data()
        self.path_dict = config_data1.obj_config["dict"] 
        self.path_ciss_web = self.path_dict["path_ciss_web"]
        self.path_ciss_rc = self.path_dict["path_ciss_rc"]
        self.path_db = self.path_dict["path_db"] 
        self.path_db_times = self.path_dict["path_db_times"]
        self.path_db_assets = self.path_dict["path_db_assets"]
        
        ################################################
        ### 导入数据地址          
        self.path_data_pms = self.path_dict["path_data_pms"]
        self.path_data_adj = self.path_dict["path_data_adj"]
        self.path_fundpool = self.path_dict["path_fundpool"]
        self.path_wind_terminal = self.path_dict["path_wind_terminal"]  
        self.path_wsd = self.path_dict["path_wsd"] 
        self.path_wss = self.path_dict["path_wss"] 
        self.path_wpf = self.path_dict["path_wpf"] 
        self.path_wpd = self.path_dict["path_wpd"] 
        ### 
        self.nan = np.nan 

        ### 
        self.path_dt = self.path_ciss_rc + "db\\db_times\\"
        ### path= C:\rc_2023\rc_202X\ciss_web\CISS_rc\db\db_times
        self.file_name_date = "date_trade.xlsx"

    def print_info(self):
        ###################################################
        ### 交易日、周、月、季度数据维护；file=date_trade.xlsx, 备份=date_trade_backup.xlsx
        print("manage_date_trade |维护A股或其他交易所交易日期：日、周、月、季、年 ")

        ###################################################
        ### 获取交易日前后定期的日期，如前后1m,3m,6m,12m月末
        print("get_date_pre_post | 给定交易日，获取之前或之后固定间隔的日期、季末日期")

        ###################################################
        ### Calculate datetime 
        print("get_date_delta |给定2个日期，计算相差的年、月或日 ")
        
        ###################################################
        ### 以下都是2018-11后不再用的功能
        print("get_dates_monthend | todo 将日期改为月末交易日或月末日期 ")
        print("gen_dates_week: 给定起始日，生成交易周数据")
        print("gen_dates_day: 给定起始日，生成交易日数据")
        print("change_datetime_format:按照给定日期格式将datetime转为str,或str转为datetime. derived from:get_time_format")
        print("update_tday_list | 根据最新交易日，更新现有日期文件 TODO！！ ")
        
        
        print(" ")

        return 1 
    
    #########################################################################################
    def manage_date_trade(self,obj_dt={} ):
        ### 维护A股或其他交易所交易日期：日、周、月、季、年
        # reference: def update_tday_list()
        exchange = "SSE"
        import datetime as dt  
        ###################################################
        ### 获取最新日期和6个月后日期
        date_latest = dt.datetime.now()
        date_latest_str = dt.datetime.strftime( date_latest, "%Y%m%d")
        
        ###################################################
        ### 导入日期数据，匹配交易所，获取最新日期
        file_name = "date_trade.xlsx"
        df_date = pd.read_excel( self.path_dt +file_name,sheet_name="date_trade"  )
        
        ### 只需要上交所的交易日数据！
        df_date = df_date[ df_date["exchange"] == exchange ]
        df_date = df_date[ df_date["type_date"] == "d" ]

        ### type of df_date["date"] : int 
        date_latest_df_str = df_date["date"].max()
        
        ###################################################
        ### str --> dt 
        date_latest_df = dt.datetime.strptime( str(date_latest_df_str), "%Y%m%d") 
        print("date_latest , date_latest_df", date_latest , date_latest_df ) 

        time_diff = date_latest - date_latest_df
        # type(time_diff.days ) = int 
        if time_diff.days > -30 :
            ### == -30 意味着还有30天已有的交易日记录就不够用了
            ### 获取6个月后的日期
            obj_date = self.get_date_pre_post()
            date_post_6m_str = obj_date["date_post_6m_str"] 

            ###################################################
            ### 用wind-api获取区间的交易日
            obj_dt = {}
            obj_dt["date_latest"] = date_latest
            ### dt --> str 
            obj_dt["date_begin"] = dt.datetime.strftime( date_latest_df+ dt.timedelta(days =1)  , "%Y%m%d") 
            ### format of date_post_6m_str = "%Y%m%d"
            obj_dt["date_end"] = date_post_6m_str

            import sys,os
            # 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
            path_ciss_web = os.getcwd().split("CISS_rc")[0] 
            sys.path.append( self.path_ciss_rc + "db\\db_assets\\" )

            from get_wind_api import wind_api 
            wind_api_1 = wind_api()
            obj_dt = wind_api_1.get_tdays( obj_dt)
            
            ''' obj_dt["df_dt"]=
                      date  date_str  date_int
            2022-09-05 2022-09-05  20220905  20220905
            2022-09-06 2022-09-06  20220906  20220906
            2022-09-07 2022-09-07  20220907  20220907 
            '''
            
            ###################################################
            ### 将交易日数据存入df 
            obj_dt["df_dt"]["exchange"] = exchange
            df_dt = obj_dt["df_dt"]
            df_dt = df_dt.loc[:,["exchange","date_int"]]
            df_dt = df_dt.rename(columns={"date_int":"date"})

            df_dt["type_date"] = "d"
            ###################################################
            ### append df_dt to df_date 
            df_date = df_date.append( df_dt.loc[:,["exchange","date","type_date" ] ] ,ignore_index=True  )
            df_date = df_date.sort_values(by="date", ascending=True)
            df_date = df_date.reset_index(drop=True ) 

            ### save to excel 
            df_date.to_excel(self.path_dt +file_name,sheet_name="date_trade",index=False )
        else :
            obj_dt = {}
            obj_dt["date_latest"] = date_latest
            obj_dt["date_latest_str"] = date_latest_str
            obj_dt["date_end"] = "No need to update"
        #############################################################################################
        ### Calculate 日期计算
        #############################################################################################
        ### 根据全部交易日，计算周、月、季、年末日期
        # notes:由于之前日期可能处于1个月的中间，因此不能直接用新的部分时间序列计算新增的月末日期
        df_date = df_date[ df_date["exchange"] == exchange ]
        df_date["date_dt"] = df_date["date"].apply(lambda x : dt.datetime.strptime( str(x) ,"%Y%m%d" ))

        #############################################################################################
        ### df_date_w 计算每周最后一个交易日
        # weekday对应0-6，isoweekday对应1-7，isocalendar返回数组：年，周号

        df_date["num_date"] = df_date["date_dt"].apply(lambda x : x.isoweekday() )
        ### 日期升序排列
        df_date =df_date.sort_values(by="date", ascending=True)
        df_date = df_date.reset_index(drop=True) 
        index_max = df_date.index.max()
        ### 
        list_index = []
        for temp_i in df_date.index :
            temp_i_next = temp_i +1 
            ### 由于节日因素，weekday可能不是从1开始，从5结束
            if temp_i < index_max :
                ### 继续寻找当周最后一个交易日 | =等号对应下一周就一天周五。
                if df_date.loc[temp_i,"num_date"] >= df_date.loc[temp_i_next,"num_date"] :
                    ### 说明temp_i 对应当周最后一个交易日
                    list_index = list_index + [temp_i ]
                    
        ### 加入最后一个日期
        list_index = list_index + [ index_max ]
        df_date_w = df_date.loc[list_index, : ]
        df_date_w["type_date"] = "w"

        ###################################################
        ### 计算日期属于每一年的第几周
        ### 取值范围：day:1~7 ;week:1~52; month:1~12 ; quarter:1~4 ;
        count_w = 1 
        # print( df_date_w.head() )
        df_date_w=df_date_w.reset_index(drop=True)
        index_max_w = df_date_w.index.max()
        for temp_i in df_date_w.index :
            ### 获取所属年份
            if temp_i < index_max_w : 
                temp_year = int( str(df_date_w.loc[temp_i,"date"])[:4] ) 
                temp_year_next = int( str(df_date_w.loc[temp_i+1 ,"date"])[:4] )
                if temp_year == temp_year_next :
                    df_date_w.loc[temp_i, "num_date"] = count_w
                    count_w = count_w +1 
                elif temp_year < temp_year_next :
                    ### 说明下一个index，年份要变了。
                    df_date_w.loc[temp_i, "num_date"] = count_w
                    count_w = 1 
        
        #############################################################################################
        ### df_date_m ,计算每月最后一个交易日| note:月末不一定是当周周末交易日！
        list_index = []
        for temp_i in df_date.index :
            temp_i_next = temp_i +1 
            if temp_i < index_max :
                ### 继续寻找当月月末的交易日；判断下一个交易日是否新的1个月
                ### "date": str ; "date_dt" :datetime
                month = int( str(df_date.loc[temp_i,"date"])[4:6] )
                month_next = int( str(df_date.loc[temp_i_next,"date"])[4:6] )
                if month != month_next :
                    ### 月份不等，说明temp_i 对应当月最后一个交易日
                    list_index = list_index + [temp_i ]

        ### 加入最后一个日期
        list_index = list_index + [ index_max ]
        df_date_m = df_date.loc[list_index, : ]
        df_date_m["type_date"] = "m"
        ### 计算日期属于每一年的第几月 || 取值范围： month:1~12  
        df_date_m["num_date"] = df_date_m["date"].apply(lambda x : int( str(x)[4:6] ) ) 

        ###################################################
        ### df_date_q ,计算每季度最后一个交易日，月份等于 3，6，9，12
        list_index = [] 
        df_date_m = df_date_m.reset_index(drop=True)
        index_max = df_date_m.index.max()
        for temp_i in df_date_m.index :
            temp_i_next = temp_i +1 
            if temp_i < index_max :
                ### 继续寻找当月月末的交易日；判断下一个交易日是否新的1个月
                ### "date": str ; "date_dt" :datetime
                month = int(  str(df_date_m.loc[temp_i,"date"])[4:6] )
                
                if month in [3,6,9,12] :
                    ### 月份不等，说明temp_i 对应当月最后一个交易日
                    list_index = list_index + [temp_i ]

        ### 加入最后一个日期
        list_index = list_index + [ index_max ]
        df_date_q = df_date_m.loc[list_index, : ]
        df_date_q["type_date"] = "q" 
        df_date_q = df_date_q.reset_index(drop=True)
        ###################################################
        ### 只保留4个columns: 
        df_date =df_date.append(df_date_w, ignore_index=True)
        df_date =df_date.append(df_date_m, ignore_index=True)
        df_date =df_date.append(df_date_q, ignore_index=True)
        
        
        df_date.to_excel("D:\\df_date.xlsx")
        
        df_date =df_date.loc[ : , [ "exchange","date","num_date","type_date"] ]
        obj_dt["df_dt"] = df_date

        ### save to excel 
        df_date.to_excel(self.path_dt +file_name,sheet_name="date_trade",index=False )

        return obj_dt



    #########################################################################################
    def get_date_delta(self,obj_dt):
        ### 给定2个交易日，计算相差的年限或日期
        
        date_begin = obj_dt["date_begin"]  
        date_end   = obj_dt["date_end"]  
        ### type(timedelta ) = <class 'datetime.timedelta'>
        timedelta = date_end - date_begin

        ### 取值范围"years" # or days, months
        if obj_dt["timedelta_type"] == "years" :
            obj_dt["date_delta"] = round(timedelta.days/365,4)
        elif obj_dt["timedelta_type"] == "months" :
            obj_dt["date_delta"] = round(timedelta.days/365*12,4)
        elif obj_dt["timedelta_type"] == "days" :
            obj_dt["date_delta"] = round(timedelta.days,4)
        

        return obj_dt



    def get_dates_monthend(self,obj_date):
        ### 将日期改为月末交易日或月末日期
        '''
        input:单个日期或日期列表，全部历史交易日
        todo
        '''



        return obj_date

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
        ### ！！！！！！！需要修改
        ### 根据最新交易日，补全和更新现有日期文件
        ### 默认是上海交易所，即 SSE
        # notes：由于节假日变更，自然灾害如台风等因素，当年内的交易日有可能发生变动，需要每个季度调整
        # note:若导入的日期原始值为"20190701"，则导入值可能为"20190701.0","float". 
        # note:若导入的日期原始值为"2019-07-01"，则需要 str1.replce("-","")
        # | since 190717
        if mkt =="" :
            mkt= "SSE"

        ### step 1 | import date file 
        date_list_raw = pd.read_csv( self.path_dt+ self.file_name_date)
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
            date_list_raw.to_csv( self.path_dt+ self.file_name_date ,index=None)

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
            date_list_raw.to_csv( self.path_dt+ self.file_name_date ,index=None)
            


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


    def get_date_pre_post(self, date_in="") :
        #######################################################################
        ### 给定交易日，获取之前或之后固定间隔的日期、季末日期
        import datetime as dt 
        if len( str(date_in ) ) < 8  :
            ### 获取区间日期
            import datetime as dt  
            time_0 = dt.datetime.now()
        else :
            time_0 = date_in
            if type(time_0) == str :
                time_0 = dt.datetime.strptime( time_0, "%Y%m%d")
            ### dt.datetime
        
        time_now_str = dt.datetime.strftime(time_0   , "%Y%m%d")

        #######################################################################
        ### 获取近1w、1m、3m、6m、1year日期
        ### dt.timedelta() 只能获取周、日、小时的差异
        date_pre_1w  =time_0 - dt.timedelta(weeks =1)  
        date_post_1w  =time_0 + dt.timedelta(weeks =1)  
        
        ### 月、年差异用 dateutil.relativedelta
        import dateutil.relativedelta as rd  
        date_pre_1m  =time_0 - rd.relativedelta(months=1)  
        date_pre_3m  =time_0 - rd.relativedelta(months=3)  
        date_pre_6m  =time_0 - rd.relativedelta(months=6)  
        date_pre_1y  =time_0 - rd.relativedelta(years=1)   
        date_pre_2y  =time_0 - rd.relativedelta(years=2)   
        date_pre_3y  =time_0 - rd.relativedelta(years=3)   
        date_pre_5y  =time_0 - rd.relativedelta(years=5)   
        date_pre_10y  =time_0 - rd.relativedelta(years=10)   

        date_post_1m  =time_0 + rd.relativedelta(months=1)  
        date_post_3m  =time_0 + rd.relativedelta(months=3)  
        date_post_6m  =time_0 + rd.relativedelta(months=6)  
        date_post_1y  =time_0 + rd.relativedelta(years=1)   
        date_post_2y  =time_0 + rd.relativedelta(years=2)   

        #######################################################################
        ### output 
        obj_date={}
        obj_date["date_pre_1w"] = date_pre_1w
        obj_date["date_pre_1m"] = date_pre_1m
        obj_date["date_pre_3m"] = date_pre_3m
        obj_date["date_pre_6m"] = date_pre_6m
        obj_date["date_pre_1y"] = date_pre_1y
        obj_date["date_pre_2y"] = time_0 - rd.relativedelta(years=2)  
        obj_date["date_pre_3y"] = time_0 - rd.relativedelta(years=3)  
        obj_date["date_pre_5y"] = time_0 - rd.relativedelta(years=5)  
        obj_date["date_pre_10y"] = time_0 - rd.relativedelta(years=10)  
        
        obj_date["date_post_1w"] = date_post_1w
        obj_date["date_post_1m"] = date_post_1m
        obj_date["date_post_3m"] = date_post_3m
        obj_date["date_post_6m"] = date_post_6m
        obj_date["date_post_1y"] = date_post_1y
        obj_date["date_post_2y"] = time_0 + rd.relativedelta(years=2)  

        ### 获取

        obj_date["date_pre_1w_str"] = dt.datetime.strftime( date_pre_1w, "%Y%m%d")
        obj_date["date_pre_1m_str"] = dt.datetime.strftime( date_pre_1m, "%Y%m%d")
        obj_date["date_pre_3m_str"] = dt.datetime.strftime( date_pre_3m, "%Y%m%d")
        obj_date["date_pre_6m_str"] = dt.datetime.strftime( date_pre_6m, "%Y%m%d")
        obj_date["date_pre_1y_str"] = dt.datetime.strftime( date_pre_1y, "%Y%m%d")
        obj_date["date_pre_2y_str"] = dt.datetime.strftime( date_pre_2y, "%Y%m%d")
        obj_date["date_pre_3y_str"] = dt.datetime.strftime( date_pre_3y, "%Y%m%d")
        obj_date["date_pre_5y_str"] = dt.datetime.strftime( date_pre_5y, "%Y%m%d")
        obj_date["date_pre_10y_str"] = dt.datetime.strftime( date_pre_10y, "%Y%m%d")
        
        obj_date["date_post_1w_str"] = dt.datetime.strftime( date_post_1w, "%Y%m%d")
        obj_date["date_post_1m_str"] = dt.datetime.strftime( date_post_1m, "%Y%m%d")
        obj_date["date_post_3m_str"] = dt.datetime.strftime( date_post_3m, "%Y%m%d")
        obj_date["date_post_6m_str"] = dt.datetime.strftime( date_post_6m, "%Y%m%d")
        obj_date["date_post_1y_str"] = dt.datetime.strftime( date_post_1y, "%Y%m%d")
        obj_date["date_post_2y_str"] = dt.datetime.strftime( date_post_2y, "%Y%m%d")

        #######################################################################
        ### 给定日期之前的最近月末和2个季末、2个半年末 
        
        ### 导入日期数据，匹配交易所，获取最新日期
        file_name = "date_trade.xlsx"
        df_date = pd.read_excel( self.path_dt +file_name,sheet_name="date_trade"  )
        
        ### 只需要上交所的交易日数据！
        exchange = "SSE"
        df_date = df_date[ df_date["exchange"] == exchange ]

        time_str = dt.datetime.strftime( time_0, "%Y%m%d")

        #######################################################################
        ### 月末数据 pre_1m_end
        df_date_sub = df_date[ df_date["type_date"] == "m" ]
        df_date_sub = df_date_sub[ df_date_sub["date"] <= int(time_str) ]
        ### 月末的交易日，按升序排列
        obj_date["date_list_m"] = list( df_date_sub["date"] )

        ### type of df_date["date"] : int 
        date_pre_1m_end = df_date_sub["date"].max()
        print("date_pre_1m_end=", date_pre_1m_end, type(date_pre_1m_end) )

        obj_date["date_pre_1m_end_str"] = str( date_pre_1m_end )
        obj_date["date_pre_1m_end"] = dt.datetime.strptime( str(date_pre_1m_end) , "%Y%m%d") 

        #######################################################################
        ### 最近4个季末交易日 pre_1q_end,pre_2q_end
        df_date_sub = df_date[ df_date["type_date"] == "q" ]
        df_date_sub = df_date_sub[ df_date_sub["date"] <= int(time_str) ]
        df_date_sub = df_date_sub.sort_values(by="date",ascending=True )
        
        date_pre_1q_end = df_date_sub["date"].max()
        date_pre_2q_end = df_date_sub["date"].values[-2]
        date_pre_3q_end = df_date_sub["date"].values[-3]
        date_pre_4q_end = df_date_sub["date"].values[-4]
        print("date_pre_1q,2q_end=", date_pre_1q_end, date_pre_2q_end )

        obj_date["date_pre_1q_end_str"] = str( date_pre_1q_end )
        obj_date["date_pre_1q_end"] = dt.datetime.strptime( str( date_pre_1q_end ) , "%Y%m%d") 
        obj_date["date_pre_2q_end_str"] = str( date_pre_2q_end )
        obj_date["date_pre_2q_end"] = dt.datetime.strptime( str( date_pre_2q_end) , "%Y%m%d") 
        obj_date["date_pre_3q_end_str"] = str( date_pre_3q_end )
        obj_date["date_pre_3q_end"] = dt.datetime.strptime( str( date_pre_3q_end ) , "%Y%m%d") 
        obj_date["date_pre_4q_end_str"] = str( date_pre_4q_end )
        obj_date["date_pre_4q_end"] = dt.datetime.strptime( str( date_pre_4q_end) , "%Y%m%d")
        
        #######################################################################
        ### 最近4个半年末交易日 pre_1halfyear_end,pre_2halfyear_end
        df_date_sub = df_date[ df_date["type_date"] == "q" ]
        df_date_sub = df_date_sub[ df_date_sub["date"] <= int(time_str) ]
        df_date_sub = df_date_sub.sort_values(by="date",ascending=True )
        
        date_pre_1halfyear_end = df_date_sub["date"].max()
        temp_num_date = df_date_sub["num_date"].values[-1]
        ### 看看 "num_date" 是否属于 2,4,整除2的余数是0  || notes:季末交易日可能是 20130329，不能用mmdd判断
        if int( temp_num_date ) % 2  == 0 :
            ### 隔2个季度取数
            date_pre_2halfyear_end = df_date_sub["date"].values[-3]
        else :
            date_pre_1halfyear_end = df_date_sub["date"].values[-2]
            date_pre_2halfyear_end = df_date_sub["date"].values[-4]

        print("date_pre_1halfyear, 2halfyear_end=", date_pre_1halfyear_end, date_pre_2halfyear_end )

        obj_date["date_pre_1halfyear_end_str"] = str( date_pre_1halfyear_end )
        obj_date["date_pre_1halfyear_end"] = dt.datetime.strptime( str(date_pre_1halfyear_end) , "%Y%m%d") 
        obj_date["date_pre_2halfyear_end_str"] = str( date_pre_2halfyear_end )
        obj_date["date_pre_2halfyear_end"] = dt.datetime.strptime( str(date_pre_2halfyear_end) , "%Y%m%d") 

        #######################################################################
        ### 最近4个半年末财务报表日 | 半年末一般延迟4个月， 4-30只能获得上一年底的年报，8-31获得半年报
        ### 半年末交易日之外，再保存财务报表对应的季末日期
        temp_m  = time_now_str[4:6]
        temp_year=time_now_str[:4] 
        temp_year_pre1 = str( int(temp_year) -1  )
        temp_year_pre2 = str( int(temp_year) -2  )
        ### 如果是9，10，11，12，1，2，3，取 pre-year+ 0630, 否则 + 1231 
        if int(temp_m) in [9,10,11,12,1,2,3] :
            obj_date["date_report_pre_1halfyear_str"] = temp_year_pre1 + "0630"
            obj_date["date_report_pre_2halfyear_str"] = temp_year_pre1 + "1231"
            obj_date["date_report_pre_3halfyear_str"] = temp_year_pre2 + "0630"
            obj_date["date_report_pre_4halfyear_str"] = temp_year_pre2 + "1231"
        else :
            obj_date["date_report_pre_1halfyear_str"] = temp_year_pre1 + "1231"
            obj_date["date_report_pre_2halfyear_str"] = temp_year_pre1 + "0630"
            obj_date["date_report_pre_3halfyear_str"] = temp_year_pre2 + "1231"
            obj_date["date_report_pre_4halfyear_str"] = temp_year_pre2 + "0630" 

            

        return obj_date

