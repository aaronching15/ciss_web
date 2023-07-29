# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function: realize active benchmark functions
功能：实现主动基准的实例测试。
last update 181031 | since 181031
derived from test_active_bm.py
Menu :
1,2012-2018年，抓去 5-31，11-30 的三个指数成份股 
    csi300,500,1000 index | 000300.SH, 000905.SH,000852.SH
    base date,20041231,20041231,20041231
    issue date,20050408,20070115,20141017   
    000906.SH = 000300.SH + 000905.SH
    notes:20110531抓取不到 000852 的CSI1000的成份股数据，000905可以。对于20140531
    抓取不到csi1000的数据，但是20141130就可以，说明是否有数据是跟着issue date 走的。
2，
    1.0，information process：
        TS --> strategy --> ranking --> signal --> portfolio rebalance 
    1.1，data_in |原始研究数据,data_in 经过研究员初步梳理后的结构化信息     
    1.2，estimates |根据data_in,生成对资产的某种预测，以变量，参数，方程等形式
    1.3，TS | data_in --> estimates，logic分析逻辑，structure框架 --> TS    时间序列标准化:
         TS_i = {ts_i_j for asset i and information j in M sources | data_in, estimtes, logic, structure, N assets, M information sources  }
    1.4，Analyzing：algo model,econ. model , ...... tail risk,active h_a ??
    1.5, strategy：
    1.6，ranking
    1.7，trade：portfolio rebalance
    1.8，port. evaluation performance and risk

Notes:
1,wind-api数据提取限制条件|单位：万,181031：
    1,wset, 10/day,50/week
    2,wss , 10/day,50/week
    3,wsd , 10/day,50/week
    4,wsq , 400/week
    5,wsi , 100/week
    6,wst , 100/week
    7,EDB , 100/week
    notes:分开计算的。 
2,wind api 每次只能抓取不超过100个数据。
===============================================
'''
import sys
import pandas as pd 


class Abm_model():
    #####################################################
    def __init__(self ):
        self.name = "Active Benchmark Model"


    def load_symbol_universe(self,file_path_funda,time_stamp_input ) :
        '''
        load grouped data of index constituents for several time series
        input data contain indicators from 7 angles:
        [行业，流动性，动量，主动收益，价值，成长，资本结构，财务优势，经营能力，人力优势，信息优势]
        
        '''
        sys.path.append("..")
        from db.assets import stocks
        stocks = stocks()
        funda_wash = stocks.data_wash_funda()
        file_path_funda = "C:\\zd_zxjtzq\\RC_trashes\\temp\\keti_sys_stra_24h\\p1\\wind_data\\funda\\"
        time_stamp_input= '20180923'
        output_type = 'dict'
        dataG = funda_wash.Get_json_groupdata('','',file_path_funda,time_stamp_input,output_type ) 

        return dataG
    def get_all_list(self,dataG,temp_date='2016-05-31') :
        # return a full symbol list with given time {csi300,csi800,csi1000}
        # to see contents in dataG.datagroup()
        temp_list = pd.DataFrame()
        for temp_i in dataG.index_list:
            # ['000300.SH', '000905.SH', '000852.SH'] 
            temp_list_sub = dataG.datagroup[temp_i+'_'+temp_date]
            temp_list = pd.concat([temp_list,temp_list_sub],ignore_index=False)
            #there are duplicate indexes, so we need to re-sort index here
            temp_list= temp_list.reset_index()
            print( temp_list.head() )
            temp_list= temp_list.drop(['index'] ,axis=1 )  

        return temp_list

    def get_symbol_list(self,dataG,wind_index='000300.SH',t_date='2014-05-31'):
        # get symbol table from data_group for given index code and tiem point 

        temp_list = dataG.datagroup[wind_index+'_'+t_date]

        return temp_list 

    def get_histData_finance_capital(self ):
        # get historical fundamental financial and capital data

        path_db_dzh = "D:\\db_dzh_dfw\\"
        file_tb_finance_finance = "tb_finance_financeData.csv"
        file_tb_finance_capital = "tb_finance_capital.csv"
        print("Loading financia data and capital data. ")
        df_tb_fi_fi = pd.read_csv(path_db_dzh+file_tb_finance_finance, encoding="GBK",sep=",",low_memory=False)
        df_tb_fi_cap = pd.read_csv(path_db_dzh+file_tb_finance_capital, encoding="GBK",sep=",",low_memory=False )

        return  df_tb_fi_fi, df_tb_fi_cap

    def calc_financial_estimates(self,temp_list,temp_date,df_tb_fi_fi) :
        # calculate financial estimates using financial data in current year and past year
        import re 
        yymmdd = re.split('-',temp_date )
        # From date for symbol list to date for fiscal quarter # "2013-03-31"  
        # Notes:要用到前一年数据，但是财务数据从2013年开始的，2013年无法取得2012年的数据！！ 
        if temp_date[-5:] == "05-31" :
            date_list = [str(int(yymmdd[0])-1)+'-12-31', yymmdd[0]+'-03-31',str(int(yymmdd[0])-1)+'-03-31']
        elif temp_date[-5:] == "11-30" :
            date_list = [ yymmdd[0]+'-09-30', str(int(yymmdd[0])-1)+'-09-30',str(int(yymmdd[0])-1)+'-12-31' ]

        for temp_i in temp_list.index :
            temp_code = temp_list.loc[temp_i,'code']
            # code2 = "SH600036" # notes: different code types in df_tb_fi_fi["证券代码"]
            # print('181025======temp_code :',type(temp_code),temp_code)
            # print(type(temp_code[-2:]),type(temp_code[:6]))
            # print('====================')
            # print('temp_code ',temp_code, '\n')
            code2 = temp_code[-2:] + temp_code[:6] 
            # print("code2  ",code2)
            if df_tb_fi_fi[df_tb_fi_fi["证券代码"]== code2 ].any().any() :
                df_code = df_tb_fi_fi[ df_tb_fi_fi["证券代码"]== code2 ] 
                # get latest dates before given date from df_code
                #   trasform from string to datetime 

                df_code.loc[:,"date"] = df_code.loc[:,"时间"] 
                # from string to DatetimeIndex(['2013-03-31',...
                df_code["date"] = df_code["date"].apply( pd.to_datetime ) 
                # print("=====df_code[时间]==181025")
                 
                temp_index1 = df_code[ df_code["date"] == date_list[0] ].index 
                temp_index2 = df_code[ df_code["date"] == date_list[1] ].index
                temp_index3 = df_code[ df_code["date"] == date_list[2] ].index
                # 根据 Q1,2,3,4的不同计算 注意：此处的q3数据对应当年前3季度之和，而不是分季度的，
                # q4对应当年全年的财务数据之和
                # df_code.index might be empty for 000562.SZ
                ## Initialize var. and para 参数初始化！由于股票缺少Q1或Q3的财务数据，要做好初始化的默认设置
                #   例：603589.SH have no records for 2015-03! 次新股。 

                if temp_date[-5:] == "05-31" and len(df_code.index) >0  :
                    # Q4_pre, Q1, Q1_pre
                    # just one date match | TypeError: cannot convert the series to <class 'float'>
                    # print( " df_code.loc[temp_index1 ,净利润（不含少数股东损益）:" )
                    # print( df_code.loc[temp_index1 , "净利润（不含少数股东损益）"]  )
                    profit_q4_pre =  float(df_code.loc[temp_index1 , "净利润（不含少数股东损益）"].values )
                    revenue_q4_pre = float(df_code.loc[temp_index1, "一.营业收入"].values )
                    cf_oper_q4_pre = float(df_code.loc[temp_index1 , "经营活动产生的现金流量净额"].values )
                    try :                
                        profit_q1 =  float(df_code.loc[temp_index2 , "净利润（不含少数股东损益）"].values )
                        revenue_q1 = float(df_code.loc[temp_index2, "一.营业收入"].values )
                        cf_oper_q1 = float(df_code.loc[temp_index2 , "经营活动产生的现金流量净额"].values )
                        profit_q1_pre =  float(df_code.loc[temp_index3 , "净利润（不含少数股东损益）"].values )
                        revenue_q1_pre = float(df_code.loc[temp_index3 , "一.营业收入"].values )
                        cf_oper_q1_pre = float(df_code.loc[temp_index3 , "经营活动产生的现金流量净额"].values )
                        # synthesis profit,revenue and cash flow 
                        # 1,计算去年1季度的全年占比
                        if profit_q1_pre != 0 :
                            profit_q1_yoy = profit_q1/profit_q1_pre # yoy
                        else :
                            profit_q1_yoy = 1
                        profit_q1_pre_pct = profit_q1_pre/profit_q4_pre
                        para_fi_max = 0.35 # for q1 
                        para_fi_1 = min(profit_q1_pre_pct,para_fi_max )
                        profit_q4_es = profit_q1 + (profit_q4_pre-profit_q1_pre)*((1-para_fi_1)+para_fi_1*profit_q1_yoy)
                        
                        if revenue_q1_pre !=0 :
                            revenue_q1_yoy = revenue_q1/revenue_q1_pre
                        else :
                            revenue_q1_yoy =1
                        revenue_q1_pre_pct = revenue_q1_pre/revenue_q4_pre
                        para_fi_2 = min(revenue_q1_pre_pct,para_fi_max )
                        revenue_q4_es = revenue_q1 + (revenue_q4_pre-revenue_q1_pre)*((1-para_fi_2)+para_fi_2*revenue_q1_yoy)

                        if cf_oper_q1_pre != 0 :
                            cf_oper_q1_yoy = cf_oper_q1/cf_oper_q1_pre
                        else :
                            cf_oper_q1_yoy = 1
                        cf_oper_q1_pre_pct = cf_oper_q1_pre/cf_oper_q4_pre
                        para_fi_3 = min(cf_oper_q1_pre_pct,para_fi_max )
                        cf_oper_q4_es = cf_oper_q1 + (cf_oper_q4_pre-cf_oper_q1_pre)*((1-para_fi_3)+para_fi_3*cf_oper_q1_yoy)
                             
                    except:
                        # 603589.SH have no records for 2015-03! 次新股。
                        # cautiously estimate current year/pre_year = 95% 
                        print('There are missing quarterly finance records:')
                        profit_q4_es = profit_q4_pre*0.95
                        revenue_q4_es = revenue_q4_pre*0.95
                        cf_oper_q4_es = cf_oper_q4_pre *0.95 
                    
                elif temp_date[-5:] == "11-30" :
                    # Q3, Q3_pre, Q4_pre,
                    profit_q4_pre =  float(df_code.loc[temp_index3 , "净利润（不含少数股东损益）"].values )
                    revenue_q4_pre = float(df_code.loc[temp_index3 , "一.营业收入"].values )
                    cf_oper_q4_pre = float(df_code.loc[temp_index3 , "经营活动产生的现金流量净额"].values )

                    try :
                        profit_q3 =  float(df_code.loc[temp_index1 , "净利润（不含少数股东损益）"].values )
                        revenue_q3 = float(df_code.loc[temp_index1, "一.营业收入"].values )
                        cf_oper_q3 = float(df_code.loc[temp_index1 , "经营活动产生的现金流量净额"].values )
                        profit_q3_pre =  float(df_code.loc[temp_index2 , "净利润（不含少数股东损益）"].values )
                        revenue_q3_pre = float(df_code.loc[temp_index2, "一.营业收入"].values )
                        cf_oper_q3_pre = float(df_code.loc[temp_index2 , "经营活动产生的现金流量净额"].values )
                        
                        # synthesis profit,revenue and cash flow 
                        # 1,计算今年3季度的全年占比
                        profit_q3_yoy = profit_q3/profit_q3_pre # yoy
                        profit_q3_pre_pct = profit_q3_pre/profit_q4_pre
                        para_fi_max = 0.75 # for q3
                        para_fi_1 = min(profit_q3_pre_pct,para_fi_max)
                        profit_q4_es = profit_q3 + (profit_q4_pre-profit_q3_pre)*((1-para_fi_1)+para_fi_1*profit_q3_yoy)
                        
                        revenue_q3_yoy = revenue_q3/revenue_q3_pre
                        revenue_q3_pre_pct = revenue_q3_pre/revenue_q4_pre
                        para_fi_2 = min(revenue_q3_pre_pct,para_fi_max)
                        revenue_q4_es = revenue_q3 + (revenue_q4_pre-revenue_q3_pre)*((1-para_fi_2)+para_fi_2*revenue_q3_yoy)

                        cf_oper_q3_yoy = cf_oper_q3/cf_oper_q3_pre
                        cf_oper_q3_pre_pct = cf_oper_q3_pre/cf_oper_q4_pre
                        para_fi_3 = min(cf_oper_q3_pre_pct,para_fi_max)
                        cf_oper_q4_es = cf_oper_q3 + (cf_oper_q4_pre-cf_oper_q3_pre)*((1-para_fi_3)+para_fi_3*cf_oper_q3_yoy)
                    except :
                        # cautiously estimate current year/pre_year = 95% 
                        profit_q4_es = profit_q4_pre*0.95
                        revenue_q4_es = revenue_q4_pre*0.95
                        cf_oper_q4_es = cf_oper_q4_pre *0.95 
                # 
                temp_list.loc[temp_i, "profit_q4_es" ] = profit_q4_es
                temp_list.loc[temp_i, "revenue_q4_es" ] = revenue_q4_es
                temp_list.loc[temp_i, "cf_oper_q4_es" ] = cf_oper_q4_es
                # get estimated growth of profits and their percentage values
                # difference value compared with last year
                temp_list.loc[temp_i, "profit_q4_es_dif" ] = profit_q4_es - profit_q4_pre
                temp_list.loc[temp_i, "revenue_q4_es_dif" ] = revenue_q4_es- revenue_q4_pre
                temp_list.loc[temp_i, "cf_oper_q4_es_dif" ] = cf_oper_q4_es- cf_oper_q4_pre
                # difference percentage value  compared with last year
                if profit_q4_pre >0 :
                    temp_list.loc[temp_i, "profit_q4_es_dif_pct" ] = (profit_q4_es - profit_q4_pre)/profit_q4_pre
                else :
                    temp_list.loc[temp_i, "profit_q4_es_dif_pct" ] = -0.001
                if revenue_q4_pre>0:
                    temp_list.loc[temp_i, "revenue_q4_es_dif_pct" ] = (revenue_q4_es- revenue_q4_pre)/revenue_q4_pre
                else :
                    temp_list.loc[temp_i, "revenue_q4_es_dif_pct" ] = -0.001
                if cf_oper_q4_pre >0 :
                    temp_list.loc[temp_i, "cf_oper_q4_es_dif_pct" ] = cf_oper_q4_es- cf_oper_q4_pre
                else :
                    temp_list.loc[temp_i, "cf_oper_q4_es_dif_pct" ] = -0.001

        for index01 in temp_list.index :
            # get industry code from 1 to 3 
            # notes: there might still be duplicate index 
            str1 = temp_list.loc[index01, "industry_gicscode"]
            temp_list.loc[index01,'ind1_code'] = str(str1)[:2]
            temp_list.loc[index01,'ind2_code'] = str(str1)[:4]
            temp_list.loc[index01,'ind3_code'] = str(str1)[:6]

        # cols_new mean new columns for temp_list
        cols_new= []
        cols_new=cols_new+['ind1_code','ind2_code','ind3_code']
        cols_new=cols_new+[ "profit_q4_es" , "revenue_q4_es" , "cf_oper_q4_es"  ]
        cols_new=cols_new+[ "profit_q4_es_dif" , "revenue_q4_es_dif" , "cf_oper_q4_es_dif"  ]
        cols_new=cols_new+[ "profit_q4_es_dif_pct" , "revenue_q4_es_dif_pct" , "cf_oper_q4_es_dif_pct"  ]
        return temp_list,cols_new

    def calc_weight_allo_ind_hierachy(self,temp_list) :
        # calculate weights of asset allocation for industry hierachy from level1 to level3 
        '''
        str_ind1，str_ind2行业中性化权重配置
        若某ind2中有N{1~10}个ind3，对于每个ind3，计算s um of profit and profit_dif,
        step1，汇总所有ind1内的股票df_ind1，df_ind1内依次获取df_ind2,计算profit_sum,profit_dif_sum
            计算, w_allo_value_ind1_ind2, w_allo_growth_ind1_ind2 :
            1, weight of allocation in value for industry level2 of level1
            2, weight of allocation in growth for industry level2 of level1

        step2，汇总所有ind12的股票df_ind3，df_ind2内依次获取df_ind3,计算profit_sum,profit_dif_sum
            计算, w_allo_value_ind2_ind3, w_allo_growth_ind2_ind3 :
            1, weight of allocation in value for industry level3 of level2
            2, weight of allocation in growth for industry level3 of level2    
        output：
        notes:若出现行业内某股票大额亏损，使得全行业加总值为负数或者极小数，则计算百分比时可能会出现异常的情况。

        '''
        # [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
        list_ind1 = list( temp_list['ind1_code'].drop_duplicates()  ) 
        list_ind2 = list( temp_list['ind2_code'].drop_duplicates()  ) 
        list_ind3 = list( temp_list['ind3_code'].drop_duplicates()  ) 
        for int_ind1 in list_ind1 :
            print("Working on industry level 1: code=", int_ind1 )
            # type is numpy.int64 if import from csv
            df_ind1 = temp_list[temp_list['ind1_code'] == int_ind1 ]
            profit_sum_ind1 = df_ind1['profit_q4_es'].sum()
            profit_dif_sum_ind1 = df_ind1['profit_q4_es_dif'].sum()
            revenue_sum_ind1 = df_ind1['revenue_q4_es'].sum()
            temp_list.loc[df_ind1.index, "profit_sum_ind1" ] = profit_sum_ind1
            temp_list.loc[df_ind1.index, "profit_dif_sum_ind1" ] = profit_dif_sum_ind1
            temp_list.loc[df_ind1.index, "revenue_sum_ind1" ] = revenue_sum_ind1
            # get industry level2 list : 
            list_ind2 = list( df_ind1['ind2_code'].drop_duplicates()  ) 
            if len(list_ind2) == 1 :
                # only one ind2 in current ind1,weight of ind2 in ind1 is 100%
                w_ind1_ind2 = 1
                # assign to temp_list
                temp_list.loc[df_ind1.index, "w_allo_value_ind1_ind2" ] = w_ind1_ind2
                temp_list.loc[df_ind1.index, "w_allo_growth_ind1_ind2" ] = w_ind1_ind2
                temp_list.loc[df_ind1.index, "profit_sum_ind2" ] = profit_sum_ind1
                temp_list.loc[df_ind1.index, "profit_dif_sum_ind2" ] = profit_dif_sum_ind1
                temp_list.loc[df_ind1.index, "revenue_sum_ind2" ] = revenue_sum_ind1

            elif len(list_ind2) > 1 :
                for int_ind2 in list_ind2 :
                    print("Working on industry level 2: code=", int_ind2 )
                    df_ind2 = temp_list[temp_list['ind2_code'] == int_ind2 ]
                    profit_sum_ind2 = df_ind2['profit_q4_es'].sum()
                    profit_dif_sum_ind2 = df_ind2['profit_q4_es_dif'].sum()
                    revenue_sum_ind2 = df_ind2['revenue_q4_es'].sum()
                    temp_list.loc[df_ind2.index, "profit_sum_ind2" ] = profit_sum_ind2
                    temp_list.loc[df_ind2.index, "profit_dif_sum_ind2" ] = profit_dif_sum_ind2
                    temp_list.loc[df_ind2.index, "revenue_sum_ind2" ] = revenue_sum_ind2

                    if profit_sum_ind1!=0 :
                        if profit_sum_ind2/profit_sum_ind1 >0 :
                            w_ind1_ind2 = profit_sum_ind2/profit_sum_ind1
                        else :
                            w_ind1_ind2 = 0 
                    else :
                        w_ind1_ind2 = 0

                    temp_list.loc[df_ind2.index, "w_allo_value_ind1_ind2" ] = min(w_ind1_ind2,1.00)
                    if profit_dif_sum_ind1!=0 :
                        if profit_dif_sum_ind2/profit_dif_sum_ind1>0 :
                            w_ind1_ind2 = profit_dif_sum_ind2/profit_dif_sum_ind1
                        else :
                            w_ind1_ind2 = 0 
                    else :
                        w_ind1_ind2 = 0 
                    temp_list.loc[df_ind2.index, "w_allo_growth_ind1_ind2" ] = min(w_ind1_ind2,1.00)
         
                    # get industry level3 list : 
                    list_ind3 = list( df_ind2 ['ind3_code'].drop_duplicates()  ) 
                    for int_ind3 in list_ind3 :
                        print("Working on industry level 3: code=", int_ind3 )
                        df_ind3 = temp_list[temp_list['ind3_code'] == int_ind3 ]
                        profit_sum_ind3 = df_ind3['profit_q4_es'].sum()
                        profit_dif_sum_ind3 = df_ind3['profit_q4_es_dif'].sum()
                        revenue_sum_ind3 = df_ind3['revenue_q4_es'].sum()
                        temp_list.loc[df_ind3.index, "profit_sum_ind3" ] = profit_sum_ind3
                        temp_list.loc[df_ind3.index, "profit_dif_sum_ind3" ] = profit_dif_sum_ind3
                        temp_list.loc[df_ind3.index, "revenue_sum_ind3" ] = revenue_sum_ind3

                        if profit_sum_ind2!=0 :
                            if profit_sum_ind3/profit_sum_ind2 >0 :
                                w_ind2_ind3 = profit_sum_ind3/profit_sum_ind2
                            else :
                                w_ind2_ind3 = 0 
                        else :
                            w_ind2_ind3 = 0
                        temp_list.loc[df_ind3.index, "w_allo_value_ind2_ind3" ] = min(w_ind2_ind3,1.00)
                        if profit_dif_sum_ind2!=0 :
                            if profit_dif_sum_ind3/profit_dif_sum_ind2>0 :
                                w_ind2_ind3 = profit_dif_sum_ind3/profit_dif_sum_ind2
                            else : 
                                w_ind2_ind3 = 0 
                        else :
                            w_ind2_ind3 = 0
                        temp_list.loc[df_ind3.index, "w_allo_growth_ind2_ind3" ] = min(w_ind2_ind3,1.00)

            else :
                pass 

        # cols_new mean new columns for temp_list
        cols_new= []
        cols_new=cols_new+["profit_sum_ind1","profit_dif_sum_ind1","revenue_sum_ind1"] 
        cols_new=cols_new+[ "w_allo_value_ind1_ind2","w_allo_growth_ind1_ind2"] 
        cols_new=cols_new+["profit_sum_ind2","profit_dif_sum_ind2","revenue_sum_ind2"] 
        cols_new=cols_new+[ "w_allo_value_ind2_ind3","w_allo_growth_ind2_ind3"] 
 
        return temp_list, cols_new

    def calc_anchor_stocks(self,temp_list ) :
        # calculate for anchor stocks in value and growth perspective
        
        list_ind1 = list( temp_list['ind1_code'].drop_duplicates()  ) 
        list_ind2 = list( temp_list['ind2_code'].drop_duplicates()  ) 
        list_ind3 = list( temp_list['ind3_code'].drop_duplicates()  )  
        # [401010, 601020, 201050, 551010, 201060, 151040, ....
        for str_ind3 in list_ind3 : 
            print("Working on industry level3: ", str_ind3, " ,type of ind3 ",type(str_ind3) )
            
            temp_df = temp_list[temp_list['ind3_code'] == str(str_ind3) ].sort_values(by=['profit_q4_es'])
            # if len(temp_df.index ) == 0 :
                # print("str_ind3: ",temp_df , temp_df   )
            index_max = temp_df.index[-1]
            code_anchor_value = temp_df.loc[index_max, 'code' ]
            profit_anchor_value= temp_df.loc[index_max, 'profit_q4_es' ]
            profit_dif_anchor_value= temp_df.loc[index_max, 'profit_q4_es_dif' ]
            revenue_anchor_value= temp_df.loc[index_max, 'revenue_q4_es' ]
            cf_oper_anchor_value= temp_df.loc[index_max, 'cf_oper_q4_es' ]
            # in case of negatvie profit for all stocks, we use weighted revenue and operating cash flow as ranking indicator
            # notes: we want more profit/cf from same revenue, such as APPLE, so revenue sould not be better indicator than operating cash flow
            # step 1: find anchor stock 
            if len(temp_df.index) > 2 :
                if temp_df.loc[index_max,'profit_q4_es'] <=0 :
                    # using operating cash flow 
                    temp_df = temp_list[temp_list['ind3_code'] == str_ind3 ].sort_values(by=['cf_oper_q4_es'])
                    index_max = temp_df.index[-1]
                    if len(temp_df.index) > 2 :
                        if temp_df.loc[index_max,'cf_oper_q4_es'] <=0 :
                            # in case of negative cf_oper for all, we use revenue to pick the ideal firm in value 
                            temp_df = temp_list[temp_list['ind3_code'] == str_ind3 ].sort_values(by=['revenue_q4_es'])
                            index_max = temp_df.index[-1]
                            code_anchor_value = temp_df.loc[index_max, 'code' ]
                            profit_anchor_value= temp_df.loc[index_max, 'profit_q4_es' ]
                            profit_dif_anchor_value= temp_df.loc[index_max, 'profit_q4_es_dif' ]
                            revenue_anchor_value= temp_df.loc[index_max, 'revenue_q4_es' ]
                            cf_oper_anchor_value= temp_df.loc[index_max, 'cf_oper_q4_es' ]
                    else :
                        index_max = temp_df.index[-1]
                        code_anchor_value = temp_df.loc[index_max, 'code' ]
                        profit_anchor_value= temp_df.loc[index_max, 'profit_q4_es' ]
                        profit_dif_anchor_value= temp_df.loc[index_max, 'profit_q4_es_dif' ]
                        revenue_anchor_value= temp_df.loc[index_max, 'revenue_q4_es' ]
                        cf_oper_anchor_value= temp_df.loc[index_max, 'cf_oper_q4_es' ]
            
            # step 2: assign anchor code to industry codes 
            temp_list.loc[temp_df.index, 'profit_anchor_value'] = profit_anchor_value
            temp_list.loc[temp_df.index, 'profit_dif_anchor_value'] = profit_dif_anchor_value
            temp_list.loc[temp_df.index, 'revenue_anchor_value'] = revenue_anchor_value
            temp_list.loc[temp_df.index, 'cf_oper_anchor_value'] = cf_oper_anchor_value
            temp_list.loc[temp_df.index, 'code_anchor_value'] = code_anchor_value
            
            ##  ANCHOR stock for (above designated size)growth
            # 按照anchor_growth 计算成长价格
            # logic:在经营稳健的基础上，选择今年预测净利润最大的股票，同时去年净利润排名也不应该太低。 # case: 在汽车行业中能选中特斯拉
            # todo：1，选择净利润增长值前35%股票，2，经营现金流增长值前35%股票；3,进一步筛选出预测净利润增长百分比最大的

            temp_df = temp_list[temp_list['ind3_code'] == str_ind3 ].sort_values(by=['profit_q4_es_dif']) # 从小到大
            index_max = temp_df.index[-1]
            code_anchor_growth = temp_df.loc[index_max, 'code' ]
            profit_anchor_growth= temp_df.loc[index_max, 'profit_q4_es' ]
            profit_dif_anchor_growth= temp_df.loc[index_max, 'profit_q4_es_dif' ]
            revenue_anchor_growth= temp_df.loc[index_max, 'revenue_q4_es' ]
            cf_oper_anchor_growth= temp_df.loc[index_max, 'cf_oper_q4_es' ]
            # step 2: assign anchor code to industry codes 
            temp_list.loc[temp_df.index, 'code_anchor_growth'] = code_anchor_growth
            if len(temp_df.index) > 2 :
                para_pct_profit = 0.35
                temp_len = len(temp_df.index)
                temp_len1 = round( int(temp_len )*para_pct_profit )
                temp_df = temp_df.iloc[-1*temp_len1:,:  ]
                if len(temp_df.index) > 2 :
                    temp_df = temp_list[temp_list['ind3_code'] == str_ind3 ].sort_values(by=['cf_oper_q4_es_dif']) # 从小到大
                    para_pct_profit = 0.35
                    temp_len = len(temp_df.index)
                    temp_len1 = round( int(temp_len)*para_pct_profit )
                    temp_df = temp_df.iloc[-1*temp_len1:,:  ]
                    if len(temp_df.index) > 2 :
                        temp_df = temp_list[temp_list['ind3_code'] == str_ind3 ].sort_values(by=['profit_q4_es_dif_pct']) # 从小到大
                        temp_len = len(temp_df.index)
                        para_pct_profit = 0.35
                        temp_len1 = round( int(temp_len)*para_pct_profit )
                        temp_df = temp_df.iloc[-1*temp_len1:,:  ]
                        index_max = temp_df.index[-1]
                        code_anchor_growth = temp_df.loc[index_max, 'code' ]
                        profit_anchor_growth= temp_df.loc[index_max, 'profit_q4_es' ]
                        profit_dif_anchor_growth= temp_df.loc[index_max, 'profit_q4_es_dif' ]
                        revenue_anchor_growth= temp_df.loc[index_max, 'revenue_q4_es' ]
                        cf_oper_anchor_growth= temp_df.loc[index_max, 'cf_oper_q4_es' ]
                    else :
                        # update anchor 
                        index_max = temp_df.index[-1]
                        code_anchor_growth = temp_df.loc[index_max, 'code' ]
                        profit_anchor_growth= temp_df.loc[index_max, 'profit_q4_es' ]
                        profit_dif_anchor_growth= temp_df.loc[index_max, 'profit_q4_es_dif' ]
                        revenue_anchor_growth= temp_df.loc[index_max, 'revenue_q4_es' ]
                        cf_oper_anchor_growth= temp_df.loc[index_max, 'cf_oper_q4_es' ]
                else :
                    # update anchor 
                    index_max = temp_df.index[-1]
                    code_anchor_growth = temp_df.loc[index_max, 'code' ]
                    profit_anchor_growth= temp_df.loc[index_max, 'profit_q4_es' ]
                    profit_dif_anchor_growth= temp_df.loc[index_max, 'profit_q4_es_dif' ]
                    revenue_anchor_growth= temp_df.loc[index_max, 'revenue_q4_es' ]
                    cf_oper_anchor_growth= temp_df.loc[index_max, 'cf_oper_q4_es' ]

            # step 2: assign anchor code to industry codes 
            temp_list.loc[temp_df.index, 'profit_anchor_growth'] = profit_anchor_growth
            temp_list.loc[temp_df.index, 'profit_dif_anchor_growth'] = profit_dif_anchor_growth
            temp_list.loc[temp_df.index, 'revenue_anchor_growth'] = revenue_anchor_growth
            temp_list.loc[temp_df.index, 'cf_oper_anchor_growth'] = cf_oper_anchor_growth
            temp_list.loc[temp_df.index, 'code_anchor_growth'] = code_anchor_growth

            ## get exchange rate of single stock using for ANCHOR stocks  
            # Qs: how to pick anchor between value and growth ?
            # idea: market cap. = para_multiple*( part_value + part_growth )
            # Unlike Factor models which persue PURE factor, we us stock with the largest value-part as unit for judging value,
            # and stock with the largest growth-part as unit for judging growth
            # Method1,based on market price, mv_stock = a*mv_s_value+ b*mv_s_growth ,股票多的时候要取平均值？或者按照公司基本面坚实程度分层？
            # Method2,based on value and growth,mv_stock = a*mv_s_value+ b*mv_s_growth，value部分按照和s_value的比值估值，
            #   growth部分按照和s_growth的比值估值，2部分估值相加。
            # Qs：如何平衡value和stock，计算个股的理论价值？？？？ ||按照anchor_value为基准,计算个股价值价格

        # cols_new mean new columns for temp_list
        cols_new= []
        cols_new=cols_new+['profit_anchor_value','profit_dif_anchor_value','revenue_anchor_value','cf_oper_anchor_value', 'code_anchor_value' ] 
        cols_new=cols_new+['profit_anchor_growth','profit_dif_anchor_growth','revenue_anchor_growth','cf_oper_anchor_growth', 'code_anchor_growth' ] 
 
        return temp_list,cols_new


    def calc_shadow_ev_from_anchor(self,temp_list) :
        # calculate shadow enterprise value from anchor stocks in value and growth perspectives
        '''
        根据锚的数值，计算个股的理论价值和价格
        方法论 
        1，确定基准股票（在经营稳健的基础上，选择今年预测净利润最大的股票，同时去年净利润排名也不应该太低。）
        2，计算基准股票总价值换算比例{s_value/base_value +s_growth/base_growth }，比如基准股票A总价值100e，个股S总价值20e，总市值(价值)比例0.2=20/100
            如果净利润太低，用壳价值(30e in 2016 and drop as time passes)或取基准股票的小系数如0.01*MV_base决定
            input：基准股票总利润，收入等，个股总利润和收入，
        3, 计算对应一段时期的股票总股本，计算出理论基准个股价值 （ 用基准股票总市值乘换算比例，再除以当前股票总股本 ） 
            A市值120e(20day MA)，个股S市值30e(20day MA)，个股S锚定价值120e*0.2=24e。
            若个股股本5e，股价6 rmb, 理论价格 4.8， (不复权股价数据要用到当期股本，复权股价数据要用最新股本计算)
        4，个股S的价值比价格 coef_s = 6/4.8=1.2 | 越小越好，说明低估

        '''
        for index01 in temp_list.index :
            # index01 = temp_list.index.values[0]
            # temp_code = "600036.SH" 
            temp_code = temp_list.loc[index01,'code']
            print("Calculating code: ",temp_code )
            # only one row for code in temp_list
            # get industry-3 code for given code 
            str_ind3 = temp_list.loc[index01, "ind3_code"]
            print('Working on industry code:')
            print( type(str_ind3),str_ind3 )
            # print( temp_INDS_sum.index ) 
            # print( temp_INDS_sum.columns ) # Index(['profit_q4_es', 'revenue_q4_es', 'cf_oper_q4_es']
            # get summary values of financial indicators from industry list
            # todo if error for int|string, then using str(str_ind3)
            # 多重索引的dataframe取值一般使用xs,可以传入多个不同级别的索引进行筛选,但不支持同一级索引多选并且xs返回的是数值而不是引用
            # temp_INDS_sum.xs(['10','1010'],level=["ind1_code","ind2_code"]).loc['101010','profit_q4_es']
            # str_ind1 = str_ind3[:2]
            # str_ind2 = str_ind3[:4]
            # # temp_INDS_sum.xs(['10','1010'],level=["ind1_code","ind2_code"]).loc['101010','profit_q4_es']
            # profit_total_ind3 = temp_INDS_sum.xs([str_ind1,str_ind2],level=["ind1_code","ind2_code"]).loc[str_ind3,'profit_q4_es']
            # revenue_total_ind3 = temp_INDS_sum.xs([str_ind1,str_ind2],level=["ind1_code","ind2_code"]).loc[str_ind3,'revenue_q4_es']
            # cf_oper_total_ind3 = temp_INDS_sum.xs([str_ind1,str_ind2],level=["ind1_code","ind2_code"]).loc[str_ind3,'cf_oper_q4_es']
            # # df_ind3 =temp_INDS_sum[ temp_INDS_sum['ind3_code']== int(str_ind3) ]
            df_ind3 = temp_list[temp_list['ind3_code'] == str_ind3 ]
            profit_total_ind3 = df_ind3['profit_q4_es'].sum()
            revenue_total_ind3 = df_ind3['revenue_q4_es'].sum()    
            cf_oper_total_ind3 = df_ind3['cf_oper_q4_es'].sum()    

            try:
                # 计算行业占比目前没看到有什么用，但是未来引入行业总增长率时，对于计算行业内部竞争可能有用。逻辑是不可能每个公司都均匀获得增长
                temp_list.loc[index01,'ind3_pct_profit_q4_es'] =temp_list.loc[index01,'profit_q4_es']/profit_total_ind3
                temp_list.loc[index01,'ind3_pct_revenue_q4_es'] =temp_list.loc[index01,'revenue_q4_es']/revenue_total_ind3
                temp_list.loc[index01,'ind3_pct_cf_oper_q4_es'] =temp_list.loc[index01,'cf_oper_q4_es']/cf_oper_total_ind3 
                # step 2 2，计算基准股票总价值换算比例 |{s_value/base_value +s_growth/base_growth }，比如基准股票A总价值100e，个股S总价值20e，总市值(价值)比例0.2=20/100
                # 如果净利润太低，用壳价值(30e in 2016 and drop as time passes)或取基准股票的小系数如0.01*MV_base决定
                code_anchor_value =  temp_list.loc[index01, 'code_anchor_value'] 
                code_anchor_growth =  temp_list.loc[index01, 'code_anchor_growth']
                # get s_value/base_value 价值的角度应该尽量给与一个合适的数值
                # temp_list.loc[index01, 'code_anchor_value'] 
                if temp_list.loc[index01, 'profit_anchor_value'] > 0 : 
                    if temp_list.loc[index01, 'profit_q4_es'] >0 :
                        para_value =  temp_list.loc[index01, 'profit_q4_es'] /temp_list.loc[index01, 'profit_anchor_value']  
                    else :
                        para_value =  temp_list.loc[index01, 'revenue_q4_es'] /temp_list.loc[index01, 'revenue_anchor_value']  
                else :
                    para_value =  temp_list.loc[index01, 'revenue_q4_es'] /temp_list.loc[index01, 'revenue_anchor_value']  
                # get s_growth/base_growth 增值乏力的情况下，可以严格地给与0值
                if temp_list.loc[index01, 'profit_dif_anchor_growth'] > 0 : 
                    if temp_list.loc[index01, 'profit_q4_es_dif'] >0 :
                        # 注意，用绝对值的情况下，会出现龙头股年利润100-110，+10，+10%；成长最快股票30,42，+12，+40%，绝对值的增长不如龙头股
                        para_growth =  temp_list.loc[index01, 'profit_q4_es_dif'] /temp_list.loc[index01, 'profit_dif_anchor_value']  
                    else :
                        para_growth =  0
                elif temp_list.loc[index01, 'profit_dif_anchor_growth'] < 0 :
                    # take nagative rate: if -1 for profit_dif_anchor_growth and -2 for profit_dif_stock, 
                    # then profit_dif_stock / profit_dif_anchor_growth = -1/-2 = 0.5 
                    # notes that here we have growth_best > growth_single for negative values
                    if temp_list.loc[index01, 'profit_q4_es_dif'] < 0 :
                        para_growth =  temp_list.loc[index01, 'profit_dif_anchor_value'] /temp_list.loc[index01, 'profit_q4_es_dif']
                    else :
                        para_growth = 0
                else :
                    # temp_list.loc[index01, 'profit_dif_anchor_growth'] == 0
                    para_growth = 0

                # para_value is the relative percentage compared with anchor stock  
                temp_list.loc[index01, 'para_value'] = para_value
                temp_list.loc[index01, 'para_growth'] = para_growth

                # 计算对应一段时期的股票总股本，计算出理论基准个股价值
                # todo 1, import tb_finance_capital股本.csv ; given date, get close, MA20 for anchor stock and single stock 
                # variable = df_tb_fi_cap 

            except:
                print('=============df_ind3')
                print( df_ind3 )
                print( type(temp_list.loc[index01,"profit_q4_es"].values[0]) )
                print( type(temp_INDS_sum.loc[df_ind3.index,"profit_q4_es"].values[0]) )
                print("====================")
                print(profit_total_ind3,revenue_total_ind3,cf_oper_total_ind3  )
              
        # cols_new mean new columns for temp_list
        cols_new= []
        cols_new=cols_new+['ind3_pct_profit_q4_es','ind3_pct_revenue_q4_es','ind3_pct_cf_oper_q4_es']
        cols_new=cols_new+['para_value', 'para_growth'  ]
        # equivalent to "para_value" , whcih using 1 for anchor stock, still need to be devided by sum of columns
        #todo create w_ind3_value from para_value 
        list_ind3 = list( temp_list['ind3_code'].drop_duplicates()  )  
        # [401010, 601020, 201050, 551010, 201060, 151040, ....
        for str_ind3 in list_ind3 : 
            df_ind3 = temp_list[temp_list['ind3_code'] == str_ind3 ]
            temp_list.loc[df_ind3.index,"w_allo_value_ind3"] = temp_list.loc[df_ind3.index,"para_value"]/temp_list.loc[df_ind3.index,"para_value"].sum() 
            temp_list.loc[df_ind3.index,"w_allo_growth_ind3"] =temp_list.loc[df_ind3.index,"para_growth"]/temp_list.loc[df_ind3.index,"para_growth"].sum() 
  
        cols_new=cols_new+['w_allo_value_ind3', 'w_allo_growth_ind3'  ]
 
        return temp_list,cols_new

    def get_zscore(self) :
        sys.path.append("..")
        ### step2 因子标准化（Z-Score)处理
        from db.basics import industry
        industry = industry()
        ind_raw = industry.load_wind_ind('')
        print("industry data from Wind-API: \n")
        # Qs UnicodeEncodeError: 'charmap' codec can't encode characters in position
        # Ans : cmd不能很好地兼容utf8,改一下编码，比如我换成“gb18030”，就能正常显示
        print( ind_raw.head(5) )





        return 1 
































































































