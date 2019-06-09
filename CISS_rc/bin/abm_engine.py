# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function: realize active benchmark functions
功能：实现主动基准的实例测试。 
last update 190412 | since 181031
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
        file_path_funda = "D:\\db_wind\\wind_data\\funda\\"
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
            # print("dataG.datagroup=========" )
            # print( dataG.datagroup ) 

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
        # last update 190412
        # get historical fundamental financial and capital data

        # ### Files since 2018q3
        # path_db_dzh = "D:\\db_dzh_dfw\\tb_finance_18q3\\"
        # file_tb_finance_finance = "tb_finance_Q3_all.csv"
        # file_tb_finance_capital = "tb_finance_capital_20181231.csv"        
        
        ### Files from 2007 to 2018q3
        path_db_dzh = "D:\\db_dzh_dfw\\tb_finance_07q1_18q3\\"
        file_tb_finance_finance = "tb_finance_from2004.csv"
        file_tb_finance_capital = "tb_finance_capital_20181231.csv"        


        print("Loading financia data and capital data. ")
        df_tb_fi_fi = pd.read_csv(path_db_dzh+file_tb_finance_finance, encoding="GBK",sep=",",low_memory=False)
        # notes: there is one problem in original csv file for capital info, 
        # some cells of columns "变动原因" or columns T consists "," and we should replace it with other seperater 
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

                # print("temp_index123 ",temp_index1,temp_index2,temp_index3)
                # print( df_code.loc[temp_index1 , "净利润（不含少数股东损益）"] )
                # print( df_code.loc[temp_index2 , "净利润（不含少数股东损益）"] )
                # print( df_code.loc[temp_index3 , "净利润（不含少数股东损益）"] )

                if temp_date[-5:] == "05-31" and len(df_code.index) >0  :
                    # Q4_pre, Q1, Q1_pre
                    # just one date match | TypeError: cannot convert the series to <class 'float'>

                    try :
                        profit_q4_pre =  float(df_code.loc[temp_index1 , "净利润（不含少数股东损益）"].values )
                        revenue_q4_pre = float(df_code.loc[temp_index1, "一.营业收入"].values )
                        cf_oper_q4_pre = float(df_code.loc[temp_index1 , "经营活动产生的现金流量净额"].values )
                    except:
                        ### 例外 SZ001872 只有2017年开始才有数据，可能因为发生过收购兼并导致了之前数据变没。
                        
                        profit_q4_es = 0.001
                        revenue_q4_es = 0.001
                        cf_oper_q4_es = 0.001

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
                    try :
                        profit_q4_pre =  float(df_code.loc[temp_index1 , "净利润（不含少数股东损益）"].values )
                        revenue_q4_pre = float(df_code.loc[temp_index1, "一.营业收入"].values )
                        cf_oper_q4_pre = float(df_code.loc[temp_index1 , "经营活动产生的现金流量净额"].values )
                    except:
                        ### 例外 SZ001872 只有2017年开始才有数据，可能因为发生过收购兼并导致了之前数据变没。
                        
                        profit_q4_es = 0.001
                        revenue_q4_es = 0.001
                        cf_oper_q4_es = 0.001


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

        ### drop nan values 
        df_dropna = temp_list.loc[:,['profit_q4_es','revenue_q4_es','profit_q4_es_dif','revenue_q4_es_dif']].dropna(axis=0)
        temp_list = temp_list.loc[df_dropna.index,:]
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
        若某ind2中有N{1~10}个ind3，对于每个ind3，计算sum of profit and profit_dif,
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
        # ind_level='3' 
        # col_name_ind = 'ind'+ ind_level + '_code' # 'ind1_code'

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

    def calc_anchor_stocks(self,temp_list, ind_level='3') :
        # notes：证券行业里000562.SZ 在2014年底退市，因此
        # calculate for anchor stocks in value and growth perspective for all industries in given level
        col_name_ind = 'ind'+ ind_level + '_code' # 'ind1_code'
        # list_ind1 = list( temp_list['ind1_code'].drop_duplicates()  ) 
        # list_ind2 = list( temp_list['ind2_code'].drop_duplicates()  ) 
        # list_ind3 = list( temp_list['ind3_code'].drop_duplicates()  )  
        
        list_ind_x = list( temp_list[ col_name_ind  ].drop_duplicates()  )  
        # drop all stocks that can do not have enough information 
        

        # [401010, 601020, 201050, 551010, 201060, 151040, ....
        for str_ind_x in list_ind_x : 
            
            temp_df = temp_list[temp_list[col_name_ind] == str(str_ind_x) ].sort_values(by=['profit_q4_es'])
            # notes: last row of temp_df["profit_q4_es"] = NaN
            # temp_df = temp_df.dropna(axis=0) # may delete too much rows
            # temp_df.to_csv("D:\\temp_df_181121.csv")
            # We only want to drop nan(type=numpy float) from column "profit_q4_es","revenue_q4_es".  ||181121，
            # Security firm 000562.SZ de-listing at end of 2014
            temp_df4index = temp_df.loc[:,['profit_q4_es','revenue_q4_es']].dropna(axis=0)
            temp_df= temp_df.loc[temp_df4index.index,:]
            # print("temp_df")
            # print(temp_df['profit_q4_es'].tail() )
            # print("Working on industry level: ", str_ind_x, " ,type of ind",ind_level,' ' ,col_name_ind,'',temp_df.index[-1] )
            # asd            
            index_temp1 = temp_df[ temp_df['profit_q4_es']==temp_df['profit_q4_es'].max() ]
            index_max = index_temp1.index[0]
            # index_max = temp_df.index[-1]
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
                    temp_df = temp_list[temp_list[col_name_ind] == str_ind_x ].sort_values(by=['cf_oper_q4_es'])
                    # index_max = temp_df.index[-1]
                    index_temp1 = temp_df[ temp_df['profit_q4_es']==temp_df['profit_q4_es'].max() ]
                    index_max = index_temp1.index[0]
                    if len(temp_df.index) > 2 :
                        if temp_df.loc[index_max,'cf_oper_q4_es'] <=0 :
                            # in case of negative cf_oper for all, we use revenue to pick the ideal firm in value 
                            temp_df = temp_list[temp_list[col_name_ind] == str_ind_x ].sort_values(by=['revenue_q4_es'])
                            index_temp1 = temp_df[ temp_df['profit_q4_es']==temp_df['profit_q4_es'].max() ]
                            index_max = index_temp1.index[0]
                            # index_max = temp_df.index[-1]
                            code_anchor_value = temp_df.loc[index_max, 'code' ]
                            profit_anchor_value= temp_df.loc[index_max, 'profit_q4_es' ]
                            profit_dif_anchor_value= temp_df.loc[index_max, 'profit_q4_es_dif' ]
                            revenue_anchor_value= temp_df.loc[index_max, 'revenue_q4_es' ]
                            cf_oper_anchor_value= temp_df.loc[index_max, 'cf_oper_q4_es' ]
                    else :
                        index_temp1 = temp_df[ temp_df['profit_q4_es']==temp_df['profit_q4_es'].max() ]
                        index_max = index_temp1.index[0]
                        # index_max = temp_df.index[-1]
                        code_anchor_value = temp_df.loc[index_max, 'code' ]
                        profit_anchor_value= temp_df.loc[index_max, 'profit_q4_es' ]
                        profit_dif_anchor_value= temp_df.loc[index_max, 'profit_q4_es_dif' ]
                        revenue_anchor_value= temp_df.loc[index_max, 'revenue_q4_es' ]
                        cf_oper_anchor_value= temp_df.loc[index_max, 'cf_oper_q4_es' ]
            
            # step 2: assign anchor code to industry codes 
            # print(profit_anchor_value, profit_dif_anchor_value, revenue_anchor_value,cf_oper_anchor_value)
            # asd
            temp_list.loc[temp_df.index, 'profit_anchor_value'] = profit_anchor_value
            temp_list.loc[temp_df.index, 'profit_dif_anchor_value'] = profit_dif_anchor_value
            temp_list.loc[temp_df.index, 'revenue_anchor_value'] = revenue_anchor_value
            temp_list.loc[temp_df.index, 'cf_oper_anchor_value'] = cf_oper_anchor_value
            temp_list.loc[temp_df.index, 'code_anchor_value'] = code_anchor_value
            
            ##  ANCHOR stock for (above designated size)growth
            # 按照anchor_growth 计算成长价格
            # logic:在经营稳健的基础上，选择今年预测净利润最大的股票，同时去年净利润排名也不应该太低。 # case: 在汽车行业中能选中特斯拉
            # todo：1，选择净利润增长值前35%股票，2，经营现金流增长值前35%股票；3,进一步筛选出预测净利润增长百分比最大的

            temp_df = temp_list[temp_list[col_name_ind] == str_ind_x ].sort_values(by=['profit_q4_es_dif']) # 从小到大
            # avoid NaN item
            temp_df4index = temp_df.loc[:,['profit_q4_es_dif','revenue_q4_es_dif']].dropna(axis=0)
            temp_df= temp_df.loc[temp_df4index.index,:]
            
            # print("temp_df")
            # print( temp_df["profit_q4_es_dif"].tail() )
            
            # index_max = temp_df.index[-1]
            index_temp1 = temp_df[ temp_df['profit_q4_es_dif']==temp_df['profit_q4_es_dif'].max() ]
            index_max = index_temp1.index[0]

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
                    temp_df = temp_list[temp_list[col_name_ind] == str_ind_x ].sort_values(by=['cf_oper_q4_es_dif']) # 从小到大
                    para_pct_profit = 0.35
                    temp_len = len(temp_df.index)
                    temp_len1 = round( int(temp_len)*para_pct_profit )
                    temp_df = temp_df.iloc[-1*temp_len1:,:  ]
                    if len(temp_df.index) > 2 :
                        temp_df = temp_list[temp_list[col_name_ind] == str_ind_x ].sort_values(by=['profit_q4_es_dif_pct']) # 从小到大
                        temp_len = len(temp_df.index)
                        para_pct_profit = 0.35
                        temp_len1 = round( int(temp_len)*para_pct_profit )
                        temp_df = temp_df.iloc[-1*temp_len1:,:  ]
                        index_temp1 = temp_df[ temp_df['profit_q4_es_dif']==temp_df['profit_q4_es_dif'].max() ]
                        index_max = index_temp1.index[0]
                        # index_max = temp_df.index[-1]
                        code_anchor_growth = temp_df.loc[index_max, 'code' ]
                        profit_anchor_growth= temp_df.loc[index_max, 'profit_q4_es' ]
                        profit_dif_anchor_growth= temp_df.loc[index_max, 'profit_q4_es_dif' ]
                        revenue_anchor_growth= temp_df.loc[index_max, 'revenue_q4_es' ]
                        cf_oper_anchor_growth= temp_df.loc[index_max, 'cf_oper_q4_es' ]
                    else :
                        # update anchor 
                        index_temp1 = temp_df[ temp_df['profit_q4_es_dif']==temp_df['profit_q4_es_dif'].max() ]
                        index_max = index_temp1.index[0]
                        # index_max = temp_df.index[-1]
                        code_anchor_growth = temp_df.loc[index_max, 'code' ]
                        profit_anchor_growth= temp_df.loc[index_max, 'profit_q4_es' ]
                        profit_dif_anchor_growth= temp_df.loc[index_max, 'profit_q4_es_dif' ]
                        revenue_anchor_growth= temp_df.loc[index_max, 'revenue_q4_es' ]
                        cf_oper_anchor_growth= temp_df.loc[index_max, 'cf_oper_q4_es' ]
                else :
                    # update anchor 
                    index_temp1 = temp_df[ temp_df['profit_q4_es_dif']==temp_df['profit_q4_es_dif'].max() ]
                    index_max = index_temp1.index[0]
                    # index_max = temp_df.index[-1]
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

            # temp_df.to_csv("D:\\temp_list_181121.csv")

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


    def calc_shadow_ev_from_anchor(self,temp_list, ind_level='3' ) :
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
        # ind_level='3' 
        col_name_ind = 'ind'+ ind_level + '_code' # 'ind1_code'

        for index01 in temp_list.index :
            # index01 = temp_list.index.values[0]
            # temp_code = "600036.SH" 
            temp_code = temp_list.loc[index01,'code']
            print("Calculating code: ",temp_code )
            # only one row for code in temp_list
            # get industry-3 code for given code 
            str_ind_x = temp_list.loc[index01, col_name_ind]
            print('Working on industry code:')
            print( type(str_ind_x),str_ind_x )
            # print( temp_INDS_sum.index ) 
            # print( temp_INDS_sum.columns ) # Index(['profit_q4_es', 'revenue_q4_es', 'cf_oper_q4_es']
            # get summary values of financial indicators from industry list
            # todo if error for int|string, then using str(str_ind3)
            # 多重索引的dataframe取值一般使用xs,可以传入多个不同级别的索引进行筛选,但不支持同一级索引多选并且xs返回的是数值而不是引用
            # temp_INDS_sum.xs(['10','1010'],level=["ind1_code","ind2_code"]).loc['101010','profit_q4_es']
            # str_ind_x = str_ind3[:2]
            # str_ind2 = str_ind3[:4]
            # # temp_INDS_sum.xs(['10','1010'],level=["ind1_code","ind2_code"]).loc['101010','profit_q4_es']
            # profit_total_ind3 = temp_INDS_sum.xs([str_ind_x,str_ind2],level=["ind1_code","ind2_code"]).loc[str_ind3,'profit_q4_es']
            # revenue_total_ind3 = temp_INDS_sum.xs([str_ind_x,str_ind2],level=["ind1_code","ind2_code"]).loc[str_ind3,'revenue_q4_es']
            # cf_oper_total_ind3 = temp_INDS_sum.xs([str_ind_x,str_ind2],level=["ind1_code","ind2_code"]).loc[str_ind3,'cf_oper_q4_es']
            # # df_ind3 =temp_INDS_sum[ temp_INDS_sum['ind3_code']== int(str_ind3) ]
            df_ind_x = temp_list[temp_list[col_name_ind] == str_ind_x ]
            profit_total_ind_x = df_ind_x['profit_q4_es'].sum()
            revenue_total_ind_x = df_ind_x['revenue_q4_es'].sum()    
            cf_oper_total_ind_x = df_ind_x['cf_oper_q4_es'].sum()    

            try:
                # 计算行业占比目前没看到有什么用，但是未来引入行业总增长率时，对于计算行业内部竞争可能有用。逻辑是不可能每个公司都均匀获得增长
                temp_list.loc[index01, 'ind'+ ind_level +'_pct_profit_q4_es'] =temp_list.loc[index01,'profit_q4_es']/profit_total_ind_x
                temp_list.loc[index01, 'ind'+ ind_level +'_pct_revenue_q4_es'] =temp_list.loc[index01,'revenue_q4_es']/revenue_total_ind_x
                temp_list.loc[index01, 'ind'+ ind_level +'_pct_cf_oper_q4_es'] =temp_list.loc[index01,'cf_oper_q4_es']/cf_oper_total_ind_x 
                # step 2 2，计算基准股票总价值换算比例 |{s_value/base_value +s_growth/base_growth }，比如基准股票A总价值100e，个股S总价值20e，总市值(价值)比例0.2=20/100
                # 如果净利润太低，用壳价值(30e in 2016 and drop as time passes)或取基准股票的小系数如0.01*MV_base决定
                code_anchor_value  =  temp_list.loc[index01, 'code_anchor_value'] 
                code_anchor_growth =  temp_list.loc[index01, 'code_anchor_growth']
                # get s_value/base_value 价值的角度应该尽量给与一个合适的数值
                # temp_list.loc[index01, 'code_anchor_value'] 
                if temp_list.loc[index01, 'profit_anchor_value'] > 0 : 
                    if temp_list.loc[index01 , 'profit_q4_es'] >0 :
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
                print('=============df_ind_x')
                print( df_ind_x )
                print( type(temp_list.loc[index01,"profit_q4_es"].values[0]) )
                print( type(temp_INDS_sum.loc[df_ind_x.index,"profit_q4_es"].values[0]) )
                print("====================")
                print(profit_total_ind_x,revenue_total_ind_x,cf_oper_total_ind_x  )
              
        # cols_new mean new columns for temp_list
        cols_new= []
        cols_new=cols_new+[ 'ind'+ ind_level +'_pct_profit_q4_es', 'ind'+ ind_level +'_pct_revenue_q4_es', 'ind'+ ind_level +'_pct_cf_oper_q4_es']
        cols_new=cols_new+['para_value', 'para_growth'  ]
        # equivalent to "para_value" , whcih using 1 for anchor stock, still need to be devided by sum of columns
        #todo create w_ind_x_value from para_value 
        list_ind_x = list( temp_list[col_name_ind].drop_duplicates()  )  
        # [401010, 601020, 201050, 551010, 201060, 151040, ....
        for str_ind_x in list_ind_x : 
            df_ind_x = temp_list[temp_list[col_name_ind] == str_ind_x ]
            # print("df_ind_x")
            # print(df_ind_x)
            # print("================================================")
            # print(  temp_list.loc[df_ind_x.index,"para_value"] )
            # print( temp_list.loc[df_ind_x.index,"para_value"].sum()  )
            # print("================================================")
            # print( temp_list.loc[df_ind_x.index,"para_growth"] )  
            # print( temp_list.loc[df_ind_x.index,"para_growth"].sum()  )
            # print("================================================")

            temp_list.loc[df_ind_x.index,"w_allo_value_"+'ind'+ ind_level ] = temp_list.loc[df_ind_x.index,"para_value"]/temp_list.loc[df_ind_x.index,"para_value"].sum() 
            temp_list.loc[df_ind_x.index,"w_allo_growth_"+'ind'+ ind_level ] =temp_list.loc[df_ind_x.index,"para_growth"]/temp_list.loc[df_ind_x.index,"para_growth"].sum() 
  
        cols_new=cols_new+["w_allo_value_"+'ind'+ ind_level, "w_allo_growth_"+'ind'+ ind_level  ]
 
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

    def output_anchor_list_indX(self,ind_level,date_reference_change   ) :
        # save anchor list to csv file for given industry level 0~3 
        # last 181130 | since 181129

        ##############################################################################
        ### Application ABM=model
        ##############################################################################
        # ### Initialize ABM model engine 
        # ### step 1 获取原始研究数据,data_in 经过研究员初步梳理后的结构化信息 
        # # 7个indicator从属于 核心因子:[行业，流动性，动量，主动收益，价值，成长，
        # # 资本结构，财务优势，经营能力，人力优势，信息优势] 
        # # Merge additional data into one pd： create/import database file,update data 
        # # with imported new info, save to database file.

        # # abm_model = Abm_model()
        # # dataG = abm_model.load_symbol_universe('','' )
        # dataG = self.load_symbol_universe('','' )
        # ### Get historical fundamental financial and capital data
        # [df_tb_fi_fi, df_tb_fi_cap] =self.get_histData_finance_capital()

        # ##################################################################
        # ### Calculate analyzing indicators for given index conponents and time periods
        # '''
        # INPUT:dataG
        # ALGO:
        # OUTPUT: temp_list
        # '''
        # ##################################################################
        # ### METHOD1: get symbol list for 1 index 
        # # temp_list = abm_model.get_symbol_list( dataG, '000300.SH','2014-05-31')
        # # temp_list = abm_model.get_symbol_list( dataG, '000300.SH',temp_date)
        # # get symbol list for all indexes

        # ## Get symbol list for all indexes 
        # # originally, we simply set start date equals to adjusting date of index,

        # temp_list = abm_model.get_all_list( dataG, date_reference_change )

        # print('==========================')
        # print('temp_list, length ', len(temp_list['code']) )
        # print(temp_list.info() ) 

        # ### Calculate financial estimates for current period
        # # (temp_list,temp_date,df_tb_fi_fi)
        # [temp_list,cols_new_es] = abm_model.calc_financial_estimates(temp_list,date_reference_change,df_tb_fi_fi)

        # ### calculate weights of asset allocation for industry hierachy from level1 to level3 
        # [temp_list,cols_new_w_allo] = abm_model.calc_weight_allo_ind_hierachy(temp_list )

        # ### calculate for anchor stocks in value and growth perspective
        # # ind_level='1'
        # [temp_list,cols_new_anchor] = abm_model.calc_anchor_stocks(temp_list,ind_level)
        # # it is fine until here, only 3 stock with no profit estimates
        # # temp_list.to_csv("D:\\temp_list_anchor_181121.csv")

        # ### Get all 

        # ### calculate shadow enterprise value from anchor stocks in value and growth perspectives
        # [temp_list,cols_new_shadow] = abm_model.calc_shadow_ev_from_anchor(temp_list, ind_level) 
        # # temp_list.to_csv("D:\\temp_list_anchor_2_181121.csv")
        # # ##################################################################
        # # ### get annalytical dateframe for specific industry     
        # # '''
        # # INPUT: temp_list,int_ind3
        # # ALGO:
        # # OUTPUT: temp_df_growth,temp_df_value
        # # '''
        # # temp_list.to_csv("D:\\CISS_db\\temp\\temp.list.csv" )

        # ##################################################################
        # ### METHOD 2 Import from existing file 
        # weights_20140531_1.csv | D:\CISS_db\abm_weights
        path_temp = "D:\\CISS_db\\abm_weights\\"
        date_str = date_reference_change.replace("-","")
        ind_level_1 = "1"
        file_name = "weights_"+ date_str + "_"+ ind_level_1 +".csv"
        df_temp = pd.read_csv( path_temp+file_name )
        print("df_temp")
        print( df_temp.head() )
        print( df_temp.info() ) 
        ### output temp_list file to csv 
        # int_ind_x = "999" 
 
        return df_temp

    def test_abm_1port_Nperiods_ind3(self,sty_v_g,date_periods,int_ind3,port_name,init_cash,dataG,df_tb_fi_fi, df_tb_fi_cap,abm_model) :
        '''
        last 181115 | since 181115 
        derived from  def test_abm_1port_1period
        '''
        #######################################################################
        import datetime as dt
        if_period_1st = 0 
        for temp_i in range( len(date_periods.periods_start) ) :
            date_reference_change= date_periods.periods_reference_change[temp_i]
            date_start = date_periods.periods_start[temp_i]
            date_end =  date_periods.periods_end[temp_i]
            # datetime to string 
            date_reference_change=  dt.datetime.strftime( date_periods.periods_reference_change[temp_i] ,"%Y-%m-%d" )
            date_start = dt.datetime.strftime( date_periods.periods_start[temp_i],"%Y-%m-%d" )
            date_end =  dt.datetime.strftime( date_periods.periods_end[temp_i],"%Y-%m-%d" )

            # suit only for one period
            if_period_1st = temp_i # 0 means the first period
            print("if_period_1st ",if_period_1st)

            if if_period_1st == 0 :
                # The first period that we need to generate a new port folio
                # we need port_name to genarate portfolio head 
                (portfolio_manage,portfolio_suites) = self.gen_abm_1port_1period_ind3(sty_v_g,port_name,int_ind3,date_start,date_end,date_reference_change,init_cash,dataG,df_tb_fi_fi, df_tb_fi_cap,abm_model) 
            else :
                # port_name = portfolio_manage.portfolio_head["portfolio_name"] || port_id = portfolio_manage.portfolio_head["portfolio_id"] 
                (portfolio_manage,portfolio_suites) = self.update_abm_1port_1period_ind3(sty_v_g,portfolio_manage,int_ind3,date_start,date_end,date_reference_change,init_cash,dataG,df_tb_fi_fi, df_tb_fi_cap,abm_model) 

        return portfolio_manage,portfolio_suites

    def gen_abm_1port_1period_ind3(self,sty_v_g,port_name,int_ind3,date_start,date_end,date_reference_change,init_cash,dataG,df_tb_fi_fi, df_tb_fi_cap,abm_model) :
        '''
        For the first time,genarate and calculate the whole portfolio suites for ABM model.

        Warning : 
        1,only accept period within which no stockpool/fundamental change.
        2,Cannot be used to calculate some portfolio suites that already exists.

        INPUT: 
            date_start,date_end: start and end date for the period
            date_reference_change:the date used to get fundamental(csi index adjusting date)
        ALGO:  
        OUTPUT: 

        run ABM model for 1 portfolio(i.e. int_ind3='401010') and 1 period
        last 181115 | since 181115 
        derived from test_abm.py,test_abm_1port_Nperiods,test_abm_1port_1period,
            def test_abm_1port_1period
        ''' 

        ##################################################################
        ### Import requirement outside modules
        sys.path.append("..") 
        import json
        import datetime as dt 

        ### Import personal model engine 
        # from bin.abm_engine import Abm_model
        ### Import data_IO modules
        from db.data_io import data_wind

        ### Import CISS modules
        path_base= "D:\\CISS_db\\" 
        # strategy and relative functions 
        from db.func_stra import stra_allocation
        # alogrithm and optimizer module 
        from db.algo_opt import optimizer
        # singals that connect strategy and simulation 
        from db.signals import signals
        # trade management
        from db.trades import manage_trades
        # generating,operating modules for portfolio import,updatean output
        from db.ports import gen_portfolios,manage_portfolios
        # abm模型的参数 config_apps_abm
        from config.config_apps_abm import config_apps_abm
        from config.config_IO import config_IO

        # import time module 
        from db.times import times
        times0 = times('CN','SSE')
        method4time='stock_index_csi'
        ##################################################################
        ### Initialize common configurations and variables

        # ##############################################################################
        # ### Quotation data(historical,feed,...) preperation , using symbol and time period
        # ### get data by downloading from Wind-API
        # # Qs：we do not own a merged symbol list for multiperiods 
        # '''
        # INPUT:symbol_list ,date_start,data_end,config_IO_0
        # ALGO: data_wind
        # OUTPUT: quotation data files
        # '''
        # # dataG.date_list
        # # dataG.datagroup[temp_index+'_'+temp_date]

        # date_start = temp_date.replace("-","") # 20140531" 
        # date_end =  temp_date_now   # temp_date2.replace("-","")  # "20141130"
        # #todo get symbol list to replace stockpool_df['code']
        # # 下载stockpool里所有股票day,or week数据，stockpool_df['code']
        # for temp_code in symbol_list :
        #     symbols = temp_code #  '600036.SH' 
        #     # multi-codes with multi-indicators is not supported 
        #     wd1 = data_wind('' ,'' ).data_wind_wsd(symbols,date_start,date_end,'day')
        #     print('symbols ',symbols )
        #     print(wd1.wind_head )
         
        #     # print(wd1.wind_df )
        #     # output wind object to json and csv file 
        #     file_json = wd1.wind_head['id']  +'.json'
        #     with open( config_IO_0.path_base_data  + file_json ,'w') as f:
        #         json.dump( wd1.wind_head  ,f) 
        #     file_csv =  wd1.wind_head['id'] +'.csv'
        #     wd1.wind_df.to_csv(config_IO_0.path_base_data  +file_csv )

        ##############################################################################
        ### Application ABM=model
        ##############################################################################
        ### Initialize ABM model engine 
        ### step 1 获取原始研究数据,data_in 经过研究员初步梳理后的结构化信息 
        # 7个indicator从属于 核心因子:[行业，流动性，动量，主动收益，价值，成长，
        # 资本结构，财务优势，经营能力，人力优势，信息优势] 
        # Merge additional data into one pd： create/import database file,update data 
        # with imported new info, save to database file.

        # # abm_model = Abm_model()
        # # dataG = abm_model.load_symbol_universe('','' )
        # dataG = self.load_symbol_universe('','' )
        # ### Get historical fundamental financial and capital data
        # [df_tb_fi_fi, df_tb_fi_cap] =self.get_histData_finance_capital()

        ##################################################################
        ### Calculate analyzing indicators for given index conponents and time periods
        '''
        INPUT:dataG
        ALGO:
        OUTPUT: temp_list
        '''
        ##################################################################
        ### METHOD1: get symbol list for 1 index 
        # temp_list = abm_model.get_symbol_list( dataG, '000300.SH','2014-05-31')
        # temp_list = abm_model.get_symbol_list( dataG, '000300.SH',temp_date)
        # get symbol list for all indexes

        ## Get symbol list for all indexes 
        # originally, we simply set start date equals to adjusting date of index,

        temp_list = abm_model.get_all_list( dataG, date_reference_change )

        print('==========================')
        print('temp_list, length ', len(temp_list['code']) )
        print(temp_list.info() ) 

        ### Calculate financial estimates for current period
        # (temp_list,temp_date,df_tb_fi_fi)
        [temp_list,cols_new_es] = abm_model.calc_financial_estimates(temp_list,date_reference_change,df_tb_fi_fi)

        ### calculate weights of asset allocation for industry hierachy from level1 to level3 
        [temp_list,cols_new_w_allo] = abm_model.calc_weight_allo_ind_hierachy(temp_list)

        ### calculate for anchor stocks in value and growth perspective
        ind_level='3'
        [temp_list,cols_new_anchor] = abm_model.calc_anchor_stocks(temp_list,ind_level)

        ### calculate shadow enterprise value from anchor stocks in value and growth perspectives

        [temp_list,cols_new_shadow] = abm_model.calc_shadow_ev_from_anchor(temp_list, ind_level) 

        # ##################################################################
        # ### get annalytical dateframe for specific industry     
        # '''
        # INPUT: temp_list,int_ind3
        # ALGO:
        # OUTPUT: temp_df_growth,temp_df_value
        # '''
        # temp_list.to_csv("D:\\CISS_db\\temp\\temp.list.csv" )

        ##################################################################
        ### METHOD2: Load annalytical dateframe for specific industry   
        # # Notes:  temp_list["ind3_code"] is int in method1 and string in method2
        # # Note: only available for one period
        # temp_list = pd.read_csv("D:\\CISS_db\\temp\\temp.list_20140531.csv" )
        # # change 1 column to str: temp_list['ind3_code'].apply(str)
        # # change whole frame to string :temp_list.applymap(str)
        # temp_list['ind3_code'] = temp_list['ind3_code'].apply(str)

        ##################################################################
        print("temp_list has been loaded.")
        # print( temp_list.info())

        print("Working on industry 3 :", int_ind3 )

        ### Get allocation weights for industry level 3 :
        ### Value allocation in industry level 3 
        # Notes:  temp_list["ind3_code"] is int in method1 and string in method2
        temp_df_value = temp_list[ temp_list["ind3_code"] ==  int_ind3  ]
        # temp_df_value["ind3_pct_profit_q4_es"].sum() # 1 
        print('VALUE:weight allocation for industry: temp_df_value')
        print(temp_df_value["w_allo_value_ind3"].sum() )
        print(temp_df_value["w_allo_value_ind3"] ) 

        ### Growth allocation in industry level 3 
        temp_df_growth = temp_list[ temp_list["ind3_code"] ==  int_ind3   ]
        # temp_df_growth["ind3_pct_profit_q4_es"].sum() # 1 
        print('GROWTH:weight allocation for industry: ')
        print(temp_df_growth["w_allo_growth_ind3"].sum() )
        print(temp_df_growth["w_allo_growth_ind3"] ) 
        # equivalent to "para_value" , whcih using 1 for anchor stock, still need to be devided by sum of columns

        ### save result to temp file directory  
        # 若 apps和abm两级文件夹都要新建，则不用mkdir，用makedirs
        temp_path = "D:\\CISS_db\\temp\\"
        import os
        if not os.path.isdir( temp_path) :
            os.makedirs(temp_path)  
        # temp_df_growth.to_csv(temp_path + "temp_df.csv"  )
        temp_df_value.to_csv(temp_path + "temp_df_value_"+ str(int_ind3)+"_"+ date_reference_change +".csv"  )
        temp_df_growth.to_csv(temp_path + "temp_df_growth_"+ str(int_ind3)+"_"+ date_reference_change +".csv"  )

        ##############################################################################
        ### Portfolio simulation using CISS standarded modules.
        #  example ,confiuration file and portfolio data 
        ##############################################################################
        ### Initialize portfolio example  
        '''
        INPUT: config_port ,port_name
        ALGO: 
        OUTPUT: portfolio_0 
        '''
        ## temp_date 后第一个交易日T开始建仓，初始资金{1,5,10,50,100,500}亿元
        # port_name=  name_id
        # name_id='sys_rc001' if it is a system with multiple portfolios 
        # name_id='tree_rc001' if it is a tree structure with multiple systems 
        config_port={} 
        portfolio_gen = gen_portfolios(config_port ,port_name )
        port_id = portfolio_gen.port_id
        port_head = portfolio_gen.port_head
        port_df = portfolio_gen.port_df

        print("Portfolio has been generated. ")
        print( portfolio_gen.port_head['portfolio_name'] )

        ##############################################################################
        ### Portfolio configurations
        # date_start= temp_date,  date_end=temp_date2
        date_start = date_start.replace("-","") # 20140531" 
        date_end = date_end.replace("-","")  # "20141130"
        # port_name=  name_id
        config_apps = config_apps_abm(init_cash,date_start,date_end,port_name ) 

        
        ### generate portfolio_suites object with  AS,Asum,trades,signal
        '''
        INPUT: config_apps,temp_df_growth,sp_name0,port_name
        ALGO: gen_port_suites 
        OUTPUT: stockpool_0,account_0,trades_0, signals_0 contents
        '''
        sp_name0=  str(int_ind3)  
        portfolio_suites = portfolio_gen.gen_port_suites(port_head,config_apps,temp_df_growth,sp_name0,port_name)
        print('portfolio_suites has been generated.')

        ### use portfolio ID to load portfolio which just be generated  
        port_id = portfolio_gen.port_head["portfolio_id"]   # id_port_1541729640_rc001_401010
        port_name=  portfolio_gen.port_head["portfolio_name"] #'port_rc001' 

        ### load configuration of portfolio 
        config= config_IO('').load_config_IO_port(port_id,path_base,port_name) 
        config_port = config
        ### save abm-model analytical data to aaps file directory of portfolio 
        import os
        if not os.path.isdir( config_port['path_apps'] ) :
            os.makedirs( config_port['path_apps'] )  
        # temp_df_growth.to_csv(temp_path + "temp_df.csv"  )
        temp_df_value.to_csv(config_port['path_apps']  + "temp_df_value_"+ str(int_ind3)+"_"+ date_reference_change +".csv"  )
        temp_df_value.to_csv(config_port['path_apps']  + "temp_df_growth_"+ str(int_ind3)+"_"+ date_reference_change +".csv"  )

        path_base = config['path_base']
        ### get portfolio object 
        portfolio_manage = manage_portfolios(path_base,config,port_name,port_id )

        ### load portfolio information
        # 导入port数据，确定需要计算的时间周期; port_head记录，导入AS,Asum,sp,trade，signal,signal_nextday等数据
        # (port_head,port_df,config_IO_0 )= portfolio_manage.load_portfolio(port_id,path_base,port_name )

        port_head =portfolio_manage.port_head
        port_df  =portfolio_manage.port_df
        config_IO_0 = portfolio_manage.config_IO_0       
        print("port_head")
        print( port_head ) 

        ##############################################################################
        '''The strategy process just began.
        the strategy is simple:
        1, for current date, judge if change of symbol universe 
        2,if yes, run ideal weights allocation and generate ana,signal,tradeplan 
        '''
        ##############################################################################
        ### get stockpool 
        stockpool_df = portfolio_suites.stockpool.sp_df
        print("Info of stockpool:")
        print( stockpool_df.info() )

        ##############################################################################
        ### Quotation data(historical,feed,...) for stockpool
        ### Method 1:get data by downloading from Wind-API 
        '''
        INPUT:symbol_list ,date_start,data_end,config_IO_0
        ALGO: data_wind
        OUTPUT: quotation data files
        '''  
        # 下载stockpool里所有股票day,or week数据，stockpool_df['code']
        # for temp_code in stockpool_df['code'] :
        #     symbols = temp_code #  '600036.SH' 
        #     # multi-codes with multi-indicators is not supported 
        #     wd1 = data_wind('' ,'' ).data_wind_wsd(symbols,date_start,date_end,'day')
        #     print('symbols ',symbols )
        #     print(wd1.wind_head )
        #     # output wind object to json and csv file 
        #     file_json = wd1.wind_head['id']  +'.json'
        #     with open( config_IO_0['path_data']+ file_json ,'w') as f:
        #         json.dump( wd1.wind_head  ,f) 
        #     file_csv =  wd1.wind_head['id'] +'.csv'
        #     wd1.wind_df.to_csv(config_IO_0['path_data']+file_csv ) 

        ### Method 2: Load quote data from existing quotation directory 
        path0 = 'D:\\db_wind\\'
        path0 = 'D:\\data_Input_Wind\\'
        data_wind_0 = data_wind('' ,path0 )
        quote_type='CN_day'

        ############################################################################## 
        ### Run strategy for allocation weights 
        # Straetgy could be a roughly estimation of how many stock to trade
        # 策略是粗线条的，只说我们要买入多少比例的股票
        stra_weight_list = stra_allocation('').stock_weights(sty_v_g,stockpool_df)
        print('weight_list of strategy:')
        print(stra_weight_list)

        ### Build value and growth portfolio, and a mixed portfolio that can dynamically changed over time  
        # 建立 value 组合，growth组合，混合组合{weight_port=[0.5,0.5]}
        # 混合组合随着时间变动，根据业绩调整权重
        stra_estimates_group = {}
        stra_estimates_group['key_1'] = stra_weight_list

        ### Strategy optimizer 
        optimizer_weight_list = optimizer('').optimizer_weight(stra_estimates_group )
        ## 3 methods:
        ## 1, w_allo, only value(current choice )
        ## 2, w_allo, only growth
        ## 3, w_allo, only half value and half growth

        ##############################################################################
        ### Signal generator
        ## get signals by strategy estimations 
        # 交易信号是精细化的，对应了目标的数量，金额，持仓百分比等要素。
        portfolio_suites = signals('sig_stra_weight').update_signals_stock_weight(optimizer_weight_list,portfolio_suites)
        signals_list = portfolio_suites.signals.signals_df 

        ##############################################################################
        ### Trade management 
        ### Generate trade plan 
        ## when and which amrket to trade, price or volumne zone for setting trade plan 
        # load trade head file 
        manager_trades = manage_trades('')
        # sty_v_g,sty_v_g is used to judge value , growth or other styles. sty_v_g='value'
        trades = manager_trades.manage_tradeplan(sty_v_g,portfolio_suites, signals_list, config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
        print('trade_plan')
        print( trades.tradeplan )

        #### get trade details 
        trades= manager_trades.manage_tradebook(trades , config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
        print( 'trades.tradebook' )
        print( trades.tradebook )

        ##############################################################################
        ### Portfolio management 
        ### update trades in portfolio_suites
        portfolio_suites.trades = trades

        ### update accounts using trade result
        ## we only update trades that have not been used by accounts
        from db.accounts import manage_accounts

        ###  get trading days using account_sum and date_start,date_end 
        date_list = portfolio_suites.account.account_sum.index
        print('date_list')
        # print(date_list[date_list<date_end ] )
        #  2014-06-03 to 2014-11-28
        date_list_units = date_list[date_list<date_end ]
        trades_0 = portfolio_suites.trades
        tradebook = trades_0.tradebook

        ### get all trading dates from tradebook
        tradebook['datetime'] = pd.to_datetime(tradebook['date'], format='%Y-%m-%d' ) 
        tradebook =tradebook.sort_values('datetime')
        date_list_trades = list( tradebook['datetime'].drop_duplicates() )

        for temp_date in date_list_units  :
            if_trade =0
            if temp_date in date_list_trades :
                # date with trading 
                portfolio_suites = manage_accounts('').update_accounts_with_trades(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0)
                if_trade =1 

            # update closing price for all holding stocks, whether date with no trading or not, 
            portfolio_suites = manage_accounts('').update_accounts_with_quotes(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0,if_trade )
            ## we should update statistics results of portfolio_head file before output 
            # todo todo 

            ## for every trading day, Out portfolio_suites to files
            # temp_date  2014-06-03 00:00:00 type is time stamp
            temp_date = dt.datetime.strftime(temp_date,"%Y%m%d")
            print( "temp_date ",temp_date )
            port_head["date_LastUpdate"] = temp_date
            portfolio_suites = portfolio_manage.output_port_suites(temp_date,portfolio_suites,config_IO_0,port_head,port_df)


        return  portfolio_manage,portfolio_suites 


    def update_abm_1port_1period_ind3(self,sty_v_g,portfolio_manage,int_ind3,date_start,date_end,date_reference_change,init_cash,dataG,df_tb_fi_fi, df_tb_fi_cap,abm_model) :
        '''
        For some existed portfolio,
        Genarate and calculate the whole portfolio suites for ABM model.

        Warning : 
        1,only accept period within which no stockpool/fundamental change.
        2,Cannot be used to calculate some portfolio suites have not been generated.

        INPUT: 
            date_start,date_end: start and end date for the period
            date_reference_change:the date used to get fundamental(csi index adjusting date)
        ALGO:  
        OUTPUT: 

        run ABM model for 1 portfolio(i.e. int_ind3='401010') and 1 period
        
        update portfolio suites 
        last 181115 | since 181115 
        derived from gen_abm_1port_1period
        '''
        ##################################################################
        ### ### get portfolio object using input:portfolio_manage
        port_name = portfolio_manage.port_head["portfolio_name"] 
        port_id = portfolio_manage.port_head["portfolio_id"] 

        ### Import requirement outside modules
        sys.path.append("..") 
        import json

        ### Import personal model engine 
        # from bin.abm_engine import Abm_model
        ### Import data_IO modules
        from db.data_io import data_wind

        ### Import CISS modules
        path_base= "D:\\CISS_db\\" 
        # strategy and relative functions 
        from db.func_stra import stra_allocation
        # alogrithm and optimizer module 
        from db.algo_opt import optimizer
        # singals that connect strategy and simulation 
        from db.signals import signals
        # trade management
        from db.trades import manage_trades
        # generating,operating modules for portfolio import,updatean output
        from db.ports import gen_portfolios,manage_portfolios
        # abm模型的参数 config_apps_abm
        from config.config_apps_abm import config_apps_abm
        from config.config_IO import config_IO

        # import time module 
        from db.times import times
        times0 = times('CN','SSE')
        method4time='stock_index_csi'

        ##############################################################################
        ### Application ABM=model
        ##############################################################################
        ### Initialize ABM model engine 
        ### step 1 获取原始研究数据,data_in 经过研究员初步梳理后的结构化信息 
        # 7个indicator从属于 核心因子:[行业，流动性，动量，主动收益，价值，成长，
        # 资本结构，财务优势，经营能力，人力优势，信息优势] 
        # Merge additional data into one pd： create/import database file,update data 
        # with imported new info, save to database file.

        # # abm_model = Abm_model()
        # # dataG = abm_model.load_symbol_universe('','' )
        # dataG = self.load_symbol_universe('','' )
        # ### Get historical fundamental financial and capital data
        # [df_tb_fi_fi, df_tb_fi_cap] =self.get_histData_finance_capital()

        ##################################################################
        ### Calculate analyzing indicators for given index conponents and time periods
        '''
        INPUT:dataG
        ALGO:
        OUTPUT: temp_list
        '''
        ### get symbol list for 1 index 
        # temp_list = abm_model.get_symbol_list( dataG, '000300.SH','2014-05-31')
        # temp_list = abm_model.get_symbol_list( dataG, '000300.SH',temp_date)
        # get symbol list for all indexes

        ### Get symbol list for all indexes 
        # originally, we simply set start date equals to adjusting date of index,

        temp_list = abm_model.get_all_list( dataG, date_reference_change )

        print('==========================')
        print('temp_list, length ', len(temp_list['code']) )
        print(temp_list.info() ) 

        ### Calculate financial estimates for current period
        # (temp_list,temp_date,df_tb_fi_fi)
        [temp_list,cols_new_es] = abm_model.calc_financial_estimates(temp_list,date_reference_change,df_tb_fi_fi)

        ### calculate weights of asset allocation for industry hierachy from level1 to level3 
        [temp_list,cols_new_w_allo] = abm_model.calc_weight_allo_ind_hierachy(temp_list)

        ### calculate for anchor stocks in value and growth perspective
        ind_level='3'
        [temp_list,cols_new_anchor] = abm_model.calc_anchor_stocks(temp_list,ind_level)

        ### calculate shadow enterprise value from anchor stocks in value and growth perspectives
        [temp_list,cols_new_shadow] = abm_model.calc_shadow_ev_from_anchor(temp_list, ind_level) 

        ##################################################################
        ### get annalytical dateframe for specific industry     
        '''
        INPUT: temp_list,int_ind3
        ALGO:
        OUTPUT: temp_df_growth,temp_df_value
        '''
        print("Working on industry 3 :", int_ind3 )

        ### Get allocation weights for industry level 3 :
        ### Value allocation in industry level 3 
        temp_df_value = temp_list[ temp_list["ind3_code"] == int_ind3  ]
        # temp_df_value["ind3_pct_profit_q4_es"].sum() # 1 
        print('VALUE:weight allocation for industry: temp_df_value')
        print(temp_df_value["w_allo_value_ind3"].sum() )
        print(temp_df_value["w_allo_value_ind3"] ) 

        ### Growth allocation in industry level 3 
        temp_df_growth = temp_list[ temp_list["ind3_code"] == int_ind3  ]
        # temp_df_growth["ind3_pct_profit_q4_es"].sum() # 1 
        print('GROWTH:weight allocation for industry: ')
        print(temp_df_growth["w_allo_growth_ind3"].sum() )
        print(temp_df_growth["w_allo_growth_ind3"] ) 
        # equivalent to "para_value" , whcih using 1 for anchor stock, still need to be devided by sum of columns

        ### save result to temp file directory  
        # 若 apps和abm两级文件夹都要新建，则不用mkdir，用makedirs
        temp_path = "D:\\CISS_db\\temp\\"
        import os
        if not os.path.isdir( temp_path) :
            os.makedirs(temp_path)  
        # temp_df_growth.to_csv(temp_path + "temp_df.csv"  )
        temp_df_value.to_csv(temp_path + "temp_df_value_"+ str(int_ind3)+"_"+ date_reference_change +".csv"  )
        temp_df_growth.to_csv(temp_path + "temp_df_growth_"+ str(int_ind3)+"_"+ date_reference_change +".csv"  )

        ##############################################################################
        ### Portfolio simulation using CISS standarded modules.
        #  example ,confiuration file and portfolio data 
        ##############################################################################
        ##############################################################################
        ### Load portfolio example  
        '''
        INPUT: config_port ,port_name
        ALGO: 
        OUTPUT: portfolio_gen 
        ''' 
        print("Portfolio to be loaded. ")
         
        ##############################################################################
        ### Portfolio configurations
        # date_start= temp_date,  date_end=temp_date2
        date_start = date_start.replace("-","") # 20140531" 
        date_end = date_end.replace("-","")  # "20141130"
        # port_name=  name_id
        config_apps = config_apps_abm(init_cash,date_start,date_end,port_name )

        ### generate portfolio_suites object with  AS,Asum,trades,signal
        '''
        INPUT: config_apps,temp_df_growth,sp_name0,port_name
        ALGO: gen_port_suites
        OUTPUT: stockpool_0,account_0,trades_0, signals_0 contents
        '''
        sp_name0=  str(int_ind3)  

        ##############################################################################
        ### Load latest portfolio suites 
        # 若temp_date=20141130为周末，非交易日，则需要选定一个外部文件里最接近的交易日,如 20141127
        date_LastUpdate = portfolio_manage.port_head["date_LastUpdate"]  
        port_head =portfolio_manage.port_head
        port_id = portfolio_manage.port_head["portfolio_id"]   # id_port_1541729640_rc001_401010
        port_name=  portfolio_manage.port_head["portfolio_name"] #'port_rc001'         
        port_df  =portfolio_manage.port_df
        config_IO_0 = portfolio_manage.config_IO_0  

        portfolio_suites = portfolio_manage.load_portfolio_suites(date_LastUpdate,config_IO_0,port_head,port_df,port_name,sp_name0)
        
        
        id_time_stamp = portfolio_manage.port_head["portfolio_id_time"]
        sp_head = portfolio_suites.stockpool.sp_head
        
        ##############################################################################
        ### Crutial: assign new stockpool df to portfolio suites
        sp_df =  temp_df_value # a new one 
        portfolio_suites.stockpool.sp_df = sp_df

        ##############################################################################
        ### load configuration of portfolio 
        config= config_IO('').load_config_IO_port(port_id,path_base,port_name) 
        config_port = config
        ### save abm-model analytical data to aaps file directory of portfolio 
     

        ############################################################################
        ### From here, it is the same to contents in gen_abm_1port_1period
        ############################################################################
        ##############################################################################
        '''The strategy process just began.
        the strategy is simple:
        1, for current date, judge if change of symbol universe 
        2,if yes, run ideal weights allocation and generate ana,signal,tradeplan 
        '''
        ##############################################################################
        ### get stockpool 
        stockpool_df = portfolio_suites.stockpool.sp_df
        print("Info of stockpool:")
        print( stockpool_df.info() )

        ##############################################################################
        ### Quotation data(historical,feed,...) for stockpool
        ### Method 1:get data by downloading from Wind-API 
        '''
        INPUT:symbol_list ,date_start,data_end,config_IO_0
        ALGO: data_wind
        OUTPUT: quotation data files
        '''  
        # 下载stockpool里所有股票day,or week数据，stockpool_df['code']
        # for temp_code in stockpool_df['code'] :
        #     symbols = temp_code #  '600036.SH' 
        #     # multi-codes with multi-indicators is not supported 
        #     wd1 = data_wind('' ,'' ).data_wind_wsd(symbols,date_start,date_end,'day')
        #     print('symbols ',symbols )
        #     print(wd1.wind_head )
        #     # output wind object to json and csv file 
        #     file_json = wd1.wind_head['id']  +'.json'
        #     with open( config_IO_0['path_data']+ file_json ,'w') as f:
        #         json.dump( wd1.wind_head  ,f) 
        #     file_csv =  wd1.wind_head['id'] +'.csv'
        #     wd1.wind_df.to_csv(config_IO_0['path_data']+file_csv ) 

        ### Method 2: Load quote data from existing quotation directory 
        path0 = 'D:\\db_wind\\'
        path0 = "D:\\data_Input_Wind\\"
        data_wind_0 = data_wind('' ,path0 )
        quote_type='CN_day'

        ############################################################################## 
        ### Run strategy for allocation weights 
        # Straetgy could be a roughly estimation of how many stock to trade
        # 策略是粗线条的，只说我们要买入多少比例的股票
        stra_weight_list = stra_allocation('').stock_weights(sty_v_g, stockpool_df)
        print('weight_list of strategy:')
        print(stra_weight_list)

        ### Build value and growth portfolio, and a mixed portfolio that can dynamically changed over time  
        # 建立 value 组合，growth组合，混合组合{weight_port=[0.5,0.5]}
        # 混合组合随着时间变动，根据业绩调整权重
        stra_estimates_group = {}
        stra_estimates_group['key_1'] = stra_weight_list

        ### Strategy optimizer 
        optimizer_weight_list = optimizer('').optimizer_weight(stra_estimates_group )
        ## 3 methods:
        ## 1, w_allo, only value(current choice )
        ## 2, w_allo, only growth
        ## 3, w_allo, only half value and half growth

        ##############################################################################
        ### Signal generator
        ## get signals by strategy estimations 
        # 交易信号是精细化的，对应了目标的数量，金额，持仓百分比等要素。
        portfolio_suites = signals('sig_stra_weight').update_signals_stock_weight(optimizer_weight_list,portfolio_suites)
        signals_list = portfolio_suites.signals.signals_df 
        print('signals_list')
        print(signals_list )
        ##############################################################################
        ### Trade management 
        ### Generate trade plan 
        ## when and which amrket to trade, price or volumne zone for setting trade plan 
        # load trade head file 
        manager_trades = manage_trades('')
        (trades,quote_df) = manager_trades.manage_tradeplan(sty_v_g,portfolio_suites, signals_list, config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
        print('trade_plan')
        print( trades.tradeplan )

        #### get trade details 
        trades= manager_trades.manage_tradebook(trades  , config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
        print( 'trades.tradebook' )
        print( trades.tradebook )

        ##############################################################################
        ### Portfolio management 
        ### update trades in portfolio_suites
        portfolio_suites.trades = trades

        ### update accounts using trade result
        ## we only update trades that have not been used by accounts
        from db.accounts import manage_accounts

        ###  get trading days using account_sum and date_start,date_end 
        date_list = portfolio_suites.account.account_sum.index
        print('date_list')
        # print(date_list[date_list<date_end ] )
        #  2014-06-03 to 2014-11-28
        date_list_units = date_list[date_list<date_end ]
        trades_0 = portfolio_suites.trades
        tradebook = trades_0.tradebook

        ### get all trading dates from tradebook
        tradebook['datetime'] = pd.to_datetime(tradebook['date'], format='%Y-%m-%d' ) 
        tradebook =tradebook.sort_values('datetime')
        date_list_trades = list( tradebook['datetime'].drop_duplicates() )

        for temp_date in date_list_units  :
            if_trade =0
            if temp_date in date_list_trades :
                # date with trading 
                portfolio_suites = manage_accounts('').update_accounts_with_trades(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0)
                if_trade =1 

            # update closing price for all holding stocks, whether date with no trading or not, 
            portfolio_suites = manage_accounts('').update_accounts_with_quotes(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0,if_trade )
            ## we should update statistics results of portfolio_head file before output 
            # todo todo 

            ## for every trading day, Out portfolio_suites to files
            portfolio_suites = portfolio_manage.output_port_suites(temp_date,portfolio_suites,config_IO_0,port_head,port_df)


        return  portfolio_manage,portfolio_suites 
