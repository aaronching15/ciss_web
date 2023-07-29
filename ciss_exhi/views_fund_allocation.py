# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
fundction:基金股债配置测算
todo:

last  | since 20221108
refernce:derived from views_fund_analysis
===============================================
''' 

###########################################################################
### Initialization
from operator import index
from django.shortcuts import render
from django.http import JsonResponse
from django.http import HttpResponse 
from django.views.decorators.csrf import csrf_protect,requires_csrf_token,csrf_exempt

###########################################################################
### 导入项目地址
import sys,os

# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"\\CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\analysis_indicators\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

import pandas as pd
import numpy as np 
import json 
sys.path.append("..") 
import datetime as dt  
time_now = dt.datetime.now()
### 获取近1w、1m、3m、6m、1year日期
time_now_str = dt.datetime.strftime(time_now   , "%Y%m%d")
time_now_str_pre1d = dt.datetime.strftime(time_now - dt.timedelta(days=1)  , "%Y%m%d")


###########################################################################
### fund_allocation 基金股债配置测算  
@csrf_protect
@requires_csrf_token
@csrf_exempt
def fund_allocation(request):
    ###########################################################################
    ### 脚本来源：file= test_fund_ana.py
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    #################################################################################
    ### 导入常用地址信息
    import os
    path_pms =  os.getcwd().split("ciss_web")[0]+"\\data_pms\\"  
    path_wind_terminal = path_pms + "wind_terminal\\" 
    path_wss = path_pms + "wss\\"
    path_wpf = path_pms + "wpf\\"
    path_wpd = path_pms + "wpd\\"
    path_wsd = path_pms + "wsd\\"
    path_ciss_web = os.getcwd().split("ciss_web")[0]+"ciss_web\\"
    path_ciss_rc = path_ciss_web +"CISS_rc\\"
    # path_db = path_ciss_rc + "db\\"
    path_dt = path_ciss_rc + "db\\db_times\\"
    sys.path.append(path_ciss_rc+ "config\\")
    from config_data import config_data
    config_data_1 = config_data() 
    path_data_strategy = config_data_1.obj_config["dict"]["path_data_strategy"] 
    path_fund_allocation = path_data_strategy + "fund_allocation\\"


    #################################################################################
    ### Part 1 准备基础数据：1，基础组合和指数数据、基金的周频数据
    ### 给定区间，检查获取区间内的 指数、基金、股票 周频率的收益率数据
    if "input_fund_allo_check_data" in request.POST.keys():   
        date = request.POST.get("date_fund_allo_check_data","")
        code = request.POST.get("code_fund_allo_check_data","") 
        port_name = request.POST.get("port_name_fund_allo_check_data","")
        
        ### 获取近20个周的数据
        num_week = 20

        ##############################################
        ### 导入股票、债券、基金指数数据
        file_name = "quote_index_week.xlsx"
        df_index = pd.read_excel(path_fund_allocation+ file_name )
        # 升序排列
        df_index = df_index.sort_values(by="date",ascending=True )
        df_index = df_index.tail( num_week )
        df_index.index = df_index["date"]
        print("df_index \n",  df_index.head() ) 
        
        ##############################################
        ### 导入 拟合基金或组合的历史数据
        file_name = "quote_week_code_port.xlsx"
        df1 = pd.read_excel(path_fund_allocation+ file_name ) 
        df1= df1.sort_values(by="date",ascending=True )
        df1 = df1.tail( num_week )
        df1.index = df1["date"]
        print( df1.head() )

        ##############################################
        ### 判断指数和 “基金或组合” 历史日期数据是否匹配
        # port_name = 普通股票基金； 要改成 ret-w_普通股票基金
        if len( code ) < 1 :
            col_name = "ret-w_" + port_name
        else :
            col_name = "ret-w_" + code
        
        ### 获取近20个周的数据
        ### join参数的属性，如果为’inner’得到的是两表的交集，如果是outer，得到的是两表的并集。
        df1 = pd.concat([df1,df_index], axis=1, join="inner" )
        print("Debug \n" ,df1  )
        
        context["output_fund_allo_check_data"] =  str(num_week) +" == " + str( len(df1.index) ) 

    #################################################################################
    ### step 1	股债比例初算	用2个主流股票和2个债券指数，初步计算股票、债、现金配置比例w_s,w_b,1-w_s-w_b；
    # 计算近5、20周相对于基准指数的差异，如果指标差异不大用近20周，差异大用近5周
    # 核心指标：近5、20周区间收益率、回撤、波动绿、指标；回归系数；
    ##############################################
    ### 
    if "input_fund_allo_weight_raw" in request.POST.keys():   
        date = request.POST.get("date_fund_allo_weight_raw","")
        code = request.POST.get("code_fund_allo_weight_raw","") 
        ### port_name= "普通股票基金"
        port_name = request.POST.get("port_name_fund_allo_weight_raw","")
        
        ### 只保留近20个周的数据
        num_week = 20 

        ##############################################
        ### 导入股票、债券、基金指数数据
        file_name = "quote_index_week.xlsx"
        df_index = pd.read_excel(path_fund_allocation+ file_name )
        # 升序排列
        df_index = df_index.sort_values(by="date",ascending=True )
        df_index = df_index[ df_index["date"]<= date ] 
        df_index = df_index.tail( num_week )
        df_index.index = df_index["date"]
        print("df_index \n",  df_index.head() ) 
        
        ##############################################
        ### 导入 拟合基金或组合的历史数据
        file_name = "quote_week_code_port.xlsx"
        df1 = pd.read_excel(path_fund_allocation+ file_name ) 
        df1 = df1[ df1["date"]<= date ] 
        df1= df1.sort_values(by="date",ascending=True )
        df1 = df1.tail( num_week )
        df1.index = df1["date"]
        print( df1.head() )

        
        ### 合并数据
        ### join参数的属性，如果为’inner’得到的是两表的交集，如果是outer，得到的是两表的并集。
        df1 = pd.concat([df1,df_index], axis=1, join="inner" ) 
        ### 可能会有重复列 df1.loc[:,~df.columns.duplicated()]
        df1 = df1.loc[:, ~ df1.columns.duplicated()]

        ##############################################
        ### 计算相关系数：     x.corr(y)     # Pearson's r
        ## x.corr(y, method='spearman')  # Spearman's rho； x.corr(y, method='kendall')   # Kendall's tau
        print("Debug \n" ,df1  )
        ### ret-w_中证100	ret-w_沪深300	ret-w_创业板指	ret-w_国证成长	ret-w_国证价值	ret-w_创成长	
        # ret-w_科创50	ret-w_科创创业50
        ### ret-w_普通股票基金	ret-w_偏股混合基金	ret-w_平衡混合基金	ret-w_偏债混合基金	ret-w_股票指数基金	
        # ret-w_债券基金	ret-w_混合债一级基金	ret-w_混合债二级基金	ret-w_中长期纯债基金
        col_index = [ "中证100","沪深300","创业板指","国证成长","国证价值","创成长","科创50","科创创业50"]
        df_stat = pd.DataFrame(index= col_index , columns=["corr","weight_simu"])
        df_stat["weight_simu"] =0.0

        for name_index in col_index :
            col_index = "ret-w_" + name_index 
            # notes:这样会报错：df1.loc[:,col_port].corr( df1.loc[:,col_index]  )
            df_stat.loc[name_index , "corr" ] = round( df1["ret-w_" + port_name].corr( df1[col_index])*100 ,2 ) 
        ##############################################
        ### 确定相关性最大的2个股票指数 df1.idxmax(0) :0返回每一列最大值对应的行index；1返回每一行最大值对应的列 
        # notes：选2个指数是为了确保匹配的稳定性
        df_stat = df_stat.sort_values(by="corr",ascending= False)
        index_s1 = df_stat.index[0]
        index_s2 = df_stat.index[1]
        ### 例如index_s1, <class 'str'> 国证成长, 0.986467
        print("df_stat \n" ,df_stat )

        ##############################################
        ### 扣除2个股票指数后，寻找最匹配的债券指数
        weight_s1 = 0.65 * df_stat["corr"][0]
        weight_s2 = 0.35 * df_stat["corr"][1] 
        df_stat.loc[index_s1, "weight_simu"] = weight_s1 
        df_stat.loc[index_s2, "weight_simu"] = weight_s2
        df1["ret-w_port_simu"] = weight_s1* df1["ret-w_" + index_s1] +weight_s2* df1["ret-w_" + index_s2]
        df1["diff_port_simu"] =  df1["ret-w_" + port_name] - df1["ret-w_port_simu"] 
        print( "df1,diff_port_simu ,", df1["diff_port_simu"].mean() ,df1["diff_port_simu"]   )
        ### notes: 如果平均值小于 5%，其实就没必要拟合了

        ### 
        col_index_bond = ["国债总财富1-3y","总财富1-3y","国债总财富3-5y","总财富3-5y","信用债总财富3-5y","金融债总财富3-5y","新中票总财富3-5y","企业债总财富3-5y"]
        df_stat_bond = pd.DataFrame(index= col_index_bond , columns=["corr","weight_simu"])
        df_stat_bond["weight_simu"] =0.0 
        for name_index in col_index_bond :
            col_index = "ret-w_" + name_index 
            df_stat_bond.loc[name_index , "corr" ] = round(  df1["ret-w_" + port_name].corr( df1[col_index] ) *100 ,2 ) 
               
        ##############################################
        ### 确定相关性最大的2个股票指数 df1.idxmax(0) :0返回每一列最大值对应的行index；1返回每一行最大值对应的列 
        # notes：选2个指数是为了确保匹配的稳定性
        df_stat_bond = df_stat_bond.sort_values(by="corr",ascending= False)
        index_b1 = df_stat_bond.index[0]
        index_b2 = df_stat_bond.index[1]
        weight_b1 = 0.65 * df_stat_bond["corr"][0]
        weight_b2 = 0.35 * df_stat_bond["corr"][1]
        df_stat_bond.loc[index_b1, "weight_simu"] = weight_b1 
        df_stat_bond.loc[index_b2, "weight_simu"] = weight_b2
        print("df_stat_bond \n" ,df_stat_bond )
        
        ##############################################
        ### 股票基金指数和股票资产相关性大，债券基金和债券指数相关性大，最后要兼顾，综合计算权重
        df_stat = df_stat.append( df_stat_bond  )
        df_stat["weight_simu"] = df_stat["weight_simu"] / df_stat["weight_simu"].sum()
        df_stat["weight_simu"] = (df_stat["weight_simu"]*100).round(2) 
        df_stat.to_excel("D:\\df_stat.xlsx") 

        ### 不知道为什么报错，对 corr进行取整的时候;解决办法：在计算corr时直接取整
        # AttributeError: 'numpy.float64' object has no attribute 'rint'
        print("Debug \n" ,df_stat  ) 
          
        context["df_stat_weight_raw"] = df_stat.T
    
    
    #################################################################################
    ### step 2	股票资产拟合：寻找近5周综合差异最小的3个拟合组合。假定w_s前后5个档差异5%（如25，30，35，40，45），用股票市场、风格、行业组合拟合股票部分收益率，
    ### 精算：不只以相关性作为判定依据，还要以平均和累计收益率、最大回测算
    if "input_fund_allo_weight_stock" in request.POST.keys():   
        date = request.POST.get("date_weight_stock","") 
        ### port_name= "普通股票基金"  port_name = "富国天惠"
        port_name = request.POST.get("port_name_weight_stock","") 
        ### 债券股票资产总权重
        # weight_bond = request.POST.get("weight_bond_weight_stock","")
        # weight_stock = 1 - weight_bond
        ### 券基准组合1名称
        name_bond1 = request.POST.get("name_bond1_weight_stock","") 
        weight_bond1 = float( request.POST.get("weight_bond1_weight_stock","") )
        name_bond2 = request.POST.get("name_bond2_weight_stock","") 
        weight_bond2 = float( request.POST.get("weight_bond2_weight_stock","") )

        weight_stock = 1 - weight_bond1 - weight_bond2

        ### 只保留近5周的数据
        num_week = 10 

        ##############################################
        ### 导入债券指数品种，计算股票部分的收益率和波动率
        ##############################################
        ### 导入股票、债券、基金指数数据
        file_name = "quote_index_week.xlsx"
        df_index = pd.read_excel(path_fund_allocation+ file_name )
        # 升序排列
        df_index = df_index.sort_values(by="date",ascending=True )        
        df_index = df_index[ df_index["date"]<= date ] 
        df_index = df_index.tail( num_week )
        df_index.index = df_index["date"] 
        
        ##############################################
        ### 导入 拟合基金或组合的历史数据
        file_name = "quote_week_code_port.xlsx"
        df1 = pd.read_excel(path_fund_allocation+ file_name ) 
        df1= df1.sort_values(by="date",ascending=True )
        df1 = df1[ df1["date"]<= date ] 
        df1 = df1.tail( num_week )
        df1.index = df1["date"] 
        
        ### 合并数据
        ### join参数的属性，如果为’inner’得到的是两表的交集，如果是outer，得到的是两表的并集。
        df1 = pd.concat([df1,df_index], axis=1, join="inner" ) 
        ### 可能会有重复列 df1.loc[:,~df.columns.duplicated()]
        df1 = df1.loc[:, ~ df1.columns.duplicated()] 
        ##############################################
        ### 股票基金指数和股票资产相关性大，债券基金和债券指数相关性大，最后要兼顾，综合计算权重        
        df1["ret-w_port_simu"] = weight_bond1* df1["ret-w_" + name_bond1] +weight_bond2* df1["ret-w_" + name_bond2] 
        df1["diff_port_simu"] =  df1["ret-w_" + port_name] - df1["ret-w_port_simu"] 
        ### ret-w_port_stock 指的是股票部分的组合收益率
        df1["ret-w_port_stock"] = df1["diff_port_simu"] / weight_stock
        print( "df1,diff_port_simu ,", df1["ret-w_port_stock"].mean() ,df1["ret-w_port_stock"]   )
        
        ##############################################
        ### 导入行业指数的周收益率
        file_name = "quote_week_index_ind.xlsx"
        df_ind = pd.read_excel(path_fund_allocation+ file_name ) 
        df_ind= df_ind.sort_values(by="date",ascending=True )
        df_ind = df_ind[ df_ind["date"]<= date ] 
        df_ind = df_ind.tail( num_week )
        df_ind.index = df_ind["date"] 
        
        ##############################################
        ### 计算每个行业组合和目标组合的：相关性、平均和累计收益率、最大回测
        df_stat_ind = pd.DataFrame( columns=["corr","ret_ave","ret_accu","mdd"])
        ### df_ind.columns 里包含 date
        list_ind = df_ind.columns
        list_ind = [ i for i in list_ind if i != "date"]
        for temp_ind in list_ind :
            try :                        
                df_stat_ind.loc[ temp_ind, "corr" ] = round(  df1["ret-w_" + port_name].corr( df_ind[temp_ind] ) *100 ,2 ) 
                ### ret_ave |notes:wind直接提取的收益率是百分制的形式，不需要乘100
                df_stat_ind.loc[ temp_ind, "ret_ave" ] = df_ind[temp_ind].mean()
                df_stat_ind.loc[ temp_ind, "ret_ave" ] = round( df_stat_ind.loc[ temp_ind, "ret_ave" ]  ,3 ) 
                df_stat_ind.loc[ temp_ind, "diff_ret_ave" ] = df_ind[temp_ind].mean() - df1["ret-w_" + port_name].mean() 
                df_stat_ind.loc[ temp_ind, "diff_ret_ave" ] = round( df_stat_ind.loc[ temp_ind, "diff_ret_ave" ] ,3 ) 
                ### ret_accu, 先计算净值
                df_ind["temp_unit"] =  df_ind[temp_ind]/100 +1 
                df_ind["temp_unit"] = df_ind["temp_unit"].cumprod()
                ret_accu = df_ind["temp_unit"][-1]/df_ind["temp_unit"][0] - 1
                df_stat_ind.loc[ temp_ind, "ret_accu" ] = round( ret_accu ,4 ) 
                ### 计算目标组合累计净值 
                df1["unit_" + port_name] = df1["ret-w_" + port_name]+1
                df1["unit_" + port_name] = df1["unit_" + port_name].cumprod()
                ret_accu_port = df1["unit_" + port_name][-1]/df1["unit_" + port_name][0] - 1
                ### 区间收益率之差值
                df_stat_ind.loc[ temp_ind, "diff_ret_accu" ] = df_ind[temp_ind].mean() - ret_accu_port
                df_stat_ind.loc[ temp_ind, "diff_ret_accu" ] = round( df_stat_ind.loc[ temp_ind, "diff_ret_accu" ] ,3 ) 

                ### 计算最大回撤 ref=line 207, performance_eval.py
                unit_list = list( df_ind["temp_unit"] )
                accu_high = np.maximum.accumulate( unit_list )
                mdd_list = unit_list /accu_high  -1 
                df_stat_ind.loc[ temp_ind, "mdd" ] = round(mdd_list.min(), 4)

            except: 
                print("Debug: ", type(df_ind[temp_ind]), df_ind[temp_ind] )
        
        ##############################################
        ### 打分加权 | 选3个指数是为了确保匹配的稳定性
        df_stat_ind[ "score" ] = 0.0 
        for temp_col in ["corr","diff_ret_ave","diff_ret_accu","mdd"] :
            ### 指标都是越大越好
            temp_max = df_stat_ind[ temp_col ].max()
            temp_min = df_stat_ind[ temp_col ].min()
            temp_score =(df_stat_ind[ temp_col ] -temp_min)/(temp_max - temp_min )
            df_stat_ind[ "score" ] =df_stat_ind[ "score" ]+temp_score 
        
        ### 
        df_stat_ind[ "score" ] = df_stat_ind[ "score" ]/df_stat_ind[ "score" ].max() 
        df_stat_ind[ "score" ] = df_stat_ind[ "score" ].apply(lambda x : round(x,2))
        df_stat_ind = df_stat_ind.sort_values(by="score",ascending= False)  
        index_i1 = df_stat_ind.index[0]
        index_i2 = df_stat_ind.index[1]
        index_i3 = df_stat_ind.index[2]
        index_i4 = df_stat_ind.index[3]
        df_stat_ind.loc[index_i1, "weight_simu"] = 0.35
        df_stat_ind.loc[index_i2, "weight_simu"] = 0.3
        df_stat_ind.loc[index_i3, "weight_simu"] = 0.25
        df_stat_ind.loc[index_i4, "weight_simu"] = 0.1
        print("df_stat_ind \n" ,df_stat_ind )

        df_stat_ind.to_excel("D:\\df_stat_ind.xlsx")
        ##############################################
        ### 
        ### 用行业指数拟合：top 5一级行业指数 和top10三级行业指数
        ### 股票组合弹性参数 para_elasticity_s = 1.0
        # para_elasticity_s = 1.0

        context["df_stat_ind"] = df_stat_ind.T
    
    #################################################################################
    ### step 3	债券资产拟合	假定w_b前后5个档差异5%，用债券市场、债券基金指数、不同久期指数拟合股票部分收益率，寻找综合差异最小的3个拟合组合
    ### 精算：不只以相关性作为判定依据，还要以平均和累计收益率、最大回测算
    ### 用入选的股票资产组合扣除后，计算债券部分的收益率

    ### 精算：不只以相关性作为判定依据，还要以平均和累计收益率、最大回测算
    if "input_fund_allo_weight_bond" in request.POST.keys():   
        date = request.POST.get("date_weight_bond","") 
        ### port_name= "普通股票基金"  port_name = "富国天惠"
        port_name = request.POST.get("port_name_weight_bond","") 
        ### 股票资产总权重
        weight_stock_sum = float( request.POST.get("weight_stock_sum_weight_bond","") )
        ### 券基准组合1名称
        name_stock1 = request.POST.get("name_stock1_weight_bond","") 
        weight_stock1 = float( request.POST.get("weight_stock1_weight_bond","") ) 
        name_stock2 = request.POST.get("name_stock2_weight_bond","") 
        weight_stock2 = float( request.POST.get("weight_stock2_weight_bond","") ) 
        ### notes:weight_stock3和weight_stock4非必须，有可能是空的
        name_stock3 = request.POST.get("name_stock3_weight_bond","") 
        if len( name_stock3 ) > 0 :
            weight_stock3 = float( request.POST.get("weight_stock3_weight_bond","") ) 
        name_stock4 = request.POST.get("name_stock4_weight_bond","") 
        if len( name_stock4 ) > 0 :
            weight_stock4 = float( request.POST.get("weight_stock4_weight_bond","") ) 
                
        weight_bond = 1 - weight_stock_sum

        ### 只保留近5周的数据
        num_week = 10 

        ##############################################
        ### 导入债券指数品种，计算股票部分的收益率和波动率
        ##############################################
        ### 导入股票、债券、基金指数数据
        file_name = "quote_index_week.xlsx"
        df_index = pd.read_excel(path_fund_allocation+ file_name )
        # 升序排列
        df_index = df_index.sort_values(by="date",ascending=True )        
        df_index = df_index[ df_index["date"]<= date ] 
        df_index = df_index.tail( num_week )
        df_index.index = df_index["date"]
        print("df_index \n",  df_index.head() ) 
        
        ##############################################
        ### 导入拟合基金或组合的历史数据 
        file_name = "quote_week_code_port.xlsx"
        df1 = pd.read_excel(path_fund_allocation+ file_name ) 
        df1= df1.sort_values(by="date",ascending=True )
        df1 = df1[ df1["date"]<= date ] 
        df1 = df1.tail( num_week )
        df1.index = df1["date"]
        print( df1.head() ) 
        
        ### 合并数据
        ### join参数的属性，如果为’inner’得到的是两表的交集，如果是outer，得到的是两表的并集。
        df1 = pd.concat([df1,df_index], axis=1, join="inner" ) 
        ### 可能会有重复列 df1.loc[:,~df.columns.duplicated()]
        df1 = df1.loc[:, ~ df1.columns.duplicated()] 
        
        ##############################################
        ### 股票基金指数和股票资产相关性大，债券基金和债券指数相关性大，最后要兼顾，综合计算权重        
        df1["ret-w_port_simu"] = weight_stock1* df1["ret-w_" + name_stock1] +weight_stock2* df1["ret-w_" + name_stock2] 
        if len( name_stock3 ) > 0 :
            df1["ret-w_port_simu"] = df1["ret-w_port_simu"] +weight_stock3 * df1["ret-w_" + name_stock3]  
        if len( name_stock4 ) > 0 :
            df1["ret-w_port_simu"] = df1["ret-w_port_simu"] +weight_stock4 * df1["ret-w_" + name_stock4]  
        df1["diff_port_simu"] =  df1["ret-w_" + port_name] - df1["ret-w_port_simu"] 
        ### ret-w_port_bond 指的是股票部分的组合收益率
        df1["ret-w_port_bond"] = df1["diff_port_simu"] / weight_bond
        print( "df1,diff_port_simu ,", df1["ret-w_port_bond"].mean() ,df1["ret-w_port_bond"]   )
        
        ##############################################
        ### 使用多种债券收益率进行拟合
        col_index_bond = ["国债总财富1-3y","总财富1-3y","国债总财富3-5y","总财富3-5y","信用债总财富3-5y","金融债总财富3-5y","新中票总财富3-5y","企业债总财富3-5y"]         
        ### TODO

        ##############################################
        ### 计算每个行业组合和目标组合的：相关性、平均和累计收益率、最大回测
        df_stat_ind = pd.DataFrame( columns=["corr","ret_ave","ret_accu","mdd"])
        ### df_ind.columns 里包含 date
        list_ind = df_ind.columns
        list_ind = [ i for i in list_ind if i != "date"]
        for temp_ind in list_ind :
            try :                        
                df_stat_ind.loc[ temp_ind, "corr" ] = round(  df1["ret-w_" + port_name].corr( df_ind[temp_ind] ) *100 ,2 ) 
                ### ret_ave |notes:wind直接提取的收益率是百分制的形式，不需要乘100
                df_stat_ind.loc[ temp_ind, "ret_ave" ] = df_ind[temp_ind].mean()
                df_stat_ind.loc[ temp_ind, "ret_ave" ] = round( df_stat_ind.loc[ temp_ind, "ret_ave" ]  ,3 ) 
                df_stat_ind.loc[ temp_ind, "diff_ret_ave" ] = df_ind[temp_ind].mean() - df1["ret-w_" + port_name].mean() 
                df_stat_ind.loc[ temp_ind, "diff_ret_ave" ] = round( df_stat_ind.loc[ temp_ind, "diff_ret_ave" ] ,3 ) 
                ### ret_accu, 先计算净值
                df_ind["temp_unit"] =  df_ind[temp_ind]/100 +1 
                df_ind["temp_unit"] = df_ind["temp_unit"].cumprod()
                ret_accu = df_ind["temp_unit"][-1]/df_ind["temp_unit"][0] - 1
                df_stat_ind.loc[ temp_ind, "ret_accu" ] = round( ret_accu ,4 ) 
                ### 计算目标组合累计净值 
                df1["unit_" + port_name] = df1["ret-w_" + port_name]+1
                df1["unit_" + port_name] = df1["unit_" + port_name].cumprod()
                ret_accu_port = df1["unit_" + port_name][-1]/df1["unit_" + port_name][0] - 1
                ### 区间收益率之差值
                df_stat_ind.loc[ temp_ind, "diff_ret_accu" ] = df_ind[temp_ind].mean() - ret_accu_port
                df_stat_ind.loc[ temp_ind, "diff_ret_accu" ] = round( df_stat_ind.loc[ temp_ind, "diff_ret_accu" ] ,3 ) 

                ### 计算最大回撤 ref=line 207, performance_eval.py
                unit_list = list( df_ind["temp_unit"] )
                accu_high = np.maximum.accumulate( unit_list )
                mdd_list = unit_list /accu_high  -1 
                df_stat_ind.loc[ temp_ind, "mdd" ] = round(mdd_list.min(), 4)

            except: 
                print("Debug: ", type(df_ind[temp_ind]), df_ind[temp_ind] )
        
        ##############################################
        ### 打分加权 | 选3个指数是为了确保匹配的稳定性
        df_stat_ind[ "score" ] = 0.0 
        for temp_col in ["corr","diff_ret_ave","diff_ret_accu","mdd"] :
            ### 指标都是越大越好
            temp_max = df_stat_ind[ temp_col ].max()
            temp_min = df_stat_ind[ temp_col ].min()
            temp_score =(df_stat_ind[ temp_col ] -temp_min)/(temp_max - temp_min )
            df_stat_ind[ "score" ] =df_stat_ind[ "score" ]+temp_score
            print("Debug : ", temp_score )
        
        ### 
        df_stat_ind[ "score" ] = df_stat_ind[ "score" ]/df_stat_ind[ "score" ].max() 
        df_stat_ind[ "score" ] = df_stat_ind[ "score" ].apply(lambda x : round(x,2))
        df_stat_ind = df_stat_ind.sort_values(by="score",ascending= False)  
        index_i1 = df_stat_ind.index[0]
        index_i2 = df_stat_ind.index[1]
        index_i3 = df_stat_ind.index[2]
        index_i4 = df_stat_ind.index[3]
        df_stat_ind.loc[index_i1, "weight_simu"] = 0.35
        df_stat_ind.loc[index_i2, "weight_simu"] = 0.3
        df_stat_ind.loc[index_i3, "weight_simu"] = 0.25
        df_stat_ind.loc[index_i4, "weight_simu"] = 0.1
        print("df_stat_ind \n" ,df_stat_ind )

        df_stat_ind.to_excel("D:\\df_stat_ind.xlsx")
        ##############################################
        ### 
        ### 用行业指数拟合：top 5一级行业指数 和top10三级行业指数
        ### 股票组合弹性参数 para_elasticity_s = 1.0
        # para_elasticity_s = 1.0

        context["df_stat_ind"] = df_stat_ind.T

    #################################################################################
    ### step 4	股债比例优化	用拟合的股票、债券资产组合，重新计算配置比例w_s,w_b;计算最新的指标差异




    #################################################################################
    ### Part 2 股票持仓分析






    #################################################################################
    ### Part 3 债券持仓分析















    ### BEFORE ### BEFORE ### BEFORE ### BEFORE ### BEFORE ### BEFORE ### BEFORE 
    #################################################################################
    ### Case 1：筛选特定基金记录
    if "input_fund_select" in request.POST.keys():   
        fund_code = request.POST.get("fund_search_fund_code","")
        fund_data_source = request.POST.get("fund_data_source","") 
        col_name = request.POST.get("fund_search_col_name","")
        col_value = request.POST.get("fund_search_col_value","")

        print("fund_code= " ,fund_code,"fund_data_source=",fund_data_source,col_name ,";col_name=",col_name , col_value )
        print("fund_search_fund_code=",  request.POST.get("fund_search_fund_code","")  )
        ################################################################################
        ### 直接拉取完成的基金表格，file= FF-基金研究-主动股票-220812.xlsx 
        if fund_data_source == "fund_search_quant_wind" : 
            ### 先统一导入所有基金代码
            ### 依次在 FF-基金研究-主动股票-220812.xlsx 里匹配
            path_fund_data = "C:\\rc_202X\\rc_202X\\data_pms\\wind_terminal\\"
            fund_type_list = ["主动股票", "偏股混合", "偏债混合", "纯债", "股票指数", "可转债", "纯债", "FOF"]
            count_f = 0 
            for fund_type in fund_type_list :
                file_name = "FF-基金研究-" + fund_type +"-220812.xlsx"
                df_temp = pd.read_excel( path_fund_data+ file_name, sheet_name="概况" )
                ###
                if count_f == 0 :
                    df_f_all = df_temp
                    count_f = 1
                else :
                    df_f_all = df_f_all.append(df_temp )
            ### 匹配基金
            df_match = df_temp[ df_temp["基金代码"]== fund_code  ]
            if (df_match.index) > 0 :
                print( df_match.T )            
            else :
                print("No record in excel file ")
            

        ################################################################################
        ### 抓取数据库内匹配的信息
        if fund_data_source == "fund_search_db_funda" : 
            obj_db = {} 
            obj_db["db_name"] = "db_funda.sqlite3"
            obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis" 
            #############################################
            from database import db_sqlite
            db_sqlite1 = db_sqlite()
            #############################################
            ### 检查是否有基金代码输入，例如 110011.OF
            if len( fund_code ) >= 6 : 
                
                #############################################
                ### 有2个列的情况
                if len( col_name ) > 0 and len(col_value)>0   :
                    ### 需要匹配 2~N 列 |dict as input 
                    obj_db["dict_select"] = { }
                    obj_db["dict_select"]["code"] = fund_code
                    obj_db["dict_select"][ col_name ] = col_value         
                    obj_db = db_sqlite1.select_table_data( obj_db )
                    
                #############################################
                ### 只有1个列=基金代码的情况
                else :
                    obj_db["col_name"] = "code"
                    obj_db["select_value"] = fund_code
                    obj_db = db_sqlite1.select_table_data( obj_db )
                    
            else :
                #############################################
                ### 没有代码，只有1个列的情况
                ### 需要匹配 2~N 列      
                obj_db["dict_select"] = { }
                obj_db["dict_select"][col_name] = col_value 
                print("Debug 222= col_name " ,col_name  , col_value )
                print("dict_select \n", obj_db["dict_select"] )
                obj_db = db_sqlite1.select_table_data( obj_db )

        ################################################################################
        ### 返回数据库中的最新N条记录  
        df_data = obj_db["df_data"]
        df_data = df_data.sort_values(by="date", ascending=False)
        context["df_fund_research_tail"] = df_data.T 


    #################################################################################
    ### Case 2：获取基金定性研究记录，上传至sql数据库
    if "input_fund_research_to_sql" in request.POST.keys():    
        ############################################
        dict_1r ={}
        ############################################ dict_1r[""]
        ### 以下部分信息是必须的，有的如果len()<1,就提取Wind-api
        dict_1r["date"] = request.POST.get("date_lastmodify","")
        dict_1r["code"] = request.POST.get("fund_code","")
        dict_1r["style_fund"] = request.POST.get("style_fund","")
        dict_1r["theme_fund"] = request.POST.get("theme_fund","")
        dict_1r["ind_1"] = request.POST.get("ind_1","")
        dict_1r["ind_2"] = request.POST.get("ind_2","")
        dict_1r["ind_3"] = request.POST.get("ind_3","")
        dict_1r["ind_num"] = request.POST.get("ind_num","")
        dict_1r["s_down_market"] = request.POST.get("s_down_market","")
        dict_1r["s_flat_market"] = request.POST.get("s_flat_market","")
        dict_1r["s_up_market"] = request.POST.get("s_up_market","")
        dict_1r["abstract_analysis"]= request.POST.get("abstract_analysis","")

        ### 计算总分
        score_performance= float( dict_1r["s_down_market"] ) + float( dict_1r["s_flat_market"] ) +float( dict_1r["s_up_market"]  )
        score_performance= round( score_performance /30*100, 2 ) 
        dict_1r["score_performance"] = score_performance

        ############################################
        ### 以下部分信息不是必须的
        dict_1r["fund_manager"] = request.POST.get("fund_manager","")
        dict_1r["note"] = request.POST.get("note","")
        dict_1r["date_lastmodify"] = request.POST.get("date_lastmodify","")
        dict_1r["if_fundmanager_fault"] = request.POST.get("if_fundmanager_fault","0")


        ################################################################################
        ### 判断是否要匹配基金信息、或提取Wind-API指标
        if len( dict_1r["fund_manager"] ) <= 1 :
            obj_f={}
            obj_f["fund_code"] = dict_1r["code"] 
            obj_f["date"] = dict_1r["date_lastmodify"] 
            obj_f["col_list"] = ["fund_fundmanageroftradedate",]
            from get_wind_api import wind_api
            wind_api1 = wind_api()
            obj_f = wind_api1.get_wss_fund_1date( obj_f )

            ### obj_f["list_data"] = obj_w.Data[0] = ['张坤']
            dict_1r["fund_manager"] = obj_f["list_data"][0]

        ### 获取基金名称
        obj_f={}
        obj_f["fund_code"] = dict_1r["code"] 
        obj_f["date"] = dict_1r["date_lastmodify"] 
        obj_f["col_list"] = ["fund_info_name",]
        from get_wind_api import wind_api
        wind_api1 = wind_api()
        obj_f = wind_api1.get_wss_fund_1date( obj_f )

        ### obj_f["list_data"] = obj_w.Data[0] = ['张坤'] 
        dict_1r["name"] = obj_f["list_data"][0] 

        ################################################################################
        ### 将变量转换为sql语言，存入数据表 db_funda\ciss_exhi_fund_analysis
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        ### insert_table_data |  "1r" 导入一行
        obj_db = {}
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] =  "ciss_exhi"+ "_" + "fund_analysis"
        obj_db["insert_type"] ="1r"
        obj_db["dict_1r"] = dict_1r
        obj_db = db_sqlite1.insert_table_data(obj_db)

        ### 返回数据库中的最新N条记录 
        context["df_fund_research_tail"] = obj_db["df_data"].T 

        ################################################################################
        ### 返回输入项对应的 ID；如果输入有误，可以输入ID进行修改 TODO

    #################################################################################
    ### Case 3：删除基金某一行 | 
    ### 删除表:  drop删除表结构和表数据，truncate删除表数据，delete删除某一行
    if "input_fund_delete" in request.POST.keys():   
        fund_code= request.POST.get("del_fund_code","")
        ### 检查是否还需要匹配列
        col_name = request.POST.get("del_col_name","")
        col_value = request.POST.get("del_col_value","")
        obj_db = {}         
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis" 
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        #############################################
        ### 有2个列的情况
        if len( col_name ) > 0 and len(col_value)>0   :
            ### 需要匹配 2~N 列               
            obj_db["col_list"] = ["code", col_name ]
            obj_db["value_list"] = [fund_code , col_value ]
            obj_db["delete_type"] = "1r" ###
            obj_db = db_sqlite1.delete_table( obj_db )

        #############################################
        ### 只有1个列=基金代码的情况
        else :
            input1 = input("Check if you want to delete all records match code=", fund_code )
            obj_db["col_name"] = "code"            
            obj_db["select_value"] = fund_code
            obj_db = db_sqlite1.delete_table( obj_db )

        ################################################################################
        ### 返回数据库中的最新N条记录 
        df_data = obj_db["df_data"].sort_values(by="id",ascending=False )
        df_data = df_data.head(10)

        context["df_fund_research_tail"] = df_data.T 




    
    #################################################################################
    ### 输出的dict对象 context | 有的df需要转置transpose 
    
    #################################################################################
    ### 输出的dict对象 context  # 
    ### 查询列表         

    ### 时间
    context["time_now_str"] = time_now_str
    context["time_now_str_pre1d"] =time_now_str_pre1d 

    return render(request,"ciss_exhi/fund_fof/index_fund_allocation.html",context) 



























###########################################################################
###########################################################################
### BEFORE 参考资料



###########################################################################
@csrf_protect
@requires_csrf_token
@csrf_exempt
def fund_analysis(request):
    ###########################################################################
    ### 基金分析和FOF分析
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    #################################################################################
    ### 导入常用地址信息
    import os
    path_pms =  os.getcwd().split("ciss_web")[0]+"\\data_pms\\"  
    path_wind_terminal = path_pms + "wind_terminal\\" 
    path_wss = path_pms + "wss\\"
    path_wpf = path_pms + "wpf\\"
    path_wpd = path_pms + "wpd\\"
    path_wsd = path_pms + "wsd\\"
    path_ciss_web = os.getcwd().split("ciss_web")[0]+"ciss_web\\"
    path_ciss_rc = path_ciss_web +"CISS_rc\\"
    # path_db = path_ciss_rc + "db\\"
    path_dt = path_ciss_rc + "db\\db_times\\"
    sys.path.append(path_ciss_rc+ "config\\")
    from config_data import config_db
    config_db_1 = config_db()
    path_db = path_ciss_web


    #################################################################################
    ### 
    #################################################################################
    ### Case 1：筛选特定基金记录
    if "input_fund_select" in request.POST.keys():   
        fund_code = request.POST.get("fund_search_fund_code","")
        fund_data_source = request.POST.get("fund_data_source","") 
        col_name = request.POST.get("fund_search_col_name","")
        col_value = request.POST.get("fund_search_col_value","")

        print("fund_code= " ,fund_code,"fund_data_source=",fund_data_source,col_name ,";col_name=",col_name , col_value )
        print("fund_search_fund_code=",  request.POST.get("fund_search_fund_code","")  )
        ################################################################################
        ### 直接拉取完成的基金表格，file= FF-基金研究-主动股票-220812.xlsx 
        if fund_data_source == "fund_search_quant_wind" : 
            ### 先统一导入所有基金代码
            ### 依次在 FF-基金研究-主动股票-220812.xlsx 里匹配
            path_fund_data = "C:\\rc_202X\\rc_202X\\data_pms\\wind_terminal\\"
            fund_type_list = ["主动股票", "偏股混合", "偏债混合", "纯债", "股票指数", "可转债", "纯债", "FOF"]
            count_f = 0 
            for fund_type in fund_type_list :
                file_name = "FF-基金研究-" + fund_type +"-220812.xlsx"
                df_temp = pd.read_excel( path_fund_data+ file_name, sheet_name="概况" )
                ###
                if count_f == 0 :
                    df_f_all = df_temp
                    count_f = 1
                else :
                    df_f_all = df_f_all.append(df_temp )
            ### 匹配基金
            df_match = df_temp[ df_temp["基金代码"]== fund_code  ]
            if (df_match.index) > 0 :
                print( df_match.T )            
            else :
                print("No record in excel file ")
            

        ################################################################################
        ### 抓取数据库内匹配的信息
        if fund_data_source == "fund_search_db_funda" : 
            obj_db = {} 
            obj_db["db_name"] = "db_funda.sqlite3"
            obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis" 
            #############################################
            from database import db_sqlite
            db_sqlite1 = db_sqlite()
            #############################################
            ### 检查是否有基金代码输入，例如 110011.OF
            if len( fund_code ) >= 6 : 
                
                #############################################
                ### 有2个列的情况
                if len( col_name ) > 0 and len(col_value)>0   :
                    ### 需要匹配 2~N 列 |dict as input 
                    obj_db["dict_select"] = { }
                    obj_db["dict_select"]["code"] = fund_code
                    obj_db["dict_select"][ col_name ] = col_value         
                    obj_db = db_sqlite1.select_table_data( obj_db )
                    
                #############################################
                ### 只有1个列=基金代码的情况
                else :
                    obj_db["col_name"] = "code"
                    obj_db["select_value"] = fund_code
                    obj_db = db_sqlite1.select_table_data( obj_db )
                    
            else :
                #############################################
                ### 没有代码，只有1个列的情况
                ### 需要匹配 2~N 列      
                obj_db["dict_select"] = { }
                obj_db["dict_select"][col_name] = col_value 
                print("Debug 222= col_name " ,col_name  , col_value )
                print("dict_select \n", obj_db["dict_select"] )
                obj_db = db_sqlite1.select_table_data( obj_db )

        ################################################################################
        ### 返回数据库中的最新N条记录  
        df_data = obj_db["df_data"]
        df_data = df_data.sort_values(by="date", ascending=False)
        context["df_fund_research_tail"] = df_data.T 


    #################################################################################
    ### Case 2：获取基金定性研究记录，上传至sql数据库
    if "input_fund_research_to_sql" in request.POST.keys():    
        ############################################
        dict_1r ={}
        ############################################ dict_1r[""]
        ### 以下部分信息是必须的，有的如果len()<1,就提取Wind-api
        dict_1r["date"] = request.POST.get("date_lastmodify","")
        dict_1r["code"] = request.POST.get("fund_code","")
        dict_1r["style_fund"] = request.POST.get("style_fund","")
        dict_1r["theme_fund"] = request.POST.get("theme_fund","")
        dict_1r["ind_1"] = request.POST.get("ind_1","")
        dict_1r["ind_2"] = request.POST.get("ind_2","")
        dict_1r["ind_3"] = request.POST.get("ind_3","")
        dict_1r["ind_num"] = request.POST.get("ind_num","")
        dict_1r["s_down_market"] = request.POST.get("s_down_market","")
        dict_1r["s_flat_market"] = request.POST.get("s_flat_market","")
        dict_1r["s_up_market"] = request.POST.get("s_up_market","")
        dict_1r["abstract_analysis"]= request.POST.get("abstract_analysis","")

        ### 计算总分
        score_performance= int( dict_1r["s_down_market"] ) + int( dict_1r["s_flat_market"] ) +int( dict_1r["s_up_market"]  )
        score_performance= round( score_performance /30*100, 2 ) 
        dict_1r["score_performance"] = score_performance

        ############################################
        ### 以下部分信息不是必须的
        dict_1r["fund_manager"] = request.POST.get("fund_manager","")
        dict_1r["note"] = request.POST.get("note","")
        dict_1r["date_lastmodify"] = request.POST.get("date_lastmodify","")
        dict_1r["if_fundmanager_fault"] = request.POST.get("if_fundmanager_fault","0")


        ################################################################################
        ### 判断是否要匹配基金信息、或提取Wind-API指标
        if len( dict_1r["fund_manager"] ) <= 1 :
            obj_f={}
            obj_f["fund_code"] = dict_1r["code"] 
            obj_f["date"] = dict_1r["date_lastmodify"] 
            obj_f["col_list"] = ["fund_fundmanageroftradedate",]
            from get_wind_api import wind_api
            wind_api1 = wind_api()
            obj_f = wind_api1.get_wss_fund_1date( obj_f )

            ### obj_f["list_data"] = obj_w.Data[0] = ['张坤']
            dict_1r["fund_manager"] = obj_f["list_data"][0]

        ### 获取基金名称
        obj_f={}
        obj_f["fund_code"] = dict_1r["code"] 
        obj_f["date"] = dict_1r["date_lastmodify"] 
        obj_f["col_list"] = ["fund_info_name",]
        from get_wind_api import wind_api
        wind_api1 = wind_api()
        obj_f = wind_api1.get_wss_fund_1date( obj_f )

        ### obj_f["list_data"] = obj_w.Data[0] = ['张坤'] 
        dict_1r["name"] = obj_f["list_data"][0] 

        ################################################################################
        ### 将变量转换为sql语言，存入数据表 db_funda\ciss_exhi_fund_analysis
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        ### insert_table_data |  "1r" 导入一行
        obj_db = {}
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] =  "ciss_exhi"+ "_" + "fund_analysis"
        obj_db["insert_type"] ="1r"
        obj_db["dict_1r"] = dict_1r
        obj_db = db_sqlite1.insert_table_data(obj_db)

        ### 返回数据库中的最新N条记录 
        context["df_fund_research_tail"] = obj_db["df_data"].T 

        ################################################################################
        ### 返回输入项对应的 ID；如果输入有误，可以输入ID进行修改 TODO

    #################################################################################
    ### Case 3：删除基金某一行 | 
    ### 删除表:  drop删除表结构和表数据，truncate删除表数据，delete删除某一行
    if "input_fund_delete" in request.POST.keys():   
        fund_code= request.POST.get("del_fund_code","")
        ### 检查是否还需要匹配列
        col_name = request.POST.get("del_col_name","")
        col_value = request.POST.get("del_col_value","")
        obj_db = {}         
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis" 
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        #############################################
        ### 有2个列的情况
        if len( col_name ) > 0 and len(col_value)>0   :
            ### 需要匹配 2~N 列               
            obj_db["col_list"] = ["code", col_name ]
            obj_db["value_list"] = [fund_code , col_value ]
            obj_db["delete_type"] = "1r" ###
            obj_db = db_sqlite1.delete_table( obj_db )

        #############################################
        ### 只有1个列=基金代码的情况
        else :
            input1 = input("Check if you want to delete all records match code=", fund_code )
            obj_db["col_name"] = "code"            
            obj_db["select_value"] = fund_code
            obj_db = db_sqlite1.delete_table( obj_db )

        ################################################################################
        ### 返回数据库中的最新N条记录 
        df_data = obj_db["df_data"].sort_values(by="id",ascending=False )
        df_data = df_data.head(10)

        context["df_fund_research_tail"] = df_data.T 




    
    #################################################################################
    ### 输出的dict对象 context | 有的df需要转置transpose 
    
    #################################################################################
    ### 输出的dict对象 context  # 
    ### 查询列表         

    ### 时间
    context["time_now_str"] = time_now_str
    context["time_now_str_pre1d"] =time_now_str_pre1d 

    return render(request,"ciss_exhi/fund_fof/index_fund_fof.html",context) 


######################################################################################################
# 获取设备列表数据 
@csrf_protect
@requires_csrf_token
@csrf_exempt
def get_data(request):
    ### layui的table设置里，默认是 get
    if request.method == 'GET':
        print("get_data | request.GET", type(request.GET.keys() ),request.GET.keys() )
        print("get_data | request.POST", type(request.POST.keys() ),request.POST.keys() )
        #########################################################
        ### 
        print("Debug-get_data: Begin of get_data"  )
        #######################################################
        ### 查找的input变量        
        dict_select = {}
        col_list = ["id","date","name","code","style_fund","theme_fund","ind_1","ind_2","ind_3","ind_num"]
        col_list = col_list + ["score_performance","s_down_market","s_flat_market","s_up_market","abstract_analysis","fund_manager"]
        for temp_col in col_list : 
            if temp_col in request.GET.keys() :
                if len( request.GET[temp_col] ) > 0 :
                    dict_select[temp_col] = request.GET[temp_col]
        print("Debug | dict_select=", dict_select ) 
        
        #######################################################
        ### 获取表格数据
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis" 
        obj_db["dict_select"] = dict_select 
        

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db ) 
        df_data = obj_db["df_data"]  
        ### 以前是id
        df_data = df_data.sort_values(by="date" ,ascending=False )
        print("Debug:End of get_data")
        
        #############################################
        ### notes:df不能直接df.to_json(),这样得到的是 str格式，df_data.to_json()不能赋值给JsonResponse里的data
        ### layui-table可以识别的data数据在views.py里长这样：dict_data= {1: {'id': 1, 'date': '600036','weight': 0.05},  
        # 2: {'id': 2, 'date': '300750', 'weight': 0.08},  
        col_list_output= col_list
        dict_data = {}
        for i in df_data.index :
            dict_data[i]={}
            for temp_col in col_list_output : 
                try :
                    dict_data[i][temp_col] = df_data.loc[i, temp_col] 
                except:
                    print("Debug: temp_col",temp_col,i ,"\n", df_data.loc[i, :] ) 
                    asd

        ### notes: 必须用 df_data.T.to_dict() ,如果 dict( df_data ) 会报错
        ### notes：如果数据库记录里有多列是null空值|例如测试用的列，就会导致这个问题！！
        # dict_data = df_data.loc[ -3: ,col_list ].T.to_dict() 
        dict_data = df_data.loc[ : ,col_list ].T.to_dict() 
        count = len( dict_data )
        print("Debug-get_data：type of data=",type(dict_data),count  ) 

        ##############################################        
        ### table 组件默认规定的数据格式：{ "code": 0, "msg": "",  "count": 1000,  "data": [{}, {}] } 
        ### 非默认的格式需要parseData转换，例如 {"status": 0,   "message": "", "total": 180, "data": {"item": [{}, {}] } }
        ### 官方格式参考，url=http://layui.winxapp.cn/demo/table/user/-page=1&limit=30.js
        if count != 0:
            # print("count=",count,"dict_data=", dict_data[0] )
            return JsonResponse({'code':0,'msg':'查询成功','count':count,'data':dict_data })
            # return JsonResponse({'code':0,'msg':'查询成功','count':count,'data':json_data })
        else:
            return JsonResponse({'code':0,'msg':'暂无数据','count':count,'data':{}  })



######################################################################################################
### 新增记录
@csrf_protect
@requires_csrf_token
@csrf_exempt
def add_data(request):
    if request.method == 'POST': 
        print("add_data | request.POST", type(request.POST.keys() ),request.POST.keys() ) 
        ##################################################################################
        ### 存入sql数据表 
        ### 必须项 ["id","date","name","code","style_fund","theme_fund","ind_1","ind_2","ind_3","ind_num"]
        id = request.POST.get("id","")
        date = request.POST.get("date","") 
        code = request.POST.get("code","") 
        name = request.POST.get("name","") 
        style_fund = request.POST.get("style_fund","")
        theme_fund = request.POST.get("theme_fund","") 
        ind_1 = request.POST.get("ind_1","") 
        ind_2 = request.POST.get("ind_2","") 
        ind_3 = request.POST.get("ind_3","") 
        ind_num = request.POST.get("ind_num","") 
        ### 必须项 ["score_performance","s_down_market","s_flat_market","s_up_market","abstract_analysis","fund_manager"]
        score_performance = request.POST.get("score_performance","") 
        s_down_market = request.POST.get("s_down_market","") 
        s_flat_market = request.POST.get("s_flat_market","") 
        s_up_market = request.POST.get("s_up_market","")
        abstract_analysis = request.POST.get("abstract_analysis","") 
        fund_manager = request.POST.get("fund_manager","")  

        # print("Debug: date=",date, )
        ### 非必须项 ["note","date_lastmodify","if_fundmanager_fault"]
        note = request.POST.get("note","")
        date_lastmodify = request.POST.get("date_lastmodify","") 
        if_fundmanager_fault = request.POST.get("if_fundmanager_fault","") 

        ##################################################################################
        ### 必须项不能是空的
        if date == '' or strategy_CN == '' or code == '' or weight== ''  : 
            return JsonResponse({'code': 10021, 'msg': '参数错误'}, json_dumps_params={'ensure_ascii': False})
        else :
            ###############################################
            ### 多个input必须至少有一项非空
            obj_db = {} 
            obj_db["db_name"] = "db_funda.sqlite3"
            obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis" 
            obj_db["insert_type"] = "1r"
            obj_db["dict_1r"] = {}
            if len( date ) > 0  :  
                obj_db["dict_1r"][ "date"] = date 
            if len( code ) > 0  :  
                obj_db["dict_1r"][ "code"] = code  
            if len( name ) > 0  :  
                obj_db["dict_1r"][ "name"] = name
            if len( style_fund ) > 0  :  
                obj_db["dict_1r"][ "style_fund"] = style_fund 
            if len( theme_fund ) > 0  :  
                obj_db["dict_1r"][ "theme_fund"] = theme_fund  
            if len( ind_1 ) > 0  :  
                obj_db["dict_1r"][ "ind_1"] = ind_1
            if len( ind_2 ) > 0  :  
                obj_db["dict_1r"][ "ind_2"] = ind_2
            if len( ind_3 ) > 0  :  
                obj_db["dict_1r"][ "ind_3"] = ind_3
            if len( ind_num ) > 0  :  
                obj_db["dict_1r"][ "ind_num"] = ind_num 
            if len( score_performance ) > 0  :  
                obj_db["dict_1r"][ "score_performance"] = score_performance  
            if len( s_down_market ) > 0  :  
                obj_db["dict_1r"][ "s_down_market"] = s_down_market
            if len( s_flat_market ) > 0  :  
                obj_db["dict_1r"][ "s_flat_market"] = s_flat_market 
            if len( s_up_market ) > 0  :  
                obj_db["dict_1r"][ "s_up_market"] = s_up_market  
            if len( abstract_analysis ) > 0  :  
                obj_db["dict_1r"][ "abstract_analysis"] = abstract_analysis
            if len( fund_manager ) > 0  :  
                obj_db["dict_1r"][ "fund_manager"] = fund_manager 
            if len( date_lastmodify ) > 0  :  
                obj_db["dict_1r"][ "date_lastmodify"] = date_lastmodify
            if len( if_fundmanager_fault ) > 0  :  
                obj_db["dict_1r"][ "if_fundmanager_fault"] = if_fundmanager_fault 
            if len( note ) > 0  :  
                obj_db["dict_1r"][ "note"] = note 

            #############################################
            from database import db_sqlite
            db_sqlite1 = db_sqlite()
            obj_db = db_sqlite1.insert_table_data(obj_db) 

            return JsonResponse({'code': 0, 'msg': '添加成功！'})

    else :
        ### 
        print("Not post for add data")


######################################################################################################
### 编辑数据表记录
@csrf_protect
@requires_csrf_token
@csrf_exempt
def edit_data(request):
    if request.method == 'POST':
        ### request.POST <class 'dict_keys'> dict_keys(['id', 'date', 'strategy_CN', 'code', 'name', 'weight'])
        print("edit_data |request.POST", type(request.POST.keys() ),request.POST.keys() )
        
        #########################################################
        ### html里提交修改后的数据,传给后台view的变量是formData1.append("id")里的 id，不是edit_id
        id = request.POST.get('id',"") 
        date = request.POST.get('date', "") 
        code = request.POST.get('code', "")
        weight = request.POST.get('weight', "")
        name = request.POST.get('name', "")
        note = request.POST.get('note', "")
        id = request.POST.get('id',"") 
        date = request.POST.get('date', "") 
        code = request.POST.get('code', "")
        weight = request.POST.get('weight', "")
        name = request.POST.get('name', "")
        note = request.POST.get('note', "")
        id = request.POST.get('id',"") 
        date = request.POST.get('date', "") 
        code = request.POST.get('code', "")
        weight = request.POST.get('weight', "")
        name = request.POST.get('name', "")
        note = request.POST.get('note', "")
        ### 
        ###############################################
        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis" 
        obj_db["insert_type"] = "1r"
        obj_db["dict_1r"] = {}
        if len( date ) > 0  :  
            obj_db["dict_1r"][ "date"] = date
        if len( strategy_CN ) > 0  :  
            obj_db["dict_1r"][ "strategy_CN"] = strategy_CN
        if len( code) > 0  :  
            obj_db["dict_1r"][ "code"] = code
        if len( weight) > 0  :  
            obj_db["dict_1r"][ "weight"] = weight
        if len( weight) > 0  :  
            obj_db["dict_1r"][ "name"] = name
        if len( weight) > 0  :  
            obj_db["dict_1r"][ "note"] = note

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        ### "update_type" = "id" ：对某一列id的值进行更新
        obj_db["update_type"] = "id"
        obj_db["id"] = id
        
        try:
            obj_db = db_sqlite1.update_table_data(obj_db) 

            return JsonResponse({'code':0,'msg':'修改成功!'})
        except :
            return JsonResponse({'status': 10023, 'msg': '数据不存在'})
            # return JsonResponse({'code':0, 'msg': '数据不存在'})



######################################################################################################
### 删除数据表记录
@csrf_protect
@requires_csrf_token
@csrf_exempt
def del_data(request):
    if request.method == 'POST':
        print("request.POST", type(request.POST.keys() ),request.POST.keys() )
        #########################################################
        ### 根据提交的ID删除记录
        id_table = request.POST.get('id',"")

        obj_db = {} 
        ### 数据库信息
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis" 
        ### obj_db["delete_type"] ==  "id"  根据给定id 删除某一行
        obj_db["delete_type"] = "id" 
        obj_db["id_table"] = id_table
        #############################################

        try :
            from database import db_sqlite
            db_sqlite1 = db_sqlite()     
            obj_db = db_sqlite1.delete_table_index(obj_db) 
            ### 无返回数据 
            return JsonResponse({'code': 0, 'msg': '删除成功','count':id_table } )

        except :
            return JsonResponse({'code':0,'msg':'删除过程出错','count':id_table } )






































