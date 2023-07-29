# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 

todo:简化views.py
功能：管理从wind-api提取的各类市场数据
last  | since 20220913
Notes: 
refernce:  views.py 
===============================================
''' 

###########################################################################
### Initialization
from django.shortcuts import render
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
### monitor_market_data 市场数据跟踪
@csrf_protect
@requires_csrf_token
@csrf_exempt
def monitor_market_data(request):
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )

    #############################################
    ### 导入配置文件对象，例如path_db_wind等
    from config_data import config_data 
    obj_config = config_data().obj_config
    path_data_adj = obj_config["dict"]["path_data_adj"]

    ###########################################################################
    ### A股港股核心指标变动
    if "input_indicator_ashares_key_change" in request.POST.keys():     
        ### 
        date_begin = request.POST.get("date_begin_key_change","")
        date_end = request.POST.get("date_end_key_change","")

        file_begin = "ah_shares_" + date_begin + ".xlsx"
        file_begin_ind1 = "a_shares_ind1_" + date_begin + ".xlsx"
        file_begin_ind3 = "a_shares_ind3_" + date_begin + ".xlsx"

        if len( date_end ) < 8 :
            date_end = ""
            file_end = "ah_shares.xlsx"
            file_end_ind1 = "a_shares_ind1.xlsx"
            file_end_ind3 = "a_shares_ind3.xlsx"
            ### 
            file_output= "df_shares_diff.xlsx"
        else :
            file_end = "ah_shares_" + date_end + ".xlsx"
            file_end_ind1 = "a_shares_ind1_" + date_end + ".xlsx"
            file_end_ind3 = "a_shares_ind3_" + date_end + ".xlsx"
            ### 
            file_output= "shares_diff_" +date_begin +"_"+ date_end + ".xlsx"

        #############################################  
        ### 导入A股股票数据       
        df_shares_begin = pd.read_excel( path_data_adj + file_begin )
        df_shares_end   = pd.read_excel( path_data_adj + file_end )
        
        ### 导入一级和三级行业数据       
        df_shares_begin_ind1 = pd.read_excel( path_data_adj + file_begin_ind1 )
        df_shares_end_ind1   = pd.read_excel( path_data_adj + file_end_ind1 )

        df_shares_begin_ind3 = pd.read_excel( path_data_adj + file_begin_ind3 )
        df_shares_end_ind3   = pd.read_excel( path_data_adj + file_end_ind3 )

        #################################################
        ### 
        diff_col_list_CN = ["月均成交额","总市值","短期趋势","中期趋势","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)"]
        diff_col_list = ["m_ave_amt","m_ave_mv","trend_short","trend_mid","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)"]
        ### 
        diff_col_list_ind_CN = ["月均成交额","总市值","短期趋势","中期趋势","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)"]
        diff_col_list_ind = ["月均成交额","总市值","trend_short","trend_mid","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)"]
        
        #################################################
        ### 个股
        col_list_keep = ["代码","名称"]
        for temp_col in diff_col_list :
            df_shares_end["diff_"+ temp_col ] = df_shares_end[ temp_col ] - df_shares_begin[ temp_col ] 
            col_list_keep = col_list_keep + ["diff_"+ temp_col ]
        
        df_shares_diff = df_shares_end.loc[:, col_list_keep ] 
        
        ### exhi格式调整 
        df_shares_diff["总市值"] = df_shares_diff["diff_"+ "m_ave_mv"].round(decimals=1 )       
        df_shares_diff["月均成交额"] = df_shares_diff["diff_"+ "m_ave_amt"].round(decimals=1 )   
        df_shares_diff["短期趋势"] = (df_shares_diff["diff_"+ "trend_short"]*100).round(decimals=1 )     
        df_shares_diff["中期趋势"] = (df_shares_diff["diff_"+ "trend_mid"]*100).round(decimals=1 )    
        df_shares_diff["基金持股比例"] = df_shares_diff["diff_"+ "基金持股比例"].round(decimals=2 )   
        df_shares_diff["净资产收益率"] = df_shares_diff["diff_"+ "净资产收益率(TTM)"].round(decimals=2 )     
        df_shares_diff["归母净利润同比增长率"] = df_shares_diff["diff_"+ "归母净利润同比增长率"].round(decimals=2 )   
        df_shares_diff["市盈率"] = df_shares_diff["diff_"+ "市盈率(TTM)"].round(decimals=2 )   
        ### 按 月均成交额 降序排列
        df_shares_diff = df_shares_diff.sort_values(by="月均成交额", ascending=False  )

        #################################################
        ### 一级行业 
        col_list_keep = ["中信一级行业"]
        for temp_col in diff_col_list_ind : 
            df_shares_end_ind1["diff_"+ temp_col ] = df_shares_end_ind1[ temp_col ] - df_shares_begin_ind1[ temp_col ] 
            col_list_keep = col_list_keep + ["diff_"+ temp_col ]
        
        df_shares_ind1_diff = df_shares_end_ind1.loc[:, col_list_keep ]
        
        ### exhi格式调整
        df_shares_ind1_diff["总市值"] = df_shares_ind1_diff["diff_"+ "总市值"].round(decimals=1 )       
        df_shares_ind1_diff["月均成交额"] = df_shares_ind1_diff["diff_"+ "月均成交额"].round(decimals=1 )   
        df_shares_ind1_diff["短期趋势"] = (df_shares_ind1_diff["diff_"+ "trend_short"]*100).round(decimals=1 )     
        df_shares_ind1_diff["中期趋势"] = (df_shares_ind1_diff["diff_"+ "trend_mid"]*100).round(decimals=1 )    
        df_shares_ind1_diff["基金持股比例"] = df_shares_ind1_diff["diff_"+ "基金持股比例"].round(decimals=2 )   
        df_shares_ind1_diff["净资产收益率"] = df_shares_ind1_diff["diff_"+ "净资产收益率(TTM)"].round(decimals=2 )     
        df_shares_ind1_diff["归母净利润同比增长率"] = df_shares_ind1_diff["diff_"+ "归母净利润同比增长率"].round(decimals=2 )   
        df_shares_ind1_diff["市盈率"] = df_shares_ind1_diff["diff_"+ "市盈率(TTM)"].round(decimals=2 )   
        ### 按 月均成交额 降序排列
        df_shares_ind1_diff = df_shares_ind1_diff.sort_values(by="月均成交额", ascending=False  )

        #################################################
        ### 三级行业
        col_list_keep = ["中信三级行业"]
        for temp_col in diff_col_list_ind :
            df_shares_end_ind3["diff_"+ temp_col ] = df_shares_end_ind3[ temp_col ] - df_shares_begin_ind3[ temp_col ] 
            col_list_keep = col_list_keep + ["diff_"+ temp_col ]
        
        ### exhi格式调整
        df_shares_ind3_diff = df_shares_end_ind3.loc[:, col_list_keep ]
        df_shares_ind3_diff["总市值"] = df_shares_ind3_diff["diff_"+ "总市值"].round(decimals=1 )       
        df_shares_ind3_diff["月均成交额"] = df_shares_ind3_diff["diff_"+ "月均成交额"].round(decimals=1 )   
        df_shares_ind3_diff["短期趋势"] = (df_shares_ind3_diff["diff_"+ "trend_short"]*100).round(decimals=1 )     
        df_shares_ind3_diff["中期趋势"] = (df_shares_ind3_diff["diff_"+ "trend_mid"]*100).round(decimals=1 )    
        df_shares_ind3_diff["基金持股比例"] = df_shares_ind3_diff["diff_"+ "基金持股比例"].round(decimals=2 )   
        df_shares_ind3_diff["净资产收益率"] = df_shares_ind3_diff["diff_"+ "净资产收益率(TTM)"].round(decimals=2 )     
        df_shares_ind3_diff["归母净利润同比增长率"] = df_shares_ind3_diff["diff_"+ "归母净利润同比增长率"].round(decimals=2 )   
        df_shares_ind3_diff["市盈率"] = df_shares_ind3_diff["diff_"+ "市盈率(TTM)"].round(decimals=2 )   
        ### 按 月均成交额 降序排列
        df_shares_ind3_diff = df_shares_ind3_diff.sort_values(by="月均成交额", ascending=False  )

        ################################################################################################
        ### save to excel file
        ### 对一个excel写入多个sheet
        df_shares_diff.to_excel(path_data_adj + file_output,sheet_name="shares")

        ### pd.ExcelWriter 只能操作已经有的excel文件
        ### *mode='a'代表append，可以实现追加sheet；如果不写mode,则默认mode='w'，会把原有的sheet覆盖。
        ### Qs：用pd.ExcelWriter导出excel时，给定sheet名称如果已经存在，Sheet1 会变成 Sheet11
        writer = pd.ExcelWriter( path_data_adj + file_output ,mode='a',engine='openpyxl') 
        df_shares_ind1_diff.to_excel(writer,"ind1")
        df_shares_ind3_diff.to_excel(writer,"ind3")
        
        writer.save()
        writer.close()
        
        #################################################
        ### 需要转置transpose
        df_shares_diff = df_shares_diff.head(50)
        print("df_shares_diff \n", df_shares_diff  )
        context["df_shares_diff"] = df_shares_diff.T 
        context["df_shares_ind1_diff"] = df_shares_ind1_diff.T 
        context["df_shares_ind3_diff"] = df_shares_ind3_diff.loc[:30,: ].T 


    ###########################################################################
    ### 查询月度行情数据和核心指标：个股、指数、基金 
    if "input_select_quote" in request.POST.keys():     
        ### 
        obj_db ={}
        obj_db["db_name"] = "db_quote.sqlite3"
        obj_db["table_name"] = request.POST.get("table_name","")
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        
        obj_db["dict_select"] = { }
        type_asset = request.POST.get("type_asset","")
        if len( type_asset ) > 0 :            
            obj_db["dict_select"][ "type_asset" ] = type_asset    
            
        date_begin = request.POST.get("date_begin","")
        if len( date_begin ) > 0 :
            obj_db["col_name_date"] ="date"
            obj_db["dict_select"][ "date_begin" ] = date_begin  
            date_end = request.POST.get("date_end","")  
            obj_db["dict_select"][ "date_end" ] = date_end
        
        ### 查询
        obj_db = db_sqlite1.select_table_data( obj_db )
        
        #################################################
        ### 统计数据量
        df_data = obj_db["df_data"]
        num_col = len( df_data.columns )
        num_index = len( df_data.index )

        ### 需要转置transpose
        context["df_data"] =  obj_db["df_data"].T 
        context["num_col"] = num_col
        context["num_index"] = num_index

    ###########################################################################
    ### 下载月度行情数据和核心指标：个股、指数、基金 |notes：3种资产的指标不太一样。
    if "input_get_quote" in request.POST.keys():     
        ### 
        obj_db ={}
        obj_db["db_name"] = "db_quote.sqlite3"
        obj_db["table_name"] = request.POST.get("table_name_get_quote","")
        type_asset = request.POST.get("type_asset_get_quote","")
        if len( type_asset ) > 0 :
            obj_db["type_asset"] = type_asset
        # print( "type_asset=",len( type_asset ), type_asset) 
        # else :
            ### 下载全部资产类别

        obj_db["date_begin"] = request.POST.get("date_begin_get_quote","")
        obj_db["date_end"] = request.POST.get("date_end_get_quote","")

        #############################################
        ### 要判断数据来源: wind_api, choice_api, wind_wds 
        obj_db["data_source"] = request.POST.get("data_source_get_quote","")
        
        from assets import quote_ashares_index_fund_month
        quote_1 = quote_ashares_index_fund_month()
        obj_data = quote_1.manage_quote_ashares_month(obj_db)

    




    ###########################################################################
    ### 






    ###########################################################################
    ### 








    #################################################################################
    ### 输出的dict对象 context  # 
    ### 查询列表         

    ### 时间
    context["time_now_str"] = time_now_str
    context["time_now_str_pre1d"] =time_now_str_pre1d 




    return render(request, 'ciss_exhi/data/monitor_market_data.html', context) 





























