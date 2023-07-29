# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
todo:对应quick.html的主要页面功能
功能：
last 20220209 | since 20220209
Notes: 
refernce:  views.py
目录：
1，市场和组合监控
2，PMS组合管理
3，股票策略
4，数据下载、分许、统计和指标计算
5，基金池和FOF
notes:202002，Django3下，render_to_response被取消，用render 代替render_to_response 
===============================================
''' 

### Initialization
from django.shortcuts import render
from django.http import HttpResponse
### Import haystack 
### 202002，Django3下，render_to_response被取消，用render 代替render_to_response 
# from django.shortcuts import render_to_response
###  you must use csrf_protect on any views that use the csrf_token template tag, as well as those that accept the POST data.
# source https://docs.djangoproject.com/en/2.1/ref/csrf/
# source https://blog.csdn.net/weixin_40612082/article/details/80686472
from django.views.decorators.csrf import csrf_protect,requires_csrf_token,csrf_exempt

###########################################################################
import sys,os

from ciss_exhi.views import index 
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
@csrf_protect
@requires_csrf_token
@csrf_exempt
def quick(request):
    ###########################################################################
    ### 快速功能
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates
    ### 若html页面商点击input按钮，则dict变量request.POST里会增加key为input:name,value为input:value
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )

    ###########################################################################
    ### 1，市场和组合监控 |TODO:个股和指数——行业风格分组——策略——组合；评估
    ######################################
    ### 市场指数监控和个股统计 
    ### 用市场监控模块获取主要指数区间涨跌幅，file=markets.py  
    ######################################
    ### 1，打印指数类型的列表+获取给定区间的涨跌幅
    if "input_index_type_period" in request.POST.keys():    
        from markets import markets_monitor
        class_markets_monitor = markets_monitor()    
        ### 
        obj_m ={}
        temp_index_type = request.POST.get("index_type2","A股")
        obj_m[ "temp_index_type"]= temp_index_type 
        ### 获取区间开始和结束日期
        obj_m[ "date_begin"]= request.POST.get("date_begin","20201231")
        obj_m[ "date_end"]  = request.POST.get("date_end","20211231")
        
        obj_m = class_markets_monitor.market_index_main(obj_m)
        ### column= "period",对应给定区间内的涨跌幅百分比
        ### 需要转置transpose
        context["df_chg"] =  obj_m["df_chg"].T 
        
    ######################################
    ### 2，只打印指数类型的列表
    elif "input_index_type" in request.POST.keys():       
        from markets import markets_monitor
        class_markets_monitor = markets_monitor()    
        ### 
        obj_m ={}
        temp_index_type = request.POST.get("index_type","A股")
        obj_m[ "temp_index_type"]= temp_index_type
        obj_m = class_markets_monitor.market_index_main(obj_m) 
        ### 把对象传给 obj_m["df_index_list"] 
        df_index_list  = obj_m["df_index_list"] 
        # df_index_list_sub.reset_index().T 
        context["df_index_list"] = df_index_list.T 
    
    ######################################
    ### 个股动量和区间收益率；个股统计 todo

    ######################################
    ### 股票行业分组动量和区间收益率 todo


    ############################################################################
    ### 基金和FOF组合区间收益率
    ##########################################
    ### 给定基金代码和日期区间，计算区间业绩指标和基金近3年内净值,以及对应基准指数相对收益
    if "input_fund_perf" in request.POST.keys():   
        from funds import fund_exhi
        class_fund_exhi = fund_exhi()    
        ### 
        obj_f ={} 
        obj_f[ "exhi_type"] = "perf"
        temp_fund_code = request.POST.get("fund_code","")
        obj_f[ "fund_code"]= temp_fund_code 
        print("temp_fund_code=",temp_fund_code )
        ### 偏股混合型基金指数 885001.WI
        # obj_f[ "benchmark_code"]= request.POST.get("benchmark_code","885001.WI")
        
        ### 获取区间开始和结束日期
        obj_f[ "date_begin"]= request.POST.get("date_begin","20201231")
        obj_f[ "date_end"]  = request.POST.get("date_end","20211231")
        ### 1 means 要简历， 0 不要
        obj_f[ "if_jjjl_resume"] = request.POST.get("if_jjjl_resume","0")
        ###
        obj_f = class_fund_exhi.get_fund_exhi(obj_f) 
        ### 需要转置transpose
        context["df_perf"] =  obj_f["df_perf"].T 
        ### 基金经理简历
        if obj_f[ "if_jjjl_resume"] in [1,"1" ] :
            context["fund_manager_resume"] =  obj_f["fund_manager_resume"] 
        ### 基金经理业绩描述
        context["str_perf"] =  obj_f["str_perf"] 
        
    
    ##########################################
    ### 基金净值和相对基准 | 周
    if "input_fund_unit" in request.POST.keys():           
        ### 
        obj_f ={} 
        obj_f[ "exhi_type"] = "unit"
        temp_fund_code = request.POST.get("fund_code_2","")
        obj_f[ "fund_code"]= temp_fund_code 
        ### 偏股混合型基金指数 885001.WI
        obj_f[ "benchmark_code"]= request.POST.get("benchmark_code","") 
        ### 净值类型，week,day
        obj_f[ "unit_type"]= request.POST.get("unit_type","")        
        ### 获取区间开始和结束日期
        obj_f[ "date_begin"]= request.POST.get("date_begin","")
        obj_f[ "date_end"]  = request.POST.get("date_end","")
        
        ##########################################
        ### 基金净值和比较基准净值
        from funds import fund_exhi
        class_fund_exhi = fund_exhi()   
        obj_f = class_fund_exhi.get_fund_exhi(obj_f) 
        # ### output：index 是日期 2022-01-14 ； obj_f["df_unit"]:"exhi_unit_fund","exhi_unit_bench"]
        # # notes:对于index是日期地，需要再次转置！！！否则index显示地是column指标        
        context["df_unit"] =  obj_f["df_unit"].T

        # print("Debug== df-unit \n", obj_f["df_unit"].T )
        ##########################################
        ### TEST 
        # list1 = ["2021年12月31日","2022年1月7日","2022年1月14日","2022年1月21日","2022年1月28日","2022年2月11日","2022年2月14日"]
        # list2 = [ 1.0, 1.016, 0.9979, 1.0095, 0.9968, 1.0815, 1.0545 ]
        # list3 = [ 1.0, 0.9508, 0.9442, 0.9346, 0.8999, 0.8854, 0.8804 ]
        # df_unit = pd.DataFrame([list2,list3] ,index=["exhi_unit_fund","exhi_unit_bench"] ,columns=list1 )
        # print("df_unit \n", df_unit )
        # # 不用转换
        # context["df_unit"] =  df_unit

        ### 基金代码和基准代码
        context["fund_code"] =  obj_f[ "fund_code"]
        context["benchmark_code"] = obj_f[ "benchmark_code"]
    
    
    ###########################################################################
    ### 2，PMS组合管理 ：pms组合的净值和持仓监控 | views_pms_manage.py
    ###########################################################################
    ### input_stra_fof_fund value="FOF重仓基金优选"
    if "input_stra_fof_fund" in request.POST.keys():  
        date_latest = request.POST.get("date_fof_activestock","")  
        quarter_end = request.POST.get("date_fof_q_end","")   

        obj_fund = {}
        obj_fund["date_latest"] = date_latest        
        obj_fund["quarter_end"] = quarter_end      
        obj_fund["selection_type"] = request.POST.get("fof_fund_type","") 

        from funds import fundpool
        class_fundpool = fundpool()   
        obj_fund = class_fundpool.fof_selection( obj_fund )
        ### 转置
        context["df_funds"] = obj_fund["df_funds"].T 

    ###########################################################################
    ### input_stra_fund_type_stock" value="基金重仓股票优选
    if "input_stra_fund_type_stock" in request.POST.keys():  
        date_latest = request.POST.get("date_fund_type_stock","")  
        quarter_end = request.POST.get("date_q_end_fund_type_stock","")   

        obj_fund = {}
        obj_fund["date_latest"] = date_latest        
        obj_fund["quarter_end"] = quarter_end      
        obj_fund["selection_type"] = request.POST.get("fund_type_stock","") 

        from funds import fundpool
        class_fundpool = fundpool()   
        obj_fund = class_fundpool.fund_stock_selection ( obj_fund )
        ### 转置
        context["df_stocks"] = obj_fund["df_stocks"].T 

    # fund_stock_selection
    ###########################################################################
    ### 行业轮动-股票基金
    ###########################################################################
    ### input_stra_fund_ind_active , 行业轮动-股票基金-主动行业轮动
    if "input_stra_fund_ind_active" in request.POST.keys():  
        date_latest = request.POST.get("date_fund_ind_active","")   
        type_ind_style = request.POST.get("stra_type4weight","")    

        ##########################################
        ### 导入配置文件
        from config_data import config_data
        config_data_1 = config_data() 

        ### 1,读取策略权重文件
        file_name = "pms_manage.xlsx"
        df_stra_weight = pd.read_excel( config_data_1.obj_config["dict"]["path_data_pms"] +file_name, sheet_name="stra_weight"   )
        
        ### 2，按type_ind_style 筛选策略持仓权重
        df_stra_weight = df_stra_weight[ df_stra_weight["type_ind_style"] == type_ind_style ] 
        
        ### 3，导出excel，和展示
        if len( df_stra_weight.index ) > 1 : 
            file_name = "stra_fund_ind_active_" + date_latest + ".xlsx"
            df_stra_weight.to_excel( config_data_1.obj_config["dict"]["path_data_adj"] + file_name ,index=False)
            file_name = "stra_fund_ind_active.xlsx"
            df_stra_weight.to_excel( config_data_1.obj_config["dict"]["path_data_adj"] + file_name ,index=False)

        ### 和展示
        df_stra_weight["exhi_"+ "weight"] = df_stra_weight["weight"].round(decimals=4 )

        context["df_stra_weight"] = df_stra_weight.T
        
    ###########################################################################
    ### 风格轮动-股票基金- 
    ###########################################################################
    ### input_stra_fund_market_trend , 风格轮动-股票基金-市场风格动量趋势
    if "input_stra_fund_market_trend" in request.POST.keys():  
        date_latest = request.POST.get("date_fund_market_trend","")   
        type_market_style = request.POST.get("stra_type4weight_market","")    
        ##########################################
        ### 导入配置文件
        from config_data import config_data
        config_data_1 = config_data() 

        ### 1,读取策略权重文件
        file_name = "pms_manage.xlsx"
        df_stra_weight = pd.read_excel( config_data_1.obj_config["dict"]["path_data_pms"] +file_name, sheet_name="stra_weight"   )
        
        ### 2，按type_market_style 筛选策略持仓权重
        df_stra_weight = df_stra_weight[ df_stra_weight["type_ind_style"] == type_market_style ] 
        
        ### 3，导出excel，和展示
        if len( df_stra_weight.index ) > 1 : 
            file_name = "stra_fund_market_trend_" + date_latest + ".xlsx"
            df_stra_weight.to_excel( config_data_1.obj_config["dict"]["path_data_adj"] + file_name  ,index=False)
            file_name = "stra_fund_market_trend.xlsx"
            df_stra_weight.to_excel( config_data_1.obj_config["dict"]["path_data_adj"] + file_name  ,index=False)

        ### 和展示
        df_stra_weight["exhi_"+ "weight"] = df_stra_weight["weight"].round(decimals=4 )

        context["df_stra_weight"] = df_stra_weight.T
        

    ###########################################################################
    ###########################################################################
    ### 4，数据下载、分许、统计和指标计算 
    # derived from test_pms_manage.py 
    if "input_date_windapi_shares" in request.POST.keys():  
        ### 用Wind导出的A股和港股和基金数据，用Wind-API提取价量指标
        date_data_terminal = request.POST.get("date_windapi_shares","")  

        from get_wind_api import wind_api
        class_wind_api = wind_api()
        # 20220107
        df_shares = class_wind_api.get_wss_ma_amt_mv( date_data_terminal )
        ### 计算完后，在网页里显示数据文件的位置 
        context["date_data_terminal"] = date_data_terminal


    if "input_cal_shares_trend" in request.POST.keys():  
        ### 计算AH股策略指标:中短期趋势abcd3d 
        date_cal_shares_trend = request.POST.get("date_cal_shares_trend","") 
        # 20220107
        #对应path：path_ciss_rc + "db\\analysis_indicators\\"
        from analysis_data_statistics import data_stat
        class_data_stat = data_stat() 
        obj_data = class_data_stat.cal_AH_momentum_abcd3d( date_cal_shares_trend )
        context["date_cal_shares_trend"] = date_cal_shares_trend 
    

    #################################################################################
    ### 输出的dict对象 context  # 
    ### 查询PMS组合列表
    from ports import portfolio_return
    class_portfolio_return = portfolio_return()        
    ### 
    obj_p ={}
    obj_p["port_type"] = "all"
    obj_p= class_portfolio_return.get_port_list( obj_p )
    context["df_pms_list"] = obj_p["df_p"].T 

    ### 时间
    context["time_now_str"] = time_now_str
    context["time_now_str_pre1d"] =time_now_str_pre1d

    ### debug 测试用数据
    df1=pd.DataFrame([[1,2,3],[1,3,4],[7,9,4],[1,2,3],[1,3,4]],columns=["a","b","c"] )
    context["df1"] = df1.T


    return render(request,"ciss_exhi/quick.html",context)
    # return render(request,"ciss_exhi/quick.html")



















































