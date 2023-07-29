# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
todo:对应pms_manage.html的主要页面功能
功能：
last 20220209 | since 20220225
Notes: 
refernce:derived from views_quick.py
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
def pms_manage(request):
    ###########################################################################
    ### 快速功能
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates
    ### 若html页面商点击input按钮，则dict变量request.POST里会增加key为input:name,value为input:value
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    
    ############################################################################
    ############################################################################
    ### 0，组合日内盯盘和监控
    ### TODO：1，给定前一交易日日期，下载持仓文件，导入持仓文件；2，展示日内涨跌幅度和持仓盈亏幅度，最新持仓权重百分比


    ############################################################################
    ### 1，基金和FOF组合区间收益率
    ### todo 提取组合绩效指标：涨跌幅、最大回撤、alpha、Sharpe、月胜率：年初至今、近1周、近1月、3月、6月、1年   
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
        from funds import fund_exhi
        class_fund_exhi = fund_exhi()   
        ### 
        obj_f ={} 
        obj_f[ "exhi_type"] = "unit"
        temp_fund_code = request.POST.get("fund_code_2","720001.OF")
        obj_f[ "fund_code"]= temp_fund_code 
        ### 偏股混合型基金指数 885001.WI
        obj_f[ "benchmark_code"]= request.POST.get("benchmark_code","885001.WI") 
        ### 净值类型
        obj_f[ "unit_type"]= request.POST.get("unit_type","week")        
        ### 获取区间开始和结束日期
        obj_f[ "date_begin"]= request.POST.get("date_begin","20201231")
        obj_f[ "date_end"]  = request.POST.get("date_end","20211231")

        ##########################################
        ### 基金净值和比较基准净值
        # obj_f = class_fund_exhi.get_fund_exhi(obj_f) 
        # ### output：index 是日期 2022-01-14 ； obj_f["df_unit"]:"exhi_unit_fund","exhi_unit_bench"]
        # # notes:对于index是日期地，需要再次转置！！！否则index显示地是column指标        
        # context["df_unit"] =  obj_f["df_unit"].T

        ### TEST 
        list1 = ["2021年12月31日","2022年1月7日","2022年1月14日","2022年1月21日","2022年1月28日","2022年2月11日","2022年2月14日"]
        list2 = [ 1.0, 1.016, 0.9979, 1.0095, 0.9968, 1.0815, 1.0545 ]
        list3 = [ 1.0, 0.9508, 0.9442, 0.9346, 0.8999, 0.8854, 0.8804 ]
        df_unit = pd.DataFrame([list2,list3] ,index=["exhi_unit_fund","exhi_unit_bench"] ,columns=list1 )
        print("df_unit \n", df_unit )
        # 不用转换
        context["df_unit"] =  df_unit

        ### 基金代码和基准代码
        context["fund_code"] =  obj_f[ "fund_code"]
        context["benchmark_code"] = obj_f[ "benchmark_code"]
    
    
    ###########################################################################
    ### 2，PMS组合管理 ：pms组合的净值和持仓监控
    ######################################
    ### pms 0：查询组合列表
    if "input_port_type" in request.POST.keys():    
        port_type = request.POST.get("port_type","stock") 
        from ports import portfolio_return
        class_portfolio_return = portfolio_return()        
        ### 
        obj_p ={}
        obj_p["port_type"] = port_type
        obj_p= class_portfolio_return.get_port_list( obj_p )

        context["df_p"] = obj_p["df_p"].T 

    ######################################
    ### 提取组合绩效指标：涨跌幅、最大回撤、alpha、Sharpe、月胜率：年初至今、近1周、近1月、3月、6月、1年
    
    if "input_port_type_perf" in request.POST.keys():   
        ### 提取组合绩效指标，如收益率、回撤等
        obj_p ={}        

        port_type_name = request.POST.get("port_type_name","固收加FOF20") 
        ### 取值2种情况：单组合名称 或组合类型
        if port_type_name in ["stock","fund","bond","hedge","all"] :
            obj_p["port_type"] = port_type_name
        else :
            ### 单组合，
            obj_p["port_name"] = port_type_name
        
        from ports import portfolio_return
        class_portfolio_return = portfolio_return()      
        
        ### get_wps 意味着需要提取组合绩效指标
        obj_p["get_wps"] = 1
        ### 获取需要提取收益率的组合
        obj_p= class_portfolio_return.get_port_perf( obj_p )
        ### 
        df_perf = obj_p["df_perf"] 

        context["df_perf"] = df_perf.T 

    ######################################
    ### 提取组合净值 
    # 用 pms_manage.xlsx 里的sheet=port_unit,维护组合净值数据，对应最近一次提取的净值
    if "input_port_name_unit" in request.POST.keys():  
        obj_p ={} 
        obj_p["port_name"]  = request.POST.get("port_name","")   
        obj_p["benchmark_code"]  = request.POST.get("benchmark_code","885001.WI")   
        ### 净值类型
        obj_p["unit_type"]= request.POST.get("unit_type","week")  
        ### 获取区间开始和结束日期   
        obj_p["date_begin"]= request.POST.get("date_begin","20201231")
        obj_p["date_end"]  = request.POST.get("date_end","20211231")

        from ports import portfolio_return
        class_portfolio_return = portfolio_return()      
        
        
        ### get_wpd 意味着需要
        obj_p["get_wpd"] = 1
        ### 获取需要提取收益率的组合
        obj_p= class_portfolio_return.get_port_unit( obj_p )
        
        ### 
        df_port = obj_p["df_port"] 
        context["df_port"] = df_port.T 
        context["df_port_tail10"] = df_port.tail(10).T 
        ### 组合名称和基准代码
        context["port_name"] =  obj_p[ "port_name"]
        context["benchmark_code"] = obj_p[ "benchmark_code"]

    ###########################################################################
    ### port_holdings_from_PMS">5-1，PMS提取组合持仓：股票、基金、债券、指数
    ###########################################################################
    if "input_port_holdings_from_PMS" in request.POST.keys():  
        obj_p ={} 
        obj_p["port_name"]  = request.POST.get("pms_name_holdings_from_PMS","")   
        ### notes:开始和结束日期是月末交易日，周末假期会报错
        obj_p["date_begin"]  = request.POST.get("date_begin_holdings_from_PMS","")   
        obj_p["date_end"]  = request.POST.get("date_end_holdings_from_PMS","")   
        ### 备选项 
        obj_p["if_quote"]  = request.POST.get("if_quote_holdings_from_PMS","")   
        obj_p["if_indicator"]  = request.POST.get("if_indicator_holdings_from_PMS","")   
        
        ###########################################################################
        ### 
        from ports import portfolio_return
        class_portfolio_return = portfolio_return()     
        obj_p = class_portfolio_return.get_port_holding(obj_p)
        
        ###########################################################################
        ### 展示列值的调整
        df_h = obj_p["df_pms_holding"]
        df_h["weight"] = df_h["weight"]*100
        df_h["weight"] = df_h["weight"].round(decimals=2)  
        df_h["weight_init"] = df_h["weight_init"]*100
        df_h["weight_init"] = df_h["weight_init"].round(decimals=2)  
        df_h["pnl_pct"] = df_h["pnl_pct"]*100
        df_h["pnl_pct"] = df_h["pnl_pct"].round(decimals=2)  
        ### 
        df_h["mv_exhi"] = df_h["NetHoldingValue"]/10000
        df_h["mv_exhi"] =df_h["mv_exhi"].round(decimals=2)  
        df_h["mv_init"] = df_h["TotalCost"]/10000
        df_h["mv_init"] = df_h["mv_init"].round(decimals=2)  
        df_h["pnl_exhi"] = df_h["EUnrealizedPL"]/10000 
        df_h["pnl_exhi"] = df_h["pnl_exhi"].round(decimals=2)  
        df_h["num_exhi"] = df_h["Position"]/10000
        df_h["num_exhi"]=df_h["num_exhi"].round(decimals=2)  
        ### 按盈亏率排序
        df_h =df_h.sort_values(by="pnl_exhi", ascending=True )

        print("Debug=666= \n", df_h.head().T )
        context["df_pms_holding"]  = df_h.T

    ###########################################################################
    ### port_holdings_from_stra_file">5-2，本地策略文件提取组合持仓：股票、基金、债券、指数


    ###########################################################################
    ###########################################################################
    ### 6，PMS多策略组合调整
    ###########################################################################
    ### 6.1：给定组合名称或分类，显示组合策略配置；   
    # 需要用到sheet=组合列表，组合策略配置，file= pms_manage.xlsx
    # notes:230322新增权重和合计列和策略列，合计包括：weight	weight_equity	weight_bond
    if "input_port_stra_weight" in request.POST.keys():    
        obj_p ={}        

        port_type_name = request.POST.get("port_stra_weight","固收加FOF20") 

        ##########################################
        ### 导入配置文件
        from config_data import config_data
        config_data_1 = config_data() 
        ### 导入组合列表
        sheet = "组合列表"
        file_name ="pms_manage.xlsx"
        df_p = pd.read_excel( config_data_1.obj_config["dict"]["path_data_pms"] +file_name,sheet_name=sheet )
        
        ##########################################
        ### 取值2种情况：单组合名称 或组合类型
        if port_type_name in ["stock","fund","bond","hedge","all"] :
            if not port_type_name == "all" :
                df_p = df_p [ df_p["asset_type"] == port_type_name ] 
        else :
            ### 单组合，
            df_p = df_p [ df_p["port_name"] == port_type_name ]          
        
        ##########################################
        ### 导入组合的策略配置比例
        sheet = "组合策略配置" 
        df_psw = pd.read_excel( config_data_1.obj_config["dict"]["path_data_pms"] +file_name,sheet_name=sheet )
        ### 筛选部分组合
        df_psw = df_psw[ df_psw["port_name"].isin( df_p["port_name"] )  ] 
        ### 百分位
        for temp_col in df_psw.columns: 
            if temp_col != "port_name" :
                df_psw[temp_col] = df_psw[temp_col] *100 
        
        ##########################################
        ### 把 NaN 替代掉
        df_psw = df_psw.fillna("")       

        print("Debug== df_port_stra_weight \n", df_psw )
        ### save to output
        context["df_port_stra_weight"] = df_psw.T 

    ###########################################################################
    ### 6.2，查询策略被哪些组合配置
    if "input_stra_by_port" in request.POST.keys():    
        obj_p ={}        

        stra_name = request.POST.get("stra_name","stockpool_active") 

        ##########################################
        ### 导入配置文件
        from config_data import config_data
        config_data_1 = config_data() 
        ### 导入组合的策略配置比例
        sheet = "组合策略配置" 
        file_name ="pms_manage.xlsx"        
        df_psw = pd.read_excel( config_data_1.obj_config["dict"]["path_data_pms"] +file_name,sheet_name=sheet )
        
        ### 筛选部分组合
        df_psw = df_psw[ df_psw[ stra_name ] > 0.0   ] 
        
        ##########################################
        ### 把 NaN 替代掉
        df_psw = df_psw.fillna("")    

        ##########################################  
        ### save to output
        context["df_stra_weight_port"] = df_psw.T 


    ###########################################################################
    ### 生成组合配置文件
    ### steps:1,选择组合，导入组合的不同策略配置比例；2，分别导入单个策略配置文件，；3，对组合内所有策略的持仓做合并同类权重；
    ### 4，实盘组合需要和现有持仓比较，计算差额数量、生成交易指令。5，生成组合配置文件。
    if "input_pms_multi_stra" in request.POST.keys():  
        ### 1,选择组合，导入组合的不同策略配置比例； 
        date_pms_multi_stra =  request.POST.get("date_pms_multi_stra","")   
        ### REQUEST.getlist取到list形式的提交结果
        port_name_list = request.POST.getlist("port_name_list","")          

        ######################################
        ### Notes:策略列表的每个策略对应一个策略文件
        ##########################################
        ### 导入配置文件
        from config_data import config_data
        config_data_1 = config_data()  
        ### 读取组合基本信息和权益配置权重等
        path_data_pms = config_data_1.obj_config["dict"]["path_data_pms"] 
        path_data_adj = config_data_1.obj_config["dict"]["path_data_adj"] 
        path_stra = config_data_1.obj_config["dict"]["path_stra"] 

        path_port_weight = path_data_pms + "port_weight\\"
        file_name = "pms_manage.xlsx" 
        df_port_stra = pd.read_excel(path_data_pms + file_name, sheet_name="组合策略配置"  ) 
        print("df_port_stra \n" , df_port_stra  )

        ##########################################
        ### 判断要更新的所有组合
        if "if_all_port_pms_multi_stra" in request.POST.keys():   
            port_name_list = list( df_port_stra["port_name"] )
            ### <class 'list'> ['指数股债配置', 'FOF行业景气配置', 'FOF期权9901']
        print("port_name_list 2", type(port_name_list) )
        print( port_name_list )  

        ### 计算每个策略的配置文件 
        stra_list = list( df_port_stra.columns) 
        ### delete port_name from stra_list 
        if  "port_name" in stra_list :
            stra_list.remove(  "port_name" )
        

        print("Debug port_name_list ", port_name_list )   
        ######################################
        for temp_port in port_name_list :     
            print("Debug \n", temp_port )   
            print("stra_list:",stra_list  )
            df_sub = df_port_stra[ df_port_stra["port_name"] == temp_port ]
            if len( df_sub.index ) == 1 :
                ######################################
                ### find strategy weights | 大部分组合是单策略组合。
                count_stra = 0 
                for temp_stra in stra_list :
                    w_stra = df_sub[temp_stra].values[0]
                    print("w_stra:",temp_stra, w_stra   )
                    if w_stra > 0.005 :
                        ### 判断文件是否存在
                        file_stra = "stra_" + temp_stra + "_" + date_pms_multi_stra + ".xlsx"
                        # 如果没有，就用以前的。
                        file_stra_back = "stra_" + temp_stra  + ".xlsx"
                        ### 根据策略权重，获取策略持仓 | 例如: stra_fundpool_activestock_20220308.xlsx
                        if_exist_stra = 0 
                        if os.path.exists(path_stra+file_stra   ) :
                            df_stra_weight = pd.read_excel(path_stra+file_stra   )
                            if_exist_stra = 1 
                        if os.path.exists(path_stra+file_stra_back   ) :
                            df_stra_weight = pd.read_excel(path_stra+file_stra_back   )
                            if_exist_stra = 1 
                        ### if_exist_stra
                        if if_exist_stra == 1 :                        
                            ### 只需要部分columns：["基金代码","weight","代码","code"]
                            if "code" in df_stra_weight.columns :
                                ### 
                                df_stra_weight = df_stra_weight.loc[:, ["code", "weight"] ]
                            elif "基金代码" in df_stra_weight.columns :
                                df_stra_weight["code"] = df_stra_weight["基金代码"]
                                df_stra_weight = df_stra_weight.loc[:, ["code", "weight"] ]
                            elif "代码" in df_stra_weight.columns :
                                df_stra_weight["code"] = df_stra_weight["代码"]
                                df_stra_weight = df_stra_weight.loc[:, ["code", "weight"] ]

                            ######################################
                            ### append 汇总
                            if count_stra == 0 :
                                df_stra_weight[ "weight"] =df_stra_weight[ "weight"]* w_stra
                                df_stra_weight_all = df_stra_weight
                                count_stra = count_stra+1 
                            else :
                                df_stra_weight[ "weight"] =df_stra_weight[ "weight"]* w_stra
                                df_stra_weight_all = df_stra_weight_all.append( df_stra_weight ) 
                                count_stra = count_stra+1 

                        else : 
                            print("No file:", path_stra+file_stra ,path_stra+file_stra_back )

                ######################################
                ### group合并权重后去除重复项，不同策略可能有相同地证券
                df_port_weight = df_stra_weight_all.groupby("code")["weight"].sum()
                print( "df_port_weight" )
                print( df_port_weight )
                df_port_weight.to_excel("D:\\df_port_weight.xlsx")

            ############################################################################
            ### Save to pms file 
            file_port = "port_" + temp_port + "_" + date_pms_multi_stra + ".xlsx"
            df_port_weight.to_excel(path_port_weight+file_port   )
            ############################################################################
            ### 
        ###
        # 每个组合都有一个df_port_weight 文件
    
    ###########################################################################
    ### PMS上传组合调整
    ### steps:1,获取组合配置文件；2，上传至PMS；3，更新组合日志文件
    if "input_pms_upload_adjust" in request.POST.keys():  
        ### 1,选择组合，导入组合的不同策略配置比例； 
        date_pms_adjust =  request.POST.get("date_pms_adjust","")   
        ### REQUEST.getlist取到list形式的提交结果
        port_name_list_upload = request.POST.getlist("port_name_list_upload","")  
        ### <class 'list'> ['指数股债配置', 'FOF行业景气配置', 'FOF期权9901']
        print("port_name_list_upload", port_name_list_upload )

        ##########################################
        ### 导入配置文件
        from config_data import config_data
        config_data_1 = config_data()  
        ### 读取组合基本信息和权益配置权重等
        path_data_pms = config_data_1.obj_config["dict"]["path_data_pms"]
        path_data_adj = config_data_1.obj_config["dict"]["path_data_adj"] 
        path_stra = config_data_1.obj_config["dict"]["path_stra"] 
        path_port_weight = path_data_pms + "port_weight\\"
        file_name = "pms_manage.xlsx" 
        df_port_list= pd.read_excel(path_data_pms+file_name,sheet_name="组合列表" )
        df_port_list = df_port_list[ df_port_list["if_active"] ==1 ]
        print("df_port_list")
        print( df_port_list )

        ##########################################
        ### 判断要更新的所有组合
        if "if_all_port_pms_upload_adjust" in request.POST.keys():   
            df_port_stra = pd.read_excel(path_data_pms + file_name, sheet_name="组合策略配置"  )  
            port_name_list_upload = list( df_port_stra["port_name"] )
            ### <class 'list'> ['指数股债配置', 'FOF行业景气配置', 'FOF期权9901']
        print("port_name_list_upload 2", type(port_name_list_upload) )
        print( port_name_list_upload )  


        for temp_port in port_name_list_upload :
            ### find port info in df_port_list
            df_sub = df_port_list[ df_port_list["port_name"]==temp_port ]
            if len( df_sub.index ) > 0 :
                ### 
                obj_port = {}    
                obj_port["asset_type"] = df_sub["asset_type"].values[0]                
                obj_port["date_latest"] = date_pms_adjust
                obj_port["port_name"] = temp_port 
                if obj_port["asset_type"] == "stock" :
                    obj_port["weight_equity"] = df_sub["weight_equity"].values[0] 
                ### port_躺赢纯债基_20220308.xlsx
                file_name = "port_" + temp_port + "_" + date_pms_adjust + ".xlsx"
                obj_port["df_port"] = pd.read_excel(path_port_weight+ file_name )
        
            ######################################
            ### 
            from ports import manage_pms
            class_manage_pms = manage_pms()  
            obj_port = class_manage_pms.pms_upload( obj_port )
            


    #################################################################################
    ### 输出的dict对象 context  # 
    ### 查询PMS组合列表
    from ports import portfolio_return
    class_portfolio_return = portfolio_return()        
    ### 获取组合列表和策略列表
    obj_p ={}
    obj_p["port_type"] = "all"
    ### C:\rc_2023\rc_202X\data_pms ; pms_manage.xlsx
    obj_p= class_portfolio_return.get_port_list( obj_p )
    ### 要剔除不在维护的组合
    obj_p["df_p"] = obj_p["df_p"][ obj_p["df_p"]["if_active"] >0 ]
    ###
    context["df_pms_list"] = obj_p["df_p"].T 
    ### notes:原始df_port_stra_weight，index行是组合，列是策略；这里不transpose转置，html网页里index就是策略
    context["df_stra_port_list"] = obj_p["df_port_stra_weight"]

    ### 时间
    context["time_now_str"] = time_now_str
    context["time_now_str_pre1d"] =time_now_str_pre1d

    ### debug 测试用数据
    df1=pd.DataFrame([[1,2,3],[1,3,4],[7,9,4],[1,2,3],[1,3,4]],columns=["a","b","c"] )
    context["df1"] = df1.T


    return render(request,"ciss_exhi/pms_manage.html",context) 



















































