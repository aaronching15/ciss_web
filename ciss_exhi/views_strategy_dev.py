# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
todo:

功能：策略研究；Index of strategy list |

last 20230316 | since 20221014
refernce:derived from views.py
===============================================
''' 

###########################################################################
### Initialization
from operator import index
from attr import asdict
from django.shortcuts import render
from django.http import JsonResponse
from django.http import HttpResponse 
from django.views.decorators.csrf import csrf_protect,requires_csrf_token,csrf_exempt

###########################################################################
### 导入项目地址
import sys,os

from ciss_exhi.views import temp_ind

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

#################################################################################
### 策略研究；Index of strategy list |derived from views.py
### working on test strategy file 
from .models import fund_analysis
import pandas as pd 

@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_index(request): 
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### 时间,最近季末日期、最近年末日期
    from times import times
    class_t = times()
    obj_date = class_t.get_date_pre_post( time_now_str )
    ### 最近一个季度末日期
    context["date_pre_1q_end_str"] =obj_date["date_pre_1q_end_str"]
    context["date_report_pre_1halfyear_str"] =obj_date["date_report_pre_1halfyear_str"]
    ### 
    context["time_now_str"] = time_now_str
    context["time_now_str_pre1d"] =time_now_str_pre1d  

    #################################################################################
    ### 定义基金池list等常用变量 || ,"对冲reits","商品"
    fundpool_list_CN = ["主动股票","偏股混合","偏债混合","纯债","美股港股","FOF","货币","股票指数"]
    fundpool_list = [ "activestock","stock_mixed","bond_mixed","bond","HKUS","FOF","currency","index"  ]
    context["fundpool_list_CN"] = fundpool_list_CN



    ###########################################################################
    ### 更新策略权重，导入excel文件
    if "input_stra_w_update" in request.POST.keys():  
        print("get_data | request.POST", type(request.POST.keys() ),request.POST.keys() )
        print( "=====================================", request.POST )
        strategy_CN = request.POST.get("strategy_CN_update","")  
        date_update = request.POST.get("date_update","")  
        path_update = request.POST.get("path_update","")  
        file_update = request.POST.get("file_update","")  
        sheet_update = request.POST.get("sheet_update","")  

        ###################################################
        ### 导入sheet表格内至少需要2列， code，weight.需要确保退出持仓的品种在更新时有 0.0%的调整记录
        df_data = pd.read_excel( path_update + file_update, sheet_name= sheet_update  ) 
        
        ###################################################
        ### 检查日期数据的准确性
        if len( date_update ) == 8 : 
            for temp_i in df_data.index : 
                ###############################################
                ### 多个input必须至少有一项非空
                obj_db = {} 
                obj_db["db_name"] = "db_funda.sqlite3"
                obj_db["table_name"] = "fundpool_stockpool_weight"
                obj_db["insert_type"] = "1r"
                obj_db["dict_1r"] = {}
                ###
                obj_db["dict_1r"][ "date"] = date_update
                obj_db["dict_1r"][ "strategy_CN"] = strategy_CN
                ### 必须项
                for temp_col in ["code","weight","name"] :
                    obj_db["dict_1r"][temp_col] = df_data.loc[temp_i, temp_col ] 
                
                ### 非必须项
                temp_list = list( df_data.columns )
                col_nonrestrict = ["name","pool_name","pool_level","type_asset_indstyle_stock","strategy","code_fund","code_stock","note" ]
                col_nonrestrict = [  x for x in col_nonrestrict if x in temp_list  ]
                for temp_col in col_nonrestrict :
                    obj_db["dict_1r"][temp_col ] = df_data.loc[temp_i, temp_col ]  

                #############################################
                from database import db_sqlite
                db_sqlite1 = db_sqlite()
                obj_db = db_sqlite1.insert_table_data(obj_db)  


        else :
            print("Bug for incalid date ", date_update )


    ###########################################################################
    ### 从sqlite导出策略权重，保存excel文件
    if "input_stra_w_export" in request.POST.keys():  
        print("get_data | request.POST", type(request.POST.keys() ),request.POST.keys() )
        print( "=====================================", request.POST )
        ### derived from views_quick.py\\3.1，行业研究策略和股票池 if "input_stra_stock_ind" in request.POST.keys():  
        strategy_CN = request.POST.get("strategy_CN_export","")
        date = request.POST.get("date_export","")  
        ### define dict of strategy name 
        dict_stra_name= {} 
        dict_stra_name["股票行业研究"]= "stra_stockpool_active"
        dict_stra_name["量化股票策略"]= "stra_stock_indi"
        dict_stra_name["市场风格趋势"]= "fund_market_trend"
        dict_stra_name["基金行业轮动"]= "fund_ind_active"
        dict_stra_name["利率债"]= ""
        dict_stra_name["信用债双利"]= ""
        
        
        ############################################################################
        ### sqlite数据库表方式：
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
        obj_db["dict_select"] = {}
        obj_db["dict_select"][ "strategy_CN"] = request.POST.get("strategy_CN_export","")  
        obj_db["dict_select"][ "date"] = request.POST.get("date_export","")  

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db ) 
        ### obj_db["path_db "] = "C:\\rc_2023\\rc_202X\\ciss_web\\"
        path_db = obj_db["path_db "]
        ### path_0 =  "C:\\rc_2023\\rc_202X\\"
        path_0 = path_db.split("ciss_web")[0] 

        print("=======================\n", obj_db["df_data"] ) 

        ################################################################################
        ### 证券池调整：如果xxx策略的 xx基金\股票有多条记录，选择日期最新的一条
        df_data = obj_db["df_data"]
        ### step1 按日期降序排列
        df_data = df_data.sort_values(by="date" ,ascending=False )
        ### step2 根据列"code"删除重复项目，只保留第一项【第一项对应了最新的日期】
        df_data = df_data.drop_duplicates( subset=["code"] ,keep="first" )
        ### step3 | 只操作weight权重一列：将nan替换为 -1 ; 部分代码权重为0%或更小，对应了退出股票池
        import numpy as np  
        df_data["weight"] = df_data["weight"].astype("float")
        df_data["weight"] = df_data["weight"].replace(np.nan, -1 ) 
        df_data = df_data[ df_data["weight"] > 0.00001  ] 

        ###################################################
        ### 再次赋值给 df_port
        df_port = df_data  
        ### 选出不超过60只个股;单只个股最大权重不超过10%
        num_max = 60 
        df_port = df_port.iloc[:num_max, : ]
        df_port[ "weight" ] = df_port[ "weight" ]/df_port[ "weight" ].sum()

        ###################################################
        ### save to excel  || file_name = "stra_stockpool_active_" + date +".xlsx" 
        path_adj = path_0 + "data_pms\\data_adj\\"
        file_name = dict_stra_name[strategy_CN] +"_" + date +".xlsx" 
        df_port.to_excel( path_adj + file_name ,index=False ) 
        file_name =  dict_stra_name[strategy_CN]  +".xlsx" 
        df_port.to_excel(  path_adj + file_name ,index=False )  

    ###########################################################################
    ### 3，基金策略
    ###########################################################################
    ### stra_fundpool">基金池-股/债/混合等
    # derived from stra_fundpool_activestock">基金池-主动股票优选
    if "input_stra_fundpool" in request.POST.keys():  
        ### getlist
        fundpool_list_CN_sub = request.POST.getlist("fundpool_list_CN","")  
        print("fundpool_list_CN_sub=",type(fundpool_list_CN_sub), fundpool_list_CN_sub ) 

        date_latest = request.POST.get("date_stra_fundpool","")  
        # 1,0 
        if_purchase= request.POST.get("if_purchase","")  
        str_purchase = ""
        if if_purchase in [1,"1" ] :
            str_purchase = "-开放申购"
        # 
        if_latest_fundpool= request.POST.get("if_latest_fundpool","")  
        ### 
        obj_fund = {}
        obj_fund["date_latest"] = date_latest        
        obj_fund["col_name"] = "score"

        ### 对选中的每个基金池进行筛选
        for temp_fp_CN in fundpool_list_CN_sub : 
            ### 获取对应的英文基金池子
            print( "fundpool=", temp_fp_CN )
            temp_fp = fundpool_list [ fundpool_list_CN.index( temp_fp_CN )] 
            # file_name_output = "stra_fundpool_activestock_" + date_latest +".xlsx"  
            ### notes： 导出的策略文件名不需要包括 str_purchase = "-开放申购"
            obj_fund["file_name"] = "stra_fundpool_"+ temp_fp + "_" + date_latest +".xlsx" 
            if if_latest_fundpool in [1,"1"] :
                ### "file_name_output" 这个可选，有值的时候才会导出最新基金池数据
                obj_fund["file_name_output"] = "stra_fundpool_" + temp_fp + ".xlsx" 
        
            ### steps：1，导入基金池；file=基金池rc_纯债_20220308.xlsx
            obj_fund["file_input"] ="基金池rc_" + temp_fp_CN +str_purchase +"_" + date_latest + ".xlsx"
            
            from func_stra import stra_allocation
            class_stra_allocation = stra_allocation()             
            obj_fund = class_stra_allocation.fund_weights_by_score( obj_fund ) 
        


    ###########################################################################
    ### input_stra_fof_fund value="FOF重仓基金优选"
    if "input_stra_fof_fund" in request.POST.keys():  
        date_latest = request.POST.get("date_fof_activestock","")  
        quarter_end = request.POST.get("date_fof_q_end","")   

        # 1,0 
        if_purchase= request.POST.get("if_purchase_fof","")  
        ### file= 基金池rc_FOF_20220308.xlsx or 基金池rc_FOF-开放申购_20230424.xlsx
        str_purchase = ""
        if if_purchase in [1,"1" ] :
            str_purchase = "-开放申购"
            

        obj_fund = {}
        obj_fund["date_latest"] = date_latest        
        obj_fund["quarter_end"] = quarter_end      
        obj_fund["selection_type"] = request.POST.get("fof_fund_type","") 
        obj_fund["str_purchase"] = str_purchase       

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
        if_purchase= request.POST.get("if_purchase_stock","")  
        str_purchase = ""
        if if_purchase in [1,"1" ] :
            str_purchase = "-开放申购"
            

        obj_fund = {}
        obj_fund["date_latest"] = date_latest        
        obj_fund["quarter_end"] = quarter_end      
        obj_fund["selection_type"] = request.POST.get("fund_type_stock","") 
        obj_fund["str_purchase"] = str_purchase     
        from funds import fundpool
        class_fundpool = fundpool()   
        obj_fund = class_fundpool.fund_stock_selection ( obj_fund )
        ### 转置
        context["df_stocks"] = obj_fund["df_stocks"].T 


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
            df_stra_weight.to_excel( config_data_1.obj_config["dict"]["path_stra"] + file_name ,index=False)
            file_name = "stra_fund_ind_active.xlsx"
            df_stra_weight.to_excel( config_data_1.obj_config["dict"]["path_stra"] + file_name ,index=False)

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
            df_stra_weight.to_excel( config_data_1.obj_config["dict"]["path_stra"] + file_name  ,index=False)
            file_name = "stra_fund_market_trend.xlsx"
            df_stra_weight.to_excel( config_data_1.obj_config["dict"]["path_stra"] + file_name  ,index=False)

        ### 和展示
        df_stra_weight["exhi_"+ "weight"] = df_stra_weight["weight"].round(decimals=4 )

        context["df_stra_weight"] = df_stra_weight.T

    ###########################################################################
    ### 3，股票策略
    ###########################################################################
    ### 3.1，量化股票策略
    if "input_stra_stock_quant" in request.POST.keys():  

        ### 用Wind导出的A股和港股和基金数据，用Wind-API提取价量指标
        date_stra_stock_quant = request.POST.get("date_stra_stock_quant","")  
        from stockpools import stockpool
        class_stockpool = stockpool()
        obj_shares = {}
        obj_shares["date_latest"] = date_stra_stock_quant
        obj_shares = class_stockpool.cal_stockpool_indi( obj_shares )

        ##########################################
        ### Part 2 生成策略配置比例，不超过50只股票
        # col_list = ["m_ave_amt","m_ave_mv","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","trend_mid","trend_short"   ]
        # obj_shares["col_list"] = col_list
        from func_stra import stra_allocation
        class_stra_allocation = stra_allocation()             
        obj_port = class_stra_allocation.stock_weights_by_indi( obj_shares ) 
        ### 策略文件格式 file_name = "stra_port_indi_" + date_latest +".xlsx" 
        ### output：obj_port["df_port"] ; obj_port["df_port_ind"] ; obj_port["date_latest"]
        # 净资产收益率(TTM)	归母净利润同比增长率,中信一级行业,中信三级行业
        ############################################################################
        ### Exhi
        ### notes: 市盈率(TTM) 字符包含的符号没法传给context；需要改名
        df_port = obj_port["df_port"]
        df_port = df_port.rename(columns = {"市盈率(TTM)": "市盈率ttm"})
        df_port = df_port.rename(columns = {"净资产收益率(TTM)": "净资产收益率ttm"})
        for temp_col in ["weight","20日涨跌幅","60日涨跌幅","120日涨跌幅"] :
            df_port[ "exhi_"+ temp_col] = df_port[ temp_col] *100
            df_port[ "exhi_"+ temp_col] = df_port[ "exhi_"+ temp_col].round(decimals=2)             
        ###
        df_port[ "exhi_"+ "总市值1"] = df_port[ "总市值1"] /100000000
        df_port[ "exhi_"+ "总市值1"] =df_port[ "exhi_"+ "总市值1"].round(decimals=1)
        #
        print("Debug== " , "市盈率ttm","市盈率ttm" in df_port.index  )
        print( df_port.head().T )
        for temp_col in ["市盈率ttm","基金持股比例","净资产收益率ttm","归母净利润同比增长率"] : 
            df_port[ "exhi_"+ temp_col] = df_port[  temp_col].round(decimals=1)             
        
        ### columns: 代码,名称,weight,20日涨跌幅	60日涨跌幅	120日涨跌幅,总市值1,市盈率(TTM)	基金持股比例	
        context["df_port_weight"] = df_port.T
        
        ### ind:中信一级行业 ; columns: weight
        obj_port["df_port_ind"][ "exhi_"+ "weight" ] = obj_port["df_port_ind"][ "weight" ].round(decimals=4)
        context["df_port_ind"] = obj_port["df_port_ind"].T


    ###########################################################################
    ### 3.1，行业研究策略和股票池
    if "input_stra_stock_ind" in request.POST.keys():  
        
        ### 用Wind导出的A股和港股和基金数据，用Wind-API提取价量指标 
        date_stra_stock_ind = request.POST.get("date_stra_stock_ind","")  
        obj_shares = {}
        obj_shares["date_latest"] = date_stra_stock_ind
        from func_stra import stra_allocation
        class_stra_allocation = stra_allocation()     
        ############################################################################
        ### sqlite数据库表方式：20221014开始用sql
        obj_port = class_stra_allocation.stock_weights_by_active_sql( obj_shares ) 

        ### Excel-sheet导入方式 | before 20221013 用excel方式。        
        # obj_port = class_stra_allocation.stock_weights_by_active( obj_shares ) 

        ### 策略文件格式 file_name = "stra_port_active_" + date_latest +".xlsx" 
        ### output：obj_port["df_port"] ; obj_port["df_port_ind"] ; obj_port["date_latest"]
        # 净资产收益率(TTM)	归母净利润同比增长率,中信一级行业,中信三级行业
        ############################################################################
        ### Exhi
        ### notes: 市盈率(TTM) 字符包含的符号没法传给context；需要改名
        df_port = obj_port["df_port"]
        df_port = df_port.rename(columns = {"市盈率(TTM)": "市盈率ttm"})
        df_port = df_port.rename(columns = {"净资产收益率(TTM)": "净资产收益率ttm"})
        for temp_col in ["weight","20日涨跌幅","60日涨跌幅","120日涨跌幅"] :
            df_port[ "exhi_"+ temp_col] = df_port[ temp_col] *100
            df_port[ "exhi_"+ temp_col] = df_port[ "exhi_"+ temp_col].round(decimals=2)             
        ### 月均总市值 |excel模式是中文， sql模式下是 "mv"
        # df_port[ "exhi_"+ "总市值1"] = df_port[ "总市值1"] /100000000
        if "月均总市值" in df_port.keys() :
            df_port[ "exhi_"+ "总市值"] = df_port[ "月均总市值"] /100000000
        else : 
            df_port[ "exhi_"+ "总市值"] = df_port[ "mv"] /100
        df_port[ "exhi_"+ "总市值"] =df_port[ "exhi_"+ "总市值"].round(decimals=1)
        #
        # print("Debug== " , "市盈率ttm","市盈率ttm" in df_port.index  )
        print( df_port.head().T )
        for temp_col in ["市盈率ttm","基金持股比例","净资产收益率ttm","归母净利润同比增长率"] : 
            df_port[ "exhi_"+ temp_col] = df_port[  temp_col].round(decimals=1)             
        
        ### columns: 代码,名称,weight,20日涨跌幅	60日涨跌幅	120日涨跌幅,总市值1,市盈率(TTM)	基金持股比例	
        context["df_port_weight"] = df_port.T
        
        ### ind:中信一级行业 ; columns: weight
        obj_port["df_port_ind"][ "exhi_"+ "weight" ] = obj_port["df_port_ind"][ "weight" ].round(decimals=4)
        context["df_port_ind"] = obj_port["df_port_ind"].T





        
    
    ###################################################
    ### NOTES:Qs:会报错： no such table: ciss_exhi_strategy; 
    return render(request, 'ciss_exhi/strategy/stra_index.html', context)


####################################################################### 
### GETDATA 获取策略列表数据 
@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_weight_getdata(request):
    ### layui的table设置里，默认是 get
    #################################################################################
    ### 给定策略组合和日期，从sql查询最新的组合权重
    # if "input_stra_weight_search" in request.POST.keys():              
    if request.method == 'GET':
        print("get_data | request.GET", type(request.GET.keys() ),request.GET.keys() )
        print( "=====================================", request.GET )

        #######################################################
        ### 查找的input变量        
        dict_select = {}
        # col_list = ["id","date","strategy_CN","code","weight", "pool_name","pool_level","type_asset_indstyle_stock","strategy","name","code_fund","code_stock","note"]
        col_list = ["strategy_CN","date","ind","weight1","weight2" ]
        # notes:除了 "strategy_CN",其他几个都是筛选条件
        
        for temp_col in ["strategy_CN"] : 
            if temp_col in request.GET.keys() :
                if len( request.GET[temp_col] ) > 0 :
                    dict_select[temp_col] = request.GET[temp_col]
        # print("Debug | dict_select=", dict_select ) 
        # print("Debug | col_list=", col_list ) 
        #######################################################
        ### 获取表格数据
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
        obj_db["dict_select"] = dict_select 
        
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db ) 
        df_data = obj_db["df_data"]  
        ### 
        df_data = df_data.sort_values(by="date" ,ascending=False )

        #############################################
        ### 筛选1：根据输入的截至日期筛选 date 格式需要是 "20230315"
        if "date" in request.GET.keys() :
            temp_date = request.GET[ "date" ]
            if len(temp_date) == 8 :                
                ### <class 'numpy.int64'> df_data.loc[230, "date"]
                df_data = df_data[ df_data["date"] <= float( temp_date  ) ]
            #############################################
            ### 筛选2：同一证券选择最新日期权重数据 
            list_index = []
            list_code = list( df_data["code"].drop_duplicates() )
            for temp_code in list_code :
                df_sub = df_data[ df_data["code"] == temp_code ]
                ### 按日期倒叙排列
                df_sub = df_sub.sort_values(by="date" ,ascending=False )
                list_index = list_index + [ df_sub.index[0] ]
            
            df_data = df_data.loc[ list_index , : ]
            #############################################
            ### 筛选3：在最新日期权重数据的基础上，除权重小于 0.00001 的 和 0.0 的
            # notes: 需要str to float； data = data.astype({'outcome':'float','age':'int'})
            df_data = df_data.astype({'weight':'float' }) 
            df_data = df_data[ df_data["weight"] >= 0.000001 ]
            

        #############################################
        ### 筛选1：根据输入的权重区间、行业等筛选
        ### todo ind和weight数据暂时传不进来，因为可能只支持 变量的精确匹配，不能用区间匹配。
        # if "weight1" in request.GET.keys() :
        #     temp_weight1 = request.GET[ "weight1" ]
        #     if len(temp_weight1 ) == 8 :
        #         temp_weight1 = float(temp_weight1 ) 
        #         if temp_weight1 > 0  : 
        #             df_data = df_data[ df_data["weight"] >= temp_weight1  ]

        # if "weight2" in request.GET.keys() :
        #     temp_weight2 = request.GET[ "weight2" ]
        #     if len(temp_weight2 ) >0 :
        #         temp_weight2 = float(temp_weight2) 
        #         if temp_weight2 > 0  :
        #             df_data = df_data[ df_data["weight"] <= temp_weight2  ]

        #############################################
        ### 排序
        df_data = df_data.sort_values(by="id" ,ascending=False ) 
        #############################################
        ### notes:df不能直接df.to_json(),这样得到的是 str格式，df_data.to_json()不能赋值给JsonResponse里的data
        ### layui-table可以识别的data数据在views.py里长这样：dict_data= {1: {'id': 1, 'date': '600036','weight': 0.05},  
        # 2: {'id': 2, 'date': '300750', 'weight': 0.08},  
        ### Qs：如果直接 dict(df_data)会报错，TypeError: Object of type Series is not JSON serializable
        ### Ana: 由于json.dumps（）函数引起的。dumps是将dict数据转化为str数据，但是dict数据中包含byte数据所以会报错。

        col_list=["id","date","strategy_CN","code","name","weight","note"]
        dict_data = {}
        for i in df_data.index :
            dict_data[i]={}
            for temp_col in col_list : 
                dict_data[i][temp_col] = df_data.loc[i, temp_col] 

        ### notes: 必须用 df_data.T.to_dict() ,如果 dict( df_data ) 会报错
        ### notes：如果数据库记录里有多列是null空值|例如测试用的列，就会导致这个问题！！
        # dict_data = df_data.loc[ -3: ,col_list ].T.to_dict() 
        dict_data = df_data.loc[ : ,col_list ].T.to_dict() 
        count = len( dict_data )
        print("Debug-get_data：type of data=",type(dict_data),count  )  
        
        if count != 0:
            # print("count=",count,"dict_data=", dict_data[0] )
            return JsonResponse({'code':0,'msg':'查询成功','count':count,'data':dict_data })
            # return JsonResponse({'code':0,'msg':'查询成功','count':count,'data':json_data })
        else:
            return JsonResponse({'code':0,'msg':'暂无数据','count':count,'data':{}  })
















######################################################################################################
######################################################################################################
@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_weight(request): 
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    # print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    # print("request.GET", type(request.GET.keys() ),request.GET.keys() ) 

    return render(request, 'ciss_exhi/strategy/stra_weight.html', context)

####################################################################### 
### GETDATA 获取策略列表数据 
@csrf_protect
@requires_csrf_token
@csrf_exempt
def get_data(request):
    ### layui的table设置里，默认是 get
    if request.method == 'GET':
        print("get_data | request.GET", type(request.GET.keys() ),request.GET.keys() )
        #########################################################
        ### 
        print("Debug-get_data: Begin of get_data"  )
        #######################################################
        ### 查找的input变量        
        dict_select = {}
        col_list = ["id","date","strategy_CN","code","weight", "pool_name","pool_level","type_asset_indstyle_stock","strategy","name","code_fund","code_stock","note"]
        for temp_col in col_list : 
            if temp_col in request.GET.keys() :
                if len( request.GET[temp_col] ) > 0 :
                    dict_select[temp_col] = request.GET[temp_col]
        print("Debug | dict_select=", dict_select ) 

        #######################################################
        ### 获取表格数据
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
        obj_db["dict_select"] = dict_select
        
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db ) 
        df_data = obj_db["df_data"]  
        
        #############################################
        ### 排序 | notes:这里的数据确实是降序排列的
        df_data = df_data.sort_values(by="date" ,ascending=False )
        print("Debug:End of get_data")
        
        #############################################
        ### notes:df不能直接df.to_json(),这样得到的是 str格式，df_data.to_json()不能赋值给JsonResponse里的data
        ### layui-table可以识别的data数据在views.py里长这样：dict_data= {1: {'id': 1, 'date': '600036','weight': 0.05},  
        # 2: {'id': 2, 'date': '300750', 'weight': 0.08},  
        ### Qs：如果直接 dict(df_data)会报错，TypeError: Object of type Series is not JSON serializable
        ### Ana: 由于json.dumps（）函数引起的。dumps是将dict数据转化为str数据，但是dict数据中包含byte数据所以会报错。

        col_list=["id","date","strategy_CN","code","name","weight","note"]
        dict_data = {}
        for i in df_data.index :
            dict_data[i]={}
            for temp_col in col_list : 
                dict_data[i][temp_col] = df_data.loc[i, temp_col] 

        ### notes: 必须用 df_data.T.to_dict() ,如果 dict( df_data ) 会报错
        ### notes：如果数据库记录里有多列是null空值|例如测试用的列，就会导致这个问题！！
        # dict_data = df_data.loc[ -3: ,col_list ].T.to_dict() 
        dict_data = df_data.loc[ : ,col_list ].T.to_dict() 
        count = len( dict_data )
        print("Debug-get_data：type of data=",type(dict_data),count  )  
        ### dict_data 的排序也是正常的 


        
        ##############################################       
        ### 创建一个非常简单的dict对象
        # dict_data = {}
        # for i in [1,2,3] :
        #     dict_data[i]={}
        #     dict_data[i]["id"] = i
        #     dict_data[i]["date"] = ["600036","300750","601021"][i-1]
        #     dict_data[i]["weight"] = [0.05,0.08,0.06][i-1]
        # count = len( dict_data )
        # print("json data=",dict_data )
        # print("type of data=",type(dict_data)  )


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
        #######################################################
        ### 存入sql数据表 
        ### 必须项
        date = request.POST.get("date","")
        strategy_CN = request.POST.get("strategy_CN","") 
        code = request.POST.get("code","") 
        weight = request.POST.get("weight","") 

        print("Debug: date=",date,"；strategy_CN=",strategy_CN,"；code=",code, "；weight=", weight)
        ### 非必须项
        pool_name = request.POST.get("pool_name","")
        pool_level = request.POST.get("pool_level","") 
        type_asset_indstyle_stock = request.POST.get("type_asset_indstyle_stock","")
        strategy = request.POST.get("strategy","") 
        name = request.POST.get("name","") 
        code_fund = request.POST.get("code_fund","") 
        code_stock = request.POST.get("code_stock","") 
        note = request.POST.get("note","") 

        ### 必须项不能是空的
        if date == '' or strategy_CN == '' or code == '' or weight== ''  : 
            return JsonResponse({'code': 10021, 'msg': '参数错误'}, json_dumps_params={'ensure_ascii': False})
        else :
            ###############################################
            ### 多个input必须至少有一项非空
            obj_db = {} 
            obj_db["db_name"] = "db_funda.sqlite3"
            obj_db["table_name"] = "fundpool_stockpool_weight"
            obj_db["insert_type"] = "1r"
            obj_db["dict_1r"] = {}
            if len( date ) > 0  :  
                obj_db["dict_1r"][ "date"] = date
            if len( strategy_CN ) > 0  :  
                obj_db["dict_1r"][ "strategy_CN"] = strategy_CN
            if len( code ) > 0  :  
                obj_db["dict_1r"][ "code"] = code
            if len( weight ) > 0  :  
                obj_db["dict_1r"][ "weight"] = weight   
            ### 非必须项
            if len( pool_name ) > 0  :  
                obj_db["dict_1r"][ "pool_name"] = pool_name
            if len( pool_level) > 0  :  
                obj_db["dict_1r"][ "pool_level"] = pool_level
            if len( type_asset_indstyle_stock) > 0  :  
                obj_db["dict_1r"][ "type_asset_indstyle_stock"] = type_asset_indstyle_stock
            if len( strategy ) > 0  :  
                obj_db["dict_1r"][ "strategy"] = strategy
            if len( name ) > 0  :  
                obj_db["dict_1r"][ "name"] = name         
            if len( code_fund ) > 0  :  
                obj_db["dict_1r"][ "code_fund"] = code_fund
            if len( code_stock ) > 0  :  
                obj_db["dict_1r"][ "code_stock"] = code_stock
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
        strategy_CN = request.POST.get('strategy_CN', "")
        code = request.POST.get('code', "")
        weight = request.POST.get('weight', "")
        name = request.POST.get('name', "")
        note = request.POST.get('note', "")
        ### 
        ###############################################
        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
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
        obj_db["table_name"] = "fundpool_stockpool_weight" 
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




