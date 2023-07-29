# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
todo:

功能：股债配置类策略

last  | since 20221206
refernce:derived from views_strategy_dev.py
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

#################################################################################
### 策略研究；Index of strategy list |derived from views.py
### working on test strategy file 
from .models import fund_analysis
import pandas as pd 

@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_allocation(request): 
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    print("request.GET", type(request.GET.keys() ),request.GET.keys() )
    #################################################################################
    ### 导入常用地址信息
    import os
    path_pms =  os.getcwd().split("ciss_web")[0]+"\\data_pms\\"  
    # path_wind_terminal = path_pms + "wind_terminal\\" 
    # path_wss = path_pms + "wss\\"
    # path_wpf = path_pms + "wpf\\"
    # path_wpd = path_pms + "wpd\\"
    # path_wsd = path_pms + "wsd\\"
    path_ciss_web = os.getcwd().split("ciss_web")[0]+"ciss_web\\"
    path_ciss_rc = path_ciss_web +"CISS_rc\\"
    # path_db = path_ciss_rc + "db\\"
    path_dt = path_ciss_rc + "db\\db_times\\"
    sys.path.append(path_ciss_rc+ "config\\")
    from config_data import config_db
    config_db_1 = config_db()
    path_db = path_ciss_web

    #################################################################################
    ### 导入基准指数的收盘价和周收益率数据
    file_name = "stra_allocation.xlsx"
    path_stra_allocation = path_pms + "data_strategy\\stra_allocation\\"
    df_week = pd.read_excel(path_stra_allocation + file_name, sheet_name="week" ) 
    ### 按日期升序排列 
    df_week = df_week.sort_values("date")
    
    col_list = df_week.columns  
    asset_list = [ i[4:] for i in col_list if i[:4]=="ret_" ]
    print("asset_list:", asset_list )

    ####################################################################################
    # ### 
    if "input_stra_allo_check_data" in request.POST.keys():    
        obj_a ={}        
        asset_list_input = request.POST.getlist("asset_list_stra_allo_check_data","") 
        if len( asset_list_input ) < 1 :
            asset_list_input= asset_list
        print("Debug; asset_list_input =" , asset_list_input  )

        ##########################################
        ### 对于选中的数据，获取期初和期末日期，计算区间内的累计收益率、平均收益率、波动率、最大回撤等。
        col_list_ret =  [ "ret_"+ i for i in asset_list_input  ]
        
        # transpose
        df_ret_describe = df_week.loc[ :, col_list_ret ].describe().T  
        df_ret_describe = df_ret_describe.rename(columns={"25%":"25pct","50%":"50pct","75%":"75pct"} )
        temp_col = "count"
        df_ret_describe[temp_col] = df_ret_describe[temp_col].round(decimals=2)  
        for temp_col in ["mean","min","max","25pct","50pct","75pct"] :
            df_ret_describe[temp_col] = df_ret_describe[temp_col]*100
            df_ret_describe[temp_col] = df_ret_describe[temp_col].round(decimals=2)  
        print("df_ret_describe \n", df_ret_describe )
        ##########################################
        ### 给定区间数据，依次计算区间净值、平均收益率、波动率、最大回测等指标。
        ### 计算历史净值 
        for asset in asset_list_input :
            df_week["unit_"+ asset ] = df_week["close_"+ asset ]/ df_week["close_"+ asset ].values[0] 
            ### 保留4位小数点
            df_week["unit_"+ asset ] = df_week["unit_"+ asset ].round(decimals=4)
        
        col_list_unit =  [ "unit_"+ i for i in asset_list_input  ]
        df_unit = df_week.loc[:,["date"]+col_list_unit ]
        print("df_unit \n" , df_unit )
        ##########################################
        ### save to context  
        ### notes: describe 不转置
        context["check_data_df_describe"]  = df_ret_describe.T 
        context["check_data_df_week"]  = df_unit.tail().T 
        

        print( "tail \n" ,   )
    
    # 














    ####################################################################################
    ### save to context 
    context["asset_list"] = asset_list
    
    
    ### NOTES: 
    return render(request, 'ciss_exhi/strategy/index_stra_allocation.html', context)














































#################################################################################
### BEFORE ### BEFORE ### BEFORE ### BEFORE ### BEFORE ### BEFORE 

#################################################################################
### notes ；stra_index2()和 stra_index2.html是临时测试layui功能用的。
@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_index2(request): 
    ### stra_index2 是测试脚本， 
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    print("request.GET", type(request.GET.keys() ),request.GET.keys() )
    #################################################################################
    ### 导入常用地址信息
    
    # import os
    # path_pms =  os.getcwd().split("ciss_web")[0]+"\\data_pms\\"  
    # path_wind_terminal = path_pms + "wind_terminal\\" 
    # path_wss = path_pms + "wss\\"
    # path_wpf = path_pms + "wpf\\"
    # path_wpd = path_pms + "wpd\\"
    # path_wsd = path_pms + "wsd\\"
    # path_ciss_web = os.getcwd().split("ciss_web")[0]+"ciss_web\\"
    # path_ciss_rc = path_ciss_web +"CISS_rc\\"
    # # path_db = path_ciss_rc + "db\\"
    # path_dt = path_ciss_rc + "db\\db_times\\"
    # sys.path.append(path_ciss_rc+ "config\\")
    # from config_data import config_db
    # config_db_1 = config_db()
    # path_db = path_ciss_web

    # #################################################################################
    # ###
    # print("Debug: Begin of stra_index")
    # #######################################################
    # ### 查找的input变量        
    # date = request.POST.get("date_to_sql","")
    # # pool_name = request.POST.get("pool_name_to_sql","")
    # # pool_level = request.POST.get("pool_level_to_sql","") 
    # # type_asset_indstyle_stock = request.POST.get("type_asset_indstyle_stock_to_sql","")
    # strategy = request.POST.get("strategy_to_sql","") 
    # ### strategy_CN = "股票行业研究"
    # strategy_CN = request.POST.get("strategy_CN_to_sql","") 
    # code = request.POST.get("code_to_sql","") 
    # name = request.POST.get("name_to_sql","") 
    # weight = request.POST.get("weight_to_sql","") 
    # # code_fund = request.POST.get("code_fund_to_sql","") 
    # # code_stock = request.POST.get("code_stock_to_sql","") 
    # # note = request.POST.get("note_to_sql","") 

    # #######################################################
    # ### 获取表格数据
    # obj_db = {} 
    # obj_db["db_name"] = "db_funda.sqlite3"
    # obj_db["table_name"] = "fundpool_stockpool_weight"
    # obj_db["dict_select"] = {}
    # if len( strategy_CN ) > 0  :  
    #     obj_db["dict_select"][ "strategy_CN"] = strategy_CN

    # from database import db_sqlite
    # db_sqlite1 = db_sqlite()
    # obj_db = db_sqlite1.select_table_data( obj_db ) 
    # df_data = obj_db["df_data"]  

    # #############################################
    # ### 
    # context = {}
    # context["df_data"] = df_data.T  

    

    
    ###################################################
    ### NOTES:Qs:会报错： no such table: ciss_exhi_strategy; 
    return render(request, 'ciss_exhi/strategy/stra_index2.html', context)

######################################################################################################
# 获取策略列表数据 
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
        # date = request.POST.get("date_to_sql","")
        # # pool_name = request.POST.get("pool_name_to_sql","")
        # # pool_level = request.POST.get("pool_level_to_sql","") 
        # # type_asset_indstyle_stock = request.POST.get("type_asset_indstyle_stock_to_sql","")
        # strategy = request.POST.get("strategy_to_sql","") 
        # ### strategy_CN = "股票行业研究"
        # strategy_CN = request.POST.get("strategy_CN_to_sql","") 
        # code = request.POST.get("code_to_sql","") 
        # name = request.POST.get("name_to_sql","") 
        # weight = request.POST.get("weight_to_sql","") 
        # # code_fund = request.POST.get("code_fund_to_sql","") 
        # # code_stock = request.POST.get("code_stock_to_sql","") 
        # # note = request.POST.get("note_to_sql","") 

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
        df_data = df_data.sort_values(by="id" ,ascending=False )
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




