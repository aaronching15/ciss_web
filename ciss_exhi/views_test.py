# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
功能：测试layui的表格功能


last  | since 20221014
refernce:derived from views.py
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
### TEST
### working on test strategy file  
import pandas as pd 


######################################################################################################
@csrf_protect
@requires_csrf_token
@csrf_exempt
def test(request): 
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    # print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    # print("request.GET", type(request.GET.keys() ),request.GET.keys() ) 

    return render(request, 'ciss_exhi/fund_fof/test.html', context)

######################################################################################################
# GETDATA 获取table数据 
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
        ### 
        df_data = df_data.sort_values(by="id" ,ascending=False )
        # print("Debug:End of get_data")
        
        #############################################
        ### 数据格式转换：df转换成layui可识别的dict
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
    ###  
    print("add_data | request.GET", type(request.GET.keys() ),request.GET.keys() )
    print("add_data | request.POST", type(request.POST.keys() ),request.POST.keys() ) 
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
        ### 计算综合得分
        if len(score_performance) > 0 : 
            if not float(score_performance) > 1 :
                ###
                try :
                    temp = float(s_down_market) +float(s_flat_market) + float(s_up_market)
                    score_performance = str( round( temp,2 ) )
                except :
                    pass
        else :
            score_performance = "0.0"
        # print("Debug: date=",date, )
        ### 非必须项 ["note","date_lastmodify","if_fundmanager_fault"]
        note = request.POST.get("note","")
        date_lastmodify = request.POST.get("date_lastmodify","") 
        if_fundmanager_fault = request.POST.get("if_fundmanager_fault","") 
        
        print("Debug: add data=",date,";code=",code, ";name=",code,"abstract_analysis=", abstract_analysis)
        ##################################################################################
        ### 必须项不能是空的
        if date == '' or code == '' : 
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
        date= request.POST.get('date',"")
        name= request.POST.get('name',"")
        code= request.POST.get('code',"")
        style_fund= request.POST.get('style_fund',"")
        theme_fund= request.POST.get('theme_fund',"")
        ind_1= request.POST.get('ind_1',"")
        ind_2= request.POST.get('ind_2',"")
        ind_3= request.POST.get('ind_3',"")
        ind_num= request.POST.get('ind_num',"")#
        score_performance= request.POST.get('score_performance',"")
        s_down_market= request.POST.get('s_down_market',"")
        s_flat_market= request.POST.get('s_flat_market',"")
        s_up_market= request.POST.get('s_up_market',"")
        abstract_analysis= request.POST.get('abstract_analysis',"")
        fund_manager= request.POST.get('fund_manager',"")#
        # 非必须项
        note= request.POST.get('note',"")
        date_lastmodify= request.POST.get('date_lastmodify',"")
        if_fundmanager_fault= request.POST.get('if_fundmanager_fault',"") 

        ### 
        ###############################################
        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "ciss_exhi"+ "_" + "fund_analysis" 
        obj_db["insert_type"] = "1r"
        obj_db["dict_1r"] = {}
        ## id是必备项
        obj_db["dict_1r"]["id"]  = id 
        if len( date ) > 0  :  
            obj_db["dict_1r"][ "date"] = date
                    
        if len( name ) > 0  :  
            obj_db["dict_1r"]["name"] = name
        if len( code ) > 0  :  
            obj_db["dict_1r"][ "code"] = code
        if len( style_fund ) > 0  :  
            obj_db["dict_1r"]["style_fund"] =style_fund
        if len( theme_fund ) > 0  :  
            obj_db["dict_1r"]["theme_fund"]  = theme_fund
        if len( ind_1 ) > 0  :  
            obj_db["dict_1r"]["ind_1"] =ind_1
        if len( ind_2 ) > 0  :  
            obj_db["dict_1r"]["ind_2"] =ind_2
        if len( ind_3 ) > 0  :  
            obj_db["dict_1r"]["ind_3"] =ind_3
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
        if len( note ) > 0  :  
            obj_db["dict_1r"][ "note"] = note 
        if len( date_lastmodify ) > 0  :  
            obj_db["dict_1r"][ "date_lastmodify"] = date_lastmodify
        if len( if_fundmanager_fault ) > 0  :  
            obj_db["dict_1r"][ "if_fundmanager_fault"] = if_fundmanager_fault 
        # print("Debug=dict_1r=", obj_db["dict_1r"] )
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





