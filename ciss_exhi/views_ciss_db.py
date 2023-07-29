# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
todo:
功能：ciss_db对应db_funda数据库地数据读写

last  | since 20220823
refernce:derived from views_pms_manage.py
===============================================
''' 

###########################################################################
### Initialization
from operator import index
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
### notes: def fund_analysis 20221027开始，迁移至 views_fund_analysis.py
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
        dict_1r["fund_namager"] = request.POST.get("fund_namager","")
        dict_1r["note"] = request.POST.get("note","")
        dict_1r["date_lastmodify"] = request.POST.get("date_lastmodify","")
        dict_1r["if_fundmanager_fault"] = request.POST.get("if_fundmanager_fault","0")


        ################################################################################
        ### 判断是否要匹配基金信息、或提取Wind-API指标
        if len( dict_1r["fund_namager"] ) <= 1 :
            obj_f={}
            obj_f["fund_code"] = dict_1r["code"] 
            obj_f["date"] = dict_1r["date_lastmodify"] 
            obj_f["col_list"] = ["fund_fundmanageroftradedate",]
            from get_wind_api import wind_api
            wind_api1 = wind_api()
            obj_f = wind_api1.get_wss_fund_1date( obj_f )

            ### obj_f["list_data"] = obj_w.Data[0] = ['张坤']
            dict_1r["fund_namager"] = obj_f["list_data"][0]

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
    # return render(request,"ciss_exhi/quick.html")




###########################################################################
@csrf_protect
@requires_csrf_token
@csrf_exempt
def event_view(request):
    import pandas as pd 
    ###########################################################################
    ### 基本面事件和观点|Fundamental Events and View
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    
    #################################################################################
    ### 导入常用地址信息
    import os
    path_pms =  os.getcwd().split("ciss_web")[0]+"\\data_pms\\"  
    path_data_adj = path_pms + "data_adj\\"
    path_wind_terminal = path_pms + "wind_terminal\\" 
    path_wss = path_pms + "wss\\"
    path_wpf = path_pms + "wpf\\"
    path_wpd = path_pms + "wpd\\"
    path_wsd = path_pms + "wsd\\"
    path_ciss_web = os.getcwd().split("ciss_web")[0]+"ciss_web\\"
    path_ciss_exhi = path_ciss_web + "ciss_exhi\\"
    path_ciss_rc = path_ciss_web +"CISS_rc\\"
    # path_db = path_ciss_rc + "db\\"
    path_dt = path_ciss_rc + "db\\db_times\\"
    sys.path.append(path_ciss_rc+ "config\\")
    from config_data import config_db
    config_db_1 = config_db()
    path_db = path_ciss_web

    ###########################################################################
    ### 汇总 SUMMARY
    ###########################################################################
    ### 汇总-大类资产最新配置观点
    if "input_event_summary_multiasset" in request.POST.keys():   
        ###########################################################################
        ### 提取近1年大类资产观点数据
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_multiasset_macro"
        ### 匹配日期区间,选取最近1年的日期区间
        obj_db["col_name_date"] = "date"
        import datetime as dt  
        time_now = dt.datetime.now()  
        import dateutil.relativedelta as rd   
        obj_db["date_begin"] = dt.datetime.strftime(time_now - rd.relativedelta(years=1) , "%Y%m%d")
        obj_db["date_end"] = dt.datetime.strftime(time_now   , "%Y%m%d") 

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db )
        ### 按日期排序
        df_data = obj_db["df_data"]
        df_data = df_data.sort_values(by="date",ascending=False ) 
        
        count = 0 
        ################################################################################
        ### Part 1 
        ### 模糊匹配之一的字符串：df[ df['通信名称'].str.contains('联通|移动|小灵通|电信')]
        keyword_list = ["策略","A股","港股","基金产品","债券","美股","宏观"]
        for temp_key in keyword_list :
            df_sub = df_data[ df_data["asset_market_topic"].str.contains( temp_key )  ]
            df_sub = df_sub[ df_sub["type_event_view"].str.contains("观点")  ]
            if len( df_sub.index ) > 0 :
                ### 只选择最新的2条记录
                if count == 0 :
                    df_output = df_sub.iloc[:2 , :]
                    count = 1
                else :
                    df_output = df_output.append( df_sub.iloc[:2 , :] ) 

        ################################################################################
        ###         
        context["df_event_summary_multiasset"] =df_output.T 

    ###########################################################################
    ### 汇总-股票行业和主题最新配置观点——行业
    if "input_event_summary_ind_style" in request.POST.keys():   
        ###########################################################################
        ### 提取近1年行业和主题最新配置观点
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_ind_style_market" 
        ### 匹配日期区间,选取最近1年的日期区间
        obj_db["col_name_date"] = "date"
        import datetime as dt  
        time_now = dt.datetime.now()  
        import dateutil.relativedelta as rd   
        obj_db["date_begin"] = dt.datetime.strftime(time_now - rd.relativedelta(years=1) , "%Y%m%d")
        obj_db["date_end"] = dt.datetime.strftime(time_now , "%Y%m%d") 

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db )
        ### 按日期排序 
        df_data = obj_db["df_data"]
        df_data = df_data.sort_values(by="date",ascending=False ) 

        ################################################################################
        ### Part 1 :中信三级行业,中信一级行业
        ### 模糊匹配之一的字符串：df[ df['通信名称'].str.contains('联通|移动|小灵通|电信')]
        # 中信一级行业取全部值：电力设备及新能源","电子","基础化工","机械","医药","有色金属","汽车","计算机","电力及公用事业","
        # 食品饮料","国防军工","通信","非银行金融","房地产","传媒","交通运输","煤炭","农林牧渔","建筑
        # ","建材","家电","石油石化","银行","钢铁","综合","轻工制造","商贸零售","消费者服务","纺织服装","综合金融
        # notes：中信三级行业取近期成交额前30的：
        #########################################################
        ### 导入行业数据 | 为了避免提取API价量行情，直接用最新的导出的A股数据
        import os
        dir_list = os.listdir( path_wind_terminal )
        ### 日期升序排列
        dir_list = sorted(dir_list,key=lambda x: os.path.getmtime(os.path.join(path_wind_terminal, x)))
        ### 全部A股_20220926.xlsx ；path_pms=C:\rc_202X\rc_202X\data_pms\ 
        file_list = [ i for i in dir_list if "全部A股_" in i ]
        ### 取最后一个
        temp_file = file_list[-1]
        print("Latest A shares file=", file_list )
        import pandas as pd 
        df_ashares =pd.read_excel( path_wind_terminal + temp_file  )
        
        ### 按成交金额、基金持股比例 排序选取前30名
        df_ind3_amt = df_ashares.loc[:,["成交额","中信三级行业"] ].groupby("中信三级行业").sum()
        df_ind3 = df_ashares.loc[:,["基金持股比例","中信三级行业"] ].groupby("中信三级行业").mean()
        df_ind3 = df_ind3.merge( df_ind3_amt, left_index=True, right_index=True)
        df_ind3["sum"] = df_ind3["成交额"]/df_ind3["成交额"].sum() + df_ind3["基金持股比例"]/df_ind3["基金持股比例"].sum()         
        df_ind3 = df_ind3.sort_values(by="sum",ascending=False )
        para_num_ind3 = 40
        list_ind3_top30 = list( df_ind3.index)[ : para_num_ind3 ]
        print("df_ind3 \n" , df_ind3.head().T ) 

        ### 中信一级行业 ，要去掉 nan
        list_ind1 = list( df_ashares["中信一级行业"].drop_duplicates() )
        import numpy as np
        list_ind1 = [i for i in list_ind1 if i not in ["nan",np.nan ] ]
        print("list_ind1=", list_ind1 ) 
        ################################################################################
        ### Part 2 :查询近1年记录        
        count = 0 
        list_ind1_no_record = []
        for temp_ind1 in list_ind1 :
            print( "temp_ind1=",temp_ind1)
            ### notes: 有可能部分一级行业没有跟踪
            df_sub = df_data[ df_data["name_index"].str.contains( temp_ind1 )  ] 
            if len( df_sub.index ) > 0 :
                ### 只选择最新的2条记录
                if count == 0 :
                    df_output = df_sub.iloc[:2 , :]
                    count = 1
                else :
                    df_output = df_output.append( df_sub.iloc[:2 , :] ) 
            else :
                ### 没有记录的要保存
                list_ind1_no_record = list_ind1_no_record + [temp_ind1]
        
        ###
        list_ind3_no_record = []
        for temp_ind3 in list_ind3_top30 :
            df_sub = df_data[ df_data["name_index"].str.contains( temp_ind3 )  ] 
            if len( df_sub.index ) > 0 :
                ### 只选择最新的2条记录
                if count == 0 :
                    df_output = df_sub.iloc[:2 , :]
                    count = 1
                else :
                    df_output2 = df_output.append( df_sub.iloc[:2 , :] ) 
            else :
                ### 没有记录的要保存
                list_ind3_no_record = list_ind3_no_record + [temp_ind3]
        ################################################################################
        ### OUTPUT        
        context["list_ind1_no_record"] =list_ind1_no_record
        context["list_ind3_no_record"] =list_ind3_no_record
        context["df_event_summary_ind_style"] =df_output.T 



    ###########################################################################
    ### 汇总-股票风格、主题和指数最新观点 
    ### 1,四类市值风格的量化指标统计：超大市值、大市值、中市值、小市值、小微市值
    # 2，行业板块的统计；上游制造、科技、消费、医疗、金融地产；风格：成长、价值；主题板块【新兴消费、新能源车等】 
    # 3，市场指数：主要市场和风格指数的观点： 
    if "input_event_summary_style_market" in request.POST.keys():   
        import pandas as pd 
        ###########################################################################
        ### 导入A股量化指标表
        df_ashares = pd.read_excel( path_data_adj + "a_shares.xlsx" )
        ### 20日涨跌幅	60日涨跌幅	120日涨跌幅	年初至今	流通市值	总市值1	
        # 市盈率(TTM)	基金持股比例	净资产收益率(TTM)	归母净利润同比增长率	
        # 中信一级行业	中信二级行业	中信三级行业	申万一级行业	
        # ma_short	ma_short_pre	pre_close	ma_mid	m_ave_amt	avg_MV_per	m_ave_mv

        ################################################################################
        ################################################################################
        ### 1，市值风格
        # "m_ave_mv",亿为单位 : 100,500,2000,5000 ; | "总市值1" 元为单位； 
        df_mv = pd.DataFrame(  index=["超大市值","大市值","中市值","小市值","小微市值" ] )
        df_mv["mv_lb"] = [5000,2000,500,100,0]
        df_mv["mv_ub"] = [500000,5000,2000,500,100] 
        ### 市值占比、成交额占比
        num_stock = df_ashares["m_ave_mv"].count()
        mv_sum  = df_ashares["m_ave_mv"].sum()
        amt_sum = df_ashares["m_ave_amt"].sum() 
        ### 
        for temp_i in df_mv.index :
            mv_lb = df_mv.loc[temp_i, "mv_lb"]
            mv_ub = df_mv.loc[temp_i, "mv_ub"]
            df_sub = df_ashares[ df_ashares["m_ave_mv"] <= mv_ub ]
            df_sub = df_sub[ df_sub["m_ave_mv"] > mv_lb ]
            ### 
            df_mv.loc[temp_i, "ave_mv_pct"] = df_sub["m_ave_mv"].sum() / mv_sum *100
            df_mv.loc[temp_i, "ave_amt_pct"] = df_sub["m_ave_amt"].sum() / amt_sum *100
            df_mv.loc[temp_i, "ave_mv"] = df_sub["m_ave_mv"].sum() 
            df_mv.loc[temp_i, "ave_amt"] = df_sub["m_ave_amt"].sum()
            df_mv.loc[temp_i, "num_stock"] = df_sub["m_ave_amt"].count() 
            ########################################################
            ### 计算市值加权平均值
            ### 20日涨跌幅	60日涨跌幅	120日涨跌幅.......
            for temp_col in ["20日涨跌幅","60日涨跌幅","120日涨跌幅" ] :
                df_sub["temp"] = df_sub["m_ave_mv"] * df_sub[ temp_col ]
                df_mv.loc[temp_i, temp_col]  = df_sub["temp"].sum() /df_sub["m_ave_mv"].sum() *100
            for temp_col in ["基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)"] :
                df_sub["temp"] = df_sub["m_ave_mv"] * df_sub[ temp_col ]
                df_mv.loc[temp_i, temp_col]  = df_sub["temp"].sum() /df_sub["m_ave_mv"].sum()  
        
        ########################################################
        ### 总数
        df_mv.loc["合计", "ave_mv_pct"] =  100.0
        df_mv.loc["合计", "ave_amt_pct"] = 100.0
        df_mv.loc["合计", "ave_mv"]  = mv_sum
        df_mv.loc["合计", "ave_amt"] = amt_sum
        df_mv.loc["合计", "num_stock"] = num_stock
        for temp_col in ["20日涨跌幅","60日涨跌幅","120日涨跌幅","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)"] :
            df_ashares["temp"] = df_ashares["m_ave_mv"] * df_ashares[ temp_col ]
            df_mv.loc["合计", temp_col]  = df_ashares["temp"].sum() /df_ashares["m_ave_mv"].sum()
        
        ########################################################
        ### 取小数点 ： .round(decimals=2)  
        for temp_col in ["ave_mv_pct","ave_amt_pct","ave_mv","ave_amt"  ] :
            df_mv[ temp_col ] = df_mv[ temp_col ].round(2)

        for temp_col in ["20日涨跌幅","60日涨跌幅","120日涨跌幅","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)" ] :
            df_mv[ temp_col ] = df_mv[ temp_col ].round(2)

        ########################################################
        ### 为了Django展示改名
        dict_col = {}
        dict_col["净资产收益率(TTM)"] = "净资产收益率ttm"
        dict_col["市盈率(TTM)"] = "市盈率ttm"
        df_mv = df_mv.rename( columns= dict_col )

        ################################################################################
        ################################################################################
        ### 2，行业板块风格
        ### 用ind1、ind3匹配成长、价值；上游制造,科技,消费,医疗,金融地产
        ########################################################
        ### 导入匹配数据
        file_name="db_manage.xlsx"
        temp_sheet = "MATCH"
        df_match = pd.read_excel(path_ciss_exhi+ file_name,sheet_name= temp_sheet )
        df_sub = df_match[ df_match["type_ind_style"] =="ind1"]
        df_ashares["ind_sector"] =""
        df_ashares["growth_value"] =""
        for temp_i in df_sub.index :
            temp_ind1 = df_sub.loc[ temp_i, "name_index" ]
            temp_ind_sector = df_sub.loc[ temp_i, "ind_sector"]
            temp_growth_value = df_sub.loc[ temp_i, "growth_value"]
            df_ashares["ind_sector"]  = df_ashares.apply(lambda x : temp_ind_sector   if x["中信一级行业"]== temp_ind1 else x["ind_sector"], axis=1  )
            df_ashares["growth_value"]= df_ashares.apply(lambda x : temp_growth_value if x["中信一级行业"]== temp_ind1 else x["growth_value"], axis=1 )

        df_ashares.to_excel("D:\\df_ashares.xlsx")
        ########################################################
        ### 新建板块和风格的df
        list_ind_sector = list( df_sub["ind_sector"].drop_duplicates() )
        list_growth_value  = list( df_sub["growth_value"].drop_duplicates() )
        df_style = pd.DataFrame(  index= list_ind_sector+list_growth_value  )   
        
        for temp_i in list_growth_value : 
            df_sub = df_ashares[ df_ashares["growth_value"] == temp_i ] 
            ### 
            df_style.loc[temp_i, "ave_mv_pct"] = df_sub["m_ave_mv"].sum() / mv_sum *100
            df_style.loc[temp_i, "ave_amt_pct"] = df_sub["m_ave_amt"].sum() / amt_sum *100
            df_style.loc[temp_i, "ave_mv"] = df_sub["m_ave_mv"].sum() 
            df_style.loc[temp_i, "ave_amt"] = df_sub["m_ave_amt"].sum()
            df_style.loc[temp_i, "num_stock"] = df_sub["m_ave_amt"].count() 
            ########################################################
            ### 计算市值加权平均值
            ### 20日涨跌幅	60日涨跌幅	120日涨跌幅.......
            for temp_col in ["20日涨跌幅","60日涨跌幅","120日涨跌幅" ] :
                df_sub["temp"] = df_sub["m_ave_mv"] * df_sub[ temp_col ]
                df_style.loc[temp_i, temp_col]  = df_sub["temp"].sum() /df_sub["m_ave_mv"].sum() *100
            for temp_col in ["基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)"] :
                df_sub["temp"] = df_sub["m_ave_mv"] * df_sub[ temp_col ]
                df_style.loc[temp_i, temp_col]  = df_sub["temp"].sum() /df_sub["m_ave_mv"].sum()     
        ########################################################
        for temp_i in list_ind_sector : 
            df_sub = df_ashares[ df_ashares["ind_sector"] == temp_i ] 
            ### 
            df_style.loc[temp_i, "ave_mv_pct"] = df_sub["m_ave_mv"].sum() / mv_sum *100
            df_style.loc[temp_i, "ave_amt_pct"] = df_sub["m_ave_amt"].sum() / amt_sum *100
            df_style.loc[temp_i, "ave_mv"] = df_sub["m_ave_mv"].sum() 
            df_style.loc[temp_i, "ave_amt"] = df_sub["m_ave_amt"].sum()
            df_style.loc[temp_i, "num_stock"] = df_sub["m_ave_amt"].count() 
            ########################################################
            ### 计算市值加权平均值
            ### 20日涨跌幅	60日涨跌幅	120日涨跌幅.......
            for temp_col in ["20日涨跌幅","60日涨跌幅","120日涨跌幅" ] :
                df_sub["temp"] = df_sub["m_ave_mv"] * df_sub[ temp_col ]
                df_style.loc[temp_i, temp_col]  = df_sub["temp"].sum() /df_sub["m_ave_mv"].sum() *100
            for temp_col in ["基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)"] :
                df_sub["temp"] = df_sub["m_ave_mv"] * df_sub[ temp_col ]
                df_style.loc[temp_i, temp_col]  = df_sub["temp"].sum() /df_sub["m_ave_mv"].sum()  


        ########################################################
        ### 取小数点 ： .round(decimals=2)  
        for temp_col in ["ave_mv_pct","ave_amt_pct","ave_mv","ave_amt"  ] :
            df_style[ temp_col ] = df_style[ temp_col ].round(2)

        for temp_col in ["20日涨跌幅","60日涨跌幅","120日涨跌幅","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","市盈率(TTM)" ] :
            df_style[ temp_col ] = df_style[ temp_col ].round(2)

        ########################################################
        ### 为了Django展示改名
        dict_col = {}
        dict_col["净资产收益率(TTM)"] = "净资产收益率ttm"
        dict_col["市盈率(TTM)"] = "市盈率ttm"
        df_style = df_style.rename( columns= dict_col )
    
        
        ################################################################################
        ### OUTPUT        
        df_mv["name"] = df_mv.index 
        print("Debug===df_mv= \n" , df_mv )
        context["df_mv_style_market"] =df_mv.T 
        ### 
        df_style["name"] = df_style.index 
        print("Debug===df_style= \n" , df_mv )
        context["df_style_style_market"] =df_style.T 


    ###########################################################################
    ### 汇总-个股股票最新配置观点汇总
    if "input_event_summary_stock_funda" in request.POST.keys():   
        ###########################################################################
        ### 提取近1年行业和主题最新配置观点
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_stock_funda" 
        ### 匹配日期区间,选取最近1年的日期区间
        obj_db["col_name_date"] = "date"
        import datetime as dt  
        time_now = dt.datetime.now()  
        import dateutil.relativedelta as rd   
        obj_db["date_begin"] = dt.datetime.strftime(time_now - rd.relativedelta(years=1) , "%Y%m%d")
        obj_db["date_end"] = dt.datetime.strftime(time_now , "%Y%m%d") 

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db )
        ### 按日期排序 
        df_data = obj_db["df_data"]
        df_data = df_data.sort_values(by="date",ascending=False ) 
        
        context["df_event_summary_stock_funda"] =df_data.T

        #############################################
        ### TODO: 提取成交金额top50、基金持股比例*流通市值的top50，计算近1年是否有跟踪记录


        #############################################
        ### TODO: 统计中信一级行业对应个股的研究数量。





    ###########################################################################
    ### 查询
    ###########################################################################
    ### 查询数据库中大类资产、投资观点事件 | 至少输入一项，否则返回最新的5条记录
    if "input_event_select" in request.POST.keys():   
        
        asset_market_topic= request.POST.get("asset_market_topic","") 
        type_event_view= request.POST.get("type_event_view","") 
        keyword= request.POST.get("keyword","") 
        abstract= request.POST.get("abstract","") 
        ###############################################
        ### 4个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_multiasset_macro"
        obj_db["dict_select"] = {}
        if len( asset_market_topic) > 0  :  
            ### 如果是 "all",就不需要限制
            if not asset_market_topic == "all" :
                obj_db["dict_select"][ "asset_market_topic"] = asset_market_topic
        if len( type_event_view) > 0  :  
            obj_db["dict_select"][ "type_event_view"] = type_event_view
        if len( keyword) > 0  :  
            obj_db["dict_select"][ "keyword"] = keyword
        if len( abstract) > 0  :  
            obj_db["dict_select"][ "abstract"] = abstract 
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db )

        ################################################################################
        ### 返回数据库中的最新N条记录  
        df_data = obj_db["df_data"].sort_values(by="date",ascending=False ) 
        
        context["df_event_view_multiasset_macro"] =df_data.T 


    ###########################################################################
    ### 提交到数据库:新增大类资产、投资观点事件
    if "input_event_to_sql" in request.POST.keys():    

        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_multiasset_macro"
        ### insert_type == "1r" 导入一行
        obj_db["insert_type"] = "1r"
        obj_db["dict_1r"] = {} 
        obj_db["dict_1r"]["date" ] = request.POST.get("date_submit","") 
        obj_db["dict_1r"]["asset_market_topic" ] = request.POST.get("asset_market_topic_submit","") 
        obj_db["dict_1r"]["type_event_view" ] = request.POST.get("type_event_view_submit","") 
        obj_db["dict_1r"]["keyword" ] =  request.POST.get("keyword_submit","") 
        obj_db["dict_1r"]["abstract" ] = request.POST.get("abstract_submit","")  
        ### 非必须项目：
        weight = request.POST.get("weight_submit","")  
        if len( weight ) > 1 :
            obj_db["dict_1r"]["weight" ] = weight 
        source = request.POST.get("source_submit","")  
        if len( source ) > 1 :
            obj_db["dict_1r"]["source" ] = request.POST.get("source_submit","")  

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.insert_table_data(obj_db) 

        context["df_event_view_multiasset_macro"] = obj_db["df_data"].T

    ###########################################################################
    ### 3,删除事件、观点和宏观研究记录：
    if "input_event_delete" in request.POST.keys():    
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_multiasset_macro"
        
        del_id = request.POST.get("del_id","")  
        del_col_name = request.POST.get("del_col_name","")  
        del_col_value = request.POST.get("del_col_value","") 
        if len(del_id) > 0 : 
            ### 按id删除row | obj_db["delete_type"] = "1r"
            obj_db["col_name"] = "id"
            obj_db["select_value"] = str(del_id )

        elif len(del_col_name) > 0  :
            ### 根据选择的列及列值删除
            obj_db["col_name"] = del_col_name
            obj_db["select_value"] = del_col_value
        else :
            print("No eligible criteria to delete rows.")

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.delete_table(obj_db) 
        ### 删除成功后 obj_db["df_data"] 是空的
        # df_data = obj_db["df_data"].sort_values(by="id",ascending=False )
        # df_data = df_data.head(10)

        context["df_event_view_multiasset_macro"] = df_data.T

    ###########################################################################
    ### 市场、行业、主题、因子风格
    ###########################################################################
    ### 1，查询市场、行业、主题、因子 ,根据给定column列名称和数值，提交数据库
    if "input_event_select_indstyle" in request.POST.keys():   

        name_index = request.POST.get("name_index_indstyle","") 
        code_index = request.POST.get("code_index_indstyle","")
        type_ind_style = request.POST.get("type_ind_style_indstyle","")
         
        keyword= request.POST.get("keyword_indstyle","") 
        abstract= request.POST.get("abstract_indstyle","") 
        ###############################################
        ### 4个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_ind_style_market"
        obj_db["dict_select"] = {}
        if len( name_index) > 0  :  
            obj_db["dict_select"][ "name_index"] = name_index 
        if len( code_index) > 0  :  
            obj_db["dict_select"][ "code_index"] = code_index
        if len( type_ind_style) > 0  :  
            obj_db["dict_select"][ "type_ind_style"] = type_ind_style
        if len( keyword) > 0  :  
            obj_db["dict_select"][ "keyword"] = keyword
        if len( abstract ) > 0  :  
            obj_db["dict_select"][ "abstract"] = abstract

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db )

        ################################################################################
        ### 返回数据库中的最新N条记录  
        obj_db["df_data"] = obj_db["df_data"].sort_values(by="date"  ,ascending=False )
        context["df_event_view_ind_style"] = obj_db["df_data"].T

    ###########################################################################
    ### 2,提交到数据库:新增市场、行业、主题、因子事件
    if "input_event_to_sql_indstyle" in request.POST.keys():            
        type_ind_style = request.POST.get("type_ind_style_submit","")
        name_index = request.POST.get("name_index_indstyle_submit","")
        code_index = request.POST.get("code_index_indstyle_submit","")
        if len( code_index ) < 1 :
            ### 需要尝试用行业名称匹配
            path_excel = "C:\\rc_202X\\rc_202X\\ciss_web\\ciss_exhi\\"
            file_excel = "db_manage.xlsx"
            sheet1 = "MATCH"
            df_ind_match = pd.read_excel(path_excel+file_excel,sheet_name= sheet1  )
            ### 
            df_sub = df_ind_match[df_ind_match["name_index" ] == name_index ]
            if len(df_sub.index) > 0 :
                code_index = df_sub["code_index"].values[0]
                type_ind_style = df_sub["type_ind_style"].values[0]

        ###############################################
        ###  
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_ind_style_market"
        ### insert_type == "1r" 导入一行
        obj_db["insert_type"] = "1r"
        obj_db["dict_1r"] = {} 
        obj_db["dict_1r"]["date" ] = request.POST.get("date_indstyle_submit","") 
        obj_db["dict_1r"]["type_ind_style" ] = type_ind_style
        obj_db["dict_1r"]["name_index" ] = request.POST.get("name_index_indstyle_submit","") 
        obj_db["dict_1r"]["code_index" ] = code_index
        obj_db["dict_1r"]["keyword" ] =  request.POST.get("keyword_indstyle_submit","") 
        obj_db["dict_1r"]["abstract" ] = request.POST.get("abstract_indstyle_submit","")  
        ### 非必须项
        if len( request.POST.get("weight_indstyle_submit","")  ) > 0 :
            obj_db["dict_1r"]["weight" ] = request.POST.get("weight_indstyle_submit","")  


        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.insert_table_data(obj_db) 
        obj_db["df_data"] = obj_db["df_data"].sort_values(by="date"  ,ascending=False )

        context["df_event_view_ind_style"] = obj_db["df_data"].T
    
    ###########################################################################
    ### 3,删除市场、行业、主题、因子事件
    if "input_event_delete_indstyle" in request.POST.keys():    
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_ind_style_market"
        
        del_id = request.POST.get("del_id_indstyle","")  
        del_col_name = request.POST.get("del_col_name_indstyle","")  
        del_col_value = request.POST.get("del_col_value_indstyle","") 
        if len(del_id) > 0 : 
            ### 按id删除row | obj_db["delete_type"] = "1r"
            obj_db["col_name"] = "id"
            obj_db["select_value"] = str(del_id )

        elif len(del_col_name) > 0  :
            ### 根据选择的列及列值删除
            obj_db["col_name"] = del_col_name
            obj_db["select_value"] = del_col_value
        else :
            print("No eligible criteria to delete rows.")

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.delete_table(obj_db) 

        df_data = obj_db["df_data"].sort_values(by="id",ascending=False )
        df_data = df_data.head(10)

        context["df_event_view_ind_style"] = df_data.T 

    ###########################################################################
    ###########################################################################
    ### 4,查询行业分类
    if "input_ind_style_match" in request.POST.keys():    
        
        type_ind_style_match = request.POST.get("type_ind_style_match","")
        column_name_match = request.POST.get("column_name_match","")
        column_value_match = request.POST.get("column_value_match","")
        ### 导入行业分类数据
        ### 需要尝试用行业名称匹配
        path_excel = "C:\\rc_202X\\rc_202X\\ciss_web\\ciss_exhi\\"
        file_excel = "db_manage.xlsx"
        sheet1 = "MATCH"
        df_ind_match = pd.read_excel(path_excel+file_excel,sheet_name= sheet1  )
        
        ########################################
        ### 匹配type_ind_style
        if len( type_ind_style_match ) > 0  :
            ### 
            df_ind_match = df_ind_match[df_ind_match["type_ind_style" ] == type_ind_style_match ] 
        
        ########################################
        ### 匹配具体的列
        if len( column_name_match ) > 0  :
            ### notes:name_index列和ind3列是一样的
            # 判断是否包含字符串报错： Cannot mask with non-boolean array containing NA / NaN values
            # Ana原因：这里就是说，分组这一列里面，包含了非字符串的内容，比如数字
            # Ans:直接忽略。你也可以写na=True
            df_sub = df_ind_match[ df_ind_match[ column_name_match ].str.contains( column_value_match,na=True ) ]
            if len( df_sub.index  ) > 0 :
                context["df_ind_style_match"] = df_sub.T
                print("df_ind_match 666---- \n", df_sub )
            else :
                context["df_ind_style_match"] = df_ind_match.T
        else :
            context["df_ind_style_match"] = df_ind_match.T

    ###########################################################################
    ###########################################################################
    ### 5,查询个股定量指标 | 
    ### file_data=ah_shares.xlsx ; path_data=C:\rc_202X\rc_202X\data_pms\data_adj
    if "input_stock_indi_search" in request.POST.keys():    
        
        code_stock = request.POST.get("code_stock","")
        name_stock = request.POST.get("name_stock","")
        ind3_stock = request.POST.get("ind3_stock","")
        ###
        column_name_stock = request.POST.get("column_name_stock","")
        column_value_lb_stock = request.POST.get("column_value_lb_stock","")
        column_value_ub_stock = request.POST.get("column_value_ub_stock","")


        #####################################    
        ### 导入个股定量指标
        ### 需要尝试用行业名称匹配
        path_excel = "C:\\rc_202X\\rc_202X\\data_pms\\data_adj\\"
        file_excel = "ah_shares.xlsx"
        sheet1 = "ah_shares"
        df_stock_indi = pd.read_excel(path_excel+file_excel,sheet_name= sheet1  )

        #####################################            
        if len( code_stock ) > 0  :
            ### 匹配股票代码
            df_sub = df_stock_indi[df_stock_indi["代码" ] == code_stock ]
        elif len( name_stock ) > 0  :
            #####################################    
            ### 匹配股票名称
            df_sub = df_stock_indi[df_stock_indi["名称" ] == name_stock ]
        elif len( ind3_stock ) > 0  :
            #####################################    
            ### 匹配股票名称
            df_sub = df_stock_indi[df_stock_indi["中信三级行业" ] == ind3_stock ]

        elif len( column_name_stock ) > 0  :
            #####################################    
            ### 匹配股票名称
            df_sub = df_stock_indi[df_stock_indi[column_name_stock ] >= float( column_value_lb_stock ) ]
            df_sub = df_sub[df_sub[column_name_stock ] <= float( column_value_ub_stock ) ]
        
        ##########################################################################
        ### 对部分column和数值做转换 | 市盈率(TTM),带括号的变量传递给Django会报错
        df_sub["pe_ttm"] = df_sub["市盈率(TTM)"].round(2)
        df_sub["roe_ttm"] = df_sub["净资产收益率(TTM)"].round(2)
        df_sub["fund_holding_pct"] = (df_sub["基金持股比例"]*100).round(2)
        ### 总市值
        df_sub["trend_mid"] = (df_sub["trend_mid"]*100).round(1)
        df_sub["trend_short"] = (df_sub["trend_short"]*100).round(1)
        df_sub["mv"] = df_sub["mv"].round(2)
        df_sub["pctchange_20d"] = (df_sub["20日涨跌幅"]*100).round(2)
        df_sub["pctchange_60d"] = (df_sub["60日涨跌幅"]*100).round(2)
        df_sub["pctchange_120d"]= (df_sub["120日涨跌幅"]*100).round(2)
        ### 排序：按近60日涨跌幅排序
        df_sub = df_sub.sort_values(by="60日涨跌幅" , ascending=False )



        context["df_stock_indi_search"] = df_sub.T

    ###########################################################################
    ### 6,查询个股历史定性研究记录 | table=event_view_stock_funda
    if "input_stock_active_search" in request.POST.keys():    
        
        date = request.POST.get("date_active_search","")
        code_stock = request.POST.get("code_stock_active_search","")
        name_stock = request.POST.get("name_stock_active_search","")
        ###
        keyword = request.POST.get("keyword_active_search","")
        abstract = request.POST.get("abstract_active_search","") 

        ###############################################
        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_stock_funda"
        obj_db["dict_select"] = {}
        if len( date ) > 0  :  
            obj_db["dict_select"][ "date"] = date
        if len( code_stock) > 0  :  
            obj_db["dict_select"][ "code"] = code_stock
        if len( name_stock) > 0  :  
            obj_db["dict_select"][ "name"] = name_stock
        if len( keyword) > 0  :  
            obj_db["dict_select"][ "keyword"] = keyword
        if len( abstract ) > 0  :  
            obj_db["dict_select"][ "abstract"] = abstract

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db )

        ### 按日期降序
        obj_db["df_data"] = obj_db["df_data"].sort_values(by="date" ,ascending=False )

        ################################################################################
        ### 返回数据库中的最新N条记录  
        context["df_stock_active_funda"] = obj_db["df_data"].T

    ###########################################################################
    ### 2,提交到数据库:个股历史定性研究记录 | table=event_view_stock_funda
    if "input_event_to_sql_stock_active" in request.POST.keys():    
        
        date = request.POST.get("date_active_submit","")
        name_stock = request.POST.get("name_stock_active_submit","")
        code_stock = request.POST.get("code_stock_active_submit","")
        if len( code_stock ) < 1 :
            ### 需要尝试用代码匹配个股名称
            path_excel = "C:\\rc_202X\\rc_202X\\data_pms\\data_adj\\"
            file_excel = "ah_shares.xlsx"
            sheet1 = "ah_shares"
            df_stock_indi = pd.read_excel(path_excel+file_excel,sheet_name= sheet1  )

            ### 
            df_sub = df_stock_indi[df_stock_indi["名称" ] == name_stock ]
            if len(df_sub.index) > 0 :
                code_stock = df_sub["代码"].values[0] 
        elif len( name_stock ) < 1 :
            ### 需要尝试用股票名称匹配个股代码
            path_excel = "C:\\rc_202X\\rc_202X\\data_pms\\data_adj\\"
            file_excel = "ah_shares.xlsx"
            sheet1 = "ah_shares"
            df_stock_indi = pd.read_excel(path_excel+file_excel,sheet_name= sheet1  )

            ### 
            df_sub = df_stock_indi[df_stock_indi["代码" ] == code_stock ]
            if len(df_sub.index) > 0 :
                name_stock = df_sub["名称"].values[0] 
        
        ###############################################
        ###  
        keyword= request.POST.get("keyword_stock_active_submit","") 
        abstract= request.POST.get("abstract_stock_active_submit","") 
        mv_buy_lb  = request.POST.get("mv_buy_lb_submit","") 
        mv_sell_ub = request.POST.get("mv_sell_ub_submit","") 
        source = request.POST.get("source_stock_active_submit","") 

        ###############################################
        ###  
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_stock_funda"
        ### insert_type == "1r" 导入一行
        obj_db["insert_type"] = "1r"
        obj_db["dict_1r"] = {} 
        obj_db["dict_1r"]["date" ] = date
        obj_db["dict_1r"]["name" ] = name_stock
        obj_db["dict_1r"]["code" ] = code_stock
        obj_db["dict_1r"]["keyword" ] = keyword
        obj_db["dict_1r"]["abstract" ] = abstract
        obj_db["dict_1r"]["mv_buy_lb" ] = mv_buy_lb
        obj_db["dict_1r"]["mv_sell_ub" ] = mv_sell_ub
        obj_db["dict_1r"]["source" ] = source

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.insert_table_data(obj_db) 

        context["df_stock_active_funda"] = obj_db["df_data"].T

    ###########################################################################
    ### 3,删除:个股历史定性研究记录 | table=event_view_stock_funda
    if "input_event_delete_stock_active" in request.POST.keys():    
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_stock_funda"
        
        del_id = request.POST.get("del_id_stock_active","")  
        del_col_name = request.POST.get("del_col_name_stock_active","")  
        del_col_value = request.POST.get("del_col_value_stock_active","") 
        if len(del_id) > 0 : 
            ### 按id删除row | obj_db["delete_type"] = "1r"
            obj_db["col_name"] = "id"
            obj_db["select_value"] = str(del_id )

        elif len(del_col_name) > 0  :
            ### 根据选择的列及列值删除
            obj_db["col_name"] = del_col_name
            obj_db["select_value"] = del_col_value
        else :
            print("No eligible criteria to delete rows.")

        
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.delete_table(obj_db) 

        ### 只返回最后10条记录
        df_data = obj_db["df_data"].sort_values(by="id",ascending=False )
        df_data = df_data.head(10)

        context["df_stock_active_funda"] = df_data.T


    ###########################################################################
    ###########################################################################
    ### 6,资产、基金池和股票池管理：I/O，weight
    ###########################################################################
    ### 1 查询股票池 | table=fundpool_stockpool_weight
    if "input_stockpool_search" in request.POST.keys():    
        
        date_pool = request.POST.get("date_pool_search","")
        pool_name = request.POST.get("pool_name_search","")
        pool_level = request.POST.get("pool_level_search","")
        ###
        type_asset_indstyle_stock = request.POST.get("type_asset_indstyle_stock_search","")
        strategy = request.POST.get("strategy_search","") 
        strategy_CN = request.POST.get("strategy_CN_search","") 
        code = request.POST.get("code_search","") 
        name = request.POST.get("name_search","") 
        weight = request.POST.get("weight_search","") 
        code_fund = request.POST.get("code_fund_search","") 
        code_stock = request.POST.get("code_stock_search","") 
        note = request.POST.get("note_search","") 

        ### 是否提取基本面研究信息：if_funda_info:点击框框数值是1 没点击是0
        if_funda_info = request.POST.get("if_funda_info_stockpool_search","") 
        ### 是否提取最近日期的量化指标信息：
        if_indicator_stockpool_search = request.POST.get("if_indicator_stockpool_search","") 
        print("是否提取基本面研究信息：if_funda_info=", if_funda_info,";是否提取最近日期的量化指标信息：",if_indicator_stockpool_search ) 
        ###############################################
        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
        obj_db["dict_select"] = {}
        if len( date_pool ) > 0  :  
            obj_db["dict_select"][ "date"] = date_pool
        if len( pool_name ) > 0  :  
            obj_db["dict_select"][ "pool_name"] = pool_name
        if len( pool_level) > 0  :  
            obj_db["dict_select"][ "pool_level"] = pool_level
        if len( type_asset_indstyle_stock) > 0  :  
            obj_db["dict_select"][ "type_asset_indstyle_stock"] = type_asset_indstyle_stock
        if len( strategy ) > 0  :  
            obj_db["dict_select"][ "strategy"] = strategy
        if len( strategy_CN ) > 0  :  
            obj_db["dict_select"][ "strategy_CN"] = strategy_CN
        if len( code ) > 0  :  
            obj_db["dict_select"][ "code"] = code
        if len( name ) > 0  :  
            obj_db["dict_select"][ "name"] = name
        if len( weight ) > 0  :  
            obj_db["dict_select"][ "weight"] = weight            
        if len( code_fund ) > 0  :  
            obj_db["dict_select"][ "code_fund"] = code_fund
        if len( code_stock ) > 0  :  
            obj_db["dict_select"][ "code_stock"] = code_stock
        if len( note ) > 0  :  
            obj_db["dict_select"][ "note"] = note 
        
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db ) 

        ################################################################################
        ### 股票池调整：如果xxx策略股票池的 xx股票有多条记录，选择日期最新的一条
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
        
        ################################################################################
        ### 返回数据库中的最新N条记录  
        context["df_fundpool_stockpool"] = df_data.T

        ################################################################################
        ### 判断是否需要提取最近的基本面研究信息 
        if if_funda_info in [1,"1"] :
            ###################################################
            ### 设置输入obj_db
            obj_db= {}
            obj_db["db_name"] = "db_funda.sqlite3"
            obj_db["table_name"] = "event_view_stock_funda"
            # 只匹配一列的特定值
            obj_db["col_name"] = "code"
            ###################################################
            ### 对于 df_data 里的每一个股票代码，查询sql数据库里的基本面研究日期、对应行业分类、关键词、概要
            for temp_i in df_data.index : 
                temp_code = df_data.loc[temp_i, "code" ]
                obj_db["select_value"] = temp_code
                #############################################
                from database import db_sqlite
                db_sqlite1 = db_sqlite()
                obj_db_out = db_sqlite1.select_table_data( obj_db ) 
                df_data_temp = obj_db_out["df_data"]
                ### 取日期最新的一列 
                df_data_temp = df_data_temp.sort_values(by="date",ascending=False)
                ### notes:df_data里已经有date了
                if len( df_data_temp.index ) > 0 :
                    df_data.loc[temp_i, "date_funda" ] = df_data_temp["date" ].values[0]
                    df_data.loc[temp_i, "keyword" ] = df_data_temp["keyword" ].values[0]
                    df_data.loc[temp_i, "mv_buy_lb" ] = df_data_temp["mv_buy_lb" ].values[0]
                    df_data.loc[temp_i, "mv_sell_ub" ] = df_data_temp["mv_sell_ub" ].values[0]
                    df_data.loc[temp_i, "abstract" ] = df_data_temp["abstract" ].values[0]
            ###################################################
            ### 再次赋值给  df_fundpool_stockpool
            context["df_fundpool_stockpool"] = df_data.T

        ################################################################################
        ### 是否提取最近日期的量化指标信息： 
        if if_indicator_stockpool_search in [1,"1"] :
            ###################################################
            ### TODO 导入最新的ah股基本面指标数据： 
            file_name = "ah_shares.xlsx"
            path_data_adj= "C:\\rc_202X\\rc_202X\\data_pms\\data_adj"
            df_temp = pd.read_excel(path_data_adj+"\\"+file_name)
            col_list_indi = df_temp.columns
            ### df_temp中没有code，"代码"
            df_temp.index = df_temp["代码"]
            df_data.index = df_data["code"]
            code_list = list( df_data["code"] )
            ### 将df_temp对应代码的所有列赋值给 df_data
            df_data.loc[code_list, col_list_indi] = df_temp.loc[code_list, col_list_indi] 

            ########################################################
            ### 取小数点 ： .round(decimals=2)  
            for temp_col in ["市盈率(TTM)","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率"  ] :
                df_data[ temp_col ] = df_data[ temp_col ].round(2)

            for temp_col in ["mv","trend_short","trend_mid","20日涨跌幅","60日涨跌幅","120日涨跌幅","年初至今" ] :
                df_data[ temp_col ] = (df_data[ temp_col ]*100).round(2)
            
            ########################################################
            ### 调整部分列名：
            dict_rename={}
            # 市盈率(TTM),净资产收益率(TTM)
            dict_rename["市盈率(TTM)"] = "市盈率ttm"
            dict_rename["净资产收益率(TTM)"] = "净资产收益率ttm"
            df_data = df_data.rename(columns = dict_rename ) 

            ###################################################
            ### 再次赋值给  df_fundpool_stockpool
            context["df_fundpool_stockpool"] = df_data.T
            ### save to xlsx
            df_data.to_excel("D:\\df_fundpool_stockpool.xlsx")

        #################################################################################
        ### 统计指标
        context["num_stock" ] = len( df_data.index ) 
        context["weight_sum" ] = round( df_data.weight.sum()*100, 2)

    ###########################################################################
    ### 2,提交调整到股票池,单一调整 | table=fundpool_stockpool_weight 
    if "input_stockpool_to_sql" in request.POST.keys():    
        
        date_pool = request.POST.get("date_pool_to_sql","")
        pool_name = request.POST.get("pool_name_to_sql","")
        pool_level = request.POST.get("pool_level_to_sql","")
        ###
        type_asset_indstyle_stock = request.POST.get("type_asset_indstyle_stock_to_sql","")
        strategy = request.POST.get("strategy_to_sql","") 
        strategy_CN = request.POST.get("strategy_CN_to_sql","") 
        code = request.POST.get("code_to_sql","") 
        name = request.POST.get("name_to_sql","") 
        weight = request.POST.get("weight_to_sql","") 
        code_fund = request.POST.get("code_fund_to_sql","") 
        code_stock = request.POST.get("code_stock_to_sql","") 
        note = request.POST.get("note_to_sql","") 

        ###############################################
        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
        obj_db["insert_type"] = "1r"
        obj_db["dict_1r"] = {}
        if len( date_pool ) > 0  :  
            obj_db["dict_1r"][ "date"] = date_pool
        if len( pool_name ) > 0  :  
            obj_db["dict_1r"][ "pool_name"] = pool_name
        if len( pool_level) > 0  :  
            obj_db["dict_1r"][ "pool_level"] = pool_level
        if len( type_asset_indstyle_stock) > 0  :  
            obj_db["dict_1r"][ "type_asset_indstyle_stock"] = type_asset_indstyle_stock
        if len( strategy ) > 0  :  
            obj_db["dict_1r"][ "strategy"] = strategy
        if len( strategy_CN ) > 0  :  
            obj_db["dict_1r"][ "strategy_CN"] = strategy_CN
        if len( code ) > 0  :  
            obj_db["dict_1r"][ "code"] = code
        if len( name ) > 0  :  
            obj_db["dict_1r"][ "name"] = name
        if len( weight ) > 0  :  
            obj_db["dict_1r"][ "weight"] = weight            
        if len( code_fund ) > 0  :  
            obj_db["dict_1r"][ "code_fund"] = code_fund
        if len( code_stock ) > 0  :  
            obj_db["dict_1r"][ "code_stock"] = code_stock
        if len( note ) > 0  :  
            obj_db["dict_1r"][ "note"] = note 

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.insert_table_data( obj_db ) 

        ################################################################################
        ### 股票池调整：如果xxx策略股票池的 xx股票有多条记录，选择日期最新的一条
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
        
        ################################################################################
        ### 返回数据库中的最新N条记录  
        context["df_fundpool_stockpool"] = df_data.T

        context["df_stock_active_funda"] = obj_db["df_data"].T

    ###########################################################################
    ### 2,提交调整到股票池,批量调整 | table=fundpool_stockpool_weight 
    ### notes:Notes:需要对上一期的退出个股或基金新增权重0%的记录
    if "input_stockpool_df_to_sql" in request.POST.keys():    
        ################################################################################
        ### step1 要先获取给定策略在数据库种，最近一期的持仓情况
        strategy = request.POST.get("strategy_df_to_sql","") 
        strategy_CN = request.POST.get("strategy_CN_df_to_sql","")  

        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
        obj_db["dict_select"] = {}
        if len( strategy_CN ) > 0  :  
            obj_db["dict_select"][ "strategy_CN"] = strategy_CN
        if len( strategy ) > 0  :  
            obj_db["dict_select"][ "strategy"] = strategy 
        
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db ) 

        #############################################
        ### 股票池调整：如果xxx策略股票池的 xx股票有多条记录，选择日期最新的一条
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

        #############################################
        ### 导入Excel外部数据表
        path_excel = request.POST.get("path_excel_stockpool_df_to_sql","")
        ### 替换 \,  \\
        file_name = request.POST.get("file_name_stockpool_df_to_sql","")
        sheet_name = request.POST.get("sheet_name_stockpool_df_to_sql","")
        df_table_import = pd.read_excel(path_excel+file_name, sheet_name= sheet_name  )
        # print("df_table_import \n", df_table_import ) 

        #############################################
        ### 在sql里的策略最新持仓文件中，寻找是否有不再持有的证券，增加一个退出的调整记录   
        col_list =  df_table_import.columns
        temp_date = df_table_import["date"].max()
        for temp_i in df_data.index :
            temp_code = df_data.loc[temp_i, "code"]
            ### find code in latest record of sql table
            df_sub = df_table_import[ df_table_import["code"] == temp_code ]
            if not len( df_sub.index ) > 0 :
                ### 在df_table_import中 增加一个退出的调整记录
                temp_index = df_table_import.index.max()+1 
                ### notes:df_data 比 df_table_import 多一列 "id"
                print("Debug   \n",temp_index ,  )
                df_table_import.loc[temp_index,col_list  ] = df_data.loc[temp_i ,col_list ]
                ### 需要修改 日期为最新、权重=0.0%
                df_table_import.loc[temp_index, "date" ]= temp_date
                df_table_import.loc[temp_index, "weight" ]= 0.0

        #############################################
        ### 将 df_table_import 导入sql 
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
        obj_db["insert_type"] = "df"
        obj_db["df_table"] = df_table_import

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.insert_table_data(obj_db) 
        
        ### 这里只需要返回和 策略名称相关的
        df_data = obj_db["df_data"] 
        df_data = df_data[df_data["strategy"] == strategy ]

        context["df_fundpool_stockpool"] = df_data.T  

        

    ###########################################################################
    ### 3 删除股票池 | table=fundpool_stockpool_weight
    if "input_stockpool_delete" in request.POST.keys():    
        
        date_pool = request.POST.get("date_pool_delete","")
        pool_name = request.POST.get("pool_name_delete","")
        pool_level = request.POST.get("pool_level_delete","")
        ###
        type_asset_indstyle_stock = request.POST.get("type_asset_indstyle_stock_delete","")
        strategy = request.POST.get("strategy_delete","") 
        strategy_CN = request.POST.get("strategy_CN_delete","") 
        code = request.POST.get("code_delete","") 
        name = request.POST.get("name_delete","") 
        weight = request.POST.get("weight_delete","") 
        code_fund = request.POST.get("code_fund_delete","") 
        code_stock = request.POST.get("code_stock_delete","") 
        note = request.POST.get("note_delete","") 

        ###############################################
        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight" 
        
        del_id = request.POST.get("id_stockpool_delete","")  
        col_name = request.POST.get("col_name_stockpool_delete","")  
        col_value = request.POST.get("col_value_stockpool_delete","") 
        if len(del_id) > 0 : 
            ### 按id删除row | obj_db["delete_type"] = "1r"
            obj_db["col_name"] = "id"
            obj_db["select_value"] = str(del_id )

        elif len(del_col_name) > 0  :
            ### 根据选择的列及列值删除
            obj_db["col_name"] = col_name
            obj_db["select_value"] = col_value
        else :
            print("No eligible criteria to delete rows.")
        
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.delete_table( obj_db ) 

        ################################################################################
        ### 股票池调整：如果xxx策略股票池的 xx股票有多条记录，选择日期最新的一条
        ### 这里只需要返回和 策略名称相关的
        df_data = obj_db["df_data"] 
        df_data = df_data[df_data["strategy"] == strategy ]
        
        ################################################################################
        ### 返回数据库中的最新N条记录  
        context["df_fundpool_stockpool"] = df_data.T

    ###########################################################################
    ### 6,event_summary_statistics
    ###########################################################################
    ### 6.1,区间汇总：大类资产和宏观、市场行业风格、个股、基金研究 | table= 
    if "input_event_summary_period" in request.POST.keys():    
        
        date_begin  = request.POST.get("date_begin_summary_period","")
        date_end  = request.POST.get("date_end_summary_period","") 

        ###########################################################################
        ### 1，匹配大类资产和宏观表格  "event_view_multiasset_macro" | 4个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_multiasset_macro"
        # obj_db["col_name_date"], 日期column的列名称（通常是 date ）
        obj_db["col_name_date"] = "date"
        obj_db["date_begin"] = date_begin
        obj_db["date_end"]   = date_end
        
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db )

        #############################################
        ### 返回数据库中的最新N条记录 | 这里要升序排列
        df_temp = obj_db["df_data"].sort_values(by="date",ascending=True ) 
        
        #############################################
        ### 做文字模板：excel-formula= =B355&"跟踪"&D355&E355&"方面变动："&LEFT(G355,60)&"..."
        df_temp["template"] = df_temp.apply(lambda x : str( x["date"])+"，跟踪"+x["asset_market_topic"]+"——"+x["keyword"]+"方面变动："+x["abstract"][:120]+"..." , axis=1)
        
        #############################################
        ### 改部分的column name
        dict_rename={}
        # asset_market_topic type_event_view,[keyword	abstract]
        dict_rename["asset_market_topic"] = "asset_market_topic"
        dict_rename["type_event_view"] = "type"
        df_temp = df_temp.rename(columns = dict_rename )
        df_data = df_temp.loc[:,["date","asset_market_topic","type","keyword","abstract","template" ]]

        #############################################
        ### 排序：因为资产列是股票名称，因此按资产排序肯定会有问题，应该从分类整合时就按顺序排列
        if len(df_data.index ) > 0 :
            df_data = df_data.sort_values( by=["asset_market_topic","type","date"], ascending=False ) 

        ###########################################################################
        ### 2，行业和风格  "event_view_ind_style_market"
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] =  "event_view_ind_style_market"
        # obj_db["col_name_date"], 日期column的列名称（通常是 date ）
        obj_db["col_name_date"] = "date"
        obj_db["date_begin"] = date_begin
        obj_db["date_end"]   = date_end 

        obj_db = db_sqlite1.select_table_data( obj_db )
        ### 
        df_temp = obj_db["df_data"].sort_values(by="date",ascending=True ) 

        #############################################
        ### 做文字模板：excel-formula==B388&"跟踪"&D388&"行业,"&AK388&","&F388&"动态:"&LEFT(G388,30)&"…"
        if len(df_temp.index ) > 0 :
            df_temp["template"] = df_temp.apply(lambda x : str( x["date"])+"，跟踪"+x["name_index"]+"——"+x["keyword"]+"方面变动："+x["abstract"][:120]+"..." , axis=1)
            
            #############################################
            ### 改部分的column name
            # name_index,type_ind_style	keyword	abstract
            dict_rename={}
            dict_rename["name_index"] = "asset_market_topic"
            dict_rename["type_ind_style"] = "type"
            df_temp = df_temp.rename(columns = dict_rename )

            #############################################
            ### 排序：因为资产列是股票名称，因此按资产排序肯定会有问题，应该从分类整合时就按顺序排列
            df_temp = df_temp.sort_values( by=["asset_market_topic","type","date"], ascending=False ) 


            df_data = df_data.append( df_temp.loc[:,["date","asset_market_topic","type","keyword","abstract","template" ]] )

        ###########################################################################
        ### 1，个股 "event_view_stock_funda" | notes:股票表格没有行业分类ind1 和 ind3，需要导入最新行业分类
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "event_view_stock_funda"
        # obj_db["col_name_date"], 日期column的列名称（通常是 date ）
        obj_db["col_name_date"] = "date"
        obj_db["date_begin"] = date_begin
        obj_db["date_end"]   = date_end 

        obj_db = db_sqlite1.select_table_data( obj_db )
        ### 
        df_temp = obj_db["df_data"].sort_values(by="date",ascending=True ) 
        
        if len(df_temp.index ) > 0 :
            #############################################
            ### 做文字模板：
            ''' 20220909，基础池。名称：德业股份、代码：605117.SH；定量指标：市场状态分析处于上涨末期、60日涨跌幅95.8%、总市值1015.5亿、
            市盈率(TTM)126.3；基金持股比例39%、净资产收益率(TTM)27.9%、归母净利润同比增长率100.4%、中信一级行业=电力设备及新能源、中信三级行业=太阳能；
            关键词：海外光伏逆变器需求爆发；概要：半年报业......
            '''
            ### 定义文字模板function 
            # 例子：dataSet['星座'] =  dataSet.apply(lambda x : function_1(x['出生月份'],x['出生日']),axis=1)

            
            #####################################    
            ### 数据准备：导入个股定量指标  
            path_excel = "C:\\rc_202X\\rc_202X\\data_pms\\data_adj\\"
            file_excel = "ah_shares.xlsx"
            sheet1 = "ah_shares"
            df_stock_indi = pd.read_excel(path_excel+file_excel,sheet_name= sheet1  )

            #####################################    
            ### 数据准备：导入主动股票池，且最新权重大于 0.01% = 未调出
            obj_db = {} 
            obj_db["db_name"] = "db_funda.sqlite3"
            obj_db["table_name"] = "fundpool_stockpool_weight"
            ### 需要匹配的columns
            obj_db["dict_select"] ={}
            obj_db["dict_select"]["pool_name"] =  "active_stock"
            obj_db["dict_select"]["pool_level"] =  "core"
            obj_db["dict_select"]["type_asset_indstyle_stock"] =  "stock"
            obj_db = db_sqlite1.select_table_data( obj_db )
            df_stockpool = obj_db["df_data"]
            # 先按日期升序排列，再去除行的重复项保留最后一次出现的 
            df_stockpool = df_stockpool.sort_values(by="date",ascending=True )
            df_stockpool = df_stockpool.drop_duplicates(subset="code", keep="last" )
            
            #####################################    
            df_temp["if_stockpool_core" ] = "否"
            df_temp["str_indicators" ] = "" 
            for temp_i in df_temp.index : 
                temp_code = df_temp.loc[temp_i, "code" ]
                ##################################### 
                ### step 1:查询个股是否属于核心或基础股票池，最新权重大于 0.01%
                df_sub1 = df_stockpool[ df_stockpool["code"] == temp_code ]
                if len( df_sub1.index ) > 0 :
                    ### 属于核心池
                    df_temp.loc[temp_i, "if_stockpool_core" ] = "是"
                
                ##################################### 
                ### step 2:查询个股基本面指标
                df_sub2 = df_stock_indi[df_stock_indi["代码" ] == temp_code ]
                if len( df_sub2.index ) > 0 :
                    ### 匹配定量指标 |注意：2个表格的column名称不一样！！！
                    str_indicators = ""
                    str_indicators = str_indicators +"名称：" + df_sub2["名称"].values[0] +"；"
                    # str_indicators = str_indicators +"代码：" + df_sub2["代码"].values[0] +"；"
                    str_indicators = str_indicators +"定量指标：月平均总市值 " + str(round( df_sub2["m_ave_mv"].values[0],1) ) +"亿元、"
                    str_indicators = str_indicators +"市盈率(TTM) " + str( round( df_sub2["市盈率(TTM)"].values[0],1) ) +"、"
                    str_indicators = str_indicators +"基金持股比例 " + str( round( df_sub2["基金持股比例"].values[0],2) ) +"%、"
                    str_indicators = str_indicators +"净资产收益率(TTM)  " + str( round( df_sub2["净资产收益率(TTM)"].values[0],1) ) +"%、"
                    str_indicators = str_indicators +"归母净利润同比增长率 " + str( round( df_sub2["归母净利润同比增长率"].values[0],1) ) +"%、"
                    
                    ### notes:  df_sub2["中信一级行业"].values[0] = 37.2%,有可能是 float
                    str_indicators = str_indicators +"中信一级行业：" + str( df_sub2["中信一级行业"].values[0] ) +"、"
                    str_indicators = str_indicators +"中信三级行业：" + str( df_sub2["中信三级行业"].values[0] ) +"。" 
                    ### 定义 ind3 ，ind1
                    df_temp.loc[temp_i, "ind1" ] = df_sub2["中信一级行业"].values[0]
                    df_temp.loc[temp_i, "ind3" ] = df_sub2["中信三级行业"].values[0]
                else :
                    str_indicators = ""
                    ### 可能是刚上市的新股或小市值股票
                    print("Check why no record in ah_shares.xlsx, as stock code=",temp_code )
                    df_temp.loc[temp_i, "ind1" ] = ""
                    df_temp.loc[temp_i, "ind3" ] = ""
                ### 
                df_temp.loc[temp_i, "str_indicators" ] = str_indicators
                
                
            ##################################### 
            ### 
            df_temp["template"] = df_temp.apply(lambda x : str( x["date"])+"，核心池："+ x["if_stockpool_core"] +"。代码："+x["code"]+";"+ x["str_indicators"]+ x["keyword"] +"、"+ x["abstract"][:120]+"..." , axis=1)
            
            #############################################
            ### 改部分的column name
            # name ind3	keyword	abstract
            dict_rename={}
            dict_rename["name"] = "asset_market_topic"
            dict_rename["ind3"] = "type"
            print("df-temp \n", df_temp.head().T )
            print("dict_rename:" ,dict_rename  )
            df_temp = df_temp.rename(columns = dict_rename )

            #############################################
            ### 排序：因为资产列是股票名称，因此按资产排序肯定会有问题，应该从分类整合时就按顺序排列
            df_temp = df_temp.sort_values( by=["asset_market_topic","type","date"], ascending=False ) 

            df_data = df_data.append( df_temp.loc[:,["date","asset_market_topic","type","keyword","abstract","template" ]] )

        

        ################################################################################
        ### Notes:三张表的column名称不同，需要进行融合
        df_data.to_excel("D:\\df_data.xlsx")

        ################################################################################
        ### 返回数据库中的最新N条记录  
        context["df_summary_period"] = df_data.T

    
    #################################################################################
    ### 输出的dict对象 context  # 
    ### 查询列表         

    ### 时间
    context["time_now_str"] = time_now_str
    context["time_now_str_pre1d"] =time_now_str_pre1d 


    return render(request,"ciss_exhi/event/index_event_view.html",context) 







###########################################################################
### Index of data list | 数据主页，包括主要数据库链接
@csrf_protect
@requires_csrf_token
@csrf_exempt
def data_manage(request):
    ###########################################################################
    ### 数据库和各类数据管理
    ###########################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    
    ###########################################################################
    ### 新建或删除sqlite表格
    if "input_table_gen" in request.POST.keys():   
        db_name_gen_del= request.POST.get("db_name_gen_del","")  
        table_gen_del= request.POST.get("table_gen_del","")  
        ###############################################
        ### 新建或删除sqlite表格
        if table_gen_del == "generate" :  
            obj_db = {} 
            obj_db["db_name"] = db_name_gen_del
            obj_db["table_name"] = request.POST.get("table_name_gen_del","")
            obj_db["gen_type"] = "excel"

            print("Debug==table_name=", obj_db["table_name"] )
            #############################################
            from database import db_sqlite
            db_sqlite1 = db_sqlite()
            obj_db = db_sqlite1.generate_table(obj_db)

            ### 
            context["table_data"] = obj_db["table_data"]

        ###############################################
        ### 删除sqlite表格
        if table_gen_del == "delete" :
            obj_db = {} 
            obj_db["db_name"] = db_name_gen_del
            obj_db["table_name"] = request.POST.get("table_name_gen_del","")
            obj_db["delete_type"] = "table" 

            #############################################
            from database import db_sqlite
            db_sqlite1 = db_sqlite()            
            obj_db = db_sqlite1.delete_table(obj_db) 

    ###########################################################################
    ### 删除Sqlite表格内，部分列的值相同的重复项，并保留最小的id
    # step 1:选出某几列数值相同的列的最大的id值； step 2:根据返回的列的id，逐一删除sql中的记录
    if "input_table_del_index" in request.POST.keys():   
        obj_db = {} 
        ### 数据库信息
        obj_db["db_name"] = request.POST.get("db_name_del_index","")  
        obj_db["table_name"] = request.POST.get("table_name_del_index","")
        col_list_str = request.POST.get("col_list_str_del_index","")
        col_list = col_list_str.split(",")
        obj_db["col_list"] = col_list
        obj_db["col_list_str"] = col_list_str
        ### obj_db["delete_type"] == "duplicates",意味着删除重复项
        obj_db["delete_type"] = "duplicates"
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()            
        obj_db = db_sqlite1.delete_table_index(obj_db) 
        ### 保存要删除的id对应df
        df_del = obj_db["df_del"] 
        context["df_del"] = df_del.T


    ###########################################################################
    ### Excel批量导入Sqlite：数据从excel-sheet 导入 sqlite-table
    # 给定表格名称和数据源，将excel文件中数据导入sqlite的table
    if "input_table_import" in request.POST.keys():   
        obj_db = {} 
        ### 数据库信息
        obj_db["db_name"] = request.POST.get("db_name_table_import","")  
        obj_db["table_name"] = request.POST.get("table_name","")
        obj_db["insert_type"] = "df"
        ### 外部数据信息
        if len( ) < 10 :
            obj_db["path_excel"] = "C:\\rc_202X\\rc_202X\\ciss_web\\ciss_exhi\\"
        else :
            obj_db["path_excel"] = request.POST.get("path_excel","")
        ### 
        obj_db["file_name"] = request.POST.get("file_name","")
        obj_db["sheet_name"] = request.POST.get("sheet_name","")
        ### 
        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.insert_table_data(obj_db) 
        
        context["df_table_import"] = obj_db["df_data"].T 

    ###########################################################################
    ### 新建Sqlite数据库
    # 例子：conn = sqlite3.connect("test.db") #连接数据库，若test.db不存在，则会创建该数据库 
    if "input_db_name_create" in request.POST.keys():   
        db_name_create = request.POST.get("db_name_create","")
        if len( db_name_create ) > 1 :
            obj_db = {} 
            ### 数据库信息
            obj_db["db_name"] = db_name_create

            #############################################
            from database import db_sqlite
            db_sqlite1 = db_sqlite()
            obj_db = db_sqlite1.create_database(obj_db)  

        else :
            print("No input name for create database")


    #################################################################################
    ### 输出的dict对象 context  # 
    ### 查询列表         

    ### 时间
    context["time_now_str"] = time_now_str
    context["time_now_str_pre1d"] =time_now_str_pre1d 

    return render(request,"ciss_exhi/data/index_data.html",context)  



































