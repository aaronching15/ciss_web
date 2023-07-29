###########################################################################
'''
list of def in this file 
def index(request):
def json_index(request):

def data_index(request):
def data_log(request):

def stra_index(request):
def stra_single(request):
def stra_abm_rc(request):
def stra_bond_jny(request):

def etf_data(request):

def port_index(request):
def port_single(request):

def docs_index(request):
def docs_index(request):    
'''
###########################################################################
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
### LEVEL 0 
###########################################################################
### Index and template page | 网站首页
def index(request):
    ###  网站首页
    return render(request,"ciss_exhi/index_ciss.html")


def log_index(request):
    ### 开发日志文件
    return render(request,"ciss_exhi/index_log.html")

def quick(request):
    ### 快速功能
    return render(request,"ciss_exhi/quick.html")

def json_index(request):
    # 直接引用static\ 目录下的json文件
    return render(request,"ciss_exhi/json/tree_stra_s.json")

##################################################################
### Level 1 
###########################################################################
### monitor_market_data 市场数据跟踪
def monitor_market_data(request):
    
    context={}

    return render(request, 'ciss_exhi/data/monitor_market_data.html', context) 

###########################################################################
### Index of AI,machine learning,deep learning,factors
    ### 人工智能模型和相关研究
def ai_index(request):
    
    context={}

    return render(request, 'ciss_exhi/ai/ai_index.html', context) 

###########################################################################
### LEVEL 1 
###########################################################################
### Index of industry research list 
### 行业产业链研究 
def industry_index(request) :

    context={}
    return render(request, 'ciss_exhi/industry/industry_index.html', context)

###########################################################################
### Index of event list  | 事件主页，包括主要事件库链接
def event_index(request):
    #  
    return render(request,"ciss_exhi/event/index_event.html")

###########################################################################
### Index of FUND,FOF list  | 基金和FOF研究，
def fund_fof_index(request):
    #  
    return render(request,"ciss_exhi/fund_fof/index_fund_fof.html")


###########################################################################
### LEVEL 2

###########################################################################
### Index of data list  | 数据主页，包括主要数据库链接
def data_index(request):
    #  
    return render(request,"ciss_exhi/data/index_data.html")
    
### Index of data log  | 数据日志
def data_log(request):
    '''
    目前需要更新的信息：
        1，三张表的最新更新日期；
        2, 个股,ETF,指数的最新更新日期；
        3，打印未正常更新的证券列表。

    last 190730 | since 190730
    derived from ..\\CISS_rc\\apps\\rc_stra\\test_Wind_19.py
    '''
    path_data = 'D:\\data_Input_Wind\\'
    file_name = "rc_data_log.csv"

    context = {}
    temp_df = pd.read_csv(path_data+"rc_data_log.csv",index_col="Unnamed: 0")
    context["data_log"] = temp_df.T()
    ### columns = "table_name","symbol","last_update","file_name","file_path"
    return render(request,"ciss_exhi/data/data_log.html",context )

### Index of wind data list  | 数据主页，包括主要数据库链接
@csrf_protect
@requires_csrf_token
@csrf_exempt
def data_wind(request):
    ### todo，1，读取返回的数据；2，运行python脚本，3，返回结果到网页的框
    '''
    参考 aactive benchmark model，def stra_abm_rc(request)
    '''
    #################################################################################
    ### Initialization 
    import os 
    # 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
    import sys
    sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
    sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )
    ### 导入wds数据转换模块
    from transform_wind_wds import transform_wds
    transform_wds1 = transform_wds()
    ### Print all modules  
    
    import pandas as pd 

    path_data_wds =  "C:\\db_wind\\data_wds\\"
    path_data_adj = "C:\db_wind\data_adj\industries_class\\"
    if not os.path.exists( path_data_wds  ) : 
        path_data_wds =  "D:\\db_wind\\data_wds\\"
        path_data_adj = "D:\db_wind\data_adj\industries_class\\"
    
    context={"info":"none"}
    # notes: type of request.POST is  <QueryDict: {}>
    # 若html页面商点击input按钮，则dict变量request.POST里会增加key为input:name,value为input:value
    print("request.POST", type(request.POST),request.POST )
    ### 判断POST中来自不同input的类型：如果是name值为input_check_table_anndates的，则执行此段代码
    if "input_check_table_anndates" in request.POST.keys():    
        #################################################################################
        ### 1, 查询Wind-wds全部数据表的最近3个交易日更新情况
        path_check_anndates = "C:\\ciss_web\\CISS_rc\\apps\\rc_data\\"
        # path_check_anndates = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
        if not os.path.exists( path_check_anndates  ) : 
            path_check_anndates = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
        
        file_data_check_anndates = "data_check_anndates.csv"
        df_check_anndates = pd.read_csv(path_check_anndates + file_data_check_anndates )
        df_check_anndates = df_check_anndates.tail(3).reset_index().T
        print("df_check_anndates",df_check_anndates)
        context["df_check_anndates"] = df_check_anndates
    
    elif "input_check_single_table" in request.POST.keys():        
        #################################################################################
        ### 2,获取表格目录下最新5个文件名
        ### Get data from html templates
        context["search_wds_table_name"] = request.POST.get("search_wds_table_name","AShareEODPrices")
        context["search_key_word"] = request.POST.get("search_key_word","S_INFO_WINDCODE")
        context["search_key_value"] = request.POST.get("search_key_value","688389.SH")

        path_dir= path_data_wds + context["search_wds_table_name"]
        # 打印目录中最近修改的3个文件，需要对目录内文件进行排序计算，会花费一些时间
        file_list = os.listdir(path_dir)
        if not file_list:
            return
        else:
            # sorted() 从小到大排列
            search_file_list_sorted = sorted(file_list, key=lambda x: os.path.getmtime(os.path.join(path_dir, x)))
            print("search_file_list_sorted")
            print(search_file_list_sorted[-5:] )

        context["search_file_list_latest5"] =  search_file_list_sorted[-5:]
        
        ### 3,查找关键词及数值对应的表格，返回最后三行
        # WDS_S_INFO_WINDCODE_688389.SH_ALL.csv
        # file_name = "WDS_"+"S_INFO_WINDCODE"+"_"+"688389.SH"+"_ALL.csv"
        file_name = "WDS_"+ context["search_key_word"] +"_"+ context["search_key_value"] +"_ALL.csv"
        
        search_temp_df = pd.read_csv(path_dir +"\\" +file_name )
        #默认 temp_df的数据在下载和保存之前已经是升序排列
        search_temp_df= search_temp_df.tail(3).reset_index().T
        print("search_temp_df", search_temp_df)
        context["search_df_tail3"] = search_temp_df

    elif "input_get_single_table" in request.POST.keys():    
        #################################################################################
        ### 4,下载单个表格或根据关键值下载
        # 默认不要直接下载数据
        context["get_wds_table_name"] = request.POST.get("get_wds_table_name","")
        # if_whole_table = 1 means download the whole table and 0 means download for specific key value.
        context["get_if_whole_table"] = request.POST.get("get_if_whole_table","0")
        context["get_key_word"] = request.POST.get("get_key_word","S_INFO_WINDCODE")
        context["get_key_value"] = request.POST.get("get_key_value","688389.SH")


        path_dir= path_data_wds + context["get_wds_table_name"]
        # 引入相关下载模块，notes:有可能存在无法连接数据库的风险，要避免整个网页无法显示。
        try :
            print("Debug = 1 ")
            from get_wind_wds import wind_wds
            wind_wds1 = wind_wds()
            ### Print all modules 
            print("Debug = 2 ")
            table_name = context["get_wds_table_name"]

            if context["get_if_whole_table"] == "0" :
                print("Debug = 3 ")
                prime_key =context["get_key_word"] 
                prime_key_value = context["get_key_value"]
                data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value ) 
                print("Debug = 4 ")

            elif context["get_if_whole_table"] == "1" :
                data_obj = wind_wds1.get_table_full_input(table_name ) 

            context["get_error"] = "Success for connecting database or fetching data"
            context["get_df_tail3"] = data_obj["wds_df"].tail(3).T
            context["get_file_path"] = data_obj["file_path"]
            context["get_file_name"] = data_obj["file_name"] 
        except:
            context["get_error"] = "Error during connecting database or fetching data"
        
        print("Debug = 5 ")
    
    elif "input_get_table_anndates" in request.POST.keys():    
        #################################################################################
        ### 5,运行每日数据下载程序
        ### notes：由于这个部分下载时间比较久，应该打开CMD下载，不应该下载完再刷新页面

        context["if_data_check_anndates"] = request.POST.get("if_data_check_anndates","0")
        if context["if_data_check_anndates"] == "1" :
            os.system( "cd C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data" )
            os.system( "python test_wds_manage.py" )
        
    elif "input_transform_stocks_ind_class" in request.POST.keys():    
        #################################################################################
        ### 5,每周/月：下载和维护个股行业分类
        ### notes：由于这个部分下载时间比较久，应该打开CMD下载，不应该下载完再刷新页面

        context["if_data_check_anndates"] = request.POST.get("if_data_check_anndates","0")
        if context["if_data_check_anndates"] == "1" :
            os.system( "cd C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data" )
            # 计算个股历史行业分类，result = transform_wds1.cal_stock_indclass()
            os.system( "python test_wds_data_transform.py" )

    elif "input_get_stock_indclass" in request.POST.keys():    
        #################################################################################
        ### 5,每周/月：下载和维护个股行业分类
        ### notes：由于这个部分下载时间比较久，应该打开CMD下载，不应该下载完再刷新页面

        context["get_stock_indclass_windcode"] = request.POST.get("get_stock_indclass_windcode","0")
        windcode = context["get_stock_indclass_windcode"] 

        df_stock_indclass = pd.read_csv(path_data_adj+"df_ind_code_stock_last.csv",encoding="gbk" )
        df_stock_indclass_sub = df_stock_indclass[ df_stock_indclass["wind_code"]== windcode ]
        if df_stock_indclass_sub.empty : 
            context["df_stock_indclass_windcode"] = "no record for " + windcode
        else :
            context["df_stock_indclass_windcode"] = df_stock_indclass_sub.T 
    
    elif "submit_get_index_period_diff" in request.POST.keys():    
        #################################################################################
        ### 6,id="get_index_period_diff">查询指数区间涨跌幅：
        ### notes： 
        asset_type = request.POST.get("period_diff_asset_type","index") 
        code_input = request.POST.get("get_index_period_diff_windcode","000001.SH")  
        date_start = request.POST.get("get_index_period_diff_date_start","20200101")
        date_end = request.POST.get("get_index_period_diff_date_end","20200323")

        ### Index 
        if asset_type == "index" :            
            period_chg = transform_wds1.get_index_period_diff(code_input,date_start,date_end)
            context["index_period_chg"] = round(period_chg,3 )

        ### Stock
        elif asset_type == "stock" :
            object_code = transform_wds1.get_stock_period_diff("",code_input,date_start,date_end)
            context["index_period_chg"] = object_code

        ### Fund 
        if asset_type == "fund" :            
            period_chg = transform_wds1.get_fund_period_diff(code_input,date_start,date_end)
            context["index_period_chg"] = round(period_chg,3 )

        context["get_index_period_diff_windcode"] =code_input 

    #################################################################################
    ###
    return render(request,"ciss_exhi/data/data_wind.html",context)

def data_ciss_web(request):
    #################################################################################
    ### 
    context = {}


    #################################################################################
    ###
    return render(request,"ciss_exhi/data/data_ciss_web.html",context)

def data_ciss_db(request):
    #################################################################################
    ### 
    context = {}


    #################################################################################
    ###
    return render(request,"ciss_exhi/data/data_ciss_db.html",context)

def data_rc_report(request):
    #################################################################################
    ### 
    context = {}


    #################################################################################
    ###
    return render(request,"ciss_exhi/data/data_rc_report.html",context)

def data_touyan(request):
    #################################################################################
    ### 
    context = {}


    #################################################################################
    ###
    return render(request,"ciss_exhi/data/data_rc_touyan.html",context)

### Index of data list  | 数据清单
def data_db_wind_wds(request):
    # last | since 191118  
    return render(request,"ciss_exhi/data/db_wind_wds/log_191118.html")

### 数据库学习的相关知识
def data_study_postgresql(request):
    # last | since 191123 
    return render(request,"ciss_exhi/data/knowledge/data_study_postgresql.html")



### 软件产业链 industry_software 
def industry_software(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_software.html', context)
### industry_media 传媒产业链
def industry_media(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_media.html', context)
### industry_finance_new 新金融产业链
def industry_finance_new(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_finance_new.html', context)

### industry_biopharmaceutical.html">生物科技和医药产业链
def industry_biopharmaceutical(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_biopharmaceutical.html', context)

### industry_manufacturing.html">新材料和装备制造
def industry_manufacturing(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_manufacturing.html', context)


### industry_comsumer_staples.html 必须消费，=日常消费
def industry_comsumer_staples(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_comsumer_staples.html', context)

### industry_comsumer_discretionary.html 可选消费，=耐用消费
def industry_comsumer_discretionary(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_comsumer_discretionary.html', context)


### industry_huawei.html">华为产业链
def industry_huawei(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_huawei.html', context)
### industry_apple.html">苹果产业链
def industry_apple(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_apple.html', context)
### industry_tesla.html">特斯拉产业链
def industry_tesla(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_tesla.html', context)

### industry_semiconductor.html">半导体产业链
def industry_semiconductor(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_semiconductor.html', context)
### industry_solar.html">光伏产业链
def industry_solar(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_solar.html', context) 

### industry_alibaba.html">阿里巴巴产业链
def industry_alibaba(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_alibaba.html', context)
### industry_alibaba_ecommerce_mcn.html">阿里-电商和MCN产业链
def industry_alibaba_ecommerce_mcn(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_alibaba_ecommerce_mcn.html', context)

### industry_alibaba_internet_finance.html">阿里-互联网金融产业链
def industry_alibaba_internet_finance(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_alibaba_internet_finance.html', context)
### industry_alibaba_cloud_computing.html">阿里-云计算产业链
def industry_alibaba_cloud_computing(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_alibaba_cloud_computing.html', context) 
### industry_tencent.html">腾讯产业链
def industry_tencent(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_tencent.html', context)
### industry_tencent_gaming.html">腾讯-游戏产业链
def industry_tencent_gaming(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_tencent_gaming.html', context)
### industry_tencent_online_video.html">腾讯-影视传媒产业链
def industry_tencent_online_video(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_tencent_online_video.html', context)
### industry_locallife_food_delivery.html">本地生活和外卖产业链
def industry_locallife_food_delivery(request) :
    
    context={} 
    return render(request, 'ciss_exhi/industry/industry_locallife_food_delivery.html', context) 
















### 临时行业研究信息
def temp_ind(request) :
    
    context={}

    return render(request, 'ciss_exhi/industry/temp.html', context)

### 行业个股模型首页 model_stock_index
def model_stock_index(request) :
    
    context={}

    
    return render(request, 'ciss_exhi/industry/model_stock_index.html', context)


###########################################################################
### Index of strategy list 
### working on test strategy file
from .models import Strategy,Portfolio
import pandas as pd 

def stra_index(request): 
    # type of Strategy is django.db.models.base.ModelBase
    
    ### last 200211 | since 181101
    # 上下文context是一个字典，它将模板内的变量映射为 Python 对象。
    context={"info":"none"} 
    # type of context is  <class 'dict'>
    
    ### define latest strategy list  
    latest_stra_list = Strategy.objects.order_by('-stra_date_last')[:5]
    context = {'latest_stra_list': latest_stra_list}

    ### define core strategy list 
    # core_stra_list_gy = Strategy.objects.filter(stra_supervisor='du')
    # context["core_stra_list_gy"]= core_stra_list_gy
    ### todo define core strategy list 

    ### type core_stra_list_cs <class 'django.db.models.query.QuerySet'>
    core_stra_list_cs = Strategy.objects.filter(stra_client='cs')
    context["core_stra_list_cs"]= core_stra_list_cs 

    # render() 「载入模板，填充上下文，再返回由它生成的 HttpResponse 对象
    return render(request, 'ciss_exhi/strategy/stra_index.html', context)

#####################################################################
### Index of single strategy
@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_single(request):
    # create page for single strategy 
    # last | since 190111
    
    context={"info":"none"} 

    return render(request,'ciss_exhi/strategy/stra_single.html',context)

#####################################################################
### working on personal strategy
#####################################################################
### strategy at csfunds  | author rC 
@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_cs_index_enhance3(request):
    ### 设计领导Feng要求和自选的3个指数增强策略：csi300，csi500，中国1000-csi800
    # last | since 191119
    # todo 
    context={"info":"none"} 

    return render(request, 'ciss_exhi/strategy/stra_cs_index_enhance3.html', context)

@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_cs_institute_insurance_holdings(request):
    ### 保险资金重仓股跟踪和制作模拟组合
    # last | since 191121
    # todo 
    context={"info":"none"} 

    return render(request, 'ciss_exhi/strategy/stra_cs_institute_insurance_holdings_1911.html', context)

@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_ashare_bm_index_replicate(request):
    ### 主动基准类策略：指数复制
    # last | since 200329
    # todo 
    context={"info":"none"} 

    return render(request, 'ciss_exhi/strategy/stra_ashare_bm_index_replicate.html', context)



#####################################################################
### Active benchmark model  | author rC 
@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_abm_rc(request):
    # create page for single strategy 
    # last | since 190117
    context={"info":"none"}
    context["date_adjust"] = request.POST.get("date_adjust","2014-05-31")
    context["country"] = request.POST.get("country","CN")
    context["code_ind"] = request.POST.get("code_ind","999")
    context["style"] = request.POST.get("style","value")

    # port_rc181123_w_allo_value_60 for all ind1 in A shares ||  
    # port_rc181205_market_value_999  ||  port_rc181130_w_allo_growth_999
    # port_rc181227_hk_market_value_999 ; port_rc181227_us_market_value_999     
    if context["code_ind"] == "999" :
        # need to devide CN,US,HK
        if context["country"] == "cn" : 
            dir_port = "port_rc181205_market_" + context["style"] +"_999"
        elif context["country"] in ["us","hk"] :
            dir_port = "port_rc181227_"+ context["country"] + "_market_" + context["style"] +"_999"
        else :
            dir_port = "No suitable portfolio directory."
    else :
        # ind case 
        dir_port = "port_rc181123_w_allo_" + context["style"] +"_" + context["code_ind"]

    print( dir_port ) 
    ########################################################################
    ### 1, 时间选择下拉框：指数和基本面数据的更新时间
    # path | D:\CISS_db\temp
    # file | temp_df_growth_999_2014-05-31.csv
    # path = "C:\\CISS_db\\temp\\"  || aliyun
    path = "D:\\CISS_db\\temp\\"

    file_name ="temp_df_"+ context["style"] + "_"+ context["code_ind"] + "_"+ context["date_adjust"] + ".csv"
    temp_df0 = pd.read_csv( path + file_name )
    # print( temp_df0.info() )
    # Qs value of  'w_allo_value_ind2_ind3' is NaN for all, but it is normal in csv file 
    # Ans :values in temp_df0 are fine 
    # columns of VALUE :
    col_indi_value= ["code","profit_q4_es","profit_sum_ind1","ind1_pct_profit_q4_es","para_value","w_allo_value_ind1","revenue_q4_es","revenue_sum_ind1","cf_oper_q4_es","w_allo_value_ind1_ind2","code_anchor_value","profit_anchor_value","profit_dif_anchor_value","w_allo_value_ind2_ind3"]
    # columns of GROWTH :
    col_indi_growth = ["code","profit_q4_es_dif","profit_dif_sum_ind1","para_growth","w_allo_growth_ind1","revenue_q4_es_dif","cf_oper_q4_es_dif","profit_q4_es_dif_pct","revenue_q4_es_dif_pct","cf_oper_q4_es_dif_pct","w_allo_growth_ind1_ind2","w_allo_growth_ind2_ind3","","code_anchor_growth","profit_anchor_growth","profit_dif_anchor_growth"]
    if context["style"] == "value" :
        temp_df = temp_df0.loc[:, col_indi_value ]
        temp_df = temp_df.sort_values(["profit_q4_es"],ascending =False  )

    elif context["style"] == "growth" :
        temp_df = temp_df0.loc[:, col_indi_growth ]
        temp_df = temp_df.sort_values(["profit_q4_es_dif"],ascending =False  )
    ### reset disgit precision
    print( path + file_name )
    print( temp_df.info() )
    if context["style"] == "value" :
        # for 10e8 values 
        for temp_col in ["profit_q4_es","profit_sum_ind1","revenue_q4_es","revenue_sum_ind1","profit_anchor_value","profit_dif_anchor_value"] :
            temp_df[temp_col] = (temp_df[temp_col]/100000000).round(decimals=2)

        # Percentage numbers 
        for temp_col in ["ind1_pct_profit_q4_es","para_value", "w_allo_value_ind1","w_allo_value_ind1_ind2","w_allo_value_ind2_ind3"] : 
            temp_df[temp_col] = (temp_df[temp_col]*100).round(decimals=2)
    elif context["style"] == "growth" :
        # for 10e8 values 
        for temp_col in ["profit_q4_es_dif","profit_dif_sum_ind1","revenue_q4_es_dif","cf_oper_q4_es_dif","profit_anchor_growth","profit_dif_anchor_growth"] :
            temp_df[temp_col] = (temp_df[temp_col]/100000000).round(decimals=2)

        # Percentage numbers 
        for temp_col in ["para_growth","w_allo_growth_ind1","profit_q4_es_dif_pct","revenue_q4_es_dif_pct", "cf_oper_q4_es_dif_pct","w_allo_growth_ind1_ind2","w_allo_growth_ind2_ind3" ] : 
            temp_df[temp_col] = (temp_df[temp_col]*100).round(decimals=2)

    ### get name of code and name of ind1 for given code 

    from CISS_rc.bin.portfolio_statistics import port_stats
    port_stats1 = port_stats()
    temp_df = port_stats1.stra_info_ind( temp_df.head(20) )

    # print("temp_df  ")
    # print( temp_df.info(20) )
    context["funda_indicators"] = temp_df.head(20).reset_index().T


    return render(request, 'ciss_exhi/strategy/stra_abm_rc.html', context)


#####################################################################
### Individual bond strategy | Author: Jiang
# 'strategy/stra_abm.html', views.stra_abm )
# 'strategy/stra_bond_jny.html', views.bond_jny )
# 'strategy/stra_multi_cryjny.html', views.multi_cryjny )
@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_bond_jny(request):
    # create page for single strategy   
    # last | since 190115 
    context={"info":"none"}
    ### step 1 get input items
    print( request.POST.get("path_base") )
    #####################################################################
    ### step 1 Get input parameters
    stra_name = request.POST.get("stra_name","bond_pct_ny")

    benchmark = request.POST.get("benchmark","CBA00203.CS")
    # date_init = request.POST.get("date_init","20130101")
    date_init = request.POST.get("date_init","20180101")
    date_last = request.POST.get("date_last","20190103")
    # 组合调整频率|1-250天 ;  组合权重分档|1~30%:
    # frequency_d = int(request.POST.get("frequency_d","5") )
    # weight_diff = int( request.POST.get("date_last","5") )/100
    weight_c_min = float( request.POST.get("weight_c_min","5") )/100


    #####################################################################
    ###  Get symbols
    symbol_a = request.POST.get("symbol_a","CBA05203.CS")
    symbol_b = request.POST.get("symbol_b","CBA04233.CS")
    symbol_c = request.POST.get("symbol_c","CBA02203.CS")

    class2_a = request.POST.get("class2_a","代偿期限")
    yrs_range_a = request.POST.get("yrs_range_a","total_value")
    name_short_a= request.POST.get("name_short_a","7-10年国开行债券全价")

    class2_b = request.POST.get("class2_b","信用等级")
    yrs_range_b = request.POST.get("yrs_range_b","yrs_3_5")
    name_short_b= request.POST.get("name_short_b","企业债AAA全价")

    class2_c = request.POST.get("class2_c","现金工具")
    yrs_range_c = request.POST.get("yrs_range_c","yrs_0_1")
    name_short_c= request.POST.get("name_short_c","货币市场基金可投资债券")

    context['benchmark'] = benchmark
    context['name_short_a'] =name_short_a
    context['symbol_a'] = symbol_a
    context['class_2_a'] =class2_a
    context['yrs_range_a'] = yrs_range_a

    context['name_short_b'] =name_short_b
    context['symbol_b'] = symbol_b
    context['class_2_b'] =class2_b
    context['yrs_range_b'] = yrs_range_b
    context['name_short_c'] =name_short_c
    context['symbol_c'] = symbol_c
    context['class_2_c'] =class2_c
    context['yrs_range_c'] = yrs_range_c   


    path_index_1to1 = "C:\\zd_zxjtzq\\\\ciss_web\\static\\"
    # ALIYUN | path_index_1to1 =  "C:\\ciss_web\\static\\"
    file_index_1to1 = "chinabond_index_1to1.csv"
    df_1to1 = pd.read_csv(path_index_1to1 + file_index_1to1,encoding = "gbk" )
    # print("df_1to1 ",class2_a ,yrs_range_a , name_short_a )
    # net price symbol for benchmark index 
    temp_df = df_1to1[ df_1to1["symbol_full_price"]== benchmark ]
    symbol_ben_net = temp_df["symbol_net_price"].values[0]

    temp_df = df_1to1[ df_1to1["name_short"]== name_short_a]
    temp_df = temp_df[ temp_df["class_yrs"]== yrs_range_a]
    temp_df = temp_df[ temp_df["class2"]== class2_a]
    symbol_a_full = temp_df["symbol_full_price"].values[0]
    symbol_a_net = temp_df["symbol_net_price"].values[0] 
    temp_df = df_1to1[ df_1to1["name_short"]== name_short_b]
    temp_df = temp_df[ temp_df["class_yrs"]== yrs_range_b] 
    temp_df = temp_df[ temp_df["class2"]== class2_b] 
    symbol_b_full = temp_df["symbol_full_price"].values[0]
    symbol_b_net = temp_df["symbol_net_price"].values[0]

    temp_df = df_1to1[ df_1to1["name_short"]== name_short_c]
    temp_df = temp_df[ temp_df["class_yrs"]== yrs_range_c]
    temp_df = temp_df[ temp_df["class2"]== class2_c]
    symbol_c_full = temp_df["symbol_full_price"].values[0]
    symbol_c_net = temp_df["symbol_net_price"].values[0]

    #############################################################
    ### TODO CHANGE YTM TO NET_PRICE 


    ### step 2 import index historical data 
    ### full price ,df=df_full
    # ALIYUUN | path_out = "C:\\db_wind\\index\\"
    path_out = "D:\\db_wind\\index\\"
    df= pd.read_csv(path_out + benchmark + ".csv"  )
    df.index = df["Unnamed: 0"]
    df= df.drop(["Unnamed: 0"] ,axis=1 )
    ### notes:there mey be some different date information
    print( "symbol_a_full ",symbol_a_full )
    df1= pd.read_csv(path_out + symbol_a_full + ".csv"  )
    df1.index = df1["Unnamed: 0"]
    df1= df1.drop(["Unnamed: 0"] ,axis=1 )
    df= pd.concat([df,df1],1) 
    df1= pd.read_csv(path_out + symbol_b_full + ".csv"  )
    df1.index = df1["Unnamed: 0"]
    df1= df1.drop(["Unnamed: 0"] ,axis=1 )
    df= pd.concat([df,df1],1)
    df1= pd.read_csv(path_out + symbol_c_full + ".csv"  )
    df1.index = df1["Unnamed: 0"]
    df1= df1.drop(["Unnamed: 0"] ,axis=1 )
    df= pd.concat([df,df1],1)
    ### net price 

    df_net= pd.read_csv(path_out + symbol_a_net + ".csv"  )
    df_net.index = df_net["Unnamed: 0"]
    df_net= df_net.drop(["Unnamed: 0"] ,axis=1 )
    df1= pd.read_csv(path_out + symbol_b_net + ".csv"  )
    df1.index = df1["Unnamed: 0"]
    df1= df1.drop(["Unnamed: 0"] ,axis=1 )
    df_net= pd.concat([df_net,df1],1)
    df1= pd.read_csv(path_out + symbol_c_full + ".csv"  )
    df1.index = df1["Unnamed: 0"]
    df1= df1.drop(["Unnamed: 0"] ,axis=1 )
    df_net= pd.concat([df_net,df1],1)

    df1= pd.read_csv(path_out +  symbol_ben_net + ".csv"  )
    df1.index = df1["Unnamed: 0"]
    df1= df1.drop(["Unnamed: 0"] ,axis=1 )
    df_net= pd.concat([df_net,df1],1)

    ### new index will be 1,2,3..., old index will become a new index 
    df["datetime"] = pd.to_datetime(df.index , format='%Y-%m-%d')
    df  = df.reset_index()
    # we cannot drop previous data now since we need historical data for algorithm
    df_full= df[ df["datetime"]>= date_init ]
    df_full= df_full[ df_full["datetime"]<= date_last ]
    df_full["date"]= df_full["Unnamed: 0"]
    df_full =  df_full.drop(["Unnamed: 0"] ,axis=1 )

    df_net["datetime"] = pd.to_datetime(df_net.index , format='%Y-%m-%d')
    df_net  = df_net.reset_index()
    # we cannot drop previous data now since we need historical data for algorithm
    df_net= df_net[ df_net["datetime"]>= date_init ]
    df_net= df_net[ df_net["datetime"]<= date_last ]
    # print( df_net.head() )
    # df_net["date"]= df_net["Unnamed: 0"]
    # df_net =  df_net.drop(["Unnamed: 0"] ,axis=1 )
    # symbol_list = ["CBA00203.CS","CBA05203.CS","CBA04233.CS","CBA02203.CS"]
    symbol_list = [symbol_a_full,symbol_b_full,symbol_c_full,benchmark]
    ### step 3 calculate percentage data and allocation weights
    for temp_i in df_full.index :
        # note 用截止前一天的数据！~
        temp_df = df.loc[:temp_i-1,:]
        # calculate percentage of ytm at 
        for symbol in symbol_list :
            temp_ytm = df.loc[temp_i, symbol+"_ytm" ]
            temp_weight = temp_df[ temp_df[symbol+"_ytm"]>temp_ytm ].count()[symbol+"_ytm"]/temp_i
            df_full.loc[temp_i, symbol+"_ytm_pct" ] = temp_weight
        
        # working on 3 index  
        weight_a = min(1-weight_c_min,df_full.loc[temp_i, symbol_a +"_ytm_pct" ])
        weight_b = min(1-weight_c_min-weight_a ,df_full.loc[temp_i, symbol_b +"_ytm_pct" ]-0.5 )
        weight_b = max(weight_b,0)
        weight_c = 1-weight_a - weight_b 
        df_full.loc[temp_i, symbol_a +"_weight" ] = weight_a
        df_full.loc[temp_i, symbol_b +"_weight" ] = weight_b
        df_full.loc[temp_i, symbol_c +"_weight" ] = weight_c
    


    ### step 4 simulate portfolio unit 
    df_full[symbol_a +"_ret" ] = df_full[symbol_a +"_close" ].pct_change()
    df_full[symbol_b +"_ret" ] = df_full[symbol_b +"_close"].pct_change()
    df_full[symbol_c +"_ret" ] = df_full[symbol_c +"_close"].pct_change()
    df_full[benchmark +"_ret" ] = df_full[benchmark +"_close"].pct_change()
    # replace NaN to 0.0 for the first row 
    index0 = df_full[symbol_a +"_ret" ].index[0]
    df_full.loc[index0,symbol_a +"_ret" ] = 0.0
    df_full.loc[index0,symbol_b +"_ret" ] = 0.0
    df_full.loc[index0,symbol_c +"_ret" ] = 0.0
    df_full.loc[index0,benchmark +"_ret" ] = 0.0

    ### step 4 simulate portfolio unit 
    df_full[symbol_a +"_w_ret" ] = df_full[symbol_a +"_ret" ]*df_full[symbol_a +"_weight" ]
    df_full[symbol_b +"_w_ret" ] = df_full[symbol_b+"_ret" ]*df_full[symbol_b +"_weight" ]
    df_full[symbol_c +"_w_ret" ] = df_full[symbol_c +"_ret" ]*df_full[symbol_c +"_weight" ]
    df_full["port_ret" ] = df_full[symbol_a +"_w_ret" ]+df_full[symbol_b +"_w_ret" ]+df_full[symbol_c +"_w_ret" ]
    df_full["port_unit" ] = ( df_full["port_ret" ] +1).cumprod()
    ### step 1
    print("head of df ")
    # print( df_full.describe() )
    # print( df_full.head() )
    # print( df_full.tail() )

    
    df_full.to_csv( path_out+"jny.csv" )

    #################################################################
    ### Output to html 
    ### 指数描述统计| Descriptive Statistics<
    df_describe = df_full.loc[:,[symbol_a+"_ytm",symbol_b+"_ytm",symbol_c+"_ytm",benchmark+"_ytm"] ].describe()
    df_describe.columns = [symbol_a ,symbol_b ,symbol_c ,benchmark ]
    df_describe.index = ["count","mean","std","min","pct_25","pct_50","pct_75","max"]
    # round to 3 digits  ???
    df_describe = df_describe.T
    df_describe["count"] = df_describe["count"].astype(int)
    for temp_col in ["mean","std","min","pct_25","pct_50","pct_75","max"] :
        df_describe[temp_col] = df_describe[temp_col].round(decimals=3)
    df_describe = df_describe.T
    context['ytm_describe']= df_describe.to_dict() 

    ### 分位数和权重描述统计| Descriptive Statistics<
    df_des2 = df_full.loc[:,[symbol_a+"_ytm_pct",symbol_b+"_ytm_pct",symbol_c+"_ytm_pct",symbol_a+"_weight",symbol_b+"_weight",symbol_c+"_weight"] ].describe()
    df_des2.columns = ["a"+"_ytm_pct","b"+"_ytm_pct","c"+"_ytm_pct","a"+"_weight","b"+"_weight","c"+"_weight" ]
    df_des2.index = ["count","mean","std","min","pct_25","pct_50","pct_75","max"]
    # round to 3 digits 
    df_des2 = df_des2.T
    df_des2["count"] = df_des2["count"].astype(int)
    for temp_col in ["mean","std","min","pct_25","pct_50","pct_75","max"] :
        df_des2[temp_col] = (df_des2[temp_col]*100).round(decimals=2)
    df_des2 = df_des2.T
    context['ytm_describe2']= df_des2.to_dict() 

    ### 对净值画图，最大回撤计算和画图，3个成分的收益率贡献占比
    # df_full.T.to_dict()  对应 1499,1500，...
    # df_full.to_dict()   对应 port_unit 等
    # port_unit_list = list( df_full['port_unit'].round(decimals=3) )
    # context['port_unit'] = port_unit_list
    ### sub step 1 df to json 
    # path4json = "C:\\zd_zxjtzq\\\\ciss_web\\static\\templates\\ciss_exhi\\strategy\\data\\"
    # df_full2 = df_full.loc[:,['date','port_unit'] ]
    # df_full2.T.to_json(path4json+"jny.json" ,orient="columns")

    # context['date_unit']= df_full.loc[:,['date','port_unit'] ]
    context['dates']= list(df_full['date' ] )
    context['units']= list(df_full['port_unit'].round(decimals=4) )
    mdd_list = [0.0]
    for temp_i in df_full.index :
        if temp_i >0 :
            temp_mdd = df_full.loc[temp_i,'port_unit']/df_full.loc[:temp_i,'port_unit'].max()-1
            mdd_list = mdd_list + [ round(min(mdd_list[-1], temp_mdd*100 ),1)  ]
    context['mdds'] = mdd_list

    context['stra_name'] = stra_name

    df_full_unit = df_full.loc[:,["date","port_unit"] ]
    df_full_unit["port_unit" ] = df_full_unit[ "port_unit" ].round(decimals=3) 
    index_0 = df_full.index[0]
    for temp_col in [benchmark,symbol_a,symbol_b,symbol_c] :
        df_full_unit[temp_col ] = df_full[temp_col +"_close"]/df_full.loc[index_0,temp_col +"_close"]
        df_full_unit[temp_col ] = df_full_unit[temp_col ].round(decimals=3) 
    
    df_full_unit.columns= ['date',"strategy","benchmark","symbol_a","symbol_b","symbol_c"  ]
    context['units_last'] = df_full_unit.tail(10).T.to_dict()

    ####################################################################
    ### import list of commonly used index 
    # ALIYUUN |  path_index = "C:\\ciss_web\\static\\"
    path_index = "C:\\zd_zxjtzq\\\\ciss_web\\static\\"
    file_index_s = "chinabond_name_short.csv"
    temp_df =pd.read_csv( path_index + file_index_s,encoding="gbk" )
    # name_short
    # 0          0-3个月国债全价 | 1      10-20年国开行债券全价
    # print("temp_df 190121 1021 ")
    print( temp_df.head() )
    context['cb_name_short'] = temp_df.T.to_dict() 



log_index
    return render(request, 'ciss_exhi/strategy/stra_bond_jny.html', context)



#####################################################################
### ETF | Author: r.Cheng 
@csrf_protect
@requires_csrf_token
@csrf_exempt
def etf_data(request):
    # create page for single strategy   
    # last | since 19723
    # 返回查询的结果。
    context={"info":"none"}

    ### step 1 get input items
    print( request.POST.get("path_base") )


    ##############################################################################
    ### 
    import json
    import pandas as pd 
    import numpy as np 
    import math

    import sys
    print(sys.path)
    sys.path.append("C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\black_litterman\\")
    # sys.path.append("C:\\zd_zxjtzq\\\\ciss_web\\CISS_rc\\apps\\black_litterman\\")
    from etf.engine_etf import ETF_manage
    etf_manage0 = ETF_manage()

    ##############################################################################
    ### Import PCF file 
    # path_etf = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\apps\\black_litterman\\etf\\"
    path_etf = "D:\\CISS_db\\etf\\"

    #####################################################################
    ### step 1 Get input parameters

    name_etf = request.POST.get("name_etf","510300")
    date_init= request.POST.get("date_init","0724")
    date_init_pre= request.POST.get("date_init_pre","0723")

    # name_etf = "510300"
    # date_init = "0723"

    df_head,df_stocks = etf_manage0.get_pcf_file(date_init,name_etf,path_etf )
    df_head.index = df_head.key
    print("df_head ", df_head)
    print( df_head.loc["TradingDay","value"] )
    print("Head of df_stocks \n", df_stocks.head() )

    context["df_head"]= df_head
    

    ### 
    ##################################################################
    ### 获取近期分红送配数据，存入现有的CSV文件
    # get_wind.py

    ### Read existing file 
    # path to save dividend and share_proportion
    file_path0 = "D:\\CISS_db\\bonus\\"
    file_name_800 = "Wind_csi800_bonus.csv"
    file_name_1000 = "Wind_csi1000_bonus.csv"

    list1= [ ["csi800","csi1000"],["a00103020a000000","1000012163000000"],[file_name_800,file_name_1000] ]
    print("list \n", list1 )

    ### Generate parameter for wind api 
    '''
    w.wset("bonus","orderby=股权登记日;startdate=2019-07-23;enddate=2019-07-23;sectorid=a00103020a000000")
    '''
    # date_start = input( "Starting date for 实施公告日: e.g.190724..." )
    # date_end   = input( "Ending date for 实施公告日: e.g.190724..." )
    date_start = "19"+date_init  # "190723"
    date_end   = "19"+date_init  # "190724"
    import datetime as dt 
    date_start_dt = dt.datetime.strptime("20"+date_start,"%Y%m%d")
    date_start =dt.datetime.strftime( dt.datetime.strptime("20"+date_start,"%Y%m%d"),"%Y-%m-%d")
    date_end   =dt.datetime.strftime( dt.datetime.strptime("20"+date_end,"%Y%m%d"),"%Y-%m-%d")

    file_path0 
    file_name =  "Wind_csi800_bonus.csv"
    df0 = pd.read_csv(file_path0+file_name ,encoding="gbk")

    ### 分红送配信息按股权登记日排序
    df0["date"] = pd.to_datetime( df0["shareregister_date"],format="%Y-%m-%d")
    df0 = df0[ df0["date"]>=date_start_dt  ]
    df0  = df0.sort_values(by="date" )
    ### 设置输出的现实格式 
    df0["date_register"] =df0["date"].apply(lambda x: dt.datetime.strftime(x ,"%Y-%m-%d")   )

    df0["share_benchmark"] =df0["share_benchmark"].apply(lambda x: round( float( x ),2) )

    df0["share_benchmark_date"] =df0["share_benchmark_date"].apply(lambda x: x[:10] )
    df0["redchips_listing_date"] =df0["redchips_listing_date"].apply(lambda x: str(x)[:10] )  

    print( df0.columns )
    context["bonus_info"]= df0.T

    datetime0 = date_end + " 00:00:00"
    # "2019-07-23 00:00:00" column最后2列的值是一样的
    ### return 
    register_date =  df0[ df0["shareregister_date" ] == datetime0 ]
    print(register_date.head(3)  )
    print("股权登记日",datetime0 )
    # print( register_date.columns)
    print( register_date.loc[:,["wind_code","sec_name","scheme_des"] ] ) 

    for temp_index in register_date.index :
        wind_code = register_date.loc[temp_index,"wind_code" ]
        register_date.loc[temp_index,"code_raw" ] =wind_code[:6]

    df_stocks.index = df_stocks["code"]
    ### Set precision 设置小数点，设置精度 |notes:不能直接转换的原因是str格式下，有空格
    ### 由于有 nan的存在，需要先替代为 0.0 

    df_stocks['premium_pct' ] =df_stocks['premium_pct' ].str.replace(" ","") 
    df_stocks['amount' ] =df_stocks['amount' ].str.replace(" ","") 
    

    for temp_i in df_stocks.index : 
        if df_stocks.loc[temp_i,'mark' ] == "3" :
            df_stocks.loc[temp_i,'mark' ] = "深市退补"
        elif df_stocks.loc[temp_i,'mark' ] == "1" :
            df_stocks.loc[temp_i,'mark' ] = "允许"
        elif df_stocks.loc[temp_i,'mark' ] == "2" :
            df_stocks.loc[temp_i,'mark' ] = "必须"


        if len( df_stocks.loc[temp_i,'premium_pct' ] )<1  :
            df_stocks.loc[temp_i,'premium_pct' ] = "-"
        else :
            df_stocks.loc[temp_i,'premium_pct' ] = round( float( df_stocks.loc[temp_i,'premium_pct' ] ),2)
               
        if len( df_stocks.loc[temp_i,'amount' ] )<1  :
            df_stocks.loc[temp_i,'amount' ] = "-"
        else :
            df_stocks.loc[temp_i,'amount' ] = round( float( df_stocks.loc[temp_i,'amount' ] ), 1)

    print("6666======================")
    list_code = list(register_date["code_raw"]) 

    df_stocks2 =  df_stocks.loc[list_code,:] 
    # axis=0 means delete by rows
    df_stocks2 = df_stocks2.dropna( axis=0 )

    import numpy as np 
    for temp_i in df_stocks2.index :
        temp_code =  df_stocks2.loc[temp_i, "code" ]

        temp_i2 = register_date[ register_date["code_raw"] == temp_code ].index[0] 
        # df_stocks2.loc[temp_i, "scheme_des" ] =0
        
        df_stocks2.loc[temp_i, "scheme_des" ] =register_date.loc[temp_i2, "scheme_des"]

        df_stocks2.loc[temp_i, "cash_per_share" ] =  round( float( register_date.loc[temp_i2, "dividendsper_share_aftertax"] )  ,3)

        ### 计算分红的现金差额数据 | 有可能出现100股送 9.7的情况
        df_stocks2.loc[temp_i, "cash_diff" ] =round( df_stocks2.loc[temp_i, "cash_per_share" ]*float( df_stocks2.loc[temp_i, "num" ]),1 ) 

        ### 送股,float
        df_stocks2.loc[temp_i, "share_div" ] =register_date.loc[temp_i2, "sharedividends_proportion"]

        ### notes:由于 nan 本身无法被 np.NaN 识别，因此需要先用 -1 替代，
        # if not df_stocks2.loc[temp_i, "share_div" ] == np.NaN :
        if df_stocks2.loc[temp_i, "share_div" ]>0 :
            df_stocks2.loc[temp_i, "num_new" ] = df_stocks2.loc[temp_i, "num"] *(1.0 + df_stocks2.loc[temp_i, "share_div" ] ) 
        else :
            df_stocks2.loc[temp_i, "num_new" ] =df_stocks2.loc[temp_i, "num" ] 
        ### 转增股,float
        df_stocks2.loc[temp_i, "share_increase" ] =register_date.loc[temp_i2, "shareincrease_proportion"]
        # if not df_stocks2.loc[temp_i, "share_div" ] == np.NaN :
        if df_stocks2.loc[temp_i, "share_div" ]>0 :
            df_stocks2.loc[temp_i, "num_new" ] = df_stocks2.loc[temp_i, "num"] *(1.0 + df_stocks2.loc[temp_i, "share_increase" ] )
        else :
            df_stocks2.loc[temp_i, "num_new" ] =df_stocks2.loc[temp_i, "num" ] 

        df_stocks2.loc[temp_i, "date_announce" ] =register_date.loc[temp_i2, "dividends_announce_date"][:10]
        df_stocks2.loc[temp_i, "date_register" ] =register_date.loc[temp_i2, "shareregister_date"][:10]
        df_stocks2.loc[temp_i, "date_share_pay" ] =register_date.loc[temp_i2, "exrights_exdividend_date"][:10]
        df_stocks2.loc[temp_i, "date_cash_pay" ] =register_date.loc[temp_i2, "dividend_payment_date"][:10]

    # qs:不知道为什么没用
    # decimals = pd.Series([3], index=['cash_per_share' ])
    # df_stocks2.round(decimals) 

    context["df_stocks2"]=df_stocks2.T

    context["df_stocks"]=df_stocks[ df_stocks["mark"]=="必须" ].T



    return render(request, 'ciss_exhi/etf/etf_data.html', context)
 






#####################################################################
### Index of portfolio list 
def port_index(request):
    #####################################################################
    ### define latest strategy list  
    latest_port_list = Portfolio.objects.order_by('-port_date_last')[:5]
    ### todo define core strategy list 
    core_port_list = Portfolio.objects.filter(port_supervisor='rc')
    # type of context is  <class 'dict'>
    context = {'latest_port_list': latest_port_list}
    context["core_port_list"]= core_port_list

    #####################################################################
    ### Get latest portfolio infomation and assign it to context["port_suites"]
    # derived from  CISS_rc\    
    ### step 1 get basic portfolio info
    
    # i=2
    # temp_port = context['latest_port_list'][i]
    # path_base1 = temp_port.port_path  
    # port_name1= temp_port.port_name
    # port_id =  temp_port.port_id  #  1544021284
    # port_config={}
    # ### step 2 import module of CISS_rc 
    # import json
    # import pandas as pd 
    # # pd.set_option('precision', 3)    # n是要显示的精度，应该是一个整数
    # import sys
    # sys.path.append("..") 
    # ### "from ..config" 表示从上一级文件夹内的config文件夹读取模块
    # from CISS_rc.db.ports import manage_portfolios
    # port_obj = manage_portfolios(path_base1, port_config,port_name1,port_id)
    # print("=======================")
    # print("portfolio object ")
    # # print( port_obj.port_head  )
    # ### Get account info from portfolio suites
    # # Qs date_LastUpdate in port_obj.port_head is wrong~
    # date_LastUpdate = "20181105" #  "20181105"

    ########################################################################
    ### Asum | Account Sum
    ## id_account_1543199012_port_rc181123_w_allo_value_15_Asum_20180531
    
    #  asum_name =  "id_account_"+port_id +"_"+ port_name1 + "_Asum_"+ date_LastUpdate +'.csv'
    #  import pandas as pd 
    #  df_asum0 =pd.read_csv(path_base1 + port_name1 +'\\accounts\\'+asum_name  )
    #  ### Data visuallization 
    #  df_asum= df_asum0.loc[:,['cash',"total_cost","market_value","total","unit","mdd"] ]
    #  df_asum.index = df_asum0["date"]
    #  for temp_col in ['cash',"total_cost","market_value","total"  ] :
    #      df_asum[temp_col]=(df_asum[temp_col]/10000).round(decimals=2)

    #  df_asum["unit"]=df_asum["unit"].round(decimals=3)
    #  df_asum["mdd"]=(df_asum["mdd"]*100).round(decimals=2)
    #  # load_portfolio_suites(self,date_LastUpdate ,config_IO_0,port_head,port_df,port_name,sp_name0)

    #  ### step 2 assign asum to context 
    #  context['latest_asum_list']={}
    #  # df_asum.tail().to_dict() |dict可以传到html里，要用
    #  #   {% for asum,bsum in latest_asum.items %}

    #  # df.to_json | '{"cash":{"0":1,"1":7},"b2":{"0":3," 
    #  # df.to_dict | {'a1': {0: 1, 1: 7}, 'b2': {0: 3, 1: 4}, 'c3': {0: 5, 1: 2}}
    #  # Qs: context['latest_asum'][i] 在网页中asum=2 for asum in  latest_asum_list
    #  context['latest_asum'] = df_asum.tail().T.to_dict()


    #  ########################################################################
    #  ### AS | Account Stocks
    # # Qs try | (df.to_html(float_format='{0:.10f}'.format))
    #  as_name =  "id_account_"+port_id +"_"+ port_name1 + "_AS_"+ date_LastUpdate +'.csv'

    #  df_as0 =pd.read_csv(path_base1 + port_name1 +'\\accounts\\'+as_name  )
    #  # not worked yet
    #  # df_as_html = df_as0.to_html( )
    #  # context['latest_as_html']

    #  # num   ave_cost    last_quote  total_cost  market_value    pnl pnl_pct w_real  w_optimal   date_update date_in code    currency    market
    #  columns = ["num","ave_cost","last_quote","market_value","pnl","pnl_pct","w_real","date_update","code","currency","market" ]
    #  df_as = df_as0.loc[:,columns]
    #  for temp_col in ["num","market_value","pnl" ] :
    #      df_as[temp_col]=(df_as[temp_col]/10000).round(decimals=2)
    #  for temp_col in ["ave_cost","last_quote","pnl" ] :
    #      df_as[temp_col]=(df_as[temp_col]).round(decimals=2)
    #  for temp_col in ["pnl_pct","w_real" ] :
    #      df_as[temp_col]=(df_as[temp_col]*100).round(decimals=2)
    #  df_as = df_as.sort_values(["market_value"],ascending=False)

    #  context['latest_as'] = df_as.head(10).T.to_dict()

    #  print("=============================="  )
    #  print( type(context['latest_as'] ) )
    # print( context['latest_as']   )


    return render(request, 'ciss_exhi/portfolio/index_port.html', context)

#####################################################################
### Index of single portfolio  
@csrf_protect
@requires_csrf_token
@csrf_exempt
def port_single(request):
    ### !!!! 不能先处理还没输入的数据？？？？？？？？？
    print( request.POST.get("path_base") )
    #####################################################################
    ### define head info of strategy
    # path_base_in = request.GET["path_base"]
    # source https://stackoverflow.com/questions/27972606/how-can-i-fix-django-error-multivaluedictkeyerror
    # todo 
    # from .forms import form_port
    # temp_form = for_port( request.POST )
    # if temp_form.is_valid():# 如果提交的数据合法
    #     path_base= temp_form.cleaned_data['path_base']stra_bond_jny
    #     port_name = temp_form.cleaned_data['port_name'] 


    path_base = request.POST.get("path_base","D:\\CISS_db\\")
    port_name = request.POST.get("port_name","port_rc181205_market_value_999")
    port_id = request.POST.get("port_id","1544021284")
    date_LastUpdate = request.POST.get("date_last","20181105")

    # path_base_in = request.POST.get("path_base","D:\\CISS_db\\")
    # port_name_in = request.POST.get("port_name","port_rc181205_market_value_999")
    # port_id_in = request.POST.get("port_id","1544021284")
    # date_LastUpdate_in = request.POST.get("date_last","20181105")

    # form = AuthenticationForm(data=request.POST)
    # if form.is_valid()
    context={"info":"No info yet."}
    ########################################################################
    ### ACCOUNT
    if path_base:
        # not noneType
        from CISS_rc.bin.portfolio_statistics import port_stats
        port_stats1 = port_stats()
        ########################################################################
        ### Asum | Account Sum
        ## id_account_in543199012_port_rc181123_w_allo_value_15_Asum_20180531
        asum_name =  "id_account_"+port_id +"_"+ port_name + "_Asum_"+ date_LastUpdate +'.csv'
        import pandas as pd 
        df_asum0 =pd.read_csv(path_base + port_name +'\\accounts\\'+asum_name  )
        ### Data visuallization 
        df_asum= df_asum0.loc[:,['cash',"total_cost","market_value","total","unit","mdd"] ]
        
        df_asum["date"] = df_asum0["date"]
        df_asum.index = df_asum0["date"]

        df_asum =df_asum[df_asum["unit"]>0 ]
        for temp_col in ['cash',"total_cost","market_value","total"  ] :
            df_asum[temp_col]=(df_asum[temp_col]/10000).round(decimals=2)

        df_asum["unit"]=df_asum["unit"].round(decimals=3)
        df_asum["mdd"]=(df_asum["mdd"]*100).round(decimals=2)
        # load_portfolio_suites(self,date_LastUpdate ,config_IO_0,port_head,port_df,port_name,sp_name0)
        
        ### step 2 assign asum to context  
        # df.to_json | '{"cash":{"0":1,"1":7},"b2":{"0":3," 
        # df.to_dict | {'a1': {0: 1, 1: 7}, 'b2': {0: 3, 1: 4}, 'c3': {0: 5, 1: 2}}
        # Qs: context['latest_asum'][i] 在网页中asum=2 for asum in  latest_asum_list
        context['latest_asum']= df_asum.tail(10).T.to_dict() 
        
        # print(context['latest_asum'] )
        
        ########################################################################
        ### Asum | Account Sum return in past 12 months
        
        df_asum2 = port_stats1.account_ret_month(df_asum)
        df_asum_mon = df_asum2.tail(12).sort_index(ascending=False  )
        for temp_col in ['ret_mon',"mdd_mon"  ] :
            df_asum_mon[temp_col]=(df_asum_mon[temp_col]*100).round(decimals=2)
        context['asum_mon']=df_asum_mon.T.to_dict() 

        ########################################################################
        ### As | Account Stocks 
        as_name =  "id_account_"+port_id +"_"+ port_name + "_AS_"+ date_LastUpdate +'.csv'
        df_as0 =pd.read_csv(path_base + port_name +'\\accounts\\'+as_name  )
        # not worked yet
        # df_as_html = df_as0.to_html( )
        # context['latest_as_html']

        # num   ave_cost    last_quote  total_cost  market_value    pnl pnl_pct w_real  w_optimal   date_update date_in code    currency    market
        columns = ["num","ave_cost","last_quote","market_value","pnl","pnl_pct","w_real","date_update","code","currency","market" ]
        df_as0 = df_as0[ df_as0["num"]>0  ]
        df_as = df_as0.loc[:,columns]
        for temp_col in ["num","market_value","pnl" ] :
            df_as[temp_col]=(df_as[temp_col]/10000).round(decimals=2)
        for temp_col in ["ave_cost","last_quote","pnl" ] :
            df_as[temp_col]=(df_as[temp_col]).round(decimals=2)
        for temp_col in ["pnl_pct","w_real" ] :
            df_as[temp_col]=(df_as[temp_col]*100).round(decimals=2)
        df_as = df_as.sort_values(["market_value"],ascending=False)

        path_ind = "C:\\zd_zxjtzq\\\\ciss_web\\static\\"
        file_ind_CN = "ind_wind_CN.csv"
        dict_ind = pd.read_csv(path_ind + file_ind_CN,encoding="gbk")
        for temp_i in df_as.index :
            temp_code = df_as.loc[temp_i,"code"]
            # get CN name
            temp_df = dict_ind[ dict_ind["symbol"]==temp_code ]
            df_as.loc[temp_i,"name"] = temp_df[ "name" ].iloc[0]

        df_as["index"] = df_as.reset_index().index
        context['latest_as'] = df_as.head(10).T.to_dict() 


    else : 
        context ={'latest_asum': ""}
        context ={'asum_mon': ""}
        context ={'latest_as': ""}

    ########################################################################
    ### TRADES
    if path_base:
        # not noneType        
        from CISS_rc.bin.portfolio_statistics import port_stats
        port_stats1 = port_stats()
        ########################################################################
        ### TP | Trade Plan
        # trades_id_1543199012_port_rc181123_w_allo_value_15_TP_20180531
        # code  date_plan   date_trade_1st  method  period  quote_index_start   quote_index_end signal_pure weight_dif  total_amount    num ave_price
        tp_name =  "trades_id_"+port_id +"_"+ port_name + "_TP_"+ date_LastUpdate +'.csv'
        path0 = path_base + port_name +'\\trades\\'
        # def f() derived from ciss_rc\apps\test_stats.py
        
        tp_summ0 = port_stats1.trade_tp_monthly_sum( path0 ,tp_name)
        # Unnamed: 0  duration  quote_index_start  quote_index_end
        # signal_pure  weight_dif  total_amount         num    ave_price
        # weight_dif2  weight_dif_add  weight_dif_minus
        columns_tp_summ = ["weight_dif","total_amount","num","weight_dif2","weight_dif_add","weight_dif_minus"]
        tp_summ = tp_summ0.loc[:,columns_tp_summ ] 
        tp_summ["date"] = tp_summ.index

        for temp_col in ["weight_dif","weight_dif2","weight_dif_add","weight_dif_minus"] :
            tp_summ[temp_col]=(tp_summ[temp_col]*100 ).round(decimals=2)

        for temp_col in ["total_amount","num"] :
            tp_summ[temp_col]=(tp_summ[temp_col]/10000).round(decimals=2)

        context['port_tp_summ']= tp_summ.T.to_dict()

        ########################################################################
        ### TB | Trade Book
        ## trades_id_1543199012_port_rc181123_w_allo_value_15_TB_20180531
        # market    currency    date    symbol  BSH ave_price   number  ave_cost    fees    profit_real open    close   amount  datetime
        tb_name =  "trades_id_"+port_id +"_"+ port_name + "_TB_"+ date_LastUpdate +'.csv'

        ### TB from Monthly view 
        (tb_summ0,tb_summ1) = port_stats1.trade_tb_stat(path0 ,tb_name)
        cols_tb_summ0=["BSH","fees","profit_real","amount","amt_buy","amt_sell","num_buy","num_sell","pct_fees_profit","ave_amt_buy","ave_amt_sell","ave_profit","ave_fees"]
        tb_summ = tb_summ0.loc[:,cols_tb_summ0 ]
        tb_summ["date"] = tb_summ.index

        # avoid inf value 
        tb_summ.loc[ tb_summ.index[0],"pct_fees_profit" ] = 0.0
        temp_col = "pct_fees_profit"
        tb_summ[temp_col]=(tb_summ[temp_col]*100 ).round(decimals=2)
                
        for temp_col in ["ave_amt_buy","ave_amt_sell","ave_profit","ave_fees" ] :
            tb_summ[temp_col]=(tb_summ[temp_col] ).round(decimals=2)

        for temp_col in ["fees","profit_real","amount","amt_buy","amt_sell"] :
            tb_summ[temp_col]=(tb_summ[temp_col]/10000).round(decimals=2)

        context['port_tb_summ']= tb_summ.T.to_dict()
        
        ### TB from single stock view 
        # print("tb_summ1")
        # print(  tb_summ1.columns )
        # print(  tb_summ1.tail() )
        # ['Unnamed: 0', 'BSH', 'ave_price', 'number', 'ave_cost', 'fees',
        # 'profit_real', 'open', 'close', 'amount', 'amt_pct', 'amt_buy',
        # 'amt_sell', 'num_buy', 'num_sell', 'profit_pct']
        cols_tb_summ1= ['BSH','ave_cost',"number","fees","profit_real","amount","amt_pct"]
        cols_tb_summ1= cols_tb_summ1 +["amt_buy","amt_sell","num_buy","num_sell","profit_pct"  ]
        cols_tb_summ1= cols_tb_summ1 +["name"  ]
        tb_summ_s = tb_summ1.loc[:,cols_tb_summ1 ]
        # todotodo
        for temp_col in ['BSH' ] :
            tb_summ_s[temp_col]=(tb_summ_s[temp_col] ).round(decimals=0)

        for temp_col in ['number' ] :
            tb_summ_s[temp_col]=(tb_summ_s[temp_col]/10000 ).round(decimals=0)

        for temp_col in ['ave_cost',"amt_pct","profit_pct"  ] :
            tb_summ_s[temp_col]=(tb_summ_s[temp_col]*100 ).round(decimals=2)

        for temp_col in ["fees","profit_real","amount","amt_buy","amt_sell","num_buy","num_sell"] :
            tb_summ_s[temp_col]=(tb_summ_s[temp_col]/10000).round(decimals=2)


        tb_summ_s_posi =  tb_summ_s.head(10)
        tb_summ_s_posi["symbol"] =tb_summ_s_posi.index
        tb_summ_s_nega =  tb_summ_s.tail(10)
        tb_summ_s_nega = tb_summ_s_nega.sort_values(["profit_real"] ,ascending=True)
        tb_summ_s_nega["symbol"] =tb_summ_s_nega.index
        context['port_tb_summ_s_posi']= tb_summ_s_posi.T.to_dict()
        context['port_tb_summ_s_nega']= tb_summ_s_nega.T.to_dict()

    else : 
        context['port_tp_summ']={}
        context['port_tb_summ']={}
        context['port_tb_summ_s_posi']= {}
        context['port_tb_summ_s_nega']= {}

    ########################################################################
    ### SIGNALS
    if path_base:
        # not noneType        
        from CISS_rc.bin.portfolio_statistics import port_stats
        port_stats1 = port_stats()
        ########################################################################
        ### Signals
        #   file_name = id_signals_1544021284_port_rc181205_market_value_999
        # code  ind1_code   ind2_code   ind3_code   w_allo_value_ind1   w_allo_growth_ind1  signal_pure
        # 000001.SZ   40  4010    401010  0.007080468 0.002543602 1
        sig_name =  "id_signals_"+port_id +"_"+ port_name+"_"+ date_LastUpdate+'.csv'
        df_sig =pd.read_csv(path_base + port_name +'\\signals\\'+ sig_name  )
        
        df_sig = df_sig.sort_values("w_allo_value_ind1",ascending=False ) 
        # columns = ["code","ind1_code","ind2_code","ind3_code","w_allo_value_ind1","w_allo_growth_ind1","signal_pure"]

        df_sig_out = port_stats1.signals_info(df_sig.head(10) )
        # new column | ["name","signal_CN"，"signal_EN"，'ind_1_name', 'ind_2_name', 'ind_3_name', 'ind_4_name']
        for temp_col in ["w_allo_value_ind1","w_allo_growth_ind1" ] :
            df_sig_out[temp_col]=(df_sig_out[temp_col]*100).round(decimals=2)

        df_sig_out["index"] = df_sig_out.reset_index().index
        context['signals_out']= df_sig_out.T.to_dict()

    else : 
        context['signals_out']={}





    return render(request, 'ciss_exhi/portfolio/port_single.html', context)




#####################################################################
### Index of single strategy

###########################################################################
### source codes







###########################################################################
### working on docs files 
def docs_index(request):
    # index of ciss module 
    # 190105
    return render(request,"ciss_exhi/docs/index_ciss.html")


def docs_5min(request):
    return render(request,"ciss_exhi/docs/5min_ciss.html")

def docs_data(request):
    return render(request,"ciss_exhi/docs/data_manage.html")

def docs_esse(request):
    return render(request,"ciss_exhi/docs/esse_func.html")

def docs_multi(request):
    return render(request,"ciss_exhi/docs/multi_asset.html")

def docs_port(request):
    return render(request,"ciss_exhi/docs/port_simu.html")
def docs_stra_ana(request):
    return render(request,"ciss_exhi/docs/stra_ana.html")
def docs_stra_eval(request):
    return render(request,"ciss_exhi/docs/stra_eval.html")
def docs_web_plat(request):
    return render(request,"ciss_exhi/docs/web_plat.html")

def docs_update(request):
    return render(request,"ciss_exhi/docs/update_coop_opensource.html")

###########################################################################
### working on test files 
# def docs_index(request):
#     # Ouputting CSV with Django
#     # 190121
#     import csv
#     response = HttpResponse(content_type='text/csv')
#     response['Content-Disposition'] = 'attachment; filename="somefilename.csv"'
#     writer = csv.writer(response)
#     writer.writerow(['First row', 'Foo', 'Bar', 'Baz'])
#     writer.writerow(['Second row', 'A', 'B', 'C', '"Testing"', "Here's a quote"])



#     return response 

