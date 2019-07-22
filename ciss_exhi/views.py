from django.shortcuts import render

from django.http import HttpResponse

### to exhibit static html file 
from django.shortcuts import render_to_response
###  you must use csrf_protect on any views that use the csrf_token template tag, as well as those that accept the POST data.
# source https://docs.djangoproject.com/en/2.1/ref/csrf/
# source https://blog.csdn.net/weixin_40612082/article/details/80686472
from django.views.decorators.csrf import csrf_protect,requires_csrf_token,csrf_exempt

###########################################################################
### Index and template page | 网站首页
def index(request):
    ###  网站首页
    return render_to_response("ciss_exhi/index_ciss.html")

def json_index(request):
    # 直接引用static\ 目录下的json文件
    return render_to_response("ciss_exhi/json/tree_stra_s.json")

###########################################################################
### Index of data list  | 数据清单
def data_index(request):
    #  
    return render_to_response("ciss_exhi/data/index_data.html")

def data_log(request):
    #  
    return render_to_response("ciss_exhi/data/data_log.html")
###########################################################################
### Index of strategy list 
### working on test strategy file
from .models import Strategy,Portfolio

import pandas as pd 

def stra_index(request):
    ### define latest strategy list  
    latest_stra_list = Strategy.objects.order_by('-stra_date_last')[:5]
    ### todo define core strategy list 
    core_stra_list = Strategy.objects.filter(stra_supervisor='du')
    # 上下文(context)。这个上下文是一个字典，它将模板内的变量映射为 Python 对象。
    # type of context is  <class 'dict'>
    context = {'latest_stra_list': latest_stra_list}
    context["core_stra_list"]= core_stra_list
    # render() 「载入模板，填充上下文，再返回由它生成的 HttpResponse 对象」
    return render(request, 'ciss_exhi/strategy/index_stra.html', context)

#####################################################################
### Index of single strategy
@csrf_protect
@requires_csrf_token
@csrf_exempt
def stra_single(request):
    # create page for single strategy 
    # last | since 190111
    # todo 
    context={"info":"none"}


    return render(request, 'ciss_exhi/strategy/stra_single.html', context)

#####################################################################
### working on personal strategy
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


    path_index_1to1 = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\static\\"
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
    # path4json = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\static\\templates\\ciss_exhi\\strategy\\data\\"
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
    path_index = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\static\\"
    file_index_s = "chinabond_name_short.csv"
    temp_df =pd.read_csv( path_index + file_index_s,encoding="gbk" )
    # name_short
    # 0          0-3个月国债全价 | 1      10-20年国开行债券全价
    # print("temp_df 190121 1021 ")
    print( temp_df.head() )
    context['cb_name_short'] = temp_df.T.to_dict() 




    return render(request, 'ciss_exhi/strategy/stra_bond_jny.html', context)


'''
label: {
                    backgroundColor: '#505765'
                }

line 153

//var data_rc = "{{ df_out |safe }}" ;
    
    //jQuery.parseJSON(jsonstr),可以将json字符串转换成json对象  | https://www.cnblogs.com/hgmyz/p/7451461.html
    // data_rc = $.parseJSON( data_rc )
    $.parseJSON( data_rc ).done(function (data_rc) { 
        var obj = data_rc.port_unit ;
        if ( obj.includes(1.00)) {
        console.log('red');
        }else{
        console.log('blue');
        }
        })


    <tr><td>高久期品种，如中债-7-10年国开行债券指数:<br></td>
        <td><input type="text" name="symbol_a" value="CBA05203.CS"> <br></td></tr>
    <tr><td>中久期品种，如中债-企业债AAA指数（3-5年）指数:<br></td>
        <td><input type="text" name="symbol_b" value="CBA04233.CS"><br></td></tr>
    <tr><td>低久期品种，如中债-货币市场基金可投资债券总值指数: <br></td>
        <td><input type="text" name="symbol_c" value="CBA02203.CS"><br></td></tr>








'''



















































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

        path_ind = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\static\\"
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
    return render_to_response("ciss_exhi/docs/index_ciss.html")


def docs_5min(request):
    return render_to_response("ciss_exhi/docs/5min_ciss.html")

def docs_data(request):
    return render_to_response("ciss_exhi/docs/data_manage.html")

def docs_esse(request):
    return render_to_response("ciss_exhi/docs/esse_func.html")

def docs_multi(request):
    return render_to_response("ciss_exhi/docs/multi_asset.html")

def docs_port(request):
    return render_to_response("ciss_exhi/docs/port_simu.html")
def docs_stra_ana(request):
    return render_to_response("ciss_exhi/docs/stra_ana.html")
def docs_stra_eval(request):
    return render_to_response("ciss_exhi/docs/stra_eval.html")
def docs_web_plat(request):
    return render_to_response("ciss_exhi/docs/web_plat.html")

def docs_update(request):
    return render_to_response("ciss_exhi/docs/update_coop_opensource.html")

###########################################################################
### working on test files 
def test_index(request):
    # Ouputting CSV with Django
    # 190121
    import csv
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="somefilename.csv"'
    writer = csv.writer(response)
    writer.writerow(['First row', 'Foo', 'Bar', 'Baz'])
    writer.writerow(['Second row', 'A', 'B', 'C', '"Testing"', "Here's a quote"])



    return response 