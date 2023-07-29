# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
拆基仿真研究支持组件

MENU :
### Given table name 
### Choice 1：根据分析指标ana1~anaN，以及前期权重w_pre，计算调整后权重w_adj
### Choice 2：半年报累加权重计算



功能
todo：
'''
#################################################################################
### Initialization 

from fund_simulation import fund_simu
fund_simu_1 = fund_simu("")

import pandas as pd 


#################################################################################
### Choice 1：根据分析指标ana1~anaN，以及前期权重w_pre，计算调整后权重w_adj
### format1 : columns= ["date_ann","ana1","ana2","ana3","ana4","w_pre"]

# file_path = "C:\\zd_zxjtzq\\rc_reports_cs\\行业量化_医疗保健\\"
# file_name_csv = "data_raw_w_rebal.csv"

# if_ana = 1
# if if_ana == 1 :
#     print("Use format1 : columns= [ date_ann , ana1 ana2 ana3 ana4 w_pre ]")
# else :
#     print("Adjust weight by *0.94/100; use format1 : columns= [ date_ann , w_pre ]")
# ### format2 : ["date_ann","w_pre"]
# para_ana={}
# para_ana["ana1"] = 1 
# # notes：为了避免如工商银行这样的股票盈利能力巨大影响权重，需要减少“盈利能力”的权重
# para_ana["ana2"] = 0.5
# para_ana["ana3"] = 1 
# para_ana["ana4"] = 1 

# df_out = fund_simu_1.weight_rebalance(para_ana, file_name_csv,file_path,if_ana )

# asd

#################################################################################
### Choice 2：半年报累加权重计算,并剔除持仓比例小于 0.3%的，这里对应的值应该是 0.3
'''
ana分析：参考H列ANN_DATE，通常1、7月披露季度前十大持仓后，3或4、 8月会披露半年报的全部或剩余持仓，
这会导致一定的问题需要用python进行处理。合理的做法一是统一延后调整、这样会损失时效性；
二是提前调整，之后不调整；这样会错过非权重股的机会；
三先按照top10调整仓位，之后再按照后续披露事项调整；这样做的问题是仓位角度，需要先用前十大打满仓位，
之后再根据持仓比例卖出非前十大部分持仓的权重，买入新增的股票。
info:基金半年度报告：基金管理人应当在上半年结束之日起六十日内，编制完成基金半年度,
     基金管理人应当在每年结束之日起九十日内，编制完成基金年度报告，并将年度报告正文登载于网站
逻辑：按照日期升序排列，半年报和年报可能发生的月份： 1，2，3，7，8都有可能出现，其中1，7会
    和q4、q2季度后15个交易日内披露重叠；季度调整对应的是1、4、7、10月份。
    即便间隔1个月，期间对应的一致预期数据也有可能发生调整，因此再做一次计算也是合理的。
办法：如果1、7月出现了某天更新，合计权重加上一次调整权重之和小于1.00，
    且股票单票最大权重小于上一次调整的平均权重，则判定为部分权重信息，此时进行加总计算权重。
'''

# file_path = "C:\\zd_zxjtzq\\rc_reports_cs\\行业量化_医疗保健\\"
# file_name_csv = "data_raw_w_adj_anndate.csv"

# df_output=fund_simu_1.weight_adj_anndate( file_name_csv,file_path  )

# asd

#################################################################################
### Choice 3：将时间顺序的交易记录或事件记录转化成每一期的配置权重 

# file_path = "C:\\zd_zxjtzq\\rc_reports_cs\\机构研究_国家队\\大基金\\"
# file_name_csv = "rawdata_views_bigfund.csv"
# col_list = ["code","date","amount"]

# df_output=fund_simu_1.weight_list_event( file_name_csv,file_path,col_list   )
# asd

#################################################################################
### Choice 4：给定时期，对list中不同行业的股票分别计算标准分值，并控制异常值的影响
'''
功能说明：
1，根据输入的指数成分，对于每个行业x：
2，对于行业x内每个风格y：对
3，对于风格y内每个指标z：计算标准分值，并谨慎去除异常值(最大值或三倍标准差)；加总合成y的分值
注意：要按初始的no升序排列，要不会乱
'''

### vip input
code_index = "000906.SH"

# date_list= []
# # temp_date=input("Type in date such as 20160301:   ")
# for temp_y in ["2014","2015","2016","2017","2018","2019"]:
#     for temp_m in ["0301","0601","0901","1201"] :
#         temp_date = temp_y +temp_m
        
#         temp_date = input("type in date 20150301 :")
        
#         print( temp_date  ) 
#         if temp_date not in ["20191201"] :
#             date_list= date_list +[temp_date ]
#             print("Check for code_index and temp_date"+code_index+"  "+temp_date+" :")
#             file_path = "D:\\CISS_db\\db_bl\\data\\"
#             file_path_input = "D:\\CISS_db\\db_bl\\data\\input\\"
#             # file_name_csv = "in_estimates2weights_000906.SH_20140601.csv"
#             file_name_csv = "in_estimates2weights_"+code_index +"_"+temp_date+".csv"
#             col_name = "ind"
#             ### paramater for x,y,z
#             para_w= [0.2,0.4,0.4]
#             # from x1	x2	y1	y2	z1	z2	ind，to  sum	x	y	z	

#             df_output=fund_simu_1.indicators2score_1p(code_index,temp_date,file_name_csv,file_path,col_name ,para_w ) 

#             ### 计算后验指标 P,Q,Omega from q_raw, omega_raw and df_output
#             df_output["P"] = df_output["weight_ind1"]
#             df_output["Q"] = df_output["q_raw"]
#             df_output["Omega"] = df_output["omega_raw"]

#             ### 计算先验部分:分行业的w_mkt from w_csi800 |、 ret_hist,sigma_hist在excel里有了
#             col_name = "ind"
#             df_output=fund_simu_1.weight2weight_sub2(df_output,col_name,code_index,temp_date,file_path   )

#             ### 把下一步BL计算需要的列保存到对应文件 
#             # code，ind，weight_sub=w_mkt，P=weight_ind1,Q=ret，Omega=sigma^2	                                       
#             # TO  ：code，ind	w_mkt	，  	ret	sigma
#             # in_stock_all_views_000906.SH_20140901 ||  D:\CISS_db\db_bl\data\input
#             df_views = df_output.loc[:,["code","ind"]]
#             # df_views["ind"] = df_output["ind"]
#             df_views["w_mkt"] = df_output["weight_sub"]
#             df_views["ret"] = df_output["Q"]
#             df_views["w_view"] = df_output["weight_ind1"]
#             import math

#             df_views["sigma"] = df_output["Omega"].apply(lambda x: math.sqrt(x) )
#             df_views.to_csv(file_path_input+ "in_stock_all_views_"+code_index+"_"+temp_date +".csv" ,encoding="gbk"  )
#         asd

# asd
#################################################################################
### Choice 5：根据给定行业或日期column，按细分组合计算权重和PMS格式文件
'''
功能说明：
1，
'''
### parameter
### Logic：第一名持股市值100亿的假设下，0.3%对应3千万
para_mv = 10000000.0
para_w = 0.003
file_path = "G:\\zd_zxjtzq\\rc_reports_cs\\机构研究_陆股通\\rawdata_wind_terminal\\"


tradingdate_list= ["20161230","20170331","20170630","20170929","20171229"]
tradingdate_list= tradingdate_list +["20180329","20180629","20180928","20181231"]
tradingdate_list= tradingdate_list +["20190329","20190628","20190930"]
# temp_date=input("Type in date such as 20160301:   ")
# "2014","2015","2016","2017","2018","2019"

i_df = 0 
for temp_date in tradingdate_list :
    temp_date_s = temp_date[2:]
    file_name_csv = "沪深港通持股_sh_"+ temp_date_s +".xlsx"
    temp_df = pd.read_excel(file_path+ file_name_csv,encoding="gbk"  )   

    file_name_csv = "沪深港通持股_sz_"+ temp_date_s +".xlsx"
    temp_df2 = pd.read_excel(file_path+ file_name_csv,encoding="gbk"  )
    temp_df = temp_df.append(temp_df2, ignore_index= True)
    
    temp_df = temp_df.fillna( value= -1.0) 
    temp_df = temp_df[temp_df["持股市值"]> para_mv   ]  
    temp_df["weight_raw"] = temp_df["持股市值"]/temp_df["持股市值"].sum()
    
    temp_df = temp_df.sort_values(by=["weight_raw"])
    print( temp_df.head() )
    print( temp_df.tail() )

    '''
    三种方案：
    1，总市值排名：占全部持仓市值比例大于0.3%的，201612看有50只；
        PMS=陆股通季1911v0300to1000
    2，总市值排名，选前30至80名的股票
        PMS=陆股通季1911vTop30to80
    3，和上一期比较，取差额
        PMS=陆股通季1911vTop30to80
    '''  
    ### Method 1 
    # para_w = 0.003 
    # temp_df= temp_df[ temp_df["weight_raw"] >= para_w ] 
    # name_method = "0003to1000"
    # temp_df["weight"] =  temp_df["weight_raw"]*0.95/temp_df["weight_raw"].sum()

    ### Method 2
    # 重新设置index ：1,2,3
    # temp_df = temp_df.sort_values(by=["weight_raw"],ascending=False ) 
    # temp_df= temp_df.head(80)
    # temp_df= temp_df.tail(50)
    # name_method = "w_top31to80"
    # temp_df["weight"] =  temp_df["weight_raw"]*0.95/temp_df["weight_raw"].sum()

    ### Method 3 
    name_method = "w_diff_top"
    if i_df > 0 : 
        for temp_i in temp_df.index :
            ### find if code in previous period 
            temp_code = temp_df.loc[temp_i,"证券代码" ] 
            temp_w =    temp_df.loc[temp_i,"weight_raw" ]
             
            df_check = temp_df_pre[ temp_df_pre[ "证券代码" ]== temp_code  ]
            
            if len( df_check.index ) > 0 :
                ### 方式一：比较的是相对于当期全部持仓市值的比例，这个更好地反映组合内的配置比例。
                ### 方式二：计算持仓市值的变动，这个容易忽视市场整体和个股的涨跌变动。
                # print( temp_df.loc[temp_i,"weight_raw" ]  )
                # print( df_check.loc[:,["weight_raw","weight"]] )
                
                w_diff = max(0.0, temp_w - df_check["weight_raw"].sum() )
                ### update "weight_raw" to difference weight 
                temp_df.loc[temp_i,"weight_raw" ] = w_diff
        
        ### ana:看201612 to 201703的情况，增持的股票数量超过300
        temp_df = temp_df.sort_values(by=["weight_raw"],ascending=False ) 
        # for debug usage 
        # temp_df.to_csv( file_path+ "temp.csv",encoding="gbk"  )
        # temp_df_pre.to_csv( file_path+ "temp_pre.csv",encoding="gbk"  )
        
        temp_df = temp_df[ temp_df["weight_raw"]>0.0 ] 
        temp_df= temp_df.head(100) 
        temp_df["weight"] =  temp_df["weight_raw"]/temp_df["weight_raw"].sum()
    else :
        ### 取前100只股票
        temp_df = temp_df[ temp_df["weight_raw"]>0.0 ] 
        temp_df = temp_df.sort_values(by=["weight_raw"],ascending=False )
        temp_df_pre = temp_df
        temp_df= temp_df.head(100)
        temp_df["weight"] =  temp_df["weight_raw"]*0.95/temp_df["weight_raw"].sum()
        
    ########################################################################
    ### Standarded step forming PMS templates       
    print(temp_df.head(3) ) 
    temp_df["weight"] =  100*temp_df["weight"]
    if i_df == 0 :
        temp_df2 = temp_df.loc[:,["证券代码","weight"] ]
        ### col_PMS=证券代码	持仓权重	成本价格	调整日期	证券类型
        temp_df2["成本价格"] = ""
        temp_df2["调整日期"] = temp_date
        temp_df2["证券类型"] = "股票"
        ### 修改列名
        temp_df2.rename(columns={'weight':'持仓权重'},inplace=True) 
        df_output = temp_df2 
    else :
        temp_df2 = temp_df.loc[:,["证券代码","weight"] ]
        ### col_PMS=证券代码	持仓权重	成本价格	调整日期	证券类型
        temp_df2["成本价格"] = ""
        temp_df2["调整日期"] = temp_date
        temp_df2["证券类型"] = "股票"
        temp_df2.rename(columns={'weight':'持仓权重'},inplace=True) 
        df_output = df_output.append(temp_df2 ) 
    
    i_df = i_df+1 
    print( df_output.tail(3) ) 

file_output = "沪深港通持股_shsz_"+ name_method+"_" + "1612_1909" +".csv"
df_output.to_csv( file_path+ file_output,encoding="gbk"  )

### Previous function
# col_name = "ind"
# para_w =0.1 
# df_output=fund_simu_1.weight2weight_sub(file_name_csv,file_path,col_name ,para_w )

asd

#################################################################################
### Choice 6：导入季度调整的BL行业组合权重，生成Wind可以识别的季度调整文件

# file_path = "D:\\CISS_db\\db_bl\\data\\output\\"
# file_name_csv = "w_20190901_000906.SH.csv"

# code_index="000906.SH"
# df_output = fund_simu_1.weight2wind_pms(code_index)

# asd

#################################################################################
### Choice 7：对所有行业，生成和保存 P_views的权重
import pandas as pd 
file_path_input = "D:\\CISS_db\\db_bl\\data\\output\\"

code_index = "000906.SH"

date_list= []
# temp_date=input("Type in date such as 20160301:   ")
for temp_y in ["2014","2015","2016","2017","2018","2019"]:
    for temp_m in ["0301","0601","0901","1201"] :
        temp_date = temp_y +temp_m
        # temp_date = input("type in date 20150301 :")
        print( temp_date  ) 
        ### "20140301","20140601","20191201"
        if temp_date not in ["20191201"] :
            date_list =date_list+[temp_date]

ind_list =["能源","材料","工业","可选消费","日常消费","医疗保健","金融","信息技术","电信服务","公用事业","房地产"]
for temp_ind in ind_list :
    temp_i = 0 
    for temp_date in date_list :
        ### 
        df_views =pd.read_csv(file_path_input+ "output_esti2w_"+code_index+"_"+temp_date +".csv" ,encoding="gbk"  )
        df_views = df_views[ df_views["ind"]==temp_ind ]
        temp_df= df_views.loc[:,[ "code","weight_ind1" ] ]
        temp_df["weight_ind1"] = temp_df["weight_ind1"]/temp_df["weight_ind1"].sum() *95
        
        temp_df.columns = ["证券代码","持仓权重" ]
        temp_df["调整日期"] = temp_date
        temp_df["成本价格" ] ="" 
        temp_df["证券类型"] = "股票"

        if temp_i <1 :
            df_ind= temp_df
        else :
            df_ind= df_ind.append(temp_df,ignore_index=True)
        
        temp_i=temp_i+1
    ### save to csv file 
    # path_temp= "D:\\CISS_db\\db_bl\\data\\output\\PMS_view\\"
    path_temp= "D:\\CISS_db\\db_bl\\data\\output\\191115update\\"
    file_temp = "PMS_bl_000906.SH_"+ temp_ind + "_20140901_20190901"+".csv"
    df_ind.to_csv(path_temp+file_temp,encoding="gbk"  )

asd





























