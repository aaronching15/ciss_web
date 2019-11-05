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

file_path = "C:\\zd_zxjtzq\\rc_reports_cs\\机构研究_国家队\\大基金\\"
file_name_csv = "rawdata_views_bigfund.csv"
col_list = ["code","date","amount"]

df_output=fund_simu_1.weight_list_event( file_name_csv,file_path,col_list   )




#################################################################################
### Initialization 































