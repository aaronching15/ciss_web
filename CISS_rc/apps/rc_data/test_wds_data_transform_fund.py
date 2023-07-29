# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
todo
后续的问题：
1，对每个交易日全市场的股票保存一个单独csv
2，当前日期更新至20200107，之后如何更新数据，避免重复全部计算？

功能：
专门处理基金持仓变动数据 || 2, 基金持仓变动，

参考资料：
1，研报：基金仓位与风格估计模型及最新配置信息.pdf | 天风证券 吴先兴 20190107
2，定期数据：海通基金排名、中信基金仓位
2.1，海通基金周报里，sheet"股票型"有1400只，若K列剔除指数基金类别，则"主动股票开放型"有415，基本和wind里的460一致。
    N列海通评级5、4、3星各有34~35只，合起来是100只
------------
信息结构梳理{参考天风的方法}：
1，公开基金数据搜集和整理：基金季度和半年度持仓披露；
1.1，对每个基金建立obejct dict，用json保存重要信息；

2，持仓组合模拟和组合调整模拟；是否模拟拟合Top10以外的剩余持仓。
    方法：通过大、小盘规模指数、行业指数、行业龙头股对(月)仓位进行拟合。
3，日常仓位监控；在月以内的频率对潜在的调仓交易进行拟合。{idea：网格交易法}
4，特征提取&风格刻画：大小市值配置区别、市值暴露(应该是相对于中位数市值的成分股市值倍数)、行业配置、股票仓位

5，模型维护分析：
5.1，数据计算上，如果加入个股总成交金额约束、区间内模拟交易对股价影响，可能有助于最优化拟合的
结果更贴近。
5.2，按月计提管理费？

6，统计分析
6.1，股票基金分行业的仓位变动

------------
假设：
1，公募基金、特别是部分公墓基金管理人、期限较长的产品具备超额收益能力。
2，对于普通股票型基金和偏股混合型基金，我们剔除成立不满 6 个月的样本、规模小于 5000万的样本、存在C 或E 或O 或
 H 类的基金只保留其A 类基金，分别估计其平均仓位。 同时我们在基金池中剔除估计置信度较低的样本，利用剩余基金作为
 样本池估计各类公募 基金平均仓位。||5千万的基金在打新的角度会导致收益高于预计。

------------
算法：
notes：每年6次计算，4次针对前10重仓、2次针对半年报全部持仓。
1，例20050725得到所有当期公募基金05q2的前十大持仓，可以和05q1持仓、05q1公司十大股东、流通股东比较。
notes，有的基金非前十大重仓股却在上市公司；季度披露的行业配置是证监会行业分类，工业动辄占比50%，没有太大意义。
1.3，看该基金历史半年度持仓变动比例,分行业和个股的仓位比例变动.(以及区间内基金经理是否变更。|似乎目前只有最新的数据。)
1.3.1，提取上一季度的持仓数据和模拟持仓数据；

1.4，看市场、行业变动和仓位变动的相关性
1.4.1，市场组合选sz50, csi300，csi500，csi1000{2005年时似乎还没有csi500，csi1000}、创业板？
    notes：创业板09年才有，中证1000也是2014年之后才有；既然具备超额收益的应该是最有成长性的股票，大盘股具备成长
    性的在上证50和沪深300中可以采集到，能否直接从市场中搜集"大盘成长","小盘价值"等4个风格组合用于匹配基金的投资风格呢？
    A,每个月，计算过去100天市场总市值、流通市值、成交金额、净利润,排名前50，300，500，1000的股票共1850；
    构建rc版rc50,rc500,rc1000.

1.4.2，行业组合需要构建28个中信一级行业组合，每个行业组合由所属的二级行业组合构成。
    A，对于每个二级行业计算行业内持股，仅考虑有股票的二级行业，05年1300多家上市公司，100个二级中信行业约有13家公司
    选股方式：1，给定二级行业内，按市值加权构建组合、按净利润加权构建组合、按growth/PE构建。
    B，每个月末，计算按上月末行业分类下的当月行业分布，并统计上述组合的收益情况

1.4.3，对于特定的篮子基金，例如基金公司旗下股票型基金篮子，或者某种风格选出来的3年期金牛基金篮子：
    A,计算篮子内基金持仓股的组合变动，可以考虑等权重或按单只基金能力加权。

1.5，按照变动比例和市场、行业变动测算当前季度基金的仓位变动比例；
1.6，最优化：引入过去1个季度基金净值变动，结合仓位变动预测，拟合出误差最小的仓位变动模式
1.7，限制因素：组合调仓、行业配置偏离、个股配置偏离{成长、价值锚}。
1.7.1，组合调仓变动季度不超过40%，月度不超过15%；
1.7.2，中信28个一级单个行业配置偏离月度和季度变动不超过
1.7.3，统计历史组合变动行为，是否低买高卖等。
1.7.4，统计值：估计的仓位和实际仓位的偏离情况；

1.8，变量：
1.8.1，每月(每周)持仓变动，中信行业组合28个、中信二级行业成长和价值龙头锚109*2=218个
notes:中信一级28个，二级109个。

2，20050830可以得到2季度所有持仓，需要用新的算法调整再优化

3，问题：其实每只基金的行业配置主要看前5~10项；确定之后可以只用持仓占比较高的10个行业组合(或细分成长、价值)
对基金组合进行拟合。
3.1，像白云山这样市值、净利润等财务指标都较好，但长期缺乏重要卖方覆盖和公募基金持有。股票长期走势温和上涨甚至小幅优于
指数，但大部分实际投资者体验不佳特别是市场好的时候会被忽视。这种股票应该在市场震荡下行时配置，可以获得超额收益。
Wind行业分类里看起来是医药股，但实际上却是饮料股。这个问题可以通过主观修订的方式来调。另外，中信曾经将其改为饮料但后来
又改回了医药股。

3，其他基金数据
------------



------------
后续：
1，引入股权质押，重要股东增减持数据。

'''
#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )
import pandas as pd 
import numpy as np 

file_path_admin = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
path_rc = "C:\\db_wind\\data_adj\\"
path0 = "C:\\db_wind\\" 
### 导入wds数据转换模块
from transform_wind_wds import transform_wds
transform_wds1 = transform_wds()
### Print all modules 
transform_wds1.print_info()
### 导入日期序列
obj_dates = transform_wds1.import_df_dates()
print("date_start",obj_dates["date_start"],"date_end",obj_dates["date_end"]  )

#####################################################################
### test 读取给定日期范围内的交易价格和各类指标变动
# date_q_end =  "20050630"
# date_q_start = "20050331"
# code_stock = "600000.SH"
# object_code= transform_wds1.get_period_change( obj_dates,code_stock,date_q_start,date_q_end) 

#####################################################################
### Choice 2 实现单一季度所有基金的数据导入和整理、指标计算、预测计算

### step 1，获取季度文件内所有基金：股票型、混合型、指数型 ||感觉指数型会
# notes:200321 def manage_fund_sp_change() 转移至 transform_wds1()

# missing 3  date_q_end =  "20191231"
temp_date_obs = "20200431"
date_q_end = "20200331" #  "20191231"
date_q_start = "20191231" # "20190930"

df_sp_diff = transform_wds1.manage_fund_sp_change(temp_date_obs,date_q_end,date_q_start)
asd

### working for 2006 to 2018 for range(2006,2019)
for year in range(2018,2019) :
    ### Q1  
    temp_date_obs = str(year) +"0430"
    date_q_end =  str(year) +"0331"
    date_q_start = str(year-1) +"1231"
    df_sp_diff = transform_wds1.manage_fund_sp_change(temp_date_obs,date_q_end,date_q_start)
    ### Q2 
    temp_date_obs = str(year) +"0730"
    date_q_end =  str(year) +"0630"
    date_q_start = str(year) +"0331"
    df_sp_diff = transform_wds1.manage_fund_sp_change(temp_date_obs,date_q_end,date_q_start)

    ### Q3
    temp_date_obs = str(year) +"1030"
    date_q_end =  str(year) +"0930"
    date_q_start = str(year) +"0630"
    df_sp_diff = transform_wds1.manage_fund_sp_change(temp_date_obs,date_q_end,date_q_start)

    ### Q4  
    temp_date_obs = str(year+1) +"0130"
    date_q_end =  str(year) +"1231"
    date_q_start = str(year) +"0930"
    df_sp_diff = transform_wds1.manage_fund_sp_change(temp_date_obs,date_q_end,date_q_start)
asd 

year = 2019
### Q1  
temp_date_obs = str(year) +"0430"
date_q_end =  str(year) +"0331"
date_q_start = str(year-1) +"1231"
df_sp_diff = transform_wds1.manage_fund_sp_change(temp_date_obs,date_q_end,date_q_start)
### Q2 
temp_date_obs = str(year) +"0730"
date_q_end =  str(year) +"0630"
date_q_start = str(year) +"0331"
df_sp_diff = transform_wds1.manage_fund_sp_change(temp_date_obs,date_q_end,date_q_start)

### Q3
temp_date_obs = str(year) +"1030"
date_q_end =  str(year) +"0930"
date_q_start = str(year) +"0630"
df_sp_diff = transform_wds1.manage_fund_sp_change(temp_date_obs,date_q_end,date_q_start)

### Q4  
temp_date_obs = str(year+1) +"0130"
date_q_end =  str(year) +"1231"
date_q_start = str(year) +"0930"
df_sp_diff = transform_wds1.manage_fund_sp_change(temp_date_obs,date_q_end,date_q_start)





# todo,计算个股在3种基金类型下的持仓；在同一基金公司下持仓。
#notes:可能出现同一个季度，分别有2种基金持仓的情况：top10和全部
### step 1，
asd



#####################################################################
### Choice 1 实现单一季度单一基金的数据导入和整理、指标计算、预测计算
### 目标：对每个季度，保存季度内所有基金持仓数据：基础指标、差异指标、分析指标
# 分别对应 object_fund、object_diff、object_ana 
object_fund_x={}
object_fund_x["temp_date_obs"]= "20050429"
object_fund_x["date_q_end"] =  "20050630"
object_fund_x["date_q_start"] =  "20050331"
object_fund_list = transform_wds1.import_fund_list(object_fund_x["temp_date_obs"],object_fund_x["date_q_end"],object_fund_x["date_q_start"])
# temp_date_obs:用户观察到数据的时间 # 对于date_latest,跟踪上一次跟踪以来的持仓变动数据
object_fund_x["fund_code"]= "080001.OF"
object_fund_x["fund_name"]= "长盛成长价值"
object_fund_x["fund_company"]= "长盛基金"
### 导入基金持仓信息
object_fund = transform_wds1.import_df_fund(object_fund_x,object_fund_list)
### 计算基金季度持仓变动、股票信息和行业变动
object_diff = transform_wds1.cal_diff_stockport(object_fund) 
# 计算季度之间的差异值，但不进行分析 
print("Directory of csv file for fund df ",   object_diff["dir_df"] )

### 构建一个基金分析指标df，columns要有中英文和notes
'''
单独有一个文件夹存放df_fund_sp_ana
fund_sp_ana基金持仓数据分析列_200203.xlsx
F:\db_wind\data_adj\fund_ana
'''
### 将基金的季度数据保存到特定文件夹
# 长盛成长价值_20050331_20050630.csv || F:\db_wind\data_adj\fund_ana\20050630\

#####################################################################
### Choice 2 实现单一季度所有基金的数据导入和整理、指标计算、预测计算
# 1，获取季度文件内所有基金：股票型、混合型、指数型 ||感觉指数型会




asd
# notes:2、4季度有2次数据披露的问题要做进一步的梳理
#####################################################################
### 初始化基金管理对象 df || 
'''



基金数据的发布时间分析：
对于每一年，对于0131、0331、0431、0731、0830、1030六个基金数据披露截止时间，要根据披露的基金持仓
信息补全。企业股东数据的披露截至时间是0430、0830、1030，基本上可以和上述6个对应起来。
区间[0101,0131],[0101,0331],[0331,0430],[0630,0731],[0630,0830],[0930,1031],
    [1231,0430],[0331,0430],[0630,0830],[0930,1030],
从20050104开始记录股票价格和基金净值数据，
1，初始化or导入基金更新表格，columns是自定的指标例如是否更新，index是每个季度截至的信息披露日期
2，获取20050104上市日期超过6个月的股票型基金，对应在上一季度报告前已经减仓完毕；也就是20040630
3，对于每只基金，获取上一期持仓信息和档期持仓信息:
3.1,如果没有上一季度的数据，则仅根据当前持仓信息进行预测
3.2,若有上一季度持仓数据，按不同类型【基金top10、基金全部持仓、企业股东】导入到基金对象里
 
4，计算方法{对0101~0131的每个交易日：}（全部基金、同一基金管理人、单一基金）：
4.1，时点季度top10仓位；上季度top10仓位，计算基金top10持仓占比变动、持仓金额变动两项；其中仓位占比接近10%
的要考虑是否存在被迫减仓的因素、持仓金额变动如果较大(计算金额同比增长百分比)要考虑自身对区间股票
价格的影响。
4.2，时点半年报全部持仓；和半年前比、和季度比，看几个点：非前10大个股的变动率和变动金额、中信1、2级
行业变动、Wind1，2级行业变动、申万一级。还要看期间的换手率，如果半年换手率低于50%，那么可能不存在其他
不在持仓内的股票
4.3，对于给定基金范围，统计：市值加权持仓、持仓占比、同比、

4.N，定量指标：


下一步思路：
1，个股拟合，选取上一季度和当季度持仓占比最大的

数据分析：对2019q3剔除季度内涨跌幅变动后的市值变动top100的股票做回归分析，分析扣除上涨后的
市值变动和区间涨跌幅相关系数是 -0.04，取top10和tail10也每发现有显著性。这说明基金整体选股
在季度区间内没有显著的超额收益能力


'''







# 要找在200501前上市的股票型基金，且目前仍然在管。200501前上市且非指数基金的股票型基金有31个，
# 最早200704到期，最晚201707到期，基本上没有持续运作的;例如长盛同德500039在1992上市，200710到期转为519039
# 逻辑：只要基金未到期，就跟踪其持仓。
# 假设：基金到期后，资金会继续留在同类型其他基金份额里。
# 富国天惠是0511减仓，0602开始上市，可以考虑从[06q1,06q2]开始跟踪
df_fund_des_0501 = df_fund_des[ df_fund_des["F_INFO_SETUPDATE"]<20050131 ]


















'''
分析的几个维度:
1,绝对值：行业，个股种类的变动、持仓市值变动，仓位比例，仓位倒退基金净值
2，相对值：行业同比、个股同比，市值同比、仓位同比、

3,行业分类，首先用中信一级行业分类划分
notes:
截至20200118，已经有2700个基金披露2019q4持仓，但很多是债券基金货币基金等。
例如兴全、工银瑞信
163417.OF	兴全合宜
007119.OF 睿远成长价值

S_INFO_STOCKWINDCODE 601012.SH
F_PRT_STKVALUE       3.27055e+09
F_PRT_STKQUANTITY    1.24687e+08
F_PRT_STKVALUETONAV  9.19

todo:
1,时间上，从基金上市3个月开始跟踪季度持仓的披露时间，在每年季报披露时间1、4、7、10和
半年报披露时间3、8


'''









#####################################################################