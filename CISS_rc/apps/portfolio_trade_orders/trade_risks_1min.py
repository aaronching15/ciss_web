# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function: realize 1min level market amount analyze
功能：实现1分钟的市场成交数据分析
last update 181030
Menu :
1, 导入市场1分钟数据
2，

output:
1分钟的数据简单分析，以10-29全天为例：
1，A股全市场每分钟平均成交股票数量为  5.42万股
2，A股全市场每分钟平均成交股票金额为 48.77万元
3，值得注意的是，早盘30分钟和尾盘10分钟的成交量显著高于其他时间段。

建议：如果在09：30-10:00时间段贴合市场成交量变化先快后慢地委托，可能比全天平均数量或百分比委托效率高点。
    但尾盘10分钟不应该扩大委托，否则可能有监管风险。

Notes:
1,wind-api数据提取： 限制条件：每周50万个，安全起见，每周45万。
2,wind api 每次只能抓取不超过100个数据。
===============================================
'''
import sys
sys.path.append("..")

import pandas as pd 
temp_df = pd.read_csv("D:\\db_wind\\Ashares_1min_20181029_2018-10-22.csv")

print("全市场每分钟成交金额|万元：",round(rr['amt'].mean()/10000,2)   )
print("全市场每分钟成交股票数|万：",round(rr['volume'].mean()/10000,2)  )

temp_df2 = temp_df.groupby(["time"])['volume','amt'].mean()

# 分析1min级别，个股委托占比：
# time  amt volume
path1= "D:\\data_O32\\"
file="O32_trade_181029_detail_2.xls"
temp_df_code0 = pd.read_excel(path1+file)
# >>> temp_df_code.loc[0,'time'] ||  '14:50:03'
temp_df_code = temp_df_code0.loc[:,["time","amt","volume"]]

temp_df_code['time2'] = pd.to_datetime(temp_df_code['time'] , format = "%H:%M:%S")
temp_df_code.index = pd.to_datetime(temp_df_code['time'] , format = "%H:%M:%S")

# resample | 重采样（resampling）指的是将时间序列从一个频率转换到另一个频率的处理过程。
# source https://blog.csdn.net/Shingle_/article/details/78238265

temp_df_code = temp_df_code.drop([0],axis=0) 

temp_df2 = temp_df_code.resample('1min').sum() # M表示月份
# another method: df.resample('2s', how='sum')

temp_df2.to_csv("D:\\single_code_181029.csv")

'''
计算个股的分档每分钟成交金额，计算出最优委托金额和数量
时间段|每30分钟为间隔：
'30min'

'''
temp_df2 = temp_df_code.resample('30min').sum() # M表示月份
# another method: df.resample('2s', how='sum')

temp_df2.to_csv("D:\\trade_30min_181029.csv")

## 分个股，挖掘出市场异动。
'''
总体维度：
1,1分钟维度，计算全天时间加权均价和成交量加权均价，比较差异。

统计每5分钟成交
1, 成交量敏感性：找到日内金额最高的5个5分钟，判断对应日内最高价，均价还是最低价。
    
2，价格敏感性分析：每5分钟成交金额，若某个5min内放量上涨，则判定影响价格一定幅度需要多少资金。

3，模拟资金推动：若某5min内，800w金额成交推动股价上涨0.3%，下一个5min：
    1,400w，股价不变，资金维持率50%
    2,1200w，股价上涨0.3%，实现同样比例上涨幅度，资金需求增加50%
    3,300万，股价回落0.3%，



'''




























