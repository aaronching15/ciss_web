# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
last update 181022 | since 181022

1,get latest A-shares list
get latest 1min data for current day or several days

===============================================
'''
import time
import datetime as dt
import pandas as pd 
import WindPy as WP
WP.w.start()
time_stamp = dt.datetime.now().strftime('%Y%m%d')
print('Current time :', time_stamp )
# w.wsi("600036.SH", "open,high,low,close,volume,amt,chg,pct_chg", "2018-10-22 09:25:00", "2018-10-22 09:32:00", "Fill=Previous;PriceAdj=F")
# get all A share codes
# w.wset("sectorconstituent","date=2018-10-22;sectorid=a001010100000000")
path0 = "D:\\db_wind\\1min\\"

date= "2018-10-22"
sector = "Ashares"
sectorid = "a001010100000000" # all a shares 
# input: date,sector,sectorid,items
filename = sector +"_" + "1min_" + time_stamp + '_'+ date + '.csv'
# winddata1 = WP.w.wset("sectorconstituent","date=2018-10-22;sectorid=a001010100000000")
winddata1 = WP.w.wset("sectorconstituent","date="+date+";sectorid="+sectorid )
# len(winddata1.Data[1]) =3554 
# print( winddata1.ErrorCode ==0 )
print( len(winddata1.Data[1]) )
if winddata1.ErrorCode == 0 :
	items = ["open","high","low","close","volume","amt","chg","pct_chg"]
	index_list = ['time', "code_wind"] + items
	temp_df = pd.DataFrame()
	time0 = date+ " 09:25:00"
	time1 =  date+ " 15:00:00"
	# for i in range(len(winddata1.Data[1])//100+1) :
	# Notes:每个股票每日都有240个数据，因此一次抓取10个股票就好了；目前来看一次抓单日100只股票是可以的。
	STEPS = 40 # 100 
	for i in range(0,len(winddata1.Data[1]),STEPS) :
		# i=0,100,200,...  | if 3553, i_max=3500,
		if i == len(winddata1.Data[1])//STEPS*STEPS :
			print('Working on ', str( i) ," to ...", ) 
			# if len(winddata1.Data[1])%100 == 0 :
			# notes if there are 3500
			codes = winddata1.Data[1][i:]
			if len(codes) >0 :
				winddata2 = WP.w.wsi(codes, items, time0, time1, "Fill=Previous;PriceAdj=F")
				if winddata2.ErrorCode == 0:
					# no error from wind api
					temp_df0 = pd.DataFrame( winddata2.Data,index=index_list  )
					temp_df0 = temp_df0.T 
					print('temp_df0 \n')
					print( temp_df0.head(5) )
					temp_df = pd.concat([temp_df,temp_df0]  ) 
					temp_df.to_csv(path0+filename)
					time.sleep(0.3)
		else :
			print('Working on ', str(i) ," to ", str(STEPS+i) ) 
			# i < len(winddata1.Data[1])//100 
			codes = winddata1.Data[1][i:STEPS+i]
			winddata2 = WP.w.wsi(codes, items, time0, time1, "Fill=Previous;PriceAdj=F")
			if winddata2.ErrorCode == 0:
				# no error from wind api
				temp_df0 = pd.DataFrame( winddata2.Data,index=index_list  )
				temp_df0 = temp_df0.T 
				print('temp_df0 \n')
				print( temp_df0.head(5) )
				temp_df = pd.concat([temp_df,temp_df0]  ) 
				temp_df.to_csv(path0+filename)
				time.sleep(0.3)

########################################################################################
# GET HongKong shares | 注意：港股1min数据wind是没有的。
# sector = "HKshares"
# sectorid= "a002010100000000"
# winddata1 = WP.w.wset("sectorconstituent","date="+date+";sectorid="+sectorid )
 

########################################################################################
# ###  single test 
# codes = ["600036.SH","000002.SZ"]
# items = ["open","high","low","close","volume","amt","chg","pct_chg"]
# index_list = ['time', "code_wind"] + items

# time0 = date+ " 09:25:00"
# time1 =  date+ " 09:31:00"
# # time1 =  date+ " 15:00:00"
# winddata2 = WP.w.wsi(codes, items, time0, time1, "Fill=Previous;PriceAdj=F")
# temp_df = pd.DataFrame()
# temp_df0 = pd.DataFrame()
# temp_list0 = []
# if winddata.ErrorCode == 0:
# 	# no error from wind api
# 	temp_df0 = pd.DataFrame( winddata2.Data,index=index_list  )
# 	temp_df0 = temp_df0.T
# 	# result = df1.append(df2)  默认沿着列进行凭借（axis = 0，列对齐）
# 	# temp_df00= pd.concat([temp_df3,temp_df4])  默认沿着列axis = 0
# 	# join参数的属性，如果为’inner’得到的是两表的交集，如果是outer，得到的是两表的并集。
# 	# result = pd.concat([df1, df4], axis=1, join='inner') 沿着行 axis = 1
# 	temp_df = pd.concat([temp_df,temp_df0]  )
# 最后temp_df会变成一个 3500*(60*4)= 84万行的数据表格






























'''
2019年全国硕士研究生招生考试初试时间为2018年12月22日至12月23日
(每天上午8:30-11:30，下午14:00-17:00)。超过3小时的考试科目在12月24日进行(起始时间8:30，截止时间由招生单位确定，不超过14:30)。
二、初试科目

初试方式均为笔试。
12月22日上午 思想政治理论、管理类联考综合能力
12月22日下午 外国语
12月23日上午 业务课一
12月23日下午 业务课二
12月24日 考试时间超过3小时的考试科目

source https://yz.chsi.com.cn/kyzx/jybzc/201808/20180821/1715735913.html'''






