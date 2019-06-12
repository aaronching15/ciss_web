# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
需求：
下载相关资产的日线数据

last 190612 || since 190612

Function:
功能：

todo:

Notes:
##############################################
变量： 
##############################################
Steps：


benchmark：000300.SH
asset：["601318.SH","600519.SH","600036.SH","000651.SZ","000333.SZ","601166.SH","600030.SH","600276.SH","601398.SH","000002.SZ","000001.SZ","000725.SZ","600104.SH","600900.SH","600009.SH"  ]


===============================================
'''
import json
import pandas as pd 
import numpy as np 
import math

import csv
#####################################################################
### Configuration parameters
path_out ="D:\\"
path0 = "D:\\data_Input_Wind\\"
### Create dictionary object for symbols
dict_symbol = {}
dict_symbol["path_csv"] = path_out

file_path0 = "D:\\data_Input_Wind\\"

#####################################################################
bench_symbol = "000300.SH"
asset_list =["601318.SH","600519.SH","600036.SH","000651.SZ","000333.SZ","601166.SH","600030.SH","600276.SH","601398.SH","000002.SZ","000001.SZ","000725.SZ","600104.SH","600900.SH","600009.SH"  ]

import WindPy as WP
# Or: from WindPy import w
WP.w.start()

code = bench_symbol 

date_0=''
date_1='20190612'
items='open,high,low,close,volume,amt,pct_chg'


def get_wind_csv(code,items, date_0, date_1,file_path0) :
	WindData = WP.w.wsd(code, items, date_0, date_1, 'Priceadj=F')

	file_path=file_path0 +'Wind_'+ code + '_updated.csv'

	with open( file_path, 'w',newline='') as csvfile:
		# fieldnames = ['first_name', 'last_name'] ; Columns=[  'date', 'open', 'high',  'low'  , 'close', 'volume']
		fieldnames = WindData.Fields #  Data3.Fields=Columns ？
		# writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer = csv.writer(csvfile ) #　delimiter=' '
		# Write the first row as head
		writer.writerow(['DATE' ]+ fieldnames )
		len_item=len(WindData.Data) # = len(Columns) =6

		len_contents=len(WindData.Data[1]) #codes here 253 

		for i in range(len_contents ) :
		    temp_list  = [WindData.Times[i]  ] 
		    
		    # items='open,high,low,close,volume,amt,pct_chg'
		    temp_list.append( WindData.Data[0][i] ) 
		    temp_list.append( WindData.Data[1][i] ) 
		    temp_list.append( WindData.Data[2][i] ) 
		    temp_list.append( WindData.Data[3][i] ) 
		    temp_list.append( WindData.Data[4][i] ) 
		    temp_list.append( WindData.Data[5][i] ) 
		    temp_list.append( WindData.Data[6][i] ) 

		    writer.writerow( temp_list ) # date


result = get_wind_csv(code,items, date_0, date_1,file_path0)

for stock_symbol in asset_list :
	print("Working on symbol ", stock_symbol )
	code= stock_symbol
	result = get_wind_csv(code,items, date_0, date_1,file_path0)























