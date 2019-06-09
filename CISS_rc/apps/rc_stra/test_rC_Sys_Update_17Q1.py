# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
Last update 170306
todo 170306
# todo 171214： 新建 Wind_INDEX.SH.csv ，每日按年化3%收益计算净值，用来替代指数，相当于取消对指数的控制

测试 test_N_Portfolio_17Q1 里的部分代码
todo
Input:
Log_Portfolio
Output:
Log_Portfolio

===============================================
'''

# todo 170323 屏幕输出的内容保持到特定 txt文件
# import sys
# screenOutput = sys.stdout
import time
temp_time1 = time.strftime('%y%m%d%H%M%S',time.localtime(time.time()))
print( temp_time1 )
# temp_file_name = 'test_N_port_temp_' + temp_time +  '.txt'

import rC_System_17Q1 as rC_Sys

# todo Part 0 : Initialization of var，para，directory
# date_LastUpdate_New = '170921'
date_LastUpdate_New = input('Please type in date_LastUpdate_New : ')
if_change_SP = 0
# todo update synthetic index | 171228
# steps import syn_Index and latest date, compared with 000300.SH,get the dif_list of dates, update for every dif date
import rC_Data_Initial as rC_Data
code_Syn = 'INDEX.SH'
code_Index = '000300.SH'
results = rC_Data.rC_Database( code_Syn )
Path_Data = 'D:\\data_Input_Wind'
temp = results.Update_Synthetic_Index( code_Syn, code_Index, Path_Data )


# todo Part0 导入TradeSys :
# for Sys_name in [ 'Sys_rC18' ] :
for Sys_name in [ 'Sys_rC1703', 'Sys_rC1712' ] :
# for Sys_name in [   'Sys_rC1712' ] :
    # str_ID = '1703'  # '1703' 1704   '1704-CSI800' | '1703_2015'
    # todo 组合 '1501-CSI800' 卡在了20150206， 因为StockPool里，有的股票那时候没有上市，未来需要解决这个问题
    if Sys_name ==  'Sys_rC1703' :
        str_ID_List = [ '1703', '1704','1704-CSI800', '1501-CSI100n200' ]
    # Sys_name = 'Sys_' + 'rC1712'
    elif Sys_name ==  'Sys_rC1712' :
        # 171217 确定需要每日更新的 ID_List
        # str_ID_List =['1712_A136','1701_HK_MaxN40' ]
        #  '1701_HK_MaxN30' 出问题， Dates不对，因为是 Index.SH
        str_ID_List = [ '1712_HK','1701_HK_MaxN30','1712_HK300','1701_HK_MaxN40', '1712_A136' ]

        # str_ID_List = [  '1712_HK300', '1701_HK_MaxN40', '1712_A136']
    elif Sys_name ==  'Sys_rC18' :
        str_ID_List = [ '1803' ]

    # todo '1704' Account_Sum_2017-04-25.csv' does not exist | 需要把Log_Portfolio 里的日期改成 date_LastUpdate_New  前一日
    # todo 170329 建立自己的股票池 symbol_List_170329.csv
    # todo ===========================================
    path_Output = 'D:\\data_Output'

    for str_ID in str_ID_List :
    # for str_ID in ['1701_HK_MaxN30' ] :

        rC_Sys1 = rC_Sys.rC_System(Sys_name, str_ID ,path_Output ,date_LastUpdate_New )
        print('str_ID is , ', str_ID )

        result = rC_Sys1.rC_System_Update( date_LastUpdate_New,Sys_name, str_ID, path_Output ,if_change_SP  )

        temp_time2 = time.strftime('%y%m%d%H%M%S',time.localtime(time.time()))
        print('Last time is ', temp_time1 )
        print('Current time is ', temp_time2 )
        print('The rC_Sys has been updated. ')


