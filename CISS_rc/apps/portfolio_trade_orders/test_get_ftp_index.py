# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
since 20200630

功能：定时自动从指数公司ftp下载数据

notes:
1,网站只有最近1个月的数据,而且其中很多文件夹无法打开。   
2,报错：550 Requested action not taken. File unavailable (e.g., file not found, no access). 
    请求操作未被执行，文件不可用。 就是用户/密码不对不让连接，连接被拒绝，或者路径不对也会产生550错误

ref:
url=https://www.cnblogs.com/baitme/p/11837017.html
''' 
############################################################################
import time
import datetime as dt 
from ftplib import FTP
import os
import zipfile

############################################################################
### 远程ftp地址,目录，文件
# path= ftp://124.74.243.125/idxdata/data/asharedata/000906/close_weight/
# notes:注意格式！！！ 连接后需要先进入"idxdata/data/asharedata"
path_asharedata = "idxdata/data/asharedata/" 

index_list= ["000016","000300","000903","000904","000906","000934","399975","930620"]

file_type_list = ["close_weight","weight_for_next_trading_day"]
'''可以打开的目录：文件名
"close_weight":000300closeweight20200624.zip
weight_for_next_trading_day :000300weightnextday20200624.zip
securities_to_be_included:000300tobeincludedweight20200624.zip
corporation_action：000300ca20200624.zip
corporation_action_future：000300cafuture20200624.zip
"securities_to_be_included" :,"tobeincludedweight" 
'''
str_connect_list = ["closeweight","weightnextday"]

############################################################################
### 本地地址,目录，文件
path_index_consti = "D:\\db_wind\\data_index_constitutes\\"

if not os.path.exists(path_index_consti) :
    os.makedirs(path_index_consti)


log_file = path_index_consti + 'ftp_log.txt'


def ftp_connect():
    """用于FTP连接"""
    ftp_server =  '124.74.243.125'  # ftp站点对应的IP地址
    username = 'csics'  # 用户名
    password = '17381906'  # 密码
    ftp = FTP()
    ftp.set_debuglevel(0) # 较高的级别方便排查问题
    ftp.connect(ftp_server, 21)
    ftp.login(username, password)
    print("Successfully connected...")
    ## ftp.nlst() = ['idxdata']
    # notes:这时候是在 ftp://124.74.243.125/
    ftp.cwd( path_file ) # 进入目标目录
    print( ftp.nlst()  )
    return ftp

def remote_file_exists_download(file_name,file_local):
    """用于FTP站点目标文件存在检测"""
    ftp = ftp_connect()
    print( path_file )
    # notes:这时候是在 ftp://124.74.243.125/， 需要进入    
    
    # ftp.cwd( "asharedata" )
    file_list_remote = ftp.nlst()  # 获取文件列表
    print("file_list_remote ======",file_list_remote)
    
    if file_name in file_list_remote :
        ### 下载文件
        fp = open(file_local, 'wb')
        ftp.set_debuglevel(0) # 较高的级别方便排查问题
        bufsize = 1024
        ftp.retrbinary('RETR ' + file_name, fp.write, bufsize)
        fp.close()
        ftp.quit()
    else :
        print("No file= ", file_name, " in ", path_file )


############################################################################
### 时间
begin_time = 1000  # 任务开始时间
end_time = 1700  # 任务结束时间
today = time.strftime("%Y%m%d")  # 当天日期


print("Input length <8 means get past 10 trading days from WindPy")
temp_date = input("type in current trading date，such as 20200629:") 

if len( temp_date ) <8 :
    ### 获取最近2周/1个月内的交易日
    date_latest = dt.datetime.strftime( dt.datetime.now(), "%Y%m%d" ) 
    date_pre_30d =dt.datetime.strftime( dt.datetime.now() - dt.timedelta(days=14), "%Y%m%d" ) 
    # 用WindPy获取区间交易日
    import WindPy as wp 
    wp.w.start()
    # data=wp.w.tdays("2020-06-01", "2020-07-01", "")
    data_obj = wp.w.tdays(date_pre_30d, date_latest, "")
    date_list=[]
    for temp_dt in data_obj.Data[0] :
        date_list=date_list + [  dt.datetime.strftime( temp_dt ,"%Y%m%d" ) ]
    print("date_list ",date_list )
    
    ### Loop , date_list 
    count = 0 
    while True:
        # 判断是否在执行时间范围
        if int(time.strftime("%H%M")) in range(begin_time, end_time):  
            ### 
            for temp_date in date_list :
                for temp_index in index_list: 
                    for temp_type in file_type_list :
                        # print("666 ", file_type_list.index(temp_type),str_connect_list[0] )
                        temp_str = str_connect_list [ file_type_list.index(temp_type) ]
                        # temp_type = "weight_for_next_trading_day"
                        # temp_str = "weightnextday"
                        path_file = path_asharedata + temp_index + "/" + temp_type + "/"
                        file_name = temp_index+ temp_str + temp_date +".zip"
                        print(str(count),file_name)
                        ### output file 
                        file_local = path_index_consti + file_name

                        remote_file_exists_download(file_name,file_local)

                        ### 解压缩文件到指定目录 path_index_consti
                        # notes:zip_manage.extractall()会保存到程序脚本目录
                        zip_manage = zipfile.ZipFile( path_index_consti + file_name) 
                        zip_manage.namelist()
                        zip_manage.extractall(path_index_consti)

                        count = count + 1 

            time.sleep(300)
            
        else:
            time.sleep(1800)
else :
    ### Loop,temp_date 
    count = 0 
    while True:
        # 判断是否在执行时间范围
        if int(time.strftime("%H%M")) in range(begin_time, end_time):  
            ### 
            for temp_index in index_list: 
                for temp_type in file_type_list :
                    # print("666 ", file_type_list.index(temp_type),str_connect_list[0] )
                    temp_str = str_connect_list [ file_type_list.index(temp_type) ]
                    # temp_type = "weight_for_next_trading_day"
                    # temp_str = "weightnextday"
                    path_file = path_asharedata + temp_index + "/" + temp_type + "/"
                    file_name = temp_index+ temp_str + temp_date +".zip"
                    print(str(count),file_name)
                    ### output file 
                    file_local = path_index_consti + file_name

                    remote_file_exists_download(file_name,file_local)

                    ### 解压缩文件到指定目录 path_index_consti
                    # notes:zip_manage.extractall()会保存到程序脚本目录
                    zip_manage = zipfile.ZipFile( path_index_consti + file_name) 
                    zip_manage.namelist()
                    zip_manage.extractall(path_index_consti)

                    count = count + 1 
            print("File saved to :", path_index_consti )
            time.sleep(300)
            
        else:
            time.sleep(1800)