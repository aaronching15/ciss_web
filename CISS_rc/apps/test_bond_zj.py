# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
需求：
1，分特征导入债券数据
    path: D:\CISS_db\bond_zj
    1.1，delete all empty rows beforehand 
    1.2,Ana:最少3项，最多6-7项

2，从3个网站获取表格数据，保存成excel
    http://bond.sse.com.cn/data/quote/tradeinfo/price/
    http://bond.szse.cn/marketdata/statistics/at/prd/index.html
    http://www.chinamoney.com.cn/chinese/mkdatabondcbcbm/


Function:
功能：

todo:


Notes:

===============================================
'''
import json
import pandas as pd 
# import sys
# sys.path.append("..") 
import re

###########################################################################
path0 = "D:\\CISS_db\\bond_zj\\"
file =  "20190118.txt"
# df_raw = pd.read_csv(path0 + file )

df_raw = pd.DataFrame( columns=["borker","bond_type","duration","name","code","rating","rates","exercise"]  )
list_brokers=["平安信用","平安利率","BGC信用","BGC利率","国际信用","国际利率","信唐成交","国利利率","国利信用"]
list_types = ["短融","中票","企业债","其他","存单","国债","金融债","地方债","地方债成交","金融债成交"]
i=0
if_broker = 0
if_bond_type = 0
# pattern of express regulations
regex_str = ".*?([\u4E00-\u9FA5]+)"
regex_str = ".*?([\u4E00-\u9FA5]+)"
regex_duration= "\d{6,}"
regex_rating = "A{2,}" # Not working 
regex_6more= "\d{6,}"
# todo
regex_rates= "\d{1,2}(\.\d+)"

broker =""
for line in open(path0 + file,encoding="utf-8" ):
    # print(line)
    # print(len(line))

    rr =  re.split(r" +",line)
    if rr == ['\ufeff平安信用\n'] :
        rr= ['平安信用']

    if len(rr) ==1 :
        rr =  re.split( "\t",rr[0])
        print("444= ",rr)
    ### get broker name 
    if len(rr) ==1 :
        ### case 1  no broker yet 
        # create a new bond type 
        # rr[0] = ['\ufeff平安信用\n']
        rr_CN0 = re.match( regex_str ,rr[0] )
        # rr_CN0[0][1:]  = 平安信用
        if not rr_CN0 == None :
            if rr_CN0[0] in list_brokers :
                # now we have a new broker until next one comes
                broker = rr_CN0[0]

            elif rr_CN0[0] in list_types :
            ### working on bond_type 
                bond_type =  rr_CN0[0]
    
            
    if len(rr) >= 3  :
        df_raw.loc[i,"broker"] = broker
        df_raw.loc[i,"bond_type"] = bond_type
        df_raw.loc[i,"duration"] = rr[0]

        for item in rr :
            # print("item ", item )
            # source https://blog.csdn.net/jimmy_gyn/article/details/79050491
            # print(re.match( regex_rates ,item) )
            ### 判断中文字符
            if re.match( regex_str ,item) :
                if item == "行权" :
                    df_raw.loc[i,"exercise"]=item
                else :
                    df_raw.loc[i,"name"] = item
            ### 判断代码，041800252， 1572002，135567.SH，147524，180210 
            # 至少6位数字
            elif re.match( regex_6more ,item) :
                df_raw.loc[i,"code"] = item
            ### 判断评级 AA to AAA
            elif re.match( regex_rating ,item) :
                df_raw.loc[i,"rating"] = item
            ### 判断收益率 3.7325 3.7 3.22行权 4.21%
            elif re.match( regex_rates ,item) :
                df_raw.loc[i,"rates"] = item

        # case for 信唐成交
        if broker == "信唐成交" and len(rr) <4 :
            # rr666  ['10D\t160003\t2.10', '\t', '\t\n']
            # rr666  ['174D', '18河钢集CP007', '041800252', '\t3.30', 'AAA', '\n']
            print("rr666 ",rr)
            rr2 = re.split("\t",rr[0])
            df_raw.loc[i,"duration"] = rr2[0]
            df_raw.loc[i,"code"] = rr2[1]
            df_raw.loc[i,"rates"] = rr2[2]

        if broker == "国际信用"  :
            ### get rid of \t
            print("666 ", df_raw.loc[i,"code"] )
            temp_name =df_raw.loc[i,"name"]
            if pd.isnull( df_raw.loc[i,"code"]  ) :
            # temp_name = " 011801139.IB    18嘉兴现代SCP002"
                rr_sub = re.split("\t", df_raw.loc[i,"name"] )
                if len(rr_sub ) >= 2 :
                    df_raw.loc[i,"name"] = rr_sub[0]
                    df_raw.loc[i,"code"] = rr_sub[1]
                # df_raw.loc[i,"rating"] = rr_sub[2]
            ### get rid of % 
            if type(df_raw.loc[i,"rates"])==str and  "%" in df_raw.loc[i,"rates"] :
                df_raw.loc[i,"rates"] =df_raw.loc[i,"rates"].replace("%","")

    # if i>30:
    #     asd
    i=i+1

df_raw.to_csv("D:\\bonds_190124.csv",encoding="utf_8_sig")
# df_raw.to_csv("D:\\bonds_190124.csv",encoding="utf-8")




























































