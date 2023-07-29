# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
todo
后续的问题： 

功能：
1,将数据转化为html可以展示的形式，例如csv to html-table

------ 
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


#################################################################################
### csv to html-table by open()


# column headers 

infile = open("D:\\temp.csv","r")
table_html="<table>"

if_header = 1 
for line in infile:
    if if_header == 1 :
        row = line.split(",")
        # write row of hearder
        table_html= table_html +"<th>"
        for item in row :
            table_html= table_html + "<td>%s</td>" % item 

        table_html= table_html +"</th>"

        if_header = 0
    else :
        row = line.split(",")
        # write row vlaues
        table_html= table_html +"<tr>"
        for item in row :
            table_html= table_html + "<td>%s</td>" % item 

        table_html= table_html +"</tr>" 

# end the table
table_html= table_html +"</table>"

print("table_html")
print( table_html )

#################################################################################
### csv to html-table by pandas 
import pandas as pd 
df_input = pd.read_csv("D:\\temp.csv")
print("df_input")
print(df_input.to_html()  )










