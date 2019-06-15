# coding: utf-8
__author__=" ruoyu.Cheng"

'''
这个文件是用来自动化抓取私募基金数据
想通过selenium实现但是公司下载不了浏览器的driver，selenium永不了。

last 190614

function：
1，获取网页，人工/自动设置每页显示100页

author : cheng ruoyu
'''

##############################################################

from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import time
import pandas as pd 

# path="D:\\softs\\chromedriver.exe" #替换成geckodriver实际所在目录
path="C:\\Program Files (x86)\\Google\\Chrome\\Application\\chromedriver.exe"
driver=webdriver.Chrome(path)

# path="C:\\Program Files (x86)\\Mozilla Firefox\\firefox.exe"
# driver=webdriver.Firefox()

### 打开浏览器
# QS 由于geckodriver.exe没有下载，导致错误提示，单位电脑怎么都下载不下来64位的
# firefoxdriver，这次在家里电脑下来后，copy到 python/scripts 目录下。运行ok了

time.sleep(2)

### 打开网页
web_name = "http://gs.amac.org.cn/amac-infodisc/res/pof/fund/index.html"
driver.get( web_name )

##########################################################################
### 等待6秒，点击提示框中的“关闭”按钮
### <button class="ui-button ui-widget ui-state-default ui-corner-all ui-button-text-only" 
#    role="button" style="color: rgb(85, 85, 85);" type="button"><span class="ui-button-text">关闭</span></button>
time.sleep(6)

# 发现Chrome的elements里可以鼠标右键复制xpath或者element
# /html/body/div[6]/div[3]/div/button
driver.find_element_by_xpath("/html/body/div[6]/div[3]/div/button").click()

# 匹配到属性尾部   
# driver.find_element_by_css_selector("[name$='ui-button-text-only']").click()
# driver.find_element_by_class_name('ui-button-text').click()

##########################################################################
### 找到下拉框，选择“100”的参数
# source 
# 实例化一个Select类的对象

from selenium.webdriver.support.select import Select
selector = Select(driver.find_element_by_name("fundlist_length") )
# # 下面三种方法用于选择"篮球运动员"
# selector.select_by_index("2")  # 通过index进行选择,index从0开始
# selector.select_by_value("210103")  # 通过value属性值进行选择
# selector.select_by_visible_text("篮球运动员")  # 通过标签显示的text进行选择

selector.select_by_value("100")  # 通过value属性值进行选择

##########################################################################
### Working page range 
page_list=[951,1000]

# done
# 651 799  ,148
# todo 894 1000 ,106
# 894 950
# 950 1000

##########################################################################
### 翻到 page 195
driver.find_element_by_id('goInput').send_keys( str(page_list[0]) )
### 按回车键

driver.find_element_by_xpath("//*[@id='fundlist_paginate']/button").click()

### 等待5秒
time.sleep(5)

##########################################################################
### Generate output dataframe

table1= driver.find_element_by_id('fundlist')
# Get all th, column names 
table_th = table1.find_elements_by_tag_name('th')

### Initialize dataframe
t_cols = []
for t_c in range(6) :
    t_cols = t_cols + [ table_th[ t_c ].text ]
print( t_cols )

df_all = pd.DataFrame( columns = t_cols ) 
count = 0 

### 确认网页中是否存在“下一页”的翻页按钮
# XPATH:  //*[@id="fundlist_paginate"]/a[3] ,注意：可以把中间的英文双引号改成单引号，匹配string外边套双引号，
# 否则无法识别
paginate_next = driver.find_element_by_xpath("//*[@id='fundlist_paginate']/a[3]")

### Check if we have next page 
while paginate_next.text == "下一页" and count < page_list[1] :
    
    count = count +1

    print("Working on page ",count )
    ##########################################################################
    ### Operation on single web page 

    ### 获取网页中的表格数据 tbody
    table1= driver.find_element_by_id('fundlist')


    ### 为了避免有的单元格是空，不应该用tr，应该用td
    # 总行数， table_rows 是一个有101行的element，把每一行保存成一个string
    ### table_tr[1].text
    # table_tr = table1.find_elements_by_tag_name('tr')


    ### Get all td, cell values, table_td[1].text
    # 所有单元格 ， 600个
    table_td = table1.find_elements_by_tag_name('td')

    ### Get all th, column names 
    # table_th = table1.find_elements_by_tag_name('th')

    # ### Initialize dataframe
    # t_cols = []
    # for t_c in range(6) :
    #     t_cols = t_cols + [ table_th[ t_c ].text ]
    # print( t_cols )

    ### Qs 感觉dataFrame有点慢，不如list
    df1 = pd.DataFrame( columns = t_cols ) 

    # temp row , temp columns, temp value
    for t_r in range( 100 ) :
        for t_c in range(6) :

            t_index = t_r*6+t_c
            # print("Table location", t_index,t_r,t_c)
            # print( table_td[ t_index ] )
            # if len( str(table_td[ t_index.text ])  ) < 1 :
            #     t_value = ""
            # else :    
            t_value = table_td[ t_index ].text
        
            df1.loc[t_r, t_cols[t_c] ] = t_value



    print("table for current page \n ",df1.head(3), df1.tail(3) ) 

    ### Concat current df1 tp df_all 
    df_all = df_all.append(df1, ignore_index=True)
    df_all.to_csv("D:\\amac_list_"+ str(page_list[0])+ "_"+ str(page_list[1])+ ".csv")

    ### click to next page 
    paginate_next.click()

    time.sleep(5)
    paginate_next = driver.find_element_by_xpath("//*[@id='fundlist_paginate']/a[3]")


driver.close()
driver.quit()











##############################################################
















































