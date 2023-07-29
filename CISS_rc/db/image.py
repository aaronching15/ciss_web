# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo 

Function: 功能：图片和视频处理的相关功能 
 
date:last | since 220919
===============================================
'''
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
import pandas as pd
import numpy as np

import sqlite3

#######################################################################
### 操作 sqlite类型数据库
class image():
    def __init__(self):        
        # 导入配置文件对象，例如path_db_wind等 

    def print_info(self):         
        ################################################################################
        ### Image operation
        print("trans_image_size |  批量转换jpg照片的大小")

        ########################################
        ### 
        print(" |  ")
        
        ########################################
        ### 
        print(" |  ")
        

    def trans_image_size(self,obj_pic ):
        ################################################################################
        ###  批量转换jpg照片的大小 
        path_0 = "D:\\softs\\全家福202209\\"
        dir_name =  "世家666" 
        path_in = path_0 + dir_name + "\\"

        path_out= "D:\\softs\\全家福202209s\\"

        from PIL import Image
        ###################################################
        # for dir_name in ["世家","海派馆","欢聚日","呈祥"   ] :
        for dir_name in ["欢聚日"    ] :
            path_in = path_0 + dir_name + "\\"
            w = 0.8
            for i in range( 0,9 )  :
                file_name = str(i+1) +".jpg"
                temp_jpg = Image.open(path_in + file_name  )
                temp_size = temp_jpg.size 
                ### type of temp_jpg is tuple,temp_size[0],temp_size[1] = 4480,5973
                print(file_name ,temp_size[0],temp_size[1], int(round(temp_size[0]* w,0)) ,int(round(temp_size[1]* w,0))  ) 
                
                new_jpg = temp_jpg.resize(( int(round(temp_size[0]* w,0)) ,int(round(temp_size[1]* w,0)) ) )
                temp_size = temp_jpg.size 
                ### type of temp_jpg is tuple,temp_size[0],temp_size[1] = 4480,5973 
                ###
                file_output = dir_name + "_" +str(i+1) +"_s.jpg"
                new_jpg.save( path_out + file_output  )

            ### 
            file_name = "九宫格竖版.jpg"
            temp_jpg = Image.open(path_in + file_name  )
            temp_size = temp_jpg.size 
            ### type of temp_jpg is tuple,temp_size[0],temp_size[1] = 4480,5973
            print(file_name ,temp_size[0],temp_size[1], int(round(temp_size[0]* w,0)) ,int(round(temp_size[1]* w,0))  ) 

            new_jpg = temp_jpg.resize(( int(round(temp_size[0]* w,0)) ,int(round(temp_size[1]* w,0)) ) )
            temp_size = temp_jpg.size 
            ### type of temp_jpg is tuple,temp_size[0],temp_size[1] = 4480,5973 
            ###
            file_output = dir_name + "_" +file_name
            new_jpg.save( path_out + file_output  )
        
        return obj_pic