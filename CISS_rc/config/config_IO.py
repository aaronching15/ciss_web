# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
TODO 

功能：配置数据、策略和组合计算后的数据导入、导出相关地址和信息；例如，新建子目录等。
Function:configuration for input and output operations for system

last update 201024 | since 181106

Menu :
1, standarded db
2, innovative apps

Notes:
===============================================
'''
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db；# C:\ciss_web\CISS_rc\config
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

### 导入数据配置信息,大部分配置地址都在config_data里
from config_data import config_data
config_data_1 = config_data()

###################################################
class config_IO():
    def __init__(self):
        ###generate object 
        self.obj_config = {}
        self.obj_config["sys"] = sys
        self.obj_config["dict"] ={}
        ######################################################################
        ### level 1 目录； ['C:\\zd_zxjtzq\\ciss_web', '\x07pps\x07ctive_bm']
        path_ciss_web = os.getcwd().split("CISS_rc")[0]
        self.obj_config["dict"]["path_ciss_web"] = path_ciss_web  
        ### level 2 目录； "CISS_rc\\"
        self.obj_config["dict"]["path_ciss_rc"] = path_ciss_web + "CISS_rc\\"
        ### level 3 目录； "apps\\"，
        self.obj_config["dict"]["path_bin"] = self.obj_config["dict"]["path_ciss_rc"] + "bin\\"
        self.obj_config["dict"]["path_db"] = self.obj_config["dict"]["path_ciss_rc"] + "db\\"
        self.obj_config["dict"]["path_apps"] = self.obj_config["dict"]["path_ciss_rc"] + "apps\\"
        self.obj_config["dict"]["path_config"] = self.obj_config["dict"]["path_ciss_rc"] + "config\\"
        ### level 4 目录；"apps\\rc_data\\"，
        self.obj_config["dict"]["path_rc_data"] = self.obj_config["dict"]["path_ciss_rc"] + "apps\\rc_data\\"
        ######################################################################
        ### CISS_db\\ 位置 | path_db_wind from path0
        if not os.path.exists( "C:\\CISS_db\\" ) : 
            self.obj_config["dict"]["path_ciss_db"] = "D:\\CISS_db\\"   
        else :
            self.obj_config["dict"]["path_ciss_db"] = "C:\\CISS_db\\"  

        self.obj_config["dict"]["path_apps"]=  self.obj_config["dict"]["path_apps"] 
        self.obj_config["dict"]["path_active_bm"]=  self.obj_config["dict"]["path_apps"] +"active_bm\\" 
        
        ######################################################################
        ### 
        self.path_base =  "D:\\CISS_db\\"
        if not os.path.isdir( self.path_base  ) :
            os.mkdir( self.path_base )        
        self.path_base_data =  self.path_base+'data' + '\\'
        if not os.path.isdir(   self.path_base+'data' + '\\'   ) :
            os.mkdir(  self.path_base+'data' + '\\'  )    
    
    def print_info(self):        
        print("   ") 
        ### 创建数据应用输出的目录及子目录
        print("gen_config_IO |创建数据应用输出的目录及子目录 ")
        print("gen_config_IO_port |新建模拟组合文件，似乎只有以前ABM model用，201024 ")
        print("load_config_IO_port |载入模拟组合配置文件，似乎只有以前ABM model用，201024 ")
        
        
        return 1

    def gen_config_IO(self,path_base='',sys_name='') :
        ### 创建数据应用输出的目录及子目录
        ### CISS_db 位置 ：self.obj_config["dict"]["path_ciss_db"]
        config_IO ={}

        ## generate path base directory if not exists.
        config_IO['path_base'] = self.obj_config["dict"]["path_ciss_db"]
        if not os.path.isdir( self.obj_config["dict"]["path_ciss_db"] ) :
            os.mkdir( self.obj_config["dict"]["path_ciss_db"] )
        
        ### generate base directory for market quotations, which can be shared by all portfolios
        config_IO['path_base_data'] = config_IO['path_base'] + 'data' + '\\'   
        if not os.path.isdir( config_IO['path_base'] + 'data' + '\\'  ) :
            os.mkdir( config_IO['path_base'] + 'data' + '\\'  )                   
        
        ## generate strategy system if not exists yet.
        if sys_name =='' :
            sys_name = 'rc001'
        path_sys = config_IO['path_base_data'] + sys_name +'\\' 
        if not os.path.isdir( path_sys) :
            os.mkdir(path_sys)                   

        config_IO['path_sys'] = path_sys
        
        ## generate application and database directory
        path_apps = path_sys + 'apps' + '\\'
        if not os.path.isdir( path_apps) :
            os.mkdir(path_apps)                   
        config_IO['path_apps'] = path_apps
        
        path_data  = path_sys + 'data' + '\\'     
        if not os.path.isdir( path_data) :
            os.mkdir(path_data)                   
        config_IO['path_data'] = path_data
        
        ## generate AS,A_sum.TB,signal files 
        for dir_name in ["assets","ports",'accounts','stockpools','trades','signals' ] :
            path_temp  = path_sys + dir_name + '\\'     
            config_IO['path_'+dir_name ] = path_temp
            if not os.path.isdir( path_temp ) :
                os.mkdir(path_temp)  

        return config_IO

    def gen_config_IO_port(self,path_base='',port_name='') :
        # 新建模拟组合文件，似乎只有以前ABM model用，201024
        # generate all necessary configurations 
        config_IO ={}

        ## generate path base directory if not exists.
        config_IO['path_base'] = self.obj_config["dict"]["path_ciss_db"]
        
        ### generate base directory for market quotations, which can be shared by all portfolios
        path_base_data  = config_IO['path_base'] + 'data' + '\\'     
        if not os.path.isdir( path_base_data) :
            os.mkdir(path_base_data)                   
        config_IO['path_base_data'] = path_base_data

        ## generate strategy porttem if not exists yet.
        if port_name =='' :
            port_name = 'rc001'

        path_port = config_IO['path_base'] + port_name +'\\' 
        if not os.path.isdir( path_port) :
            os.mkdir(path_port)                   

        config_IO['path_port'] = path_port
        
        ## generate application and database directory
        path_apps = path_port + 'apps' + '\\'
        if not os.path.isdir( path_apps) :
            os.mkdir(path_apps)                   
        config_IO['path_apps'] = path_apps
        
        ### generate portfolio directory for market quotations,only used by self portfolio
        path_data  = path_port + 'data' + '\\'     
        if not os.path.isdir( path_data) :
            os.mkdir(path_data)                   
        config_IO['path_data'] = path_data
        
        ## generate AS,A_sum.TB,signal files 
        for dir_name in ["assets","ports",'accounts','stockpools','trades','signals' ] :
            path_temp  = path_port + dir_name + '\\'     
            config_IO['path_'+dir_name ] = path_temp
            if not os.path.isdir( path_temp ) :
                os.mkdir(path_temp)  

        return config_IO

    def load_config_IO_port(self,port_id='',path_base='',port_name='rc001'):
        ### 载入模拟组合配置文件，似乎只有以前ABM model用，201024
        # load configurations from existing directory 
        config_IO ={}
        ## set path base directory if not exists. 
        config_IO['path_base'] = self.obj_config["dict"]["path_ciss_db"]
        
        ## set strategy system if not exists yet.
        if port_name =='' :
            port_name = 'rc001'
        
        path_port = path_base + port_name +'\\'     
        config_IO['path_ports'] = path_port

        ## set application and database directory
        path_apps = path_port + 'apps' + '\\'             
        config_IO['path_apps'] = path_apps
        
        path_data  = path_port + 'data' + '\\'                        
        config_IO['path_data'] = path_data
        
        ## set AS,A_sum.TB,signal files 
        for dir_name in ["assets","ports",'accounts','stockpools','trades','signals' ] :
            path_temp  = path_port + dir_name + '\\'     
            config_IO['path_'+dir_name ] = path_temp


        return config_IO