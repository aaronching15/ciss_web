# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：configuration for input and output operations for system
last update 181106 | since 181106
Menu :
1, standarded db

2, innovative apps

todo:


Notes:


===============================================
'''
import os 
###################################################
class config_IO():
    def __init__(self,config_name=''):
        self.config_name = config_name 
        self.path_base =  "D:\\CISS_db\\"
        if not os.path.isdir( self.path_base  ) :
            os.mkdir(path_base )        
        self.path_base_data =  self.path_base+'data' + '\\'
        if not os.path.isdir(   self.path_base+'data' + '\\'   ) :
            os.mkdir(  self.path_base+'data' + '\\'  )    

    def gen_config_IO(self,path_base='',sys_name='') :
        # system
        # generate all necessary configurations 
        config_IO ={}

        ## generate path base directory if not exists.
        if path_base == '' :
            path_base = "D:\\CISS_db\\"
            if not os.path.isdir( path_base) :
                os.mkdir(path_base)
        config_IO['path_base'] = path_base

        ### generate base directory for market quotations, which can be shared by all portfolios
        path_base_data  = path_base + 'data' + '\\'     
        if not os.path.isdir( path_base_data) :
            os.mkdir(path_base_data)                   
        config_IO['path_base_data'] = path_base_data

        ## generate strategy system if not exists yet.
        if sys_name =='' :
            sys_name = 'rc001'
            path_sys = path_base + sys_name +'\\' 
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
        # porttem
        # generate all necessary configurations 
        config_IO ={}

        ## generate path base directory if not exists.
        if path_base == '' :
            path_base = "D:\\CISS_db\\"
            if not os.path.isdir( path_base) :
                os.mkdir(path_base)
        config_IO['path_base'] = path_base
        
        ### generate base directory for market quotations, which can be shared by all portfolios
        path_base_data  = path_base + 'data' + '\\'     
        if not os.path.isdir( path_base_data) :
            os.mkdir(path_base_data)                   
        config_IO['path_base_data'] = path_base_data

        ## generate strategy porttem if not exists yet.
        if port_name =='' :
            port_name = 'rc001'

        path_port = path_base + port_name +'\\' 
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
        # load configurations from existing directory 
        config_IO ={}
        ## set path base directory if not exists.
        if path_base == '' :
            path_base = "D:\\CISS_db\\" 
        config_IO['path_base'] = path_base
        
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