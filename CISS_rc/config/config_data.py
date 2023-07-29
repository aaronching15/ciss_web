# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
1，Class info | Function: 功能：
1.1，功能：configuration for operating data.For instance, data from Wind API
1.2，config_data ：母类：主要配置目录path和数据参数para等
1.2，config_data_factor_model：子类：多因子模型配置目录和数据参数等 

3，关联脚本：对应数据文件 | db\data_io.py

4,OUTPUT:
    1,obj_1["dict"]，字典信息,json
    2,obj_1["df"]，表格信息,dataframe  

5,分析：目标是所有数据变量以object类型作为输入输出，其中主要是2个key:
    1,obj_1["dict"]:字典格式，数据io都采用json的字典格式。
    2,obj_1["df"]:DataFrame格式

Notes:
# os.getcwd()对应脚本当前工作目录，os.getcwd()[:2]一般是"C:"
例如：
    str = os.getcwd()
    str = "C:\zd_zxjtzq\ciss_web\CISS_rc\apps\active_bm"

last update 230118 | since 181107

===============================================
'''
import sys,os
# "C:\\rc_2023\\rc_202X\\ciss_web" 
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

###################################################
class config_data():
    ### 不同数据来源的原始数据
    def __init__(self):
        ###generate object 
        self.obj_config = {}
        self.obj_config["sys"] = sys
        self.obj_config["dict"] = {}

        ######################################################################
        ######################################################################
        ### 代码目录； "C:\\rc_202X\\rc_202X\\ciss_web" to "C:\\rc_2023\\rc_202X\\ciss_web" 
        path_root = os.getcwd().split("ciss_web")[0]  
        self.obj_config["dict"]["path_root"] = path_root

        path_ciss_web = os.getcwd().split("ciss_web")[0] +"ciss_web\\" 
        self.obj_config["dict"]["path_ciss_web"] = path_ciss_web  
        ### level 2 目录； "CISS_rc\\"
        self.obj_config["dict"]["path_ciss_rc"] = path_ciss_web + "CISS_rc\\"
        self.obj_config["dict"]["path_ciss_exhi"] = path_ciss_web + "ciss_exhi\\"
        
        ### level 3 目录； "apps\\"，
        self.obj_config["dict"]["path_bin"] = self.obj_config["dict"]["path_ciss_rc"] + "bin\\"
        self.obj_config["dict"]["path_db"] = self.obj_config["dict"]["path_ciss_rc"] + "db\\"
        self.obj_config["dict"]["path_db_times"] = self.obj_config["dict"]["path_db"] + "db_times\\" 
        self.obj_config["dict"]["path_db_assets"] = self.obj_config["dict"]["path_db"] + "db_assets\\"
        self.obj_config["dict"]["path_apps"] = self.obj_config["dict"]["path_ciss_rc"] + "apps\\"
        self.obj_config["dict"]["path_config"] = self.obj_config["dict"]["path_ciss_rc"] + "config\\"
        
        ### level 4 目录；"apps\\rc_data\\"，
        self.obj_config["dict"]["path_rc_data"] = self.obj_config["dict"]["path_ciss_rc"] + "apps\\rc_data\\"
        
        ######################################################################
        ######################################################################
        ### level 1 微云同步目录设置 | last 230118 | since 20211227
        self.obj_config["dict"]["path_data_sync"] = path_root
        self.obj_config["dict"]["path_data_0"] = path_root
        
        ######################################################################
        ### data_pms 组合管理文件夹，也保存wind-api获取的数据
        self.obj_config["dict"]["path_data_pms"] = self.obj_config["dict"]["path_data_sync"] + "data_pms\\"

        ######################################################################
        ### data_strategy\ 文件夹存放各类策略研究相关的数据,例如基金股债配置测算：fund_allocation
        self.obj_config["dict"]["path_data_strategy"] = self.obj_config["dict"]["path_data_pms"] + "data_strategy\\"

        ######################################################################
        ### choice-api 数据
        self.obj_config["dict"]["path_data_choice"] = self.obj_config["dict"]["path_data_sync"] + "data_choice\\"

        ######################################################################
        ### data_adj 计算后的文件：策略权重、策略指标、行业分类、AH动量策略等
        self.obj_config["dict"]["path_data_adj"] = self.obj_config["dict"]["path_data_pms"] + "data_adj\\"
        
        ######################################################################
        ######################################################################
        ### FUND 基金
        ######################################################################
        ### fund 基金池 
        self.obj_config["dict"]["path_fundpool"] = self.obj_config["dict"]["path_data_pms"] + "fund\\"

        ### 基金指标 
        self.obj_config["dict"]["path_fund_indi"] = self.obj_config["dict"]["path_fundpool"] + "fund_indi\\"
        
        ######################################################################
        ######################################################################
        ### wind_terminal ，wind终端导出数据
        self.obj_config["dict"]["path_wind_terminal"] = self.obj_config["dict"]["path_data_pms"] + "wind_terminal\\"    
        ### Wind导出的分类基金数据
        self.obj_config["dict"]["path_wind_fund"] = self.obj_config["dict"]["path_wind_terminal"] + "FF-基金研究\\"  
        
        ### wpd 下载的PMS组合日涨跌的时间序列
        self.obj_config["dict"]["path_wpd"] = self.obj_config["dict"]["path_data_pms"] + "wpd\\"

        ### wpf 下载的PMS组合持仓的时点数据
        self.obj_config["dict"]["path_wpf"] = self.obj_config["dict"]["path_data_pms"] + "wpf\\"
        ### wsd, wss
        self.obj_config["dict"]["path_wsd"] = self.obj_config["dict"]["path_data_pms"] + "wsd\\"
        self.obj_config["dict"]["path_wss"] = self.obj_config["dict"]["path_data_pms"] + "wss\\"
        
        ######################################################################
        ### Portfolio,PMS
        ######################################################################
        ### port_weight 组合权重
        self.obj_config["dict"]["path_port_weight"] = self.obj_config["dict"]["path_data_pms"] + "port_weight\\"

        ######################################################################
        ### 策略组合文件
        self.obj_config["dict"]["path_stra"] = self.obj_config["dict"]["path_data_pms"] + "strategy\\"

        ### 之前的恒生数据hs 和 通联数据 yes
        # self.obj_config["dict"]["path_data_hs"] = self.obj_config["dict"]["path_data_0"] +"data_hs\\"
        # self.obj_config["dict"]["path_data_yes"] = self.obj_config["dict"]["path_data_0"] +"data_yes\\"


        ######################################################################
        ######################################################################
        ### 2022以后暂时不用
        ######################################################################
        ### Wind数据目录:设置db_wind 数据文件位置，只读取数据
        if os.path.exists( "C:\\db_wind\\" ) : 
            self.obj_config["dict"]["path_disk"] = "C:\\" 
        elif os.path.exists( "D:\\db_wind\\" ) : 
            self.obj_config["dict"]["path_disk"] = "D:\\"   
        elif os.path.exists( "F:\\db_wind\\" ) :
            self.obj_config["dict"]["path_disk"] = "F:\\" 
        
        ### db_wind\\ 位置 | path_db_wind from path0
        self.obj_config["dict"]["path_db_wind"] = self.obj_config["dict"]["path_disk"]   +"db_wind\\"        
        
        ######################################################################
        ### level 1 目录:设置data_hs 恒生数据文件位置，只读取数据
        # self.obj_config["dict"]["path_data_hs"] = self.obj_config["dict"]["path_db_wind"] +"data_hs\\"        
        
        ######################################################################
        ### level 1 目录:设置datayes 通联数据文件位置，只读取数据
        # self.obj_config["dict"]["path_data_yes"] = self.obj_config["dict"]["path_db_wind"] +"data_yes\\"
        
        ###################################################################### 
        ### level 2 目录
        ### 整理后的wind wds数据 | path_wind_adj from path_rc
        self.obj_config["dict"]["path_wind_adj"] =self.obj_config["dict"]["path_db_wind"] + "data_adj\\" 

        ### 整理后的tushare数据 | 
        self.obj_config["dict"]["path_tushare"] =self.obj_config["dict"]["path_db_wind"] + "tushare\\" 

        ### 原始wind wds表格
        self.obj_config["dict"]["path_wind_wds"] =self.obj_config["dict"]["path_db_wind"] + "data_wds\\"

        ### level 3 目录
        ### 导入Wind全历史行业分类数据 || df_600151.SH
        self.obj_config["dict"]["path_rc_ind"] =self.obj_config["dict"]["path_wind_adj"] + "industries_class\\" 
        
        ######################################################################
        ######################################################################
        ### 内部数据库CISS_db\\ 位置 | path_db_wind from path0
        if not os.path.exists( "C:\\CISS_db\\" ) : 
            self.obj_config["dict"]["path_ciss_db"] = "D:\\CISS_db\\"   
        else :
            self.obj_config["dict"]["path_ciss_db"] = "C:\\CISS_db\\"  

        self.obj_config["dict"]["path_apps"]=  self.obj_config["dict"]["path_apps"] 
        self.obj_config["dict"]["path_active_bm"]=  self.obj_config["dict"]["path_apps"] +"active_bm\\" 
        
        ######################################################################
        ### 定义基础数据位置
        # 历史日、周、月； notes:20060403 基金无净值，手工删除
        self.obj_config["dict"]["file_date_tradingday"] = "date_list_tradingday.csv"
        self.obj_config["dict"]["file_date_week"] = "date_list_week.csv"
        self.obj_config["dict"]["file_date_month"] = "date_list_month.csv"
        self.obj_config["dict"]["file_date_quarter"] = "date_list_quarter.csv"
        self.obj_config["dict"]["path_dates"] = self.obj_config["dict"]["path_wind_adj"]
        
    
    def print_info(self):        
        print("   ") 
        ### 创建数据应用输出的目录及子目录
        print("class config_data |基础数据配置：包括所有相关的文件目录和位置 ")
        print("config_data_factor_model | 因子模型及相关位置path ")
        print("config_data_timing_abcd3d | abcd3d交易日市场状态择时模型及相关位置path ")
        print("config_data_fund_ana | 基金分析模型及相关位置path ")
        
        return 1

class config_data_factor_model():
    def __init__(self) :
        ### 继承父类config_data的定义，等价于
        config_data.__init__(self)

        ### factor_model 目录位置：
        self.obj_config["dict"]["path_factor_model"]= self.obj_config["dict"]["path_ciss_db"] +"factor_model\\"
        

class config_data_timing_abcd3d():
    def __init__(self) :
        ### 继承父类config_data的定义，等价于
        config_data.__init__(self)
        
        ### timing_abcd3d 目录
        self.obj_config["dict"]["path_timing_abcd3d"]= self.obj_config["dict"]["path_ciss_db"] +"timing_abcd3d\\"
        

class config_data_fund_ana():
    def __init__(self) :
        ### 继承父类config_data的定义，等价于
        config_data.__init__(self)
        
        ### 转换后的基金wds数据表
        self.obj_config["dict"]["path_wds_fund"]= self.obj_config["dict"]["path_wind_adj"] +"fund_ana\\"
        
        ### 基金分析数据输出目录
        self.obj_config["dict"]["path_ciss_db_fund"]= self.obj_config["dict"]["path_ciss_db"] +"fund_simulation\\"
        

###################################################
class config_db():
    ### 不同数据来源的原始数据
    def __init__(self):
        ###generate object 
        self.obj_config = {}
        self.obj_config["sys"] = sys
        self.obj_config["dict"] = {}

        ######################################################################
        ######################################################################
        ### 代码目录； 
        ### path_ciss_web = "C:\rc_202X\rc_202X\ciss_web\"
        path_ciss_web = os.getcwd().split("ciss_web")[0] + "ciss_web\\"   
        ### ciss_web 的根目录
        self.obj_config["dict"]["path_ciss_web"] = path_ciss_web  
        ### 数据库的位置 ### path_db = "C:\rc_202X\rc_202X\ciss_web\"
        self.obj_config["dict"]["path_db"] = path_ciss_web
        ### level 2 目录； "CISS_rc\\"
        self.obj_config["dict"]["path_ciss_rc"] = path_ciss_web + "CISS_rc\\"
        ### level 3 目录； "apps\\"，
        
        ### level 4 目录；"apps\\rc_data\\"，
        self.obj_config["dict"]["path_rc_data"] = self.obj_config["dict"]["path_ciss_rc"] + "apps\\rc_data\\"
        


 













    # def gen_config_wsd(self,type_wsd='week') :
    #     # generate all necessary configurations 
    #     if type_wsd in ['week','w','W','Week'  ]:
    #         # original
    #         # items = ["open","high","low","close","volume","amt","turn"]
    #         # short items
    #         items = ["open","close","volume","amt","turn"]
    #         para = "Period=W;PriceAdj=F"
    #     elif type_wsd in [ 'day','d','D','Day' ]:
    #         # original
    #         # items = ["open","high","low","close","volume","amt","turn"]
    #         # short items
    #         items = ["open","close","volume","amt"]
    #         para = "PriceAdj=F"

    #     elif type_wsd in [ 'day_us','day_hk' ]:
    #         # for US or HK market
    #         # items = ["open","high","low","close","volume","amt","turn"]
    #         # short items
    #         items = ["open","close","volume"]
    #         para = "PriceAdj=F"

    #     config={}
    #     config["items"] = items
    #     config["para"] = para
        
    #     return config