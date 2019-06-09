from django.db import models

# Create your models here.
################################################################################
### Head dictionary
################################################################################
### try sample model for ciss_web model | source https://docs.djangoproject.com/zh-hans/2.0/intro/tutorial02/
from django.contrib.auth.models import User
from django.utils import timezone

class User_ciss(models.Model):
    ### 看到被分配的所有策略，自己开发的所有策略
    ### from default setting of django   
    user_ciss = models.ForeignKey(User, on_delete=models.CASCADE)
    ### define User :analyst,pm,fund manager,head of team,leader, client,risks  
    # positions and groups
    position = models.CharField(max_length=200)
    group_ciss = models.CharField(max_length=200)
    # strategy owned 
    user_stra = models.ForeignKey('Strategy', on_delete=models.CASCADE )
    user_port = models.ForeignKey('Portfolio', on_delete=models.CASCADE  )
    user_assett = models.ForeignKey('Asset', on_delete=models.CASCADE  )
    user_data = models.ForeignKey('DB', on_delete=models.CASCADE  )
    # if user is in job list and if user is active 
    user_date_in = models.DateTimeField('date include')
    user_date_last = models.DateTimeField('date last active')
    user_active = models.BooleanField(default=False)

class Asset(models.Model):
    ### Define asset infomation 
    asset_industry = models.CharField(max_length=200,default="bank")

    stock = "股票|stock"
    bond = "债券|bond"
    index = "指数|index"
    derivative="衍生品|derivative"
    commodity="商品|commodity"

    choices_type_asset=(
        (stock,"股票|stock"),(bond,"债券|bond"),(index,"指数|index"),
        (derivative,"衍生品|derivative"),(commodity,"商品|commodity"),
        )
    asset_type = models.CharField(max_length=200,
        choices= choices_type_asset ,
        default= stock,)
    ###    
    asset_market = models.CharField(max_length=200)
    ###
    CN ="中国内地|China"
    HK ="中国香港|Hong Kong"
    US ="美国|United States"
    EU = "欧盟|EU"
    BR = "英国|British"
    choices_type_country=(
        (CN,"中国内地|China"),(HK,"中国香港|Hong Kong"),(US,"美国|United States"),
        (EU,"欧盟|EU"),(BR,"英国|British"),
        )

    asset_country = models.CharField(max_length=200,
        choices= choices_type_country ,
        default= CN,)

    def is_core_asset(self) :
        # define stock and bond in China markets to be core assets  
        return (self.asset_type in (self.stock,self.bond)) and ( self.country in self.CN )
    

class Strategy(models.Model):
    '''
    Strategy structure
    1，head information of strategy
    2, strategy hierachy
    3, economic model and infomation system
    last | since 190114
    '''
    ### 包括了策略的生命周期：设计阶段，模型阶段，模拟实盘，实盘，...
    stra_name = models.CharField(max_length=200,default="rc_abm_01")
    stra_code = models.CharField(max_length=200,default="port_symbol_01")
    stra_intro = models.CharField(max_length=200,default="intro")
    stra_target = models.CharField(max_length=200,default="price_or_return")
    stra_link = models.CharField(max_length=200,default="stra_abm_rc")

    stra_report_type = models.CharField(max_length=200,default="funda")
    stra_hier_1 = models.CharField(max_length=200,default="hier_1")
    stra_hier_2 = models.CharField(max_length=200,default="hier_2")
    stra_hier_3 = models.CharField(max_length=200,default="hier_3")
    stra_hier_4 = models.CharField(max_length=200,default="hier_4")
    stra_report_type = models.CharField(max_length=200,default="funda")

    stra_author = models.CharField(max_length=200,default="rc")
    stra_supervisor = models.CharField(max_length=200,default="rc")
    stra_client = models.CharField(max_length=200,default="gy")

    stra_date_pub = models.DateTimeField('Date published',default=timezone.now)
    stra_date_last = models.DateTimeField('Last update',default=timezone.now)
    ### notes:下边这种是Django在migrations里的标准写法。
    # stra_port1 = models.ForeignKey(default='rc_a1', on_delete=models.deletion.CASCADE, to='Portfolio')

    stra_port_list = models.ManyToManyField('Portfolio', through='Stra_Port_links')

        ### todo not clear about manytomany cases , p1123 in django.pdf
    # stra_port = models.ManyToManyField(
    #     'Portfolio',
    #     through='Stra_Port_links',
    #     through_fields=('Strategy', 'Portfolio') ,)
    def __str__(self):
        return self.stra_name

class Portfolio(models.Model):
    ### 对应的策略，模拟组合本身。
    port_name = models.CharField(max_length=200,default="rc_abm")
    port_type = models.CharField(max_length=200,default="market")
    port_author = models.CharField(max_length=200,default="rc")
    port_supervisor = models.CharField(max_length=200,default="rc")
    port_client = models.CharField(max_length=200,default="rc")
    port_date_pub = models.DateTimeField('date published',default=timezone.now)
    port_date_last = models.DateTimeField('Last update',default=timezone.now)
    # port_stra1 = models.ForeignKey(default='rc_a1', on_delete=models.deletion.CASCADE, to='Strategy')
    # ## todo not clear about manytomany cases 
    # port_stra = models.ManyToManyField(
    #     'Strategy',
    #     through='Stra_Port_links',
    #     through_fields=('Strategy', 'Portfolio') , )
    port_id = models.CharField(max_length=200,default="1544021284")
    port_path = models.CharField(max_length=200,default="D:\\CISS_db\\")
    port_name = models.CharField(max_length=200,default="port_rc181205_market_value_999 - Copy")
    ###
    
    
    def __str__(self):
        return self.port_name

class Stra_Port_links(models.Model):
#     ### todo not clear about manytomany cases , p1123 in django.pdf
#     # N:N relationship between Strategy and Portfolio
    portfolio = models.ForeignKey('Portfolio', on_delete=models.CASCADE)
    strategy = models.ForeignKey('Strategy', on_delete=models.CASCADE)
#     ### contact person who maintain such links 
    user_connect = models.CharField(max_length=64,default="rc")
    date_connect = models.DateField()


################################################################################
class Person(models.Model):
    name = models.CharField(max_length=128)

    def __str__(self):
        return self.name

class Group(models.Model):
    name = models.CharField(max_length=128)
    members = models.ManyToManyField(Person, through='Membership')

    def __str__(self):
        return self.name

class Membership(models.Model):
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    date_joined = models.DateField()
    invite_reason = models.CharField(max_length=64)
################################################################################

class Multi_asset(models.Model):
    ### Multi Asset Allocation,多资产配置
    
    asset_name = models.CharField(max_length=200)
    asset_type = models.CharField(max_length=200)
    asset_author = models.CharField(max_length=200,default="rc")
    asset_supervisor = models.CharField(max_length=200)
    asset_client = models.CharField(max_length=200,default="rc")
    asset_date_pub = models.DateTimeField('date published')

    asset_group = models.CharField(max_length=200) 


class DB(models.Model):
    ### No clear ideas yet 
    name_db = models.CharField(max_length=200)
    init_date = models.DateTimeField('date initialization')



################################################################################
# class Choice(models.Model):
#     question = models.ForeignKey(Question, on_delete=models.CASCADE)
#     choice_text = models.CharField(max_length=200)
#     votes = models.IntegerField(default=0)








################################################################################
### Level 1 Content tables
################################################################################

class Basics(models.Model):
    # Define industry ,tradable time for asset 
    industry =  models.CharField(max_length=200)
    type_asset = models.CharField(max_length=200)
    date_published = models.DateTimeField('date published')
    date_last = models.DateTimeField('date last traded')

class Stock(models.Model):
    ### Define asset infomation 
    stock_asset = models.ForeignKey('Asset', on_delete=models.CASCADE  )

    stock_stra = models.ForeignKey('Strategy', on_delete=models.CASCADE  )
    stock_port = models.ForeignKey( Portfolio, on_delete=models.CASCADE  )

class Index(models.Model):
    ### Define Benchmark == Index
    bench_asset = models.ForeignKey('Asset', on_delete=models.CASCADE  )

    bench_stra = models.ForeignKey('Strategy', on_delete=models.CASCADE  )
    bench_port = models.ForeignKey('Portfolio', on_delete=models.CASCADE  )

class Bond(models.Model):
    ### Define asset infomation 
    bond_asset = models.ForeignKey('Asset', on_delete=models.CASCADE  )

    bond_stra = models.ForeignKey('Strategy', on_delete=models.CASCADE  )
    bond_port = models.ForeignKey('Portfolio', on_delete=models.CASCADE  )

class Cash_tool(models.Model):
    ### Define asset infomation 
    bond_asset = models.ForeignKey('Asset', on_delete=models.CASCADE  )

    bond_stra = models.ForeignKey('Strategy', on_delete=models.CASCADE  )
    bond_port = models.ForeignKey('Portfolio', on_delete=models.CASCADE  )

################################################################################
### Level 2 Content tables
################################################################################

class Stock_derivative(models.Model):
    ### Define asset infomation 
    deriv_stock = models.ForeignKey('Stock', on_delete=models.CASCADE  )
    deriv_type  =  models.CharField(max_length=200)

    deriv_stra = models.ForeignKey('Strategy', on_delete=models.CASCADE  )
    derive_port = models.ForeignKey('Portfolio', on_delete=models.CASCADE  )

class Bond_derivative(models.Model):
    ### Define asset infomation 
    deriv_bond = models.ForeignKey('Bond', on_delete=models.CASCADE  )
    deriv_type  =  models.CharField(max_length=200)

    deriv_stra = models.ForeignKey('Strategy', on_delete=models.CASCADE  )
    derive_port = models.ForeignKey('Portfolio', on_delete=models.CASCADE  )

class Index_derivative(models.Model):
    ### Define asset infomation 
    deriv_Index = models.ForeignKey( 'Index' , on_delete=models.CASCADE )
    deriv_type  =  models.CharField(max_length=200)

    deriv_stra = models.ForeignKey( 'Strategy', on_delete=models.CASCADE  )
    derive_port = models.ForeignKey( 'Portfolio', on_delete=models.CASCADE  )