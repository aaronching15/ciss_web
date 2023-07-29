# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"


'''
#######################################################################
Qs: 暂时没能解决 
# 




#######################################################################
基本操作：
开启数据库服务
cd C:\Program Files\PostgreSQL\12\bin
pg_ctl start -w -D "D:\CISS_db\ciss_db"

进入命令行：进入控制环境：psql -U rc -d ciss_db

查看当前用户和数据库： ciss_db-# \c
    您现在已经连接到数据库 "ciss_db",用户 "rc".



CREATE USER MAPPING FOR public SERVER foreign_server OPTIONS (user "wind", password "wind")
CREATE FOREIGN TABLE foreign_table_t2(id int,name varchar(10)) SERVER foreign_server options(schema_name 'public',table_name 'AIndexEODPrices')



#######################################################################
1，新建数据库
命令：
C:\Program Files\PostgreSQL\12\bin>initdb -D D:\CISS_db\ciss_db -E UTF8

2，注册服务或增加一个服务
即将postgres注册为服务，服务名为pgsql
pg_ctl register -D D:\CISS_db\ciss_db -Npgsql

Qs:pg_ctl: 无法打开服务管理器
Ans:管理员运行cmd

3，启动服务
net start pgsql
或
pg_ctl start -w -D "D:\CISS_db\ciss_db"

4，创建测试数据库
createdb -E UTF8 ciss_db
5,命令行连接测试，说明已经配置成功
psql -hlocalhost -d ciss_db
# 进入用户界面
select * from pg_class

5，创建用户postgres,密码同样是postgres:
net user postgres postgres /add
net user rc password /add

6,创建PostgreSQL用户和它要找的那个相符
createuser --superuser rc

7,以用户rc进入数据库ciss_db 
psql -U rc -d ciss_db
#######################################################################
pgAdmin 连接数据库
1，"Add new server : 输入数据库名称、连接地址127.0.0.1，5432，主要是用户名和密码要对


#######################################################################
连接oracle数据库

1,数据封装器fdw（Foreign Data Wrappers）在PostgreSQL中相当于oracle中的dblink，可以很方便的操作其他数据库中的数据。




source:https://blog.csdn.net/weixin_34405925/article/details/89624140
安装oracle_fdw
1.下载oracle_fdw
点击github下载。注意，你要下载和你postgresql版本项目的安装包。我就是下载的不一样的导致这里找了好久。
2.将文件移动到pg安装路径下
下载完成将zip包解压，把【lib】文件夹的oracle_fdw.dll和【share/extension】目录下的三个文件分别复制到PostgreSQL安装目录下的【lib】文件夹和【share/extension】目录里去。

pre steps：
1，运行数据库： pg_ctl start -w -D "D:\CISS_db\ciss_db" 
2，进入控制环境：psql -U rc -d ciss_db
3，

-- 创建oracle_fdw
create extension oracle_fdw
-- 语句能查询到oracle_fdw extension，如下图
select * from pg_available_extensions
notes:这一步要成功才算数。

link：https://blog.csdn.net/ljinxin/article/details/77896295
1，oradb为外部服务器名(可自定义名称)；oradatabase为需要访问的oracle数据库名，或为在tnsnames.org配置的数据库实例名 */
create server oradb foreign data wrapper oracle_fdw options(dbserver '10.232.195:1521/wind')
2，rc为Postgre数据库用户*/ 
grant usage on foreign server oradb to rc
3，rc为Postgre数据库用户;orauser为被访问的oracle数据库用户名，orapwd为密码*/
create user mapping for rc server oradb options(user 'orauser', password 'orapwd')
create user mapping for rc server oradb options(user 'wind', password 'wind')

4,oratab为在Postgres数据库显示的外部表名（可自定义）
'ORAUSER为oracle数据库用户名，DEPT为需要访问的Oracle数据库的表，两者均要大写*/ 
create foreign table oratab ( deptno integer, dname character varying(20), 
    loc character varying(20)) server oradb options(schema 'ORAUSER', table 'DEPT');
select * from oratab;

create foreign table table1 ( deptno integer, dname character varying(20), loc character varying(20)) server oradb options(schema 'rc', table 'AIndexEODPrices')
select * from table1





CREATE SERVER oradb_bill FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '10.232.195:1521/wind')
create user mapping for bill server oradb_bill options(user 'wind',password 'wind')
GRANT USAGE ON FOREIGN SERVER oradb_bill TO bill
create foreign table fdw_test1(id int,info varchar(10)) server oradb_bill options(schema 'orauser',table 'TEST1')

select * from fdw_test1


1、安装postgres_fdw扩展
create extension postgres_fdw 
2、CREATE SERVER
create server fdw_server1
foreign data wrapper postgres_fdw 
options (host'10.232.195',port'1521',dbname'wind')
CREATE SERVER
3、CREATE USER MAPPING
create user MAPPING FOR bill       
server fdw_server1 
options (user'wind',password'wind')
CREATE USER MAPPING
4、创建FOREIGN TABLE
create foreign table fdw_t1(id int ,info text)
server fdw_server1 
options(schema_name'public',table_name'AIndexEODPrices')
CREATE FOREIGN TABLE

5,验证—在本地查询fdw_t1 ||—远程服务器查询t2 
select * from fdw_t1
select * from 'AIndexEODPrices'


'''
import psycopg2
import datetime as dt 
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
#################################################################################
# Python连接PostgreSQL数据库  https://www.yiibai.com/postgresql/postgresql_python.html

conn = psycopg2.connect(database="ciss_db", user="rc", password="shen_dn74", host="127.0.0.1", 
    port="5432")
cur = conn.cursor()

# #### create table 
# # cur.execute('''CREATE TABLE COMPANY
# #        (ID INT PRIMARY KEY     NOT NULL,
# #        NAME           TEXT    NOT NULL,
# #        AGE            INT     NOT NULL,
# #        ADDRESS        CHAR(50),
# #        SALARY         REAL);''')
# cur.execute("SELECT * FROM table1 LIMIT 10")
# rows = cur.fetchall()
# print(rows)


# conn.commit()
# conn.close()
# asd

###################################################################################
### 2.使用pandas的to_sql导入
# 这种方式要用到两个模块pandas和sqlalchemy
# 虽然比insert要慢了一些，但是这种方式的数据库内存占用小，不会对数据库造成压力。
# 另外要注意其中的to_sql参数说明情况：index = False，不写入索引列，否则必须在目标表中构建一列index。
# chunksize，一次性写入的数据量，默认为全部数据一次性写入，可以根据性能进行修改。

start = dt.datetime.now() 
### Get df object 
path0 = "C:\\db_wind\\"
temp_table = "AIndexEODPrices"
temp_path = path0+ temp_table
code_wind = "000300.SH"
file_name = "WDS_S_INFO_WINDCODE_"+code_wind  +"_ALL.csv"
# 不带列index值读取。
df_csi300 = pd.read_csv( temp_path +"\\"+ file_name,encoding="gbk",index_col=0  )
print("df_csi300")
print( df_csi300.head().T )

### Create engine 
# engine = create_engine('postgresql://user:password@host:port/database')
engine = create_engine('postgresql+psycopg2://rc:shen_dn74@127.0.0.1:5432/ciss_db')

table_name = temp_table + "_" + "000300.SH"
df_csi300.to_sql( table_name, engine,index= False, if_exists='replace')
# chunksize = 1000 表示一次写入1000行
# if_exists : {‘fail’, ‘replace’, ‘append’}, default ‘fail’
# • fail: If table exists, do nothing.
# • replace: If table exists, drop it, recreate it, and insert data.
# • append: If table exists, insert data. Create if does not exist.
conn.commit()


end = dt.datetime.now()
print('time cost:',(end - start))

#######################################################################################
### 查询数据库里所有表格和表格列名
from sqlalchemy import inspect
inspector = inspect(engine)

for table_name in inspector.get_table_names():
    print("table name :", table_name)
    for column in inspector.get_columns(table_name):
        print("Column: %s" % column['name'])


#######################################################################################
### 读取表格数据 ||notes:还没成功！！！读取不到上述的表格。
table_name = "AIndexEODPrices_000300.SH"
prime_key = "TRADE_DT"
prime_key_value = "20200109"

# params = {'TRADE_DT': prime_key_value }
# temp_sql = "SELECT * FROM " + table_name +" WHERE "+ prime_key +  "="+prime_key_value
# temp_table1 = cur.execute(temp_sql,params )
# temp_table1 = cur.execute(temp_sql )
# temp_data = temp_table1.fetchall()

temp_sql = "SELECT * FROM " + table_name +" WHERE "+ prime_key +  "="+prime_key_value
df_pgsql = pd.read_sql_query(temp_sql, con=conn  )

print( df_pgsql.head()  )
print( df_pgsql.info()  )
df_pgsql.to_csv("D:\\df_pgsql.csv") 

#######################################################################################
###3.使用copy_from导入
# 相比于前面两种传统的都是利用insert命令的插入方式，这种方式其实是先将结果写入缓存文件中，然后利用copy_from方法直接将文件复制到目标表中的操作，
# 这种操作的用时令我有点震精，写入13642条数据用时不到1s。
#  这种方式直接用了psycopg2模块中的copy_from方法，写入速度最快。
#  dataframe类型转换为IO缓冲区中的str类型

# start = dt.datetime.now() 
# output = StringIO()
# df_csi300.to_csv(output, sep='\t', index=False, header=False)
# output1 = output.getvalue()
 
# conn = pgconnection()
# cur = conn.cursor()

# table_name2 = temp_table + "_" + "000300.SH_test"
# cur.copy_from(StringIO(output1),table_name2,columns=df_csi300.columns )

# conn.commit()
 
# cur.close()
# conn.close()
 
# end = dt.datetime.now()
# print('time cost:',(end - start))

#######################################################################################

### ??? 还没成功
table_name = "AIndexEODPrices_000300.SH"
prime_key = "TRADE_DT"
prime_key_value = "20200109"

params = {'TRADE_DT': prime_key_value }
temp_sql = "SELECT * FROM " + table_name +" WHERE "+ prime_key +  "="+prime_key_value
temp_table1 = cur.execute(temp_sql,params )
temp_data = temp_table1.fetchall()

print("temp_data ")
print(temp_data)




### dataframe 直接从数据库中读取数据
df = pd.read_sql(query.statement, query.session.bind)
# pandas.read_sql_table(table_name, con, schema=None, index_col=None, coerce_float=True,parse_dates=None, columns=None, chunksize=None)







