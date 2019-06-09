import json
import pandas as pd 
import sys
sys.path.append("..") 


#####################################################################
import datetime as dt
### Import Wind index unit for given time 

date_start = "20170103"
date_start = "20170103"


path1= "C:\\zd_zxjtzq\\RC_trashes\\temp\\sys_stra_24h\\CISS_rc\\db\\db_assets\\"
file_ind3= "codelist_ind4.csv" 
ind_df = pd.read_csv( path1+ file_ind3,encoding="GBK" )
print("ind_df")
print(  ind_df.info() )
# "401010" banks | "601020" real-estates
# 351020
int_ind3= "351020"

temp_index = ind_df.index[ind_df['ind3_code']== int(int_ind3) ][0] # can be any index 
temp_WI = ind_df.loc[temp_index,"ind3_index_code"]
print( "temp_WI ",temp_WI  )


file_name = "Wind_"+temp_WI +"_updated.csv"
# file_name = "Wind_"+"884230.WI"+"_updated.csv"
path0 = "D:\\data_Input_Wind\\"
data_raw = pd.read_csv(path0+file_name)
print(data_raw.head()  )
data_raw['date']  = data_raw["Unnamed: 0"]
data_raw = data_raw.drop(["Unnamed: 0"],axis=1)


data_raw['date'] = pd.to_datetime( data_raw['date'] ,format="%Y-%m-%d") 
data_2= data_raw[ data_raw['date']>=date_start ]
temp_i = data_2.index[0]
data_2['unit'] =data_2['CLOSE']/data_2.loc[temp_i,'CLOSE']

print("data_2")
print(data_2.info() )
print(data_2.head() )

# p947,999/2207 22.1 Basic Plotting: plot || ts = ts.cumsum()
data_3 = data_2['unit']
data_3.index = data_2['date']

### Output file 
file_out = "unit_"+ temp_WI+"_"+date_start   + ".csv"
data_3.to_csv( "D:\\"+file_out )

print("data_3")
print(data_3 )
import matplotlib.pyplot as plt
plt.figure()
data_3.plot()
plt.show()
# data_2.plot( kind='scatter',x='date',y='unit')


# plt.plot(data_2['date'],data_2['unit'])


