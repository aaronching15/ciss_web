

# read json file and load results
import json

# dir0= "C:\\zd_zxjtzq\\RC_trashes\\temp\\keti_sys_stra_24h\\templates"
# filename = 'json_1.json'
# path_file = dir0 + '\\' + filename

dir1 = "C:\\zd_zxjtzq\\RC_trashes\\temp\\keti_sys_stra_24h"
filename1 = "keti_opensource_py-fi-invest.json"
path_file = dir1 + '\\' + filename1

f = open(path_file, encoding='utf-8')
json_obj = json.load(f)
print("json_obj[\"head\"][\"topic\"]" )
print(json_obj["head"]["topic"])
print(json_obj["head"]["dir"])
print(json_obj["head"]["annotation"])
print('=======================')
print(json_obj["body"].keys() )




