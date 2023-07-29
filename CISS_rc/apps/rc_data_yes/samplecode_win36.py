# -*- coding: utf-8 -*-
from dataapi_win36 import Client
if __name__ == "__main__":
    try:
        client = Client()
        client.init('your token')
		#方式1，直接调取数据，不做任何处理
        url1='/api/master/getSecID.json?field=&assetClass=&ticker=688001,000001&partyID=&cnSpell='
        code, result = client.getData(url1)#调用getData函数获取数据，数据以字符串的形式返回
        if code==200:
            print(result.decode('utf-8'))#url1须为json格式，才可使用utf-8编码
			#pd_data=pd.DataFrame(eval(result)['data'])#将数据转化为DataFrame格式
        else:
            print (code)
            print (result)
		#方式2，调取数据后写入csv格式文件
        url2='/api/master/getSecID.csv?field=&assetClass=&ticker=688001,000001&partyID=&cnSpell='     #url2须为csv格式才可使用GB18030编码，即将url1中的.json替换为.csv
        code, result = client.getData(url2)
        if(code==200):
            file_object = open('getSecID.csv', 'w')
            file_object.write(result.decode('GB18030'))
            file_object.close( )
        else:
            print (code)
            print (result) 
    except Exception as e:
        #traceback.print_exc()
        raise e