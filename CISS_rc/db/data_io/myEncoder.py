# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"


'''
notes:还没有起到什么作用


url= https://www.cnblogs.com/qiqi-yhq/articles/12557870.html
python3运行报错：TypeError: Object of type 'type' is not JSON serializable解决方法
首先网上大多数博客没有明确说明问题的来源

这个问题是由于json.dumps（）函数引起的。dumps是将dict数据转化为str数据，但是dict数据中包含byte数据所以会报错。
解决：编写一个解码类 遇到byte就转为str
1、新建一个.py文件

myEncoder.py
用法：需要转换dict时，
### myEncoder 没什么用 
# from myEncoder import MyEncoder
# json_data = json.dumps(dict_data ,cls=MyEncoder,indent=4)
# count = len( json_data )
# print("json data=",json_data  )

sice 20221018
'''''
import json


class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        """
        只要检查到了是bytes类型的数据就把它转为str类型
        :param obj:
        :return:
        """
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)
        