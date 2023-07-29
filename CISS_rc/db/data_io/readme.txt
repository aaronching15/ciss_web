20221018：Ans:新建一个.py文件=myEncoder.py. Qs: python3运行报错：TypeError: Object of type 'type' is not JSON serializable解决方法。这个问题是由于json.dumps（）函数引起的。dumps是将dict数据转化为str数据，但是dict数据中包含byte数据所以会报错。解决：编写一个解码类 遇到byte就转为str。url=https://www.cnblogs.com/qiqi-yhq/articles/12557870.html
20200608：data_io.py文件日益变大后，需要拆分。
当前需要拆分的是因子模型class data_factor_model和data_wind
last  | since 20200608