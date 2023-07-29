# -*- coding:utf-8 -*-
from EmQuantAPI import *
import platform
#手动激活范例(单独使用)
#获取当前安装版本为x86还是x64
data = platform.architecture()
if data[0] == "64bit":
    bit = "x64"
elif data[0] == "32bit":
    bit = "x86"
data1 = platform.system()
if data1 == 'Linux':
    system1 = 'linux'
    lj = c.setserverlistdir("libs/" + system1 + '/' + bit)
elif data1 == 'Windows':
    system1 = 'windows'
    lj = c.setserverlistdir("libs/" + system1)
elif data1 == 'Darwin':
    system1 = 'mac'
    lj = c.setserverlistdir("libs/" + system1)
else:
    pass
#调用manualactive函数，修改账号、密码、有效邮箱地址，email=字样需保留
# data = c.manualactivate("账号", "密码", "email=有效邮箱地址")
### 账号：huarzq0593，密码：mq229314
data = c.manualactivate("huarzq0593", "mq229314", "email=aaronching@foxmail.com")
if data.ErrorCode != 0:
    print ("manualactivate failed, ", data.ErrorMsg)