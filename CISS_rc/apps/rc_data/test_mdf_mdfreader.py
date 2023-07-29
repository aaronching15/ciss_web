
'''
Qs:程序跑不同，
Exception: file D:\db_factset\FDS.mdf is not an MDF file!

其他方案：
下载sqlserver2008r2将其转换为其他格式；或Aqua Data Studio 19 破解版

需求：
1，其他基金经理研究员需要CSV格式 ；
2，陈亘斯需要.h5 格式。

功能：
用mdfreader读取.mdf格式数据库


'''

import mdfreader

file_path="D:\\db_factset\\"
file_name="FDS.mdf"

yop=mdfreader.Mdf(file_path+file_name )

yop.keys() # list channels names
# list channels grouped by raster or master channel
yop.masterChannelList
yop.plot('channelName') or yop.plot({'channel1','channel2'})
yop.resample(0.1) or yop.resample()

file_output = "fds.h5"
# yop.export_to_csv(sampling=0.01)
# yop.export_to_NetCDF()

yop.export_to_hdf5( file_path + file_output  )


# yop.export_to_matlab()
# yop.export_to_excel()
# yop.export_to_xlsx()
# yop.convert_to_pandas() # converts data groups into pandas dataframes
# yop.write() # writes mdf file
# |  # drops all the channels except the one in argument
# yop.keep_channels(['channel1','channel2','channel3'])
# yop.get_channel_data('channelName') # returns channel numpy array
# yop=mdfreader.Mdf()  # create an empty Mdf object
# |  # add channel in Mdf object
# yop.add_channel(channel_name, data, master_channel, master_type, unit='lumen', description='what you want')
# yop.write('filename') # change version with yop.MDFVersionNumber or specifically use write3/4()
# |



