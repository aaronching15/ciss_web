20220928
这张图片的账号  huarzq0593  就是有权限的，但是没有绑定手机号只能人工激活，您确认下这个账号的密码，然后走一下人工激活
人工激活方式：1. 把Manualactivate_all.py 这个文件放到installEmQuantAPI同级地方；
2. 先运行installEmQuantAPI，返回installed success之后；
3. 在Manualactivate.py这个文件里填上用户名，密码，和有效的邮箱，运行返回成功， 注意：email=字样不要省略；
4. 通知下我这边申请激活的账号，这边后台帮您处理.
邮件已发送到您填写的邮箱里。您将邮件里面 userinfo 下载下来 放到serverlist.json.e同级目录下，注意：userinfo是没有后缀的,激活生成的登陆令牌是绑定设备号的并且里面含有您的账号信息，在不更换设备和修改密码的情况下，有效期是1年。