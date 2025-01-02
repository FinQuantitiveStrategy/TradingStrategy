
#coding:utf-8
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime as dt
import pandas as pd
import logging
logging.basicConfig(filename='app.log', level=logging.INFO)
def signal_notify():
	today = str(dt.datetime.today().date())
	yesterday= str(dt.datetime.today().date() - dt.timedelta(days=1))

	# 读取CSV文件
	todayfile_path = rf"output\{today}.csv"  
	yesterdayfile_path = rf"output\{yesterday}.csv"  
	todaydf = pd.read_csv(todayfile_path)
	yesterdaydf = pd.read_csv(yesterdayfile_path)
	content = None
  
	if todaydf.equals(yesterdaydf):
		print("两个DataFrame相同,今日无票")
		logging.info(f"{today} : 今日无票")

	else:
		df_diff = todaydf.drop_duplicates().merge(yesterdaydf.drop_duplicates(), indicator=True, copy=False)
		# 将 'stocknum' 列转换为字符串
		df_diff['stocknum'] = df_diff['stocknum'].astype(str)
		# 将多行合并成一行
		content = ';'.join(df_diff['stocknum'])
		send_mail(content)


def send_mail(content):
	today = str(dt.datetime.today().date())
	#设置SMTP地址
	host = 'smtp.qq.com'
	#设置发件服务器端口号，注意，这里有SSL和非SSL两种形式，qq SSL端口为465，非SSL为端口默认25
	port = "465"
	#设置发件邮箱
	sender = "617391241@qq.com"
	#设置发件邮箱的授权码 ,qq邮箱ssl发送需要先开启stmp并获取密码 
	pwd = 'thdvyebqhvjebbie' #16授权码
	#设置邮件接收人,发送给多人，隔开 
	receiver = 'xiaojiaozhi@126.com,magicalkejia@qq.com' 
	#需要发送附件的方法实例
	msg = MIMEMultipart()
	#设置发送头信息
	title=today+"今日新票"
	msg.add_header('subject', title) #设置邮件标题
	msg.add_header('from', sender)   # 设置发送人
	msg.add_header('to', receiver)   # 设置接收人
  #设置正文内容
	if content is None:
		msg.attach(MIMEText('今日无票', 'plain', 'utf-8'))
	else:
		msg.attach(MIMEText('今日新票'+content, 'plain', 'utf-8'))

	try:
		#注意！如果是使用非SSL端口，这里就要改为SMTP
		smtpObj = smtplib.SMTP_SSL(host, port)
		#登陆邮箱
		smtpObj.login(sender, pwd)
		#发送邮件,注意第二个参数是发送人抄送人地址
		smtpObj.sendmail(sender, receiver.split(','), msg.as_string()) 
		logging.info(f"{today} : 今日邮件发送成功~")
	except smtplib.SMTPException as e:
		logging.info(f"{today} : 今日邮件发送失败。{e}")
		print(e)
	finally:
		smtpObj.quit()





