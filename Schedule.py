from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from DataConnector import dataUpdater
from BackTest_multipro import config,backTest
from Sendmail import signal_notify
import logging
logging.basicConfig(filename='app.log', level=logging.INFO)
import datetime as dt
global_icon = None
global_scheduler = None


def backtestToSelect():

		try:
			today = str(dt.datetime.today().date())
			config['回测输出文件名'] = rf"output\{today}.csv"
			config['买入价格'] = "收盘"
			begin_date = config['回测开始时间']
			end_date = f"{today}"
			logging.info(f"{today} :start today 's stockselect")

			backTest(begin_date,end_date)
			logging.info(f"{today} : today 's stockselect succeed without exception")
		except Exception as e:
			print(f"{today} :stockselect failed with exception: {e}")
			logging.info(f"{today} : stockselect failed with exception: {e}")

def job_listener(event):
	logging.info(f"{dt.datetime.today().date()} : today 's dataUpdate succeed without fatal error")
	backtestToSelect()
	signal_notify()

# def testEvent():
#     print("This is for testing")
#     logging.info(f"Test Event trigggered !" )
#用来测试apscheduler的函数，就放着吧，dataUpdater执行一次要几十分钟，不适合用来测试joblistener

def autoUpdate():
	global global_scheduler
	scheduler = BackgroundScheduler()
	# 每天定时执行
	scheduler.add_job(dataUpdater, 'cron', hour=16, minute=00,misfire_grace_time=60)
	scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
	scheduler.start()
	global_scheduler = scheduler
	return global_scheduler

def on_exit():
	if global_scheduler:
		global_scheduler.shutdown(wait=False)  # 停止调度器