import sqlite3
import atexit

# 使用import导入该模块时:
# 必须只用import或import from, 禁止使用 import as

# new database logic
dbconnection = None

def dbconn():
	global dbconnection
	if dbconnection == None:
		dbconnection = sqlite3.connect(r'.\database\stocks_data.db', check_same_thread=False)
	return dbconnection


def dbCloseall(): #close db function
	if dbconnection != None:
		dbconnection.close()
	return 0

atexit.register(dbCloseall) #register safe close at program exit