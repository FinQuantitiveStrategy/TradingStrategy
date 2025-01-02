
#在开头设置好全局参数后即可开始回测
import pandas as pd
from MyTT import *
from 公式库 import *
from numpy import *
import time
import multiprocessing
import os
import importlib
import logging
#无视警告
import warnings
from DataConnector import query_stock_data,query_stock_heatdata,query_stock_basicinfo,query_single_stock_point,query_stock_fiveline
from DBAccess import dbconn, dbCloseall
from StockPool import StockSet
warnings.filterwarnings("ignore")
logging.basicConfig(
	filename='backtest_error.log',
	level=logging.ERROR,
	format='%(asctime)s - %(levelname)s - %(message)s'
)
#初始化部分，设立数据库连接、全局性的参数
programStartTime = time.time()

#region 设置全局参数.
config = {
	"策略路径": "StrategyPool",
	"策略": [
		"templateStrategy",
		"Buy4win"
	], #hotStockFollow 热度策略  OldtrendFollew右侧追击反转策略 trendFollew历史新高趋势追随策略 Buy4win买就赢利策略 Fiveline五线谱策略
	'额外数据需求': {
		# None
		# 'basic_stock_info': query_stock_basicinfo,
		# 'redu': query_stock_heatdata,
		# 'hottopic': query_single_stock_point
		# 'fiveline': query_stock_fiveline
	},  #没有设置为none
	"买入价格" : "开盘",   #收盘回测以当天收盘价格买入，否则以第二天开盘价格买入
	#  回测板块、剔除ST、basic_stock_info 等信息通过下面SQL语句查询和筛选
	# "样本数" : 3107,#上证1697 深圳1504 全样本3201去掉st3107   
	# "回测板块" : ["上证主板","深圳主板"], #要回测哪些板块就写哪些，分了4个
	# "剔除ST" : True,  #True剔除ST股票
	'回测开始时间' : "2018-01-01",
	"回测结束时间" : "2024-10-14",
	"使用进程数" : multiprocessing.cpu_count(),#自动获取，一般不会错
	'回测输出文件名' : "回测输出.csv"
	# '回测输出文件名' : "主力建仓+五线谱超跌+14型买入+财务评分60买+90天内dif二次卖+急涨大绿柱止盈+90天外超级趋势卖+开盘价计算.csv"
}
buy_price_type = '收盘' if config['买入价格'] == "收盘" else 'next_open'
#endregion



# region 调整股票池带基本面 
stockset = StockSet()
# condition = """SELECT basic_stock_info.StockCode,common_indicators.ReportDate,common_indicators.ROE
#     FROM basic_stock_info
#     JOIN common_indicators  ON basic_stock_info.StockCode = common_indicators.StockCode
#     WHERE common_indicators.ReportDate >= '2018-01-01' AND common_indicators.ROE > 0.15"""

condition = """SELECT *
	FROM basic_stock_info
	WHERE FinancialRate > 0
	AND ST = 0
	AND (Sector = '上证主板' OR Sector = '深圳主板')"""
stockcodedf = stockset.custom_query(condition)
if stockcodedf.empty == True:
	print("查询出错，请检查SQL")
if stockcodedf.shape[0] <= 0:
	print("回测样本为0，请检查筛选条件")

#endregion


# 导入策略池
module = importlib.import_module(config["策略路径"])

 

#region 回测单支股票
def calcStock(stocknum,start,end, strategyname): 
	strategy = getattr(module, strategyname)
	start_time = time.time()
	try:
		debugout = '' # 过程打印缓存

		debugout = debugout + f"{stocknum} "


		# -------回测数据加载 ------
		#初始化--加载需要使用的历史数据（包括K线或者其它数据）
		
		conn = dbconn()

		kilne =query_stock_data(
		code_to_query = stocknum,
		start_date = start, 
		end_date = end,
		)

		if len(kilne.index)<50:
			return  [stocknum, [], f"{stocknum} 数据长度不足"]
		data_to_pass = {}
		data_to_pass['kline'] = kilne

		if config['额外数据需求'] is not None:
			for data_type, data_function in config['额外数据需求'].items():
				if  data_type == 'basic_stock_info':
					data = data_function(conn,True)
				else:
					data = data_function(conn,
										stocknum,
										start,
										end)
				data_to_pass[data_type] = data
		init_time = time.time() - start_time  # 获取此前代码块运行消耗时间
		debugout = debugout + f"数据用时 {init_time:.4f} "

		# 执行交易策略
		df = strategy(**data_to_pass)
		df['pos'].fillna(value=0, inplace=True)
		if buy_price_type == 'next_open':
			df['next_open'] = df['开盘'].shift(-1)
		initial_cash = 10000
		cash = initial_cash
		holdings = 0
		transactions = []
		average_buy_price = 0
		buy_date = None  # 用于记录买入日期
		for index, row in df.iterrows():
			buy_price = row[buy_price_type]
			sell_price = row['收盘']
			action = row['pos']  
			# 买入,起码得能买一股（资金10000的设置很多票没法采用至少一手100股的限制）
			if action == 1 and cash > buy_price:  
				num_shares = (cash * row['volpercent']) // buy_price
				if num_shares == 0:
					continue
				cash -= num_shares * buy_price
				if holdings > 0:
					# 更新平均买入成本
					average_buy_price = (average_buy_price * holdings + buy_price * num_shares) / (holdings + num_shares)
				else:
					average_buy_price = buy_price
				holdings += num_shares
				buy_date = row['日期']  # 记录买入日期
				transactions.append({'stocknum':stocknum,
									'date': row['日期'], 
									'type': 'Buy', 
									'price': buy_price, 
									'shares': num_shares, 
									'average_price': average_buy_price,  # 记录买入时的平均成本
									'remaining_cash': cash, 
									'remaining_shares': holdings})

			elif action == 2 and holdings > 0:  # 卖出
				shares_to_sell = holdings * row['volpercent']
				cash += shares_to_sell * sell_price
				holdings -= shares_to_sell
				sell_date = row['日期']
				hold_time = (pd.to_datetime(sell_date) - pd.to_datetime(buy_date)).days
				profit_percentage = ((sell_price - average_buy_price) / average_buy_price) * 100  # 计算收益率
				transactions.append({'stocknum':stocknum,
									'date': row['日期'], 
									'type': 'Sell',
									'price': sell_price, 
									'shares': -shares_to_sell,
									'held_time': hold_time,
									'remaining_cash': cash, 
									'remaining_shares': holdings,
									'yield': profit_percentage # 收益率
									})

		loop_time = time.time() - start_time # 获取此前代码块运行消耗时间
		debugout = debugout + f"全部用时 {loop_time:.4}"
		#print(debugout)
		result = [stocknum, transactions, debugout]
		return result
	except Exception as e:
		# 记录详细错误信息
		logging.error(f"Error occurred in calcStock with stocknum: {stocknum}, "
					f"Error: {e}")
		print('Error:', f"{e}")
		result = [stocknum, [], f"{e}"]
		return result
#endregion

#region 输出调整
def sortSheets(df):
	buy_df = df[df['type'] == 'Buy'].reset_index(drop=True)
	sell_df = df[df['type'] == 'Sell']

	rows = []
	for buy_index, buy_row in buy_df.iterrows():
		# 找到所有日期在买入之后的卖出记录
		post_buy_sells = sell_df[(sell_df['stocknum'] == buy_row['stocknum']) & 
								 (pd.to_datetime(sell_df['date']) > pd.to_datetime(buy_row['date']))]
		post_buy_sells = post_buy_sells.sort_values('date')
		
		# 用于标记是否有卖出记录
		has_sells = False
		
		for _, sell_row in post_buy_sells.iterrows():
			rows.append({
				'stocknum': buy_row['stocknum'],
				'buyday': buy_row['date'],
				'buyprice': buy_row['price'],
				'sellday': sell_row['date'],
				'sellprice': sell_row['price'],
				'heldtime': sell_row['held_time'],
				'yield': sell_row['yield']
			})
			has_sells = True
			if sell_row['remaining_shares'] == 0:
				break

		# 如果没有卖出记录，添加一条只有买入信息的记录
		if not has_sells:
			rows.append({
				'stocknum': buy_row['stocknum'],
				'buyday': buy_row['date'],
				'buyprice': buy_row['price'],
				'sellday': None,
				'sellprice': None,
				'heldtime': None,
				'yield': None
			})
	data = pd.DataFrame(rows).sort_values("buyday", ascending=False)
	return data
# endregion

# region回测调度
def backTest(begin_date,end_date, strategyname):

	joblog = '回测启动: ' + strategyname + '\n'

	joblog = joblog + f"Parent PID: {os.getpid()} \n"

	print('Parent PID: ', os.getpid())
	jobs = []
	pool = multiprocessing.Pool(config['使用进程数'])

	print('Appending Pool Started.')
	for nthstock in range(0, stockcodedf.shape[0]):#stockcodedf.shape[0]
		stocknum = stockcodedf.at[nthstock,'StockCode']
		p = pool.apply_async(calcStock, args=(stocknum,begin_date,end_date, strategyname)) 
		jobs.append(p)
	
	print('All Jobs Queued.')

	pool.close()
	pool.join()
	print('All subprocess done.')

	allresult = {}
	calclog = {}
	for job in jobs: 
		res = job.get()
		num = res[0]
		#print(res)
		allresult[num] = res[1]
		calclog[num] = res[2]

	allresult = dict(sorted(allresult.items()))
	calclog = dict(sorted(calclog.items()))
	#print(calclog)
	for each in calclog:
		joblog = joblog + f"{calclog[each]}\n"

	try: 
		#db.close_conn() # 回测结束后统一关闭数据库
		pass
	except Exception as e:
		print(f"database failed in closing : {e}")

	# 汇总所有交易记录到一个列表
	aggregated_transactions = []
	for stocknum, transactions in allresult.items():
		for transaction in transactions:
			aggregated_transactions.append(transaction)
	if(aggregated_transactions == []):
		print('Warning: no purchase! No csv generated.')
		joblog = joblog + "Warning: no purchase!"
		exit(0)

	# 创建DataFrame汇总
	transactions_df = pd.DataFrame(aggregated_transactions)
	transactions_df = sortSheets(transactions_df)
	
	transactions_df = transactions_df.to_csv(None, 
						   index=True, 
						   encoding='utf-8' ,
						   #mode='w'  # mode='w'代表覆盖之前的文件，mode='a'，代表接在原来数据的末尾
						   ) 

	#print('全部程序用时: ', time.time() - programStartTime)
	joblog = joblog + '策略 ' + strategyname + ' 用时: ' + f"{time.time() - programStartTime}"

	return transactions_df, joblog

# endregion

def testAll(begindate=None, enddate=None):
	begin_date = config['回测开始时间']
	if begindate != None:
		begin_date = begindate
	end_date = config['回测结束时间']
	if enddate != None:
		end_date = enddate

	# 从策略池中导入想要测试的策略
	txall = {}
	joblogs = {}
	for each in config['策略']:
		print('Running strategy:', each)
		transactions, joblog = backTest(begin_date,end_date, each)
		txall[each] = transactions
		joblogs[each] = joblog
	
	return txall, joblogs


if  __name__ == "__main__":
	
	txall, joblogs = testAll()
	print('Done. Start typing to use REPL.')
	# If running standalone, use "python3 -i" to get into interactive. 