import pandas as pd
import akshare as ak
import time
import datetime as dt
from datetime import datetime, timedelta
import random
import logging
import requests
import os
from column_mapping import column_mapping

from DBAccess import dbconn

# 设置环境变量，忽略系统代理，设置后即使使用了VPN也可以正常更新，但是会莫名蜜汁卡顿，使用时还是不建议设置任何系统代理
os.environ['NO_PROXY'] = '*'
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
# 设置 Pandas 显示的最大列数（None 表示不限制）
pd.set_option('display.max_columns', None)

# 设置 Pandas 显示的最大宽度（可以设置具体数值，None 表示不限制）
pd.set_option('display.width', None)

#更新每日股票数据
def updateHisData(conn,stocknum,today):
        #不包含前缀的stocknum
        Nstocknum = stocknum[2:]

        cursor = conn.cursor()
        query = """
            SELECT MAX(日期) as 最新日期
            FROM stock_data
            WHERE StockCode = ?;
            """
        cursor.execute(query, (Nstocknum,))
        result = cursor.fetchone()
        if result and result[0]:
            unformat_last_date = dt.datetime.strptime(result[0],'%Y-%m-%d')
            unformat_next_day = unformat_last_date + dt.timedelta(days=1)
            last_date  = unformat_last_date.strftime('%Y%m%d')
            next_day = unformat_next_day.strftime('%Y%m%d')

            print(f"存储最新日期: {last_date}")
            if last_date == today:
                return None
            else:
                print(f"需要更新的起始日期: {next_day}")
        else:
            #如果库内没有任何这只票的数据，那么直接从13年开始获取，实际上是在补充数据
            next_day = "20180101"

        readdf = ak.stock_zh_a_daily(symbol=stocknum,start_date= next_day, end_date=today, adjust="hfq")
        readdf.dropna(inplace=True)
        #表列名转换
        readdf['日期']=readdf['date']
        readdf['开盘']=readdf['open']
        readdf['收盘']=readdf['close']
        readdf['最高']=readdf['high']
        readdf['最低']=readdf['low']
        readdf['成交量']=readdf['volume']
        readdf['成交额']=readdf['amount']
        readdf['流动股本']=readdf['outstanding_share']
        readdf['换手率']=readdf['turnover']
        readdf['StockCode']=Nstocknum
        readdf.drop(columns=['date','open','close','high','low','volume','amount','outstanding_share','turnover'], axis=1, inplace=True)
        readdf = readdf.rename(columns= {"股票代码" :"StockCode"})
        return readdf

def updateHeatData(conn,stocknum,stock_prefix):

    cursor = conn.cursor()
    # # 查询数据库中的最新日期
    query = """
            SELECT MAX(时间) 
            FROM heat_data 
            WHERE StockCode = ?;
                   """
    cursor.execute(query, (stocknum,))
    result = cursor.fetchone()
    latest_date = result[0] if result[0] is not None else None
    today = str(dt.datetime.today().date())
    redudf = ak.stock_hot_rank_detail_em(symbol=(stock_prefix + stocknum))
    if latest_date is not None:
        if latest_date == today:
            return None
        new_data = redudf[redudf['时间'] > latest_date].copy()
    else:
        new_data = redudf
    new_data.loc[:, 'StockCode']= stocknum
    return new_data

def updateHeatData_WenCai(conn,today):
    cursor = conn.cursor()
    # # 查询数据库中的最新日期
    query = """
            SELECT MAX(RankDate) 
            FROM heatdata_wencai;
                """
    cursor.execute(query)
    result = cursor.fetchone()
    latest_date = result[0] if result[0] is not None else None
    if latest_date is None:
        latest_date = "20201101"
    start_date = datetime.strptime(latest_date, "%Y%m%d")
    end_date = datetime.strptime(today, "%Y%m%d")

    if start_date > end_date:
        print("improper start date")
        logging.error(f"improper start date input in  {today}")
    elif start_date == end_date:
        return
    
    # 使用列表推导式生成所有日期字符串
    date_list = [(start_date + timedelta(days=i)).strftime("%Y%m%d") 
                for i in range(1, (end_date - start_date).days + 1)]
    # print(date_list)
    for day in date_list:
        max_retries = 1  # 设置最大重试次数
        retries = 0
        while retries < max_retries:
            try:
                data_ = ak.stock_hot_rank_wc(date=day)
                data_.rename(columns=column_mapping, inplace=True)
                data_.drop(['序号'], axis=1, inplace=True)
                data_.to_sql('heatdata_wencai', conn, if_exists='append', index=False)
                print(f"heat_data of {day} has been updated")
                break  
            except Exception as e:
                logging.error(f"Error with stock {day} on attempt {retries + 1}: {e}")
                print(f"Error with stock {day} on attempt {retries + 1}: {e}")
                retries += 1
                time.sleep(random.randint(3, 8))  # 在重试之间暂停
        if retries >= max_retries:
            logging.error(f"Max retries reached for data at {day}")
    print("-----all needed heat data has been updated----")
    return 

def insertHottopicData(conn, data,date):
    try:
        if isinstance(date, dt.datetime):
            date = date.strftime('%Y-%m-%d')
        # 检查 data 是否包含 'items' 键，并且 'items' 可迭代
        if 'items' in data and isinstance(data['items'], list):
            for stock in data['items']:
                code = stock[0]
                name = stock[1]

                #循环加入所有标签
                labelist = [label['name'] for label in stock[8]]
                str_labelist = ', '.join(labelist)
                conn.execute("INSERT INTO hottopic_data VALUES (?,?,?,?)",(date,code,name,str_labelist))
                print(date,code, name,str_labelist)
            conn.commit()
            print(date)
    except Exception as e:
        print(e)

def updateHottopicData(conn):
    #遍历日期，从2019.01.01开始至今
    # 调用选股宝接口获取股票信息
    #获取数据库最新日期

    cursor = conn.cursor()
    cursor.execute('SELECT 日期 FROM hottopic_data ORDER BY 日期 DESC LIMIT 1')
    last_date = cursor.fetchone()
    if last_date and last_date[0]:
        last_date=last_date[0]
    else:
        last_date = "2019-01-01"
    #获取交易日历
    tradedatedf = ak.tool_trade_date_hist_sina()
    tradedatedf['trade_date'] = pd.to_datetime(tradedatedf['trade_date'])
    last_date = pd.Timestamp(last_date)
    date_to_update = tradedatedf[tradedatedf['trade_date']>last_date]
    if not date_to_update.empty:
        session = requests.Session()

        for index, row in date_to_update.iterrows():
                date_ = row['trade_date'].strftime('%Y%m%d')
                url = f"https://flash-api.xuangubao.cn/api/surge_stock/stocks?date={date_}&normal=true&uplimit=true"
                response = session.get(url, proxies={"http": None, "https": None})
                if response.status_code == 200:   
                    if response:
                        data = response.json().get('data', {})        
                        insertHottopicData(conn, data,row['trade_date'])
                    else:
                        logging.error(f"No data available for {date_}")
                else:
                    print(f"Failed to fetch data from {url}")
                    logging.error(f"Failed to fetch data from {url}")
        session.close()
    else:
        print("已经是最新数据了，无需更新~")


def dataUpdater():
    # 设置日志记录
    logging.basicConfig(filename='dataUpdater.log', level=logging.ERROR)
    conn = dbconn()

    stockcodedf = query_stock_basicinfo(conn,True)

    unformat_today= dt.datetime.today().date()
    today = unformat_today.strftime('%Y%m%d')
    print(f"The current time is {today}")

    #-----首先更新宏观市场层面的风口数据------#
    updateHottopicData(conn)
    print("宏观风口数据更新完毕！")
    
    #-----随后更新每只票的时序数据------#

    #更新每只票的热度数据——问财数据源
    updateHeatData_WenCai(conn,today)

    #用于调整symbol前缀
    board_to_prefix = {
    '上证主板': 'SH',
    '上证创业板': 'SH',
    '深圳主板': 'SZ',
    '深圳创业板': 'SZ'
    }
    for o in range(0, stockcodedf.shape[0]):
        #按照给定列表更新，范围包括上证和深证的主板+创业板
        stocknum=stockcodedf.at[o,'StockCode']
        stock_prefix = board_to_prefix.get(stockcodedf.at[o,'Sector'])
        lower_stock_prefix=stock_prefix.lower()
        #stocknummax为带前缀的股票代码
        stocknummax=lower_stock_prefix+stocknum
        max_retries = 1  # 设置最大重试次数
        retries = 0

        while retries < max_retries:
            try:
                latest_data = updateHisData(conn,stocknummax,today)
                # 将 新增的行情数据 存储到名为 'stock_data' 的表中
                if latest_data is not None:
                    latest_data.to_sql('stock_data', conn, if_exists='append', index=False)
                    print(f"{o+1}/{stockcodedf.shape[0]} : HisData of {stocknum} has been updated")
                #-----行情数据更新后，停顿随后更新热度数据------#
                    time.sleep(0.1)
                
                # redudf = updateHeatData(conn,stocknum,stock_prefix)
                # if redudf is not None:
                #     redudf.to_sql('heat_data', conn, if_exists='append', index=False)
                #     print(f"{o+1}/{stockcodedf.shape[0]} : heat_data of {stocknum} has been updated")
                #-----热度数据更新后，停顿随后更新下一只票------#
                # time.sleep(random.randint(1, 5))
                break  
            except Exception as e:
                logging.error(f"Error with stock {stocknum} on attempt {retries + 1}: {e}")
                print(f"Error with stock {stocknum} on attempt {retries + 1}: {e}")
                retries += 1
                time.sleep(random.randint(3, 8))  # 在重试之间暂停
        if retries >= max_retries:
            logging.error(f"Max retries reached for stock {stocknum}")
                #如果错误日志里面有这样的字样，说明多次尝试后爬取依然失败，如果少量出现手动补下即可，大量出现说明可能被ban了或者网络有问题
    print("-----All data has been updated----")

#已经运行过一次，不用高频率运行了，可以后期再弄个更新维护程序，2-3个月运行一次
#这段代码机制较为完善，以后web爬虫相关代码都可以考虑复用它
def fetchBasicStockInfo():
    session = requests.Session()  # 使用 session 对象提高性能
    conn = dbconn()
    df = pd.read_csv('沪深列表.csv', dtype=str)
    board_to_suffix = {
    '上证主板': '.SS',
    '上证创业板': '.SS',
    '深圳主板': '.SZ',
    '深圳创业板': '.SZ'
    }
    df.loc[df['ST'] == 'ST','ST'] = 1
    df['StockCode'] = df['证券代码']
    df.drop(['证券代码','序号','上市日期','Unnamed: 6'], axis=1, inplace=True)
    df['suffix_stocknum'] = df['StockCode'].map(lambda x: x + board_to_suffix.get(df.loc[df['StockCode'] == x, '板块'].values[0], ''))
    df['plate_names'] = ''
    count = 1
    for index, row in df.iterrows():
        max_retries = 5  # 设置最大重试次数
        retries = 0
        symbol = row['suffix_stocknum']
        url = f"https://flash-api.xuangubao.cn/api/stage2/plates_by_any_stock?symbol={symbol}&fields=core_avg_pcp,plate_name"
        while retries < max_retries:
            try:
                response = session.get(url, proxies={"http": None, "https": None})
                if response.status_code == 200:
                    data = response.json().get('data', {})
                    plate_names = [info['plate_name'] for info in data.values() if 'plate_name' in info]
                    df.at[index, 'plate_names'] = ', '.join(plate_names)
                else:
                    print(f"Failed to fetch data from {url}")
                    logging.warning(f"Failed to fetch data from {url}, status code {response.status_code}")

            except Exception as e:
                print(f"Error with stock {row['StockCode']} on attempt {retries + 1}: {e}")
                retries += 1
                time.sleep(random.randint(10, 15))
        if retries >= max_retries:
            logging.error(f"Max retries reached for stock {row['StockCode']}")
        print(f"{count}/{df.shape[0]} : Succeed in fetching data ")
        count = count+1
    df.to_sql('basic_stock_info', conn, if_exists='append', index=False)
    session.close()
    logging.info(f"_____Completed fetching all the labelinfo from certion site, Congratulation!_____")


def fetchFinanceIndicator():
    conn = dbconn()
    stockcodedf = query_stock_basicinfo(conn,True)

    for o in range(0, stockcodedf.shape[0]):

        stocknum=stockcodedf.at[o,'StockCode']
        max_retries = 5  # 设置最大重试次数
        retries = 0

        while retries < max_retries:
            try:
                indicator_data = ak.stock_financial_abstract_ths(symbol=stocknum, indicator="按报告期")
                indicator_data.loc[:, 'StockCode']= stocknum
                indicator_data = indicator_data.rename(columns=column_mapping)

                indicator_data.to_sql('common_indicators', conn, if_exists='append', index=False)
                print(f"{o+1}/{stockcodedf.shape[0]} : CommonIndicators of {stocknum} has been updated")

                break  
            except Exception as e:
                logging.error(f"Error with stock {stocknum} on attempt {retries + 1}: {e}")
                print(f"Error with stock {stocknum} on attempt {retries + 1}: {e}")
                retries += 1
                time.sleep(random.randint(8, 15))  # 在重试之间暂停
        if retries >= max_retries:
            logging.error(f"Max retries reached for stock {stocknum}")
                #如果错误日志里面有这样的字样，说明多次尝试后爬取依然失败，如果少量出现手动补下即可，大量出现说明可能被ban了或者网络有问题
    print("-----The CommonIndicators data has been updated----")

def update_basicinfo():
    conn = dbconn()
    cursor = conn.cursor()

    stockcodedf = query_stock_basicinfo(conn,True)
    query = '''
    UPDATE basic_stock_info
    SET 
        ListingDate = ?,
        TotalMarketValue = ?,
        CirculatingMarketValue = ?,
        TotalShares = ?,
        CirculatingShares = ?
    WHERE StockCode = ?
    '''
    batch_size = 100  # 每次批量提交的大小
    updated_count = 0
    for o in range(0, stockcodedf.shape[0]):
        stocknum=stockcodedf.at[o,'StockCode']
        max_retries = 5  # 设置最大重试次数
        retries = 0
        while retries < max_retries:
            try:
                df  = ak.stock_individual_info_em(symbol=stocknum)
                if not df.empty:
                    ListingDate  = df[df['item'] == '上市时间']['value'].values[0]
                    TotalMarketValue  = df[df['item'] == '总市值']['value'].values[0]
                    CirculatingMarketValue  = df[df['item'] == '流通市值']['value'].values[0]
                    TotalShares  = df[df['item'] == '总股本']['value'].values[0]
                    CirculatingShares  = df[df['item'] == '流通股']['value'].values[0]

                    cursor.execute(query,(ListingDate,TotalMarketValue,CirculatingMarketValue,TotalShares,CirculatingShares,stocknum))
                    updated_count += 1

                    # 每 batch_size 次更新后提交一次
                    if updated_count % batch_size == 0:
                        conn.commit()
                        print(f"{updated_count}/{stockcodedf.shape[0]}")
                break  
            except Exception as e:
                logging.error(f"Error with stock {stocknum} on attempt {retries + 1}: {e}")
                print(f"Error with stock {stocknum} on attempt {retries + 1}: {e}")
                retries += 1
                time.sleep(random.randint(8, 15))  # 在重试之间暂停
        if retries >= max_retries:
            logging.error(f"Max retries reached for stock {stocknum}")
        # 提交剩余的更新（不足 batch_size 的部分）
    if updated_count % batch_size != 0:
        try:
            conn.commit()

        except Exception as e:
            logging.error("Error committing the final batch: commit rollback", e)
            conn.rollback()  # 在提交失败时回滚防止数据损坏
    print("-----The Basic data has been updated----")

# 查询特定股票代码的数据
def query_stock_data(code_to_query,start_date,end_date):
    conn = dbconn()
    try:
        query = f"SELECT * FROM stock_data WHERE StockCode = ? AND 日期 >= ? AND 日期 <= ?"

        df_query = pd.read_sql_query(query, conn, params=(code_to_query, start_date, end_date))
        return df_query
    except Exception as e:
        print(f"During the query an error occurred: {e}")
        return None

#查询特定股票热度数据
#注意热度数据只有自23年12以来的，如果结束时间早于2023-12-19，查询会返回空empty
def query_stock_heatdata(conn,code_to_query,start_date,end_date):
    try:
        query = f"SELECT * FROM heat_data WHERE StockCode = ? AND 时间 >= ? AND 时间 <= ?"

        df_query = pd.read_sql_query(query, conn, params=(code_to_query, start_date, end_date))
        return df_query
    except Exception as e:
        print(f"During the query an error occurred: {e}")
        return None 
    
#查询所有股票基础信息数据
#第二个参数为true则查询结果会包括ST票否则不包括
def query_stock_basicinfo(conn,st):
    try:
        if not st:
            query = f"SELECT * FROM basic_stock_info where ST != '1'"
        else:
            query = f"SELECT * FROM basic_stock_info"
        df_query = pd.read_sql_query(query, conn)
        return df_query

    except Exception as e:
        print(f"During the query an error occurred: {e}")
        return None 
    
#查询返回一个表格，dataframe格式，和其他数据一致，格式如下：
#             日期      Label  Hotpoint  Total Hotpoint
# 0   2019-01-02         5G        13              51
# 1   2019-01-02        ST股         9              51
# 2   2019-01-02         公告         3              51
# 18  2019-01-03         其他         5              56
# 19  2019-01-03         军工        12              56
# 20  2019-01-03       军工集团         1              56
#需要注意K线和这个数据只有交易日有，但是热度数据不限于交易日，使用时需要小心！

#查询所有股票五线谱数据信息
#第二个参数为true则查询结果会包括ST票否则不包括
def query_stock_fiveline(code_to_query,start_date,end_date):
    conn = dbconn()
    try:
        query = f"SELECT * FROM stock_data_fiveline WHERE StockCode = ?"
        df_query = pd.read_sql_query(query, conn, params=(code_to_query,))
        return df_query

    except Exception as e:
        print(f"During the query an error occurred: {e}")
        return None 


def query_market_hotpoint(conn, start_date, end_date):
    try:
        # 修改 SQL 查询以包括日期范围，并选择日期字段
        query = "SELECT 日期, Label FROM hottopic_data WHERE 日期 BETWEEN ? AND ?"
        df_query = pd.read_sql_query(query, conn, params=(start_date, end_date))
        # 拆分标签并展开为新的行，同时保留日期信息
        df_query['Label'] = df_query['Label'].str.split(',')
        df_exploded = df_query.explode('Label')
        df_exploded['Label'] = df_exploded['Label'].str.strip()
        
        # 计算每个日期每个标签的数量
        hotpoints = df_exploded.groupby(['日期', 'Label']).size().reset_index(name='Hotpoint')

        # 计算每个日期的总热度
        daily_totals = hotpoints.groupby('日期')['Hotpoint'].sum().reset_index(name='Total Hotpoint')

        # 合并日总热度到主数据框中
        hotpoints = hotpoints.merge(daily_totals, on='日期')

        return hotpoints
    except Exception as e:
        print("An error occurred:", e)
        return None
    
def calculate_daily_scores(group, plate_names):
    # 为给定日期计算总得分
    daily_score = group[group['Label'].isin(plate_names)]['Hotpoint'].sum()
    return daily_score

#            日期  Hotpoint  Total Hotpoint
# 0  2019-01-02     0              51      
# 1  2019-01-03     0              56
# 2  2019-01-04     1             150
# 3  2019-01-07     0             133
# 4  2019-01-08     0              74
# 5  2019-01-09     0              87
#查询结果会类似上述表格，得到一段时间内特点票的风口得分
# （标签在里面有就+对应的分数，累加，例如600000在2019-01-04，
# 当天券商有一个票上榜了，600000在数据库内有券商标签，所以当天得1分）
def query_single_stock_point(conn,code_to_query, start_date, end_date):
    try:

        hotpoints = query_market_hotpoint(conn, start_date, end_date)
        if hotpoints is None or hotpoints.empty:
            return None
        # 查询股票的板块信息
        query_stock_info = """
        SELECT PlateNames FROM basic_stock_info WHERE StockCode=?
        """
        stock_info = pd.read_sql_query(query_stock_info, conn, params=(code_to_query,))
        if stock_info.empty:
            return None
            # 获取股票的板块标签列表
        plate_names = stock_info.iloc[0]['PlateNames'].split(', ')
        
        scores = hotpoints.groupby('日期').apply(lambda x: calculate_daily_scores(x, plate_names)).reset_index(name='Hotpoint')
        scores = scores.merge(hotpoints[['日期', 'Total Hotpoint']].drop_duplicates(), on='日期')
        return scores
    except Exception as e:
        print(f"During the query an error occurred: {e}")
        return None 



#region 入口
if __name__ == "__main__":

    dataUpdater()

#endregion 入口