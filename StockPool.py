import pandas as pd
from MyTT import *
from 公式库 import *
from numpy import *
import re
#无视警告
import warnings
from DBAccess import dbconn
from column_mapping import column_mapping

warnings.filterwarnings("ignore")



class StockSet:
    # 内部的列名映射表，将用户友好的名称映射到数据库中的实际列名
    mapping = column_mapping

    def __init__(self):
        # 初始化数据库连接
        self.conn = dbconn()
        self.cursor = self.conn.cursor()

    def get_column(self, cn_name):
        """根据中文名称获取数据库中的实际列名"""
        return self.mapping.get(cn_name, cn_name)

    def replace_columns_in_condition(self, condition):
        """将条件中的用户友好列名替换为数据库中的实际列名"""
        def replace(match):
            column = match.group(0)
            return self.get_column(column)
        
        # 用正则表达式匹配列名并替换为数据库实际列名
        pattern = re.compile(r'\b\w+\b')
        return pattern.sub(replace, condition)

    def custom_query(self, condition, params=(),mapping = True):
        # basic_list = query_stock_basicinfo(self.conn,True)

        """执行自定义条件的查询，并自动替换列名"""
        # 替换条件中的列名
        if mapping:
            translated_condition = self.replace_columns_in_condition(condition)
        else:
            translated_condition = condition
        
        # 构建查询语句
        query = translated_condition
        try:
        # 执行查询
            self.cursor.execute(query, params)
            df_query = pd.read_sql_query(query, self.conn,params = params)
            # 去除 StockCode 列的重复行，保留首次出现的行
            df_query = df_query.drop_duplicates(subset=['StockCode'], keep='first')
            df_query.reset_index(inplace=True)
            # return self.cursor.fetchall()
            return df_query
        
        except Exception as e:
            print(f"During the query an error occurred: {e}")
            return None 
        finally:
            self.cursor.close()


if __name__ == "__main__":
    # 使用示例
    stockset = StockSet()
    print(stockset.get_column("基本每股收益"))
    # 获取总市值超过20亿并且上市时间在2020年后的行,非ST
    condition = """SELECT basic_stock_info.StockCode,common_indicators.ReportDate,common_indicators.ROE
    FROM basic_stock_info
    JOIN common_indicators  ON basic_stock_info.StockCode = common_indicators.StockCode
    WHERE common_indicators.ReportDate >= '2018-01-01' AND common_indicators.ROE > 0.15"""

    df = stockset.custom_query(condition)

    print(df)

    stockset.close()
