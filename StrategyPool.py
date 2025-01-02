import pandas as pd
from MyTT import *
from 公式库 import *
import numpy as np
import math
#region 右侧追击反转买卖
def OldtrendFollew(**kwargs):
	#策略指标 ------
	#计算月MACD指标，一般过程比较复杂的指标都推荐+入公式库获取，提高策略部分代码整洁度
	kline = kwargs.get('kline', None)
	readdf = kline
	readdf = dynamic_MACD(readdf)

	t1=30#低于x值涨跌买入
	t2=40#高于X值涨跌卖出
   # 定义函数ZJFZ，用于计算ZJFZ指标
	predf=ZJFZ(readdf)

	#去缺失值
	df=predf
	# df.reset_index(inplace=True)

	df['MA300']=MA(df['收盘'],300)
	df['VOLMA60']=MA(df['成交量'],60)
	#日MACD
	df['DIF'],df['DEA'],df['MACD']=MACD(df['收盘'])
	df['红绿']=df['收盘']-df['开盘']

	#填充macd
	df=df.fillna(value=0) 

 
	# 计算买入卖出点
	df['t1'] = t1
	df['t2'] = t2


	# ===找出买入信号
	df['pos']=0
	df['volpercent'] = 0
	# 1.主力建仓信号
	# 2. 合并小于t1
	# 3.收盘高于ma300
	# 4.三日反转
	# 当天的入场f小于等于t1
	condition1 = (df['合并'] <= df['t1'])
	# # 合并高于昨日
	# condition11 = (df['合并'].shift(1) < df['合并'])
	#主力建仓小于前一日信号
	condition2 = (df['主力建仓'].shift(1) > df['主力建仓'])
	condition22 = (df['主力建仓'].shift(1) > 1)
	condition23 = (df['主力建仓'].shift(1) > df['主力建仓'].shift(2) )
	# 三日反转 
	df['3DLLV']=LLV(df['收盘'],3)  #3日LLV
	condition3 = (df['收盘'] > df['3DLLV'])

	#收盘高于均线MA300以上
	conditionMA1=(df['收盘'] > df['MA300'])
	conditionMA2=(df['MA300'] > df['MA300'].shift(10)) 
	conditionMA3=(df['MA300'] > 0)

	# 月MACD为红色
	conditionMMACD= (df['月MACD']> 0)
	#本月macd高于上月
	conditionMMACD2= (df['MACD相差']> 0 )

	df.loc[condition1  & condition2 & condition22 
		   &condition23 &condition3 & conditionMA1
		   & conditionMA2& conditionMA3&conditionMMACD
		   &conditionMMACD2, ['pos', 'volpercent']] = 1,1


	# ===找出卖出信号
	# 涨跌高于t2并且相较于前一天下降
	conditionsell01 = (df['SMASELL']> df['t2'])
	conditionsell02 = (df['SMASELL'] < df['SMASELL'].shift(1))
	conditionsell06=(df['主力建仓']< 1)
	df.loc[conditionsell01 & 
		   conditionsell02 &
		   conditionsell06, ['pos', 'volpercent']] = 2,1
	# 大绿柱
	conditionsell03=(df['红绿']<0)
	conditionsell05=(df['成交量']/df['VOLMA60']>2)
	df.loc[conditionsell03 & 
		   conditionsell05 & 
		   conditionsell06, ['pos', 'volpercent']] = 2,1


	# #止损模块
	#跳空止损
	#计算跳空
	df['跳空']=df['最高']-df['最低'].shift(1)
	condition9 =(df['跳空'] < 0)
	df.loc[condition9 , ['pos', 'volpercent']] = 2,1
	

	# 将无关的变量删除
	df.drop(['t1', 't2'], axis=1, inplace=True)
	return df
#endregion

#region 热度策略
def hotStockFollow(**kwargs):
	#策略初始化
	kline = kwargs.get('kline', None)
	redu = kwargs.get('redu', None)
	hottopic = kwargs.get('hottopic', None)

	#获取

	redu['日期']= redu['时间']
	df=pd.merge(kline,redu,on='日期',how='left')
	df=pd.merge(df,hottopic,on='日期',how='left')
	df['pos']=0
	df['refbollup']=0
	df['volpercent'] = 0
	# df['bollhigh'],df['boll'],df['bolllow'] = BOLLM(df['收盘'],20)
	df['红绿']=df['收盘']-df['开盘']
	df['一字']=2
	conditionyizi = (df['开盘'] == df['收盘'])
	df.loc[conditionyizi,'一字']=1

	# 设置参数和指标
	t1=30#低于x值涨跌买入
	t2=40#高于X值涨跌卖出
	df = ZJFZ(df)
	df['t1'] = t1
	df['t2'] = t2
	df['Hotpointpercent']=df['Hotpoint']/df['Total Hotpoint']

#------buy conditions--------
	condition1 = (df['合并'] <= df['t1'])
	condition2 = (df['主力建仓'].shift(1) > df['主力建仓'])
	condition22 = (df['主力建仓'].shift(1) > 1)

	# 三日反转 
	df['3DLLV']=LLV(df['收盘'],3)  #3日LLV
	condition3 = (df['收盘'] > df['3DLLV'])

	#热度排名小于100
	conditionREDU1= (df['排名']<100)
	conditionREDU2= (df['排名']>0)
	#新进粉丝高于0.5
	conditionREDU3= (df['新晋粉丝']>0.5)
	#买入当天要有涨停
	conditionzhangting= (df['涨跌幅']>9.5)
	#有选股宝风口板块热度高于3
	conditionHotpoint= (df['Hotpoint']>3)
	#总热度高于50
	conditionTotalHotpoint= (df['Total Hotpoint']>50)
	#板块热度占比高于10%
	conditionHotpointpercent= (df['Hotpointpercent']>0.08)
	#第二天买入时不是一字板
	conditionNotyizi= (df['一字'].shift(-1)==2)

	df.loc[condition1&condition2&condition22&condition3&conditionREDU1&conditionREDU2
		   &conditionREDU3&conditionTotalHotpoint&conditionHotpoint&conditionHotpointpercent&conditionNotyizi, ['pos', 'volpercent']] = 1,1

	#确定有买入信号后接入选股宝数据
	#

#------sell conditions--------#
	# 涨跌高于t2并且相较于前一天下降
	conditionsell01 = (df['SMASELL']> df['t2'])
	conditionsell02 = (df['SMASELL'] < df['SMASELL'].shift(1))
	df.loc[conditionsell01 & conditionsell02,  ['pos', 'volpercent']] = 2,1
	# 大绿柱
	df['VOLMA60']=MA(df['成交量'],60)
	conditionsell03=(df['红绿']<0)
	conditionsell05=(df['成交量']/df['VOLMA60']>3)
	df.loc[conditionsell03 & conditionsell05, ['pos', 'volpercent']] = 2,1
	# #布林带卖出
	# conditionbollsell01=(df['收盘'] < df['boll'])
	# df.loc[conditionbollsell01, ['pos', 'volpercent']] = 2,1

#------stop conditions--------#

#------end strategy--------#
	# 将无关的变量删除
	df.drop(['t1', 't2','合并','主力建仓','红绿','SMASELL','新晋粉丝','排名','VOLMA60'], axis=1, inplace=True)
	return df
#endregion

#region 买就盈利策略
def Buy4win(**kwargs):
	#策略指标 ------
	readdf = kwargs.get('kline', None)

	t1=30#低于x值涨跌买入
	t2=40#高于X值涨跌卖出
   # 定义函数ZJFZ，用于计算ZJFZ指标
	df=MJYL(readdf)
	df=ZJFZ(df)
	df = dynamic_Week_trend(df)
	df=SUPERTREND(df)
	df=golden_bottom(df)

	df['红绿']=df['收盘']-df['开盘']
	df['VOLMA60']=MA(df['成交量'],60)
	df['RSI']=RSI(df['收盘'],6)

	#计算MACD
	df['DIF'],df['DEA'],df['MACD']=MACD(df['收盘'])
	#计算两次DIF
	conditionDIFTOP1=(df['DIF']<df['DIF'].shift(1))
	conditionDIFTOP2=(df['DIF'].shift(1)>df['DIF'].shift(2))
	df.loc[conditionDIFTOP1 & conditionDIFTOP2,  ['波峰']] = 1
	df['DIF数值']=VALUEWHEN(df['波峰']==1,df['DIF'])
	conditionDIFTOP3=(df['波峰']==1)
	conditionDIFTOP4=(df['DIF数值'].shift(1)>df['DIF数值'])
	df.loc[conditionDIFTOP3 & conditionDIFTOP4,  ['DIFTOP']] = 1

	#是否一字板
	df['一字']=0
	conditionyizi = (df['开盘'] == df['收盘'])
	df.loc[conditionyizi,'一字']=1
	#阴线是否连续
	df['阴线']=0
	conditionyinxian = (df['收盘'] < df['开盘'])
	df.loc[conditionyinxian,'阴线']=1
	#是否有跳空
	df['跳空']=0
	condition跳空 =(df['最高']-df['最低'].shift(1) < 0)
	df.loc[condition跳空,'跳空']=1
	#填充
	df=df.fillna(value=0) 

	# 计算买入卖出点
	df['t1'] = t1
	df['t2'] = t2

	# ===找出买入信号
	df['pos']=0
	
	
	#买就赢利
	condition01=(df['买就赢利'] == 1)
	condition近期买就赢利=(df['买就赢利'].rolling(5).sum() >0)
	#最近15日内出现3次以上买就赢利条件
	# condition01 = (df['买就赢利'].rolling(15).sum() > 2)
	#不连续5日阴线
	conditionyinxian = (df['阴线'].rolling(5).mean() !=1)
	#近n日没有跳空
	condition无跳空 = (df['跳空'].rolling(5).max() ==0)
	#成交量
	conditionvol= (df['成交量'] > df['VOLMA60'] )
	# 三日反转 
	df['3DLLV']=LLV(df['收盘'],3)  #3日LLV
	condition3DLLV = (df['收盘'] > df['3DLLV'])
	#黄金底
	condition黄金底 = (df['golden_bottom'] > 0)
	#一种更简洁的写法,当我们需要跟踪买入卖出的结果时
	signal_enter = condition近期买就赢利 &condition黄金底
	# signal_enter2 = conditionzhuli1 &conditionwuyizi &conditionyinxian &condition无跳空 
	#pandas中赋值为none会被自动转化为NaN,使用isna() 或 isnull() 方法作为判断条件
	df.loc[signal_enter
		, ['pos', 'volpercent']] = 1,1


	# ===找出卖出信号      
	#卖出
	#dif二次卖出
	df['买入间隔']=BARSLAST(df['pos']==1)
	conditionsell01 = (df['SMASELL']> df['t2'])
	conditionsell02 = (df['SMASELL'] < df['SMASELL'].shift(1))
	conditionsell03 = (df['DIFTOP']==1)
	condition买入间隔dif = (df['买入间隔'] < 90)
	df.loc[conditionsell01&conditionsell02&conditionsell03&condition买入间隔dif,  ['pos', 'volpercent']] = 2,1

	#过超级趋势卖出
	conditionsell超级趋势 = (df['收盘']<df['超级趋势'])
	condition买入间隔超级趋势= (df['买入间隔'] > 90)
	df.loc[conditionsell超级趋势&condition买入间隔超级趋势,  ['pos', 'volpercent']] = 2,1
		   
	# # 金蜘蛛见顶卖
	# conditionsell01 = (80 < df['RSI'])
	# conditionsell02 = (80 > df['RSI'].shift(1))
	# # conditionsell06 = (df['主力建仓'] < 1)
	# df.loc[conditionsell01 & 
	#        conditionsell02 , ['pos', 'volpercent']] = 2,1

	# #满仓卖出
	# conditionsell满仓卖出 = (df['满仓卖出'] ==1)
	# df.loc[conditionsell满仓卖出  , ['pos', 'volpercent']] = 2,1

	# #有一字卖出
	# conditionsell满仓卖出 = (df['一字'] ==1)
	# df.loc[conditionsell满仓卖出  , ['pos', 'volpercent']] = 2,1

	# 急涨大绿柱 
	df['五日前斜率']=(df['最高']/REF(df['最高'],5)-1)*100
	df['近5年斜率高']=HHV(df['五日前斜率'],1250)
	df['五年斜率比例']=(df['五日前斜率']/df['近5年斜率高'])*100
	conditionselllvzhu01=(df['红绿']<0)
	conditionselllvzhu02=(df['成交量']/df['VOLMA60']>2)
	conditionselllvzhu03 = (df['五年斜率比例'] > 80)
	df.loc[conditionselllvzhu01 & conditionselllvzhu02 & conditionselllvzhu03 ,  ['pos', 'volpercent']] = 2,1

	# # 合并高于t2并且相较于前一天下降
	# conditionsell01 = (df['合并']> df['t2'])
	# conditionsell02 = (df['合并'] < df['合并'].shift(1))
	# df.loc[conditionsell01 & conditionsell02,  ['pos', 'volpercent']] = 2,1

	#止损模块
	#一周后周macd下降止损
	condition止损1 = (df['MACDWEEK'] < df['MACDWEEK'].shift(5))
	df['买入间隔']=BARSLAST(df['pos']==1)
	condition止损2 = (df['买入间隔'] >= 5)
	df.loc[condition止损1&condition止损2 , ['pos', 'volpercent']] = 2,1

	#跳空止损
	#计算跳空（近n日？）
	# df['跳空']=df['最高']-df['最低'].shift(1)
	# condition9 =(df['跳空'] < 0)
	# df.loc[condition9 , ['pos', 'volpercent']] = 2,1
	#固定止损
	# stopcondition = df.loc[df['收盘'] <= ]



	# # SMASELL
	# conditionsell01 = (df['SMASELL']> df['t2'])
	# conditionsell02 = (df['SMASELL'] < df['SMASELL'].shift(1))
	# df.loc[conditionsell01 & conditionsell02,  ['pos', 'volpercent']] = 2,1

	# 将无关的变量删除
	df.drop(['t1', 't2'], axis=1, inplace=True)

	# #输出 
	# df.to_csv('600004.csv', index=False,
	#         mode='w',  # mode='w'代表覆盖之前的文件，mode='a'，代表接在原来数据的末尾
	#         float_format='%.15f',  # 控制输出浮点数的精度
	#         # header=None,  # 不输出表头
	#         encoding='gbk'
	# )
	# print('完成')
	# input()

	return df
#endregion
	
#region 历史新高趋势追随策略
def trendFollew(**kwargs):
	#策略指标 ------
	kline = kwargs.get('kline', None)
	readdf = kline
	t1=30#低于x值涨跌买入
	t2=80#高于X值涨跌卖出

   # 定义函数ZJFZ，用于计算ZJFZ指标
	df=ZJFZ(readdf)
	# df=zhushenglang(df)
	# df = dynamic_Week_trend(df)

	df['红绿']=df['收盘']-df['开盘']
	df['MA300']=MA(df['收盘'],300)
	df['MA120']=MA(df['收盘'],120)
	df['MA60']=MA(df['收盘'],60)
	df['VOLMA60']=MA(df['成交量'],60)

	#计算MACD
	df['DIF'],df['DEA'],df['MACD']=MACD(df['收盘'])
	#计算两次DIF
	conditionDIFTOP1=(df['DIF']<df['DIF'].shift(1))
	conditionDIFTOP2=(df['DIF'].shift(1)>df['DIF'].shift(2))
	df.loc[conditionDIFTOP1 & conditionDIFTOP2,  ['波峰']] = 1
	df['DIF数值']=VALUEWHEN(df['波峰']==1,df['DIF'])
	conditionDIFTOP3=(df['波峰']==1)
	conditionDIFTOP4=(df['DIF数值'].shift(1)>df['DIF数值'])
	df.loc[conditionDIFTOP3 & conditionDIFTOP4,  ['DIFTOP']] = 1

	# #是否有跳空
	# df['跳空']=0
	# condition跳空 =(df['最高']-df['最低'].shift(1) < 0)
	# df.loc[condition跳空,'跳空']=1

	# #判断涨停
	# condition涨停 =df['涨跌幅']>9
	# #或者用计算方式涨停也可以
	# df['涨停计算']=(df['收盘']-df['收盘'].shift(1))/df['收盘'].shift(1)
	# condition涨停计算 =(df['涨停计算'])>0.09
	# df.loc[condition涨停|condition涨停计算,'涨停']=1

	#填充
	df=df.fillna(value=0) 
	df['t1'] = t1
	df['t2'] = t2

	# ===找出买入信号
	df['pos']=0 

	# #红柱WEEK
	# condition红柱WEEK = (df['红柱WEEK']==1)
	# #SAR红WEEK
	# conditionSAR红WEEK = (df['SAR红']==1)
	# #BOLL红WEEK
	# conditionBOLL红WEEK = (df['BOLL红']==1)
	# #主力建仓下降
	# conditionzhuli1 = (df['主力建仓'].shift(1) > df['主力建仓'])
	# conditionzhuli2 = (df['主力建仓']> 1)
	# conditionzhuli3 = (df['主力建仓'].shift(1) > df['主力建仓'].shift(2))
	# df.loc[conditionzhuli1 & conditionzhuli2&conditionzhuli3,'主力信号']=1
	# #主力建仓信号出现到现在的日期
	# df['主力信号时间']=BARSLAST(df['主力信号']==1)
	# #主力建仓信号小于7天
	# conditionzhuli4 = (df['主力信号时间']<7)
	# #非涨停买入
	# condition非涨停 = (df['涨跌幅'] < 9.5)
	# #买入接近boll中线
	# conditionbuyboll1 = (df['BOLL']-df['BOLL区间']*0.1<df['最高'])
	# #买入时最近没有DIF二次下降
	# conditionbuyDIF = (df['DIFTOP'].rolling(5).max()==0)
	# #记录金柱时成本价
	# df['金柱成本'] = VALUEWHEN(df['金柱WEEK']==1,df['收盘'])
	# #买入时不低于金柱成本
	# condition金柱成本 = (df['收盘'] > df['金柱成本'])
	#记录近3年收盘最高价
	df['历史高点']=df['最高'].rolling(400).max()
	df['历史高点200']=df['最高'].rolling(200).max()
	df['高点信号']=0
	df['高点信号200']=0
	df.loc[df['历史高点']==df['最高'],'高点信号']=1
	df.loc[df['历史高点200']==df['最高'],'高点信号200']=1
	#历史高点的二次突破，间隔超过30天
	df['高点间隔']=0
	df['TOPRANGE']=TOPRANGE(df['最高'])
	df['高点间隔']=BARSLAST(df['高点信号']==1)
	df['高点间隔200']=BARSLAST(df['高点信号200']==1)
	condition高点信号1=(df['高点间隔200'].shift(1)>30)
	condition高点信号2=(df['高点间隔200'].shift(1)<90)
	condition高点信号3=(df['高点信号']==1)
	#信号之间是否突破ma

	#放量
	conditionVOL1= (df['成交量']>df['VOLMA60'])
	#红柱
	condition红绿= (df['红绿']>0)

	# signal_enter = condition红柱WEEK&conditionzhuli1&conditionzhuli2& conditionFZ1&condition行数&condition非涨停&conditionbuyDIF&condition金柱成本
	signal_enter = condition高点信号1&condition高点信号2&condition高点信号3&condition红绿&conditionVOL1

	# #MACD箱体买
	# conditionMACD买入1=df['MACD']<0
	# conditionMACD买入2=df['MACD'].shift(1)<df['MACD']
	# conditionMACD买入3=df['MACD'].shift(2)>df['MACD'].shift(1)
	# conditionMACD买入4=df['最高'].shift(1)<df['最高']
	# signal_MACD = conditionMACD买入1&conditionMACD买入2&conditionMACD买入3&conditionMACD买入4
	df.loc[signal_enter
		, ['pos', 'volpercent']] = 1,1,
	
	# #MACD箱体卖
	# conditionMACD卖出1=df['MACD']>0
	# conditionMACD卖出2=df['MACD'].shift(1)>df['MACD']
	# conditionMACD卖出3=df['MACD'].shift(2)<df['MACD'].shift(1)
	# conditionMACD卖出4=df['最低'].shift(1)>df['最低']
	# df.loc[conditionMACD卖出1&conditionMACD卖出2&conditionMACD卖出3&conditionMACD卖出4,  ['pos', 'volpercent']] = 2,1



	# #去除重复多余买入、红柱信号
	# # 计算每个pos的差值
	# df['diff'] = df['pos'].diff()
	# # 将差值等于0的行赋值为0
	# df.loc[df['diff'] == 0, 'pos'] = 0
	# # 删除列
	# df.drop(['t1', 't2', 'diff'], axis=1, inplace=True)

	# df.loc[signal_enter2
	#     , ['pos', 'volpercent']] = 1,1

	# ===找出卖出信号      
	# 合并高于t2并且相较于前一天下降
	# conditionsell01 = (df['涨跌'].shift(1)> df['t2'])
	# conditionsell02 = (df['涨跌'] < df['涨跌'].shift(1))
	# conditionsell01 = (df['绿柱']> 0)
	# conditionsell02 = (df['MA20'] > df['收盘'])
	# conditionsell非近期=(df['S2'].rolling(7).max() ==0)
	# df.loc[conditionsell01&conditionsell非近期,  ['pos', 'volpercent']] = 2,1


	# # 卖出2 dif两次波峰下降 
	conditionsell01 = (df['SMASELL']> df['t2'])
	conditionsell02 = (df['SMASELL'] < df['SMASELL'].shift(1))
	conditionsell03 = (df['DIFTOP']==1)
	df.loc[conditionsell01& conditionsell02&conditionsell03 ,  ['pos', 'volpercent']] = 2,1

	# #卖出3 绿柱WEEK
	# conditionsell01 = (df['绿柱']==1)
	# df.loc[conditionsell01,  ['pos', 'volpercent']] = -1,1

	# #卖出4 SAR绿
	# conditionsell04 = (df['SAR红']==0)
	# df.loc[conditionsell04,  ['pos', 'volpercent']] = -1,1

	# #卖出5 突破BOLL中线卖出
	# conditionsellboll1 = (df['BOLL'].shift(1)-df['BOLL区间'].shift(1)*0.1<df['收盘'].shift(1))
	# conditionsellboll2 = (df['BOLL']-df['BOLL区间']*0.1>df['收盘'])
	# df.loc[conditionsellboll1&conditionsellboll2,  ['pos', 'volpercent']] = -1,1

	
	#止损模块
	# 急涨大绿柱 
	df['五日前斜率']=(df['最高']/REF(df['最高'],5)-1)*100
	df['近5年斜率高']=HHV(df['五日前斜率'],1250)
	df['五年斜率比例']=(df['五日前斜率']/df['近5年斜率高'])*100
	conditionselllvzhu01=(df['红绿']<0)
	conditionselllvzhu02=(df['成交量']/df['VOLMA60']>2)
	conditionselllvzhu03 = (df['五年斜率比例'] > 80)
	df.loc[conditionselllvzhu01 & conditionselllvzhu02 & conditionselllvzhu03 ,  ['pos', 'volpercent']] = 2,1

	# #避免最近15日信号干扰
	# condition买入成本1=(df['持仓状态']==1)
	# condition买入成本2=(df['持仓状态'].shift(1)==0)
	# df.loc[condition买入成本1&condition买入成本2,  ['买入成本']] =df['开盘']
	# condition买入成本3=(df['持仓状态']==0)
	# df.loc[condition买入成本3,  ['买入成本']] =0
	# #填充
	# df['买入成本'].fillna(method='ffill', inplace=True)
	# # df['买入成本'] = VALUEWHEN(df['持仓状态']==1,df['开盘'])

	# condition止损1=(df['收盘']<df['买入成本'])
	# condition止损2=(df['买入间隔']<15)
	# df.loc[condition止损1&condition止损2,  ['pos', 'volpercent']] = 2,1
	
	# 将无关的变量删除
	# df.drop(['t1', 't2'], axis=1, inplace=True)

	# #输出 
	# df.to_csv('600690.csv', index=False,
	#         mode='w',  # mode='w'代表覆盖之前的文件，mode='a'，代表接在原来数据的末尾
	#         float_format='%.15f',  # 控制输出浮点数的精度
	#         # header=None,  # 不输出表头
	#         encoding='GBK'
	# )
	# print('完成')
	# input()

	return df
#endregion

#region 五线谱策略
def Fiveline(**kwargs):
	#策略初始化
	df = kwargs.get('kline', None)
	Fivelinedf = kwargs.get('fiveline', None)
	Fivelinedf=Fivelinedf[['日期','priceTL','TL-2SD','TL-SD','TL+SD','TL+2SD']]
	df['pos']=0
	df['volpercent'] = 0
	#数据合并
	df = pd.merge(df, Fivelinedf, on='日期', how='left')
	df=SUPERTREND(df)
	df=ZJFZ(df)
	df=jgss_buy(df)
	# df=MJYL(df)
	# df=zhushenglang(df)
	# df = dynamic_Week_trend(df)
	#设置参数和指标
	df['红绿']=df['收盘']-df['开盘']
	df['MA60']=MA(df['收盘'],60)
	df['VOLMA60']=MA(df['成交量'],60)
	#计算MACD
	df['DIF'],df['DEA'],df['MACD']=MACD(df['收盘'])
	#计算两次DIF
	df['DIFTOP']=0
	conditionDIFTOP1=(df['DIF']<df['DIF'].shift(1))
	conditionDIFTOP2=(df['DIF'].shift(1)>df['DIF'].shift(2))
	df.loc[conditionDIFTOP1 & conditionDIFTOP2,  ['波峰']] = 1
	df['DIF数值']=VALUEWHEN(df['波峰']==1,df['DIF'])
	conditionDIFTOP3=(df['波峰']==1)
	conditionDIFTOP4=(df['DIF数值'].shift(1)>df['DIF数值'])
	df.loc[conditionDIFTOP3 & conditionDIFTOP4,  ['DIFTOP']] = 1

	# #计算阳包阴指标
	# df['阳包阴']=0
	# df['全']=df['最高']-df['最低']
	# df['涨下影线']=df['开盘']-df['最低']
	# df['跌下影线']=df['收盘']-df['最低']
	# df['下影线全比值']=IF(df['收盘']>df['开盘'],df['涨下影线']/df['全'],df['跌下影线']/df['全'])
	# condition阳包阴01=df['收盘']>REF(df['最高'],1)
	# condition阳包阴02=df['收盘']>REF(df['收盘'],2)
	# condition阳包阴03=REF(df['最低'],1)<REF(df['最低'],2)
	# condition阳包阴04=REF(df['开盘'],2)>REF(df['收盘'],2)
	# condition阳包阴05=REF(df['下影线全比值'],1)>0.5
	# condition阳包阴06=REF(df['收盘'],2)>REF(df['收盘'],1)*0.95
	# df.loc[condition阳包阴01 & condition阳包阴02 & condition阳包阴03 & condition阳包阴04 & condition阳包阴05 & condition阳包阴06,  ['阳包阴']] = 1

	# #历史新高法
	# #记录近3年收盘最高价
	# # df['历史高点50']=df['最高'].rolling(50).max()
	# df['历史高点200']=df['最高'].rolling(200).max()
	# # df['高点信号50']=0
	# df['高点信号200']=0
	# # df.loc[df['历史高点50']==df['最高'],'高点信号50']=1
	# df.loc[df['历史高点200']==df['最高'],'高点信号200']=1
	# #历史高点的二次突破，间隔超过30天
	# # df['高点间隔50']=0
	# df['高点间隔200']=0
	# # df['高点间隔50']=BARSLAST(df['高点信号50']==1)
	# df['高点间隔200']=BARSLAST(df['高点信号200']==1)
	# condition高点信号1=(df['高点间隔200'].shift(1)>0)
	# condition高点信号2=(df['高点间隔200'].shift(1)<10)
	# condition高点信号3=(df['高点信号200']==1)

	# df['RSI']=RSI(df['收盘'],6)
	# conditionRSI01=(df['RSI']<80)
	# conditionRSI02=(df['RSI'].shift(1)>80)
	# df['RSI信号']=0
	# df.loc[conditionRSI01 & conditionRSI02,  ['RSI信号']] = 1

	t1=30
	t2=40
	df['t1'] = t1
	df['t2'] = t2

	# #绿柱卖出
	# condition绿柱= (df['绿柱']== 1)
	# df.loc[condition绿柱,  ['pos', 'volpercent']] = 2,1

#------buy conditions-------
	#1超跌恢复，2.可以有主力建仓下降或没有。3.买入主升浪阳线 4.买入可以适当放宽到2天满足条件
	#买入1 收盘处于超跌
	conditionbuy01 = (df['收盘']<df['TL-2SD'])
	conditionbuy02 = (df['TL-2SD']>df['TL-2SD'].shift(3))
	# conditionbuy03 = (df['收盘'].shift(3)<df['TL+2SD'].shift(3))
	conditionbuy04=(df['收盘'].shift(1)<df['TL-2SD'].shift(1))
	df['TDmark']=0
	df.loc[conditionbuy01&conditionbuy02,  ['TDmark']] = 1
	# conditionbuy03 = (df['收盘'].shift(2)<df['TL-2SD'].shift(2))
	conditionTDMARK=(df['TDmark']==1)
	#买入2主力建仓下降
	df['主力mark']=0
	conditionb主力01 = df['主力建仓']>1
	conditionb主力02 = (df['主力建仓']<df['主力建仓'].shift(1))
	conditionb主力03 = (df['主力建仓'].shift(2)<df['主力建仓'].shift(1))
	df.loc[conditionb主力01&conditionb主力02&conditionb主力03,  ['主力mark']] = 1
	#超级趋势
	df['超级趋势mark']=0
	condition超级趋势01=(df['收盘']>df['超级趋势'])
	condition超级趋势02=(df['收盘'].shift(1)<df['超级趋势'].shift(1))
	df.loc[condition超级趋势01&condition超级趋势02,  ['超级趋势mark']] = 1
	condition超级趋势buy=df['超级趋势mark'].rolling(7).max()==1

	# 1型买入
	conditionbuy1型买入 = (df['1型买入']==1)
	conditionbuy4型买入= (df['4型买入']==1)
	# #主升浪
	# condition主升浪01 = (df['红柱']==1)
	# #买入近期有金柱
	# conditionbuy金柱 = (df['金柱'].rolling(3).max()==1)
	# 最近没有卖出
	conditionDIFTOP=(df['DIFTOP'].rolling(20).max()<1)
	#近n日有主力建仓下降
	condition主力信号=(df['主力mark']==1)
	# #阳包阴
	# condition阳包阴 = (df['阳包阴']==1)

	signal_enter = conditionbuy01&condition主力信号&(conditionbuy1型买入|conditionbuy4型买入)
	df.loc[signal_enter, ['pos', 'volpercent']] = 1,1

#------sell conditions--------#
	#卖出
	#dif二次卖出
	conditionsell01 = (df['SMASELL']> df['t2'])
	conditionsell02 = (df['SMASELL'] < df['SMASELL'].shift(1))
	conditionsell03 = (df['DIFTOP']==1)
	df['买入间隔']=BARSLAST(df['pos']==1)
	condition买入间隔dif = (df['买入间隔'] < 90)
	df.loc[conditionsell01&conditionsell02&conditionsell03&condition买入间隔dif,  ['pos', 'volpercent']] = 2,1

	#过超级趋势卖出
	conditionsell超级趋势 = (df['收盘']<df['超级趋势'])
	condition买入间隔超级趋势= (df['买入间隔'] > 90)
	df.loc[conditionsell超级趋势&condition买入间隔超级趋势,  ['pos', 'volpercent']] = 2,1

#------stop conditions--------#
	#止损模块

	# 急涨大绿柱 
	df['五日前斜率']=(df['最高']/REF(df['最高'],5)-1)*100
	df['近一年斜率高']=HHV(df['五日前斜率'],250)
	df['一年斜率比例']=(df['五日前斜率']/df['近一年斜率高'])*100
	conditionselllvzhu01=(df['红绿']<0)
	conditionselllvzhu02=(df['成交量']/df['VOLMA60']>2)
	conditionselllvzhu03 = (df['一年斜率比例'] > 80)
	df.loc[conditionselllvzhu01 & conditionselllvzhu02 & conditionselllvzhu03 ,  ['pos', 'volpercent']] = 2,1

	# #超级趋势结束卖出
	# condition超级趋势结束=(df['收盘']<df['超级趋势'])
	# df.loc[condition超级趋势结束,  ['pos', 'volpercent']] = 2,1

	# #一周后周macd下降止损
	# condition止损1 = (df['MACDWEEK'] < df['MACDWEEK'].shift(5))
	# df['买入间隔']=BARSLAST(df['pos']==1)
	# condition止损2 = (df['买入间隔'] >= 5)
	# df.loc[condition止损1&condition止损2 , ['pos', 'volpercent']] = 2,1

	# #最低价突破MA60止损
	# condition止损3 = (df['最低'] < df['MA60'])
	# df.loc[condition止损3 , ['pos', 'volpercent']] = 2,1


#------end strategy--------#
	# 将无关的变量删除
	df.drop(['t1', 't2'], axis=1, inplace=True)

	# #输出df 
	# df.to_csv('600004.csv', index=False,
	#         mode='w',  # mode='w'代表覆盖之前的文件，mode='a'，代表接在原来数据的末尾
	#         float_format='%.15f',  # 控制输出浮点数的精度
	#         # header=None,  # 不输出表头
	#         encoding='GBK'
	# )
	# print('完成')
	# input()

	return df
#endregion

#region 策略空白模板
def templateStrategy(**kwargs):
	#策略初始化
	df = kwargs.get('kline', None)
	df['pos']=0
	df['volpercent'] = 0

	# 设置参数和指标

#------buy conditions-------

	df.loc[1, ['pos', 'volpercent']] = 1,1

#------sell conditions--------#

	df.loc[2, ['pos', 'volpercent']] = 2,1

#------stop conditions--------#

#------end strategy--------#
	# 将无关的变量删除
	# df.drop(['1'], axis=1, inplace=True)
	return df
#endregion