from MyTT import *
import datetime as dt
# import time
def zhulijiancangplus(zhulidf):
  #主力建仓
  zhulidf['XA_1']=REF(zhulidf['最低'],1)
  zhulidf['XA_2']=SMA(ABS(zhulidf['最低']-zhulidf['XA_1']),3,1)/SMA(MAX(zhulidf['最低']-zhulidf['XA_1'],0),3,1)*100
  zhulidf['XA_3']=EMA(IF(zhulidf['收盘']*1.2,zhulidf['XA_2']*10,zhulidf['XA_2']/10),3)
  zhulidf['XA_4']=LLV(zhulidf['最低'],50)
  zhulidf['XA_5']=HHV(zhulidf['XA_3'],50)
  zhulidf['XA_6']=IF(LLV(zhulidf['最低'],90),1,0)
  zhulidf['主力建仓']=EMA(IF(zhulidf['最低']<=zhulidf['XA_4'],(zhulidf['XA_3']+zhulidf['XA_5']*2)/2,0),3)/618*zhulidf['XA_6']
  zhulidf['XA_8']=(zhulidf['收盘']-LLV(zhulidf['最低'],21))/(HHV(zhulidf['最高'],21)-LLV(zhulidf['最低'],21))*100
  zhulidf['XA_9']=SMA(zhulidf['XA_8'],13,8)
  zhulidf['风险']=SMA(zhulidf['XA_9'],13,8)
  zhulidf['涨跌']=MA(3*SMA((zhulidf['收盘']-LLV(zhulidf['最低'],27))/(HHV(zhulidf['最高'],27)-LLV(zhulidf['最低'],27))*100,5,1)-2*SMA(SMA((zhulidf['收盘']-LLV(zhulidf['最低'],27))/(HHV(zhulidf['最高'],27)-LLV(zhulidf['最低'],27))*100,5,1),3,1),5) 
  zhulidf['主力建仓消化']=MA(EMA((zhulidf['XA_3']+zhulidf['XA_5']*2)/2,3)/618*zhulidf['XA_6'],30)
  zhulidf['主力消化均值']=MA(zhulidf['主力建仓消化'],5)
  
  zhulidf['MA5']=MA(zhulidf['收盘'],5)#xa8
  zhulidf['MA10']=MA(zhulidf['收盘'],10)#xa9
  zhulidf['MA30']=MA(zhulidf['收盘'],30)#xa10
  zhulidf['XA_16']=SUM(zhulidf['收盘']*zhulidf['成交量']*100,28)/SUM(zhulidf['成交量']*100,28)
  zhulidf['XA_117']=(zhulidf['XA_16']*100)
  zhulidf['XA_1117']=zhulidf['XA_117'].round(0)
  zhulidf['XA_17']=zhulidf['XA_1117']/100 #XA_17
  zhulidf['XA_18']=EMA(zhulidf['收盘'],5)-EMA(zhulidf['收盘'],10)
  zhulidf['XA_19']=EMA(zhulidf['XA_18'],9)
  zhulidf['XA_21']=0-100*(HHV(zhulidf['收盘'],10)-zhulidf['收盘'])/(HHV(zhulidf['收盘'],10)-LLV(zhulidf['最低'],10))+100
  zhulidf['XA_24']=REF(zhulidf['XA_19'],1)
  zhulidf['XA_26']=zhulidf['XA_19']-zhulidf['XA_24']
  zhulidf['XA_27']=REF(zhulidf['XA_18'],1)
  zhulidf['XA_29']=zhulidf['XA_18']-zhulidf['XA_27']
  
  # XA_32:=(CLOSE-LLV(LOW,27))/(HHV(HIGH,27)-LLV(LOW,27))*100;
  # XA_33:=REVERSE(XA_32);
  # XA_34:=SMA(XA_32,3,1);
  # 趋势:SMA(XA_34,3,1);
  zhulidf['XA_32']=(zhulidf['收盘']-LLV(zhulidf['最低'],27))/(HHV(zhulidf['最高'],27)-LLV(zhulidf['最低'],27))*100
  zhulidf['XA_34']=SMA(zhulidf['XA_32'],3,1)
  zhulidf['趋势']=SMA(zhulidf['XA_34'],3,1)
  #私募进场
  condition1=(zhulidf['开盘']<=zhulidf['MA5'])
  condition2=(zhulidf['开盘']<=zhulidf['MA10'])
  condition3=(zhulidf['开盘']<=zhulidf['MA30'])
  condition4=(zhulidf['收盘']>=zhulidf['MA5'])
  condition5=(zhulidf['收盘']>=zhulidf['XA_17'])
  condition6=(zhulidf['XA_26']>=0)
  condition7=(zhulidf['XA_29']>=0)
  zhulidf['私募进场']=0
  zhulidf.loc[condition1 & condition2& condition3& condition4& condition5& condition6& condition7, '私募进场'] = 1
  #短线指标 IF(CROSS(XA_18,XA_19) AND XA_18<0 AND XA_19<0-0.2 AND XA_21>45 AND XA_26>0,(-10),0)
  condition11=(zhulidf['XA_18'].shift(1)<zhulidf['XA_19'].shift(1))
  condition12=(zhulidf['XA_18']>=zhulidf['XA_19'])
  condition22=(zhulidf['XA_18']<0)
  condition23=(zhulidf['XA_19']<-0.2)
  condition24=(zhulidf['XA_21']>45)
  condition25=(zhulidf['XA_26']>=0)
  zhulidf['短线指标']=0
  zhulidf.loc[condition11 & condition12 &condition22& condition23& condition24& condition25 , '短线指标'] = 1
  # 将无关的变量删除
  zhulidf.drop(['XA_1','XA_2','XA_3','XA_4','XA_5','XA_6','XA_8','XA_9','MA5','MA10','MA30','XA_16','XA_17','XA_18','XA_19','XA_117','XA_1117','XA_21','XA_24','XA_26','XA_27','XA_29','XA_32','XA_34'], axis=1, inplace=True)
  return zhulidf

def zhulijiancang_month(df):
  #主力建仓
  df['XA_1']=REF(df['最低'],1)
  df['XA_2']=SMA(ABS(df['最低']-df['XA_1']),3,1)/SMA(MAX(df['最低']-df['XA_1'],0),3,1)*100
  df['XA_3']=EMA(IF(df['收盘']*1.2,df['XA_2']*10,df['XA_2']/10),3)
  df['XA_4']=LLV(df['最低'],50)
  df['XA_5']=HHV(df['XA_3'],50)
  df['XA_6']=IF(LLV(df['最低'],90),1,0)
  df['主力建仓']=EMA(IF(df['最低']<=df['XA_4'],(df['XA_3']+df['XA_5']*2)/2,0),3)/618*df['XA_6']
  df['XA_8']=(df['收盘']-LLV(df['最低'],21))/(HHV(df['最高'],21)-LLV(df['最低'],21))*100
  df['XA_9']=SMA(df['XA_8'],13,8)
  df['风险']=SMA(df['XA_9'],13,8)
  df['涨跌']=MA(3*SMA((df['收盘']-LLV(df['最低'],27))/(HHV(df['最高'],27)-LLV(df['最低'],27))*100,5,1)-2*SMA(SMA((df['收盘']-LLV(df['最低'],27))/(HHV(df['最高'],27)-LLV(df['最低'],27))*100,5,1),3,1),5) 

  #golden bottom indicator
  df['机构']=EMA(100*(df['收盘']-LLV(df['最低'],34))/(HHV(df['最高'],34)-LLV(df['最低'],34)),3)
  df['VAR000']=(df['收盘']-LLV(df['最低'],60))/(HHV(df['最高'],60)-LLV(df['最低'],60))*100
  df['VAR003']=SMA(df['VAR000'],3,1)
  df['VAR001']=SMA(df['VAR003'],4,1)-50
  df['VAR002']=REF(df['VAR001'],1)<df['VAR001']
  condition1=(df['机构']>9)
  condition2=(REF(df['机构'],1)<9)
  df['CROSS']=0
  df.loc[condition1 & condition2, 'CROSS']=1
  condition3=(df['VAR002']==1)
  condition4=(df['CROSS']==1)
  df['goldbottom']=0
  df.loc[condition3&condition4, 'goldbottom']=1
  # 将无关的变量删除
  df.drop(['XA_1','XA_2','XA_3','XA_4','XA_5','XA_6','XA_8','XA_9','机构','VAR000','VAR003','VAR001','VAR002','CROSS'], axis=1, inplace=True)
  return df

def zhulijiancang(zhulidf):
  #主力建仓
  zhulidf['XA_1']=REF(zhulidf['最低'],1)
  zhulidf['XA_2']=SMA(ABS(zhulidf['最低']-zhulidf['XA_1']),3,1)/SMA(MAX(zhulidf['最低']-zhulidf['XA_1'],0),3,1)*100
  zhulidf['XA_3']=EMA(IF(zhulidf['收盘']*1.2,zhulidf['XA_2']*10,zhulidf['XA_2']/10),3)
  zhulidf['XA_4']=LLV(zhulidf['最低'],38)
  zhulidf['XA_5']=HHV(zhulidf['XA_3'],38)
  zhulidf['XA_6']=IF(LLV(zhulidf['最低'],90),1,0)
  zhulidf['主力建仓']=EMA(IF(zhulidf['最低']<=zhulidf['XA_4'],(zhulidf['XA_3']+zhulidf['XA_5']*2)/2,0),3)/618*zhulidf['XA_6']
  zhulidf['XA_8']=(zhulidf['收盘']-LLV(zhulidf['最低'],21))/(HHV(zhulidf['最高'],21)-LLV(zhulidf['最低'],21))*100
  zhulidf['XA_9']=SMA(zhulidf['XA_8'],13,8)
  zhulidf['风险']=SMA(zhulidf['XA_9'],13,8)
  zhulidf['涨跌']=MA(3*SMA((zhulidf['收盘']-LLV(zhulidf['最低'],27))/(HHV(zhulidf['最高'],27)-LLV(zhulidf['最低'],27))*100,5,1)-2*SMA(SMA((zhulidf['收盘']-LLV(zhulidf['最低'],27))/(HHV(zhulidf['最高'],27)-LLV(zhulidf['最低'],27))*100,5,1),3,1),5) 
  zhulidf['主力建仓消化']=MA(EMA((zhulidf['XA_3']+zhulidf['XA_5']*2)/2,3)/618*zhulidf['XA_6'],30)
  zhulidf['主力消化均值']=MA(zhulidf['主力建仓消化'],5)
  
  zhulidf['MA5']=MA(zhulidf['收盘'],5)#xa8
  zhulidf['MA10']=MA(zhulidf['收盘'],10)#xa9
  zhulidf['MA30']=MA(zhulidf['收盘'],30)#xa10
  zhulidf['XA_16']=SUM(zhulidf['收盘']*zhulidf['成交量']*100,28)/SUM(zhulidf['成交量']*100,28)
  zhulidf['XA_117']=(zhulidf['XA_16']*100)
  zhulidf['XA_1117']=zhulidf['XA_117'].round(0)
  zhulidf['XA_17']=zhulidf['XA_1117']/100 #XA_17
  zhulidf['XA_18']=EMA(zhulidf['收盘'],5)-EMA(zhulidf['收盘'],10)
  zhulidf['XA_19']=EMA(zhulidf['XA_18'],9)
  zhulidf['XA_21']=0-100*(HHV(zhulidf['收盘'],10)-zhulidf['收盘'])/(HHV(zhulidf['收盘'],10)-LLV(zhulidf['最低'],10))+100
  zhulidf['XA_24']=REF(zhulidf['XA_19'],1)
  zhulidf['XA_26']=zhulidf['XA_19']-zhulidf['XA_24']
  zhulidf['XA_27']=REF(zhulidf['XA_18'],1)
  zhulidf['XA_29']=zhulidf['XA_18']-zhulidf['XA_27']

  #私募进场
  condition1=(zhulidf['开盘']<=zhulidf['MA5'])
  condition2=(zhulidf['开盘']<=zhulidf['MA10'])
  condition3=(zhulidf['开盘']<=zhulidf['MA30'])
  condition4=(zhulidf['收盘']>=zhulidf['MA5'])
  condition5=(zhulidf['收盘']>=zhulidf['XA_17'])
  condition6=(zhulidf['XA_26']>=0)
  condition7=(zhulidf['XA_29']>=0)
  zhulidf['私募进场']=0
  zhulidf.loc[condition1 & condition2& condition3& condition4& condition5& condition6& condition7, '私募进场'] = 1
  #短线指标 IF(CROSS(XA_18,XA_19) AND XA_18<0 AND XA_19<0-0.2 AND XA_21>45 AND XA_26>0,(-10),0)
  condition11=(zhulidf['XA_18'].shift(1)<zhulidf['XA_19'].shift(1))
  condition12=(zhulidf['XA_18']>=zhulidf['XA_19'])
  condition22=(zhulidf['XA_18']<0)
  condition23=(zhulidf['XA_19']<-0.2)
  condition24=(zhulidf['XA_21']>45)
  condition25=(zhulidf['XA_26']>=0)
  zhulidf['短线指标']=0
  zhulidf.loc[condition11 & condition12 &condition22& condition23& condition24& condition25 , '短线指标'] = 1
  # 将无关的变量删除
  zhulidf.drop(['XA_1','XA_2','XA_3','XA_4','XA_5','XA_6','XA_8','XA_9','MA5','MA10','MA30','XA_16','XA_17','XA_18','XA_19','XA_117','XA_1117','XA_21','XA_24','XA_26','XA_27','XA_29'], axis=1, inplace=True)
  return zhulidf

def ZJFZ(df):
  #追击反转
  df['XA_1']=REF(df['最低'],1)
  df['XA_2']=SMA(ABS(df['最低']-df['XA_1']),3,1)/SMA(MAX(df['最低']-df['XA_1'],0),3,1)*100
  df['XA_3']=EMA(IF(df['收盘']*1.2,df['XA_2']*10,df['XA_2']/10),3)
  df['XA_4']=LLV(df['最低'],50)
  df['XA_5']=HHV(df['XA_3'],50)
  df['XA_6']=IF(LLV(df['最低'],90),1,0)
  df['主力建仓']=EMA(IF(df['最低']<=df['XA_4'],(df['XA_3']+df['XA_5']*2)/2,0),3)/618*df['XA_6']
  df['XA_8']=(df['收盘']-LLV(df['最低'],21))/(HHV(df['最高'],21)-LLV(df['最低'],21))*100
  df['XA_9']=SMA(df['XA_8'],13,8)
  df['风险']=SMA(df['XA_9'],13,8)
  df['涨跌']=MA(3*SMA((df['收盘']-LLV(df['最低'],27))/(HHV(df['最高'],27)-LLV(df['最低'],27))*100,5,1)-2*SMA(SMA((df['收盘']-LLV(df['最低'],27))/(HHV(df['最高'],27)-LLV(df['最低'],27))*100,5,1),3,1),5) 
  df['SMASELL']=SMA((df['收盘']-LLV(df['最低'],27))/(HHV(df['最高'],27)-LLV(df['最低'],27))*100,7,1)
  #计算合并
  df['合并']=(df['风险']+df['涨跌'])/2
  # 将无关的变量删除
  df.drop(['XA_1','XA_2','XA_3','XA_4','XA_5','XA_6','XA_8','XA_9'], axis=1, inplace=True)
  return df

#六因子
def Sixfactor(df):

  df['DIF']=EMA(df['收盘'],12)-EMA(df['收盘'],26)
  df['DEA']=EMA(df['DIF'],9)
  df['MACDDIFF']=df['DIF']-df['DEA']
  conditionMACD1=(df['MACDDIFF']>0)
  df['MACDCROSS']=0
  df.loc[conditionMACD1, 'MACDCROSS']=1

  df['RSV']=(df['收盘']-LLV(df['最低'],9))/(HHV(df['最高'],9)-LLV(df['最低'],9))*100
  df['K']=SMA(df['RSV'],3,1)
  df['D']=SMA(df['K'],3,1)
  df['KDDIFF']=df['K']-df['D']
  conditionKD1=(df['KDDIFF']>0)
  df['KDCROSS']=0
  df.loc[conditionKD1, 'KDCROSS']=1

  df['LC']=REF(df['收盘'],1)
  df['RSI1']=(SMA(MAX(0,df['收盘']-df['LC']),6,1)/SMA(ABS(df['收盘']-df['LC']),6,1)*100)
  df['RSI2']=(SMA(MAX(0,df['收盘']-df['LC']),12,1)/SMA(ABS(df['收盘']-df['LC']),12,1)*100)
  df['RSIDIFF']=df['RSI1']-df['RSI2']
  conditionRSI1=(df['RSIDIFF']>0)
  df['RSICROSS']=0
  df.loc[conditionRSI1, 'RSICROSS']=1

  df['LWRRSV']=(HHV(df['最高'],9)-df['收盘'])/(HHV(df['最高'],9)-LLV(df['最低'],9))*100
  df['LWR1']=SMA(df['LWRRSV'],3,1)
  df['LWR2']=SMA(df['LWR1'],3,1)
  df['LWRDIFF']=df['LWR2']-df['LWR1']
  conditionLWR1=(df['LWRDIFF']>0)
  df['LWRCROSS']=0
  df.loc[conditionLWR1, 'LWRCROSS']=1

  df['BBI']=(MA(df['收盘'],3)+MA(df['收盘'],6)+MA(df['收盘'],12)+MA(df['收盘'],24))/4
  df['BBICLOSE']=IF(df['收盘']>df['BBI'],1,0)

  df['MTM'],df['MTMMA']=MTM(df['收盘'])
  df['MTMDIFF']=df['MTM']-df['MTMMA']
  conditionMTM1=(df['MTMDIFF']>0)
  df['MTMCROSS']=0
  df.loc[conditionMTM1, 'MTMCROSS']=1

  df['6']=df['MACDCROSS']+df['KDCROSS']+df['RSICROSS']+df['LWRCROSS']+df['BBICLOSE']+df['MTMCROSS']
  df['SIX']=IF(df['6']==6,2,0)
  df['SIXSELL']=IF(df['6']<4,1,0)
  return df



# 计算支撑位，输入当前时间，最低价df和时间窗口
def zhichengwei(timenow,zuididf,timewindow):
  #计算当前时间时间窗口内的最低价
  zuidi=zuididf['最低'].rolling(timewindow).min()
  zhichengprice=zuidi.at[timenow,'最低']
  return zhichengprice

# 计算偏移买卖指标
def pianyi(pianyidf):
  #偏移指标
  pianyidf['VAR1']=(pianyidf['最低']+pianyidf['最高']+pianyidf['收盘']+pianyidf['开盘'])/4
  pianyidf['EMA']=EMA(pianyidf['VAR1'],20)
  #输出结果
  return pianyidf
  
# 温度计
def wendu(wendudf):
  #写入条件
  wendudf['9日最低']=wendudf['最低'].rolling(9).min()
  wendudf['9日最高'] = wendudf['最高'].rolling(9).max()
  wendudf['LLV']=wendudf['收盘']-wendudf['9日最低']
  wendudf['HHV']=wendudf['9日最高']-wendudf['9日最低']
  wendudf['rsv']=(wendudf['LLV']/wendudf['HHV'])*100
  # 1.(当日收盘价-9日内最低价)/（9日内最高价-9日内最低价）*100
  condition1 = (wendudf['rsv'])
  #2.K=rsv的3日移动平均
  wendudf['K']=SMA(wendudf['rsv'],3,1)
  # LC=前一日收盘价
  wendudf['LC']=wendudf['收盘'].rolling(2).sum()-wendudf['收盘']
  #3.RSI1=【（收盘-LC）与0的最大值的6日移动平均】/【收盘-lc的绝对值的6日移动平均】*100
  wendudf['rsi1max']= wendudf['收盘']-wendudf['LC']
  condition1 = (wendudf['rsi1max'] <= 0)
  wendudf.loc[condition1, 'rsi1max'] = 0
  wendudf['rsi1r6']=SMA(wendudf['rsi1max'],6,1)
  wendudf['rsi2']= wendudf['收盘']-wendudf['LC']
  condition2 = (wendudf['rsi2'] < 0)
  wendudf.loc[condition2, 'rsi2'] = wendudf['rsi2'].abs()
  wendudf['rsi2r6']=SMA(wendudf['rsi2'],6,1)
  wendudf['rsi1']=((wendudf['rsi1r6']/wendudf['rsi2r6'])*100)
  #温度:(K+RSI1)/2;
  wendudf['温度']=(wendudf['rsi1']+wendudf['K'])/2
  #删除中间过程列
  wendudf.drop(['9日最低', '9日最高','rsv','K','LC','rsi1max','rsi1r6','rsi2','rsi1'], axis=1, inplace=True)
  #输出结果
  resultwendudf=pd.DataFrame(data=[])
  resultwendudf['日期']=wendudf['日期']
  resultwendudf['温度']=wendudf['温度']
  
  return resultwendudf

#机构杀手指标 
def JGSS(zhulidf):
  #主力建仓
  zhulidf['邰1']=LLV(zhulidf['最低'],8)
  zhulidf['邰2']=HHV(zhulidf['最高'],13)
  zhulidf['股市黑客']=EMA((zhulidf['收盘']-zhulidf['邰1'])/(zhulidf['邰2']-zhulidf['邰1'])*100,5)
  zhulidf['升跌临界']=EMA(0.667*REF(zhulidf['股市黑客'],1)+0.333*zhulidf['股市黑客'],2)

  C1=zhulidf['收盘']>REF(zhulidf['收盘'],1)
  C2=zhulidf['收盘']>REF(zhulidf['收盘'],2)
  zhulidf.loc[C1 & C2 , 'VAR1'] = 1
  C3=zhulidf['收盘']<REF(zhulidf['收盘'],1)
  C4=zhulidf['收盘']<REF(zhulidf['收盘'],2)
  zhulidf.loc[C3 & C4 , 'VARD'] = 1
  C5=REF(zhulidf['VARD'],1)
  C6=zhulidf['收盘']>=REF(zhulidf['收盘'],1)
  C7=zhulidf['收盘']<=REF(zhulidf['收盘'],2)
  zhulidf.loc[C5 & C6 & C7, 'VARE'] = 1
  C8=REF(zhulidf['VARE'],1)
  C9=zhulidf['收盘']<=REF(zhulidf['收盘'],1)
  C10=zhulidf['收盘']>=REF(zhulidf['收盘'],2)
  zhulidf.loc[C8 & C9 & C10, 'VARF'] = 1
  C11=REF(zhulidf['VARF'],1)
  C12=zhulidf['收盘']>=REF(zhulidf['收盘'],1)
  C13=zhulidf['收盘']<=REF(zhulidf['收盘'],2)
  zhulidf.loc[C11 & C12 & C13, 'VAR10'] = 1
  C14=REF(zhulidf['VAR10'],1)
  C15=zhulidf['收盘']<=REF(zhulidf['收盘'],1)
  C16=zhulidf['收盘']>=REF(zhulidf['收盘'],2)
  zhulidf.loc[C14 & C15 & C16, 'VAR11'] = 1
  C17=REF(zhulidf['VAR11'],1)
  C18=zhulidf['收盘']>=REF(zhulidf['收盘'],1)
  C19=zhulidf['收盘']<=REF(zhulidf['收盘'],2)
  zhulidf.loc[C17 & C18 & C19, 'VAR12'] = 1
  C20=REF(zhulidf['VAR12'],1)
  C21=zhulidf['收盘']<=REF(zhulidf['收盘'],1)
  C22=zhulidf['收盘']>=REF(zhulidf['收盘'],2)
  zhulidf.loc[C20 & C21 & C22, 'VAR13'] = 1
  C23=REF(zhulidf['VAR13'],1)
  C24=zhulidf['收盘']>=REF(zhulidf['收盘'],1)
  C25=zhulidf['收盘']<=REF(zhulidf['收盘'],2)
  zhulidf.loc[C23 & C24 & C25, 'VAR14'] = 1
  C26=REF(zhulidf['VAR14'],1)
  C27=zhulidf['收盘']<=REF(zhulidf['收盘'],1)
  C28=zhulidf['收盘']>=REF(zhulidf['收盘'],2)
  zhulidf.loc[C26 & C27 & C28, 'VAR15'] = 1
  C29=REF(zhulidf['VAR15'],1)
  C30=zhulidf['收盘']>=REF(zhulidf['收盘'],1)
  C31=zhulidf['收盘']<=REF(zhulidf['收盘'],2)
  zhulidf.loc[C29 & C30 & C31, 'VAR16'] = 1
  C32=REF(zhulidf['VAR16'],1)
  C33=zhulidf['收盘']<=REF(zhulidf['收盘'],1)
  C34=zhulidf['收盘']>=REF(zhulidf['收盘'],2)
  zhulidf.loc[C32 & C33 & C34, 'VAR17'] = 1
  C35=REF(zhulidf['VAR17'],1)
  C36=zhulidf['收盘']>=REF(zhulidf['收盘'],1)
  C37=zhulidf['收盘']<=REF(zhulidf['收盘'],2)
  zhulidf.loc[C35 & C36 & C37, 'VAR18'] = 1

  CVARD=(zhulidf['VARD']==1)
  CVARE=(zhulidf['VARE']==1)
  CVARF=(zhulidf['VARF']==1)
  CVAR10=(zhulidf['VAR10']==1)
  CVAR11=(zhulidf['VAR11']==1)
  CVAR12=(zhulidf['VAR12']==1)
  CVAR13=(zhulidf['VAR13']==1)
  CVAR14=(zhulidf['VAR14']==1)
  CVAR15=(zhulidf['VAR15']==1)
  CVAR16=(zhulidf['VAR16']==1)
  CVAR17=(zhulidf['VAR17']==1)
  CVAR18=(zhulidf['VAR18']==1)

  zhulidf.loc[CVARD | CVARE | CVARF |CVAR10 | CVAR11 | CVAR12 | CVAR13 | CVAR14| CVAR15| CVAR16| CVAR17| CVAR18, 'C38'] = 1
  C39=REF(zhulidf['C38'],1)
  CVAR1=(zhulidf['VAR1']==1)
  zhulidf.loc[C39 & CVAR1 , 'VAR19'] = 1

  C40=REF(zhulidf['股市黑客'],1)<=13
  #CROSS方法有问题
  # C41=CROSS(zhulidf['股市黑客'],zhulidf['升跌临界'])>1
  condition1=zhulidf['股市黑客']>zhulidf['升跌临界']
  condition2=zhulidf['股市黑客'].shift(1)<=zhulidf['升跌临界'].shift(1)
  zhulidf.loc[condition1 & condition2 , 'C41'] = 1
  C411=(zhulidf['C41']==1)

  C42=REF(MA(zhulidf['收盘'],55),1)>REF(MA(zhulidf['收盘'],55),2)
  # zhulidf.loc[C40 & C41 & C42, 'C43'] = 1
  #股市黑客+CROSS+REF=C43
  zhulidf.loc[C40 & C411 & C42, 'C43'] = 1
  #OR=C45
  C44=REF(zhulidf['VAR19'],1)
  CVAR19=zhulidf['VAR19']==1
  zhulidf.loc[CVAR19|C44 , 'C45'] = 1

  CC43=zhulidf['C43']==1
  CC45=zhulidf['C45']==1
  
  zhulidf.loc[CC43 & CC45 , 'JGSS'] = 1



  # 将无关的变量删除
  zhulidf.drop(['邰1','邰2','VAR1','VARD','VARE','VARF','VAR10','VAR11','VAR12','VAR13','VAR14','VAR15','VAR16','VAR17','VAR18','XA_117','XA_1117','XA_21','XA_24','XA_26','XA_27','XA_29','XA_32','XA_34'], axis=1, inplace=True)
  return zhulidf

#机构杀手买入指标
def jgss_buy(df):
  df['1型买入']=0
  df['4型买入']=0
  #1型买入 REF((MA(C,5)-C)/C>0.04 AND (MA(C,10)-MA(C,5))/MA(C,5)>0.04 AND 观望期=0 AND C>REF(C,1),1)
  df['B1']=(HHV(df['最高'],9)-df['收盘'])/(HHV(df['最高'],9)-LLV(df['最低'],9))*100- 70
  df['B2']=SMA(df['B1'],9,1)+100
  df['B3']=(df['收盘']-LLV(df['最低'],9))/(HHV(df['最高'],9)- LLV(df['最低'],9))*100
  df['B4']=SMA(df['B3'],3,1)
  df['B5']=SMA(df['B4'],3,1)+100
  df['B6']=df['B5']-df['B2']
  df['观望期']=IF(df['B6']>60,df['B6']-60,0)
  condition1=(MA(df['收盘'],5)-df['收盘'])/df['收盘']>0.04
  condition2=(MA(df['收盘'],10)-MA(df['收盘'],5))/MA(df['收盘'],5)>0.04
  condition3=(df['观望期']==0)
  condition4=(df['收盘']>REF(df['收盘'],1))
  df.loc[condition1 & condition2 & condition3 & condition4, ['1型买入']] = 1
  df['1型买入']=REF(df['1型买入'],1)
  #4型底背离买入
  df['DIFF']=EMA(df['收盘'],12)-EMA(df['收盘'],26)
  df['DEA']=EMA(df['DIFF'],9)
  conditionA01=df['DIFF']>df['DEA']
  conditionA02=REF(df['DIFF'],1)<REF(df['DEA'],1)
  df['cross']=0
  df.loc[conditionA01 & conditionA02, 'cross'] = 1
  df['crossref']=REF(df['cross'],1)
  df['A1']=BARSLAST(df['crossref'])
  condition41=REF(df['收盘'],df['A1']+1)>df['收盘']
  condition42=df['DIFF']>REF(df['DIFF'],df['A1']+1)
  condition43=df['cross']==1
  df.loc[condition41 & condition42 & condition43, ['4型买入']] = 1
  df.drop(['B1','B2','B3','B4','B5','B6','观望期','DIFF','DEA','cross','A1'], axis=1, inplace=True)
  return df

def golden_bottom(df):
  df['机构'] = EMA(100*(df['收盘']-LLV(df['最低'],34))/(HHV(df['最高'],34)-LLV(df['最低'],34)),3)
  df['VAR000'] =(df['收盘']-LLV(df['最低'],60))/(HHV(df['最高'],60)-LLV(df['最低'],60))*100
  df['VAR003'] =SMA(df['VAR000'],3,1)
  df['VAR001'] =SMA(df['VAR003'],4,1)-50
  df['VAR002'] =REF(df['VAR001'],1)<df['VAR001']
  df['机构cgolden_bottomross']=0
  condition1 = (df['机构']>9)
  condition2 = (REF(df['机构'],1)<9)
  condition3=(df['VAR002']==1)
  df.loc[condition1 & condition2&condition3, ['golden_bottom']] = 1
  return df

def dynamic_MACD(readdf):
    
    readdf['日期'] = pd.to_datetime(readdf['日期'])
    readdf.set_index('日期', inplace=True)

      #计算月K
    df_month = pd.DataFrame()
    df_month['开盘'] = readdf['开盘'].resample('M').first()
    df_month['收盘'] = readdf['收盘'].resample('M').last()
    df_month['最高'] = readdf['最高'].resample('M').max()
    df_month['最低'] = readdf['最低'].resample('M').min()
    #计算月K指标
    df_month['DIF'],df_month['DEA'],df_month['MACD']=MACD(df_month['收盘'])
    df_month.reset_index(inplace=True) #清除当前index列，使其变回column普通数据
    readdf.reset_index(inplace=True) #清除当前index列，使其变回column普通数据
    #计算月风险涨跌值
    readdf['当前年']=readdf['日期'].dt.year
    readdf['当前月']=readdf['日期'].dt.month
    readdf['当前日']=readdf['日期'].dt.dayofyear
    df_month['所在年']=df_month['日期'].dt.year
    df_month['所在月']=df_month['日期'].dt.month
    df_month['所在日']=df_month['日期'].dt.dayofyear
    #补齐df_month缺失值
    df_month=df_month.fillna(method='ffill')
    #月KMACD赋予日K
    for i in range(0, readdf.shape[0]):
        #循环日K，每一日收盘重新赋值月df中当月收盘
        shoupan=readdf.at[i, '收盘']
        #动态月df
        df_moving=df_month
        dfmval = df_month.to_numpy()
        rdfval = readdf.to_numpy()

        movdf所在年列 = df_month.columns.get_loc('所在年')
        movdf所在月列 = df_month.columns.get_loc('所在月')

        readdf当前年列 = readdf.columns.get_loc('当前年')
        readdf当前月列 = readdf.columns.get_loc('当前月')

        #找到当月日期并赋予本月收盘，重新计算macd
        for o in range(0, df_month.shape[0]):
          #找到对应年月
            #不使用at方法而是在循环前找到年月可以减少一层at内置的遍历提高速度
            #if df_moving.at[o, '所在年'] == readdf.at[i, '当前年'] and df_moving.at[o, '所在月'] == readdf.at[i, '当前月']:
            if dfmval[o, movdf所在年列] == rdfval[i, readdf当前年列] and dfmval[o, movdf所在月列] == rdfval[i, readdf当前月列]:

                df_moving.at[o, '收盘']=shoupan
                #计算动态macd
                df_moving['DIF'],df_moving['DEA'],df_moving['MACD']=MACD(df_moving['收盘'])
                #计算月Kmacd相比较上个月是否上涨
                df_moving['MACD相差']=df_moving['MACD']-df_moving['MACD'].shift(1)
                #将计算结果返回给日Kdf
                if df_moving.at[o, 'MACD相差']>-10000:
                    readdf.loc[i, "月MACD"]=df_moving.at[o, 'MACD']
                    readdf.loc[i, "MACD相差"]=df_moving.at[o, 'MACD相差']
    # nancount = 0 #数一下多少个nan
    # for i in range(0, readdf.shape[0]): 
    #     if pd.isna(readdf['月MACD'][i]):
    #         nancount = nancount +1
    # debugout = debugout + f"NaN计数:\t {nancount}\n"
    # macd_calculate = time.time()  # 获取此前代码块运行消耗时间
    # debugout = debugout + f"MACD用时: \t {macd_calculate - start_time}\n"

    return readdf

def dynamic_Week_trend(readdf):
    
    readdf['日期'] = pd.to_datetime(readdf['日期'])
    readdf.set_index('日期', inplace=True)

    #计算周K
    df_week = pd.DataFrame()
    df_week['开盘'] = readdf['开盘'].resample('W').first()
    df_week['收盘'] = readdf['收盘'].resample('W').last()
    df_week['最高'] = readdf['最高'].resample('W').max()
    df_week['最低'] = readdf['最低'].resample('W').min()

    df_week.reset_index(inplace=True) #清除当前index列，使其变回column普通数据
    readdf.reset_index(inplace=True) #清除当前index列，使其变回column普通数据
    #计算月风险涨跌值
    readdf['当前年']=readdf['日期'].dt.year
    readdf['当前周']=readdf['日期'].dt.isocalendar().week
    readdf['当前日']=readdf['日期'].dt.dayofyear
    df_week['所在年']=df_week['日期'].dt.year
    df_week['所在周']=df_week['日期'].dt.isocalendar().week
    df_week['所在日']=df_week['日期'].dt.dayofyear
    #补齐df_week缺失值
    df_week=df_week.fillna(method='ffill')
    df_week['DIFWEEK']=0
    df_week['DEAWEEK']=0
    df_week['MACDWEEK']=0

    #月KMACD赋予日K
    for i in range(0, readdf.shape[0]):

        #动态月df
        df_moving=df_week
        dfmval = df_week.to_numpy()
        rdfval = readdf.to_numpy()

        movdf所在年列 = df_week.columns.get_loc('所在年')
        movdf所在周列 = df_week.columns.get_loc('所在周')

        readdf当前年列 = readdf.columns.get_loc('当前年')
        readdf当前周列 = readdf.columns.get_loc('当前周')

        #找到当月日期并赋予本月收盘，重新计算macd
        for o in range(0, df_week.shape[0]):
          #找到对应年月
            #不使用at方法而是在循环前找到年月可以减少一层at内置的遍历提高速度
            #if df_moving.at[o, '所在年'] == readdf.at[i, '当前年'] and df_moving.at[o, '所在月'] == readdf.at[i, '当前月']:
            if dfmval[o, movdf所在年列] == rdfval[i, readdf当前年列] and dfmval[o, movdf所在周列] == rdfval[i, readdf当前周列]:

                #计算周K指标
                df_moving['DIFWEEK'],df_moving['DEAWEEK'],df_moving['MACDWEEK']=MACD(df_moving['收盘'])
                #返回df
                readdf.loc[i, "DIFWEEK"]=df_moving.at[o, 'DIFWEEK']
                readdf.loc[i, "DEAWEEK"]=df_moving.at[o, 'DEAWEEK']
                readdf.loc[i, "MACDWEEK"]=df_moving.at[o, 'MACDWEEK']

    # nancount = 0 #数一下多少个nan
    # for i in range(0, readdf.shape[0]): 
    #     if pd.isna(readdf['月MACD'][i]):
    #         nancount = nancount +1
    # debugout = debugout + f"NaN计数:\t {nancount}\n"
    # macd_calculate = time.time()  # 获取此前代码块运行消耗时间
    # debugout = debugout + f"MACD用时: \t {macd_calculate - start_time}\n"
    
    # df_week.to_csv('603918week.csv', index=False,
    #   mode='w',  # mode='w'代表覆盖之前的文件，mode='a'，代表接在原来数据的末尾
    #   float_format='%.15f',  # 控制输出浮点数的精度
    #   # header=None,  # 不输出表头
    #   encoding='GBK'
    # )

    return readdf

def dynamic_month_trend(readdf):
    
    readdf['日期'] = pd.to_datetime(readdf['日期'])
    readdf.set_index('日期', inplace=True)

    #计算周K
    df_month = pd.DataFrame()
    df_month['开盘'] = readdf['开盘'].resample('M').first()
    df_month['收盘'] = readdf['收盘'].resample('M').last()
    df_month['最高'] = readdf['最高'].resample('M').max()
    df_month['最低'] = readdf['最低'].resample('M').min()

    df_month.reset_index(inplace=True) #清除当前index列，使其变回column普通数据
    readdf.reset_index(inplace=True) #清除当前index列，使其变回column普通数据
    #计算月风险涨跌值
    readdf['当前年']=readdf['日期'].dt.year
    readdf['当前月']=readdf['日期'].dt.month
    readdf['当前日']=readdf['日期'].dt.dayofyear
    df_month['所在年']=df_month['日期'].dt.year
    df_month['所在月']=df_month['日期'].dt.month
    df_month['所在日']=df_month['日期'].dt.dayofyear
    #补齐df_week缺失值
    df_month=df_month.fillna(method='ffill')
    df_month['绿柱WEEK']=0
    df_month['金柱WEEK']=0
    df_month['红柱WEEK']=0
    df_month['SAR红']=0
    df_month['BOLL红']=0

    #月KMACD赋予日K
    for i in range(0, readdf.shape[0]):
        #循环日K，每一日收盘重新赋值月df中当月收盘
        shoupan=readdf.at[i, '收盘']
        #动态月df
        df_moving=df_month
        dfmval = df_month.to_numpy()
        rdfval = readdf.to_numpy()

        movdf所在年列 = df_month.columns.get_loc('所在年')
        movdf所在月列 = df_month.columns.get_loc('所在月')

        readdf当前年列 = readdf.columns.get_loc('当前年')
        readdf当前月列 = readdf.columns.get_loc('当前月')

        #找到当月日期并赋予本月收盘，重新计算macd
        for o in range(0, df_month.shape[0]):
          #找到对应年月
            #不使用at方法而是在循环前找到年月可以减少一层at内置的遍历提高速度
            #if df_moving.at[o, '所在年'] == readdf.at[i, '当前年'] and df_moving.at[o, '所在月'] == readdf.at[i, '当前月']:
            if dfmval[o, movdf所在年列] == rdfval[i, readdf当前年列] and dfmval[o, movdf所在月列] == rdfval[i, readdf当前月列]:

                #计算月K指标
                df_month['x1'] = (df_month['收盘']+df_month['最低']+df_month['最高'])/3
                df_month['x2'] = EMA(df_month['x1'],6)
                df_month['x3'] = EMA(df_month['x2'],5)
                conditionweek1=(df_month['x2']>df_month['x3'])
                df_month.loc[conditionweek1  , '红柱WEEK'] = 1

                conditionweek2=(REF(df_month['x3'],1)>REF(df_month['x2'],1))
                conditionweek3=(df_month['x2']<df_month['x3'])
                df_month.loc[conditionweek1 &conditionweek2 , '金柱WEEK'] = 1
                df_month.loc[conditionweek3 , '绿柱WEEK'] = 1

                df_month['SAR'] = TDX_SAR(df_month['最高'],df_month['最低'])
                conditionweek4=(df_month['收盘']>df_month['SAR'])
                df_month.loc[conditionweek4 , 'SAR红'] = 1

                df_month['UP'],df_month['BOLL'],df_month['LOW']=BOLLM(df_month['收盘'])
                conditionweek5=(df_month['收盘']>df_month['BOLL'])
                df_month.loc[conditionweek5 , 'BOLL红'] = 1
                
                #返回df
                readdf.loc[i, "红柱WEEK"]=df_moving.at[o, '红柱WEEK']
                readdf.loc[i, "金柱WEEK"]=df_moving.at[o, '金柱WEEK']
                readdf.loc[i, "绿柱WEEK"]=df_moving.at[o, '绿柱WEEK']
                readdf.loc[i, "SAR红"]=df_moving.at[o, 'SAR红']
                readdf.loc[i, "BOLL红"]=df_moving.at[o, 'BOLL红']

    # nancount = 0 #数一下多少个nan
    # for i in range(0, readdf.shape[0]): 
    #     if pd.isna(readdf['月MACD'][i]):
    #         nancount = nancount +1
    # debugout = debugout + f"NaN计数:\t {nancount}\n"
    # macd_calculate = time.time()  # 获取此前代码块运行消耗时间
    # debugout = debugout + f"MACD用时: \t {macd_calculate - start_time}\n"
    
    # df_week.to_csv('603918week.csv', index=False,
    #   mode='w',  # mode='w'代表覆盖之前的文件，mode='a'，代表接在原来数据的末尾
    #   float_format='%.15f',  # 控制输出浮点数的精度
    #   # header=None,  # 不输出表头
    #   encoding='GBK'
    # )

    return readdf

#买就盈利
def MJYL(df):
  df['VAR1G'] = (((df['收盘']/MA(df['收盘'],40))*100)<78)
  df['VAR2G'] = (((df['收盘']/MA(df['收盘'],60))*100)<74)
  df['VAR3G'] = (df['最高']>(df['最低']*1.051))
  # df['VAR4G'] = (df['VAR3G'] and COUNT(df['VAR3G'],5)>1)
  condition1=(COUNT(df['VAR3G'],5)>1)
  condition2=(df['VAR3G']==True)
  df.loc[condition1 &condition2, 'VAR4G'] = 1
  # df['AA'] = IF((df['VAR4G'] and (df['VAR1G'] or df['VAR2G'])),2,0)
  condition3=(df['VAR4G']==True)
  condition4=(df['VAR1G']==True)
  condition5=(df['VAR2G']==True)
  df.loc[condition4 |condition5, 'AApre'] = 1
  condition5=df['AApre']==True
  df.loc[condition3 &condition5, 'AApre2'] = 1
  df['AA'] = IF(df['AApre2'] ==1,2,0)
  df['BB'] = df['收盘']/REF(df['收盘'],25)<=1.1
  # df['CC'] = (SMA((MAX(df['收盘']-REF(df['收盘'],2),0),7),1)/SMA((ABS(df['收盘']-REF(df['收盘'],2)),7),1)*100)<15
  df['CC1'] = MAX(df['收盘']-REF(df['收盘'],2),0)
  df['CC1'].fillna(int(0),inplace=True)
  df['CC2'] = ABS(df['收盘']-REF(df['收盘'],2))
  df['CC'] = (SMA(df['CC1'],7,1)/SMA(df['CC2'],7,1)*100)<15
  df['V3'] = (df['收盘']-LLV(df['最低'],8))/(HHV(df['最高'],8)-LLV(df['最低'],8))*100
  df['操盘线'] = SMA(df['V3'],2,1)
  df['V5'] = SMA(df['操盘线'],2,1)
  # df['抄底'] = IF(df['操盘线']>REF(df['操盘线'],1) and REF(df['操盘线'],1)<REF(df['操盘线'],2) and df['操盘线']<23,1.5,0)
  condition抄底1=(df['操盘线']>REF(df['操盘线'],1))
  condition抄底2=(REF(df['操盘线'],1)<REF(df['操盘线'],2))
  condition抄底3=(df['操盘线']<23)
  df.loc[condition抄底1 &condition抄底2 &condition抄底3, '抄底1'] = 1
  df['抄底'] = IF(df['抄底1'] ==1,1.5,0)
  # df['买就赢利']=df['AA'] and df['BB'] and df['CC'] and df['抄底']
  condition买就赢利1=df['AA']==2
  condition买就赢利2=df['BB']==True
  condition买就赢利3=df['CC']==True
  condition买就赢利4=df['抄底']==1.5
  df.loc[condition买就赢利1 &condition买就赢利2 &condition买就赢利3 &condition买就赢利4, '买就赢利'] = 1
  df['X_23']=EMA(df['收盘'],2)
  df['X_24']=EMA(df['收盘'],5)
  df['X_25']=EMA(df['收盘'],13)
  df['X_26']=EMA(df['收盘'],30)
  conditionx27=(df['X_24']>=REF(df['X_24'],1))
  df.loc[conditionx27, 'X_27'] = 1
  df['X_28']=MAX(MAX(df['X_24'],df['X_25']),df['X_26'])
  df['X_29']=MIN(MIN(df['X_24'],df['X_25']),df['X_26'])
  conditionx301=(df['X_28']<df['收盘'])
  conditionx302=(df['开盘']<df['X_29'])
  conditionx303=(df['X_27']==1)
  conditionx304=(df['X_23']>REF(df['X_23'],1))
  df.loc[conditionx301 &conditionx302 &conditionx303& conditionx304, '满仓卖出'] = 1
  df['X_55']=LLV(df['最低'],36)
  df['X_56']=HHV(df['最高'],30)
  df['X_57']=EMA((df['收盘']-df['X_55'])/(df['X_56']-df['X_55'])*4,4)*25
  condition顶背离=(df['X_57']>90)
  df.loc[condition顶背离, '顶背离'] = 1

  return df

#动量
def dongliang(df):
  df['VAR1'] = df['最高']-df['开盘']+2*df['收盘']-df['最低']-REF(df['收盘'],1)
  df['VAR2'] = df['开盘']-df['最低']+df['最高']-2*df['收盘']+REF(df['收盘'],1)
  df['VAR3'] = df['成交量']*df['VAR1']/(df['VAR1']+df['VAR2'])
  df['VAR4'] = df['成交量']-df['VAR3']
  df['动量'] = EMA(df['VAR3']-df['VAR4'],5)*df['成交量']
   
  return df

#主升浪
def zhushenglang(df):
  # df['XA_1']=REF(df['最低'],1)
  # df['XA_2']=SMA(ABS(df['最低']-df['XA_1']),3,1)/SMA(MAX(df['最低']-df['XA_1'],0),3,1)*100
  # df['XA_3']=EMA(IF(df['收盘']*1.2,df['XA_2']*10,df['XA_2']/10),3)
  # df['XA_4']=LLV(df['最低'],50)
  # df['XA_5']=HHV(df['XA_3'],50)
  # df['XA_6']=IF(LLV(df['最低'],90),1,0)
  # df['主力建仓']=EMA(IF(df['最低']<=df['XA_4'],(df['XA_3']+df['XA_5']*2)/2,0),3)/618*df['XA_6']
  # df['VOLMA60'] = MA(df['成交量'],60)
  # df['VOLMA20'] = MA(df['成交量'],20)
  # df['BAR_LEN'] = ABS(REF(df['收盘'],1) - df['收盘'])
  # df['HLEN'] = HHV(df['BAR_LEN'],20)
  # df['USELEN'] = df['HLEN']*0.4
  # conditionS11=df['主力建仓']>1
  # conditionS12=df['成交量']>df['VOLMA60']
  # conditionS13=df['成交量']>df['VOLMA20']
  # conditionS14=df['BAR_LEN']>df['USELEN']
  # conditionS15=df['收盘']>df['开盘']
  # df.loc[conditionS11 &(conditionS12 |conditionS13 )&conditionS14 &conditionS15, 'S1'] = 1
  # df['GAP']=BARSLAST(df['S1']==1)
  # df['S1_OPEN']=VALUEWHEN(df['S1']==1,df['开盘'])
  # df['S1_BAR_LEN']=VALUEWHEN(df['S1'],df['BAR_LEN'])
  # df['S12_BETWEEN01']=HHV(df['BAR_LEN'],df['GAP']) 
  # conditionS12_BETWEEN1=df['S12_BETWEEN01']>=df['S1_BAR_LEN']
  # conditionS12_BETWEEN2=df['收盘']-REF(df['收盘'],1)<0
  # df['S12_BETWEEN']=0
  # df.loc[conditionS12_BETWEEN1 &conditionS12_BETWEEN2, 'S12_BETWEEN'] = 1
  # conditionS21=df['GAP']>1
  # conditionS22=df['GAP']<5
  # conditionS23=EXIST(df['GAP'],5)==True
  # conditionS24=EXIST(df['收盘']<df['开盘'],df['GAP'])==True
  # conditionS25=LLV(df['收盘'],df['GAP']) >= df['S1_OPEN']
  # conditionS27=df['BAR_LEN'] > df['USELEN']
  # conditionS28=df['收盘']>df['收盘'].shift(1)
  # conditionS29=df['S12_BETWEEN']==0
  # # df.loc[conditionS21 &conditionS22 &conditionS23 &conditionS25 &conditionS27 &(conditionS12 |conditionS13 )&conditionS28 &conditionS29, 'S2'] = 1
  # df.loc[conditionS21 &conditionS22 &conditionS23 &conditionS24 &conditionS25 &conditionS27 &(conditionS12 |conditionS13) &conditionS28&conditionS29 , 'S2'] = 1
  df['金柱']=0
  df['x1'] = (df['收盘']+df['最低']+df['最高'])/3
  df['x2'] = EMA(df['x1'],6)
  df['x3'] = EMA(df['x2'],5)
  # df['红柱数量']= SUM(df['x2']>df['x3'],10)
  condition1=(df['x2']>df['x3'])
  condition2=(REF(df['x3'],1)>REF(df['x2'],1))
  condition3=(df['x2']<=df['x3'])
  condition4=(df['x2']>=df['x3'])
  df.loc[condition1 &condition2 , '金柱'] = 1
  df.loc[condition4 , '红柱'] = 1
  df.loc[condition3 , '绿柱'] = 1

  return df

#SUPERTREND超级趋势
def SUPERTREND(df):
  df['TR1']=MAX(MAX((df['最高']-df['最低']),ABS(REF(df['收盘'],1)-df['最高'])),ABS(REF(df['收盘'],1)-df['最低']))
  df['UP']=(df['最高']+df['最低'])/2+MA(df['TR1'],10)*3
  df['DN']=(df['最高']+df['最低'])/2-MA(df['TR1'],10)*3
  df['BARSLASTUP']=BARSLAST(df['UP']<=REF(df['UP'],1))
  df['L1']=REF(df['UP'],df['BARSLASTUP'])
  df['L2']=LLV(df['UP'],15)
  df['LLpre']=IF(df['L1']==df['L2'],df['L1'],df['L2'])
  condition01=df['L2']!=REF(df['L2'],1)
  condition02=df['L1']<REF(df['L1'],1)
  df.loc[condition01 &condition02 ,  ['LLpre02']] = 1
  df['LL']=IF(df['LLpre02']==1,df['L1'],df['LLpre'])
  #S1
  df['S1corss']=0
  condition001=(df['UP']!=df['LL'])
  UPshifted = df['UP'].shift(1)
  LLshifted = df['LL'].shift(1)
  condition002=(UPshifted==LLshifted)
  df.loc[condition001&condition002,  ['S1corss']] = 1
  df['S1']=BARSLAST(df['S1corss'])+1
  #S2
  df['S2pre']=0
  conditionCROSSCLL01=(df['收盘']>df['LL'])
  conditionCROSSCLL02=(REF(df['收盘'],1)<REF(df['LL'],1))
  df.loc[conditionCROSSCLL01&conditionCROSSCLL02,  ['condition3']] = 1
  condition03=df['condition3']==1
  conditionCROSSCLL03=(df['收盘']>REF(df['LL'],2))
  conditionCROSSCLL04=(REF(df['收盘'],1)<REF(df['LL'],3))
  df.loc[conditionCROSSCLL03&conditionCROSSCLL04,  ['condition4']] = 1
  condition04=df['condition4']==1
  condition05=df['UP']>df['LL']
  df.loc[condition03|condition04 &condition05,  ['S2pre']] = 1
  df['S2count']=SUMplus(df['S2pre'],df['S1'])
  conditionS201=df['S2count']>0.5
  conditionS202=REF(df['S2count'],1)<0.5
  df['S2']=0
  df.loc[conditionS201&conditionS202,  ['S2']] = 1
  #A6
  df['A6']=BARSLAST(df['S2'])
  # B6:=BARSLAST(CROSS(HHV(DN,A6+1),C));
  df['HHVDN']=HHV(df['DN'],df['A6']+1)
  conditionHHVDN01=df['HHVDN']>df['收盘']
  conditionHHVDN02=REF(df['HHVDN'],1)<REF(df['收盘'],1)
  df['B6pre']=0
  df.loc[conditionHHVDN01&conditionHHVDN02,  ['B6pre']] = 1
  df['B6']=BARSLAST(df['B6pre'])
  conditionBY01=df['B6']>df['A6']
  conditionBY02=REF(df['B6'],1)<REF(df['A6'],1)
  df['BY']=0
  df.loc[conditionBY01&conditionBY02,  ['BY']] = 1
  #SL
  conditionSL01=df['A6']>df['B6']
  conditionSL02=REF(df['A6'],1)<REF(df['B6'],1)
  df['SL']=0
  df.loc[conditionSL01&conditionSL02,  ['SL']] = 1
  #趋势
  df['超级趋势']=IF(df['B6']>df['A6'],HHV(df['DN'],BARSLAST(df['BY'])+1),LLV(df['UP'],BARSLAST(df['SL'])+1))
  df.drop(['TR1','UP','DN','BARSLASTUP','L1','L2','LLpre','LLpre02','LL','S1corss','S1','S2pre','S2count','HHVDN','B6pre','B6','BY','SL'],axis=1,inplace=True)
  return df
