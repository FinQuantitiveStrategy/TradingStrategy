from MyTT import *
from 公式库 import *
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