#!/usr/bin/env python
#-*-coding:utf-8-*-


# sudo su
# export HADOOP_USER_NAME=hive
# pyspark --master yarn --deploy-mode client --queue default --archives hdfs:///lib/anaconda2-2.zip  --conf spark.pyspark.driver.python=/home/jiamao.sun/anaconda2/bin/python --conf spark.pyspark.python=./anaconda2-2.zip/anaconda2/bin/python

# sudo su
# export HADOOP_USER_NAME=hive
# pyspark --master yarn --deploy-mode client --queue default --archives hdfs:///workspace/ds-anaconda-hdfs-2.zip  --conf spark.pyspark.driver.python=/home/haitao.liu/ds-anaconda-hdfs-2/anaconda2/bin/python --conf spark.pyspark.python=./ds-anaconda-hdfs-2.zip/anaconda2/bin/python


# df.coalesce(1).write.format('csv').option('header','true').option("compression", "uncompressed").mode('overwrite').save('hdfs://'+'/upload/lht')

import re
import sys
import pandas as pd
import numpy as np
# from dateutil.relativedelta import relativedelta
import math
import datetime
# from collections import Counter
# import time

def days_between(dt1,dt2):
    if np.isnan(dt1) or np.isnan(dt2) or dt1 == 0 or dt2 == 0:
        return np.nan
    return (datetime.datetime.strptime(str(dt1),'%Y%m%d')-datetime.datetime.strptime(str(dt2),'%Y%m%d')).days

    
def days_ext(dt_input,days_f=2):
    '''
    正负两天日期计算
    :param dt_input: 
    :param days_f: 
    :return: 
    '''
    dt_new=datetime.datetime.strptime(str(dt_input), '%Y%m%d')
    return [int((dt_new+datetime.timedelta(days=i-2)).strftime('%Y%m%d')) for i in range(0,days_f*2+1)]

def days_ext_cnt(dt_input,dt_list):
    dt_ext_list = days_ext(dt_input)
    return len([x for x in dt_list if x in dt_ext_list])

def amt_ext(dt_input,df_input):
    dt_list_bx = [x for x in df_input['dt'] if x == dt_input]+[x for x in df_input['dt'] if x != dt_input & x in days_ext(dt_input)]
    if len(dt_list_bx)>0:
        amt_tgt = df_input.loc[df_input['dt']==dt_list_bx[0],'amt_sum'].iloc[0]  
    else:
        amt_tgt = None
    return amt_tgt

def mth_between(begin,end):
    # return rrule.rrule(rrule.MONTHLY,dtstart=datetime.datetime.strptime(begin, '%Y-%m'),until=end).count() - 1
    return (int(end // 100)-int(begin // 100))*12 + (int(end % 100)-int(begin % 100))

def monthmove(month,move):
    month=int(month)
    yr=month//100
    mth=month%100
    mth_add=mth+move
    yr_new=yr+mth_add//12
    mth_new=mth_add%12
    if mth_new==0:
        mth_new=12
        yr_new-=1
    return yr_new*100+mth_new


def month_add(dt_input,add_num):
    mth_input = monthmove(str(dt_input)[:6],add_num)
    if len(str(dt_input)) == 6:
        return mth_input
    else:
        return int(str(mth_input)+str(dt_input)[-2:])
  

# month_add(20230131,9)


def month_cont(month_list):
    '''
    预先判断此首笔是否包含连续的2个月,且前后不能空超过两个月,可判断的必须超过3期,返回TRUE
    :param month_series: 
    :return: 
    '''
    list_month = list(set(month_list))
    list_month.sort()
    new_first = list_month[0]
    all_month = 0
    for month_tmp in list_month[1:]:
        month_between = mth_between(new_first,month_tmp)
        if month_between > 2:
            break
        else:
            all_month += 1
            new_first = month_tmp
    return True if all_month >= 3 else False


def not_empty(s):
    if s is not None:
        return True 
    else:
        return False

def ft_amt_same(df):
    amt_same_mth = df\
        .assign(dt_lag = lambda df : df.dt.shift(-1).fillna(0).map(int))\
        .assign(days = lambda df : df.apply(lambda s:days_between(s['dt_lag'],s['dt']),axis = 1))\
        .assign(days_flag = lambda df : df.days.map(lambda x:1 if x>14 else None),resp_flag = lambda df : df.resp_cd4.map(lambda x:1 if x == '00' else None))                
    # 首次出现间隔大于14天 or 首次出现成功交易
    min_index = min(filter(not_empty,[amt_same_mth.days_flag.first_valid_index(),amt_same_mth.resp_flag.first_valid_index(),amt_same_mth.index[-1]]))
    amt_same_mth = amt_same_mth[amt_same_mth.index.map(lambda x : x <= min_index)]
    return min_index,amt_same_mth


def ft_amt_faxi(df):
    amt_faxi_mth = df\
        .assign(dt_lag = lambda df :df['dt'].shift(1).fillna(0).map(int))\
        .assign(days = lambda df :df.apply(lambda s:days_between(s['dt'],s['dt_lag']),axis = 1))\
        .assign(days_flag = lambda df :df.days.map(lambda x:1 if x > 7 else None))\
        .assign(resp_flag = lambda df :df.apply(lambda s:1 if (s['resp_cd4'] == '00') & s.faxi_flag else None,axis=1))
    min_index_faxi = min(filter(not_empty,[amt_faxi_mth.days_flag.first_valid_index(),amt_faxi_mth.resp_flag.first_valid_index(),amt_faxi_mth.index[-1]]))
    amt_faxi_mth = amt_faxi_mth[amt_faxi_mth.index.map(lambda x : x <= min_index_faxi)]
    return amt_faxi_mth

# 流水筛选
# 1、去掉198、199金额流水
# 2、去掉套路贷商户流水
# 金额筛选
# 1、去掉仅出现一次的金额
# 2、去掉首尾间隔<59天的金额

# cardno = '3ADCE2FAEE5E83BA175103FD58AEBC2E26969AE0A14E972824AD60633E2CF28F'
# input_mp = df.where(df['pri_acct_no_conv'] == cardno).toPandas()
# df.where(df['pri_acct_no_conv'] == cardno).coalesce(1).write.format('csv').option('header','true').option("compression", "uncompressed").mode('overwrite').save('hdfs://'+'/upload/lht')

def get_debj_bill(input_mp):
    # cardno = '336DE5CD316BFD3946C919EBEFB583BD1333407154BED23362F32AA31F65A7F4'
    # input_mp = df.where(df['pri_acct_no_conv'] == cardno).toPandas()
    # input_mp = pd.read_csv('input_mp.csv')
    # input_mp['resp_cd4']=input_mp['resp_cd4'].map(lambda x:str(x).rjust(2,'0'))
    input_mp = input_mp.sort_values(by = ['dt','latest_settle_tm']).reset_index()
    bill_no = 0  
    input_mp['bill_no'] = bill_no
    mchnt_temp = input_mp.groupby('card_accptr_nm_addr')['dt'].agg(['count','max','min'])
    mchnt_temp.columns = ['cnt','dt_max','dt_min']
    mchnt_temp['days'] = mchnt_temp.apply(lambda s:days_between(s['dt_max'],s['dt_min']),axis = 1)
    mchnt_temp = mchnt_temp.sort_values(by = 'cnt',ascending = False).query('(cnt > 1) & (days >= 60)')
    for mchnt in mchnt_temp.index:
        bill_flag = 0
        # amt  = amt_temp.index[1]
        # 临时表，剔除已打标的流水
        input_temp = input_mp[input_mp['bill_no'] == 0]
        # 相同金额的流水表
        mchnt_same = input_temp.query("card_accptr_nm_addr == '%s'"%mchnt).sort_values(by = ['dt','latest_settle_tm'])
        dt_last = mchnt_temp.loc[mchnt,'dt_max']
        new_ver = month_add(dt_last,1)
        for first_dt in mchnt_same['dt']:#循环查找第一笔
            if bill_flag:
                break
            if first_dt % 100 in [29,30,31]:
                continue           
            mth_list = mchnt_same['yearmonth'][mchnt_same['dt']>=first_dt]
            dt_list = mchnt_same['dt'][mchnt_same['dt']>=first_dt]
            if not month_cont(mth_list):
                continue
            cnt_months = mth_between(first_dt//100,new_ver//100)
            #超过两期没有,退出           
            yx_days_mth = [days_ext_cnt(month_add(first_dt,month),dt_list) for month in range(cnt_months)]
            if len([x for x in yx_days_mth if x==0]) > 2:
                continue
            #存在隔月金额相差较大的情况
            yx_amt_mth = [amt_ext(month_add(first_dt,month),mchnt_same) for month in range(cnt_months)]
            yx_amt_mth_t = [x for x in yx_amt_mth if x is not None]
            yx_amt_dif_mth = np.array(yx_amt_mth_t[1:]) - np.array(yx_amt_mth_t[:-1])
            if len([x for x in yx_amt_dif_mth if abs(x)>50])>0:
                continue
            record_index = pd.Index([], dtype='int64')
            for month in range(cnt_months):#从第一期开始计算
                dt_tmp = month_add(first_dt, month)
                dt_tmp_after = month_add(first_dt, month + 1)  
                # 优先选当天，然后前后两天
                amt_cur_mth = yx_amt_mth[month]
                if amt_cur_mth is None:
                    continue
                mchnt_same_mth = mchnt_same[['dt','resp_cd4','amt_sum','card_accptr_nm_addr']]\
                    .query('dt >= %d & dt < %d '%(dt_tmp,dt_tmp_after))
                if len(mchnt_same_mth) == 0:
                    continue
                min_index,mchnt_same_mth = ft_amt_same(mchnt_same_mth)
                dt_tgt = mchnt_same_mth['dt'][min_index]
                if len(mchnt_same_mth) == 0:
                    continue
                bill_flag += 1
                record_index = record_index.append(mchnt_same_mth.index)
                status_last = mchnt_same_mth['resp_cd4'][min_index]
                if status_last == '00':
                    continue
                # 相同金额扣款失败之后
                mchnt_faxi_mth = input_temp[['dt','resp_cd4','card_accptr_nm_addr','amt_sum','latest_settle_tm']]\
                    .query('dt > %d & dt < %d'%(dt_tgt,dt_tmp_after))\
                    .sort_values(by = ['dt','latest_settle_tm'])\
                    .assign(faxi_tmp = lambda df :df.apply(lambda s:(s['amt_sum']-amt_cur_mth)/days_between(s['dt'],dt_tmp),axis = 1))\
                    .assign(faxi_flag = lambda df :(df.faxi_tmp > 0) & (df.faxi_tmp < amt_cur_mth * 0.1) & (df.card_accptr_nm_addr.map(lambda x:x in name_list)))\
                    .query('faxi_flag == True')
                if len(mchnt_faxi_mth) == 0:
                    continue
                mchnt_faxi_mth = ft_amt_faxi(mchnt_faxi_mth)
                if len(mchnt_faxi_mth)>0:
                    record_index = record_index.append(mchnt_faxi_mth.index)
            if bill_flag > 2:
                bill_no += 1
                input_mp.loc[record_index,'bill_no'] = bill_no
                continue
            elif bill_flag > 0:
                input_mp.loc[record_index, 'bill_no'] = -1
    return input_mp.query('bill_no > 0')


def get_debx_bill(input_mp):
    # cardno = '336DE5CD316BFD3946C919EBEFB583BD1333407154BED23362F32AA31F65A7F4'
    # input_mp = df.where(df['pri_acct_no_conv'] == cardno).toPandas()
    input_mp = input_mp.sort_values(by = ['dt','latest_settle_tm']).reset_index()
    bill_no = 0  
    input_mp['bill_no'] = bill_no
    amt_temp = input_mp.groupby('amt_sum')['dt'].agg(['count','max','min'])
    amt_temp.columns = ['cnt','dt_max','dt_min']
    amt_temp['days'] = amt_temp.apply(lambda s:days_between(s['dt_max'],s['dt_min']),axis = 1)
    amt_temp = amt_temp.sort_values(by = 'cnt',ascending = False).query('(cnt > 1) & (days >= 60)')
    for amt in amt_temp.index:
        bill_flag = 0
        # amt  = amt_temp.index[1]
        # 临时表，剔除已打标的流水
        input_temp = input_mp[input_mp['bill_no'] == 0]
        # 相同金额的流水表
        amt_same = input_temp.query('amt_sum == %d'%amt).sort_values(by = ['dt','latest_settle_tm'])
        dt_last = amt_temp.loc[amt,'dt_max']
        new_ver = month_add(dt_last,1)

        for first_dt in amt_same['dt']:#循环查找第一笔
            # 每个amt最多一个bill,对应一个first_dt
            if bill_flag:
                break
            if first_dt % 100 in [29,30,31]:
                continue           
            mth_list = amt_same['yearmonth'][amt_same['dt']>=first_dt]
            dt_list = amt_same['dt'][amt_same['dt']>=first_dt]
            if not month_cont(mth_list):
                continue
            #超过两期没有,退出           
            yx_days_mth = [days_ext_cnt(month_add(first_dt,month),dt_list) for month in range(mth_between(first_dt//100,new_ver//100))]
            if len([x for x in yx_days_mth if x==0]) > 2:
                continue
            record_index = pd.Index([], dtype='int64')
            for month in range(mth_between(first_dt//100,new_ver//100)):#从第一期开始计算
                dt_tmp = month_add(first_dt, month)
                dt_tmp_after = month_add(first_dt, month + 1)                 
                amt_same_mth = amt_same[['dt','resp_cd4','card_accptr_nm_addr']]\
                    .query('dt >= %d & dt < %d'%(dt_tmp,dt_tmp_after))
                if len(amt_same_mth) == 0:
                    continue
                min_index,amt_same_mth = ft_amt_same(amt_same_mth)
                dt_tgt = amt_same_mth['dt'][min_index]
                if len(amt_same_mth) == 0:
                    continue
                bill_flag += 1
                record_index = record_index.append(amt_same_mth.index)
                name_list = set(amt_same_mth['card_accptr_nm_addr'])
                status_last = amt_same_mth['resp_cd4'][min_index]
                if status_last == '00':
                    continue
                # 相同金额扣款失败之后
                amt_faxi_mth = input_temp[['dt','resp_cd4','card_accptr_nm_addr','amt_sum','latest_settle_tm']]\
                    .query('dt > %d & dt < %d'%(dt_tgt,dt_tmp_after))\
                    .sort_values(by = ['dt','latest_settle_tm'])\
                    .assign(faxi_tmp = lambda df :df.apply(lambda s:(s['amt_sum']-amt)/days_between(s['dt'],dt_tmp),axis = 1))\
                    .assign(faxi_flag = lambda df :(df.faxi_tmp > 0) & (df.faxi_tmp < amt * 0.1) & (df.card_accptr_nm_addr.map(lambda x:x in name_list)))\
                    .query('faxi_flag == True')
                if len(amt_faxi_mth) == 0:
                    continue                
                amt_faxi_mth = ft_amt_faxi(amt_faxi_mth)
                if len(amt_faxi_mth)>0:
                    record_index = record_index.append(amt_faxi_mth.index)
            if bill_flag > 2:
                bill_no += 1
                input_mp.loc[record_index,'bill_no'] = bill_no
                continue
            elif bill_flag > 0:
                input_mp.loc[record_index,'bill_no'] = -1
    return input_mp.query('bill_no > 0')


input_mp = pd.read_csv('df_temp2.csv',encoding='gb2312')
input_mp['resp_cd4']=input_mp['resp_cd4'].map(lambda x:str(x).rjust(2,'0'))
input_final= get_debx_bill(input_mp)
input_final.to_csv('input_final3.csv')

df = spark.sql(
    '''
    SELECT 
      card_accptr_nm_addr
    , pri_acct_no_conv
    , resp_cd4
    , trans_id
    , amt_sum/100 as amt_sum
    , cnt_sum
    , amt_max/100 as amt_max
    , amt_min/100 as amt_min
    , latest_settle_tm
    , yearmonth
    , dt
    , dc_flag
    , brand 
    FROM yuanfei.crdt_payment_trans_temp
    where amt_sum <> 19800 and amt_sum <> 19900
    and card_accptr_nm_addr not regexp '信用卡|卡中心|掌上生活'
''')


spark.sql(
    '''
    create table if not exists lht.daily_2022
    SELECT 
      card_accptr_nm_addr
    , pri_acct_no_conv
    , resp_cd4
    , trans_id
    , amt_sum/100 as amt_sum
    , cnt_sum
    , amt_max/100 as amt_max
    , amt_min/100 as amt_min
    , latest_settle_tm
    , yearmonth
    , dt
    , dc_flag
    FROM ds_etl_prod.daily_summary
    where yearmonth between 202201 and 202212
    and trans_id regexp 'S22|S56|S23|W20'
    and amt_sum <> 19800 and amt_sum <> 19900
    and card_accptr_nm_addr not regexp '信用卡|卡中心|掌上生活'
    union all
    SELECT 
      card_accptr_nm_addr
    , pri_acct_no_conv
    , resp_cd4
    , trans_id
    , amt_sum/100 as amt_sum
    , cnt_sum
    , amt_max/100 as amt_max
    , amt_min/100 as amt_min
    , latest_settle_tm
    , yearmonth
    , dt
    , dc_flag
    FROM ds_etl_prod.daily_summary_filter
    where yearmonth between 202201 and 202212
    and amt_sum <> 19800 and amt_sum <> 19900
    and card_accptr_nm_addr not regexp '信用卡|卡中心|掌上生活'
''')

spark.sql(
    '''
    create table if not exists lht.card_2022
    select pri_acct_no_conv from lht.daily_2022
    group by pri_acct_no_conv
    '''
)

df = spark.read.table('lht.daily_2022')
df_card = spark.read.table('lht.card_2022')
# df_card = df.select('pri_acct_no_conv').distinct()
df_card_smp = df_card.sample(True, 0.01, 42)



n = 0
for r in df_card_smp.collect():
    cardno = r['pri_acct_no_conv']
    input_mp = df.where(df['pri_acct_no_conv'] == cardno).toPandas()
    if n == 0:
        input_final = get_debx_bill(input_mp)
    else:
        input_final = input_final.append(get_debx_bill(input_mp))
    n += 1


output_final = spark.createDataFrame(input_final)
output_final.write.saveAsTable('lht.output_final')



n = 0
for r in df_card.collect():
    cardno = r['pri_acct_no_conv']
    input_mp = df.where(df['pri_acct_no_conv'] == cardno).toPandas()   
    if n == 0:
        input_final = get_debx_bill(input_mp)
        output_final = spark.createDataFrame(input_final)
        output_final.write.saveAsTable('lht.output_final',mode = "overwrite")
    else:
        input_final = input_final.append(get_debx_bill(input_mp))
        temp_len = len(input_final)    
        if temp_len > 500000:
            output_final = spark.createDataFrame(input_final)
            output_final.write.saveAsTable('lht.output_final',mode = "append")
            input_final = input_final.loc[[],:]
    n += 1

output_final = spark.createDataFrame(input_final)
output_final.write.saveAsTable('lht.output_final',mode = "append")    
    

def output(cardno):
    input_mp = df.where(df['pri_acct_no_conv'] == cardno).toPandas()
    if len(input_mp) > 0:
        input_final = get_debx_bill(input_mp)
        if len(input_final) > 0 :
            output_final = spark.createDataFrame(input_final)
            output_final.write.saveAsTable('lht.output_final',mode = 'append')
        return len(input_final)

import pyspark.sql.functions as F 
from pyspark.sql.types import *
udf_output = F.udf(output, IntegerType())
df_card.withColumn('cnt_result',udf_output('pri_acct_no_conv'))


from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# 创建 SparkContext 对象，表示与 Spark 集群的连接
# sc = SparkContext(appName="PySparkDStreamExample")
# 创建 StreamingContext 对象，每 0.001 钟处理一次数据，这个时间间隔也称为 batch interval
ssc = StreamingContext(sc, 0.001)
dstream = ssc.queueStream(df_card)





