#!/usr/bin/env python
#-*-coding:utf-8-*-
"""
# -*- coding: utf-8 -*-
# trans_red_r2.py  by  ykx  Time    : 19-7-11 下午15:30  
# 此版本为map版本
cat data/ceshi_20190625.txt |python trans_map_r2.py |sort |python trans_red_r2.py >result4
"""

import sys
# import pandas as pd
# from dateutil.relativedelta import relativedelta
import math
import datetime
# from collections import Counter

# import time
'''
1、先对同一天账单进行合并
2、对同月账单进行合并
3、不同月份账单进行合并
4、剩余输出为非分期账单
'''
version=sys.argv[1]
# version='2019-07'

# f=open('/home/upsmart/files/Pro_py3/loan_trans/test_0724')
# # input_mp={}
# # tag=0
# for line in f.readlines():
#     items = line.strip("\n").split("\t")
#     card = items[0].split('#')[0]
#     # if card!='0522271BEDE518EB16CB80EECD79EEE14B924213FDA0EC77144B2D82780B4B18':
#     #     continue
#     time = datetime.datetime.strptime(items[0].split('#')[1][:19], '%Y-%m-%d %H:%M:%S')
#     dt = items[0].split('#')[1][:10]
#     month = dt[:7]
#
#     info = items[1].split(',')
#     name = info[0]
#     amt = float(info[1])
#     if float(amt) <= 0:
#         continue
#     status = '0' if info[2] in ['00', 'AA', '', '10', '11', '16', 'A2', 'A4', 'A5', 'A6', 'Y1', 'Y3'] else '1'
#
#
#     input_mp = init_all(input_mp,tag=1)


def date_rule(begin,end):
    # return rrule.rrule(rrule.MONTHLY,dtstart=datetime.datetime.strptime(begin, '%Y-%m'),until=end).count() - 1
    return (int(end[:4])-int(begin[:4]))*12 + (int(end[5:7])-int(begin[5:7]))


def number_format(num,fn=1):
    # num_split=str(num).split('.')
    # if len(num_split)==2:
    #     return float('.'.join([num_split[0],num_split[1][:fn]]))
    # else:
    #     return num
    return round(num,fn)


def month_add(dt_input,add_num):
    tmp_month=add_num+int(dt_input[5:7])
    month_mode = tmp_month%12
    month = '12' if month_mode == 0 else str(month_mode)
    month = '0' + month if len(month) == 1 else month
    year = int(dt_input[:4])+math.floor(tmp_month/ 12) if month_mode!=0 else int(dt_input[:4])+math.floor(tmp_month/ 12)-1
    return str(int(year))+'-'+month+'-'+dt_input[-2:] if len(dt_input)==10 else str(int(year))+'-'+month

# for year in range(2018,2021):
#     for month in range(1,13):
#         if len(str(month))==1:
#             month='0'+str(month)
#         for add in range(20):
#             print(str(year)+'-'+str(month),add,month_add(str(year)+'-'+str(month),add))


def days_between(time1,time2):
    '''
    输入格式为dt='2018-09-10'
    :param time1: 较大日期
    :param time2: 较小日期
    :return: 
    '''
    return (datetime.datetime.strptime(time1,'%Y-%m-%d')-datetime.datetime.strptime(time2,'%Y-%m-%d')).days


def days_add(dt_input,days_f=2):
    '''
    正负两天日期计算
    :param dt_input: 
    :param days_f: 
    :return: 
    '''
    dt_new=datetime.datetime.strptime(dt_input, '%Y-%m-%d')
    return [(dt_new+datetime.timedelta(days=i-2)).strftime('%Y-%m-%d') for i in list(range(0,days_f*2+1)) if i!=days_f]


def month_conti_func(list_month):
    '''
    预先判断此首笔是否包含连续的2个月,且前后不能空超过两个月,可判断的必须超过3期,返回TRUE
    :param month_series: 
    :return: 
    '''
    map_all={}
    for first_index,first_key in enumerate(list_month[:-2]):
        all_month = 1
        map_all[first_key]=all_month
        new_first=first_key
        for month_tmp in list_month[first_index+1:]:
            month_between=date_rule(new_first,month_tmp)
            if month_between>2:
                break
            else:
                all_month+=1
                new_first=month_tmp
        if all_month<3:
            map_all.pop(first_key)
        else:
            map_all[first_key]=all_month
    return map_all


def deng_e_b_x(input_mp,id_list_local,record_list,all_bill_num=0):
    '''
    等额本息,每期还款金额相等,时间固定
    :param data_input: 输入全量数据
    :param all_bill_num: 当前账单起始号
    :return: 账单字典
    '''
    stats_tp={i:input_mp['amt'].count(i) for i in set(input_mp['amt']) if input_mp['amt'].count(i)>=2}
    amt_list=[i[0] for i in sorted(stats_tp.items(),key=lambda x:x[1],reverse=True)]
    # amt_list=list(stats[stats >= 2].index)
    bill_map_debx={}
    for amt in amt_list:
        mp_tp=[(i,j,k,l,m,n) for i,j,k,l,m,n in zip(id_list_local,input_mp['amt'],input_mp['dt'],input_mp['month'],input_mp['status'],input_mp['name']) if i not in record_list and j==amt]
        if len(mp_tp)<2:
            continue
        id_index=0
        amt_index=1
        dt_index=2
        month_index=3
        status_index=4
        name_index=5
        panduan_last=mp_tp[-1][dt_index]
        panduan_first=mp_tp[0][dt_index]
        if days_between(panduan_last,panduan_first)<59:#最大减最小少于3个月,肯定不是
            continue
        new_version=month_add(panduan_last[:7],1)
        dt_list=[i[dt_index] for i in mp_tp]
        month_list=[i[month_index] for i in mp_tp]
        id_list=[i[id_index] for i in mp_tp]
        status_list=[i[status_index] for i in mp_tp]
        name_list=[i[name_index] for i in mp_tp]

        list_month=list(set(month_list))
        list_month.sort()
        map_all_month=month_conti_func(list_month)
        for first_dt,first_dt_id in zip(dt_list,id_list):#循环查找第一笔
            if first_dt[:7] not in map_all_month:
                continue
            if days_between(panduan_last, first_dt) < 59:
                continue
            if first_dt[-2:] in ['29','30','31']:
                continue
            if first_dt_id in record_list:
                continue
            all_bill_num += 1
            bill_num_now = 0
            bill_map_debx[all_bill_num] = {}
            record_list_tmp=[]
            month_chmod=1
            # print(record_list,record_list_tmp,bill_map_debx)
            for month in range(date_rule(first_dt,new_version)):#从第一期开始计算
                dt_tmp=month_add(first_dt,month)
                dt_tmp_after = month_add(first_dt, month + 1)
                bill_num_now += 1
                '''
                正负两天的日期
                '''
                dt_extend_list=days_add(dt_tmp)
                id_extend_list=[j for i,j in zip(dt_list,id_list) if i in dt_extend_list]
                new_extend_id=[i for i in id_extend_list if i not in record_list+record_list_tmp]#有可能有-days包含在上月的展期里

                if month_chmod>2:#超过两期没有,退出
                    if len(bill_map_debx[all_bill_num]) <= 2:
                        bill_map_debx.pop(all_bill_num)
                    break

                beixuan_list=[(i,j) for i,j in zip(dt_list,id_list) if j not in record_list and i==dt_tmp]
                # print(amt,first_dt,first_dt_id,month,beixuan_list,dt_tmp,new_extend_id)
                if len(beixuan_list)!=0 or len(new_extend_id)>0:
                    if len(beixuan_list)!=0:  # 固定日期扣款
                        bill_id = beixuan_list[0][1]#存疑,是否应该考虑多个对应值
                        dt_tmp = beixuan_list[0][0]
                    elif len(new_extend_id)>0:#非固定时间扣款
                        bill_id = new_extend_id[0]
                        dt_tmp = dt_list[id_list.index(bill_id)]

                    bill_map_debx[all_bill_num][bill_num_now] = [bill_id]
                    record_list_tmp += [bill_id]

                    '''
                    先判断14天内是否有相同金额
                    '''
                    same_amt_data = [(j,i,status_list[id_list.index(j)],name_list[id_list.index(j)]) for i,j in zip(dt_list,id_list) if i >= dt_tmp and i<dt_tmp_after ]
                    same_list, same_name_list = same_data_func(same_amt_data, dt_tmp)
                    same_list = [i for i in same_list if i not in record_list+record_list_tmp]
                    bill_map_debx[all_bill_num][bill_num_now] += same_list
                    record_list_tmp += same_list
                    '''
                    罚息判断
                    '''
                    start_index = bill_map_debx[all_bill_num][bill_num_now][-1]
                    # dt_end = dt_tmp_after
                    data_fenqi_tmp = [(i,j,k,l,m) for i,j,k,l,m
                                      in zip(id_list_local,input_mp['dt'],input_mp['status'],input_mp['name'],input_mp['amt'])
                                      if dt_tmp<=j<dt_tmp_after and i>=start_index and i not in record_list]
                    faxi_list = faxi_panduan(data_fenqi_tmp, dt_tmp,same_name_list)
                    bill_map_debx[all_bill_num][bill_num_now] += faxi_list
                    record_list_tmp += faxi_list
                    month_chmod = 1
                else:
                    month_chmod += 1


            if all_bill_num in bill_map_debx and len(bill_map_debx[all_bill_num])<=2:
                bill_map_debx.pop(all_bill_num)
            elif all_bill_num in bill_map_debx and len(bill_map_debx[all_bill_num])>=2:
                record_list+=[i for i in record_list_tmp if i not in record_list]


    return bill_map_debx,record_list,all_bill_num


def faxi_panduan(data_input,first_input_dt,name_list=set()):
    '''
    #默认输入的第一笔就是最近一次正常金额扣款(排除相同金额连续扣款期),查找相同商户名称下的第一笔罚息
    #(id,dt,status,name,amt)
    :param data_input: 
    :param first_input_dt: 
    :param name_list: 
    :return: 
    '''
    # index_list=data_input.index.tolist()

    if len(data_input)==0:
        return []
    else:
        # first_index=data_input[0][0]
        first_dt = data_input[0][1]
        first_status = data_input[0][2]
        first_amt=data_input[0][4]
        if first_status != '0':
            faxi=''
            last_dt=first_dt
            faxi_list=[]
            for i in data_input[1:]:
                id_i=i[0]
                dt_i=i[1]
                status_i = i[2]
                name_i = i[3]
                amt_i=i[4]
                if dt_i==first_dt :
                    continue
                faxi_tmp=number_format((amt_i-first_amt)/days_between(dt_i,first_input_dt))
                last_days=days_between(dt_i,last_dt)
                # print(id_i)
                if last_days>7:
                    break
                if faxi=='' and name_i in name_list and 0 < faxi_tmp < first_amt * 0.1:
                    faxi=faxi_tmp
                    faxi_list.append(id_i)
                    # print('hew1')
                    last_dt = dt_i
                    if status_i == '0':
                        break
                    continue
                if faxi!='' and faxi_tmp==faxi:
                    faxi_list.append(id_i)
                    last_dt=dt_i
                    # print('dfwe2',id_i,dt_i,status_i)
                    if status_i == '0':
                        break
            return faxi_list
        else:
            return []

def same_data_func(datainput,first_same_dt,days_f=14):
    '''
    #当月相同金额扣款,限制每笔最大间隔为14天
    #(id,dt,status,name)
    :param datainput: 
    :param first_same_dt: 
    :param first_index: 
    :param days_f: 
    :return: 
    '''
    same_result_list=[]
    name_list=set()
    name_list.add(datainput[0][3])
    same_id_list = [i[0] for i in datainput]
    if len(same_id_list) != 1 and datainput[0][2]!='0':  # 仅包含当前条
        last_same_dt = first_same_dt
        for same_index in datainput[1:]:
            same_tmp_id = same_index[0]
            same_tmp_dt = same_index[1]
            same_tmp_status = same_index[2]
            same_tmp_name = same_index[3]

            if days_between(same_tmp_dt, last_same_dt) <= days_f:
                same_result_list += [same_tmp_id]
                name_list.add(same_tmp_name)
                last_same_dt = same_tmp_dt
            else:
                break
            if same_tmp_status == '0':
                break
    return same_result_list,name_list


def deng_e_b_j(input_mp,id_list_local,record_list,all_bill_num):
    '''
    等额本金分析,商户名称相同,扣款日期固定,金额减少在50元以内
    :param data_input: 
    :param all_bill_num: 
    :param record_id: 
    :return: 
    '''
    stats_tp = {i: input_mp['name'].count(i) for i in set(input_mp['name']) if input_mp['name'].count(i) >= 2}
    name_list = [i[0] for i in sorted(stats_tp.items(), key=lambda x: x[1], reverse=True)]
    bill_map_debx = {}
    for name in name_list:
        mp_tp=[(i,j,k,l,m,n) for i,j,k,l,m,n in zip(id_list_local,input_mp['amt'],input_mp['dt'],input_mp['month'],input_mp['status'],input_mp['name']) if i not in record_list and n==name]
        if len(mp_tp)<2:
            continue
        id_index = 0
        amt_index = 1
        dt_index = 2
        month_index = 3
        status_index = 4
        name_index = 5
        panduan_last = mp_tp[-1][dt_index]
        panduan_first = mp_tp[0][dt_index]
        if days_between(panduan_last, panduan_first) < 59:  # 最大减最小少于3个月,肯定不是
            continue
        new_version = month_add(panduan_last[:7], 1)

        dt_list = [i[dt_index] for i in mp_tp]
        amt_list = [i[amt_index] for i in mp_tp]
        month_list = [i[month_index] for i in mp_tp]
        id_list = [i[id_index] for i in mp_tp]
        status_list = [i[status_index] for i in mp_tp]
        name_list = [i[name_index] for i in mp_tp]

        list_month = list(set(month_list))
        list_month.sort()
        map_all_month = month_conti_func(list_month)
        for first_dt,first_dt_id in zip(dt_list,id_list):
            if first_dt[:7] not in map_all_month:
                continue
            if days_between(panduan_last, first_dt) < 59:
                continue
            if first_dt[-2:] in ['29','30','31']:
                continue
            if first_dt_id in record_list:
                continue
            all_bill_num += 1
            bill_map_debx[all_bill_num] = {}
            bill_num_now = 0
            last_amt = 0
            month_chmod = 1
            record_list_tmp = []
            for month in range(date_rule(first_dt, new_version)):
                # if last_amt!=0:
                #     tmp_dt_list = [i for i,j,k in zip(dt_list,id_list,amt_list)
                #                    if i==month_add(first_dt, month) and j not in record_list and 0<=last_amt-k<=50]
                # else:
                #     tmp_dt_list = [i for i, j, k in zip(dt_list, id_list, amt_list)
                #                    if i == month_add(first_dt, month) and j not in record_list ]
                # if len(tmp_dt_list)==0:
                #     month_chmod += 1
                #     continue
                dt_tmp=month_add(first_dt, month)#存疑,是否应该考虑多个对应值
                dt_tmp_after = month_add(first_dt, month + 1)
                bill_num_now += 1
                '''
                正负两天的日期
                '''
                dt_extend_list = days_add(dt_tmp)
                id_extend_list = [j for i, j in zip(dt_list, id_list) if i in dt_extend_list]
                new_extend_id = [i for i in id_extend_list if i not in record_list+record_list_tmp]

                if month_chmod>2:
                    if len(bill_map_debx[all_bill_num]) <= 2:
                        bill_map_debx.pop(all_bill_num)
                    break

                beixuan_list = [(i, j) for i, j in zip(dt_list, id_list) if j not in record_list and i == dt_tmp]
                if len(beixuan_list) != 0 or len(new_extend_id) > 0:
                    if len(beixuan_list) != 0:  # 固定日期扣款
                        bill_id = beixuan_list[0][1]  # 存疑,是否应该考虑多个对应值
                    elif len(new_extend_id)>0:#非固定时间扣款
                        bill_id = new_extend_id[0]#存疑,是否应该考虑多个对应值

                    amt = amt_list[id_list.index(bill_id)]
                    dt_tmp = dt_list[id_list.index(bill_id)]

                    if last_amt !=0 and 0<=last_amt-amt<=50 :#与上期还款相差小于50
                        record_list_tmp += [bill_id]
                        bill_map_debx[all_bill_num][bill_num_now] = [bill_id]
                    elif last_amt !=0:#与上期还款相差超过50
                        if len(bill_map_debx[all_bill_num]) <= 2:
                            bill_map_debx.pop(all_bill_num)
                        break
                    else:#首期
                        record_list_tmp += [bill_id]
                        bill_map_debx[all_bill_num][bill_num_now] = [bill_id]
                    '''
                    先判断14天内是否有相同金额
                    '''
                    same_amt_data = [(j, i, status_list[id_list.index(j)], name_list[id_list.index(j)]) for i, j,k in
                                     zip(dt_list, id_list,amt_list) if i >= dt_tmp and i < dt_tmp_after and  k==amt]
                    same_list, same_name_list = same_data_func(same_amt_data, dt_tmp)
                    same_list = [i for i in same_list if i not in record_list + record_list_tmp]
                    bill_map_debx[all_bill_num][bill_num_now] += same_list
                    record_list_tmp += same_list
                    '''
                    罚息判断
                    '''
                    start_index = bill_map_debx[all_bill_num][bill_num_now][-1]
                    # dt_end = dt_tmp_after
                    data_fenqi_tmp = [(i, j, k, l, m) for i, j, k, l, m in
                                      zip(id_list_local, input_mp['dt'], input_mp['status'], input_mp['name'],input_mp['amt'])
                                      if dt_tmp <= j < dt_tmp_after and i >= start_index and i not in record_list]
                    faxi_list = faxi_panduan(data_fenqi_tmp, dt_tmp, same_name_list)
                    bill_map_debx[all_bill_num][bill_num_now] += faxi_list
                    record_list_tmp += faxi_list

                    last_amt=amt
                    month_chmod = 1
                else:
                    month_chmod += 1

            if all_bill_num in bill_map_debx and len(bill_map_debx[all_bill_num])<=2:
                bill_map_debx.pop(all_bill_num)
            elif all_bill_num in bill_map_debx and len(bill_map_debx[all_bill_num]) >= 2:
                record_list+=[i for i in record_list_tmp if i not in record_list]


    return bill_map_debx,record_list,all_bill_num


def normal_faxi(input_mp,id_list_local,record_list,all_bill_num):
    '''
    不分期罚息,先查找相同还款金额交易(因为有些本月相同金额,但是没有达到3期的等额本息的要求,以及对当天的金额相同的合并),再查找罚息交易
    :param data_input:
    :param all_bill_num:
    :param record_id_list:
    :return:
    '''
    mp_tp = [(i, j, k, l, m, n) for i, j, k, l, m, n in
             zip(id_list_local, input_mp['amt'], input_mp['dt'], input_mp['month'], input_mp['status'], input_mp['name'])
             if i not in record_list]# and j>=500.00为了保证月增的数据一致性，尝试都取200以上#20191212
    faxi_map={}
    for data_i in mp_tp:
        # import time
        # start_time = time.time()
        bill_num_now = 1
        id_i=data_i[0]
        status_i=data_i[4]

        if id_i in record_list :
            continue

        if status_i=='0':
            all_bill_num += 1
            faxi_map[all_bill_num] = {}
            faxi_map[all_bill_num][bill_num_now] = [id_i]
            record_list += [id_i]
            continue
        amt_i=data_i[1]
        dt_i = data_i[2]
        all_bill_num+=1
        faxi_map[all_bill_num]={}
        faxi_map[all_bill_num][bill_num_now] = [id_i]

        # end_time = time.time()
        # sys.stderr.write("new1耗时%s秒\n" % ((end_time - start_time)))

        # 先判断14天内是否有相同金额
        same_amt_data = [(j, i, input_mp['status'][id_list_local.index(j)], input_mp['name'][id_list_local.index(j)])
                         for i,j,k in zip(input_mp['dt'], id_list_local,input_mp['amt'])
                         if i >= dt_i and j not in record_list and k==amt_i]
        same_list, same_name_list = same_data_func(same_amt_data, dt_i)
        same_list = [i for i in same_list if i not in record_list]
        faxi_map[all_bill_num][bill_num_now] += same_list

        # end_time = time.time()
        # sys.stderr.write("new2耗时%s秒\n" % ((end_time - start_time)))
        '''
        罚息判断
        '''
        data_fenqi_tmp = [(j, i, input_mp['status'][id_list_local.index(j)], input_mp['name'][id_list_local.index(j)],k)
                         for i,j,k in zip(input_mp['dt'], id_list_local,input_mp['amt'])
                         if i >= dt_i and j not in record_list and j>=faxi_map[all_bill_num][bill_num_now][-1]]
        # print(data_fenqi_tmp)
        faxi_list = faxi_panduan(data_fenqi_tmp, dt_i, same_name_list)
        faxi_map[all_bill_num][bill_num_now] += faxi_list
        record_list += [id_i]
        record_list += same_list
        record_list += faxi_list

        # end_time = time.time()
        # sys.stderr.write("new3耗时%s秒\n" % ((end_time - start_time)))

    keys=list(faxi_map.keys())
    for i in keys:
        keys_i=list(faxi_map[i].keys())
        for j in keys_i:
            if len(faxi_map[i][j])==0:
                faxi_map[i].pop(j)
        ### if len(faxi_map[i][1])==1:
        ###     record_list=[k for k in record_list if k not in faxi_map[i][1]]
        ###     faxi_map.pop(i)

    return faxi_map,record_list,all_bill_num


def normal_func(input_mp,id_list_local,record_list,all_bill_num):
    '''
    只针对金额大于500的数据进行单笔输出,皆属于非分期账单,暂时未用到
    :param data_input: 
    :param all_bill_num: 
    :param record_id: 
    :return: 
    '''
    mp_tp = [(i, j, k, l, m, n) for i, j, k, l, m, n in
             zip(id_list_local, input_mp['amt'], input_mp['dt'], input_mp['month'], input_mp['status'],input_mp['name'])
             if i not in record_list]#and j >= 500.00为了保证月增的数据一致性，尝试都取200以上#20191212

    # data_500=data_input[data_input['amt']>=500.00]
    nor_map = [i[0] for i in mp_tp]
    record_list=record_list+nor_map

    stats_tp = {i: input_mp['amt'].count(i) for i in set(input_mp['amt']) if input_mp['amt'].count(i) >= 2}
    amt_list = [i[0] for i in sorted(stats_tp.items(), key=lambda x: x[1], reverse=True)]

    # amt_stats=data_500['amt'].value_counts()
    # amt_list=list(amt_stats[amt_stats>=2].index)
    combine_map={}

    for tmp_amt in amt_list:
        # last_tag=''
        all_bill_num += 1
        data_tmp=[(i, j, k, l, m, n) for i, j, k, l, m, n in mp_tp if i not in record_list and j==tmp_amt]
        for tmp_data in data_tmp:

            id_tmp=tmp_data[0]
            combine_map[all_bill_num] = [id_tmp]
            dt_tmp=tmp_data[2]
            same_list, same_name_list =same_data_func(data_tmp, dt_tmp)
            combine_map[all_bill_num] += same_list

    keys=list(combine_map.keys())
    for i in keys:
        if len(combine_map[i])==1:
            combine_map.pop(i)
        else:
            for j in combine_map[i]:
                nor_map.pop(nor_map.index(j))

    return nor_map,combine_map,record_list,all_bill_num


def data_filter_byIndex(data_input,id_filter_list):
    if data_input.shape[0]==0:
        return data_input
    else:
        return data_input[data_input['id'].map(lambda x:False if x in id_filter_list else True)]

def data_filter_byDt(data_input,start_index,dt_end):
    if data_input.shape[0]==0:
        return data_input
    else:
        return data_input[data_input['dt'].map(lambda x: x < dt_end)].ix[start_index:,]


def bill_func(input_mp):
    # record_list.__len__()
    # set(record_list).__len__()

    # import time
    # start_time = time.time()
    id_list_local=list(range(len(input_mp['amt'])))


    #198\199金额特别标注
    renzhen_tp=[(i,j) for i,j in zip(id_list_local,input_mp['amt']) if j in [198,199]]
    renzheng_list=[i[0] for i in renzhen_tp]
    record_list=[i[0] for i in renzhen_tp]
    # end_time = time.time()
    # sys.stderr.write("%s\n0耗时%s秒\n" % (last_key, (end_time - start_time)))

    #套路贷待商户清洗

    #信用卡还款特别标记
    credit_tp=[(i,j) for i,j in zip(id_list_local,input_mp['name']) if '信用卡' in j or '卡中心' in j or '掌上生活' in j and i not in record_list]
    credit_list = [i[0] for i in credit_tp]
    record_list += credit_list

    # end_time = time.time()
    # sys.stderr.write("1耗时%s秒\n" % ((end_time - start_time)))

    #等额本息
    deng_ebx,record_list,all_bill_num=deng_e_b_x(input_mp,id_list_local, record_list,all_bill_num=0)

    # end_time = time.time()
    # sys.stderr.write("2耗时%s秒\n" % ((end_time - start_time)))

    # 等额本金,还需要判断罚息
    deng_ebj, record_list, all_bill_num_new1 = deng_e_b_j(input_mp,id_list_local,record_list,all_bill_num)

    # end_time = time.time()
    # sys.stderr.write("3耗时%s秒\n" % ((end_time - start_time)))

    # 不分期,逾期,罚息
    faxi_mp, record_id_list, all_bill_num_new2=normal_faxi(input_mp,id_list_local,record_list,all_bill_num)

    # end_time = time.time()
    # sys.stderr.write("4耗时%s秒\n" % ((end_time - start_time)))

    # 正常订单合并,大于500输出
    # nor_mp,combine_mp,record_id_list,all_bill_num_new3=normal_func(input_mp,id_list_local,record_list,all_bill_num)
    # print(nor_mp,combine_mp)
    # print(nor_mp)

    # end_time = time.time()
    # sys.stderr.write("5耗时%s秒\n" % ((end_time - start_time)))

    #按首笔排序,赋予订单编号
    sort_list=[deng_ebx[i][1][0] for i in deng_ebx.keys()]
    sort_list+=[deng_ebj[i][1][0] for i in deng_ebj.keys()]
    sort_list+=[faxi_mp[i][1][0] for i in faxi_mp.keys()]
    # sort_list+=[combine_mp[i][0] for i in combine_mp.keys()]
    # sort_list+=nor_mp
    sort_list.sort()
    sort_map={j:i for i,j in enumerate(sort_list,start=1)}

    # end_time = time.time()
    # sys.stderr.write("6耗时%s秒\n" % ((end_time - start_time)))

    all_id_map={}
    # 添加标注:是否分期,订单号,当前期数,总期数,应还款日期
    for i in deng_ebx.keys():
        all_num=max(deng_ebx[i].keys())
        for j in deng_ebx[i].keys():
            #如果是第一天扣款,最后一笔成功,前几笔失败也不算逾期
            # should_pay_dt = data[data['id'] == deng_ebx[i][j][0]]['dt'].values[0]
            should_pay_dt = input_mp['dt'][id_list_local.index(deng_ebx[i][j][0])]
            last_dt=input_mp['dt'][id_list_local.index(deng_ebx[i][j][-1])]
            if last_dt == should_pay_dt and input_mp['status'][id_list_local.index(deng_ebx[i][j][-1])] == '0':
                is_delay_tag = 'n'
            else:
                is_delay_tag = 'y'
            for k in deng_ebx[i][j]:
                all_id_map[k]=['y',sort_map[deng_ebx[i][1][0]],j,all_num,should_pay_dt,is_delay_tag]


    for i in deng_ebj.keys():
        all_num=max(deng_ebj[i].keys())
        for j in deng_ebj[i].keys():
            should_pay_dt = input_mp['dt'][id_list_local.index(deng_ebj[i][j][0])]
            last_dt = input_mp['dt'][id_list_local.index(deng_ebj[i][j][-1])]
            if last_dt == should_pay_dt and input_mp['status'][id_list_local.index(deng_ebj[i][j][-1])] == '0':
                is_delay_tag = 'n'
            else:
                is_delay_tag = 'y'
            for k in deng_ebj[i][j]:
                all_id_map[k]=['y',sort_map[deng_ebj[i][1][0]],j,all_num,should_pay_dt,is_delay_tag]

    for i in faxi_mp.keys():
        for j in faxi_mp[i].keys():
            should_pay_dt = input_mp['dt'][id_list_local.index(faxi_mp[i][j][0])]
            last_dt = input_mp['dt'][id_list_local.index(faxi_mp[i][j][-1])]
            if last_dt == should_pay_dt and input_mp['status'][id_list_local.index(faxi_mp[i][j][-1])] == '0':
                is_delay_tag = 'n'
            else:
                is_delay_tag = 'y'
            for k in faxi_mp[i][j]:
                all_id_map[k]=['n',sort_map[faxi_mp[i][1][0]],'','',should_pay_dt,is_delay_tag]

    # for i in combine_mp.keys():
    #     for j in combine_mp[i]:
    #         should_pay_dt = input_mp['dt'][id_list_local.index(combine_mp[i][0])]
    #         last_dt = input_mp['dt'][id_list_local.index(combine_mp[i][-1])]
    #         if last_dt == should_pay_dt and input_mp['status'][id_list_local.index(combine_mp[i][j][-1])] == '0':
    #             is_delay_tag = 'n'
    #         else:
    #             is_delay_tag = 'y'
    #         all_id_map[j]=['n',sort_map[combine_mp[i][0]],'','',should_pay_dt,is_delay_tag]
    #
    # for i in nor_mp:
    #     all_id_map[i]=['n',sort_map[i],'','',input_mp['dt'][id_list_local.index(i)],'y']

    # end_time = time.time()
    # sys.stderr.write("7耗时%s秒\n" % ((end_time - start_time)))

    '''
    全部输出
    '''
    for id_out,name_out,time_out,status_out,amt_out,dt_out in \
            zip(id_list_local,input_mp['name'],input_mp['time_ori'],input_mp['status'],input_mp['amt'],input_mp['dt']):
        # print(data_out_tmp)
        # name_out=data_out_tmp['']
        time_out=str(time_out)
        # status_out=data_out_tmp[2]
        amt_out='%.2f' % amt_out
        # dt_out=data_out_tmp[4]
        # month_out=data_out_tmp[5]
        is_renzhen = 'y' if id_out in renzheng_list else ''
        is_xyk = 'y' if id_out in credit_list else ''
        fenqi_y = str(all_id_map[id_out][0]) if id_out in all_id_map else ''
        dingdan_num = str(all_id_map[id_out][1]) if id_out in all_id_map else ''
        fenqi_num = str(all_id_map[id_out][2]) if id_out in all_id_map else ''
        all_fenqi = str(all_id_map[id_out][3]) if id_out in all_id_map else ''
        shuld_pay_dt = str(all_id_map[id_out][4]) if id_out in all_id_map else ''
        is_delay_tag = str(all_id_map[id_out][5]) if id_out in all_id_map else ''
        is_delay = 'n' if shuld_pay_dt == dt_out and (status_out == '0' or is_delay_tag == 'n') else 'y'
        is_delay = is_delay if id_out in all_id_map else ''
        print('%s\t%s' % (last_key,','.join([name_out,time_out,status_out,amt_out,dt_out,is_renzhen,is_xyk,shuld_pay_dt,is_delay,fenqi_y,dingdan_num,fenqi_num,all_fenqi])))


    # end_time = time.time()
    # sys.stderr.write("8耗时%s秒\n" % ((end_time - start_time)))
    # return renzheng_list,credit_list,all_id_map

def init_all(input_mp,tag):
    if tag==0:
        input_mp={}
        input_mp['name']=[]
        input_mp['time']=[]
        input_mp['status']=[]
        input_mp['amt']=[]
        input_mp['dt']=[]
        input_mp['month']=[]
        input_mp['time_ori']=[]
    else:
        input_mp['name'].append(name)
        input_mp['time'].append(time)
        input_mp['status'].append(status)
        input_mp['amt'].append(amt)
        input_mp['dt'].append(dt)
        input_mp['month'].append(month)
        input_mp['time_ori'].append(time_ori)
    return input_mp



last_key=''
input_mp={}
for line in sys.stdin:
    items = line.strip("\n").split("\t")
    card = items[0].split('#')[0]
    time = datetime.datetime.strptime(items[0].split('#')[1][:19], '%Y-%m-%d %H:%M:%S')
    time_ori = items[0].split('#')[1]
    dt = items[0].split('#')[1][:10]
    month=dt[:7]

    info = items[1].split(',')
    name = info[0]
    amt = float(info[1])
    if float(amt) <= 0:
        continue
    status = '0' if info[2] in ['00', 'AA', '', '10', '11', '16', 'A2', 'A4', 'A5', 'A6', 'Y1', 'Y3'] else '1'

    if last_key !=card and last_key!='' and len(input_mp)!=0:
        # id_list = list(range(len(input_mp['amt'])))
        if len(input_mp['time'])<=1000:
            bill_func(input_mp)
        # info_list=[]
        input_mp=init_all(input_mp,tag=0)
    if last_key=='':
        input_mp = init_all(input_mp, tag=0)

    if len(input_mp['time'])<1010:
        input_mp=init_all(input_mp,tag=1)
    last_key=card

if len(input_mp)!=0:
    # id_list = list(range(len(input_mp['amt'])))
    if len(input_mp['time']) <= 1000:
        bill_func(input_mp)