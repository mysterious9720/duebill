#!/usr/bin/env python
#-*-coding:utf-8-*-
"""
# -*- coding: utf-8 -*-
# trans_map_r2.py  by  ykx  Time    : 19-5-29 上午10:30  
# cat test_data |python trans_map_r2.py |head -100|sort|awk -F"\t" '{print $1"\t"$2","$3","$4","$5}'>aa
# 对此部分结果需要按时间排序
"""

import sys
version=sys.argv[1]

for line in sys.stdin:
    items = line.strip().split("\t")
    card_time = items[0]
    time = card_time.split('#')[1]
    dt=time[:10]
    if dt<'2018-01-01':
        continue
    info = items[1].split(',')
    if time[:7]>=version:
        continue
    name = info[0]
    amt = '%.2f' % (float(info[1])/100)
    if float(amt)<100.00:#为了保证月增的数据一致性，尝试都取200以上#20191212
        continue
    acpt = info[2]
    # '''
    # test
    # '''
    # items = line.strip().split("\t")
    # card = items[0]
    # # info = items[1].split(',')
    # name = items[1]
    # time = items[2]
    # acpt = '00' if items[3]=='0' else '11'
    # amt = '%.2f' % float(items[4])
    # card_time=card+'#'+time
    print('%s\t%s' % (card_time ,','.join([name,amt,acpt])))

