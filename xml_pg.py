#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   xml_pg.py
@Time    :   2020/07/05 11:51:08
@Author  :   Haifeng
@Contact :   hubert28@qq.com
@Desc    :   xml数据包转换为tick数据存入pg中
'''

# here put the import lib
import os, sys, time, tarfile, csv, json
from datetime import datetime, timedelta
from time import localtime, strftime
from lxml import etree as ET
from io import StringIO
import config as cfg

trade_time = {}
trading_days = []

def init():
    """初始化:取交易日历和品种时间"""
    trading_days.clear()
    with open('/home/calendar.csv') as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r['tra'] == 'false':
                continue
            trading_days.append(r['day'])
            
    trade_time.clear()
    tmp = {}
    with open('/home/tradingtime.csv') as f:
        reader = csv.DictReader(f)
        proc_day = {}
        for r in reader:
            # 按时间排序, 确保最后实施的时间段作为依据.
            if r['GroupId'] not in proc_day or r['OpenDate'] > proc_day[r['GroupId']]:
                tmp[r['GroupId']] = r['WorkingTimes']
            proc_day[r['GroupId']] = r['OpenDate']
    # 根据时间段设置,生成 opens; ends; mins盘中时间
    for g_id, section  in tmp.items():
        opens = []
        ends = []
        mins = []
        for s in json.loads(section):
            opens.append((datetime.strptime(s['Begin'], '%H:%M:%S') + timedelta(minutes=-1)).strftime('%H:%M:00'))
            ends.append(s['End'])
            t_begin = datetime.strptime('20180101' + s['Begin'], '%Y%m%d%H:%M:%S')
            s_end = datetime.strptime('20180101' + s['End'], '%Y%m%d%H:%M:%S')
            if t_begin > s_end:  # 夜盘
                s_end += timedelta(days=1)
            while t_begin < s_end:
                mins.append(t_begin.strftime('%H:%M:00'))
                t_begin = t_begin + timedelta(minutes=1)
        trade_time[g_id] = {'Opens': opens, 'Ends': ends, 'Mins': mins}
        
def xml_pg(day: str):
    """"""
    # 初始化数据
    init()
    # 数据压缩包解压
    cfg.log.info(f'extract {day} ...')
    tar = tarfile.open(os.path.join(cfg.xml_zip_path, day + '.tar.gz'))
    # 解压到./tradingday/
    tar.extract('marketdata.xml', f'./{day}')
    tar.close()

    xml_file = f'./{day}/marketdata.xml'

    # 写入数据
    cfg.log.info(f'dicts to pg {day} ...')
    output = StringIO()
    
    connection = cfg.en_pg.raw_connection()
    cursor = connection.cursor()
    cursor.execute(f"select count(1) from pg_tables where schemaname='future_tick' and tablename='{day}';")  # 表名必须用'引起来
    # 存在: 删除
    if cursor.fetchone()[0] > 0:
        sqlstr = f'DROP TABLE future_tick."{day}";'
        cursor.execute(sqlstr)
        connection.commit()

    sqlstr = f'CREATE TABLE future_tick."{day}" ("AskPrice" float8 NULL, "AskVolume" int8 NULL, "BidPrice" float8 NULL, "BidVolume" int8 NULL, "Instrument" text NULL, "LastPrice" float8 NULL, "Volume" int8 NULL, "OpenInterest" float8 NULL, "UpdateTime" text NULL, "Actionday" text NULL,  "UpdateMillisec" int8 NULL) WITH(OIDS=FALSE)'
    cursor.execute(sqlstr)
    connection.commit()

    # 实际交易的日期和次日(夜盘用)
    actionday = day if trading_days.index(day) == 0 else trading_days[trading_days.index(day) - 1]
    actionday1 = (datetime.strptime(actionday, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
    dic_cnt = 0
    for event, elem in ET.iterparse(xml_file, events=('end',)):
        if elem.tag == 'NtfDepthMarketDataPackage':
            e_time = elem.find('MarketDataUpdateTimeField')
            e_last = elem.find('MarketDataLastMatchField')
            e_best = elem.find('MarketDataBestPriceField')
            if e_best is None or e_time is None or e_last is None or e_time.get('InstrumentID') is None:
                elem.clear()
                continue
            last_price = e_last.get('LastPrice')
            ask_price = e_best.get('AskPrice1')
            bid_price = e_best.get('BidPrice1')
            volume = e_last.get('Volume')
            if last_price == '' or ask_price == '' or bid_price == '' or volume == '0':
                elem.clear()
                continue
            doc = {}
            if e_best is not None:
                doc['AskPrice'] = float(ask_price)
                doc['AskVolume'] = int(e_best.get('AskVolume1'))
                doc['BidPrice'] = float(bid_price)
                doc['BidVolume'] = int(e_best.get('BidVolume1'))
            else:
                doc['AskVolume'] = int(0)  # 防止默认值为float
                doc['BidVolume'] = int(0)

            doc['Instrument'] = e_time.get('InstrumentID')
            doc['LastPrice'] = float(last_price)
            doc['Volume'] = int(e_last.get('Volume'))
            doc['OpenInterest'] = float(e_last.get('OpenInterest'))
            doc['UpdateTime'] = e_time.get('UpdateTime')
            doc['Actionday'] = actionday if doc['UpdateTime'][0:2] >= '19' else actionday1 if doc['UpdateTime'][0:2] <= '04' else day
            doc['UpdateMillisec'] = int(e_time.get('UpdateMillisec'))
            elem.clear()
            # dics.append(doc)
            dic_cnt += 1
            output.write(f"{doc['AskPrice']}\t{doc['AskVolume']}\t{doc['BidPrice']}\t{doc['BidVolume']}\t{doc['Instrument']}\t{doc['LastPrice']}\t{doc['Volume']}\t{doc['OpenInterest']}\t{doc['UpdateTime']}\t{doc['Actionday']}\t{doc['UpdateMillisec']}\n")
            if dic_cnt >= 150000:
                cfg.log.info(f'write to pg records: {dic_cnt}')
                dic_cnt = 0
                output.seek(0)
                cursor.copy_from(output, f'future_tick."{day}"')
                connection.commit()
                output.flush()
            # print(doc)
    output.seek(0)
    cursor.copy_from(output, f'future_tick."{day}"')
    connection.commit()
    cursor.close()
    cfg.log.info(f'write to pg records: {dic_cnt}')
    # df: DataFrame = DataFrame(dics)
    os.remove(xml_file)
    os.removedirs(day)


if __name__ == "__main__":

    if not os.path.exists(cfg.xml_zip_path):
        cfg.log.error(f'xml path {cfg.xml_zip_path} is NOT exists!')
        sys.exit(-1)

    # 已经存在的数据
    days = os.listdir(cfg.xml_zip_path)
    if len(sys.argv) > 1:
        days = [d.split('.')[0] for d in days if d >= sys.argv[1]] # 处理>=参数的日期

    connection = cfg.en_pg.raw_connection()  # engine 是 from sqlalchemy import create_engine
    cursor = connection.cursor()
    cursor.execute(f"select count(1) from pg_catalog.pg_namespace where nspname = 'future_tick'") # schema是否存在
    if cursor.fetchone()[0] == 0:
        sqlstr = f'CREATE SCHEMA future_tick;'
        cursor.execute(sqlstr)
        connection.commit()
    elif len(sys.argv) == 1: # 无参数传递
        sqlstr = f"select max(tablename) from pg_catalog.pg_tables where schemaname = 'future_tick'"
        cursor.execute(sqlstr)
        maxday = cursor.fetchone()[0]
        if maxday is not None:
            days = [d for d in days if d > maxday]
        
    for day in days:
        xml_pg(day)

    next_day = time.strftime('%Y%m%d', time.localtime())
    next_day = [d for d in trading_days if d >= next_day][0]
    cfg.log.info(f'wait for next day: {next_day}')
    while True:
        if not os.path.exists(os.path.join(cfg.xml_zip_path, f'{next_day}.tar.gz')):
            time.sleep(60 * 10)
            continue
        time.sleep(60) # 待文件写入
        xml_pg(next_day)
        next_day = [d for d in trading_days if d > next_day][0]
        cfg.log.info(f'wait for next day: {next_day}')
