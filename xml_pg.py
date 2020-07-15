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
import os, sys, time, tarfile
from datetime import datetime, timedelta
from time import localtime, strftime
from lxml import etree as ET
from io import StringIO
import config as cfg

conn = cfg.en_pg.raw_connection()
cursor = conn.cursor()
cursor.execute('''select "day" from future.calendar where tra''') # and "day" between to_char(now()::timestamp + '-30 days', 'yyyyMMdd') and to_char(now()::timestamp + '30 days', 'yyyyMMdd')''')
trading_days = [c[0] for c in cursor.fetchall()]

    
def xml_pg(day: str):
    """"""
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
    return


if __name__ == "__main__":

    if not os.path.exists(cfg.xml_zip_path):
        cfg.log.error(f'xml path {cfg.xml_zip_path} is NOT exists!')
        sys.exit(-1)

    # 已经存在的数据
    days = os.listdir(cfg.xml_zip_path)
    days = [d.split('.')[0] for d in days if d >= '20200203'] # 新数据

    connection = cfg.en_pg.raw_connection()  # engine 是 from sqlalchemy import create_engine
    cursor = connection.cursor()
    cursor.execute(f"select count(1) from pg_catalog.pg_namespace where nspname = 'future_tick'") # schema是否存在
    if cursor.fetchone()[0] == 0:
        sqlstr = f'CREATE SCHEMA future_tick;'
        cursor.execute(sqlstr)
        connection.commit()
    else:
        sqlstr = f"select max(tablename) from pg_catalog.pg_tables where schemaname = 'future_tick'"
        cursor.execute(sqlstr)
        maxday = cursor.fetchone()[0]
        if maxday is not None:
            days = [d for d in days if d > maxday]
        
    for day in days:
        xml_pg(day)

    next_day = time.strftime('%Y%m%d', time.localtime())
    next_day = [d for d in trading_days if d >= next_day][0]
    while True:
        if not os.path.exists(os.path.join(cfg.xml_zip_path, f'{next_day}.tar.gz')):
            time.sleep(60 * 10)
            continue
        time.sleep(60) # 待文件写入
        xml_pg(next_day)
        next_day = [d for d in trading_days if d > next_day][0]
        cfg.log.info(f'wait for next day: {next_day}')
