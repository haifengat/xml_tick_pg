#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = ''
__author__ = 'HaiFeng'
__mtime__ = '20180808'

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from color_log import Logger


log = Logger()
"""日志"""

xml_zip_path = '/mnt/future_xml/'
"""xml压缩包路径"""

if 'xml_zip_path' in os.environ:
    xml_zip_path = os.environ['xml_zip_path']

pg_conn = 'postgres://postgres:123456@172.19.129.98:15432/postgres'
if 'pg_conn' in os.environ:
    pg_conn = os.environ['pg_conn']

en_pg: Engine = create_engine(pg_conn)
