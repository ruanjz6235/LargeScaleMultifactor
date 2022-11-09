#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/28 上午10:27  
# @author: ctp


# -*- coding: utf-8 -*-
from pyhive import hive

#conn = hive.Connection(host='192.168.1.72', port=10000, username='root', database='testdalu')

conn = hive.Connection(host='192.168.0.130', port=10000, username='root', database='default')
cursor = conn.cursor()
cursor.execute('select * from pokes')
for result in cursor.fetchall():
     print(result)
cursor.close()
conn.close()
