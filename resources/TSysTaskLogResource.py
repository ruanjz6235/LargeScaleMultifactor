#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/6/10 10:32 上午  
# @author: ctp
from datetime import datetime

from framework.injection import database_injector

from common.log import logger


class TSysTaskLogResource:

    def insert(self, task_id=None, params=None, client_ip=None, function_name=None,
               start_time=None, log_info=None, log_detail_info=None, info_type=None, exception_type=None,
               create_user=None, update_user=None):
        db_connection = database_injector.get_resource()
        cursor = db_connection.cursor()
        try:
            log_id = task_id
            now = datetime.now()
            create_time = now
            update_time = now
            sql = 'insert into SYS_TASK_LOG (log_id,task_id,params,client_ip,function_name,start_time, \
                  log_info,log_detail_info,info_type,exception_type,create_user,create_time,update_user,update_time) \
                  values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)'
            cursor.execute(sql, (
            log_id, task_id, params, client_ip, function_name, start_time, log_info, log_detail_info, info_type,
            exception_type, create_user, create_time, update_user, update_time))
            db_connection.commit()
        except Exception as e:
            logger.exception('[taskId:%s] insert SYS_TASK_LOG,error' % task_id)
            db_connection.rollback()
            raise e
        finally:
            cursor.close()
