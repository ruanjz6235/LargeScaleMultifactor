#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/6/10 10:32 上午  
# @author: ctp

from app.framework.injection import database_injector
from app.common.log import logger


class TSysExecTaskResource:

    def update_by_task_id(self, task_id=None, start_time=None, end_time=None, progress=None, status=None,
                          insert_rows=None):
        db_connection = database_injector.get_resource()
        cursor = db_connection.cursor()
        try:
            sql = """
                  update SYS_EXEC_TASK set start_time=:1,end_time=:2,
                  progress=:3,status=:4,"ROWS"=:5
                  where task_id=:6
                  """
            cursor.execute(sql, (start_time, end_time, progress, status, insert_rows, task_id))
            db_connection.commit()
        except Exception as e:
            logger.exception('[taskId:%s] insert SYS_EXEC_TASK,error' % task_id)
            db_connection.rollback()
            raise e
        finally:
            cursor.close()
