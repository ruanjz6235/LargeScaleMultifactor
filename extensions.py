#! /usr/bin/env python
# -*- coding: utf-8 -*-
from celery import Celery
from flask_redis import FlaskRedis
from flask_restplus import Api
from flask_moment import Moment
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy as _BaseSQLAlchemy
from sqlalchemy import exc
from sqlalchemy import event
from app.framework.injection import database_injector

from app.common.log import logger


class SQLAlchemy(_BaseSQLAlchemy):
    def apply_pool_defaults(self, app, options):
        super(SQLAlchemy, self).apply_pool_defaults(app, options)
        options["pool_pre_ping"] = True


# 创建扩展实例
# api = Api(doc=False)
api = Api()
moment = Moment()
db = SQLAlchemy()
migrate = Migrate()
start_app = False
redis_client = FlaskRedis()
celery = Celery('app-python-tasks')


# 初始化
def extension_config(app):
    global start_app
    start_app = True
    api.init_app(app)
    moment.init_app(app)
    db.app = app
    db.init_app(app=app)
    db.create_all()
    migrate.init_app(app, db)
    redis_client.init_app(app)
    database_config = app.config["DB_URI"]
    database_injector.config(**database_config)

    @event.listens_for(db.engine, 'checkout')
    def checkout(dbapi_con, con_record, con_proxy):
        try:
            try:
                dbapi_con.ping(False)
            except TypeError:
                logger.debug('connection died. Restoring...')
                dbapi_con.ping()
        except Exception as e:
            logger.warning(e)
            raise exc.DisconnectionError()

    @event.listens_for(db.engine, "engine_connect")
    def ping_connection(connection, branch):
        if branch:
            # "branch" refers to a sub-connection of a connection,
            # we don't want to bother pinging on these.
            return

        # turn off "close with result".  This flag is only used with
        # "connectionless" execution, otherwise will be False in any case
        save_should_close_with_result = connection.should_close_with_result
        connection.should_close_with_result = False

        try:
            # run a SELECT 1.   use a core select() so that
            # the SELECT of a scalar value without a table is
            # appropriately formatted for the backend
            # connection.scalar(select(1))
            connection.execute('select 1 from dual')
        except exc.DBAPIError as err:
            # catch SQLAlchemy's DBAPIError, which is a wrapper
            # for the DBAPI's exception.  It includes a .connection_invalidated
            # attribute which specifies if this connection is a "disconnect"
            # condition, which is based on inspection of the original exception
            # by the dialect in use.
            if err.connection_invalidated:
                # run the same SELECT again - the connection will re-validate
                # itself and establish a new connection.  The disconnect detection
                # here also causes the whole connection pool to be invalidated
                # so that all stale connections are discarded.
                # connection.scalar(select(1))
                connection.execute('select 1 from dual')
            else:
                raise
        finally:
            # restore "close with result"
            connection.should_close_with_result = save_should_close_with_result