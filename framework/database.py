import cx_Oracle
from dbutils.pooled_db import PooledDB

from common.log import logger


class DatabasePool:

    def __init__(self, host='127.0.0.1', port=1521, user=None, password=None, database=None,
                 pool_size=5, autocommit=True, echo=False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool_size = pool_size
        self.autocommit = autocommit
        self.echo = echo
        self._pool = None
        self.url = 'oracle://%s@%s:%s/%s' % (self.user, self.host, self.port,
                                            self.database if self.database is not None else '')

    def __call__(self, host='127.0.0.1', port=1521, user=None, password=None, database=None,
                 pool_size=5, autocommit=True, echo=False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool_size = pool_size
        self.autocommit = autocommit
        self.echo = echo
        self._pool = None
        self.url = 'oracle://%s@%s:%s/%s' % (self.user, self.host, self.port,
                                            self.database if self.database is not None else '')
        return Connection(self)

    def init(self):
        if self._pool:
            return

        logger.info('init db pool %s', self.url)
        logger.info('init db sid %s', self.database)
        # dsn = cx_Oracle.makedsn(self.host, self.port, sid=self.database)
        dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.database)
        self._pool = PooledDB(cx_Oracle, threaded=True, user=self.user, password=self.password, encoding='UTF-8', dsn=dsn, mincached=5, maxcached=30)

        logger.info('init db pool success %s', self.url)

    def close(self):
        if not self._pool:
            return

        logger.info('close connection to database %s', self.url)
        self._pool.terminate()
        return self._pool.wait_closed()

    def get_connection(self):
        self.init()
        return self._pool.connection(shareable=1)


class Connection:

    def __init__(self, pool):
        self.pool = pool
        self._connection = None

    def cursor(self, *cursors):
        self._init_connection()
        return self._connection.cursor(*cursors)

    def close(self):
        if not self._connection:
            return

        self._connection.close()

    def commit(self):
        self._connection.commit()

    def _init_connection(self):
        if self._connection:
            return
        self._connection = self.pool.get_connection()
