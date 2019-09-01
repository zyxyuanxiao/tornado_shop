# coding=utf8
from DBUtils.PooledDB import PooledDB
import pymysql
from conf import system_config
from pymysql.cursors import DictCursor
from conn_utils.mysql_conn.db_utils import dict_decimal_to_float
import threading
import logging
import traceback


class MysqlConn(object):
    _db_pools = {}
    _risk_min_cached = 0
    _risk_max_cached = 1
    _risk_max_connections = 1
    _risk_block = False

    _master_min_cached = 0
    _master_max_cached = 1
    _master_max_connections = 1
    _master_block = False

    _reptile_min_cached = 0
    _reptile_max_cached = 1
    _reptile_max_connections = 1
    _reptile_block = False

    _tidb_min_cached = 0
    _tidb_max_cached = 1
    _tidb_max_connections = 1
    _tidb_block = False

    _risk_loan_read_min_cached = 0
    _risk_loan_read_max_cached = 1
    _risk_loan_read_connections = 1
    _risk_loan_read_block = False

    _lock = threading.Lock()

    @classmethod
    def config_pool(cls, read_config=None, master_config=None):
        MysqlConn._lock.acquire()
        try:
            if read_config is not None and 'risk' in MysqlConn._db_pools:
                raise Exception("risk db pool has been inited")
            if master_config is not None and 'master' in MysqlConn._db_pools:
                raise Exception("master db pool has been inited")

            if read_config is not None:
                cls._read_min_cached = read_config.get("min_cached", 0)
                cls._read_max_cached = read_config.get("max_cached", 1)
                cls._read_max_connections = read_config.get("max_connections", 5)
                cls._read_block = read_config.get("block", False)
            if master_config is not None:
                cls._master_min_cached = master_config.get("min_cached", 0)
                cls._master_max_cached = master_config.get("max_cached", 1)
                cls._master_max_connections = master_config.get("max_connections", 5)
                cls._master_block = master_config.get("block", False)
        finally:
            MysqlConn._lock.release()

    @staticmethod
    def connection_statics():
        ret = {}
        max_ret = {}
        for db_type, db_pool in MysqlConn._db_pools.items():
            ret[db_type] = db_pool._connections
            max_ret[db_type] = db_pool._maxconnections
        return ret, max_ret

    def __init__(self, db_type="risk"):
        if db_type not in ["read", "master"]:
            raise Exception("db_type must be read or master or reptile")
        if db_type not in MysqlConn._db_pools:
            MysqlConn._lock.acquire()
            try:
                if db_type not in MysqlConn._db_pools:
                    if db_type == "read":
                        MysqlConn._db_pools[db_type] = PooledDB(creator=pymysql, mincached=MysqlConn._risk_min_cached,
                                                                maxcached=MysqlConn._risk_max_cached,
                                                                maxconnections=MysqlConn._risk_max_connections,
                                                                host=system_config.get_mysql_server_ip(),
                                                                user=system_config.get_mysql_server_user(),
                                                                passwd=system_config.get_mysql_passwd(),
                                                                db=system_config.get_mysql_default_db(),
                                                                use_unicode=True,
                                                                charset=system_config.get_mysql_charset(),
                                                                cursorclass=DictCursor,
                                                                blocking=MysqlConn._risk_block)
                    elif db_type == "master":
                        MysqlConn._db_pools[db_type] = PooledDB(creator=pymysql, mincached=MysqlConn._master_min_cached,
                                                                maxcached=MysqlConn._master_max_cached,
                                                                maxconnections=MysqlConn._master_max_connections,
                                                                host=system_config.get_mysql_master_server_ip(),
                                                                user=system_config.get_mysql_master_server_user(),
                                                                passwd=system_config.get_mysql_master_passwd(),
                                                                db=system_config.get_mysql_master_default_db(),
                                                                use_unicode=True,
                                                                charset=system_config.get_mysql_charset(),
                                                                cursorclass=DictCursor,
                                                                blocking=MysqlConn._master_block)




            finally:
                MysqlConn._lock.release()

        self._conn = MysqlConn._db_pools[db_type].connection()
        self._cur = self._conn.cursor()

    def select_one(self, sql, params=None):
        count = self._cur.execute(sql, params)
        if count > 0:
            return dict_decimal_to_float(self._cur.fetchone())
        else:
            return None

    def select_one_value(self, sql, params=None):
        count = self._cur.execute(sql, params)
        if count > 0:
            result = dict_decimal_to_float(self._cur.fetchone())
            return list(result.values())[0]
        else:
            return None

    def select_many(self, sql, params=None):
        count = self._cur.execute(sql, params)
        if count > 0:
            result = self._cur.fetchall()
            return list(map(lambda one: dict_decimal_to_float(one), result))
        else:
            return []

    def select_many_one_value(self, sql, params=None):
        count = self._cur.execute(sql, params)
        if count > 0:
            result = self._cur.fetchall()
            return list(map(lambda one: list(dict_decimal_to_float(one).values())[0], result))
        else:
            return []

    def insert_one(self, sql, params=None, return_auto_increament_id=False):
        self._cur.execute(sql, params)
        if return_auto_increament_id:
            return self._cur.lastrowid

    def insert_many(self, sql, params):
        count = self._cur.executemany(sql, params)
        return count

    def update(self, sql, param=None):
        return self._cur.execute(sql, param)

    def delete(self, sql, param=None):
        return self._cur.execute(sql, param)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._cur.close()
        self._conn.close()

    def executed(self):
        """获取刚刚执行的SQL语句
        _cur._executed 是私有属性，可能会在没有警告的情况下改变，所以本方法仅适用于调试时使用"""
        return self._cur._executed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logging.error(exc_type)
            logging.error(exc_val)
            logging.error(traceback.format_exc())
        self.close()
        return False


if __name__ == '__main__':
    with MysqlConn() as connect:
        a = connect.select_one("select * from installmentdb.t_user limit 1")
        print(a)
