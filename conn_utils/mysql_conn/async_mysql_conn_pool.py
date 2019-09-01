# coding:utf-8

import asyncio
from akpi.conn_tools.mysql_conn.pool_wrapper import PoolWrapper
from conf import system_config
from conn_utils.mysql_conn.db_utils import dict_decimal_to_float
import threading


class AsyncMysqlConn(object):
    _db_pools = {}
    _risk_min_cached = 0
    _risk_max_cached = 1
    _risk_max_connections = 1
    _risk_block = False

    _master_min_cached = 0
    _master_max_cached = 1
    _master_max_connections = 1
    _master_block = False

    _lock = threading.Lock()

    @classmethod
    def config_pool(cls, read_config=None, master_config=None):
        AsyncMysqlConn._lock.acquire()
        try:
            if read_config is not None and "read" in AsyncMysqlConn._db_pools:
                raise Exception("read db pool has been inited")
            if master_config is not None and "master" in AsyncMysqlConn._db_pools:
                raise Exception("master db pool has been inited")

            if read_config is not None:
                cls._risk_min_cached = read_config.get("min_cached", 0)
                cls._risk_max_cached = read_config.get("max_cached", 1)
                cls._risk_max_connections = read_config.get("max_connections", 5)
                cls._risk_block = read_config.get("block", False)
            if master_config is not None:
                cls._master_min_cached = master_config.get("min_cached", 0)
                cls._master_max_cached = master_config.get("max_cached", 1)
                cls._master_max_connections = master_config.get("max_connections", 5)
                cls._master_block = master_config.get("block", False)

        finally:
            AsyncMysqlConn._lock.release()

    @staticmethod
    def connection_statics():
        ret = {}
        for db_type, db_pool in AsyncMysqlConn._db_pools.items():
            ret[db_type] = db_pool._used
        return ret

    def __init__(self, db_type="read"):
        if db_type not in ["read", "master"]:
            raise Exception("db_type must be read or master")
        self.db_type = db_type
        if db_type not in AsyncMysqlConn._db_pools:
            AsyncMysqlConn._lock.acquire()
            try:
                if db_type not in AsyncMysqlConn._db_pools:
                    if db_type == "read":
                        AsyncMysqlConn._db_pools[db_type] = \
                            PoolWrapper(mincached=AsyncMysqlConn._risk_min_cached,
                                        maxcached=AsyncMysqlConn._risk_max_cached,
                                        maxsize=AsyncMysqlConn._risk_max_connections,
                                        minsize=AsyncMysqlConn._risk_min_cached,
                                        loop=asyncio.get_event_loop(),
                                        pool_recycle=3600,
                                        host=system_config.get_mysql_server_ip(),
                                        user=system_config.get_mysql_server_user(),
                                        password=system_config.get_mysql_passwd(),
                                        port=3306,
                                        db=system_config.get_mysql_default_db(),
                                        echo=False)
                    elif db_type == "master":
                        AsyncMysqlConn._db_pools[db_type] = \
                            PoolWrapper(mincached=AsyncMysqlConn._master_min_cached,
                                        maxcached=AsyncMysqlConn._master_max_cached,
                                        maxsize=AsyncMysqlConn._master_max_connections,
                                        minsize=AsyncMysqlConn._master_min_cached,
                                        loop=asyncio.get_event_loop(),
                                        pool_recycle=3600,
                                        host=system_config.get_mysql_master_server_ip(),
                                        user=system_config.get_mysql_master_server_user(),
                                        password=system_config.get_mysql_master_passwd(),
                                        port=3306,
                                        db=system_config.get_mysql_master_default_db(),
                                        echo=False)

            finally:
                AsyncMysqlConn._lock.release()

    async def select_one(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                result = await self._cur.fetchone()
                result = dict_decimal_to_float(result)
                return result
            else:
                return None
        except Exception as e:
            await self.rollback()
            raise e

    async def select_one_value(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                result = await self._cur.fetchone()
                result = dict_decimal_to_float(result)
                return list(result.values())[0]
            else:
                return None
        except Exception as e:
            await self.rollback()
            raise e

    async def select_many(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                result = await self._cur.fetchall()
                return list(map(lambda one: dict_decimal_to_float(one), result))
            else:
                return []
        except Exception as e:
            await self.rollback()
            raise e

    async def select_many_one_value(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                result = await self._cur.fetchall()
                return list(map(lambda one: list(dict_decimal_to_float(one).values())[0], result))
            else:
                return []
        except Exception as e:
            await self.rollback()
            raise e

    async def insert_one(self, sql, params=None, return_auto_increament_id=False):
        try:
            await self._cur.execute(sql, params)
            if return_auto_increament_id:
                return self._cur.lastrowid
        except Exception as e:
            await self.rollback()
            raise e

    async def insert_many(self, sql, params):
        try:
            count = await self._cur.executemany(sql, params)
            return count
        except Exception as e:
            await self.rollback()
            raise e

    async def update(self, sql, params=None):
        try:
            result = await self._cur.execute(sql, params)
            return result
        except Exception as e:
            await self.rollback()
            raise e

    async def delete(self, sql, params=None):
        try:
            result = await self._cur.execute(sql, params)
            return result
        except Exception as e:
            await self.rollback()
            raise e

    async def begin(self):
        await self._conn.begin()

    async def commit(self):
        try:
            await self._conn.commit()
        except Exception as e:
            await self.rollback()
            raise e

    async def rollback(self):
        await self._conn.rollback()

    async def close(self):
        AsyncMysqlConn._db_pools[self.db_type].close()
        await AsyncMysqlConn._db_pools[self.db_type].wait_closed()

    def executed(self):
        return self._cur._executed

    async def __aenter__(self):
        self._conn = await AsyncMysqlConn._db_pools[self.db_type].acquire()
        self._cur = await self._conn.cursor()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.commit()
            await self._cur.close()
        except Exception as e:
            raise e
        finally:
            await AsyncMysqlConn._db_pools[self.db_type].release(self._conn)
