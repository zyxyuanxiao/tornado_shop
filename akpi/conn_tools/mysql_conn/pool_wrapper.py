#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (python) ,2018-2036, AKULAKU Tech. Co,Ltd
# @Author: YouShaoPing
# @Version: 0.0.1
# @Date:   2018-10-23 09:56:05
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2018-11-12 14:42:56

from aiomysql import Pool
from aiomysql import DictCursor
from aiomysql.connection import connect


class PoolWrapper(Pool):
    """add cache function"""

    def __init__(self, echo, pool_recycle, loop, mincached=0, maxcached=0,
                 minsize=0, maxsize=0, **kwargs):
        kwargs["cursorclass"] = DictCursor
        super(PoolWrapper, self).__init__(minsize, maxsize, echo, pool_recycle, loop, **kwargs)
        if maxcached < mincached:
            raise ValueError("maxcached should be not less than mincached")
        if maxsize < maxcached:
            raise ValueError("maxsize should be not less than maxcached")
        if minsize < mincached:
            raise ValueError("minsize should be not less than mincached")
        self._mincached = mincached
        self._maxcached = maxcached

    async def _fill_free_pool(self, override_min):
        # iterate over free connections and remove timeouted ones
        free_size = len(self._free)
        used_size = len(self._used)
        n = 0
        while n < free_size:
            conn = self._free[-1]
            if conn._reader.at_eof() or conn._reader.exception():
                self._free.pop()
                conn.close()

            elif (self._recycle > -1 and
                  self._loop.time() - conn.last_usage > self._recycle):
                self._free.pop()
                conn.close()

            else:
                self._free.rotate()
            n += 1

        n = 0
        tmp_used = self._used.copy()
        for conn in self._used:
            if conn._reader.at_eof():
                tmp_used.remove(conn)
                conn.close()
            elif (self._recycle > -1 and
                  self._loop.time() - conn.last_usage > self._recycle):
                tmp_used.remove(conn)
                conn.close()
        self._used = tmp_used 

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    def release(self, conn):
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        fut = self._loop.create_future()
        fut.set_result(None)

        if conn in self._terminated:
            assert conn.closed, conn
            self._terminated.remove(conn)
            return fut
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            in_trans = conn.get_transaction_status()
            if in_trans:
                conn.close()
                return fut
            if self._closing:
                conn.close()
            elif len(self._free) >= self._maxcached:
                conn.close()
            else:
                self._free.append(conn)
            fut = self._loop.create_task(self._wakeup())
        return fut
