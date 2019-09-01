# -*- coding: utf-8 -*-
# @Author: YouShaoPing
# @Date:   2019-01-16 10:21:31
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2019-01-16 11:02:27

import uuid
import time
import logging
import traceback
from redis import StrictRedis


class RedisLock():

    def __init__(self, lock_name, host="localhost",
                 port=6379, db=0, password=None,
                 acquire_timeout=0, lock_timeout=10,
                 **kwargs):
        self.redis_conn = StrictRedis(host=host, port=port, db=db, password=password, **kwargs)
        self.lock = f'Akulaku:RedisLock:{lock_name}'
        self.acquire_timeout = acquire_timeout
        self.lock_timeout = lock_timeout

    def __enter__(self):
        end = time.time() + self.acquire_timeout
        while 1:
            time.sleep(0.1)
            self.value = str(uuid.uuid1())
            if self.redis_conn.set(self.lock, self.value, nx=True, ex=self.lock_timeout):
                logging.info(f'get DistributeRedisLock {self.lock}')
                break
            if self.acquire_timeout == 0:
                continue
            if time.time() > end:
                break

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exit_value = self.redis_conn.get(self.lock)
        if self._exit_value is None:
            return
        if str(self._exit_value, encoding='utf-8') == self.value:
            self.redis_conn.delete(self.lock)
            logging.info(f'delete DistributeRedisLock {self.lock}')
        logging.info(f'exit DistributeRedisLock {self.lock}')
        if exc_type is not None:
            logging.error(exc_type)
            logging.error(exc_val)
            logging.error(traceback.format_exc())
