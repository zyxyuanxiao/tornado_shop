#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (python) ,2018-2036, AKULAKU Tech. Co,Ltd
# @Author: YouShaoPing
# @Version: 0.0.1
# @Date:   2018-11-15 14:27:10
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2018-12-07 10:39:50

import time
import ujson
import traceback
import asyncio
from aiokafka.producer import AIOKafkaProducer

loop = asyncio.get_event_loop()


class KafkaProducer(object):
    """docstring for KafkaProducer"""

    """
    metadata_max_age_ms 强制刷新最大时间ms
    request_timeout_ms 超时时间ms
    acks (0, -1, 1, all) 0: 只管发送;
                        -1: 默认-1=all需要等待所有副本确认;
                         1: 只需要leader节点接收到数据;
    compression_type('gzip', 'snappy', 'lz4', None) 数据压缩格式默认为None
    max_batch_size 每个分区缓冲数据最大
    max_request_size 一次请求的最大大小，超过会自动发送send
    linger_ms 延迟发送时间
    connections_max_idle_ms 空闲连接关闭检测时间
    enable_idempotence 保证数据送达标志为True acks必须为(-1, all)
    """
    Producer = AIOKafkaProducer(loop=loop,
                                bootstrap_servers='localhost',
                                metadata_max_age_ms=30000,
                                request_timeout_ms=1000,
                                max_batch_size=16384,
                                max_request_size=1048576,
                                linger_ms=0,
                                connections_max_idle_ms=540000)


    async def partitions_for(self):
        return await self.Producer.partitions_for()

    async def start(self):
        if self.Producer._sender_task is None:
            await self.Producer.start()

    async def stop(self):
        await self.Producer.stop()

    async def flush(self):
        await self.Producer.flush()

    @classmethod
    def code_data(cls, value, data_type=None, uid=None, country_id=None):
        data = {"data": value, "createTime": int(time.time() * 1000)}
        if data_type is not None:
            data["type"] = int(data_type)
        if uid is not None:
            data["uid"] = int(uid)
        if country_id is not None:
            data["country_id"] = country_id
        return data

    async def send(self, topic, value, key=None, data_type=None, uid=None, country_id=None, partition=None, timestamp_ms=None):
        try:
            data = self.code_data(value, data_type, uid, country_id)
            data = bytes(ujson.dumps(data), encoding='utf-8')
            return await self.Producer.send_and_wait(topic, value=data, key=bytes(key, encoding='utf-8'), partition=partition, timestamp_ms=timestamp_ms)
        except Exception:
            print(traceback.format_exc())

    async def send_many(self, topic, values, key, data_type=None, uid=None, country_id=None, partition=None, timestamp_ms=None):
        batch = self.Producer.create_batch()
        for value in values:
            data = self.code_data(value, data_type, uid, country_id)
            metadata = batch.append(key=key, value=data, timestamp=timestamp_ms)
            if metadata is None:
                await self.Producer.send_batch(batch, topic, partition=partition)
                batch = self.Producer.create_batch()
                continue
        await self.Producer.send_batch(batch, topic, partition=partition)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.flush()


class RiskProducer(KafkaProducer):
    Producer = AIOKafkaProducer(
        loop=loop,
        bootstrap_servers='172.31.10.78:9092')
    """docstring for Master_MysqlConn"""

    def __init__(self):
        super(RiskProducer, self).__init__()


async def some_work():
    async with RiskProducer() as Producer:
        import uuid
        n = 0
        while n < 10000:
            time.sleep(1)
            data = {"uid": str(uuid.uuid1()), "n": str(n)}
            print(data)
            await Producer.send("test", data, str(uuid.uuid1()))
            n = n + 1

if __name__ == "__main__":
    asyncio.ensure_future(some_work(), loop=loop)
    loop.run_forever()
