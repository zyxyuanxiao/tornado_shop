#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (python) ,2018-2036, AKULAKU Tech. Co,Ltd
# @Author: YouShaoPing
# @Version: 0.0.1
# @Date:   2018-11-15 16:49:26
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2018-12-08 10:15:08

import uuid
import asyncio
import logging
import traceback
from aiokafka.errors import OffsetOutOfRangeError
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from aiokafka import OffsetAndMetadata, TopicPartition

loop = asyncio.get_event_loop()
root_logger = logging.getLogger()
default_listener = ConsumerRebalanceListener()


class KafkaConsumer(object):
    """docstring for KafkaConsumer"""

    """
        group_id: 消费者名称
        fetch_max_wait_ms: 没有数据等待时间
        fetch_max_bytes:最大数据量(50M)
        fetch_min_bytes:最小数据量
        max_partition_fetch_bytes: 单个分区可以接收最大数据（必须和Producer的max_request_size一样）
        request_timeout_ms 超时时间ms
        auto_offset_reset: 开始读取数据位置默认latest 读取最近的
                            earliest从头开始读取，但是需要group_id不同
        enable_auto_commit: 是否自动提交默认为True
        auto_commit_interval_ms: 自动提交的时间间隔
        check_crcs: crc检测
        metadata_max_age_ms: 强制刷新时间
        heartbeat_interval_ms: 心跳时间
        session_timeout_ms: 会话超时时间
        exclude_internal_topics: 为真内部主题消费只能使用订阅消费
    """

    def __init__(self, loop=loop,
                 fetch_max_wait_ms=500,
                 fetch_max_bytes=52428800,
                 max_partition_fetch_bytes=1 * 1024 * 1024,
                 request_timeout_ms=40 * 1000,
                 auto_offset_reset='latest',
                 enable_auto_commit=True,
                 auto_commit_interval_ms=5000,
                 check_crcs=True,
                 metadata_max_age_ms=5 * 60 * 1000,
                 heartbeat_interval_ms=3000,
                 session_timeout_ms=30000,
                 exclude_internal_topics=True,
                 connections_max_idle_ms=540000):
        self.loop = loop
        self.fetch_max_wait_ms = fetch_max_wait_ms
        self.fetch_max_bytes = fetch_max_bytes
        self.max_partition_fetch_bytes = max_partition_fetch_bytes
        self.request_timeout_ms = request_timeout_ms
        self.auto_offset_reset = auto_offset_reset
        self.check_crcs = check_crcs
        self.enable_auto_commit = enable_auto_commit
        self.metadata_max_age_ms = metadata_max_age_ms
        self.auto_commit_interval_ms = auto_commit_interval_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.session_timeout_ms = session_timeout_ms
        self.exclude_internal_topics = exclude_internal_topics
        self.connections_max_idle_ms = connections_max_idle_ms

    async def _seek_offsets(self, consumer, topics, offsets):
        for topic in topics:
            parts = consumer.partitions_for_topic(topic)
            for part in parts:
                tp = TopicPartition(topic, part)
                max_offsets = await consumer.end_offsets([tp])
                max_offsets = max_offsets[tp]
                if 0 > offsets or offsets > max_offsets:
                    raise ValueError("offsets out of range of value")
                consumer.seek(tp, offsets)

    async def _do_some_work(self, work, topics, group_id, offsets, listener, bootstrap_servers, enable_commit, **kwargs):
        consumer = AIOKafkaConsumer(loop=self.loop,
                                    bootstrap_servers=bootstrap_servers,
                                    group_id=group_id,
                                    fetch_max_wait_ms=self.fetch_max_wait_ms,
                                    max_partition_fetch_bytes=self.max_partition_fetch_bytes,
                                    request_timeout_ms=self.request_timeout_ms,
                                    auto_offset_reset=self.auto_offset_reset,
                                    enable_auto_commit=self.enable_auto_commit,
                                    auto_commit_interval_ms=self.auto_commit_interval_ms,
                                    check_crcs=self.check_crcs,
                                    metadata_max_age_ms=self.metadata_max_age_ms,
                                    heartbeat_interval_ms=self.heartbeat_interval_ms,
                                    session_timeout_ms=self.session_timeout_ms,
                                    exclude_internal_topics=self.exclude_internal_topics,
                                    connections_max_idle_ms=self.connections_max_idle_ms,
                                    **kwargs)
        consumer.subscribe(topics=topics, listener=listener)
        await consumer.start()
        if offsets is not None:
            await self._seek_offsets(consumer, topics, offsets)
        try:
            async for msg in consumer:
                try:
                    if msg is None:
                        continue
                    await work(msg)
                    if enable_commit:
                        meta = "Some utf-8 metadata"
                        tp = TopicPartition(msg.topic, msg.partition)
                        offsets = {tp: OffsetAndMetadata(msg.offset + 1, meta)}
                        await consumer.commit(offsets)
                except OffsetOutOfRangeError as err:
                    tps = err.args[0].keys()
                    await consumer.seek_to_beginning(*tps)
                    continue
                except Exception as e:
                    root_logger.error(f'{traceback.format_exc()}')
                    continue
        except Exception as e:
            raise e
        finally:
            await consumer.stop()

    def add_task(self, work, topics, group_id=str(uuid.uuid1()), callback=None, offsets=None, listener=default_listener, bootstrap_servers='172.31.10.78:9092', enable_commit=True, **kwargs):
        mwork = self._do_some_work(work, topics, group_id=group_id, offsets=offsets, listener=listener, bootstrap_servers=bootstrap_servers, enable_commit=enable_commit, **kwargs)
        task = asyncio.ensure_future(mwork, loop=loop)
        if callback is not None:
            task.add_done_callback(callback)

    def run(self):
        print("KafkaConsumer start runing")
        loop.run_forever()
