#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (python) ,2018-2036, AKULAKU Tech. Co,Ltd
# @Author: YouShaoPing
# @Version: 0.0.1
# @Date:   2018-11-18 17:04:18
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2018-12-06 18:07:38
import redis


class RedisSubscriber(object):
    """
    Redis频道订阅辅助类
    """
    # def __new__(cls):
    #     # 单列
    #     if not hasattr(cls, 'instance'):
    #         cls.instance = super(RedisSubscriber, cls).__new__(cls)
    #     return cls.instance

    def __init__(self, host, port, password, channel, socket_keepalive=True, socket_timeout=300):
        self.conn = redis.StrictRedis(host=host, port=port, password=password, socket_keepalive=socket_keepalive,
                                      socket_timeout=socket_timeout)
        self.channel = channel    # 频道名称

    def subscribe(self):
        """
        订阅方法
        """
        pub = self.conn.pubsub()
        pub.subscribe(self.channel)  # 同时订阅多个频道，要用psubscribe
        pub.parse_response()
        return pub

    def public(self, msg):
        """
        发布消息
        """
        self.conn.publish(self.channel, msg)
        return True
