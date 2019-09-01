#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import logging
from confluent_kafka import Producer

root_logger = logging.getLogger()


class KafkaProducer(object):

    def __init__(self, cluster):
        self.producer = Producer({'bootstrap.servers': cluster, "log.connection.close": False})

    def send(self, topic, key, value, data_type=None, uid=None, country_id=None):
        try:
            data = {"data": value, "createTime": int(time.time() * 1000)}
            if data_type is not None:
                data["type"] = int(data_type)
            if uid is not None:
                data["uid"] = int(uid)
            if country_id is not None:
                data["country_id"] = country_id
            self.producer.produce(topic, json.dumps(data), key)
            self.producer.poll(0)
        except BufferError as e:
            root_logger.error("""%% Local producer queue is full (%d messages awaiting delivery): try again\n""" % len(self.producer))
            raise e

    def send_raw(self, topic, key, value):
        try:
            data = {"data": value, "createTime": int(time.time() * 1000)}
            self.producer.produce(topic, json.dumps(data), key)
            self.producer.poll(0)
        except BufferError as e:
            root_logger.error("""%% Local producer queue is full (%d messages awaiting delivery): try again\n""" % len(self.producer))
            raise e

    def flush(self):
        self.producer.flush()
