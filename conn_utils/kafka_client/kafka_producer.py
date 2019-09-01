import json
import logging
import time
from confluent_kafka import Producer
from conf import system_config

logger = logging.getLogger()


class KafkaProducer(object):
    """ Compatible with the original class singleton. Give the methods a param
    kf_type, default is 'installment', if you want to send to risk control
    kafka cluster, pass it a sting 'risk'."""

    installment_cluster = system_config.get_kafka_bootstrap_server()
    inst_producer = Producer({'bootstrap.servers': installment_cluster, "log.connection.close":False})

    risk_cluster = system_config.get_kafka_bootstrap_server('risk')
    risk_producer = Producer({'bootstrap.servers': risk_cluster, "log.connection.close":False})

    def __init__(self):
        pass

    @classmethod
    def send(cls, topic, key, value, data_type=None, uid=None, country_id=None, kf_type='installment'):
        """
        :param kf_type: 'installment', means business kafka cluster
                        'risk', means risk control kafka cluster
        """
        if kf_type == 'risk':
            producer = cls.risk_producer
        else:
            producer = cls.inst_producer
        try:
            data = {"data": value, "createTime": int(time.time() * 1000)}
            if data_type is not None:
                data["type"] = int(data_type)
            if uid is not None:
                data["uid"] = int(uid)
            if country_id is not None:
                data["country_id"] = country_id
            producer.produce(topic, json.dumps(data), key)
            producer.poll(0)
        except BufferError as e:
            logger.error("""%% Local producer queue is full (%d messages awaiting delivery): try again\n""" % len(producer))
            raise e

    @classmethod
    def send_raw(cls, topic, key, value, kf_type='installment'):
        if kf_type == 'risk':
            producer = cls.risk_producer
        else:
            producer = cls.inst_producer
        try:
            data = {"data": value, "createTime": int(time.time() * 1000)}
            producer.produce(topic, json.dumps(data), key)
            producer.poll(0)
        except BufferError as e:
            logger.error("""%% Local producer queue is full (%d messages awaiting delivery): try again\n""" % len(producer))
            raise e

    @classmethod
    def send_byte(cls, topic, key, value, kf_type='installment'):
        if kf_type == 'risk':
            producer = cls.risk_producer
        else:
            producer = cls.inst_producer
        try:
            value.update({"createTime": int(time.time() * 1000)})
            producer.produce(topic, json.dumps(value), partition=int(key)%10)
            producer.poll(0)
        except BufferError as e:
            logger.error("""%% Local producer queue is full (%d messages awaiting delivery): try again\n""" % len(producer))
            raise e

    @classmethod
    def flush(cls, kf_type='installment'):
        if kf_type == 'risk':
            producer = cls.risk_producer
        else:
            producer = cls.inst_producer
        producer.flush()


if __name__ == "__main__":
    i = 0
    #while True:
        #KafkaProducer.send("neo4jInsert", "key" + str(i), "value" + str(i))
        #time.sleep(5)
    KafkaProducer.flush()
