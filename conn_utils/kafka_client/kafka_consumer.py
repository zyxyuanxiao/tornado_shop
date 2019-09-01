# coding:utf-8
from akpi.conn_tools.kafka_conn.asyn_consumer import KafkaConsumer
from conn_utils.mysql_conn.mysql_conn_pool import MysqlConn
from conf import system_config
import uuid
import asyncio

class KafkaSubscriber(object):

    group_id_dict = {
        "UNIQUE_GROUP": 0,
    }

    def __init__(self, group_id,  kf_type='installment', auto_offset_reset='earliest'):
        """
        :param kf_type: 'installment' means business kafka, it's the default
                        'risk' means risk control kafka
                        'auto_offset_reset':  earliest means If committed offset not found, start rom beginnig
                                              latest means if If committed offset not found, start rom latest
        """
        if type(group_id) != str:
                raise TypeError("group_type must be str")

        self._group_id = group_id
        self._kf_type = kf_type
        self._consumer = None
        self._auto_offset_reset = auto_offset_reset
        self._bootstrap_server_host = system_config.get_kafka_bootstrap_server(kf_type)
        if group_id == "UNIQUE_GROUP":
            self._group_id = str(uuid.uuid1())
        # if group_id == 'test':
        #     self._group_id = group_id
        #     self._topics = ['installmentdb_t_cash_loan', 'installmentdbBillMerge', 'installmentdb_t_purchase_order']
        else:
            with MysqlConn() as conn:
                row = conn.select_one(
                    "select group_id, topics from midatadb.r_kafka_consumer_group where group_id = %s limit 1",
                    [group_id])
                if row is None:
                    raise ValueError("group_id unsurported")
                self._group_id = row["group_id"]
                self._topics = str(row["topics"]).split(",")

    def start(self, work, loop=None, offsets=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._consumer = KafkaConsumer(loop, auto_offset_reset=self._auto_offset_reset)
        self._consumer.add_task(work, self._topics, group_id=self._group_id, offsets=offsets,
                                bootstrap_servers=self._bootstrap_server_host)
        self._consumer.run()

    def stop(self):
        self._consumer.loop.stop()
