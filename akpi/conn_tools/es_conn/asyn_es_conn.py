#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (python) ,2018-2036, AKULAKU Tech. Co,Ltd
# @Author: YouShaoPing
# @Version: 0.0.1
# @Date:   2018-12-06 09:33:27
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2018-12-07 10:55:41
import asyncio
from elasticsearch_async import AsyncElasticsearch
from akpi.conn_tools.es_conn.helpers import bulk, scan


class AsynEsConn(object):
    """docstring for AsynEsConn"""

    def __init__(self, hosts=None, retry_on_timeout=True, **kwargs):
        self.loop = asyncio.get_event_loop() if kwargs.get("loop", 0) == 0 else kwargs['loop']
        self.es_conn = AsyncElasticsearch(hosts, **kwargs)

    def conn(self):
        return self.es_conn

    async def ping(self, **query_params):
        return await self.es_conn.ping(**query_params)

    async def info(self, **query_params):
        return await self.es_conn.info(**query_params)

    async def create(self, index, doc_type, id, body, **query_params):
        return await self.es_conn.create(index=index, doc_type=doc_type, id=id, body=body, **query_params)

    async def index(self, index, doc_type, body, id=None, **query_params):
        return await self.es_conn.index(index=index, doc_type=doc_type, body=body, id=id, **query_params)

    def build_aggs_search_body(self, must_conditions, aggs_result):
        """
        build search have aggs
        :param must_conditions: must conditions
        :param aggs_result: aggs conditions
        :return: search body
        """
        search_body = {
            "size": 0,
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
            "aggs": aggs_result
        }
        return search_body

    @staticmethod
    def build_must_terms(objs):
        """
        build must term
        :param objs: must term obj
        :return: aggs body
        """
        if len(objs) > 0:
            tmp = []
            for (k, v) in objs.items():
                obj = dict()
                obj[k] = v
                obj_term = dict()
                obj_term['term'] = obj
                tmp.append(obj_term)
        return tmp

    @staticmethod
    def build_normal_aggs(aggs_field):
        """
        build normal aggs
        :param aggs_field: aggs field
        :return: aggs body
        """
        tmp = dict()
        aggs_result = dict()
        terms = dict()
        terms["field"] = aggs_field
        terms["size"] = 0
        order = dict()
        order["_count"] = "desc"
        terms["order"] = order
        aggs_result["terms"] = terms
        tmp["aggs_result"] = aggs_result
        return tmp

    @staticmethod
    def build_cardinality_aggs(cardinality_field):
        """
        build aggs of cardinality
        :param cardinality_field: field to compute cardinality
        :return: aggs body
        """
        cardinality_body = {
            "distinct_msgId": {
                "cardinality": {
                    "field": cardinality_field
                }
            }
        }
        return cardinality_body

    @staticmethod
    def build_count_aggs(count_field):
        """
        build aggs of value_count
        :param value_count: field to compute value_count
        :return: aggs body
        """
        value_count_body = {
            "count_of_field": {
                "value_count": {
                    "field": count_field
                }
            }
        }
        return value_count_body

    @staticmethod
    def build_min_aggs(min_field):
        """
        build aggs of min
        :param cardinality_field: field to compute min value
        :return: aggs body
        """
        min_body = {
            "min_sms_time": {"min": {"field": min_field}}
        }
        return min_body

    def build_sms_cnt_search_body(self, uid, device_id):
        """
        build simple search body for sms count
        :param uid: uid , need to be long value
        :param device_id:  device_id, need to be string
        :return: search body
        """
        must_conditions = dict()
        must_conditions['data.uid'] = uid
        must_conditions['data.deviceId.keyword'] = device_id
        must_terms = self.build_must_terms(must_conditions)
        aggs_result = dict()
        aggs_result.update(self.build_count_aggs('data.sms.msgId.keyword'))
        aggs_result.update(self.build_min_aggs('data.sms.msgTime'))
        search_body = self.build_aggs_search_body(must_terms, aggs_result)
        return search_body

    def build_user_phoneNumber_changelist_body(self, uid):
        search_body = {
            "_source": {
                "includes": [
                    "data.phoneNumber"
                ]
            },
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "data.uid": uid
                            }
                        },
                        {
                            "exists": {
                                "field": "data.phoneNumber"
                            }
                        }
                    ],
                    "must_not": [],
                    "should": []
                }
            }
        }
        return search_body

    @staticmethod
    def build_password_changelist_body(uid):
        search_body = {
            "_source": {
                "includes": [
                    "data.loginPwd"
                ]
            },
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "uid": uid
                            }
                        },
                        {
                            "exists": {
                                "field": "data.loginPwd"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "password": {
                    "terms": {
                        "field": "data.loginPwd.keyword",
                        "size": 1000
                    }
                }
            }
        }
        return search_body

    async def search(self, index=None, doc_type=None, body=None, **query_params):
        return await self.es_conn.search(index=index, doc_type=doc_type, body=body, **query_params)

    async def search_multi(self, index, query_body, doc_type="doc", scroll='5m', preserve_order=True):
        data = []
        async for e in scan(self.es_connection.es_conn, index=index, doc_type=doc_type, query=query_body,
                            scroll=scroll, preserve_order=preserve_order):
            data.append(e)
        return data

    async def save_bulk_data(self, actions, stats_only=False, *args, **kwargs):
        return await bulk(self.es_conn, actions=actions, stats_only=stats_only, *args, **kwargs)

    def __del__(self):
        self.loop.run_until_complete(self.es_conn.transport.close())
