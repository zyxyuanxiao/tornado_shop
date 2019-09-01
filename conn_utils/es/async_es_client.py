# coding:utf=8
from akpi.conn_tools.es_conn.asyn_es_conn import AsynEsConn
from system_config import system_config
from risk_platform.es import helpers
import logging
import asyncio


ES_HOST = system_config.get_es_host()
ES_PORT = system_config.get_es_port()

logging.info("ElasticSearch Server host:%s, port:%s.", ES_HOST, ES_PORT)


class ESSearch(object):
    _es_conns = {}

    def __init__(self, host=ES_HOST, port=ES_PORT):
        if (host, port) not in ESSearch._es_conns:
            ESSearch._es_conns[(host, port)] = AsynEsConn(host=host, port=port, timeout=60, max_retries=10,
                                                          retry_on_timeout=True)
        self.es_connection = ESSearch._es_conns[(host, port)]

    async def search(self, index, doc_type, query_body):
        """
        build simple search and result
        :param index: index name
        :param doc_type: doc_type
        :param query_body: dsl query_body
        :return: search result
        """
        ret = await self.es_connection.search(index=index, doc_type=doc_type, body=query_body)
        return ret

    async def scroll(self, index, query_body, scroll_id, scroll='5m'):
        if scroll_id is None:
            resp = await self.es_connection.search(index=index, body=query_body, scroll=scroll, doc_type='doc')
            scroll_id = resp.get('_scroll_id')
            if scroll_id is None:
                return
        scroll_kwargs = {}
        #logging.info('es scroll id %s' % scroll_id)
        ret = await self.es_connection.es_conn.scroll(scroll_id, scroll=scroll, **scroll_kwargs)
        if ret["_shards"]["successful"] < ret["_shards"]["total"]:
            raise Exception(
                scroll_id,
                'Scroll request has only succeeded on %d shards out of %d.' %
                (ret['_shards']['successful'], ret['_shards']['total'])
            )
        return ret

    async def clear_scroll(self, scroll_id):
        await self.es_connection.es_conn.clear_scroll(body={'scroll_id': [scroll_id]}, ignore=(404,))

    async def search_multi(self, index, query_body, doc_type="doc", scroll='5m', preserve_order=True):
        data = []
        async for e in helpers.scan(self.es_connection.es_conn, index=index, doc_type=doc_type, query=query_body,
                                    scroll=scroll, preserve_order=preserve_order):
            data.append(e)
        return data

    async def sql_query(self, sql_query_body):
        """
        test es sql
        :param sql_query_body: type dict, es sql
        :return: 
        """
        ret = await self.es_connection.conn().transport.perform_request('POST', '/_xpack/sql', params={'format': 'json'},
                                                                        body=sql_query_body)
        return ret

    async def sql_tranlate(self, sql_query_body):
        ret = await self.es_connection.conn().transport.perform_request('POST', '/_xpack/sql/translate',
                                                                        body=sql_query_body)
        return ret

    """
    build simple search and result
    :param index: index name
    :param doc_type: doc_type
    :param query_body: dsl query_body
    :scroll: Specify how long a consistent view of the index should be maintained for scrolled search
    :preserve_order: whether perserve search order
    :return: search result
    """
    # async_conn def search_multi(self, index, query_body, doc_type="doc", scroll='5m', preserve_order=True):
    #
    #     data = await self.es_connection.search(index=index, doc_type=doc_type, body=query_body,
    #                                            scroll=scroll, preserve_order=preserve_order)
    #     return [e for e in data]

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
    def build_must_terms(terms_dict):
        """
        build must term
        :param terms_dict: must term obj
        :return: aggs body
        """
        tmp = []
        if len(terms_dict) > 0:
            for (k, v) in terms_dict.items():
                obj = dict()
                obj[k] = v
                obj_term = dict()
                obj_term['term'] = obj
                tmp.append(obj_term)
        return tmp

    @staticmethod
    def build_normal_aggs(aggs_field_seq):
        """
        build normal aggs
        :param aggs_field_seq: aggs field
        :return: aggs body
        """
        tmp = dict()
        aggs_result = dict()
        terms = dict()
        terms["field"] = aggs_field_seq
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
        :param : value_count field to compute value_count
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
        :param : cardinality_field field to compute min value
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

    def build_user_sms_hit_sensitive_words(self, uid, ak_phone, sns_words,
                                           start_timestamp):
        """
        build query body of counting sensitive words occured in user's
        phone messages.

        :param uid: uid
        :param ak_phone: phone number of akulaku company, it will be excluded
        :param sns_words: sensitive words list, like: ['vay tiền', 'repayment']
        :param start_timestamp: the start timestamp that will be filtered out.
        """
        search_body = {
            "size": 0,
            "query": {
              "bool": {
                "must": [
                  {
                    "match": {
                    "data.uid": uid
                    }
                  },
                  {
                    "bool": {
                      "should": [
                      ]
                    }
                  }
                ],
                "must_not": [
                  {"match": {
                    "data.sms.msgAddr": ak_phone
                  }}
                ],
                "filter": {
                    "range": {
                      "data.sms.msgTime" : {"gt": start_timestamp}
                    }
                }
              }
            }
        }
        should = search_body['query']['bool']['must'][1]['bool']['should']
        for words in sns_words:
            match_dict = {'match_phrase': {"data.sms.msgBody": words}}
            should.append(match_dict)
        return search_body

    @staticmethod
    def build_password_changelist_body(uid):
        search_body =  {
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

    @staticmethod
    def build_app_list_body(uid):
        search_body = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "uid.keyword": str(uid)
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "app_name": {
                    "terms": {
                        "size": 10000,
                        "field": "app_name.keyword"
                    }
                }
            }
        }
        return search_body


async def query():
    es = ESSearch()
    uid = 15198785
    body = {
        "_source": {
            "includes": [
                "package_name"
            ]
        },
        "aggs": {
            "1": {
                "terms": {
                    "field": "package_name.keyword",
                    "size": 200000
                }
            }
        },
        "query": {
            "bool": {
                "must": [
                    {
                        "match_all": {}
                    },
                    {
                        "match_phrase": {
                            "uid": {
                                "query": str(uid)
                            }
                        }
                    }
                ],
                "filter": [],
                "should": [],
                "must_not": []
            }
        }
    }

    es_data = await es.search(index="dw_user_app_info*", doc_type="doc", query_body=body)
    print(es_data)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(query())
#    loop.close()
