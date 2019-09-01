#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (python) ,2018-2036, AKULAKU Tech. Co,Ltd
# @Author: YouShaoPing
# @Version: 0.0.1
# @Date:   2018-11-30 18:05:10
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2019-03-06 16:30:41

import logging
import pymysql
import traceback
from pymysql.cursors import DictCursor


class PyMysqlConn(object):
    conn = None

    @classmethod
    def create_conn(cls, **kwargs):
        try:
            kwargs["cursorclass"] = DictCursor
            cls.conn = pymysql.connect(**kwargs)
            print(cls.conn)
        except Exception as e:
            print(traceback.format_exc())
            print("connect mysql error")
            raise e

    def __init__(self):
        self._cur = None

    def __get_cursor(self):
        if PyMysqlConn.conn is None:
            raise AttributeError("please create_conn first")
        if self._cur is None:
            self._cur = PyMysqlConn.conn.cursor()
        return self._cur

    def close(self):
        if self._cur is not None:
            self._cur.close()
        if PyMysqlConn.conn is not None:
            PyMysqlConn.conn.close()
        return True

    def commit(self):
        return PyMysqlConn.conn.commit()

    def rollback(self):
        return PyMysqlConn.conn.rollback()

    def select_one(self, sql, params=None):
        cursor = self.__get_cursor()
        count = cursor.execute(sql, params)
        if count > 0:
            return cursor.fetchone()
        else:
            return None

    def select_one_value(self, sql, params=None):
        cursor = self.__get_cursor()
        count = cursor.execute(sql, params)
        if count > 0:
            result = cursor.fetchone()
            return list(result.values())[0]
        else:
            return None

    def select_many(self, sql, params=None):
        cursor = self.__get_cursor()
        count = cursor.executemany(sql, params)
        if count > 0:
            return cursor.fetchall()
        else:
            return []

    def select_many_one_value(self, sql, params=None):
        cursor = self.__get_cursor()
        count = cursor.executemany(sql, params)
        if count > 0:
            result = cursor.fetchall()
            return list(map(lambda one: list(one.values())[0], result))
        else:
            return []

    def insert_one(self, sql, params=None, return_auto_increament_id=False):
        cursor = self.__get_cursor()
        cursor.execute(sql, params)
        if return_auto_increament_id:
            return cursor.lastrowid

    def insert_many(self, sql, params):
        cursor = self.__get_cursor()
        count = cursor.executemany(sql, params)
        return count

    def update(self, sql, param=None):
        cursor = self.__get_cursor()
        return cursor.execute(sql, param)

    def delete(self, sql, param=None):
        cursor = self.__get_cursor()
        return cursor.execute(sql, param)

    def executed(self):
        cursor = self.__get_cursor()
        return cursor._executed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logging.error(exc_type)
            logging.error(exc_val)
            logging.error(traceback.format_exc())
            logging.error(self.executed())
        self.commit()
        self.close()
        return False

    def __to_params(self, **kwargs):
        keys = [k for k in kwargs["params"].keys()]
        _keys = ['%s' for k in kwargs["params"].keys()]
        return keys, _keys

    def __to_filters(self, filters, operator='AND'):
        if not isinstance(filters, list):
            return ''
        filters_sql = ''
        filter_values = []
        for argument_filter in filters:
            # Resolve right attribute
            if "val" in argument_filter.keys():
                right = argument_filter["val"]
            elif "value" in argument_filter.keys():
                right = argument_filter["value"]
            elif "values" in argument_filter.keys():
                right = argument_filter["values"]
            else:
                right = None
            # Operator
            op = argument_filter["op"].upper()
            if op in ["LIKE"]:
                right = f"'%%{right}%%'"
            # Resolve left attribute
            if "key" not in argument_filter:
                raise BaseException("Missing field_key attribute 'key'")
            # Operators from flask-restless
            left = argument_filter["key"]
            if op in ["IS NULL", "IS NOT NULL"]:
                filters_sql = f'{filters_sql} {operator} {left} {op}'
            # right is [start,end]
            elif op in ["BETWEEN", "NOT BETWEEN"]:
                filter_values.append(right[0], right[1])
                right = ['%s', '%s']
                filters_sql = f'{filters_sql} {operator} {left} {op} %s AND %s '
            elif op in ["CONTAINS"]:
                filters_sql = f'{filters_sql} {operator} {op}({left}, {right})'
            elif op in ["NOT EXISTS"]:
                filters_sql = f'{filters_sql} {operator} {op} {left}'
            elif op in ["EXTRA"]:
                if isinstance(left, list):
                    for sql_str in left:
                        filters_sql = f'{filters_sql} {operator} {sql_str}'
                else:
                    filters_sql = f'{filters_sql} {operator} {left}'
            # Raise Exception
            # op in ["is", "is_not", "==", "!=", ">", "<", ">=", "<=", ""]
            # [like, not like, in , not in, ]
            else:
                filter_values.append(right)
                filters_sql = f'{filters_sql} and {left} {op} %s'
        return filters_sql[4:], filter_values

    def __join_where(self, **kwargs):
        where_flag = 1
        filter_values = []
        if "params"in kwargs and kwargs["params"]:
            keys, _keys = self.__to_params(**kwargs)
            where = ' AND '.join(k + '=' + _k for k, _k in zip(keys, _keys))
            if where_flag:
                kwargs["sql"] = f'{kwargs["sql"]} WHERE {where}'
                where_flag = 0
            else:
                kwargs["sql"] = f'{kwargs["sql"]} AND {where}'
        if "or_params" in kwargs and kwargs["or_params"]:
            keys, _keys = self.__to_params(**kwargs)
            where = ' OR '.join(k + '=' + _k for k, _k in zip(keys, _keys))
            if where_flag:
                kwargs["sql"] = f'{kwargs["sql"]} WHERE {where}'
                where_flag = 0
            else:
                kwargs["sql"] = f'{kwargs["sql"]} OR {where}'

        if "filters" in kwargs and kwargs['filters']:
            filters_sql, and_values = self.__to_filters(kwargs["filters"])
            filter_values = filter_values + and_values
            if where_flag:
                kwargs["sql"] = f'{kwargs["sql"]} WHERE {filters_sql}'
                where_flag = 0
            else:
                kwargs["sql"] = f'{kwargs["sql"]} AND {filters_sql}'

        if 'or_filters' in kwargs and kwargs['or_filters']:
            argument_filters = kwargs.pop('or_filters')
            filters_sql, or_values = self.__to_filters(argument_filters, "OR")
            filter_values = filter_values + or_values
            if where_flag:
                kwargs["sql"] = f'{kwargs["sql"]} WHERE {filters_sql}'
                where_flag = 0
            else:
                kwargs["sql"] = f'{kwargs["sql"]} OR {filters_sql}'
        if 'params' in kwargs or 'or_params' in kwargs:
            params_values = list(kwargs['params'].values()) if 'params' in kwargs else []
            or_params_values = list(kwargs['or_params'].values()) if 'or_params' in kwargs else []
            filter_values = params_values + or_params_values + filter_values
        return kwargs["sql"], filter_values

    def count(self, field='1', **kwargs):
        sql = f'SELECT COUNT({field}) FROM {kwargs["table"]}'
        # join_tables
        if "join_tables" in kwargs and kwargs['join_tables'] and isinstance(kwargs["join_tables"], list):
            for join in kwargs["join_tables"]:
                join_sqls = {f'{u} on {join[u]} 'for u in join}
                sql = f'{sql} {"".join(join_sqls)}'
        kwargs["sql"] = sql
        sql, values = self.__join_where(**kwargs)
        cursor = self.__get_cursor()
        result = cursor.execute(sql, values)
        return result[0] if result else 0

    def exist(self, **kwargs):
        return self.count(**kwargs) > 0

    def add(self, **kwargs):
        result = 0
        cursor = self.__get_cursor()
        if 'table' in kwargs and 'data' in kwargs:
            if kwargs['data'] and isinstance(kwargs['data'], dict):
                fields = ','.join(k for k in kwargs["data"].keys())
                values = ','.join(("%s", ) * len(kwargs["data"]))
                sql = f'INSERT INTO {kwargs["table"]} ({fields}) VALUES ({values})'
                result = cursor.execute(sql, tuple(kwargs["data"].values()))
            elif kwargs['data'] and isinstance(kwargs['data'], (list, set)):
                fields = ','.join(k for k in kwargs["data"][0].keys())
                values = ','.join(("%s", ) * len(kwargs["data"][0]))
                sql = f'INSERT INTO {kwargs["table"]} ({fields}) VALUES ({values})'
                values_list = []
                for data in kwargs["data"]:
                    values_list.append(list(data.values()))
                result = cursor.executemany(sql, values_list)
        else:
            return result

    def updated(self, **kwargs):
        if kwargs["data"] and "table" in kwargs:
            if kwargs.get("params", 0) == 0:
                kwargs["params"] = {}
            fields = ','.join('`' + k + '`=%s' for k in kwargs["data"].keys())
            values = list(kwargs["data"].values())

            values.extend(list(kwargs["params"].values()))
            sql = f"UPDATE `{kwargs['table']}` SET {fields}"
            kwargs["sql"] = sql
            sql, where_values = self.__join_where(**kwargs)
            cursor = self.__get_cursor()
            values.extend(where_values)
            cursor.execute(sql, tuple(values))
            return cursor.rowcount
        else:
            return 0

    def deleted(self, **kwargs):
        if "table" in kwargs:
            if kwargs.get("params", 0) == 0:
                kwargs["params"] = {}
            sql = f"DELETE FROM `{kwargs['table']}`"
            kwargs["sql"] = sql
            sql, values = self.__join_where(**kwargs)
            cursor = self.__get_cursor()
            cursor.execute(sql, values)
            return cursor.rowcount
        else:
            return 0

    def _apply_kwargs(self, **kwargs):
        sql = 'SELECT '
        if "table" not in kwargs:
            return False
        # fields
        if 'fields' in kwargs and kwargs['fields'] and isinstance(kwargs['fields'], (list, set)):
            sql += ','.join(field for field in kwargs['fields'])
        else:
            sql += ' * '
        # table
        sql = f'{sql} FROM {kwargs["table"]}'

        # join_tables
        if "join_tables" in kwargs and kwargs['join_tables'] and isinstance(kwargs["join_tables"], list):
            for join in kwargs["join_tables"]:
                join_sqls = {f'{u} on {join[u]} 'for u in join}
                sql = f'{sql} {"".join(join_sqls)}'

        kwargs["sql"] = sql
        # where
        sql, values = self.__join_where(**kwargs)

        # group by
        if 'group' in kwargs:
            sql = f'{sql} GROUP BY {kwargs["group"]}'
        # having
        if 'having' in kwargs:
            sql = f'{sql} HAVING {kwargs["having"]["key"]} {kwargs["having"]["op"]} {kwargs["having"]["value"]}'
        # order by
        if 'order' in kwargs:
            sql = f'{sql} ORDER BY {kwargs["order"]}'
        # limit
        if 'limit' in kwargs:
            sql = f'{sql} LIMIT %s'
            values.append(kwargs['limit'])
        # offset
        if 'offset' in kwargs:
            sql = f'{sql} OFFSET %s'
            values.append(kwargs['offset'])
        return sql, values

    def __query(self, **kwargs):
        sql, values = self._apply_kwargs(**kwargs)
        cursor = self.__get_cursor()
        if values:
            cursor.execute(sql, values)
        else:
            cursor.execute(sql)
        if kwargs.get("one", 0) == 0:
            rows = cursor.fetchall()
        else:
            rows = cursor.fetchone()
        return rows

    def all(self, **kwargs):
        return self.__query(**kwargs)

    def one(self, **kwargs):
        kwargs["limit"] = 1
        kwargs["one"] = 1
        return self.__query(**kwargs)
