#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio


class MysqlConn(object):
    pool = None

    def __init__(self, loop=asyncio.get_event_loop()):
        self._conn = None
        self._cur = None

    async def select_one(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                return await self._cur.fetchone()
            else:
                return None
        except Exception as e:
            await self.rollback()
            raise e

    async def select_one_value(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                result = await self._cur.fetchone()
                return list(result.values())[0]
            else:
                return None
        except Exception as e:
            await self.rollback()
            raise e

    async def select_many(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                return await self._cur.fetchall()
            else:
                return []
        except Exception as e:
            await self.rollback()
            raise e

    async def select_many_one_value(self, sql, params=None):
        try:
            count = await self._cur.execute(sql, params)
            if count > 0:
                result = await self._cur.fetchall()
                return list(map(lambda one: list(one.values())[0], result))
            else:
                return []
        except Exception as e:
            await self.rollback()
            raise e

    async def insert_one(self, sql, params=None, return_auto_increament_id=False):
        try:
            await self._cur.execute(sql, params)
            if return_auto_increament_id:
                return self._cur.lastrowid
        except Exception as e:
            await self.rollback()
            raise e

    async def insert_many(self, sql, params):
        try:
            count = await self._cur.executemany(sql, params)
            return count
        except Exception as e:
            await self.rollback()
            raise e

    async def update(self, sql, params=None):
        try:
            result = await self._cur.execute(sql, params)
            return result
        except Exception as e:
            await self.rollback()
            raise e

    async def delete(self, sql, params=None):
        try:
            result = await self._cur.execute(sql, params)
            return result
        except Exception as e:
            await self.rollback()
            raise e

    async def begin(self):
        await self._conn.begin()

    async def commit(self):
        try:
            await self._conn.commit()
        except Exception as e:
            await self.rollback()
            raise e

    async def rollback(self):
        await self._conn.rollback()

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()

    def executed(self):
        return self._cur._executed

    async def __aenter__(self):
        self._conn = await self.pool.acquire()
        self._cur = await self._conn.cursor()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.commit()
            await self._cur.close()
        except Exception as e:
            raise e
        finally:
            await self.pool.release(self._conn)

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

    async def count(self, field='1', **kwargs):
        sql = f'SELECT COUNT({field}) FROM {kwargs["table"]}'
        # join_tables
        if "join_tables" in kwargs and kwargs['join_tables'] and isinstance(kwargs["join_tables"], list):
            for join in kwargs["join_tables"]:
                join_sqls = {f'{u} on {join[u]} 'for u in join}
                sql = f'{sql} {"".join(join_sqls)}'
        kwargs["sql"] = sql
        sql, values = self.__join_where(**kwargs)
        result = await self.select_one_value(sql, values)
        return result if result else 0

    async def exist(self, **kwargs):
        return await self.count(**kwargs) > 0

    async def add(self, return_auto_increament_id=False, **kwargs):
        result = 0
        if 'table' in kwargs and 'data' in kwargs:
            if kwargs['data'] and isinstance(kwargs['data'], dict):
                fields = ','.join(k for k in kwargs["data"].keys())
                values = ','.join(("%s", ) * len(kwargs["data"]))
                sql = f'INSERT INTO {kwargs["table"]} ({fields}) VALUES ({values})'
                result = await self.insert_one(sql, tuple(kwargs["data"].values()), return_auto_increament_id)
            elif kwargs['data'] and isinstance(kwargs['data'], (list, set)):
                fields = ','.join(k for k in kwargs["data"][0].keys())
                values = ','.join(("%s", ) * len(kwargs["data"][0]))
                sql = f'INSERT INTO {kwargs["table"]} ({fields}) VALUES ({values})'
                values_list = []
                for data in kwargs["data"]:
                    values_list.append(list(data.values()))
                result = await self.insert_many(sql, values_list)
        return result

    async def updated(self, **kwargs):
        if kwargs["data"] and "table" in kwargs:
            if kwargs.get("params", 0) == 0:
                kwargs["params"] = {}
            fields = ','.join(k + '=%s' for k in kwargs["data"].keys())
            values = list(kwargs["data"].values())

            sql = f"UPDATE {kwargs['table']} SET {fields}"
            kwargs["sql"] = sql
            sql, where_values = self.__join_where(**kwargs)
            values.extend(where_values)
            return await self.update(sql, values)
        else:
            return 0

    async def deleted(self, **kwargs):
        if "table" in kwargs:
            if kwargs.get("params", 0) == 0:
                kwargs["params"] = {}
            sql = f"DELETE FROM {kwargs['table']}"
            kwargs["sql"] = sql
            sql, values = self.__join_where(**kwargs)
            return await self.delete(sql, values)
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

    async def __query(self, **kwargs):
        sql, values = self._apply_kwargs(**kwargs)
        if values:
            return await self.select_many(sql, values)
        else:
            return await self.select_many(sql)

    async def all(self, **kwargs):
        return await self.__query(**kwargs)

    async def one(self, **kwargs):
        kwargs["limit"] = 1
        kwargs["one"] = 1
        return await self.__query(**kwargs)
