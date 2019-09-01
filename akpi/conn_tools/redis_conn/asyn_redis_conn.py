# -*- coding: utf-8 -*-
# @Author: YouShaoPing
# @Date:   2019-01-21 09:53:07
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2019-01-28 11:16:16
import math
import time
import uuid
import logging
import asyncio
import traceback
from aredis import StrictRedis

logger = logging.getLogger()


def _normalize_startup_nodes(startup_nodes):
    result = []
    startup_nodes = startup_nodes.split(',')
    for node in startup_nodes:
        node = node.split(':')
        result.append({'host': node[0], 'port': node[1]})
    return result


class AsynRedisPool(object):
    """docstring for AsynRedisPool"""

    def __init__(self, host="localhost", port=6379, db=None, password=None, encoding='utf-8',
                 socket_keepalive=False, connection_pool=None, startup_nodes=None,
                 create_connection_timeout=None, project='', **kwargs):
        if project:
            project = f'{project}:'
        self.cluster_flag = False
        self.project = project
        if startup_nodes:
            from aredis import StrictRedisCluster
            if isinstance(startup_nodes, (str, bytes)):
                startup_nodes = _normalize_startup_nodes(startup_nodes)
            self._redis = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True, encoding=encoding,
                                             skip_full_coverage_check=True, **kwargs)
            self.cluster_flag = True
        else:
            self._redis = StrictRedis(host=host, port=port, db=db, password=password, encoding=encoding,
                                      socket_keepalive=socket_keepalive, connection_pool=connection_pool,
                                      **kwargs)

    def add_head(self, key):
        return f'{self.project}{key}'

    def format_key():
        def make_wrapper(func):
            def wrapper(self, key, *args, **kwargs):
                new_key = self.add_head(key)
                return func(self, new_key, *args, **kwargs)
            return wrapper
        return make_wrapper

    def format_key_keys():
        def make_wrapper(func):
            def wrapper(self, key, keys, *args, **kwargs):
                new_key = self.add_head(key)
                new_keys = list(map(self.add_head, keys))
                return func(self, new_key, new_keys, *args, **kwargs)
            return wrapper
        return make_wrapper

    def format_args():
        def make_wrapper(func):
            def wrapper(self, *args, **kwargs):
                new_args = list(map(self.add_head, list(args)))
                return func(self, *new_args, **kwargs)
            return wrapper
        return make_wrapper

    def format_two_key():
        def make_wrapper(func):
            def wrapper(self, src, dst, *args, **kwargs):
                new_src = self.add_head(src)
                new_dst = self.add_head(dst)
                return func(self, new_src, new_dst, *args, **kwargs)
            return wrapper
        return make_wrapper

    def format_keys():
        def make_wrapper(func):
            def wrapper(self, keys, *args):
                new_keys = list(map(self.add_head, keys))
                return func(self, new_keys, *args)
            return wrapper
        return make_wrapper

    def format_dicts():
        def make_wrapper(func):
            def wrapper(self, mapping, *args):
                new_mapping = {}
                for key in mapping.keys():
                    new_key = self.add_head(key)
                    new_mapping[new_key] = mapping[key]
                return func(self, new_mapping, *args)
            return wrapper
        return make_wrapper

    @format_args()
    async def unlink(self, *keys):
        """
        time complexity O(1)
        redis异步删除keys
        """
        return await self._redis.unlink(*keys)
# {
    """===============================string-start=========================="""
    @format_key()
    async def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """
        time complexity O(1)
        Set the value at key ``key`` to ``value``
        Arguments:
            key (str):     key key
            value (str):    key value
            ex(int):    过期时间(秒)
            px(int):    过期时间(豪秒)
            nx(bool):   如果设置为True,则只有key不存在时,当前set操作才执行(新建)
            xx(bool):   如果设置为True,则只有key存在时,当前set操作才执行 （修改）
        Returns:
            result(bool): 是否成功成功是True失败可能是None
        """
        return await self._redis.set(key, value, ex, px, nx, xx)

    async def get(self, key):
        """
        time complexity O(1)
        Return the value at ``key``, or None if the key doesn't exist
        Arguments:
            key (str):     key
        Returns:
            value (str):返回value
        """
        return await self._redis.get(key)

    @format_key()
    async def getset(self, key, value):
        """
        time complexity O(1)
        设置新值并获取原来的值
        """
        return await self._redis.getset(key, value)

    @format_key()
    async def strlen(self, key):
        """
        time complexity O(1)
        获得key对应的value长度
        """
        return await self._redis.strlen(key)

    @format_key()
    async def getrange(self, key, start, end):
        """
        time complexity O(1)
        获得key对应的value的start到end长度字符返回
        """
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(end, int):
            raise TypeError("end argument must be int")
        return await self._redis.getrange(key, start, end)

    @format_key()
    async def setrange(self, key, offset, value):
        """
        time complexity O(1)
        设置key对应的value从offset地方用新value替换
        """
        if not isinstance(offset, int):
            raise TypeError("offset argument must be int")
        if offset < 0:
            raise ValueError("offset must be greater equal 0")
        return await self._redis.setrange(key, offset, value)

    @format_key()
    async def setbit(self, key, offset, value):
        """
        time complexity O(1)
        value值只能是1或0
        设置key对应的value二进制在offset位用value替换
        """
        if not isinstance(offset, int):
            raise TypeError("offset argument must be int")
        if offset < 0:
            raise ValueError("offset must be greater equal 0")
        if value not in (0, 1):
            raise ValueError("value argument must be either 1 or 0")
        return await self._redis.setbit(key, offset, value)

    @format_key()
    async def getbit(self, key, offset):
        """
        time complexity O(1)
        获取key对应的value二进制在offset位的值
        """
        if not isinstance(offset, int):
            raise TypeError("offset argument must be int")
        if offset < 0:
            raise ValueError("offset must be greater equal 0")
        return await self._redis.getbit(key, offset)

    @format_key()
    async def expire(self, key, time):
        """
        time complexity O(1)
        设置key的过期时间s
        """
        return await self._redis.expire(key, time)

    @format_key()
    async def pexpire(self, key, time):
        """
        time complexity O(1)
        设置key的过期时间ms
        """
        return await self._redis.pexpire(key, time)

    @format_key()
    async def pexpireat(self, key, when):
        """
        time complexity O(1)
        设置key的过期时间(在什么时候过期)
        when是uninx的时间戳ms
        """
        return await self._redis.pexpireat(key, when)

    @format_key()
    async def pttl(self, key):
        """
        time complexity O(1)
        获得key过期时间(ms),没有设置过期时间返回-1
        """
        return await self._redis.pttl(key)

    @format_key()
    async def ttl(self, key):
        """
        time complexity O(1)
        获得name过期时间(s),没有设置过期时间返回-1
        """
        return await self._redis.ttl(key)

    @format_dicts()
    async def mset(self, mapping):
        """
        time complexity O(n)
        Arguments:
            mapping (dict):   {name: value,name1: value1}
        Returns:
            return ok
        """
        return await self._redis.mset(mapping)

    @format_dicts()
    async def msetnx(self, mapping):
        """
        time complexity O(n)
        Arguments:
            mapping (dict):   {name: value,name1: value1}
        Returns:
            return (bool): 与mset区别是指定的key中有任意一个已存在,则不进行任何操作,返回错误
        """
        return await self._redis.msetnx(mapping)

    @format_keys()
    async def mget(self, keys, *args):
        """
        time complexity O(n)
        Arguments:
            keys (list): [name, name1]
        Returns:
            return (list): 返回对应keys的value, name在数据库不存在返回None
        Mind!:
            一次性取多个key确实比get提高了性能,但是mget的时间复杂度O(n),
            实际使用过程中测试当key的数量到大于100之后性能会急剧下降,
            建议mget每次key数量不要超过100。在使用前根据实列的redis吞吐量可能会不一样。
        """
        return await self._redis.mget(keys, *args)

    @format_key()
    async def incr(self, key, amount=1):
        """
        time complexity O(1)
        将key对应的value值自增amount，并返回自增后的值。只对可以转换为整型的String数据起作用。
        用于统计sql型数据库大表里面的数据量
        """
        return await self._redis.incr(key, amount)

    @format_key()
    async def incrbyfloat(self, key, amount=1.0):
        """
        time complexity O(1)
        amount 可以为负数代表减法
        将key对应的value值自增amount，并返回自增后的值。只对可以转换为float的String数据起作用。
        用于统计sql型数据库大表里面的数据量
        """
        return await self._redis.incrbyfloat(key, amount)

    @format_key()
    async def decr(self, key, amount=1):
        """
        time complexity O(1)
        将key对应的value值自减amount，并返回自减后的值。只对可以转换为整型的String数据起作用。
        用于统计sql型数据库大表里面的数据量
        """
        return await self._redis.decr(key, amount)

    async def keys(self, pattern='*'):
        """
        time complexity O(n)
        获取匹配pattern的所有key.实际项目中慎用
        """
        return await self._redis.keys(pattern)

    @format_key()
    async def move(self, key, db):
        """
        time complexity O(1)
        移动key到其他db
        """
        return await self._redis.move(key, db)

    async def randomkey(self):
        """
        time complexity O(1)
        随机返回一个key
        """
        return await self._redis.randomkey()

    @format_args()
    async def rename(self, src, dst):
        """
        time complexity O(1)
        重命名key src to dst
        """
        return await self._redis.rename(src, dst)

    @format_args()
    async def exists(self, *keys):
        """
        time complexity O(1)
        查看keys是否存在返回存在的key数量
        """
        return await self._redis.exists(*keys)

    @format_args()
    async def delete(self, *keys):
        """
        time complexity O(1)
        删除keys
        """
        return await self._redis.delete(*keys)

    @format_key()
    async def type(self, key):
        """
        time complexity O(1)
        查看key对应value类型
        """
        return await self._redis.type(key)
    """===============================string-end============================"""
# }

# {
    """===============================list-start============================"""
    @format_key()
    async def blpop(self, key, timeout=0):
        """
        如果keys里面有list为空要求整个服务器被阻塞以保证块执行时的原子性，
        该行为阻止了其他客户端执行 LPUSH 或 RPUSH 命令
        阻塞的一个命令，用来做轮询和会话配合使用
        Arguments:
            keys(str): key
            timeout(int): S
        """
        return await self._redis.blpop(key, timeout)

    @format_key()
    async def brpop(self, key, timeout=0):
        """
        同上,取数据的方向不同
        """
        return await self._redis.brpop(key, timeout)

    @format_two_key()
    async def brpoplpush(self, src, dst, timeout=0):
        """
        从src表尾取一个数据插入dst表头。同上src为空阻塞
        """
        return await self._redis.brpoplpush(src, dst, timeout)

    @format_key()
    async def lpush(self, key, *values):
        """
        time complexity O(n)
        Set the value at key ``key`` to ``value``
        Arguments:
            key (str):     key key
            value (list):    key value
        Returns:
            result(int): 插入成功之后list长度
        """
        return await self._redis.lpush(key, *values)

    @format_key()
    async def lpushx(self, key, *values):
        """
        time complexity O(n)
        only key not exists
        Arguments:
            key (str):     key
            value (list):    key value
        Returns:
            result(int): 插入成功之后list长度
        """
        return await self._redis.lpushx(key, *values)

    @format_key()
    async def lpop(self, key):
        """
        time complexity O(1)
        移除并返回列表 key 的头元素。
        """
        return await self._redis.lpop(key)

    @format_key()
    async def rpush(self, key, *values):
        """
        time complexity O(n)
        Set the value at key ``key`` to ``value``
        Arguments:
            key (str):     key key
            value (list):    key value
        Returns:
            result(int): 插入成功之后list长度
        """
        return await self._redis.rpush(key, *values)

    @format_key()
    async def rpushx(self, key, *values):
        """
        time complexity O(n)
        only key not exists
        Arguments:
            key (str):     key
            value (list):    key value
        Returns:
            result(int): 插入成功之后list长度
        """
        return await self._redis.rpushx(key, *values)

    @format_key()
    async def rpop(self, key):
        """
        time complexity O(1)
        移除并返回列表 key尾元素。
        """
        return await self._redis.rpop(key)

    @format_key()
    async def lrange(self, key, start, end):
        """
        time complexity O(n)
        获取list数据包含start,end.在不清楚list的情况下尽量不要使用lrange(key, 0, -1)操作
        应尽可能控制一次获取的元素数量
        """
        return await self._redis.lrange(key, start, end)

    @format_args()
    async def rpoplpush(self, src, dst):
        """
        从src表尾取一个数据插入dst表头
        """
        return await self._redis.rpoplpush(src, dst)

    @format_key()
    async def llen(self, key):
        """
        time complexity O(1)
        获取list长度,如果key不存在返回0,如果key不是list类型返回错误
        """
        return await self._redis.llen(key)

    @format_key()
    async def lindex(self, key, index):
        """
        time complexity O(n) n为经过的元素数量
        返回key对应list的index位置的value
        """
        return await self._redis.lindex(key, index)

    @format_key()
    async def linsert(self, key, where, refvalue, value):
        """
        time complexity O(n) n为经过的元素数量
        key或者refvalue不存在就不进行操作
        Arguments:
            where(str): BEFORE|AFTER  后|前
            refvalue(str): list里面的值
        """
        return await self._redis.linsert(key, where, refvalue, value)

    @format_key()
    async def lrem(self, key, count, value):
        """
        time complexity O(n)
        删除count数量的value
        Arguments:
            count(int): count>0 表头开始搜索
                        count<0 表尾开始搜索
                        count=0 删除所有与value相等的数值
        Returns:
            result(int): 删除的value的数量
        """
        return await self._redis.lrem(key, count, value)

    @format_key()
    async def lset(self, key, index, value):
        """
        time complexity O(n)
        设置list的index位置的值，没有key和超出返回错误
        """
        return await self._redis.lset(key, index, value)

    @format_key()
    async def ltrim(self, key, start, end):
        """
        time complexity O(n) n为被删除的元素数量
        裁剪让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
        """
        return await self._redis.ltrim(key, start, end)

    @format_key()
    async def sort(self, key, start=None, num=None, by=None, get=None,
                   desc=False, alpha=False, store=None, groups=None):
        """
        time complexity O(n)
        O(N+M*log(M))， N 为要排序的列表或集合内的元素数量， M 为要返回的元素数量。
        删除count数量的value
        Arguments:
            by(str): 让排序按照外部条件排序,
                    可以先将权重插入redis然后再作为条件进行排序如(user_level_*)
            get(str): redis有一组user_name_*然后*是按照list里面的值，
                    按照排序取一个个key的value
            store(str): 保留sort之后的结果，可以设置expire过期时间作为结果缓存
            alpha: 按照字符排序
            desc: 逆序
        Returns:
            result(list): 排序之后的list
        """
        return await self._redis.sort(key, start, num, by, get, desc, alpha, store, groups)

    async def scan(self, cursor=0, match=None, count=None):
        """
        time complexity O(1) 单次
        增量迭代返回redis数据库里面的key,因为是增量迭代过程中返回可能会出现重复
        Arguments:
            cursor(int): 游标
            match(str): 匹配
            count(int): 每次返回的key数量
        Returns:
            result(set): 第一个是下次scan的游标,后面是返回的keys(list)当返回的游标为0代表遍历完整个redis
        """
        return await self._redis.scan(cursor, match, count)

    """===============================list-end=============================="""
# }

# {
    """===============================hash-start==================================="""
    @format_key()
    async def hdel(self, key, *names):
        """
        time complexity O(n) n为names长度
        Return the value at ``key``, or None if the key doesn't exist
        Arguments:
            key (str):     key
            names(list): hash里面的域
        Returns:
            result (int): 成功删除的个数
        """
        return await self._redis.hdel(key, *names)

    @format_key()
    async def hexists(self, key, name):
        """
        time complexity O(1)
        判断key中是否有name域
        """
        return await self._redis.hexists(key, name)

    @format_key()
    async def hget(self, key, name):
        """
        time complexity O(1)
        """
        return await self._redis.hget(key, name)

    @format_key()
    async def hgetall(self, key):
        """
        time complexity O(n)
        """
        return await self._redis.hgetall(key)

    @format_key()
    async def hincrby(self, key, name, amount=1):
        """
        time complexity O(1)
        amount可以为负数,且value值为整数才能使用否则返回错误
        """
        return await self._redis.hincrby(key, name, amount)

    @format_key()
    async def hincrbyfloat(self, key, name, amount=1.0):
        """
        time complexity O(1)
        """
        return await self._redis.hincrbyfloat(key, name, amount)

    @format_key()
    async def hkeys(self, key):
        """
        time complexity O(n)
        """
        return await self._redis.hkeys(key)

    @format_key()
    async def hlen(self, key):
        """
        time complexity O(1)
        """
        return await self._redis.hlen(key)

    @format_key()
    async def hset(self, key, name, value):
        """
        time complexity O(1)
        """
        return await self._redis.hset(key, name, value)

    @format_key()
    async def hsetnx(self, key, name, value):
        """
        time complexity O(1)
        """
        return await self._redis.hsetnx(key, name, value)

    @format_key()
    async def hmset(self, key, mapping):
        """
        time complexity O(n)
        """
        return await self._redis.hmset(key, mapping)

    @format_key()
    async def hmget(self, key, *args):
        """
        time complexity O(n)
        """
        return await self._redis.hmget(key, *args)

    @format_key()
    async def hvals(self, key):
        """
        time complexity O(n)
        返回hash表所有的value
        """
        return await self._redis.hvals(key)

    @format_key()
    async def hstrlen(self, key, name):
        """
        time complexity O(1)
        """
        return await self._redis.hstrlen(key, name)
    """===============================hash-end====================================="""
# }

# {
    """=================================set-start================================="""
    @format_key()
    async def sadd(self, key, *values):
        """
        time complexity O(n) n为values长度
        """
        return await self._redis.sadd(key, *values)

    @format_key()
    async def scard(self, key):
        """
        time complexity O(n) set长度
        返回set大小
        """
        return await self._redis.scard(key)

    @format_args()
    async def sdiff(self, key, *args):
        """
        time complexity O(n) N 是所有给定集合的成员数量之和
        返回差集成员的列表。
        """
        return await self._redis.sdiff(key, *args)

    @format_args()
    async def sdiffstore(self, dest, keys, *args):
        """
        time complexity O(n) N 是所有给定集合的成员数量之和
        返回差集成员的数量。并将结果保存到dest这个set里面
        """
        return await self._redis.sdiffstore(dest, keys, *args)

    @format_args()
    async def sinter(self, key, *args):
        """
        time complexity O(N * M)， N 为给定集合当中基数最小的集合， M 为给定集合的个数。
        返回交集数据的list
        """
        return await self._redis.sinter(key, *args)

    @format_args()
    async def sinterstore(self, dest, keys, *args):
        """
        time complexity O(n) N 是所有给定集合的成员数量之和
        返回交集成员的数量。并将结果保存到dest这个set里面
        """
        return await self._redis.sinterstore(dest, keys, *args)

    @format_key()
    async def sismember(self, key, name):
        """
        time complexity O(1)
        判断name是否在key中
        """
        return await self._redis.sismember(key, name)

    @format_key()
    async def smembers(self, key):
        """
        time complexity O(n)
        返回set里面所有成员
        """
        return await self._redis.smembers(key)

    @format_two_key()
    async def smove(self, src, dst, value):
        """
        time complexity O(1)
        将value从src移动到dst原子性操作
        """
        return await self._redis.smove(src, dst, value)

    @format_key()
    async def spop(self, key, count=None):
        """
        time complexity O(n) n
        默认随机删除一条, 删除count条
        """
        return await self._redis.spop(key, count)

    @format_key()
    async def srandmember(self, key, number=None):
        """
        time complexity O(n) n
        默认随机返回一条, 返回number条
        """
        return await self._redis.srandmember(key, number)

    @format_key()
    async def srem(self, key, *values):
        """
        time complexity O(n) n为values长度
        移除key里面values
        """
        return await self._redis.srem(key, *values)

    @format_args()
    async def sunion(self, keys, *args):
        """
        time complexity O(N)， N 是所有给定集合的成员数量之和
        返回并集
        """
        return await self._redis.sunion(keys, *args)

    @format_args()
    async def sunionstore(self, dest, keys, *args):
        """
        time complexity O(N)， N 是所有给定集合的成员数量之和。
        求并集并保存
        """
        return await self._redis.sunionstore(dest, keys, *args)

    @format_key()
    async def sscan(self, key, cursor=0, match=None, count=None):
        """
        time complexity O(1)
        同scan只是这个是set使用
        """
        return await self._redis.sscan(key, cursor, match, count)
    """=================================set-end==================================="""
# }

# {
    """===============================SortedSet-start============================="""
    @format_key()
    async def zadd(self, key, *args, **kwargs):
        """
        time complexity O(M*log(N)), N 是有序集的基数， M 为成功添加的新成员的数量。
        Arguments:
            mapping(dict): (value:score)
            CH(bool): 修改返回值为发生变化的成员总数，原始是返回新添加成员的总数 (CH 是 changed 的意思)。
                      更改的元素是新添加的成员，已经存在的成员更新分数。 所以在命令中指定的成员有相同的分数将不被计算在内。
                      注：在通常情况下，ZADD返回值只计算新添加成员的数量。
            INCR(bool): 当ZADD指定这个选项时，成员的操作就等同ZINCRBY命令，对成员的分数进行递增操作。
        Returns:
            result(int): 成功插入数量
        """
        return await self._redis.zadd(key, *args, **kwargs)

    @format_key()
    async def zcard(self, key):
        """
        time complexity O(1)
        返回zset()基数
        """
        return await self._redis.zcard(key)

    @format_key()
    async def zcount(self, key, minz, maxz):
        """
        time complexity O(log(N)), N 为有序集的基数。
        返回score在min和max之间的value的个数
        """
        return await self._redis.zcount(key, minz, maxz)

    @format_key()
    async def zincrby(self, key, value, amount=1):
        """
        time complexity O(log(N)), N 为有序集的基数。
        amount 可以为负数
        """
        return await self._redis.zincrby(key, value, amount)

    @format_key_keys()
    async def zinterstore(self, dest, keys, aggregate=None):
        """
        time complexity O(N*K)+O(M*log(M))， N 为给定 key 中基数最小的有序集， K 为给定有序集的数量， M 为结果集的基数。
        求交集并按照aggregate做处理之后保存到dest。默认是求和
        Arguments:
            aggregate(str):sum 和, min 最小值, max 最大值
        返回新zset里面的value个数
        """
        return await self._redis.zinterstore(dest, keys, aggregate)

    @format_key()
    async def zrange(self, key, start, end, desc=False, withscores=False,
                     score_cast_func=float):
        """
        time complexity O(log(N)+M)， N 为有序集的基数，而 M 为结果集的基数。
        Arguments:
            start,有序集合索引起始位置（非分数）
            end,有序集合索引结束位置（非分数）
            desc,排序规则，默认按照分数从小到大排序
            withscores,是否获取元素的分数，默认只获取元素的值
        """
        return await self._redis.zrange(key, start, end, desc, withscores, score_cast_func)

    @format_key()
    async def zrevrange(self, key, start, end, withscores=False, score_cast_func=float):
        """
        time complexity O(log(N)+M)， N 为有序集的基数，而 M 为结果集的基数。
        Arguments:
            start,有序集合索引起始位置（非分数）
            end,有序集合索引结束位置（非分数）
            withscores,是否获取元素的分数，默认只获取元素的值
            score_cast_func,对分数进行数据转换的函数
        """
        return await self._redis.zrevrange(key, start, end, withscores, score_cast_func)

    @format_key()
    async def zrangebyscore(self, key, minz, maxz, start=None, num=None, withscores=False, score_cast_func=float):
        """
        time complexity O(log(N)+M)， N 为有序集的基数，而 M 为结果集的基数。
        有序集成员按 score 值递增(从小到大)次序排列。
        """
        return await self._redis.zrangebyscore(key, minz, maxz, start, num, withscores, score_cast_func)

    @format_key()
    async def zrevrangebyscore(self, key, minz, maxz, start=None, num=None, withscores=False, score_cast_func=float):
        """
        time complexity O(log(N)+M)， N 为有序集的基数，而 M 为结果集的基数。
        有序集成员按 score 值递减(从大到小)次序排列。
        """
        return await self._redis.zrevrangebyscore(key, minz, maxz, start, num, withscores, score_cast_func)

    @format_key()
    async def zrangebylex(self, key, minz, maxz, start=None, num=None):
        """
        time complexity O(log(N)+M)， N 为有序集的基数，而 M 为结果集的基数。
        有序集成员按 value 字典序递增(从小到大)次序排列。
        """
        return await self._redis.zrangebylex(key, minz, maxz, start, num)

    @format_key()
    async def zrevrangebylex(self, key, minz=b'-', maxz=b'+', start=None, num=None):
        """
        time complexity O(log(N)+M)， N 为有序集的基数，而 M 为结果集的基数。
        有序集成员按 value 字典序递减(从大到小)次序排列。
        """
        return await self._redis.zrevrangebylex(key, minz, maxz, start, num)

    @format_key()
    async def zrank(self, key, value):
        """
        time complexity O(log(N))
        查找zset里面这个value的rank排名从0开始
        """
        return await self._redis.zrank(key, value)

    @format_key()
    async def zrevrank(self, key, value):
        """
        time complexity O(log(N))
        查找zset里面这个value的rank排名从0开始
        """
        return await self._redis.zrevrank(key, value)

    @format_key()
    async def zrem(self, key, *values):
        """
        time complexity O(M*log(N))， N 为有序集的基数， M 为被成功移除的成员的数量
        删除zset里面单个或者多个成员
        """
        return await self._redis.zrem(key, *values)

    @format_key()
    async def zremrangebylex(self, key, minz=b'-', maxz=b'+'):
        """
        time complexity O(log(N)+M)， N 为有序集的基数，而 M 为被移除成员的数量。
        按照字典增序范围删除
        """
        return await self._redis.zremrangebylex(key, minz, maxz)

    @format_key()
    async def zremrangebyrank(self, key, minz, maxz):
        """
        time complexity O(log(N)+M)， N 为有序集的基数，而 M 为被移除成员的数量。
        按照rank范围删除
        """
        return await self._redis.zremrangebyrank(key, minz, maxz)

    @format_key()
    async def zremrangebyscore(self, key, minz, maxz):
        """
        time complexity O(log(N)+M)， N 为有序集的基数，而 M 为被移除成员的数量。
        按照score范围删除
        """
        return await self._redis.zremrangebyscore(key, minz, maxz)

    @format_key()
    async def zscore(self, key, value):
        """
        time complexity O(log(N))
        查找zset里面这个value的score排名从0开始
        """
        return await self._redis.zscore(key, value)

    @format_key_keys()
    async def zunionstore(self, dest, keys, aggregate=None):
        """
        time complexity O(N)+O(M log(M))， N 为给定有序集基数的总和， M 为结果集的基数。
        求并集保存
        """
        return await self._redis.zunionstore(dest, keys, aggregate)

    @format_key()
    async def zscan(self, key, cursor=0, match=None, count=None):
        """
        time complexity O(1)
        同SCAN
        """
        return await self._redis.zscan(key, cursor, match, count)

    async def zlexcount(self, key, minz=b'-', maxz=b'+'):
        """
        time complexity O(log(N))，其中 N 为有序集合包含的元素数量。
        min -负无限  [闭空间不包括自己 (开空间包括自己
        max +正无限 [a, (c
        """
        return await self._redis.zlexcount(key, minz, maxz)
    """===============================SortedSet-end==============================="""
# }

# {
    """===============================HyperLogLog-start================================="""
    @format_key()
    async def pfadd(self, key, *values):
        """
        time complexity O(n)
        """
        return await self._redis.pfadd(key, *values)

    @format_args()
    async def pfcount(self, *sources):
        """
        time complexity O(1)
        计算key的基数
        """
        return await self._redis.pfcount(*sources)

    @format_args()
    async def pfmerge(self, dest, *sources):
        """
        time complexity O(n) 其中 N 为被合并的 HyperLogLog 数量,不过这个命令的常数复杂度比较高
        合并HyperLogLog
        """
        return await self._redis.pfmerge(dest, *sources)
    """===============================HyperLogLog-end==================================="""
# }

# {
    """==================================GEO-start===================================="""
    @format_key()
    async def geoadd(self, key, *values):
        """
        time complexity O(log(N)) 每添加一个元素的复杂度为 O(log(N)) ， 其中 N 为键里面包含的位置元素数量。
        """
        return await self._redis.geoadd(key, *values)

    @format_key()
    async def geopos(self, key, *values):
        """
        time complexity O(log(N))
        从键里面返回所有给定位置元素的位置（经度和纬度）。
        """
        return await self._redis.geopos(key, *values)

    @format_key()
    async def geohash(self, key, *values):
        """
        time complexity O(log(N))
        命令返回的 geohash 的位置与用户给定的位置元素的位置一一对应
        """
        return await self._redis.geohash(key, *values)

    @format_key()
    async def geodist(self, key, place1, place2, unit=None):
        """
        time complexity O(log(N))
        返回两个给定位置之间的距离。
        Argument:
            unit : m: 米,km: 千米,mi: 英里,ft: 英尺
        """
        return await self._redis.geodist(key, place1, place2, unit)

    @format_key()
    async def georadius(self, key, longitude, latitude, radius, unit='m',
                        withdist=False, withcoord=False, withhash=False, count=None,
                        sort=None, store=None, store_dist=None):
        """
        time complexity O(N+log(M))， 其中 N 为指定半径范围内的位置元素数量， 而 M 则是被返回位置元素的数量。
        以给定的经纬度为中心， 返回键包含的位置元素当中， 与中心的距离不超过给定最大距离的所有位置元素。
        Argument:
            longitude: 经度
            latitude: 纬度
            radius: 距离
            unit: 距离单位
            withdist: 在返回位置元素的同时， 将位置元素与中心之间的距离也一并返回。 距离的单位和用户给定的范围单位保持一致。
            withcoord: 将位置元素的经度和维度也一并返回
            withhash: 以 52 位有符号整数的形式， 返回位置元素经过原始 geohash 编码的有序集合分值。
                      这个选项主要用于底层应用或者调试， 实际中的作用并不大。
            sort: 根据中心的位置排序 ASC,DESC
            count: 取前多少个
            store: 保存
            store_dist: 存储地名和距离
        Return:
            list(list)
            [['Foshan', 109.4922], ['Guangzhou', 105.8065]]
        """
        return await self._redis.georadius(key, longitude, latitude, radius, unit, withdist, withcoord,
                                           withhash, count, sort, store, store_dist)

    @format_key()
    async def georadiusbymember(self, key, member, radius, unit='m',
                                withdist=False, withcoord=False, withhash=False, count=None,
                                sort=None, store=None, store_dist=None):
        """
        time complexity O(N+log(M))， 其中 N 为指定半径范围内的位置元素数量， 而 M 则是被返回位置元素的数量。
        以给定的经纬度为中心， 返回键包含的位置元素当中， 与中心的距离不超过给定最大距离的所有位置元素。
        Argument:
            member: 位置元素
            radius: 距离
            unit: 距离单位
            withdist: 在返回位置元素的同时， 将位置元素与中心之间的距离也一并返回。 距离的单位和用户给定的范围单位保持一致。
            withcoord: 将位置元素的经度和维度也一并返回
            withhash: 以 52 位有符号整数的形式， 返回位置元素经过原始 geohash 编码的有序集合分值。 这个选项主要用于底层应用或者调试， 实际中的作用并不大。
            sort: 根据中心的位置排序 ASC,DESC
            count: 取前多少个
            store: 保存
            store_dist: 存储地名和距离
        Return:
            list(list)
            [['Foshan', 109.4922], ['Guangzhou', 105.8065]]
        """
        return await self._redis.georadiusbymember(key, member, radius, unit, withdist, withcoord,
                                                   withhash, count, sort, store, store_dist)
    """==================================GEO-end======================================"""
# }

# {
    """==================================Lock-start===================================="""
    async def acquire_lock(self, lockname, acquire_timeout=10, lock_timeout=10):
        identifier = str(uuid.uuid4())
        lock_timeout = int(math.ceil(lock_timeout))

        end = time.time() + acquire_timeout
        while time.time() < end:
            if await self.set(
                    lockname,
                    identifier,
                    ex=lock_timeout,
                    nx=True):
                return identifier
            await asyncio.sleep(1)
        return False

    async def release_lock(self, lockname, identifier):
        try:
            if await self.get(lockname) == identifier:
                await self.delete(lockname)
                return True
        except BaseException:
            logger.error(traceback.format_exc())
            return False
    """==================================Lock-end======================================"""
# }
