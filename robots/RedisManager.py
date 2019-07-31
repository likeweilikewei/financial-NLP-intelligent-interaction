#! /user/bin/env python
# -*- coding=utf-8 -*-


import redis


class RedisManager:
    """生成redis连接池"""

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(RedisManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, setting):
        self.__connection = redis.StrictRedis(connection_pool=redis.ConnectionPool(**setting))

    def keys(self, name):
        return self.__connection.keys(name)

    def type(self, name):
        return self.__connection.type(name)

    def keys_all(self):
        return self.__connection.keys()

    def pipeline(self):
        return self.__connection.pipeline(True, None)

    # 有序set 操作
    def zadd(self, name, score, value):
        return self.__connection.zadd(name, score, value)

    def zadds(self, name, result):
        return self.__connection.zadd(name, result)

    def zscore(self, name, value):
        return self.__connection.zscore(name=name, value=value)

    def zscan(self, name):
        return self.__connection.zscan(name=name)

    def zrem(self, name):
        return self.__connection.__delitem__(name)

    def zrange(self, name, start, end, desc, withscores):
        return self.__connection.zrange(name, start, end, desc=desc, withscores=withscores)

    def zreverrange(self, name, start, end, withscores):
        return self.__connection.zrevrange(name, start, end, withscores)

    def zrangeByScore(self, name, min, max, start, num, withscores):
        return self.__connection.zrangebyscore(name, min, max, start, num, withscores)

    def zreverseRange(self, name, max, min, start, num, withscore):
        return self.__connection.zrevrangebyscore(name, max, min, start, num, withscore)

    def zcard(self, name):
        return self.__connection.zcard(name)

    # 删除有序集合中指定score 范围的成员
    def zremByScoreLimit(self, name, min, max):
        return self.__connection.zremrangebyscore(name, min, max)

    def zinterstore(self, dest, keys):
        return self.__connection.zinterstore(dest, keys)

    def lpush(self, name, value):
        self.__connection.lpush(name, value)

    def lrange(self, name, start, end):
        return self.__connection.lrange(name, start, end)

    def hkeys(self, name):
        return self.__connection.hkeys(name)

    def exists(self, name):
        return True if self.__connection.exists(name) else False

    def hexists(self, name, key):
        return True if self.__connection.hexists(name, key) else False

    def set(self, key, value):
        self.__connection.set(key, value)

    def get(self, key):
        return self.__connection.get(key)

    def sadd(self, key, value):
        return self.__connection.sadd(key, value)

    def smembers(self, key):
        return list(self.__connection.smembers(key))

    def hset(self, name, key, count):
        self.__connection.hset(name, key, count)

    def hdel(self, name, *key):
        self.__connection.hdel(name, *key)

    def hdel_set(self, name):
        self.__connection.__delitem__(name)

    def hget(self, name, key):
        if self.exists(name):
            return self.__connection.hget(name, key)
        return key

    def hmget(self, name, keys):
        return self.__connection.hmget(name, keys)

    def hmset(self, name, mapping):
        return self.__connection.hmset(name, mapping)

    def hgetf(self, name, key):
        if self.exists(name):
            data = self.hgetall(name)
            datas = dict((v, k) for k, v in data.iteritems())
            return datas.get(key, key)
        return key

    def hsets(self, name, data):
        for key, value in data.iteritems():
            self.hset(name, key, value)

    def hincrby(self, name, key, count):
        self.__connection.hincrby(name, key, count)

    def sunion(self, tag):
        return self.__connection.sunion(tag)

    def hgetall(self, name):
        return self.__connection.hgetall(name)

    def getRedisConn(self):
        return self.__connection

    def expire(self, key, time):
        self.__connection.expire(key, time)
        self.__connection.register_script()

    def qsize(self, key):
        return self.__connection.llen(key)

    def empty(self, key):
        return self.qsize(key) == 0

    def put(self, key, item):
        self.__connection.rpush(key, item)

    def qget(self, key, block=True, timeout=None):
        if block:
            item = self.__connection.blpop(key, timeout=timeout)
        else:
            item = self.__connection.lpop(key)

        if item:
            item = item[1]
        return item

    def srem(self, key, item):
        self.__connection.srem(key, item)

    def get_nowait(self):
        return self.get(False)

    def hvals(self, key):
        return self.__connection.hvals(key)

    def delete(self, key):
        return self.__connection.delete(key)

    def delete_by_list(self, key_list):
        for __key in key_list:
            self.__connection.delete(__key)

    def sort(self, key, name, get, start, num, desc, alpha):
        return self.__connection.sort(key, by=name, get=get, start=start, num=num, desc=desc, alpha=alpha)

    def scard(self, key):
        return self.__connection.scard(key)

    def sscan(self, name):
        return self.__connection.sscan(name=name)

    def sinter(self, key1, key2):
        return self.__connection.sinter(key1, key2)

    def sinterstore(self, dest, key1, key2):
        return self.__connection.sinterstore(dest, key1, key2)

    def close(self, name):
        return self.__connection.client_kill(name)
