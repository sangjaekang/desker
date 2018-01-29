# -*- coding: utf-8 -*-
import redis
import ujson as json
import codecs

class Redissaver(object):
    def __init__(self,key,**kwargs):
        self.key = key
        self.host = kwargs.get('host','localhost')
        self.port = kwargs.get('port',6379)
        self.db = kwargs.get('db',0)
        self.init = kwargs.get('init',False)
        self.r = redis.Redis(host=self.host,port=self.port,db=self.db)
        self.store_type = kwargs.get('store_type','set')

        if self.init:
            self.r.delete(key)

        if self.store_type == 'set':
            self.method = lambda value : self.r.sadd(self.key,value)
        elif self.store_type == 'list':
            self.method = lambda value : self.r.lpush(self.key,value)
        elif self.store_type == 'hmset':
            self.method = lambda value : self.r.hmset(self.key,value)\
                    if not self.r.hexists(self.key,list(value)[0]) else None
        elif self.store_type == 'hmset_update':
            self.method = lambda value : self.r.hmset(self.key,value)

    def __call__(self,value):
        self.method(value)


class Redisloader(object):
    def __init__(self,key,**kwargs):
        self.key = key
        self.host = kwargs.get('host','localhost')
        self.port = kwargs.get('port',6379)
        self.db = kwargs.get('db',0)
        self.r = redis.Redis(host=self.host,port=self.port,db=self.db)
        self.cond = kwargs.get('cond',lambda x : True)
        self.store_type = kwargs.get('store_type','set')

    def __call__(self):
        if self.store_type == "set":
            member_list = [*(self.r.smembers(self.key))]
        elif self.store_type == "list":
            member_list = self.r.lrange(self.key,0,-1)
        elif self.store_type == 'hmset':
            items = {}
            for k,v in self.r.hgetall(self.key).items():
                key = codecs.decode(k,'utf-8')
                value = codecs.decode(v,'utf-8')
                items[key] = value
            return items

        row_list = []
        for row_serial in member_list:
            row = json.loads(row_serial)
            if self.cond(row):
                row_list.append(row)
        return row_list
