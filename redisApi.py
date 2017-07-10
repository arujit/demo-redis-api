import redis
import yaml
import datetime

"""
author : james.bondu
TO_DO:
---get,set,exit,remove
---key and value adding with a ttl
---value is string as of now
---redis for set,list,hashmap
"""

class RedisApi(object):
    def __init__(self,host,port,database):
        print "hello RedisApi"
        self.host = host
        self.port = port
        self.database = database
        self.pool = redis.ConnectionPool(host=self.host,port=self.port,db = self.database)
        self.strict_redis = redis.StrictRedis(connection_pool = self.pool)
        print "Welcome to Redis!!!"
        

    def set_string(self,key,value,ttl):
        self.value = value
        self.key = key
        self.time = ttl
        self.strict_redis.setex(self.key,self.time,self.value)
        return True

    def get_string(self,key):
        self.search_key = key
        self.search_value = self.strict_redis.get(key)
        try:
            if search_value == None:
                raise ValueError
        except ValueError :
            print "Seach key not found!!!"
        else:
            pass
        finally:
            return self.search_value

    def exist(self,key):
        self.key = key
        """
        bool value indicating the presence of the key
        """
        self.is_exist =  self.strict_redis.exists(key)
        return self.is_exist

    def remove_key(self,key):
        self.remove_key = key
        self.strict_redis.delete(self.remove_key)

    def set_add(self,key, * values):
        self.strict_redis.sadd(*values)
        return True

        
    def hash_set(self,key,token):
        """
        multiple value insertion in dictionary
        token must be a dictionary type
        """
        try:
            if type(token) != dict:
                raise TypeError
        except TypeError:
            print "Provide dict here!!!"
            return None
        else:
            self.strict_redis.hmset(key,token)
            return True    

    def hash_get(self,key,field):
        """
        single querry of a hash field
        """
        return self.strict_redis.hget(key,field)

    def hash_remove(self,key,field):
        """
        single field removal
        """
        try :
            self.responce = self.strict_redis.hdel(key,field)
            if self.resp == 0:
                raise ValueError
        except:
            print "provide correct field value"    
        else:
            pass
        finally:
            return self.responce

    





