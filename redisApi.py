import redis
import yaml
import datetime

"""
author : james.bondu
TO_DO:
---get,set,exit,remove
---key and value adding with a ttl
---value is string as of now
---one main thing is ttl in session is probably in unix timestamp,so converting it to a ttl valued thing; i.e. remaining seconds is a thing 
Target : 
---3PM
"""

class RedisApi(object):
    def __init__(self):
        print "hello RedisApi"
        with open("config.yaml","r") as configfile:
            self.config_file = yaml.load(configfile)
            
        self.redis_config = self.config_file["redis"]
        self.pool = redis.ConnectionPool(host=self.redis_config["host"],port=self.redis_config["port"],db = self.redis_config["db"])
        self.r = redis.StrictRedis(connection_pool = self.pool)
        print "Welcome to Redis!!!"
        

    def set_string(self,key,token,ttl):
        self.value = token
        self.key = key
        self.time = ttl
        self.r.setex(self.key,self.time,self.value)
        return True

    def get_string(self,key):
        self.search_key = key
        self.search_value = self.r.get(key)
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
        self.is_exist =  self.r.exists(key)
        return self.is_exist

    def remove_key(self,key):
        self.remove_key = key
        self.r.delete(self.remove_key)
        
    








