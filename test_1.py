import sys,os
from redisApi import RedisApi

r = RedisApi()

test1 = {"field-1":"value-1","key-2":"value-2"}
#r.hash_set("test-1",test1)

print r.hash_remove("test-1","field-1")
