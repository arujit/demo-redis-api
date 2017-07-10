import socket
import os,sys
import pickle
from redisApi import RedisApi
import yaml
from threading import Thread

"""
author:james.bondu
TO_DO:
---simple TCP server for handeling Rediscalls
---Remove SQL section
---Improve method for condition handeling
"""

class Communication:
    """ TCP Server initialization"""
    def __init__(self):
        with open("config.yaml","r") as configfile:
            self.config_file = yaml.load(configfile)

        self.tcp_config = self.config_file["tcp"]
        print self.tcp_config     
        self.server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.server.bind((self.tcp_config["host"],self.tcp_config["port"]))
        self.server.listen(1)
        print "server set-up finished"

    def close(self):
        self.server.close()

    def send(self,data):
        print "gonna send data"
        self.server.send(data)    


class Db:
    """Class to handel all dbs"""
    def __init__ (self,message):
        self.message = message
        print self.message
        self.redis_api = RedisApi()
        
    def set(self):
        return self.redis_api.set_string(self.message["key"],self.message["value"],self.message["ttl"])

    def get(self):
        return self.redis_api.get_string(self.message["key"])

    def exist(self):
        return self.redis_api.exist(self.message["key"])

    def remove(self):
        return self.redis_api.remove_key(self.message["key"])    

    def process_handler(self,message):
        try :
            if message["task"] == "set":
                return self.set()

            elif message["task"] == "get":
                return self.get()

            elif message["task"] == "exist":
                return self.exist()

            elif message["task"] == "remove":
                return self.remove()

            else:
                raise ValueError

        except ValueError:
            print "Please give proper task"
        else:
            return None

            

            
        
class Messagehandler(Thread):
    """
    I want to establish multiple connection at a time... so message must be received here may be in another class
    """
    def __init__(self,client):
        Thread.__init__(self)
        self.client = client
        self.client_pickle = self.client.recv(512)
        self.message = pickle.loads(self.client_pickle)
        print self.message
        print "Message Received !!!"
        
    def run(self):
        db = Db(self.message)
        self.resp = db.process_handler(self.message)
        if type(self.resp) == bool:
            self.client.send(str(self.resp))
        else:
            self.client.send(self.resp)
        
    
        
if __name__ == "__main__":
    """Main thread"""
    check_server = Communication()
    threads = []
    while True:
        #print "true"
        client,address = check_server.server.accept()
        """message is a dictionary that has some attributes like what to do, username and password """ 
        newthread =Messagehandler(client)
        newthread.start()
        threads.append(newthread)

    for t in threads:
        t.join()    
        
