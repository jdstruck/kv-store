#!/usr/bin/env python

# import glob
import sys
sys.path.append('gen-py')
# sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])
# from chord import FileStore
from kvstore import KVStore

from kvstore.ttypes import KVPair, NodeID #, RFile, RFileMetadata, SystemException

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import json
import pathlib
import re
import socket
# import hashlib
import time


DEBUG = True 


def create_node(s):
    m=re.match('([^:]+):([^:]+)',s)
    u = (m.group(1),int(m.group(2)))
    return u

class KVStoreHandler:

    def __init__(self, id=None, ip=None, port=None, servers=None):
        self.n = NodeID(id, ip, int(port))
        self.kvstore = {}
        self.servers = servers
        self.__populateKvstoreFromCommitLog()
        
    def get(self, key, clevel):
# TODO: consistency level retrieval logic
        if DEBUG:
            print("get", key)
        if(key in self.kvstore):
            return self.kvstore[key]
        else:
            return None

    def put(self, kvpair, clevel):
        if DEBUG:
            print("put", str(kvpair))
        # TODO: add timestamp to output
        self.__writeToCommitLog(kvpair)
        self.__storeKVPair(kvpair)
        return

    def __storeKVPair(self, kvpair):
# TODO: consistency level replication logic
        self.kvstore[kvpair.key] = kvpair.val


    def __populateKvstoreFromCommitLog(self):
        if DEBUG:
            print("Populating kvstore from commit_log...")
        file = pathlib.Path('commit_log')
        if file.exists():
            with open('commit_log', 'r') as f:
                data = json.load(f)
                temp = data['commit_log']
                for l in temp:
                    self.kvstore[l['key']] = l['val']
        if DEBUG:
            print("Contents of kvstore:", self.kvstore)

    def __writeToCommitLog(self, kvpair):
        file = pathlib.Path('commit_log')
        if file.exists():
            with open('commit_log', 'r') as f:
                data = json.load(f)
                temp = data['commit_log']
                temp.append({"key":kvpair.key, "val":kvpair.val})
            with open('commit_log', 'w') as f:    
                json.dump(data,f)
        else:
            with open('commit_log', 'w') as f:
                json.dump({"commit_log": [ {"key":kvpair.key, "val":kvpair.val} ] }, f)

def getIP():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    ip = s.getsockname()[0]
    s.close()
    return ip

def getServersAndID(ip, port):
    servers = []
    id = None
    for count, l in enumerate(open('nodes')):
        m = re.match('([^:]+):([^:]+)',l)
        u = (count, m.group(1), int(m.group(2)))
        print(u[0], u[1], u[2], end='')
        if u[1] == ip and int(u[2]) == int(port):
            print(" <- this server", end='')
            id = count
        print()
        servers.append(u)
    assert(id != None)
    return (servers, id)

def initServer():
    ip, port = getIP(), sys.argv[1]
    servers, id = getServersAndID(ip, port)
    # print(ip, port, id) 
    handler = KVStoreHandler(id, ip, port, servers)
    processor = KVStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    return server


if __name__ == '__main__':
    print('Starting the server...')
    server = initServer()
    server.serve()
