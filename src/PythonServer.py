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

MAXKEY = 256

DEBUG = True 


def create_node(s):
    m=re.match('([^:]+):([^:]+)',s)
    u = (m.group(1),int(m.group(2)))
    return u

class KVStoreHandler:

    def __init__(self, id=None, ip=None, port=None, servers=None):
        self.meta = NodeID(id, ip, int(port))
        self.kvstore = {}
        self.servers = servers
        self.__populateKvstoreFromCommitLog()
        
    def get(self, key, clevel):
# TODO: consistency level retrieval logic
        if DEBUG:
            print("get", key)
        if(key in self.kvstore):
            return self.kvstore[key]
        # else:
            # return None

    def put(self, kvpair, clevel):
        if DEBUG and 1:
            print("\nput called at", self.meta.ip, self.meta.port, "key:", str(kvpair.key), "at time", time.time())
        # TODO: add timestamp to output
        self.__storeKVPair(kvpair, clevel)
        return

    def __storeKVPair(self, kvpair, clevel):
        slen = len(self.servers)
        if DEBUG and 0:
            print("num of servers", slen, "kvpair.key", kvpair.key)

        # Partitioner
        partition = 0
        for i in range(slen):
            if kvpair.key >= partition and kvpair.key < partition + (MAXKEY//(slen)):
                id0 = self.servers[i][0] #, ip0, port0 = self.servers[i][0], self.servers[i][1], self.servers[i][2];
                if DEBUG and 1:
                    print("\tkey", kvpair.key, "goes to server at id", id0);
            partition += MAXKEY//slen

        # Write to replicas
        for i in range(3):
            idx = (id0+i) % len(self.servers)
            id, ip, port = self.servers[idx][0], self.servers[idx][1], self.servers[idx][2]
            # if DEBUG and 1:
            # if ip == self.meta.ip and port == self.meta.port:
            if id == self.meta.id:
                if DEBUG and 1:
                    print("\tBase case: put to this server")
                self.put_local(kvpair, clevel) # self.kvstore[kvpair.key] = kvpair.val
                # self.__replicate(kvpair, clevel)
            else:
                if DEBUG and 1:
                    print("\tPut to another server...")
                transport = TSocket.TSocket(ip, port);
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = KVStore.Client(protocol)
                transport.open()
                client.put_local(kvpair, clevel)
                transport.close()

    def put_local(self, kvpair, clevel):
        if DEBUG and 1:
            print("\t\tput_local at", self.meta.id, self.meta.ip, self.meta.port)
        self.__writeToCommitLog(kvpair, time.time())
        self.kvstore[kvpair.key] = kvpair.val

    def __populateKvstoreFromCommitLog(self):
        filename = 'commit_log' + str(self.meta.id)
        file = pathlib.Path(filename)
        if file.exists():
            with open(filename, 'r') as f:
                data = json.load(f)
                temp = data['commit_log']
                for l in temp:
                    self.kvstore[l['key']] = l['val']
        if DEBUG and 0:
            print("Populating kvstore from commit_log...")
            print("Contents of kvstore:", self.kvstore)

    def __writeToCommitLog(self, kvpair, timestamp):
        filename = 'commit_log' + str(self.meta.id)
        if DEBUG and 0:
            print("commit filename", filename)
        file = pathlib.Path(filename)
        if file.exists():
            with open(filename, 'r') as f:
                data = json.load(f)
                temp = data['commit_log']
                temp.append({"key":kvpair.key, "val":kvpair.val, "time":timestamp})
            with open(filename, 'w') as f:    
                json.dump(data,f)
        else:
            with open(filename, 'w') as f:
                json.dump({"commit_log": [ {"key":kvpair.key, "val":kvpair.val, "time":timestamp} ] }, f)

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
