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
import socket
import hashlib

DEBUG = True 

class KVStoreHandler:

    def __init__(self, id=None, ip=None, port=None):
        self.n = NodeID(id, ip, int(port))
        self.kvstore = {}
        
        if DEBUG:
            print("Initialize Node...\nNode ID: " + id, "\nNode IP: " + ip, "\nNode Port: " + port + "\n")
    def get(self, key):
        if DEBUG:
            print("get", key)
        return kvstore[key]

    def put(self, KVPair):
        if DEBUG:
            print("put", KVPair.key, KVPair.val)
        return

def getIP():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    ip = s.getsockname()[0]
    s.close()
    return ip

def getID(s):
    return hashlib.sha256((s).encode('utf-8')).hexdigest()

def initServer():
    ip, port = getIP(), sys.argv[1]
    handler = KVStoreHandler(getID(ip + ":" + port), ip, port)
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
