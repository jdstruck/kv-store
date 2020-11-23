#!/usr/bin/env python

import glob
import sys
sys.path.append('gen-py')
# sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])
# from chord import FileStore

from kvstore.ttypes import KVPair # NodeID, RFile, RFileMetadata, SystemException

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import socket
import hashlib

DEBUG = True 

class FileStoreHandler:

    def __init__(self, id=None, ip=None, port=None):
        self.log = {}
        self.files = []
        self.n = NodeID(id, ip, int(port))
        self.succ = None
        self.pred = None
        self.fingertable = []
        if DEBUG:
            print("Initialize Node...\nNode ID: " + id, "\nNode IP: " + ip, "\nNode Port: " + port + "\n")

    def writeFile(self, rFile):
        """
        Given filename and contents, corresponding file should be written to the server
        Meta-information, incl filename and version to be stored on server side
        If filename does not exist, create new file version 0, otherwise overwrite file, increment version
        If server does not own fileID, i.e. the server is not the file's successor, SystemException should be thrown
        Run findPred once to find predecessor node of server and store returned information

        """
        fileID = getID(rFile.meta.filename)
        if (self.findSucc(fileID)) != self.n:
            SystemException("Server does not own file")
            

        if not self.pred:
            self.pred = self.findPred(fileID)

        

        if DEBUG:
            print("len(self.files)", len(self.files))

        if len(self.files) == 0:
            self.files.append(rFile)
            if DEBUG:
                print("writeFile(): new file (first)", rFile.meta.filename)
        else:
            infile = False
            for f in self.files:
                if f.meta.filename == rFile.meta.filename:
                    f.content = rFile.content
                    f.meta.version = f.meta.version + 1
                    if DEBUG:
                        print("writeFile(): same filename:", f.meta.filename + ",", "update version:", f.meta.version)
                    infile = True
            if not infile:
                self.files.append(rFile)
        if DEBUG:
            for f in self.files:
                print("\t" + f.meta.filename, f.meta.version, getID(f.meta.filename))
            print()

    def readFile(self, filename):
        """
        Given a filename, find on server, return contents and meta-information, otherwise SystemException should be thrown

        """
        if (self.findSucc(fileID)) != self.n:
            SystemException("Server does not own file")
            
        for f in self.files:
            if f.meta.filename == filename:
                if DEBUG:
                    print("readFile() success:", f.meta.filename)
                return f

        raise SystemException("file not found")

    def setFingertable(self, node_list):
        """
        Set current node's fingertable to the fingertable provided in argument of function

        """
        self.fingertable.extend(node_list)

        if DEBUG:
            for f in self.fingertable:
                print(f)
            print("len(self.fingertable)", len(self.fingertable))

        self.succ = self.fingertable[0]
        self.pred = self.fingertable[-1]

        if DEBUG:
            print("self.pred", self.pred)
            print("curr_node", self.n)
            print("self.succ", self.succ)

    def findSucc(self, key):
        """
        Given identifier in DHT's key space, return DHT node that owns the id. Implemented in two parts
            1. function should call findPred to discover the DHT node that preceedes the given id
            2. function should call getNodeSucc to find the successor of this predecessor node

        """
        node = self.findPred(key)
        transport = TSocket.TSocket(node.ip, node.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = FileStore.Client(protocol)
        return client.getNodeSucc()

    def findPred(self, key):
        """
        Given an identifier in the DHT's key space, function should return DHT node that immediately precedes the id
        This preceeding node is the first node in the counter-clockwise direction in the Chord key space

        """
        if DEBUG:
            print("findPred", len(self.fingertable))
            print("findPred(key)")
            
        # SystemException is thrown if no fingertable exists for the current node
        if len(self.fingertable) == 0:
            raise SystemException("no fingertable")

        node1 = self.n
        node2 = self.fingertable[0]
        isbtw = is_btw(key, node1.id, node2.id)
        if DEBUG:
            print("node1", node1.id)
            print("keyar", key)
            print("node2", node2.id)
            print(isbtw)

        while not (is_btw(key, node1.id, node2.id)):
            for i in range(len(self.fingertable)-1, -1, -1):
                node2 = self.fingertable[i]
                if is_btw(node2.id, node1.id, key):
                    transport = TSocket.TSocket(node2.ip, node2.port)
                    transport = TTransport.TBufferedTransport(transport)
                    protocol = TBinaryProtocol.TBinaryProtocol(transport)
                    client = FileStore.Client(protocol)
                    return client.findPred(key)
            break
        return node1

    def getNodeSucc(self):
        """
        Return the closest node that follows current node in Chord key space

        SystemException is thrown if no fingertable exists for the current node

        """

        if DEBUG:
            print("getNodeSucc", "len(self.fingertable):", len(self.fingertable))

        # SystemException is thrown if no fingertable exists for the current node
        if len(self.fingertable) == 0:
            raise SystemException("no fingertable")

        # return the closest node that follows current node in the Chord key space
        return self.succ

def is_btw(file_key, node1_id, node2_id):
    if file_key > node1_id and file_key < node2_id:
        return True
    if node1_id > node2_id:
        if file_key < node1_id and file_key < node2_id:
            return True
        else:
            return False
    return False

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
    handler = FileStoreHandler(getID(ip + ":" + port), ip, port)
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    return server


if __name__ == '__main__':
    print('Starting the server...')
    server = initServer()
    server.serve()
