#!/usr/bin/env python

import glob
import sys
sys.path.append('gen-py')
# sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])
from kvstore import KVStore
from kvstore.ttypes import KVPair, NodeID, GetRet, GetRetTime, SystemException #, RFile, RFileMetadata, 
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import json
import pathlib
import re
import socket
import time

MAXKEY = 256
ONE = 0
QUORUM = 1
DEBUG = 0 

def p(b, msg):
    if DEBUG and b:
        print(msg)

def create_node(s):
    m=re.match('([^:]+):([^:]+)',s)
    nodes = (m.group(1),int(m.group(2)))
    return nodes

class KVStoreHandler:

    def __init__(self, id=None, ip=None, port=None, servers=None):
        self.meta = NodeID(id, ip, int(port))
        self.kvstore = {}
        self.kvtime = {}
        self.hinted = {}
        self.servers = servers
        self.__populateKvstoreFromCommitLog()
        self.getval = ""

    def __partition(self, key):
        slen = len(self.servers)
        p(1, "\tPartitioner: %d servers, key %d, MAXKEY %d" % (slen, key, MAXKEY-1))

        # Divide and portion range by number of servers
        partition = 0
        for i in range(slen):
            if key >= partition and key < partition + (MAXKEY//(slen)):
                id = self.servers[i][0]
                p(1, "\tkey " + str(key) + " goes to server at id " + str(id));
            partition += MAXKEY//slen
        return id

    def get(self, key, clevel):
        p(1, "get(" + str(key) + ") called")
        return self.__getFromReplicas(key, clevel)

    def _get(self, key):
        p(1, "\t\t_get from %d %s %d" % (self.meta.id, self.meta.ip, self.meta.port))
        if key in self.kvstore:
            return GetRetTime(self.kvstore[key], True, self.kvtime[key])
        else:
            return GetRetTime('', False, '')

    def __getFromReplicas(self, key, clevel):
        clevel_str = "ONE" if clevel == ONE else "QUORUM"
        getlist = []
        id0 = self.__partition(key)
        p(1, "\tGet key %d at consistency level %s from server %d" % (key, clevel_str, id0))
        servers_reached = 0
        for i in range(3):
            idx = (id0+i) % len(self.servers)
            id, ip, port = self.servers[idx][0], self.servers[idx][1], self.servers[idx][2]
            p(1, "%d %s:%d" % (id, ip, port))

            # Read locally if this server
            if id == self.meta.id:
                p(1, "\t\tGet from this server")
                # ret = self._get(key, clevel)
                ret = self._get(key)
                getlist.append(ret)
                servers_reached = servers_reached + 1

            # Otherwise request from remote server
            else:
                p(1, "\t\tGet from another server...")
                transport = TSocket.TSocket(ip, port);
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = KVStore.Client(protocol)
                try:
                    transport.open()
                    ret = client._get(key)
                    getlist.append(ret)
                except:
                    print("\t\t\tServer %d %s:%d not found" % (id, ip, port))
                else:
                    servers_reached = servers_reached + 1
                transport.close()
        p(1, ("\t%d servers reached" % servers_reached))
        if (clevel == ONE and servers_reached >= 1) or (clevel == QUORUM and servers_reached >= 2):
            p(1, ("\tconsistency level %s achieved" % "ONE" if clevel == ONE else "QUORUM"))
            most_recent = 0
            for i, l in enumerate(getlist):
                if l.time > getlist[most_recent].time:
                    most_recent = i
            return GetRet(getlist[most_recent].val, getlist[most_recent].ret) 
        else:
            raise SystemException("Consistently level %s not achieved" % clevel_str)

    def put(self, kvpair, clevel):
        p(1, ("\nput called at %s %d key: %s at time %s" % (self.meta.ip, self.meta.port, str(kvpair.key), str(time.time()))))
        self.__storeAndReplicate(kvpair, clevel)

    def __storeAndReplicate(self, kvpair, clevel):
        clevel_str = "ONE" if clevel == ONE else "QUORUM"
        id0 = self.__partition(kvpair.key)
        p(1, "\tPut kvpair %s at consistency level %s to server %d" % (kvpair.key, clevel_str, id0))
        servers_reached = 0
        failed_servers = []
        for i in range(3):
            idx = (id0+i) % len(self.servers)
            id, ip, port = self.servers[idx][0], self.servers[idx][1], self.servers[idx][2]

            # Write locally if this server
            if id == self.meta.id:
                p(1, "\t\tPut to this server")
                self._put(kvpair, clevel)
                servers_reached = servers_reached + 1

            # Otherwise send to remote server
            else:
                p(1, "\t\tPut to another server...")
                transport = TSocket.TSocket(ip, port);
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = KVStore.Client(protocol)
                try:
                    transport.open()
                    client._put(kvpair, clevel)
                except:
                    print("\t\t\tServer %d %s:%d not found" % (id, ip, port))
                    failed_servers.append(id)

                else:
                    servers_reached = servers_reached + 1
                transport.close()
        p(1, ("\t%d servers reached clevel %d" % (servers_reached, clevel)))
        if (clevel == ONE and servers_reached >= 1) or (clevel == QUORUM and servers_reached >= 2):
            p(1, ("\tconsistency level %s achieved" % "ONE" if clevel == ONE else "QUORUM"))
            if len(failed_servers) > 0:            
                # Store hinted handoff
                for i in failed_servers:
                    if i in self.hinted:
                        kv = self.hinted[i]
                        kv.append(KVPair(kvpair.key, kvpair.val))
                        self.hinted[i] = kv 
                    else:
                        self.hinted[i] = [KVPair(kvpair.key, kvpair.val)]
                    p(1, "\t\tHinted handoff to server %d, contents of self.hinted %s" % (i, self.hinted))
            return True
        else:
            raise SystemException("Consistently level %s not achieved" % clevel_str)

    def _put(self, kvpair, clevel):
        p(1, "\t\t_put at %d %s %d" % (self.meta.id, self.meta.ip, self.meta.port))
        t = time.time()
        self.__writeToCommitLog(kvpair, t)
        self.kvstore[kvpair.key] = kvpair.val
        self.kvtime[kvpair.key] = t

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

    def __populateKvstoreFromCommitLog(self):
        p(1, "Populating kvstore from commit_log...")
        filename = 'commit_log' + str(self.meta.id)
        file = pathlib.Path(filename)
        if file.exists():
            with open(filename, 'r') as f:
                data = json.load(f)
                temp = data['commit_log']
                for l in temp:
                    self.kvstore[l['key']] = l['val']
                    self.kvtime[l['key']] = l['time']
    
        p(1, "Contents of kvstore: %s" % self.kvstore)

        # # Populate from hinted handoff 
        # for s in self.servers:
        #     # print(s[1], s[2])
        #     id, ip, port = s[0], s[1], s[2]
        #     if s[0] != self.meta.id:
        #         transport = TSocket.TSocket(ip, port);
        #         transport = TTransport.TBufferedTransport(transport)
        #         protocol = TBinaryProtocol.TBinaryProtocol(transport)
        #         client = KVStore.Client(protocol)
        #         try:
        #             transport.open()
        #             client._put(KVPair(0, "hello"), 1)
        #         except:
        #             print("\t\t\tServer %d %s:%d not found" % (id, ip, port))

        #         transport.close()

    def _get_hints(self, id):
        return self.hinted[id]

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
        if DEBUG:
            print(u[0], u[1], u[2], end='')
        if u[1] == ip and int(u[2]) == int(port):
            if DEBUG:
                print(" <- this server", end='')
            id = count
        if DEBUG:
            print()
        servers.append(u)
    assert(id != None)
    return (servers, id)

def initServer():
    ip, port = getIP(), sys.argv[1]
    print('Starting server at %s:%s...' % (ip, port))
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
    server = initServer()
    server.serve()
