#!/usr/bin/env python

import sys
import glob
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from chord import FileStore
from chord.ttypes import NodeID, RFile, RFileMetadata, SystemException

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import hashlib


def main():
    print(sys.argv[0])
    print(sys.argv[1:])
    # Make socket
    transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)

    # Connect!
    transport.open()
    filename = sys.argv[3]
    content = filename + "'s content"
    file = RFile(RFileMetadata(filename, 0), content)
    #file = RFile(RFileMetadata("new_file", 0), "this is the content of the file")
    print("file content:", file.content)

    client.writeFile(file)
    print('writeFile(rFile)')

    readfile = client.readFile(filename)
    print("File Read: " + readfile.meta.filename, readfile.meta.version)

    node_list = []

    client.setFingertable(node_list)
    print('gsetFingertable(node_list)')

    file_key = hashlib.sha256((filename).encode('utf-8')).hexdigest()

    client.findSucc(file_key)
    print("findSucc(" + file_key + ")")

    client.findPred(file_key)
    print("findPred(" + file_key + ")")

    client.getNodeSucc()
    print('getNodeSucc()')
    # Close!
    transport.close()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
