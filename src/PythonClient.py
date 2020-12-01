#!/usr/bin/env python

import sys
# import glob
sys.path.append('gen-py')
# sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from kvstore import KVStore
from kvstore.ttypes import KVPair, NodeID #, RFile, RFileMetadata, SystemException

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import hashlib


QUORUM = 0
ONE = 1

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
    client = KVStore.Client(protocol)

    # Connect!
    transport.open()
    # filename = sys.argv[3]
    # content = filename + "'s content"
    # file = RFile(RFileMetadata(filename, 0), content)
    while (True):
        inputstr = input('Enter 1 for get(), 2 for put()\n')
        try:
            func = int(inputstr)
        except ValueError:
            print("> Error:","'" + inputstr + "'", "is not a number")
            continue
        if func < 1 or func > 2:
            print('> Error: you entered', inputstr + '.\n', 'Enter 1 for get(), 2 for put()')
            continue

        # get()
        if func == 1:
            inputstr = input("Enter a number in range [0, 255]: ")
            try:
                key = int(inputstr)
            except ValueError:
                print("> Error:","'" + inputstr + "'", "is not a number")
                continue

            if(key < 0 or key > 255):
                print("> Error:", key, "is not between 0 and 255")
            else:
                val =  client.get(key, QUORUM)
                # assert(client.get(key, QUORUM) == val)
                print("Value =", val)
                print()
                print("Press CTRL-C to exit, or...")

        # put()
        else:
            inputstr = input("Enter a number in range [0, 255]: ")
            try:
                key = int(inputstr)
            except ValueError:
                print("> Error:","'" + inputstr + "'", "is not a number")
                continue

            if(key < 0 or key > 255):
                print("> Error:", key, "is not between 0 and 255")
            else:
                val = input("Enter a string of characters: ")
                client.put(KVPair(key, val), QUORUM)
                # assert(client.get(key, QUORUM) == val)
                print("Success!\n")
                print("Press CTRL-C to exit, or...")

    transport.close()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
