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


ONE = 0
QUORUM = 1

def main():
    print(sys.argv[0])
    print(sys.argv[1:])

    # Connect to KVStore Thrift server
    transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = KVStore.Client(protocol)
    transport.open()

    # Main program loop (CTRL-C to exit)
    while (True):

        # Choose get (1) or put (2)
        inputstr = input('Enter 1 for get(), 2 for put()\n')
        try:
            func = int(inputstr)
        except ValueError:
            print("> Error:","'" + inputstr + "'", "is not a number")
            continue
        if func < 1 or func > 2:
            print('> Error: you entered', inputstr + '.\n', 'Enter 1 for get(), 2 for put()')
            continue
        
        # Indicate consistenty level ONE (1) or QUORUM (2)
        inputstr = input('Enter 1 for consistently level ONE, 2 for QUORUM\n')
        try:
            clevel = int(inputstr)
        except ValueError:
            print("> Error:","'" + inputstr + "'", "is not a number")
            continue
        if clevel < 1 or clevel > 2:
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
                getret = client.get(key, clevel-1)
                val = getret.val
                ret = getret.ret
                if ret:
                    print(ret)
                    print("\nValue =", val)
                else:
                    print("\nValue for key", key, "not found")

                print("\nPress CTRL-C to exit, or...")

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
                client.put(KVPair(key, val), clevel-1)
                print("\nSuccess!\n")
                print("Press CTRL-C to exit, or...")

    transport.close()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
