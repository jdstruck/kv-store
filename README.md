# CS557 Programming Assignment 2: Chord Distributed Hash Table 
Distributed file server using a Chord-based Distributed Hash Table (DHT). Remote Procedure Calls (RPC) are handled by Apache Thrift.

## How to run
### Start the server
The server executable takes a single command-line argument specifying the port where the Thrift Start the server will listen for remote clients. For example:

    $ ./server.py 9000


### Run the initializer

    $ chmod +x init

The initializer program takes a single argument, the name of a file containing all existing nodes' IP and Port number formatted <ip>:<port>. 
This must be populated manually before running ust be presentmust be populated (manually) containing all of the existing nodes. For Example:

    $ ./init notes.txt
