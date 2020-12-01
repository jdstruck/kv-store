exception SystemException {
  1: optional string message
}

/* struct RFileMetadata { */
/*   1: optional string filename; */
/*   2: optional i32 version; */
/* } */

/* struct RFile { */
/*   1: optional RFileMetadata meta; */
/*   2: optional string content; */
/* } */

struct NodeID {
  1: string id;
  2: string ip;
  3: i32 port;
}

struct KVPair {
  1: i32 key;
  2: string val;
}


service KVStore {

  string get(1: i32 key, 2: i32 clevel)
    throws (1: SystemException systemException),

  void put(1: KVPair kvpair, 2: i32 clevel)
    throws (1: SystemException systemException),

  void put_local(1: KVPair kvpair, 2: i32 clevel)
    throws (1: SystemException systemException),
 
  /* void writeFile(1: RFile rFile) */
  /*   throws (1: SystemException systemException), */

  /* RFile readFile(1: string filename) */
  /*   throws (1: SystemException systemException), */

  /* void setFingertable(1: list<NodeID> node_list), */

  /* NodeID findSucc(1: string key) */
  /*   throws (1: SystemException systemException), */

  /* NodeID findPred(1: string key) */
  /*   throws (1: SystemException systemException), */

  /* NodeID getNodeSucc() */
  /*   throws (1: SystemException systemException), */
}

