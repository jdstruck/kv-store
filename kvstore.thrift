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

struct KVPairHinted {
  1: i32 key;
  2: string val;
}

struct GetRet {
  1: string val;
  2: bool ret;
}

struct GetRetTime {
  1: string val;
  2: bool ret;
  3: optional double time;
}

service KVStore {

  GetRet get(1: i32 key, 2: i32 clevel)
    throws (1: SystemException systemException),

  GetRetTime _get(1: i32 key)
    throws (1: SystemException systemException),
    
  void put(1: KVPair kvpair, 2: i32 clevel)
    throws (1: SystemException systemException),

  void _put(1: KVPair kvpair, 2: i32 clevel)
    throws (1: SystemException systemException),
  
  list<KVPair> _get_hints(1: i32 id)

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

