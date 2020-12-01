#
# Autogenerated by Thrift Compiler (0.14.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys
import logging
from .ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
all_structs = []


class Iface(object):
    def get(self, key, clevel):
        """
        Parameters:
         - key
         - clevel

        """
        pass

    def put(self, kvpair, clevel):
        """
        Parameters:
         - kvpair
         - clevel

        """
        pass

    def put_local(self, kvpair, clevel):
        """
        Parameters:
         - kvpair
         - clevel

        """
        pass


class Client(Iface):
    def __init__(self, iprot, oprot=None):
        self._iprot = self._oprot = iprot
        if oprot is not None:
            self._oprot = oprot
        self._seqid = 0

    def get(self, key, clevel):
        """
        Parameters:
         - key
         - clevel

        """
        self.send_get(key, clevel)
        return self.recv_get()

    def send_get(self, key, clevel):
        self._oprot.writeMessageBegin('get', TMessageType.CALL, self._seqid)
        args = get_args()
        args.key = key
        args.clevel = clevel
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_get(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = get_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        if result.systemException is not None:
            raise result.systemException
        raise TApplicationException(TApplicationException.MISSING_RESULT, "get failed: unknown result")

    def put(self, kvpair, clevel):
        """
        Parameters:
         - kvpair
         - clevel

        """
        self.send_put(kvpair, clevel)
        self.recv_put()

    def send_put(self, kvpair, clevel):
        self._oprot.writeMessageBegin('put', TMessageType.CALL, self._seqid)
        args = put_args()
        args.kvpair = kvpair
        args.clevel = clevel
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_put(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = put_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.systemException is not None:
            raise result.systemException
        return

    def put_local(self, kvpair, clevel):
        """
        Parameters:
         - kvpair
         - clevel

        """
        self.send_put_local(kvpair, clevel)
        self.recv_put_local()

    def send_put_local(self, kvpair, clevel):
        self._oprot.writeMessageBegin('put_local', TMessageType.CALL, self._seqid)
        args = put_local_args()
        args.kvpair = kvpair
        args.clevel = clevel
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_put_local(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = put_local_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.systemException is not None:
            raise result.systemException
        return


class Processor(Iface, TProcessor):
    def __init__(self, handler):
        self._handler = handler
        self._processMap = {}
        self._processMap["get"] = Processor.process_get
        self._processMap["put"] = Processor.process_put
        self._processMap["put_local"] = Processor.process_put_local
        self._on_message_begin = None

    def on_message_begin(self, func):
        self._on_message_begin = func

    def process(self, iprot, oprot):
        (name, type, seqid) = iprot.readMessageBegin()
        if self._on_message_begin:
            self._on_message_begin(name, type, seqid)
        if name not in self._processMap:
            iprot.skip(TType.STRUCT)
            iprot.readMessageEnd()
            x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
            oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.trans.flush()
            return
        else:
            self._processMap[name](self, seqid, iprot, oprot)
        return True

    def process_get(self, seqid, iprot, oprot):
        args = get_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = get_result()
        try:
            result.success = self._handler.get(args.key, args.clevel)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except SystemException as systemException:
            msg_type = TMessageType.REPLY
            result.systemException = systemException
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("get", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_put(self, seqid, iprot, oprot):
        args = put_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = put_result()
        try:
            self._handler.put(args.kvpair, args.clevel)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except SystemException as systemException:
            msg_type = TMessageType.REPLY
            result.systemException = systemException
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("put", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_put_local(self, seqid, iprot, oprot):
        args = put_local_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = put_local_result()
        try:
            self._handler.put_local(args.kvpair, args.clevel)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except SystemException as systemException:
            msg_type = TMessageType.REPLY
            result.systemException = systemException
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("put_local", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

# HELPER FUNCTIONS AND STRUCTURES


class get_args(object):
    """
    Attributes:
     - key
     - clevel

    """


    def __init__(self, key=None, clevel=None,):
        self.key = key
        self.clevel = clevel

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I32:
                    self.key = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I32:
                    self.clevel = iprot.readI32()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('get_args')
        if self.key is not None:
            oprot.writeFieldBegin('key', TType.I32, 1)
            oprot.writeI32(self.key)
            oprot.writeFieldEnd()
        if self.clevel is not None:
            oprot.writeFieldBegin('clevel', TType.I32, 2)
            oprot.writeI32(self.clevel)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(get_args)
get_args.thrift_spec = (
    None,  # 0
    (1, TType.I32, 'key', None, None, ),  # 1
    (2, TType.I32, 'clevel', None, None, ),  # 2
)


class get_result(object):
    """
    Attributes:
     - success
     - systemException

    """


    def __init__(self, success=None, systemException=None,):
        self.success = success
        self.systemException = systemException

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.STRING:
                    self.success = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 1:
                if ftype == TType.STRUCT:
                    self.systemException = SystemException.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('get_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.STRING, 0)
            oprot.writeString(self.success.encode('utf-8') if sys.version_info[0] == 2 else self.success)
            oprot.writeFieldEnd()
        if self.systemException is not None:
            oprot.writeFieldBegin('systemException', TType.STRUCT, 1)
            self.systemException.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(get_result)
get_result.thrift_spec = (
    (0, TType.STRING, 'success', 'UTF8', None, ),  # 0
    (1, TType.STRUCT, 'systemException', [SystemException, None], None, ),  # 1
)


class put_args(object):
    """
    Attributes:
     - kvpair
     - clevel

    """


    def __init__(self, kvpair=None, clevel=None,):
        self.kvpair = kvpair
        self.clevel = clevel

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.kvpair = KVPair()
                    self.kvpair.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I32:
                    self.clevel = iprot.readI32()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('put_args')
        if self.kvpair is not None:
            oprot.writeFieldBegin('kvpair', TType.STRUCT, 1)
            self.kvpair.write(oprot)
            oprot.writeFieldEnd()
        if self.clevel is not None:
            oprot.writeFieldBegin('clevel', TType.I32, 2)
            oprot.writeI32(self.clevel)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(put_args)
put_args.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'kvpair', [KVPair, None], None, ),  # 1
    (2, TType.I32, 'clevel', None, None, ),  # 2
)


class put_result(object):
    """
    Attributes:
     - systemException

    """


    def __init__(self, systemException=None,):
        self.systemException = systemException

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.systemException = SystemException.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('put_result')
        if self.systemException is not None:
            oprot.writeFieldBegin('systemException', TType.STRUCT, 1)
            self.systemException.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(put_result)
put_result.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'systemException', [SystemException, None], None, ),  # 1
)


class put_local_args(object):
    """
    Attributes:
     - kvpair
     - clevel

    """


    def __init__(self, kvpair=None, clevel=None,):
        self.kvpair = kvpair
        self.clevel = clevel

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.kvpair = KVPair()
                    self.kvpair.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I32:
                    self.clevel = iprot.readI32()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('put_local_args')
        if self.kvpair is not None:
            oprot.writeFieldBegin('kvpair', TType.STRUCT, 1)
            self.kvpair.write(oprot)
            oprot.writeFieldEnd()
        if self.clevel is not None:
            oprot.writeFieldBegin('clevel', TType.I32, 2)
            oprot.writeI32(self.clevel)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(put_local_args)
put_local_args.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'kvpair', [KVPair, None], None, ),  # 1
    (2, TType.I32, 'clevel', None, None, ),  # 2
)


class put_local_result(object):
    """
    Attributes:
     - systemException

    """


    def __init__(self, systemException=None,):
        self.systemException = systemException

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.systemException = SystemException.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('put_local_result')
        if self.systemException is not None:
            oprot.writeFieldBegin('systemException', TType.STRUCT, 1)
            self.systemException.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(put_local_result)
put_local_result.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'systemException', [SystemException, None], None, ),  # 1
)
fix_spec(all_structs)
del all_structs
