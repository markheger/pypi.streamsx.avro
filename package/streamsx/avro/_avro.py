# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import os
from tempfile import mkstemp
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.spl.types import rstring
import datetime
from streamsx.toolkits import download_toolkit
import streamsx.topology.composite

_TOOLKIT_NAME = 'com.ibm.streamsx.avro'


AvroStreamSchema = CommonSchema.Binary
"""Structured schema containing the message in Avro format.

``'tuple<blob binary>'``
"""

def _check_time_param(time_value, parameter_name):
    if isinstance(time_value, datetime.timedelta):
        result = time_value.total_seconds()
    elif isinstance(time_value, int) or isinstance(time_value, float):
        result = time_value
    else:
        raise TypeError(time_value)
    if result <= 1:
        raise ValueError("Invalid "+parameter_name+" value. Value must be at least one second.")
    return result


def _add_avro_message_schema_file(topology, message_schema):
    if os.path.isfile(message_schema):
        path = message_schema
    else:
        fd, path = mkstemp(suffix='.json', prefix='avsc', text=True)
        with open(fd, 'w') as tmpfile:
            tmpfile.write(message_schema)
    # add to application dir in bundle
    topology.add_file_dependency(path, 'etc')
    filename = os.path.basename(path)
    return 'etc/'+filename

def download_toolkit(url=None, target_dir=None):
    r"""Downloads the latest Avro toolkit from GitHub.

    Example for updating the Avro toolkit for your topology with the latest toolkit from GitHub::

        import streamsx.avro as avro
        # download Avro toolkit from GitHub
        avro_toolkit_location = avro.download_toolkit()
        # add the toolkit to topology
        streamsx.spl.toolkit.add_toolkit(topology, avro_toolkit_location)

    Example for updating the topology with a specific version of the Avro toolkit using a URL::

        import streamsx.avro as avro
        url122 = 'https://github.com/IBMStreams/streamsx.avro/releases/download/v1.2.2/streamsx.avro-1.2.2-95c5cd9-20190311-1233.tgz'
        avro_toolkit_location = avro.download_toolkit(url=url122)
        streamsx.spl.toolkit.add_toolkit(topology, avro_toolkit_location)

    Args:
        url(str): Link to toolkit archive (\*.tgz) to be downloaded. Use this parameter to 
            download a specific version of the toolkit.
        target_dir(str): the directory where the toolkit is unpacked to. If a relative path is given,
            the path is appended to the system temporary directory, for example to /tmp on Unix/Linux systems.
            If target_dir is ``None`` a location relative to the system temporary directory is chosen.

    Returns:
        str: the location of the downloaded Avro toolkit

    .. note:: This function requires an outgoing Internet connection
    .. versionadded:: 1.1
    """
    _toolkit_location = streamsx.toolkits.download_toolkit (toolkit_name=_TOOLKIT_NAME, url=url, target_dir=target_dir)
    return _toolkit_location



def json_to_avro(stream, message_schema, embed_avro_schema=False, time_per_message=None, tuples_per_message=None, bytes_per_message=None, name=None):
    """Converts JSON strings into binary Avro messages with schema :py:const:`CommonSchema.Binary`. 

    Args:
        stream(streamsx.topology.topology.Stream): Stream of tuples containing the JSON records. Supports :py:const:`CommonSchema.Json` as input.
        message_schema(str|file): Avro schema to serialize the Avro message from JSON input.
        embed_avro_schema(bool): Embed the schema in the generated Avro message. When generating Avro messages that must be persisted to a file system, the schema is expected to be included in the file. If this parameter is set to true, incoming JSON tuples are batched and a large binary object that contains the Avro schema and 1 or more messages is generated. Also, you must specify one of the parameters (bytes_per_message, tuples_per_message, time_per_message) that controls when Avro message block is submitted to the output port, otherwise it would expect a window punctuation marker. After submitting the Avro message to the output port, a punctuation is generated so that the receiving operator can potentially create a new file. 
        time_per_file(int|float|datetime.timedelta): Specifies the approximate time, in seconds, after before the Avro message block is submitted to the output port. Only valid if Avro schema is embedded. The ``bytes_per_message``, ``time_per_message`` and ``tuples_per_message`` parameters are mutually exclusive.
        tuples_per_file(int): The minimum number of tuples that the Avro message block should contain before it is submitted to the output port. Only valid if Avro schema is embedded. The ``bytes_per_message``, ``time_per_message`` and ``tuples_per_message`` parameters are mutually exclusive. 
        bytes_per_file(int): The minimum size in bytes that the Avro message block should be before it is submitted to the output port. Only valid if Avro schema is embedded. The ``bytes_per_message``, ``time_per_message`` and ``tuples_per_message`` parameters are mutually exclusive.

        name(str): Operator name in the Streams context, defaults to a generated name.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Stream`: Output Stream with schema :py:const:`CommonSchema.Binary` (Avro records in binary format).

    .. deprecated:: 1.2.0
        Use the :py:class:`~JSONToAvro`.

    """

    # check bytes_per_message, time_per_message and tuples_per_message parameters
    if (time_per_message is not None and tuples_per_message is not None) or (bytes_per_message is not None and time_per_message is not None) or (tuples_per_message is not None and bytes_per_message is not None):
        raise ValueError("The parameters are mutually exclusive: bytes_per_message, time_per_message, tuples_per_message")


    _op = _JSONToAvro(stream, schema=AvroStreamSchema, name=name)
    _op.params['avroMessageSchemaFile'] = _op.expression('getThisToolkitDir()+"/'+_add_avro_message_schema_file(stream.topology, message_schema)+'"')

    if embed_avro_schema is True:
        _op.params['embedAvroSchema'] = _op.expression('true')
        if time_per_message is None and tuples_per_message is None and bytes_per_message is None:
            _op.params['submitOnPunct'] = _op.expression('true')
        if time_per_message is not None:
            _op.params['timePerMessage'] = streamsx.spl.types.float64(_check_time_param(time_per_message, 'time_per_message'))
        if tuples_per_message is not None:
            _op.params['tuplesPerMessage'] = streamsx.spl.types.int64(tuples_per_message)
        if bytes_per_message is not None:
            _op.params['bytesPerMessage'] = streamsx.spl.types.int64(bytes_per_message)

    return _op.outputs[0]


def avro_to_json(stream, message_schema=None, name=None):
    """Converts binary Avro messages to JSON strings.

    Args:
        stream(streamsx.topology.topology.Stream): Stream of tuples containing the binary Avro records. Supports :py:const:`CommonSchema.Binary` as input.
        message_schema(str|file): Avro schema to deserialize the binary Avro message to JSON. If not specified, it is expected that the schema is embedded in the message.
        name(str): Operator name in the Streams context, defaults to a generated name.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Stream`: Output Stream with schema :py:const:`CommonSchema.Json`.

    .. deprecated:: 1.2.0
        Use the :py:class:`~AvroToJSON`.
    """

    _op = _AvroToJSON(stream, schema=CommonSchema.Json, name=name)
    if message_schema is not None:
        _op.params['avroMessageSchemaFile'] = _op.expression('getThisToolkitDir()+"/'+_add_avro_message_schema_file(stream.topology, message_schema)+'"')
    return _op.outputs[0]


class AvroToJSON(streamsx.topology.composite.Map):
    """
    Converts binary Avro messages to JSON strings.

    Supports :py:const:`CommonSchema.Binary` as input and returns output stream with schema :py:const:`CommonSchema.Json`.

    Example mapping stream ``b`` with binary AVRO messages to JSON strings in stream ``j``::

        import streamsx.avro as avro
        topo = Topology()
        ...
        avro_schema = '{"type" : "record", "name" : "hw_schema", "fields" : [{"name" : "a", "type" : "string"}]}'
        j = b.map(avro.AvroToJSON(message_schema=avro_schema))

    .. versionadded:: 1.2

    Attributes
    ----------
    message_schema : str|file
        Avro schema to deserialize the binary Avro message to JSON. If not specified, it is expected that the schema is embedded in the message.
    """

    def __init__(self, message_schema):

        self.message_schema = message_schema
        self.vm_arg = None
        
    @property
    def vm_arg(self):
        """
            str: Arbitrary JVM arguments can be passed to the Streams operator
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, value):
        self._vm_arg = value

    def populate(self, topology, stream, schema, name, **options):

        _op = _AvroToJSON(stream, schema=CommonSchema.Json, vmArg=self.vm_arg, name=name)
        if self.message_schema is not None:
            _op.params['avroMessageSchemaFile'] = _op.expression('getThisToolkitDir()+"/'+_add_avro_message_schema_file(topology, self.message_schema)+'"')
        return _op.outputs[0]



class JSONToAvro(streamsx.topology.composite.Map):
    """
    Converts JSON strings into binary Avro messages with schema :py:const:`CommonSchema.Binary`

    Supports :py:const:`CommonSchema.Json` as input and returns output stream with schema :py:const:`CommonSchema.Binary`.

    Example mapping stream ``j`` with JSON string to stream ``b`` with binary AVRO messages::

        import streamsx.avro as avro
        topo = Topology()

        avro_schema = '{"type" : "record", "name" : "hw_schema", "fields" : [{"name" : "a", "type" : "string"}]}'
        j = topo.source([{'a': 'Hello'}, {'a': 'World'}, {'a': '!'}]).as_json()
        b = j.map(avro.JSONToAvro(message_schema=avro_schema))

    .. versionadded:: 1.2

    Attributes
    ----------
    message_schema : str|file
        Avro schema to deserialize the binary Avro message to JSON. If not specified, it is expected that the schema is embedded in the message.
    embed_avro_schema: bool
        Embed the schema in the generated Avro message. When generating Avro messages that must be persisted to a file system, the schema is expected to be included in the file. If this parameter is set to true, incoming JSON tuples are batched and a large binary object that contains the Avro schema and 1 or more messages is generated. Also, you must specify one of the parameters (bytes_per_message, tuples_per_message, time_per_message) that controls when Avro message block is submitted to the output port, otherwise it would expect a window punctuation marker. After submitting the Avro message to the output port, a punctuation is generated so that the receiving operator can potentially create a new file. 
    time_per_file: int|float|datetime.timedelta
        Specifies the approximate time, in seconds, after before the Avro message block is submitted to the output port. Only valid if Avro schema is embedded. The ``bytes_per_message``, ``time_per_message`` and ``tuples_per_message`` parameters are mutually exclusive.
    tuples_per_file: int
        The minimum number of tuples that the Avro message block should contain before it is submitted to the output port. Only valid if Avro schema is embedded. The ``bytes_per_message``, ``time_per_message`` and ``tuples_per_message`` parameters are mutually exclusive. 
    bytes_per_file: int
        The minimum size in bytes that the Avro message block should be before it is submitted to the output port. Only valid if Avro schema is embedded. The ``bytes_per_message``, ``time_per_message`` and ``tuples_per_message`` parameters are mutually exclusive.
    """

    def __init__(self, message_schema, embed_avro_schema=False, time_per_message=None, tuples_per_message=None, bytes_per_message=None):

        self.message_schema = message_schema
        self.embed_avro_schema = embed_avro_schema
        self.time_per_message = time_per_message
        self.tuples_per_message = tuples_per_message
        self.bytes_per_message = bytes_per_message
        self.vm_arg = None
        
    @property
    def vm_arg(self):
        """
            str: Arbitrary JVM arguments can be passed to the Streams operator
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, value):
        self._vm_arg = value

    def populate(self, topology, stream, schema, name, **options):

        # check bytes_per_message, time_per_message and tuples_per_message parameters
        if (self.time_per_message is not None and self.tuples_per_message is not None) or (self.bytes_per_message is not None and self.time_per_message is not None) or (self.tuples_per_message is not None and self.bytes_per_message is not None):
            raise ValueError("The parameters are mutually exclusive: bytes_per_message, time_per_message, tuples_per_message")

        _op = _JSONToAvro(stream, schema=AvroStreamSchema, vmArg=self.vm_arg, name=name)
        _op.params['avroMessageSchemaFile'] = _op.expression('getThisToolkitDir()+"/'+_add_avro_message_schema_file(topology, self.message_schema)+'"')

        if self.embed_avro_schema is True:
            _op.params['embedAvroSchema'] = _op.expression('true')
            if self.time_per_message is None and self.tuples_per_message is None and self.bytes_per_message is None:
                _op.params['submitOnPunct'] = _op.expression('true')
            if self.time_per_message is not None:
                _op.params['timePerMessage'] = streamsx.spl.types.float64(_check_time_param(self.time_per_message, 'time_per_message'))
            if self.tuples_per_message is not None:
                _op.params['tuplesPerMessage'] = streamsx.spl.types.int64(self.tuples_per_message)
            if self.bytes_per_message is not None:
                _op.params['bytesPerMessage'] = streamsx.spl.types.int64(self.bytes_per_message)
        return _op.outputs[0]


class _AvroToJSON(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema, avroKeySchemaFile=None, avroMessageSchemaFile=None, inputAvroKey=None, inputAvroMessage=None, outputJsonKey=None, outputJsonMessage=None, vmArg=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.avro::AvroToJSON"
        inputs=stream
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if avroKeySchemaFile is not None:
            params['avroKeySchemaFile'] = avroKeySchemaFile
        if avroMessageSchemaFile is not None:
            params['avroMessageSchemaFile'] = avroMessageSchemaFile
        if inputAvroKey is not None:
            params['inputAvroKey'] = inputAvroKey
        if inputAvroMessage is not None:
            params['inputAvroMessage'] = inputAvroMessage
        if outputJsonKey is not None:
            params['outputJsonKey'] = outputJsonKey
        if outputJsonMessage is not None:
            params['outputJsonMessage'] = outputJsonMessage

        super(_AvroToJSON, self).__init__(topology,kind,inputs,schema,params,name)


class _JSONToAvro(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema, avroMessageSchemaFile=None, bytesPerMessage=None, embedAvroSchema=None, ignoreParsingError=None, inputJsonMessage=None, outputAvroMessage=None, submitOnPunct=None, timePerMessage=None, tuplesPerMessage=None, vmArg=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.avro::JSONToAvro"
        inputs=stream
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if avroMessageSchemaFile is not None:
            params['avroMessageSchemaFile'] = avroMessageSchemaFile
        if bytesPerMessage is not None:
            params['bytesPerMessage'] = bytesPerMessage
        if embedAvroSchema is not None:
            params['embedAvroSchema'] = embedAvroSchema
        if ignoreParsingError is not None:
            params['ignoreParsingError'] = ignoreParsingError
        if inputJsonMessage is not None:
            params['inputJsonMessage'] = inputJsonMessage
        if outputAvroMessage is not None:
            params['outputAvroMessage'] = outputAvroMessage
        if submitOnPunct is not None:
            params['submitOnPunct'] = submitOnPunct
        if timePerMessage is not None:
            params['timePerMessage'] = timePerMessage
        if tuplesPerMessage is not None:
            params['tuplesPerMessage'] = tuplesPerMessage

        super(_JSONToAvro, self).__init__(topology,kind,inputs,schema,params,name)

