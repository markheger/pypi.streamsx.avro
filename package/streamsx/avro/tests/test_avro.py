from unittest import TestCase

import streamsx.avro as avro

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr
import datetime
import os
import uuid
import json

class JsonData(object):
    def __init__(self, prefix, count):
        self.prefix = prefix
        self.count = count
    def __call__(self):
        for i in range(self.count):
            yield {'p': self.prefix + '_' + str(i), 'c': i}

def avro_test_schema_string():
    return '{"type" : "record", "name" : "test_schema", "namespace" : "streamsx.avro.tests", "fields" : [ {"name" : "p", "type" : "string", "doc" : "prefix"}, {"name" : "c", "type" : "long", "doc" : "counter"}], "doc:" : "test message schema"}'

def avro_test_schema_file():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    return script_dir+'/test.avsc'

class TestParams(TestCase):

    def test_to_avro_params(self):
        topo = Topology()
        s = topo.source(JsonData('a', 1)).as_json()
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, tuples_per_message=1000)
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, bytes_per_message=1024)
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, time_per_message=datetime.timedelta(seconds=5))
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, time_per_message=5)
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, time_per_message=15.0)


class TestAvro(TestCase):

    def _test_json_type(self, name, avro_schema):
        n = 100
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.avro_toolkit_home)

        uid = str(uuid.uuid4())
        s = topo.source(JsonData(uid, n)).as_json()
        # convert json to avro blob
        o = avro.json_to_avro(s, avro_schema)
        # convert avro blob to json
        res = avro.avro_to_json(o, avro_schema)
        res.print()

        tester = Tester(topo)
        tester.tuple_count(res, n)
        #tester.run_for(60)
        # setup test config
        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    def test_json_type_with_schema_as_str(self):
        self._test_json_type('test_json_type_with_schema_as_str', avro_test_schema_string())

    def test_json_type_with_schema_as_file(self):
        self._test_json_type('test_json_type_with_schema_as_file', avro_test_schema_file())


    def test_hw_json(self):
        topo = Topology('test_hw_json')
        streamsx.spl.toolkit.add_toolkit(topo, self.avro_toolkit_home)

        avro_schema = '{"type" : "record", "name" : "hw_schema", "fields" : [{"name" : "a", "type" : "string"}]}'
        s = topo.source([{'a': 'Hello'}, {'a': 'World'}, {'a': '!'}]).as_json()
        
        # convert json to avro blob
        o = avro.json_to_avro(s, avro_schema)
        # convert avro blob to json
        res = avro.avro_to_json(o, avro_schema)
        res.print()

        tester = Tester(topo)
        tester.tuple_count(res, 3)
        #tester.run_for(60)
        # setup test config
        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    def test_hw_embed_schema(self):
        topo = Topology('test_hw_embed_schema')
        streamsx.spl.toolkit.add_toolkit(topo, self.avro_toolkit_home)

        avro_schema = '{"type" : "record", "name" : "hw_schema", "fields" : [{"name" : "a", "type" : "string"}]}'
        s = topo.source([{'a': 'Hello'}, {'a': 'World'}, {'a': '!'},{'a': 'Hello'}, {'a': 'World'}, {'a': '!'}]).as_json()
        
        # convert json to avro blob
        o = avro.json_to_avro(s, avro_schema, embed_avro_schema=True, tuples_per_message=3)
        # convert avro blob to json
        res = avro.avro_to_json(o)
        res.print()

        tester = Tester(topo)
        tester.tuple_count(res, 6)
        #tester.run_for(60)
        # setup test config
        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

class TestDistributed(TestAvro):
    def setUp(self):
        Tester.setup_distributed(self)
        self.avro_toolkit_home = os.environ["AVRO_TOOLKIT_HOME"]

class TestStreamingAnalytics(TestAvro):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.avro_toolkit_home = os.environ["AVRO_TOOLKIT_HOME"]

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()

